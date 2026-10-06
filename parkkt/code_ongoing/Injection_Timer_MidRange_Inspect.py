"""15~25분 / 25~35분대 유지 구간이 실제로 '가동 중(정상 사이클이 원래 긴 것)'
인지 '쉬는데 짧게 끝난 것'인지 눈으로 확인하기 위한 원자료 추출.

배경: Injection_Timer_Stable_Duration_PerTag.py 의 의미그룹 분류는 35분을
'활발 가동'과 '정지 추정'의 경계로 썼다. 그런데 경계 바로 아래인 15~35분대에
HD2_P2 가 1175건(전체 1276+81건의 대부분)으로 몰려 있어, 이게 노이즈인지
HD2_P2 의 정상 사이클 길이가 원래 더 긴 것인지 들여다봐야 한다. HD1_P1/HD4_P1
은 같은 구간에 각각 105건/77건뿐이라 성격이 다를 수 있다.

사용법:
    PYTHONIOENCODING=utf-8 python Injection_Timer_MidRange_Inspect.py
"""
import os

import pandas as pd

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
OUT_DIR = os.path.join(ROOT_DIR, "parkkt", "analysis_out", "injection_timer_stable_duration")
CSV_PATH = os.path.join(OUT_DIR, "stable_durations.csv")

LO_SEC, HI_SEC = 900, 2100  # 15분 ~ 35분
BREAK_HOURS = {10, 19, 23}  # 기존에 확인한 정기 휴식/야간 셧다운 시각대


def main():
    df = pd.read_csv(CSV_PATH, parse_dates=["start"])
    sub = df[(df["duration_sec"] >= LO_SEC) & (df["duration_sec"] < HI_SEC)].copy()

    sub["end"] = sub["start"] + pd.to_timedelta(sub["duration_sec"], unit="s")
    sub["duration_min"] = (sub["duration_sec"] / 60).round(2)
    sub["hour"] = sub["start"].dt.hour
    sub["weekday"] = sub["start"].dt.day_name()
    sub["date"] = sub["start"].dt.date
    sub["sub_bucket"] = pd.cut(sub["duration_sec"], bins=[900, 1500, 2100],
                                labels=["15~25분", "25~35분"], right=False)
    sub["known_break_hour"] = sub["hour"].isin(BREAK_HOURS)

    sub = sub[["tag", "start", "end", "duration_min", "sub_bucket", "value",
               "date", "weekday", "hour", "known_break_hour"]].sort_values(["tag", "start"])

    out_path = os.path.join(OUT_DIR, "mid_range_15_35min_detail.csv")
    sub.to_csv(out_path, index=False, encoding="utf-8-sig")

    print(f"15~35분대 구간: 총 {len(sub)}건")
    print(sub["tag"].value_counts().to_string())
    print(f"\n기존 휴식시간대(10/19/23시)와 겹치는 비율:")
    print(sub.groupby("tag")["known_break_hour"].mean().mul(100).round(1).to_string())
    print(f"\n태그별 duration_min 분포 (min/median/max):")
    print(sub.groupby("tag")["duration_min"].agg(["min", "median", "max", "count"]).to_string())
    print(f"\n저장: {out_path}")


if __name__ == "__main__":
    main()
