"""Shot_Injection_HD1_P1 (0/1 펄스 — 샷이 발사되는 순간 1, 평소 0) 값이 안 바뀌고
유지된 기간의 분포를 본다. Injection_Timer_Stable_Duration_Analysis.py 와 같은
방법(연속 동일값 구간 = 런)을 쓰지만, 이번 태그는 ON_T 타이머와 달리 0/1
이진값이라 두 가지 의미가 섞여 있다:
  - 값 0 유지 = 샷과 샷 사이 대기/정지 시간 (길면 "멈춰 있다"는 뜻)
  - 값 1 유지 = 샷 신호가 켜진 채로 유지되는 시간 (원래 짧아야 정상 — 길면 이상)
그래서 버킷 표와 타임라인 CSV 모두 값(0/1)을 같이 남겨서 구분할 수 있게 한다.

세부 버킷은 1~2번만 있는 구간도 안 묻히게 촘촘하게 잡는다(사용자가 5분대/9분대
처럼 드문 구간도 놓치고 싶지 않다고 함).

기간: 2026-09-09 19:00 ~ now (요청대로). 데이터는 이미 받아둔
parkkt/extracted_csv/2026-10-02_154723_analysis.csv 를 재사용— 이 파일이
2026-09-09 18:14 부터 커버하고 있어 새로 InfluxDB 조회가 필요 없다.

사용법:
    PYTHONIOENCODING=utf-8 python Shot_Injection_HD1_P1_Stable_Duration.py
"""
import os

import pandas as pd

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SRC_CSV = os.path.join(ROOT_DIR, "parkkt", "extracted_csv", "2026-10-02_154723_analysis.csv")
OUT_DIR = os.path.join(ROOT_DIR, "parkkt", "analysis_out", "shot_injection_hd1_p1_stable_duration")

TAG = "Shot_Injection_HD1_P1"
START = pd.Timestamp("2026-09-09 19:00:00")

# 촘촘한 버킷 — 1~2번만 있어도 보이도록 짧은 구간은 1초, 중간(4~12분)은 1분,
# 긴 구간은 10~30분~시간 단위로 세분화. 라벨은 경계값에서 자동 생성해서
# "경계 개수 = 라벨 개수 + 1" 이 항상 맞게 한다(수동으로 따로 적다가 어긋나기 쉬움).
BUCKETS_SEC = (
    [0, 1, 2, 3, 4, 5, 10, 20, 30, 45, 60, 90, 120, 180, 240]
    + list(range(300, 720 + 1, 60))        # 5~12분, 1분 단위
    + [900, 1200, 1800, 2700, 3600, 7200, 14400, float("inf")]
)


def _fmt_edge(sec):
    if sec < 60:
        return f"{sec:g}초"
    if sec < 3600:
        return f"{sec/60:g}분"
    return f"{sec/3600:g}시간"


def _make_labels(edges):
    labels = []
    for lo, hi in zip(edges[:-1], edges[1:]):
        if hi == float("inf"):
            labels.append(f"{_fmt_edge(lo)} 이상")
        else:
            labels.append(f"{_fmt_edge(lo)}~{_fmt_edge(hi)}")
    return labels


BUCKET_LABELS = _make_labels(BUCKETS_SEC)


def load():
    df = pd.read_csv(SRC_CSV, usecols=["Time", TAG], parse_dates=["Time"])
    df = df.rename(columns={"Time": "time"}).set_index("time").sort_index()
    df = df[~df.index.duplicated(keep="last")]
    df = df[df.index >= START]
    return df[TAG].dropna()


def stable_runs(series):
    """연속 동일값 구간(런)마다 (start, end, value, duration_sec). 양끝(절단) 제외."""
    s = series
    changed = s[s.diff() != 0]
    starts = changed.index[:-1]
    ends = changed.index[1:]
    vals = changed.to_numpy()[:-1]
    dur_sec = (ends - starts).total_seconds()
    df = pd.DataFrame({"start": starts, "end": ends, "value": vals, "duration_sec": dur_sec})
    return df.iloc[1:].reset_index(drop=True)  # 첫 구간(왼쪽 절단) 제외 — 마지막은 이미 ends 길이에서 빠짐


def bucket_table(runs):
    runs = runs.copy()
    runs["bucket"] = pd.cut(runs["duration_sec"], bins=BUCKETS_SEC, labels=BUCKET_LABELS,
                             right=False, include_lowest=True)
    table = pd.crosstab(runs["bucket"], runs["value"]).reindex(index=BUCKET_LABELS, fill_value=0)
    table.columns = [f"값={int(c)}" for c in table.columns]
    table = table[table.sum(axis=1) > 0]  # 어느 값이든 1건 이상 있는 버킷만 (1~2건도 포함)
    table["합계"] = table.sum(axis=1)

    time_sum = (runs.groupby(["bucket", "value"], observed=False)["duration_sec"].sum().unstack("value")
                .reindex(index=BUCKET_LABELS, fill_value=0) / 60)
    time_sum.columns = [f"값={int(c)}_총시간(분)" for c in time_sum.columns]
    time_sum = time_sum.loc[table.index]

    return pd.concat([table, time_sum.round(1)], axis=1)


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    s = load()
    print(f"원본: {len(s):,}행 ({s.index.min()} ~ {s.index.max()})")

    runs = stable_runs(s)
    print(f"유지 구간(런) 총 {len(runs):,}개 (첫 구간 1개는 왼쪽 절단이라 제외)")
    print(f"  값=0(대기) {len(runs[runs['value']==0]):,}개 / 값=1(샷 유지) {len(runs[runs['value']==1]):,}개")

    table = bucket_table(runs)
    pd.set_option("display.width", 160)
    print("\n=== 유지시간 세부 버킷 x 값(0/1) — 1건 이상 있는 구간만 전부 ===")
    print(table.to_string())

    # 사용자가 언급한 "5분대" / "9분대" 만 따로
    for lo, hi, label in [(270, 330, "~5분"), (510, 570, "~9분")]:
        hit = runs[(runs["duration_sec"] >= lo) & (runs["duration_sec"] < hi)]
        print(f"\n=== {label} (duration {lo}~{hi}초) 해당 구간: {len(hit)}건 ===")
        if not hit.empty:
            disp = hit.copy()
            disp["duration_min"] = (disp["duration_sec"] / 60).round(2)
            print(disp[["start", "end", "value", "duration_min"]].to_string(index=False))

    # 타임라인 전체 CSV (1~2건짜리도 전부 포함 — 필터링 없이 그대로)
    runs_out = runs.copy()
    runs_out["duration_min"] = (runs_out["duration_sec"] / 60).round(3)
    runs_out["date"] = runs_out["start"].dt.date
    runs_out["weekday"] = runs_out["start"].dt.day_name()
    runs_out["hour"] = runs_out["start"].dt.hour
    runs_out = runs_out[["start", "end", "value", "duration_sec", "duration_min", "date", "weekday", "hour"]]
    runs_out = runs_out.sort_values("start")

    timeline_path = os.path.join(OUT_DIR, "shot_injection_hd1_p1_timeline.csv")
    runs_out.to_csv(timeline_path, index=False, encoding="utf-8-sig")

    bucket_path = os.path.join(OUT_DIR, "shot_injection_hd1_p1_bucket_table.csv")
    table.to_csv(bucket_path, encoding="utf-8-sig")

    print(f"\n타임라인 저장(전체 유지구간, 1~2건짜리도 포함): {timeline_path}")
    print(f"버킷 표 저장: {bucket_path}")


if __name__ == "__main__":
    main()
