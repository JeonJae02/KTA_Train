"""P1 ("워킹 Tank 1 BACK Exchanger SOL") 가 꺼진 뒤 다시 켜질 때까지의
시간차(= OFF 구간 길이)를 이번주 월요일부터 지금까지 날짜별/구간별로 센다.

사용자 가설: 짧은 쪽(10초 내외)과 긴 쪽(10분 이상) 두 그룹으로 갈릴 것 같다 —
그 가설을 세밀한 구간(버킷)으로 쪼개서 실측으로 확인한다.

**"기계가 완전히 서 있는 동안"은 제외한다.** 그 구간은 exchanger 가 그냥
"안 켤 이유가 없어서" 꺼져 있는 것이라 냉각 사이클과 무관한 OFF 다 — 포함하면
10분 이상 버킷이 전부 "야간 비가동"으로 덮여서 질문의 핵심(사이클링 중 OFF
길이)이 묻힌다. 기존 분석(Exchanger_Heat_Recovery_Time.py 등)과 같은 기준으로
제외한다: 0~7시 야간, 그리고 대차(Wagon)가 최근 30분간 전혀 안 움직인 구간.

사용법:
    PYTHONIOENCODING=utf-8 python Exchanger_Off_On_Gap_Analysis.py
"""

import os
import sys

import pandas as pd

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT_DIR)

from Log_Extractor import LogExtractor

ENV_PATH = os.path.join(ROOT_DIR, ".env")
SAVE_DIR = os.path.join(ROOT_DIR, "parkkt", "extracted_csv")
OUT_DIR = os.path.join(ROOT_DIR, "parkkt", "analysis_out", "exchanger_off_on_gap")

EXCH_TAG = "워킹 Tank 1 BACK Exchanger SOL"
WAGON_TAG = "Now_Actual_Wagon_Num"

START = "2026-09-28 00:00:00"   # 이번주 월요일 00:00 (KST)
END = "now()"

EXCLUDE_HOURS = (0, 7)          # 기존 분석과 동일 — 야간 비가동 시간대 제외
RUNNING_WINDOW_MIN = 30

# 구간(초) 경계 — 사용자가 말한 "5초 내외 / 5~10초 / ..." 를 촘촘히 깐 뒤,
# 10초 vs 10분 가설을 확인할 수 있게 중간 구간도 둔다.
BUCKETS_SEC = [0, 3, 5, 10, 20, 30, 60, 120, 300, 600, 1800, float("inf")]
BUCKET_LABELS = [
    "0~3초", "3~5초", "5~10초", "10~20초", "20~30초", "30~60초",
    "1~2분", "2~5분", "5~10분", "10~30분", "30분 이상",
]


def fetch():
    extractor = LogExtractor(env_path=ENV_PATH)
    df = extractor.get_data(start_time=START, end_time=END, target_tags=[EXCH_TAG, WAGON_TAG])
    os.makedirs(SAVE_DIR, exist_ok=True)
    extractor.save_to_csv(df, save_dir=SAVE_DIR)
    return df


def build_ok_mask(df):
    """야간(0~7시) + 대차 비가동 구간 제외 마스크."""
    w = df[WAGON_TAG].resample("1min").last().ffill()
    running = (w.diff().abs() > 0).rolling(RUNNING_WINDOW_MIN, min_periods=1).max()
    running = running.reindex(df.index, method="ffill").fillna(0)
    h = df.index.hour
    night_ok = (h < EXCLUDE_HOURS[0]) | (h >= EXCLUDE_HOURS[1])
    return night_ok & (running == 1)


def extract_off_on_gaps(df):
    """OFF 시작(= exch 1->0 시각) 과 바로 다음 ON 시작(= exch 0->1 시각) 쌍을 만든다."""
    s = df[EXCH_TAG].dropna()
    s = s[s.diff() != 0]  # 변화 지점만 남김(연속 중복 제거)
    changes = s.to_frame("val")
    changes["prev"] = changes["val"].shift(1)

    off_starts = changes.index[(changes["prev"] == 1) & (changes["val"] == 0)]
    on_starts = changes.index[(changes["prev"] == 0) & (changes["val"] == 1)]

    rows = []
    oi = 0
    for off_t in off_starts:
        # off_t 이후 첫 on_start 찾기
        while oi < len(on_starts) and on_starts[oi] <= off_t:
            oi += 1
        if oi >= len(on_starts):
            break
        on_t = on_starts[oi]
        rows.append({"off_time": off_t, "on_time": on_t,
                      "gap_sec": (on_t - off_t).total_seconds()})

    gaps = pd.DataFrame(rows)
    if gaps.empty:
        return gaps

    ok = build_ok_mask(df)
    gaps["ok"] = ok.reindex(gaps["off_time"], method="ffill").values
    gaps["day"] = gaps["off_time"].dt.date
    gaps["bucket"] = pd.cut(gaps["gap_sec"], bins=BUCKETS_SEC, labels=BUCKET_LABELS,
                            right=False, include_lowest=True)
    return gaps


def report(gaps):
    total = len(gaps)
    kept = gaps[gaps["ok"]]
    excluded = total - len(kept)
    print(f"\n전체 OFF→ON 이벤트 {total}개 중 야간/비가동 제외 {excluded}개, "
          f"분석 대상 {len(kept)}개\n")

    print("=== 날짜별 x 구간별 횟수 (야간/비가동 제외) ===")
    table = pd.crosstab(kept["day"], kept["bucket"])
    table = table.reindex(columns=BUCKET_LABELS, fill_value=0)
    table["합계"] = table.sum(axis=1)
    print(table.to_string())

    print("\n=== 전체 기간 구간별 횟수/비율 ===")
    overall = kept["bucket"].value_counts().reindex(BUCKET_LABELS, fill_value=0)
    overall_pct = (overall / overall.sum() * 100).round(1)
    summary = pd.DataFrame({"횟수": overall, "비율(%)": overall_pct})
    print(summary.to_string())

    print("\n=== 사용자 가설 검증: ~10초 그룹 vs 10분 이상 그룹 ===")
    short = kept[kept["gap_sec"] < 10]
    mid = kept[(kept["gap_sec"] >= 10) & (kept["gap_sec"] < 600)]
    long = kept[kept["gap_sec"] >= 600]
    for name, grp in (("10초 미만", short), ("10초~10분(중간)", mid), ("10분 이상", long)):
        if len(grp) == 0:
            print(f"  {name}: 0개")
            continue
        print(f"  {name}: {len(grp)}개 "
              f"(중앙값 {grp['gap_sec'].median():.1f}초, "
              f"평균 {grp['gap_sec'].mean():.1f}초, "
              f"범위 {grp['gap_sec'].min():.1f}~{grp['gap_sec'].max():.1f}초)")

    gap_zone = kept[(kept["gap_sec"] >= 10) & (kept["gap_sec"] < 600)]
    print(f"\n  10초~10분 사이(중간지대)에 낀 이벤트가 {len(gap_zone)}개"
          f"({100*len(gap_zone)/len(kept):.1f}%) 입니다 — "
          f"이 비율이 작을수록 '짧은 그룹 vs 긴 그룹'으로 뚜렷이 갈린다는 뜻입니다.")

    os.makedirs(OUT_DIR, exist_ok=True)
    csv_out = os.path.join(OUT_DIR, "off_on_gaps.csv")
    kept.to_csv(csv_out, index=False, encoding="utf-8-sig")
    print(f"\n이벤트 표 저장: {csv_out}")


def main():
    df = fetch()
    if df.empty:
        print("데이터가 비어 있습니다.")
        return
    gaps = extract_off_on_gaps(df)
    if gaps.empty:
        print("OFF→ON 이벤트가 없습니다.")
        return
    report(gaps)


if __name__ == "__main__":
    main()
