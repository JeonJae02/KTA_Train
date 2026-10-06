"""날짜별(그리고 날짜 x 3교대별)로 exchanger 자체 횟수, cool 자체 횟수(둘 다
180초 디바운스로 채터링 합친 사이클 기준), 그리고 exchanger 사이클 중 cool이
겹친 횟수/비율을 같이 보여준다.

exchanger 횟수 / cool 횟수는 서로 **독립적으로** 센 것이다(cool 이 exchanger
없이 혼자 켜지는 경우도 있을 수 있어서 따로 센다) — "겹침 횟수"만 두 신호의
교집합이다.

사용법 (장기구간 raw CSV 가 이미 있어야 함 —
Exchanger_Cool_Overlap_By_Shift_LongRange.py 참고):
    PYTHONIOENCODING=utf-8 python Exchanger_Cool_Counts_By_Day_Shift.py
"""

import glob
import os

import numpy as np
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RAW_DIR = os.path.join(BASE_DIR, "parkkt", "extracted_csv")
OUT_DIR = os.path.join(BASE_DIR, "parkkt", "analysis_out", "exchanger_off_on_gap")

EXCH_TAG = "워킹 Tank 1 BACK Exchanger SOL"
COOL_TAG = "워킹 Tank 1 BACK Cooling SOL"
THRESHOLD_SEC = 180
SHIFT_BINS = [0, 8, 16, 24]
SHIFT_LABELS = ["00-08", "08-16", "16-24"]


def latest_csv_with(*tags):
    cands = sorted(glob.glob(os.path.join(RAW_DIR, "*_analysis.csv")))
    for p in reversed(cands):
        cols = pd.read_csv(p, nrows=0).columns
        if all(t in cols for t in tags):
            return p
    return None


def raw_segments(series):
    s = series[series.diff() != 0]
    changes = pd.DataFrame({"val": s})
    changes["prev"] = changes["val"].shift(1)
    on_starts = changes.index[(changes["prev"].isna() | (changes["prev"] == 0)) & (changes["val"] == 1)]
    off_starts = changes.index[(changes["prev"] == 1) & (changes["val"] == 0)]
    segs, oi = [], 0
    for on_t in on_starts:
        while oi < len(off_starts) and off_starts[oi] <= on_t:
            oi += 1
        if oi >= len(off_starts):
            break
        segs.append((on_t, off_starts[oi]))
    return segs


def merge_cycles(segs, threshold):
    if not segs:
        return pd.DataFrame(columns=["cycle_start", "cycle_end"])
    rows = []
    cycle_start = segs[0][0]
    for i in range(len(segs) - 1):
        gap = (segs[i + 1][0] - segs[i][1]).total_seconds()
        if gap < threshold:
            continue
        rows.append({"cycle_start": cycle_start, "cycle_end": segs[i][1]})
        cycle_start = segs[i + 1][0]
    rows.append({"cycle_start": cycle_start, "cycle_end": segs[-1][1]})
    return pd.DataFrame(rows)


def main():
    path = latest_csv_with(EXCH_TAG, COOL_TAG)
    print(f"원본 CSV: {path}")
    raw = pd.read_csv(path, index_col="Time", parse_dates=True, usecols=["Time", EXCH_TAG, COOL_TAG])
    raw = raw[~raw.index.duplicated(keep="last")].sort_index()
    exch = raw[EXCH_TAG].dropna()
    cool = raw[COOL_TAG].dropna()

    exch_cycles = merge_cycles(raw_segments(exch), THRESHOLD_SEC)
    cool_cycles = merge_cycles(raw_segments(cool), THRESHOLD_SEC)
    print(f"exchanger 사이클 {len(exch_cycles)}개, cool 사이클 {len(cool_cycles)}개 (전체 기간)")

    # exchanger 사이클 중 cool 겹침 여부
    cool_sorted = cool.sort_index()
    overlaps = []
    for _, r in exch_cycles.iterrows():
        seg = cool_sorted.loc[r["cycle_start"]:r["cycle_end"]]
        before = cool_sorted.loc[:r["cycle_start"]]
        start_val = before.iloc[-1] if len(before) else 0
        overlaps.append(bool((seg == 1).any() or start_val == 1))
    exch_cycles["cool_overlap"] = overlaps

    exch_cycles["day"] = exch_cycles["cycle_start"].dt.date
    exch_cycles["shift"] = pd.cut(exch_cycles["cycle_start"].dt.hour, bins=SHIFT_BINS, labels=SHIFT_LABELS,
                                   right=False, include_lowest=True)
    cool_cycles["day"] = cool_cycles["cycle_start"].dt.date
    cool_cycles["shift"] = pd.cut(cool_cycles["cycle_start"].dt.hour, bins=SHIFT_BINS, labels=SHIFT_LABELS,
                                   right=False, include_lowest=True)

    # ---- 날짜별 ----
    exch_day = exch_cycles.groupby("day").size().rename("exchanger_횟수")
    cool_day = cool_cycles.groupby("day").size().rename("cool_횟수")
    overlap_day = exch_cycles.groupby("day")["cool_overlap"].sum().rename("겹침_횟수")
    day_table = pd.concat([exch_day, cool_day, overlap_day], axis=1).fillna(0).astype(int)
    day_table["겹침비율(%)"] = (day_table["겹침_횟수"] / day_table["exchanger_횟수"] * 100).round(1)
    print("\n=== 날짜별 (exchanger / cool 각자 횟수 + 겹침) ===")
    print(day_table.to_string())

    # ---- 날짜 x 교대별 ----
    exch_ds = exch_cycles.groupby(["day", "shift"], observed=True).size().rename("exchanger_횟수")
    cool_ds = cool_cycles.groupby(["day", "shift"], observed=True).size().rename("cool_횟수")
    overlap_ds = exch_cycles.groupby(["day", "shift"], observed=True)["cool_overlap"].sum().rename("겹침_횟수")
    idx = pd.MultiIndex.from_product([sorted(set(exch_cycles["day"]) | set(cool_cycles["day"])), SHIFT_LABELS],
                                      names=["day", "shift"])
    ds_table = pd.concat([exch_ds, cool_ds, overlap_ds], axis=1).reindex(idx).fillna(0).astype(int)
    denom = ds_table["exchanger_횟수"].astype(float).replace(0, np.nan)
    ds_table["겹침비율(%)"] = (ds_table["겹침_횟수"] / denom * 100).round(1).fillna(0.0)
    print("\n=== 날짜 x 교대별 (exchanger / cool 각자 횟수 + 겹침) ===")
    print(ds_table.to_string())

    print("\n=== 교대별 합계(전체 기간) ===")
    shift_total = ds_table.groupby("shift", observed=True)[["exchanger_횟수", "cool_횟수", "겹침_횟수"]].sum()
    shift_total["겹침비율(%)"] = (shift_total["겹침_횟수"] / shift_total["exchanger_횟수"] * 100).round(1)
    print(shift_total.to_string())

    day_table.to_csv(os.path.join(OUT_DIR, "counts_by_day.csv"), encoding="utf-8-sig")
    ds_table.to_csv(os.path.join(OUT_DIR, "counts_by_day_shift.csv"), encoding="utf-8-sig")
    print(f"\n저장: counts_by_day.csv / counts_by_day_shift.csv ({OUT_DIR})")


if __name__ == "__main__":
    main()
