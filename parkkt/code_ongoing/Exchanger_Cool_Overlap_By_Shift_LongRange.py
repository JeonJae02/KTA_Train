"""`Exchanger_Cool_Overlap_By_Shift.py` 와 같은 분석(날짜 x 3교대별 exchanger
횟수 / cool 겹침 횟수·비율, RAW/EXCL 두 버전)을 더 긴 구간(2026-09-09 10:00 ~
지금)에 대해 처음부터 다시 돌린다 — raw InfluxDB 추출부터 사이클 병합(180초
디바운스), cool 겹침 판정, 교대별 집계까지 전부 포함.

사용법:
    PYTHONIOENCODING=utf-8 python Exchanger_Cool_Overlap_By_Shift_LongRange.py
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
COOL_TAG = "워킹 Tank 1 BACK Cooling SOL"

START = "2026-09-09 10:00:00"
END = "now()"

THRESHOLD_SEC = 180
SHUTDOWN_OFF_SEC = 3600
SHIFT_BINS = [0, 8, 16, 24]
SHIFT_LABELS = ["00-08", "08-16", "16-24"]


def fetch():
    extractor = LogExtractor(env_path=ENV_PATH)
    df = extractor.get_data(start_time=START, end_time=END, target_tags=[EXCH_TAG, COOL_TAG])
    os.makedirs(SAVE_DIR, exist_ok=True)
    extractor.save_to_csv(df, save_dir=SAVE_DIR)
    return df


def raw_segments(exch):
    s = exch[exch.diff() != 0]
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
    rows = []
    cycle_start = segs[0][0]
    n_bridged = 0
    for i in range(len(segs) - 1):
        gap = (segs[i + 1][0] - segs[i][1]).total_seconds()
        if gap < threshold:
            n_bridged += 1
            continue
        rows.append({"cycle_start": cycle_start, "cycle_end": segs[i][1], "next_on": segs[i + 1][0],
                      "off_duration_sec": gap, "n_bridged_short_offs": n_bridged})
        cycle_start = segs[i + 1][0]
        n_bridged = 0
    return pd.DataFrame(rows)


def flag_cool_overlap(cycles, cool):
    cool_sorted = cool.sort_index()
    overlaps = []
    for _, r in cycles.iterrows():
        seg = cool_sorted.loc[r["cycle_start"]:r["cycle_end"]]
        before = cool_sorted.loc[:r["cycle_start"]]
        start_val = before.iloc[-1] if len(before) else 0
        overlaps.append(bool((seg == 1).any() or start_val == 1))
    cycles = cycles.copy()
    cycles["cool_overlap"] = overlaps
    return cycles


def flag_edges(cycles):
    cycles = cycles.sort_values("cycle_start").reset_index(drop=True)
    day = cycles["cycle_start"].dt.date
    first_idx = cycles.groupby(day)["cycle_start"].idxmin()
    cycles["is_first_of_day"] = cycles.index.isin(first_idx)
    cycles["is_pre_shutdown"] = cycles["off_duration_sec"] >= SHUTDOWN_OFF_SEC
    cycles["edge_case"] = cycles["is_first_of_day"] | cycles["is_pre_shutdown"]
    return cycles


def summarize(df, label):
    print(f"\n{'='*20} {label} ({len(df)}개) {'='*20}")

    day = df["cycle_start"].dt.date
    shift = pd.cut(df["cycle_start"].dt.hour, bins=SHIFT_BINS, labels=SHIFT_LABELS,
                   right=False, include_lowest=True)
    df = df.assign(day=day, shift=shift)

    shift_total = df.groupby("shift", observed=True).agg(exchanger_횟수=("cool_overlap", "size"),
                                                           cool_겹침_횟수=("cool_overlap", "sum"))
    shift_total["겹침비율(%)"] = (shift_total["cool_겹침_횟수"] / shift_total["exchanger_횟수"] * 100).round(1)
    shift_total["exchanger_비중(%)"] = (shift_total["exchanger_횟수"] / shift_total["exchanger_횟수"].sum() * 100).round(1)
    print("-- 교대별 합계(전체 기간 통합) --")
    print(shift_total.to_string())

    day_total = df.groupby("day").agg(exchanger_횟수=("cool_overlap", "size"),
                                       cool_겹침_횟수=("cool_overlap", "sum"))
    day_total["겹침비율(%)"] = (day_total["cool_겹침_횟수"] / day_total["exchanger_횟수"] * 100).round(1)
    print("\n-- 날짜별 합계 --")
    print(day_total.to_string())

    g = df.groupby(["day", "shift"], observed=True)
    total = g.size().rename("exchanger_횟수")
    cool = g["cool_overlap"].sum().rename("cool_겹침_횟수")
    table = pd.concat([total, cool], axis=1).fillna(0).astype(int)
    table["겹침비율(%)"] = (table["cool_겹침_횟수"] / table["exchanger_횟수"].replace(0, pd.NA) * 100).round(1).fillna(0.0)
    return table, shift_total, day_total


def main():
    raw = fetch()
    exch = raw[EXCH_TAG].dropna()
    cool = raw[COOL_TAG].dropna()

    segs = raw_segments(exch)
    print(f"raw ON 구간 {len(segs)}개")
    cycles = merge_cycles(segs, THRESHOLD_SEC)
    cycles = flag_cool_overlap(cycles, cool)
    cycles = flag_edges(cycles)
    print(f"병합 사이클 {len(cycles)}개 "
          f"(첫가동 {cycles['is_first_of_day'].sum()}개, 셧다운직전 {cycles['is_pre_shutdown'].sum()}개)")

    table_raw, shift_raw, day_raw = summarize(cycles, "RAW (제외 없음)")
    table_excl, shift_excl, day_excl = summarize(cycles[~cycles["edge_case"]], "EXCL (첫가동+셧다운직전 제외)")

    out_cycles = os.path.join(OUT_DIR, "cycles_long_range_09-09_to_now.csv")
    cycles.to_csv(out_cycles, index=False, encoding="utf-8-sig")
    table_raw.to_csv(os.path.join(OUT_DIR, "cool_overlap_by_shift_raw_longrange.csv"), encoding="utf-8-sig")
    table_excl.to_csv(os.path.join(OUT_DIR, "cool_overlap_by_shift_excl_longrange.csv"), encoding="utf-8-sig")
    print(f"\n저장: {out_cycles}")
    print("저장: cool_overlap_by_shift_raw_longrange.csv / cool_overlap_by_shift_excl_longrange.csv")


if __name__ == "__main__":
    main()
