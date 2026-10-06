"""하루를 3교대(00-08 / 08-16 / 16-24)로 나눠서, 날짜별 x 교대별로

- exchanger 가 켜진(= 180초 임계값으로 채터링 합친 사이클) 전체 횟수
- 그중 cool 이 같이 겹쳐 있던 횟수
- 겹침 비율(%)

을 센다. exchanger 전체 횟수는 cool 겹침 여부와 무관하게 '모든' exchanger
사이클을 센다(겹친 것도 포함) — 뺀 게 아니라 전체를 보는 것.

두 버전:
- RAW: 전부 포함(246개)
- EXCL: 공장 첫 가동(그날 첫 신호) + 공장 셧다운 직전 사이클(7개) 제외(239개)

사용법:
    PYTHONIOENCODING=utf-8 python Exchanger_Cool_Overlap_By_Shift.py
"""

import os

import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
OUT_DIR = os.path.join(BASE_DIR, "parkkt", "analysis_out", "exchanger_off_on_gap")
CYCLES_CSV = os.path.join(OUT_DIR, "cycles_with_cool_overlap.csv")

SHUTDOWN_OFF_SEC = 3600
SHIFT_BINS = [0, 8, 16, 24]
SHIFT_LABELS = ["00-08", "08-16", "16-24"]


def load():
    df = pd.read_csv(CYCLES_CSV, parse_dates=["cycle_start", "cycle_end", "next_on"])
    df["is_first_of_day"] = df["is_post_shutdown"].astype(bool)
    df["is_pre_shutdown"] = df["off_duration_sec"] >= SHUTDOWN_OFF_SEC
    df["edge_case"] = df["is_first_of_day"] | df["is_pre_shutdown"]
    df["day"] = df["cycle_start"].dt.date
    df["shift"] = pd.cut(df["cycle_start"].dt.hour, bins=SHIFT_BINS, labels=SHIFT_LABELS,
                          right=False, include_lowest=True)
    return df


def summarize(df, label):
    print(f"\n{'='*20} {label} ({len(df)}개) {'='*20}")

    g = df.groupby(["day", "shift"], observed=True)
    total = g.size().rename("exchanger_횟수")
    cool = g["cool_overlap"].sum().rename("cool_겹침_횟수")
    table = pd.concat([total, cool], axis=1).fillna(0).astype(int)
    table["겹침비율(%)"] = (table["cool_겹침_횟수"] / table["exchanger_횟수"] * 100).round(1)
    table = table.reindex(pd.MultiIndex.from_product(
        [sorted(df["day"].unique()), SHIFT_LABELS], names=["day", "shift"]), fill_value=0)
    table.loc[table["exchanger_횟수"] == 0, "겹침비율(%)"] = 0.0

    print(table.to_string())

    print("\n-- 날짜별 합계 --")
    day_total = df.groupby("day").agg(exchanger_횟수=("cool_overlap", "size"),
                                       cool_겹침_횟수=("cool_overlap", "sum"))
    day_total["겹침비율(%)"] = (day_total["cool_겹침_횟수"] / day_total["exchanger_횟수"] * 100).round(1)
    print(day_total.to_string())

    print("\n-- 교대별 합계(전체 날짜 통합) --")
    shift_total = df.groupby("shift", observed=True).agg(exchanger_횟수=("cool_overlap", "size"),
                                                           cool_겹침_횟수=("cool_overlap", "sum"))
    shift_total["겹침비율(%)"] = (shift_total["cool_겹침_횟수"] / shift_total["exchanger_횟수"] * 100).round(1)
    shift_total["exchanger_비중(%)"] = (shift_total["exchanger_횟수"] / shift_total["exchanger_횟수"].sum() * 100).round(1)
    print(shift_total.to_string())

    return table


def main():
    df = load()

    table_raw = summarize(df, "RAW (제외 없음)")
    table_excl = summarize(df[~df["edge_case"]], "EXCL (첫가동+셧다운직전 제외)")

    table_raw.to_csv(os.path.join(OUT_DIR, "cool_overlap_by_shift_raw.csv"), encoding="utf-8-sig")
    table_excl.to_csv(os.path.join(OUT_DIR, "cool_overlap_by_shift_excl.csv"), encoding="utf-8-sig")
    print(f"\n저장: cool_overlap_by_shift_raw.csv / cool_overlap_by_shift_excl.csv ({OUT_DIR})")


if __name__ == "__main__":
    main()
