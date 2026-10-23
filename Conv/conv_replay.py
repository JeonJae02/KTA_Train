"""캐시된 과거 데이터를 tick 단위로 conv_realtime 에 흘려 넣는다 — 배치(conv_02)와 같은지 확인용.

    python Conv/conv_replay.py 2026-10-13 2026-10-17
"""
import sys
import time

import numpy as np
import pandas as pd

from conv_core import load_group
from conv_realtime import ConveyorRealtime, REQUIRED_TAGS

GROUP_COLS = {"main": ["R00080", "P00140", "P00141", "Hz_In_Conv"], "closing": ["P00154"],
              "opening": ["R00070", "P00164"], "md_fwd": ["R00055", "P00451"], "md_rev": ["R00060", "P00459"]}
COMMON = ["R00030", "R00040", "Now_Actual_Wagon_Num", "Conv_Run_Out"]


def day_rows(day):
    """그룹 파일을 시각으로 합친 하루치 표 (REQUIRED_TAGS 컬럼)."""
    parts = []
    for g, cols in GROUP_COLS.items():
        d = load_group(g, day)
        if d.empty:
            return pd.DataFrame()
        keep = cols + (COMMON if g == "main" else [])
        parts.append(d[[c for c in keep if c in d]])
    df = pd.concat(parts, axis=1).sort_index()
    df = df[~df.index.duplicated()].ffill()
    return df[[c for c in REQUIRED_TAGS if c in df]]


def replay(days, mon=None, **kw):
    mon = mon or ConveyorRealtime(**kw)
    for day in days:
        df = day_rows(day)
        if df.empty:
            continue
        cols = list(df.columns)
        vals = df.to_numpy()
        for t, v in zip(df.index, vals):
            mon.update(t, {c: x for c, x in zip(cols, v) if x == x})
    return mon


if __name__ == "__main__":
    days = pd.date_range(sys.argv[1], sys.argv[2]).strftime("%Y-%m-%d")
    t0 = time.time()
    m = replay(days, day_start_hour=0, eval_sec=10 ** 9)
    print(f"{len(days)}일 {time.time() - t0:.0f}초")
