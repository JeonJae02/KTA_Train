"""캐시된 과거 데이터를 tick 단위로 실시간 모델에 흘려 넣는다 — 배치와 같은지 확인용.

    python Conv/conv_replay.py 2026-10-13 2026-10-17
"""
import sys
import time

import pandas as pd

from conv_grid import load_merged
from conveyor_monitor import ConveyorMonitor, all_required_tags

GROUPS = ["main", "closing", "opening", "md_fwd", "md_rev", "airbag"]


def day_rows(day, tags=None):
    """그룹 파일을 시각으로 합친 하루치 표 (실시간 모델이 읽는 태그만)."""
    df = load_merged(day, GROUPS)
    if df.empty:
        return df
    tags = tags or all_required_tags()
    return df[[c for c in tags if c in df]]


def replay(days, mon=None, **kw):
    mon = mon or ConveyorMonitor(**kw)
    for day in days:
        df = day_rows(day)
        if df.empty:
            continue
        cols = list(df.columns)
        for t, v in zip(df.index, df.to_numpy()):
            mon.update(t, {c: x for c, x in zip(cols, v) if x == x})
    return mon


if __name__ == "__main__":
    days = pd.date_range(sys.argv[1], sys.argv[2]).strftime("%Y-%m-%d")
    t0 = time.time()
    m = replay(days, day_start_hour=0, eval_sec=10 ** 9)
    print(f"{len(days)}일 {time.time() - t0:.0f}초")
