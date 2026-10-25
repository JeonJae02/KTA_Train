"""이식 묶음(out/backend) 코드로 과거 데이터를 tick 단위로 재생해, 배치와 같은지 확인한다.

    python Conv/relay_port/verify_port.py [시작일] [끝일]      # 기본 2026-10-13 – 2026-10-17

1) 코어: 하루 마감 칸 → 칸 상태가 배치(conv_02) 와 같은지
2) 유닛: 하루 평균 지표가 배치(conv_03 cyl_daily) 와 같은지
3) Conveyor_Health.ConveyorMonitor.read() 가 프론트 계약 모양으로 나오는지
"""
import os
import sys
import time

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
CONV = os.path.dirname(HERE)
ROOT = os.path.dirname(CONV)
OUT = os.path.join(HERE, "out", "backend")
sys.path.insert(0, OUT)                    # 묶음 코드를 먼저
sys.path.insert(1, CONV)                   # 배치 코드·캐시 읽기

from tools_yeji.Conveyor_Health import ConveyorMonitor as BackendMonitor     # noqa: E402
from tools_yeji.conveyor_models.conveyor_monitor import ConveyorMonitor     # noqa: E402
from tools_yeji.conveyor_models.core_rules import add_metrics, classify_cell  # noqa: E402
from conv_grid import day_events, daily_table                               # noqa: E402
from conv_cyl import cyl_daily                                              # noqa: E402
from conv_replay import day_rows                                            # noqa: E402


def states(T):
    t = add_metrics(T)
    t["state"] = t.apply(classify_cell, axis=1)
    return t


def main(start="2026-10-13", end="2026-10-17"):
    days = list(pd.date_range(start, end).strftime("%Y-%m-%d"))
    warm = (pd.Timestamp(start) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    t0, n_tick = time.time(), 0
    mon = ConveyorMonitor(data_dir=None, day_start_hour=0, eval_sec=10 ** 9)
    for d in [warm] + days:
        df = day_rows(d)
        cols = list(df.columns)
        for t, v in zip(df.index, df.to_numpy()):
            mon.update(t, {c: x for c, x in zip(cols, v) if x == x})
            n_tick += 1
    mon.core.close_day()
    for m in mon.units.values():
        m.close_day()
    sec = time.time() - t0
    print(f"재생 {len(days) + 1}일 · tick {n_tick:,} · {sec:.0f}초 ({sec / n_tick * 1e6:.0f} µs/tick, 코어+유닛 3)")

    R = pd.DataFrame(mon.core.history)
    R = R[R.day.isin(pd.to_datetime(days))]
    B = pd.concat([daily_table(day_events(d)) for d in days])
    x = states(R).merge(states(B[B.n >= 20]), on=["day", "sensor", "wagon"], suffixes=("_rt", "_b"))
    print(f"[코어] 칸 {len(x)} — 상태 일치 {(x.state_rt == x.state_b).mean() * 100:.2f}%")

    Db = cyl_daily(pd.read_pickle(os.path.join(ROOT, "cache", "conv_feat", "cyl_cycles.pkl")))
    for u, m in mon.units.items():
        H = pd.DataFrame(m.history)
        y = H[H.day.isin(pd.to_datetime(days))].merge(Db[Db.unit == u], on="day", suffixes=("_rt", "_b"))
        worst = max((y[c + "_rt"] - y[c + "_b"]).abs().max() for c in m.cols if c + "_rt" in y)
        dc = (y.cycles_rt - y.cycles_b).abs().max()
        print(f"[유닛 {u}] {len(y)}일 — 지표 하루 평균 최대 차이 {worst:.4f} (초·카운트), 사이클 수 차이 최대 {dc}")

    bm = BackendMonitor(data_dir=None)
    df = day_rows(days[-1]).iloc[:30000]
    cols = list(df.columns)
    out = None
    for t, v in zip(df.index, df.to_numpy()):
        out = bm.read({c: x for c, x in zip(cols, v) if x == x}, now=t.tz_localize("Asia/Seoul").timestamp())
    print(f"[백엔드 래퍼] 키 {sorted(out)} · 마커 {[(k, m['kind'], m['unit']) for k, m in out['markers'].items()]} · "
          f"대차 {len(out['wagons'])}대")


if __name__ == "__main__":
    main(*sys.argv[1:3])
