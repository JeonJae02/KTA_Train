"""이식 묶음(out/backend) 코드로 과거 데이터를 tick 단위로 재생해, 배치(conv_02) 판정과 같은지 확인한다.

    python Conv/relay_port/verify_port.py [시작일] [끝일]      # 기본 2026-10-13 – 2026-10-17

1) 하루 마감 칸(dist · width · rearm · jitter) 이 배치 일별 표와 같은지
2) 그 칸으로 매긴 상태(classify_cell) 가 배치와 같은지
3) ConveyorMonitor.read() 가 프론트 계약 모양으로 나오는지
"""
import os
import sys
import time

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
CONV = os.path.dirname(HERE)
OUT = os.path.join(HERE, "out", "backend")
sys.path.insert(0, OUT)                    # 묶음 코드를 먼저
sys.path.insert(1, CONV)                   # 배치 코드·캐시 읽기

from tools_yeji.Conveyor_Health import ConveyorMonitor          # noqa: E402
from tools_yeji.conveyor_models.conv_realtime import ConveyorRealtime   # noqa: E402
from tools_yeji.conveyor_models.conv_rules import add_metrics, classify_cell   # noqa: E402
from conv_grid import day_events, daily_table                   # noqa: E402
from conv_replay import day_rows                                # noqa: E402


def states(T):
    t = add_metrics(T)
    t["state"] = t.apply(classify_cell, axis=1)
    return t


def main(start="2026-10-13", end="2026-10-17"):
    days = list(pd.date_range(start, end).strftime("%Y-%m-%d"))
    warm = (pd.Timestamp(start) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    t0 = time.time()
    rt = ConveyorRealtime(day_start_hour=0, eval_sec=10 ** 9)
    n_tick = 0
    for d in [warm] + days:
        df = day_rows(d)
        cols = list(df.columns)
        for t, v in zip(df.index, df.to_numpy()):
            rt.update(t, {c: x for c, x in zip(cols, v) if x == x})
            n_tick += 1
    rt.close_day()
    sec = time.time() - t0
    R = pd.DataFrame(rt.history)
    R = R[R.day.isin(pd.to_datetime(days))]
    B = pd.concat([daily_table(day_events(d)) for d in days])
    B = B[B.n >= 20]
    x = states(R).merge(states(B), on=["day", "sensor", "wagon"], suffixes=("_rt", "_b"))
    print(f"재생 {len(days) + 1}일 · tick {n_tick:,} · {sec:.0f}초 ({sec / n_tick * 1e6:.0f} µs/tick)")
    print(f"칸 {len(x)} — 상태 일치 {(x.state_rt == x.state_b).mean() * 100:.2f}%")
    for c in ["dist", "width", "rearm", "jitter"]:
        dd = (x[c + "_rt"] - x[c + "_b"]).abs()
        print(f"  {c:6s} 같음 {(dd < 1e-6).mean() * 100:5.1f}%  최대 차이 {dd.max():.2f}")

    # 3) 백엔드 래퍼 모양
    mon = ConveyorMonitor(history_path=None)
    df = day_rows(days[-1]).iloc[:30000]
    cols = list(df.columns)
    out = None
    for t, v in zip(df.index, df.to_numpy()):
        out = mon.read({c: x for c, x in zip(cols, v) if x == x}, now=t.tz_localize("Asia/Seoul").timestamp())
    keys = sorted(out)
    print(f"ConveyorMonitor.read() 키: {keys} · ready={out['ready']} · 스테이션 {list(out['stations'])}")


if __name__ == "__main__":
    main(*sys.argv[1:3])
