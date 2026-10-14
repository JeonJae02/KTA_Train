"""수집 간격 가드 확인 — 백엔드가 공장 알람 때 DIAGNOSTIC(2초 틱)·ANALYZING(예지 호출 멈춤)으로 가는 상황을 재생에 끼워 본다.

    python HDI_MODEL/relay_port/verify_gap.py                       # 10/10 06:40~10:00, 아래 SCENARIOS

각 장면은 평소 재생(기준)과 같은 원시 데이터에서 구간만 바꾼다:
  thin  : 펌프마다 2초에 한 틱만 남긴다 (DIAGNOSTIC 2초 틱)
  drop  : 그 구간 틱을 모두 지운다 (ANALYZING — 예지 호출 없음)
보는 것: 그 구간 사이클이 판정에서 빠지는가(수집 간격·버림), 거짓 알림이 새로 생기지 않는가, 언제 다시 판정하는가.
"""

import argparse
import os
import sys
import time

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
MODEL_DIR = os.path.dirname(HERE)
sys.path.insert(0, MODEL_DIR)

import Pump_Realtime_Model as rtm  # noqa: E402
from extract_pump_cache import load_range  # noqa: E402

#: (이름, [(시작, 끝, 동작)]) — 동작 thin / drop
SCENARIOS = [
    ("공장 알람 1회 — 진단 모드 6초 + 진단 4분",
     [("07:20:00", "07:20:06", "thin"), ("07:20:06", "07:24:00", "drop")]),
    ("현장이 진단 모드로 10분 머묾 (2초 틱)",
     [("08:00:00", "08:10:00", "thin")]),
    ("진단이 40분 걸림 (창 30분 리셋까지)",
     [("09:00:00", "09:00:06", "thin"), ("09:00:06", "09:40:00", "drop")]),
]


def load(start, end, pumps):
    frames = {}
    tags = rtm.PumpRealtimeGroup(pumps, keep_cycles=1).tags
    for p in pumps:
        x = load_range(p, start[:10], end[:10]).select_dtypes("number")
        x = x[(x.index >= start) & (x.index < end)]
        frames[p] = x[[c for c in x.columns if c in tags[p]]]
    return frames


def transform(frames, day, edits):
    out = {}
    for p, x in frames.items():
        keep = np.ones(len(x), bool)
        for a, b, how in edits:
            m = (x.index >= f"{day} {a}") & (x.index < f"{day} {b}")
            if how == "drop":
                keep &= ~m
            else:                                   # thin: 2초 칸마다 첫 틱만
                slot = (x.index.asi8 // 2_000_000_000)
                first = np.r_[True, slot[1:] != slot[:-1]]
                keep &= ~m | first
        out[p] = x[keep]
    return out


def replay(frames):
    mon = rtm.PumpRealtimeGroup(list(frames), keep_cycles=None)
    times = np.concatenate([x.index.to_numpy() for x in frames.values()])
    rows = [(x.index.to_numpy(), x.to_numpy(float), list(x.columns)) for x in frames.values()]
    who = np.concatenate([np.c_[np.full(len(ix), k), np.arange(len(ix))] for k, (ix, _, _) in enumerate(rows)])
    for i in np.argsort(times, kind="stable"):
        k, j = who[i]
        _, vals, cols = rows[k]
        mon.update(times[i], {c: (None if np.isnan(v) else v) for c, v in zip(cols, vals[j])})
    cyc = pd.DataFrame(list(mon.cycles))
    noti = pd.DataFrame(list(mon.notified))
    return cyc, noti


def show_noti(noti):
    if noti.empty:
        return "없음"
    return ", ".join(f"{t:%H:%M:%S} {','.join(p)} {r.replace('pump_', '')}"
                     for t, p, r in zip(noti.time, noti.pumps, noti.reason))


def main(start, end):
    day = start[:10]
    pumps = rtm.PUMPS
    t0 = time.time()
    frames = load(start, end, pumps)
    base_c, base_n = replay(frames)
    auto = base_c[base_c["mode"] == "자동 운전"]
    print(f"기준 (평소 재생) — 사이클 {len(base_c):,}, 모드 {base_c['mode'].value_counts().to_dict()} ({time.time() - t0:.0f}s)")
    print(f"  알림 {len(base_n)}: {show_noti(base_n)}")
    ok = True
    for name, edits in SCENARIOS:
        c, n = replay(transform(frames, day, edits))
        a, b = f"{day} {edits[0][0]}", f"{day} {edits[-1][1]}"
        inside = c[(c.t_start >= a) & (c.t_start < b)]
        judged_inside = inside[inside["mode"] == "자동 운전"]
        base_inside = auto[(auto.t_start >= a) & (auto.t_start < b)]
        # 다시 판정: 구간 끝 뒤 펌프별 첫 자동운전 사이클까지
        after = c[(c.t_start >= b) & (c["mode"] == "자동 운전")].groupby("pump").t_start.min()
        delay = (after - pd.Timestamp(b)).dt.total_seconds()
        new = set(map(lambda r: (r[0], tuple(r[1])), zip(n.reason, n.pumps))) if len(n) else set()
        old = set(map(lambda r: (r[0], tuple(r[1])), zip(base_n.reason, base_n.pumps))) if len(base_n) else set()
        extra = new - old
        print(f"\n[{name}] {edits}")
        print(f"  구간 안 시작 사이클 — 기준 자동운전 {len(base_inside)} → 이번 {inside['mode'].value_counts().to_dict()}"
              f" | 그중 판정에 들어간 것 {len(judged_inside)}")
        print(f"  구간 뒤 첫 판정까지 (펌프별 초) — 중앙 {delay.median():.0f}, 최대 {delay.max():.0f} ({delay.idxmax()})")
        print(f"  알림 {len(n)}: {show_noti(n)}")
        print(f"  기준에 없던 알림: {sorted(extra) or '없음'}")
        ok &= len(judged_inside) == 0 and not extra
    return ok


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-10-10 06:40:00")
    ap.add_argument("--end", default="2026-10-10 10:00:00")
    a = ap.parse_args()
    ok = main(a.start, a.end)
    print("\n결과:", "가드 정상 — 구간 사이클 판정 제외, 거짓 알림 없음" if ok else "확인 필요")
    sys.exit(0 if ok else 1)
