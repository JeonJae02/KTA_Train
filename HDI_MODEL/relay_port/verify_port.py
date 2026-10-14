"""이식본(out/backend)이 원본(HDI_MODEL)과 같은 결과를 내는지 — 같은 틱을 둘 다에 흘려 tick 마다 비교한다.

이식본은 백엔드처럼 Pump_Monitor.check 로 부른다 — 진단 경보는 원본과 tick 마다 같아야 하고, 사람에게 알린 펌프는
그 tick 에 Pump_Score over=True (→ Pump_Anomaly) 여야 하고, Pump_Score 는 JSON·프론트 계약(isPumpAnomaly)을 지켜야 한다.

    python HDI_MODEL/relay_port/verify_port.py                         # 10/10 06:40~10:00, 14대
    python HDI_MODEL/relay_port/verify_port.py --start "2026-09-16 21:30" --end "2026-09-17 08:00"
    python HDI_MODEL/relay_port/verify_port.py --backend ../HDI_Relay/backend      # 이식이 끝난 백엔드 그대로

이식본은 **실제 백엔드 코드**(HDI_Relay/backend 의 config/, tools_yeji/common/)와 같이 돌린다 — 임시 폴더에
이식본 + 백엔드 config·time_utils 를 합쳐 놓고 거기서 import 한다. topology·근무표는 HDI_Relay/shared 것.
"""

import argparse
import importlib
import json
import os
import shutil
import sys
import tempfile
import time
from collections import deque

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
MODEL_DIR = os.path.dirname(HERE)
OUT = os.path.join(HERE, "out", "backend")
RELAY = os.path.abspath(os.path.join(MODEL_DIR, "..", "..", "HDI_Relay"))


def port_env(backend=None):
    """이식본을 import 할 수 있게 한다. backend 를 주면 그 백엔드 폴더(이식이 끝난 HDI_Relay/backend)를 그대로 쓴다."""
    os.environ["TOPOLOGY_PATH"] = os.path.join(RELAY, "shared", "topology.json")
    os.environ["OPERATION_PATH"] = os.path.join(RELAY, "shared", "operation.json")
    if backend:
        os.environ.pop("PUMP_CYCLE_DIR", None)          # 백엔드 기본 경로 models/pump_cycle 를 그대로
        sys.path.insert(0, os.path.abspath(backend))
        return None
    tmp = tempfile.mkdtemp(prefix="pump_port_")
    shutil.copytree(OUT, tmp, dirs_exist_ok=True)
    shutil.copytree(os.path.join(RELAY, "backend", "config"), os.path.join(tmp, "config"), dirs_exist_ok=True)
    shutil.copytree(os.path.join(RELAY, "backend", "tools_yeji", "common"), os.path.join(tmp, "tools_yeji", "common"),
                    dirs_exist_ok=True)
    shutil.copy2(os.path.join(RELAY, "backend", "tools_yeji", "Fault_Registry.py"), os.path.join(tmp, "tools_yeji"))
    os.environ["PUMP_CYCLE_DIR"] = os.path.join(tmp, "models", "pump_cycle")
    sys.path.insert(0, tmp)
    return tmp


def is_pump_anomaly(x):
    """프론트 계약 (frontend/src/types/socket.ts isPumpAnomaly)."""
    return all(isinstance(x.get(k), str) for k in ("time", "pump", "device", "type", "message"))


def main(start, end, backend=None):
    tmp = port_env(backend)
    PumpMonitor = importlib.import_module("tools_yeji.Pump_Monitor").PumpMonitor
    sys.path.insert(0, MODEL_DIR)
    import Pump_Realtime_Model as orig
    from extract_pump_cache import load_range

    a, mon = orig.PumpRealtimeGroup(keep_cycles=None), PumpMonitor()
    b = mon._rt
    b.cycles = deque(maxlen=None)
    got = {}
    rt_update = b.update

    def capture(t, row):                                 # PumpMonitor 안에서 부르는 update 의 반환을 같이 본다
        got["alerts"] = rt_update(t, row)
        return got["alerts"]
    b.update = capture

    print("이식본 빠진 태그 (캐시 기준):", b.missing_tags(set().union(*a.tags.values())) or "없음")
    frames = []
    for p in a.pumps:
        x = load_range(p, start[:10], end[:10]).select_dtypes("number")
        x = x[(x.index >= start) & (x.index < end)]
        cols = [c for c in x.columns if c in a.tags[p]]
        frames.append((x.index.to_numpy(), x[cols].to_numpy(float), cols))
    times = np.concatenate([ix for ix, _, _ in frames])
    who = np.concatenate([np.c_[np.full(len(ix), k), np.arange(len(ix))] for k, (ix, _, _) in enumerate(frames)])
    key = lambda al: [(x["relay"], x["reason"], x["show"], x["message"], x["pump"], x["pumps"]) for x in al]  # noqa: E731
    t0, n_alert_ticks, diff, bad = time.time(), 0, 0, []
    shown, board = {}, []                                # 지금 over 인 펌프 {pid: reason}, 켜짐·꺼짐 기록
    missed = []                                          # 알렸는데 그 tick 에 over 가 아닌 펌프
    for n, i in enumerate(np.argsort(times, kind="stable")):
        k, j = who[i]
        _, vals, cols = frames[k]
        row = {c: (None if np.isnan(v) else v) for c, v in zip(cols, vals[j])}
        ra = a.update(times[i], dict(row))
        scores = mon.check(dict(row), now=times[i])
        rb = got["alerts"]
        if key(ra) != key(rb):
            diff += 1
        n_alert_ticks += bool(ra)

        # Pump_Score → Pump_Anomaly (PredictiveEngine 과 같은 거르기)
        if set(scores) != set(orig.PUMPS):
            bad.append("Pump_Score 키가 14개 전량이 아님")
        anomalies = [s for s in scores.values() if s and s["over"]]
        now_on = {s["pump"]: s["reason"] for s in anomalies}
        t = pd.Timestamp(times[i])
        for pid in sorted(set(now_on) | set(shown)):
            if shown.get(pid) != now_on.get(pid):
                board.append((t, pid, shown.get(pid), now_on.get(pid),
                              scores[pid]["message"] if pid in now_on else ""))
        shown = now_on
        for x in ra:
            if x["show"]:
                missed += [(t, pid) for pid in x["pumps"] if pid not in now_on]
        if anomalies and (n % 200 == 0 or any(x["show"] for x in ra)):
            json.dumps(scores, ensure_ascii=False)                       # 소켓으로 나갈 수 있는가
            bad += [f"isPumpAnomaly 실패 {s.get('pump')}" for s in anomalies if not is_pump_anomaly(s)]

    ca, cb = list(a.cycles), list(b.cycles)
    same_cycles = len(ca) == len(cb) and all(
        x["t_start"] == y["t_start"] and x["mode"] == y["mode"] and x["level"] == y["level"]
        and (np.isclose(x["score"], y["score"], equal_nan=True)) and x["pump_level"] == y["pump_level"]
        for x, y in zip(ca, cb))
    print(f"행 {len(times):,} ({time.time() - t0:.0f}s) | 사이클 원본 {len(ca):,} / 이식 {len(cb):,} — "
          f"사이클 판정 {'모두 같음' if same_cycles else '다름!'} | 경보 있는 tick {n_alert_ticks:,}, 경보가 다른 tick {diff}")
    print("사람에게 간 경보 — 원본", len(a.notified), "/ 이식", len(b.notified))
    for x in b.notified:
        print(f"  {x['time']:%m-%d %H:%M:%S} {x['label']:12s} {x['reason']:28s} → {','.join(x['pumps'])}")
    print(f"Pump_Anomaly 켜짐·꺼짐 {len(board)}건 (펌프 마커, 근무 끝까지 유지)")
    for t, pid, was, now_, msg in board:
        print(f"  {t:%m-%d %H:%M:%S} {pid:3s} {was or '-':28s} → {now_ or '꺼짐':28s} {msg}")
    print("알렸는데 over 아닌 펌프:", missed or "없음", "| 형식 문제:", sorted(set(bad)) or "없음")
    if tmp:
        shutil.rmtree(tmp, ignore_errors=True)
    return same_cycles and diff == 0 and not missed and not bad


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-10-10 06:40:00")
    ap.add_argument("--end", default="2026-10-10 10:00:00")
    ap.add_argument("--backend", help="이식이 끝난 백엔드 폴더를 그대로 확인 (예: ../HDI_Relay/backend)")
    a = ap.parse_args()
    ok = main(a.start, a.end, a.backend)
    print("결과:", "이식본 = 원본" if ok else "차이 있음")
    sys.exit(0 if ok else 1)
