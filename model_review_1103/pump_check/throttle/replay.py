"""펌프 모델 — DB 그대로 vs 엣지 스로틀 입력 재생.

엣지는 메시지(약 0.515초)마다 전체 태그 스냅샷을 PredictiveEngine.maybe_publish 에 넘기고,
직전 처리 뒤 0.5초가 안 지난 메시지는 모델에 넘기지 않는다(약 9%). 펌프도 같은 길이다
(maybe_publish → PredictiveHealth.generate_idle_json → PumpMonitor.check → PumpRealtimeGroup.update).

캐시 cache/raw/pump/<day>.pkl.xz 는 메시지마다 한 줄, 모든 태그가 채워진 스냅샷이다(ffill).
  db        모든 줄을 넣는다 (학습·기준선과 같은 입력)
  throttle  직전에 넣은 줄에서 0.5초 안 된 줄은 버린다 (엣지와 같은 입력)

나오는 것 (out/<mode>_<day>.pkl)
  cycles  사이클마다 피처 전부 + 판정(mode·level·score·zmax·zn·pump_level·notify)
  alerts  show=True 인 진단 경보 (사람에게 간 것)

달력 날짜 단위로 새 모델을 만든다(00 ~ 06시는 정지 — 창은 30분 공백이면 어차피 비워진다).
펌프 코드는 v2.1 · v2.2 가 같다.

사용: venv/bin/python model_review_1103/pump_check/throttle/replay.py [day ...]   (없으면 캐시 전부, 병렬)
"""
import os
import pickle
import sys
import time as _time
from contextlib import redirect_stdout
from io import StringIO
from multiprocessing import Pool

import numpy as np
import pandas as pd

ROOT = "/Users/jeonjae/Desktop/KTA/DB_Log/model_review_1103"
OUT = ROOT + "/pump_check/throttle/out"
CACHE = ROOT + "/cache/raw/pump"
THROTTLE_SEC = 0.5
sys.path.insert(0, ROOT)


def _group():
    from review_lib import backend
    backend.use("v2.2")
    with redirect_stdout(StringIO()):
        from tools_yeji.pump_models.pump_realtime import PumpRealtimeGroup

    class Rec(PumpRealtimeGroup):
        """_on_cycle 에서 사이클 피처를 판정 기록에 같이 남긴다."""

        def _on_cycle(self, p, cy, t):
            super()._on_cycle(p, cy, t)
            rec = self.cycles[-1]
            for k, v in cy.items():
                rec.setdefault(k, v)

    return Rec(keep_cycles=None)


def run_day(args):
    day, mode = args
    f = f"{OUT}/{mode}_{day}.pkl"
    if os.path.exists(f):
        return f
    t0 = _time.time()
    g = _group()
    d = pd.read_pickle(f"{CACHE}/{day}.pkl.xz").drop(columns=["source"])
    cols = list(d.columns)
    ts = d.index
    utc = ts.tz_localize("Asia/Seoul").tz_convert("UTC").tz_localize(None)
    epoch = ((utc - pd.Timestamp("1970-01-01")) / pd.Timedelta(seconds=1)).to_numpy(float)   # 인덱스 단위(us·ns)와 무관
    vals = d.to_numpy(dtype=object)
    alerts, kept, n_in = [], None, 0
    for i in range(len(d)):
        if mode == "throttle":
            if kept is not None and epoch[i] - kept < THROTTLE_SEC:
                continue
            kept = epoch[i]
        n_in += 1
        row = dict(zip(cols, vals[i]))
        for a in g.update(epoch[i], row):
            if a["show"]:
                alerts.append({"time": ts[i], **a})
    out = {"day": day, "mode": mode, "n_rows": len(d), "n_in": n_in,
           "cycles": pd.DataFrame(list(g.cycles)), "alerts": pd.DataFrame(alerts)}
    pickle.dump(out, open(f, "wb"))
    print(day, mode, f"{n_in}/{len(d)}", len(out["cycles"]), "cycles", len(alerts), "alerts",
          f"{_time.time() - t0:.0f}s", flush=True)
    return f


if __name__ == "__main__":
    os.makedirs(OUT, exist_ok=True)
    days = sys.argv[1:] or sorted(x[:10] for x in os.listdir(CACHE) if x.endswith(".pkl.xz"))
    jobs = [(d, m) for d in days for m in ("db", "throttle")]
    if len(jobs) <= 2:
        for j in jobs:
            run_day(j)
    else:
        with Pool(min(10, len(jobs))) as pool:
            list(pool.imap_unordered(run_day, jobs))
