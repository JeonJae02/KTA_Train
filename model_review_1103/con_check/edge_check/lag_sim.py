"""엣지 루프가 밀리거나 읽는 주기가 길 때 코어 하루 칸이 어떻게 바뀌나 — 엣지 가동일 기록과 비교."""
import sys, json
from contextlib import redirect_stdout
from io import StringIO
import numpy as np, pandas as pd

ROOT = "/Users/jeonjae/Desktop/KTA/DB_Log/model_review_1103"
sys.path.insert(0, ROOT)
from review_lib import backend, db
backend.use("v2.2")
with redirect_stdout(StringIO()):
    from tools_yeji.conveyor_models.core_realtime import CoreRealtime, DAY_MIN_PASSES

edge = pd.DataFrame(json.load(open(ROOT + "/v2_2_1/backend/data/conveyor_core_history.json")))
tags = db.model_tags("conveyor", "v2.2")

def run(rows, scenario, seed=0):
    rng = np.random.default_rng(seed)
    c = CoreRealtime(history_path=None)
    c.evaluate = lambda t: None
    kind, val = scenario
    last_eval, skip_until, keep_next = None, None, True
    for t, row in rows:
        if kind == "lag":
            if skip_until is not None and t < skip_until:
                continue
            if last_eval is None or (t - last_eval).total_seconds() >= 60:
                last_eval = t
                skip_until = t + pd.Timedelta(seconds=val)
        elif kind == "drop" and rng.random() < val:
            continue
        c.update(t, row)
    return pd.DataFrame([c._cell(c.day, s, w, r) for (s, w), r in c.today.items() if len(r) >= DAY_MIN_PASSES])

def compare(rep, day):
    e = edge[edge.day == day]
    m = rep.merge(e, on=["sensor", "wagon"], suffixes=("_r", "_e"))
    dw = (m.width_e - m.width_r)
    dd = (m.dist_e - m.dist_r)
    return dict(cells=len(m), width_same=(dw.abs() < 0.01).mean(), edge_longer=(dw > 0.01).mean(),
                edge_shorter=(dw < -0.01).mean(), dw_mean=dw.mean(), front_same=(dd.abs() < 0.01).mean(),
                cap_r=(m.width_n_r / m.n_r).mean(), cap_e=(m.width_n_e / m.n_e).mean(),
                n_ratio=(m.n_e / m.n_r).mean())

day = sys.argv[1]
d0 = pd.Timestamp(day) + pd.Timedelta(hours=6)
raw = pd.concat([db.fetch_day(day, tags, "conveyor"), db.fetch_day(pd.Timestamp(day) + pd.Timedelta(days=1), tags, "conveyor")])
raw = raw[(raw.index >= d0) & (raw.index < d0 + pd.Timedelta(days=1))]
cols = [c for c in raw.columns if c != "source"]
rows = [(t, {c: x for c, x in zip(cols, v) if x is not None and x == x}) for t, v in zip(raw.index, raw[cols].to_numpy(dtype=object))]
out = []
for sc in [("base", 0), ("lag", 0.34), ("lag", 1.0), ("lag", 3.0), ("drop", 0.06), ("drop", 0.25), ("drop", 0.5)]:
    out.append(dict(day=day, scenario=f"{sc[0]} {sc[1]}", **compare(run(rows, sc), day)))
    print(out[-1], flush=True)
pd.DataFrame(out).to_csv(f"/Users/jeonjae/Desktop/KTA/DB_Log/model_review_1103/con_check/edge_check/out/lag_{day}.csv", index=False)
