import sys, json
from contextlib import redirect_stdout
from io import StringIO
import numpy as np, pandas as pd
ROOT = "/Users/jeonjae/Desktop/KTA/DB_Log/model_review_1103"
sys.path.insert(0, ROOT)
from review_lib import backend, db
backend.use("v2.2")
with redirect_stdout(StringIO()):
    from tools_yeji.conveyor_models.conveyor_realtime import ConveyorRealtimeGroup
day = sys.argv[1]
tags = db.model_tags("conveyor", "v2.2")
d0 = pd.Timestamp(day) + pd.Timedelta(hours=6)
raw = pd.concat([db.fetch_day(day, tags, "conveyor"), db.fetch_day(pd.Timestamp(day) + pd.Timedelta(days=1), tags, "conveyor")])
raw = raw[(raw.index >= d0) & (raw.index < d0 + pd.Timedelta(days=1))]
cols = [c for c in raw.columns if c != "source"]
rows = [(t, {c: x for c, x in zip(cols, v) if x is not None and x == x}) for t, v in zip(raw.index, raw[cols].to_numpy(dtype=object))]
edge = {u: {r["day"]: r for r in json.load(open(f"{ROOT}/v2_2_1/backend/data/conveyor_unit_{u}_history.json"))} for u in ("md_fwd", "md_rev", "airbag")}
out = []
for name, drop, seed in [("DB", 0, 0), ("edge-like", 0.10, 0), ("edge-like", 0.10, 1)]:
    rng = np.random.default_rng(seed)
    g = ConveyorRealtimeGroup(data_dir=None)
    g.evaluate = lambda t: None
    for t, row in rows:
        if drop and rng.random() < drop:
            continue
        g.update(t, row)
    for u, m in g.units.items():
        r = m._day_row(); e = edge[u].get(day)
        if e is None: continue
        for k in ("air_time", "stop_rev_time", "nozzle_rev_time", "stop_fwd_time", "drag_cnt", "catch_cnt"):
            if k in r and k in e and r[k] is not None and e[k] is not None:
                out.append(dict(day=day, run=f"{name}#{seed}", unit=u, metric=k, replay=r[k], edge=e[k], diff=e[k] - r[k]))
pd.DataFrame(out).to_pickle(f"/Users/jeonjae/Desktop/KTA/DB_Log/model_review_1103/con_check/edge_check/out/unit_{day}.pkl")
print(day, "ok")
