"""DB 그대로 vs 엣지처럼 10% 덜 읽기 — 켜짐 길이(이물 앞·뒤·기타) 대차 경고가 생기나."""
import sys, json
from contextlib import redirect_stdout
from io import StringIO
import numpy as np, pandas as pd
ROOT = "/Users/jeonjae/Desktop/KTA/DB_Log/model_review_1103"
sys.path.insert(0, ROOT)
from review_lib import backend, db
backend.use("v2.2")
with redirect_stdout(StringIO()):
    from tools_yeji.conveyor_models.core_realtime import CoreRealtime
hist = json.load(open(ROOT + "/v2_2_1/backend/data/conveyor_core_history.json"))
tags = db.model_tags("conveyor", "v2.2")
day = sys.argv[1]
d0 = pd.Timestamp(day) + pd.Timedelta(hours=6)
raw = pd.concat([db.fetch_day(day, tags, "conveyor"), db.fetch_day(pd.Timestamp(day) + pd.Timedelta(days=1), tags, "conveyor")])
raw = raw[(raw.index >= d0) & (raw.index < d0 + pd.Timedelta(days=1))]
cols = [c for c in raw.columns if c != "source"]
rows = [(t, {c: x for c, x in zip(cols, v) if x is not None and x == x}) for t, v in zip(raw.index, raw[cols].to_numpy(dtype=object))]
out = []
for name, drop, seed in [("DB", 0, 0), ("throttle", -1, 0)]:
    rng = np.random.default_rng(seed)
    c = CoreRealtime(history_path=None)
    c.history = [{**r, "day": pd.Timestamp(r["day"])} for r in hist if r["day"] < day]
    last = None
    kept = None
    for t, row in rows:
        if drop == -1:
            if kept is not None and (t - kept).total_seconds() < 0.5:
                continue
            kept = t
        elif drop and rng.random() < drop:
            continue
        c.update(t, row)
        if last is None or (t - last).total_seconds() >= 300:
            last = t
            res = c.evaluate(t)
            if not res.get("ready"):
                continue
            for wk, w in res["wagons"].items():
                for it in w["items"]:
                    if it["sev"] >= 2:
                        out.append(dict(day=day, run=f"{name}#{seed}", t=t, sensor=it["sensor"], wagon=it["wagon"], state=it["state"]))
pd.DataFrame(out).to_pickle(f"/Users/jeonjae/Desktop/KTA/DB_Log/model_review_1103/con_check/edge_check/out/wfp_thr_{day}.pkl")
print(day, "done", len(out))
