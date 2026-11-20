"""컨베이어 실시간 모델: 틱 처리 시간 vs evaluate() 시간 (v2.2, DB 재생)."""
import sys, time, pickle
from contextlib import redirect_stdout
from io import StringIO
import numpy as np, pandas as pd

ROOT = "/Users/jeonjae/Desktop/KTA/DB_Log/model_review_1103"
sys.path.insert(0, ROOT)
from review_lib import backend, db
backend.use("v2.2")
with redirect_stdout(StringIO()):
    from tools_yeji.conveyor_models.conveyor_realtime import ConveyorRealtimeGroup

DATA = sys.argv[1]
day, h0, h1 = sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
raw = db.fetch_day(day, db.model_tags("conveyor", "v2.2"), "conveyor")
raw = raw[(raw.index >= pd.Timestamp(day) + pd.Timedelta(hours=h0)) & (raw.index < pd.Timestamp(day) + pd.Timedelta(hours=h1))]
print("rows", len(raw), "median dt", raw.index.to_series().diff().median())

g = ConveyorRealtimeGroup(data_dir=DATA)
ev = []          # (t, core_sec, units_sec, total_sec, n_cur)
orig_core_eval = g.core.evaluate
def timed_eval(t):
    a = time.perf_counter(); g.core.evaluate(t); b = time.perf_counter()
    for m in g.units.values(): m.evaluate()
    c = time.perf_counter()
    g.result = g._assemble(t)
    d = time.perf_counter()
    ev.append((t, b - a, c - b, d - a, len(g.core.current_table())))
    return g.result
g.evaluate = timed_eval

cols = [c for c in raw.columns if c != "source"]
vals = raw[cols].to_numpy(dtype=object)
tick = []
for t, v in zip(raw.index, vals):
    row = {c: x for c, x in zip(cols, v) if x is not None and x == x}
    n0 = len(ev)
    a = time.perf_counter(); g.update(t, row); b = time.perf_counter()
    if len(ev) == n0:
        tick.append(b - a)
E = pd.DataFrame(ev, columns=["t", "core", "units", "total", "n_cells"])
tick = np.array(tick)
print(f"tick (판정 없는 틱) n={len(tick)}  median {np.median(tick)*1e6:.0f} us  p99 {np.percentile(tick,99)*1e6:.0f} us  max {tick.max()*1e3:.1f} ms")
print(f"evaluate n={len(E)}")
full = E[E.n_cells > 0]
print("판정 칸이 있는 evaluate:", len(full))
print(full[["core", "units", "total"]].describe(percentiles=[.5, .9, .99]).round(3).to_string())
E.to_pickle(DATA + "/../eval_%s.pkl" % day)
