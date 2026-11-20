"""유닛 판정 단위 — 하루 vs 교대. 사이클마다 지표를 시각과 함께 뽑는다 (HDI_MODEL 수정본, MAX_GAP 0.75초, DB 전부).

출력: out/unit_cycles.pkl (unit, t, 지표들)
"""
import importlib.util, sys
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
import numpy as np, pandas as pd

ROOT = Path("/Users/jeonjae/Desktop/KTA/DB_Log/model_review_1103")
PKG = Path("/Users/jeonjae/Desktop/KTA/DB_Log/HDI_MODEL/conveyor_models")
sys.path.insert(0, str(ROOT))
from review_lib import backend, db
backend.use("v2.2")
with redirect_stdout(StringIO()):
    import tools_yeji
    spec = importlib.util.spec_from_file_location("tools_yeji.conveyor_models", PKG / "__init__.py",
                                                  submodule_search_locations=[str(PKG)])
    mod = importlib.util.module_from_spec(spec); sys.modules["tools_yeji.conveyor_models"] = mod; spec.loader.exec_module(mod)
    import tools_yeji.conveyor_models.unit_realtime as ur
    from tools_yeji.conveyor_models.conveyor_realtime import ConveyorRealtimeGroup

REC = []


class U(ur.UnitRealtime):
    def _close_cycle(self, ts_next):
        c = self.cyc
        before = dict(self.sum), dict(self.cnt)
        super()._close_cycle(ts_next)
        if c is None:
            return
        row = dict(unit=self.unit, t=self.t0 + pd.Timedelta(seconds=c["start"]))
        for col in self.cols:
            if self.cnt[col] > before[1].get(col, 0):
                row[col] = self.sum[col] - before[0][col]
        if len(row) > 2:
            REC.append(row)


ur.UnitRealtime = U
import tools_yeji.conveyor_models.conveyor_realtime as crt
crt.UnitRealtime = U
g = ConveyorRealtimeGroup(data_dir=None)
g.evaluate = lambda t: None
tags = db.model_tags("conveyor", "v2.2")
cache = db.paths.CACHE_DIR / "raw" / "conveyor"
for day in pd.date_range(sys.argv[1], sys.argv[2]):
    if not (cache / f"{day:%Y-%m-%d}.pkl.xz").exists():
        continue
    raw = db.fetch_day(day, tags, "conveyor")
    cols = [c for c in raw.columns if c != "source"]
    for t, v in zip(raw.index, raw[cols].to_numpy(dtype=object)):
        g.update(t, {k: x for k, x in zip(cols, v) if x is not None and x == x})
    print(day.date(), len(REC), flush=True)
pd.DataFrame(REC).to_pickle(ROOT / f"con_check/stepwise/out/unit_cycles_{sys.argv[1]}.pkl")
