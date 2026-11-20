"""유닛 지표 — 엣지 입력(0.5초 스로틀) vs DB 전부, 하루 평균 차이를 σ 로. MAX_GAP 을 바꿔 가며.

사용: venv/bin/python unit_bias.py <throttle|db> <MAX_GAP>  → out/ubias_<mode>_<gap>.pkl (가동일 × 유닛 하루 평균)
"""
import importlib.util, sys
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
import pandas as pd

ROOT = Path("/Users/jeonjae/Desktop/KTA/DB_Log/model_review_1103")
PKG = Path("/Users/jeonjae/Desktop/KTA/DB_Log/HDI_MODEL/conveyor_models")
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT / "con_check" / "stepwise"))
from review_lib import backend, db
backend.use("v2.2")
with redirect_stdout(StringIO()):
    import tools_yeji
    spec = importlib.util.spec_from_file_location("tools_yeji.conveyor_models", PKG / "__init__.py", submodule_search_locations=[str(PKG)])
    mod = importlib.util.module_from_spec(spec); sys.modules["tools_yeji.conveyor_models"] = mod; spec.loader.exec_module(mod)
    import tools_yeji.conveyor_models.unit_realtime as ur
    from tools_yeji.conveyor_models.conveyor_realtime import ConveyorRealtimeGroup
import harness as H

mode, gap = sys.argv[1], float(sys.argv[2])
ur.MAX_GAP = gap
g = ConveyorRealtimeGroup(data_dir=None)
g.evaluate = lambda t: None
tags = db.model_tags("conveyor", "v2.2")
kept = None
for day in pd.date_range("2026-10-26", "2026-11-14"):
    for t, row in H.rows_of(day, tags):
        if mode == "throttle":
            if kept is not None and (t - kept).total_seconds() < 0.5:
                continue
            kept = t
        g.update(t, row)
for m in g.units.values():
    m.close_day()
pd.DataFrame([r for m in g.units.values() for r in m.history]).to_pickle(ROOT / f"con_check/stepwise/out/ubias_{mode}_{gap}.pkl")
print(mode, gap, "done")
