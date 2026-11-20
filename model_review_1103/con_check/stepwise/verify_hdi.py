"""HDI_MODEL/conveyor_models (수정본) 확인 — 엣지 모니터(ConveyorMonitor)가 그 패키지를 쓰게 끼워 넣고 20 가동일 재생.

harness.py S3 와 같은 조건: 0.5초 스로틀 입력, 코어 기록 = 엣지 기록 중 10-26 전, 유닛 기록 = HDI_MODEL/conveyor_models/data(엣지 방식 재생) 중 10-26 전.
판정 속도만 harness.FastCore 와 같은 방법(지난날 판정 캐시)으로 바꾼다 — evaluate 로직은 v2.2 와 같아 그대로 유효하다.
결과: out/ep_HDI_throttle.pkl → score.py HDI 로 점수.
"""
import importlib.util, sys, json, pickle
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
import pandas as pd

ROOT = Path("/Users/jeonjae/Desktop/KTA/DB_Log/model_review_1103")
PKG = Path("/Users/jeonjae/Desktop/KTA/DB_Log/HDI_MODEL/conveyor_models")
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "con_check" / "stepwise"))
from review_lib import backend, db
backend.use("v2.2")
with redirect_stdout(StringIO()):
    import tools_yeji
    spec = importlib.util.spec_from_file_location("tools_yeji.conveyor_models", PKG / "__init__.py",
                                                  submodule_search_locations=[str(PKG)])
    mod = importlib.util.module_from_spec(spec)
    sys.modules["tools_yeji.conveyor_models"] = mod
    spec.loader.exec_module(mod)
    import tools_yeji.conveyor_models.core_realtime as cr
    import tools_yeji.conveyor_models.unit_params as up
    import tools_yeji.Conveyor_Monitor as cmon
assert Path(cr.__file__).parent == PKG, cr.__file__
TAG = sys.argv[1] if len(sys.argv) > 1 else "HDI"
if len(sys.argv) > 2 and sys.argv[2] == "daily":          # 비교용: 슬라이딩 끄고 하루 마감만
    import tools_yeji.conveyor_models.unit_realtime as _ur
    _ur.ROLL_BLOCKS = 10 ** 9
import harness as H                       # _loop · _classify · rows_of 만 쓴다 (v2.2 core_judge 와 같은 로직)


class Fast(cr.CoreRealtime):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self._ck, self._JH, self._end = None, None, {}

    def evaluate(self, t):
        cur = self.current_table()
        if not cur:
            self.result = self.empty()
            return self.result
        today = pd.Timestamp(self.day)
        hist = [r for r in self.history if r["day"] < today]
        key = (today, len(hist), hist[-1]["day"] if hist else None, id(self.history))
        if key != self._ck:
            self._JH, self._end = H._loop(H._classify(pd.DataFrame(hist)), {}) if hist else (None, {})
            self._ck = key
        Jc, _ = H._loop(H._classify(pd.DataFrame(cur)), self._end)
        J = Jc if self._JH is None else pd.concat([self._JH, Jc], ignore_index=True)
        _, lv = cr.sensor_level(Jc, pd.DataFrame([{"sensor": s, **v} for s, v in cr.LONG_REF.items()]))
        self.result = self._build(J, Jc, lv)
        return self.result


mon = cmon.ConveyorMonitor(data_dir=None)
core = Fast(history_path=None)
hist = json.load(open(ROOT / "v2_2_1/backend/data/conveyor_core_history.json"))
core.history = [{**r, "day": pd.Timestamp(r["day"])} for r in hist if r["day"] < "2026-10-26"]
mon._rt.core = core
# 유닛 기록: HDI_MODEL/conveyor_models/data (엣지 방식으로 다시 만든 기록) 중 10-26 전
for u, m in mon._rt.units.items():
    h = json.load(open(PKG / "data" / f"conveyor_unit_{u}_history.json"))
    m.history = [{**r, "day": pd.Timestamp(r["day"])} for r in h if r["day"] < "2026-10-26"]

tags = db.model_tags("conveyor", "v2.2")
eps, kept, obs, last_eval = {}, None, [], None
for day in pd.date_range("2026-10-26", "2026-11-14"):
    for t, row in H.rows_of(day, tags):
        if kept is not None and (t - kept).total_seconds() < 0.5:
            continue
        kept = t
        _, warnings = mon.check(row, now=t.tz_localize("Asia/Seoul").timestamp())
        for w in warnings:
            k = (w["conv"], w["time"])
            e = eps.setdefault(k, dict(conv=w["conv"], onset=t, first=t, last=t, reasons=[], messages=[], sev=0, type=w["type"]))
            e["last"] = t
            if w["reason"] not in e["reasons"]:
                e["reasons"].append(w["reason"])
            if w["message"] not in e["messages"]:
                e["messages"].append(w["message"])
            e["sev"] = max(e["sev"], w["sev"] or 0)
        if mon._rt.last_eval is not None and mon._rt.last_eval != last_eval:
            last_eval = mon._rt.last_eval
            for u, m in mon._rt.units.items():
                for it in m.result.get("items", []):
                    obs.append(dict(t=t, unit=u, metric=it["metric"], state=it["state"], sev=it["sev"], basis=m.result["basis"]))
    print(TAG, day.date(), len(eps), flush=True)
pd.DataFrame(eps.values()).to_pickle(ROOT / f"con_check/stepwise/out/ep_{TAG}_throttle.pkl")
pd.DataFrame(obs).to_pickle(ROOT / f"con_check/stepwise/out/unitobs_{TAG}.pkl")   # 유닛 관찰 이상(빨라짐 포함) 흔적

print("done")
