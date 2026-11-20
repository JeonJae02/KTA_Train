"""C-04 수정안 확인 — 켜짐 길이를 '빠진 샘플을 건너 잰 통과'에서는 버린다.

변형
  V0  지금 코드 (v2.2)
  V1  꺼짐을 본 샘플에서 체인이 STEP_MAX 카운트보다 많이 갔으면 그 통과의 켜짐 길이를 버린다
      (정상 샘플 하나 ≈ 6카운트, 메시지 하나 빠지면 ≈ 12카운트)
  W6  비교용 — 켜짐 길이 문턱 W_TOL 3 → 6
입력
  db        DB 행 전부 (모델이 모든 메시지를 봤을 때)
  throttle  엣지 그대로 — 직전 처리 뒤 0.5초 안 된 행은 모델이 못 본다 (PredictiveEngine.maybe_publish)

10-26 06시부터 하루씩 이어서 돈다. 가동일 기록은 엣지 기록 중 10-26 전(DB로 만든 씨앗)에서 시작해
재생이 스스로 쌓는다. 판정은 10분마다.
사용: venv/bin/python model_review_1103/con_check/edge_check/fix_check.py V1 throttle
"""
import sys, json, pickle
from contextlib import redirect_stdout
from io import StringIO
import numpy as np, pandas as pd

ROOT = "/Users/jeonjae/Desktop/KTA/DB_Log/model_review_1103"
OUT = ROOT + "/con_check/edge_check/out"
sys.path.insert(0, ROOT)
from review_lib import backend, db
backend.use("v2.2")
with redirect_stdout(StringIO()):
    import tools_yeji.conveyor_models.core_judge as cj
    from tools_yeji.conveyor_models.core_realtime import CoreRealtime, DAY_MIN_PASSES, SENSORS

STEP_MAX = 9.0
variant, mode = sys.argv[1], sys.argv[2]
DAYS = pd.date_range("2026-10-26", "2026-11-14")
EVAL_MIN = 10

if variant == "W6":
    cj.W_TOL = 6.0


class CoreV1(CoreRealtime):
    """꺼짐을 본 샘플의 이동이 STEP_MAX 를 넘으면 그 통과의 켜짐 길이를 버린다 (NaN — 칸 중앙값에서 빠짐)."""

    def update(self, t, row):
        pos0 = self.pos
        before = {s: o["width"] for s, o in self.open.items()}
        super().update(t, row)
        step = self.pos - pos0
        if step > STEP_MAX:
            for s, o in self.open.items():
                if s in before and before[s] is None and o["width"] is not None:
                    o["width"] = float("nan")


Core = CoreV1 if variant == "V1" else CoreRealtime
hist = json.load(open(ROOT + "/v2_2_1/backend/data/conveyor_core_history.json"))
c = Core(history_path=None)
c.history = [{**r, "day": pd.Timestamp(r["day"])} for r in hist if r["day"] < "2026-10-26"]

daily = []
orig_close = c.close_day
def close_day():
    rows = [c._cell(c.day, s, w, r) for (s, w), r in c.today.items() if len(r) >= DAY_MIN_PASSES]
    daily.extend(rows)
    orig_close()
c.close_day = close_day

tags = db.model_tags("conveyor", "v2.2")
evals, last_eval, kept = [], None, None
for day in DAYS:
    d0 = day + pd.Timedelta(hours=6)
    raw = pd.concat([db.fetch_day(day, tags, "conveyor"), db.fetch_day(day + pd.Timedelta(days=1), tags, "conveyor")])
    raw = raw[(raw.index >= d0) & (raw.index < d0 + pd.Timedelta(days=1))]
    cols = [x for x in raw.columns if x != "source"]
    for t, v in zip(raw.index, raw[cols].to_numpy(dtype=object)):
        if mode == "throttle":
            if kept is not None and (t - kept).total_seconds() < 0.5:
                continue
            kept = t
        c.update(t, {k: x for k, x in zip(cols, v) if x is not None and x == x})
        if last_eval is None or (t - last_eval).total_seconds() >= EVAL_MIN * 60:
            last_eval = t
            res = c.evaluate(t)
            if not res.get("ready"):
                continue
            for wk, w in res["wagons"].items():
                for it in w["items"]:
                    if it["sev"] >= 2:
                        evals.append(dict(t=t, kind="cell", sensor=it["sensor"], wagon=it["wagon"], state=it["state"]))
            for s, se in res["sensors"].items():
                for f in se["flags"]:
                    evals.append(dict(t=t, kind="flag", sensor=s, wagon=None, state=f["flag"]))
    print(variant, mode, day.date(), len(evals), flush=True)

pickle.dump(dict(evals=pd.DataFrame(evals), daily=pd.DataFrame(daily)), open(f"{OUT}/fix_{variant}_{mode}.pkl", "wb"))
print("done")
