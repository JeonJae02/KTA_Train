"""컨베이어 단계별 수정 — 엣지처럼 돌려 경고 에피소드를 낸다.

입력: DB 원시(cache/raw/conveyor)를 엣지와 같은 0.5초 스로틀로 거른다
      (PredictiveEngine.maybe_publish: 직전 처리 뒤 0.5초 안 된 메시지는 모델이 못 본다).
모델: v2_2_1 ConveyorMonitor (근무 단위 유지 포함) → 경고 목록 → 엣지 WarningLog·build_episodes 와 같은
      (conv, time) 단위 에피소드.

단계 (누적)
  S0  지금 코드 (v2.2)
  S1  + C-04 켜짐 길이: 꺼짐을 본 샘플의 이동이 9카운트를 넘으면 그 통과의 켜짐 길이를 버린다
  S2  + C-06 구동부: ① 샘플 간격 2초 미만까지 쓴다(지금은 1초 미만 — 스로틀이 건너뛴 구간이 빠지면서 남는
        샘플의 시각 간격이 길게 찍힌 쪽으로 치우쳐 속도가 낮게 나온다) ② 재가동 뒤 60초 샘플은 버린다(가속 중)
        ③ 생산 속도(30 Hz 이상) 샘플만 쓴다(15 Hz 에서는 비율이 0.271 ~ 0.273 — 슬립) ④ 창 안 정속 시간 5분 이상
        ⑤ 재가동(또는 시작) 뒤 5분 유예 — con_check/stepwise/drive_check.py
  S3  + C-07 유닛: 간격 0.75초 넘는 샘플을 건넌 변화는 버린다 · 기준을 09-22 ~ 10-25 DB로 다시 잡는다
        (σ 하한 시간 3 ms · 거리 0.1카운트) · 오늘 평균 판정을 끄고 마감된 가동일로만 판정
  S4  + 속도: 전진·후진 주입 시간은 판정에서 뺀다(끌림 거리와 같은 정보) ·
        걸림 거리를 '스톱퍼 SOL ON → 정위치 OFF'로 잰다(SOL 은 카운터 위치로 켜지므로 속도와 무관)

판정 속도: CoreRealtime.evaluate 는 매번 지난 60일 전체를 다시 판정한다(0.32초).
지난날 결과는 오늘 칸과 무관하므로(add_metrics 는 날짜별, 판정 루프는 시간 순) 하루 한 번만 계산하고
칸별 루프 상태를 이어받아 오늘 칸만 판정한다. verify_fast.py 로 원래 코드와 같은지 확인했다.

사용: venv/bin/python model_review_1103/con_check/stepwise/harness.py S0 [throttle|db]
"""
import sys, json, pickle, time as _time
from collections import deque
from contextlib import redirect_stdout
from io import StringIO
import numpy as np, pandas as pd

ROOT = "/Users/jeonjae/Desktop/KTA/DB_Log/model_review_1103"
OUT = ROOT + "/con_check/stepwise/out"
sys.path.insert(0, ROOT)
from review_lib import backend, db
backend.use("v2.2")
with redirect_stdout(StringIO()):
    import tools_yeji.conveyor_models.core_judge as cj
    import tools_yeji.conveyor_models.core_realtime as cr
    import tools_yeji.conveyor_models.unit_realtime as ur
    import tools_yeji.conveyor_models.unit_judge as uj
    import tools_yeji.Conveyor_Monitor as cmon
    from tools_yeji.conveyor_models.core_params import RUN_TAG

START, END = pd.Timestamp("2026-10-26 06:00"), pd.Timestamp("2026-11-15 06:00")
STEPS = ["S0", "S1", "S2", "S3", "S4"]
WIDTH_STEP_MAX = 9.0
DRIVE_GRACE_SEC = 300
DRIVE_MIN_SPAN_SEC = 300
DRIVE_SKIP_AFTER_RUN = 60
DRIVE_MIN_HZ = 300
DRIVE_MAX_DT = 2.0
UNIT_MAX_GAP = 0.75


# --------------------------------------------------------------------------- #
# 빠른 판정 (원래 judge 와 같은 결과)
# --------------------------------------------------------------------------- #
def _loop(t, init):
    """core_judge.judge 의 칸별 루프를 그대로 옮기고, 시작 상태를 받아 끝 상태를 돌려준다."""
    t = t.sort_values(["sensor", "wagon", "day"]).reset_index(drop=True)
    t["accepted"] = False
    t["event"] = ""
    end = {}
    for (s, w), g in t.groupby(["sensor", "wagon"], sort=False):
        prev_state, run, run_val, accepted_val = init.get((s, w), ("정상", 0, None, 0.0))
        for i, r in g.iterrows():
            st = r.state
            recovered = (r.width_ex < cj.BACK_TOL) and (r.get("rearm", 0) < cj.REARM_TOL / 2)
            if prev_state.startswith("이물") and not st.startswith("이물") and not recovered:
                st = prev_state
                t.at[i, "state"] = st
            if st.startswith("이물") and not prev_state.startswith("이물"):
                t.at[i, "event"] = "새 이물"
            elif prev_state.startswith("이물") and not st.startswith("이물"):
                t.at[i, "event"] = "이물 복귀"
            if st == "위치 변화":
                same = run_val is not None and abs(r.rise_off - run_val) < r.t_tol
                run = run + 1 if same else 1
                run_val = r.rise_off if not same else run_val
                if abs(r.rise_off - accepted_val) < r.t_tol:
                    t.at[i, "accepted"] = True
                elif run == 1:
                    t.at[i, "event"] = "위치 변화 점검"
                elif run >= cj.N_ACCEPT:
                    accepted_val = run_val
                    t.at[i, "accepted"] = True
                    t.at[i, "event"] = "위치 승인"
            else:
                run, run_val = 0, None
                if st == "정상":
                    if abs(accepted_val) >= r.t_tol:
                        t.at[i, "event"] = "위치 복귀"
                    accepted_val = 0.0
            if st == "흔들림" and prev_state != "흔들림":
                t.at[i, "event"] = "흔들림"
            prev_state = st
        end[(s, w)] = (prev_state, run, run_val, accepted_val)
    return t, end


def _classify(df):
    t = cj.add_metrics(df)
    t["state"] = t.apply(cj.classify_cell, axis=1)
    return t


class FastCore(cr.CoreRealtime):
    """evaluate 만 빠르게. 지난날 판정은 기록이 바뀔 때만 다시 한다."""
    width_drop = False
    drive_fix = False

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self._ck, self._JH, self._end = None, None, {}
        self.run_on_t, self.now_t = None, None
        self.sp2, self.prev_t2 = deque(), None

    def _hist_state(self, today):
        hist = [r for r in self.history if r["day"] < today]
        key = (today, len(hist), hist[-1]["day"] if hist else None, id(self.history))
        if key != self._ck:
            if hist:
                self._JH, self._end = _loop(_classify(pd.DataFrame(hist)), {})
            else:
                self._JH, self._end = None, {}
            self._ck = key
        return self._JH, self._end

    def evaluate(self, t):
        cur = self.current_table()
        if not cur:
            self.result = self.empty()
            return self.result
        today = pd.Timestamp(self.day)
        JH, end = self._hist_state(today)
        Jc, _ = _loop(_classify(pd.DataFrame(cur)), end)
        J = Jc if JH is None else pd.concat([JH, Jc], ignore_index=True)
        now = Jc
        _, lv = cj.sensor_level(now, pd.DataFrame([{"sensor": s, **v} for s, v in cr.LONG_REF.items()]))
        self.result = self._build(J, now, lv)
        return self.result

    def update(self, t, row):
        prev_run = self.last.get(RUN_TAG)
        pos0 = self.pos
        before = {s: o["width"] for s, o in self.open.items()} if self.width_drop else None
        super().update(t, row)
        tt = cr.to_kst_naive(t)
        self.now_t = tt
        if self.run_on_t is None or ((prev_run or 0) <= 0 < (self.last.get(RUN_TAG) or 0)):
            self.run_on_t = tt
        if self.drive_fix:
            dt = None if self.prev_t2 is None else (tt - self.prev_t2).total_seconds()
            self.prev_t2 = tt
            hz, inc = self.last.get(cr.HZ_TAG), self.pos - pos0
            steady = (hz is not None and hz >= DRIVE_MIN_HZ and len(self.hz_hist) == self.hz_hist.maxlen
                      and len(set(self.hz_hist)) == 1 and all(self.run_hist))
            if (steady and not self.new_seg and dt is not None and dt < DRIVE_MAX_DT and inc > 0
                    and (tt - self.run_on_t).total_seconds() >= DRIVE_SKIP_AFTER_RUN):
                self.sp2.append((tt, inc, dt * hz / 10, dt))
            while self.sp2 and (tt - self.sp2[0][0]).total_seconds() > cr.SPEED_WINDOW_SEC:
                self.sp2.popleft()
        if self.width_drop and self.pos - pos0 > WIDTH_STEP_MAX:
            for s, o in self.open.items():
                if s in before and before[s] is None and o["width"] is not None:
                    o["width"] = float("nan")

    def _speed(self):
        if not self.drive_fix:
            return super()._speed()
        if (self.now_t is None or (self.now_t - self.run_on_t).total_seconds() < DRIVE_GRACE_SEC
                or sum(x[3] for x in self.sp2) < DRIVE_MIN_SPAN_SEC):
            return dict(sev=None, status=None, reason=None)
        a = np.array([(x[1], x[2]) for x in self.sp2])
        dev = a[:, 0].sum() / a[:, 1].sum() / cr.K_SPEED - 1
        sev = 3 if abs(dev) > cj.SPEED_TOL else 0
        return dict(sev=sev, status=cj.SEV_NAME[sev],
                    reason="정상" if sev == 0 else "속도 비율이 기준에서 벗어남 → 구동부·체인 점검")


# --------------------------------------------------------------------------- #
# 단계별 설정
# --------------------------------------------------------------------------- #
CATCH_SOL = ("catch_cnt", "stop_sol", +1, "org", -1, "cnt", "대차가 걸리기까지: 스톱퍼 SOL ON → 정위치 OFF")


def apply_unit_step(step):
    """유닛 모듈 상수를 단계에 맞게 바꾼다 (프로세스 하나에 단계 하나)."""
    if step in ("S3", "S4"):
        ur.MAX_GAP = UNIT_MAX_GAP
        ur.MIN_CYCLES_TODAY = 10 ** 9                     # 오늘 평균 판정 끔 → 마감된 가동일만
    if step == "S4":
        ur.METRICS = [CATCH_SOL if m[0] == "catch_cnt" else m for m in ur.METRICS]
        orig = uj.cyl_judge

        def cyl_judge_s4(D, refs=None):
            if len(D) and D["unit"].iloc[0] in ("md_fwd", "md_rev") and "air_time" in D:
                D = D.drop(columns="air_time")
            return orig(D, refs)
        ur.cyl_judge = cyl_judge_s4


def unit_refs(seed_rows):
    """09-22 ~ 10-25 DB 가동일 평균으로 기준 (중앙값, σ = 연속한 날 차이 기반, 하한 3 ms · 0.1카운트)."""
    floor = {"s": 0.003, "cnt": 0.1}
    refs = {}
    D = pd.DataFrame(seed_rows)
    for u, g in D.groupby("unit"):
        g = g[g.day >= pd.Timestamp("2026-09-22")].sort_values("day")
        refs[u] = {}
        for c in [c for c in g.columns if c not in ("unit", "day", "cycles")]:
            x = g[c].to_numpy(dtype=float)
            x = x[np.isfinite(x)]
            if len(x) < 5:
                continue
            kind = uj.metric_kind(c)
            dx = np.abs(np.diff(x))
            s = 1.4826 * np.median(dx) / np.sqrt(2)
            refs[u][c] = (float(np.median(x)), float(max(s, floor[kind])))
    return refs


def make_monitor(step):
    apply_unit_step(step)
    mon = cmon.ConveyorMonitor(data_dir=None)
    rt = mon._rt
    core = FastCore(history_path=None)
    core.width_drop = step in ("S1", "S2", "S3", "S4")
    core.drive_fix = step in ("S2", "S3", "S4")
    core_hist = json.load(open(ROOT + "/v2_2_1/backend/data/conveyor_core_history.json"))
    core.history = [{**r, "day": pd.Timestamp(r["day"])} for r in core_hist if r["day"] < "2026-10-26"]
    rt.core = core
    if step in ("S3", "S4"):
        seed = pickle.load(open(f"{OUT}/seed_units_{step}.pkl", "rb"))
        ur.UNIT_REF = unit_refs(seed)
        for u, m in rt.units.items():
            m.history = [r for r in seed if r["unit"] == u]
    else:
        for u, m in rt.units.items():
            h = json.load(open(f"{ROOT}/v2_2_1/backend/data/conveyor_unit_{u}_history.json"))
            m.history = [{**r, "day": pd.Timestamp(r["day"])} for r in h if r["day"] < "2026-10-26"]
    return mon


# --------------------------------------------------------------------------- #
# 재생
# --------------------------------------------------------------------------- #
def rows_of(day, tags):
    d0 = day + pd.Timedelta(hours=6)
    raw = pd.concat([db.fetch_day(day, tags, "conveyor"), db.fetch_day(day + pd.Timedelta(days=1), tags, "conveyor")])
    raw = raw[(raw.index >= d0) & (raw.index < d0 + pd.Timedelta(days=1))]
    cols = [x for x in raw.columns if x != "source"]
    for t, v in zip(raw.index, raw[cols].to_numpy(dtype=object)):
        yield t, {k: x for k, x in zip(cols, v) if x is not None and x == x}


def run(step, mode="throttle"):
    mon = make_monitor(step)
    tags = db.model_tags("conveyor", "v2.2")
    eps, kept, t0 = {}, None, _time.time()
    for day in pd.date_range(START.normalize(), END.normalize() - pd.Timedelta(days=1)):
        for t, row in rows_of(day, tags):
            if mode == "throttle":
                if kept is not None and (t - kept).total_seconds() < 0.5:
                    continue
                kept = t
            now = t.tz_localize("Asia/Seoul").timestamp()
            _, warnings = mon.check(row, now=now)
            for w in warnings:
                k = (w["conv"], w["time"])
                e = eps.get(k)
                if e is None:
                    e = eps[k] = dict(conv=w["conv"], onset=t, first=t, last=t, reasons=[], messages=[], sev=0,
                                      type=w["type"])
                e["last"] = t
                if w["reason"] not in e["reasons"]:
                    e["reasons"].append(w["reason"])
                if w["message"] not in e["messages"]:
                    e["messages"].append(w["message"])
                e["sev"] = max(e["sev"], w["sev"] or 0)
        print(step, mode, day.date(), len(eps), f"{_time.time() - t0:.0f}s", flush=True)
    ep = pd.DataFrame(eps.values())
    ep.to_pickle(f"{OUT}/ep_{step}_{mode}.pkl")
    return ep


def seed_units(step):
    """S3·S4 유닛 기준·기록용 — 09-22 ~ 10-25 DB 전부(스로틀 없음)로 유닛 가동일 평균을 만든다."""
    apply_unit_step(step)
    with redirect_stdout(StringIO()):
        from tools_yeji.conveyor_models.conveyor_realtime import ConveyorRealtimeGroup
    g = ConveyorRealtimeGroup(data_dir=None)
    g.evaluate = lambda t: None
    tags = db.model_tags("conveyor", "v2.2")
    days = [d for d in pd.date_range("2026-09-22", "2026-10-25")
            if (db.paths.CACHE_DIR / "raw" / "conveyor" / f"{d:%Y-%m-%d}.pkl.xz").exists()
            and (db.paths.CACHE_DIR / "raw" / "conveyor" / f"{d + pd.Timedelta(days=1):%Y-%m-%d}.pkl.xz").exists()]
    for day in days:
        for t, row in rows_of(day, tags):
            g.update(t, row)
        print("seed", step, day.date(), flush=True)
    for m in g.units.values():
        m.close_day()
    rows = [r for m in g.units.values() for r in m.history]
    pickle.dump(rows, open(f"{OUT}/seed_units_{step}.pkl", "wb"))


if __name__ == "__main__":
    if sys.argv[1] == "seed":
        seed_units(sys.argv[2])
    else:
        run(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else "throttle")
