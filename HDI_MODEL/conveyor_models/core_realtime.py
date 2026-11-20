"""[코어 · 전처리 + 실시간 판정] 대차 통과 센서 6개 — CoreRealtime.

ConveyorRealtimeGroup 가 tick 마다 update(t, row) 를 부른다. 유닛(UnitRealtime)은 여기서 만든 체인 위치(pos)와
마지막 태그 값(last)을 빌려 쓴다. CoreRealtime 는 유닛을 모른다 — 유닛이 없어도 똑같이 돈다.

## 전처리 (tick 마다)

1. 체인 위치: 카운터 6개 변화량의 중앙값을 누적한다 (position_increment).
   왜: 시간 대신 거리로 보면 속도가 바뀌어도 센서 사이 거리는 같다. 중앙값이라 카운터 하나가 리셋 중이어도 안 흔들린다.
   샘플 간격이 GAP_SEC 보다 길면 그 사이 이동을 모르므로 이어 붙이지 않는다 (seg 증가).
2. 센서 통과: 카운터가 50 넘게 떨어짐 = 대차 통과. 위치 = 지금 위치 − (값 − 1).
   왜: 센서 비트는 0.5초 샘플 사이에 묻혀 40% 가까이 놓치지만, 카운터 값은 매 샘플 남는다.
   조금 떨어짐 = 같은 대차에서 다시 켜짐(2차 리셋) → 그 통과의 재감지 수.
3. 켜짐 길이: 원본 비트가 꺼진 위치 − 통과 위치 (40카운트 안).
   꺼짐을 본 샘플에서 체인이 WIDTH_STEP_MAX 넘게 갔으면 그 통과의 켜짐 길이는 버린다 (NaN).
   왜: 꺼진 위치는 "꺼진 것을 처음 본 샘플"이라 샘플 하나(정속 약 6카운트)만큼 오차가 있다. 메시지가 하나 빠지면
   (엣지 예지 엔진의 0.5초 스로틀이 약 9%를 건너뛴다) 12카운트로 벌어져 문턱 3카운트를 넘는다 (C-04).
4. 대차 번호: "통과 위치 − 센서 간 거리 D" 에 가장 가까운 P00140 통과의 대차 번호.
   왜: 대차 번호는 주입부에서 바뀌므로, 다른 센서가 켜진 순간의 번호는 그 센서 앞 대차가 아니다.
5. (센서, 대차) 칸마다 최근 WINDOW 번 통과를 모은다. 오늘 통과 전부는 하루 마감용으로 따로 모은다.

## 판정 (evaluate, 1분마다 — ConveyorRealtimeGroup 가 부름)

최근 통과로 만든 오늘 칸 + 지난 가동일 기록 → core_judge.judge (칸 판정 · 히스테리시스 · 3일 승인)
→ core_judge.sensor_level (센서 단위) → 속도 비율 (구동부).
구동부 속도 비율은 생산 속도(SPEED_MIN_HZ 이상)·재가동 뒤 SPEED_SKIP_AFTER_RUN 이 지난 정속 샘플로만 잡고,
창 안 정속 시간이 SPEED_MIN_STEADY_SEC 이상이고 재가동 뒤 SPEED_GRACE_SEC 가 지났을 때만 판정한다 (C-06).
하루 마감(close_day, 오전 6시): 그날 통과 전부로 칸을 만들어 기록에 더하고 저장한다.
"""

import json
import os
from collections import defaultdict, deque
from datetime import timedelta

import numpy as np
import pandas as pd

from .core_judge import SEV_NAME, FLAG_ACTION, SPEED_TOL, absorbed_by_sensor, cell_reason, cell_sev, judge, sensor_level
from .core_params import COUNTERS, HZ_TAG, K_SPEED, LONG_REF, RUN_TAG, SENSORS, WAGON_TAG

N_WAGON = 42
RESET_DROP = 50          #: 카운터가 이만큼 떨어지면 통과(1차 리셋), 그보다 적게 떨어지면 2차 리셋
GAP_SEC = 5.0            #: 샘플 간격이 이보다 길면 위치를 이어 붙이지 않는다
WIDTH_WIN = 40           #: 통과 뒤 이 카운트 안의 첫 하강을 같은 펄스로 본다
WIDTH_STEP_MAX = 9.0     #: 꺼짐을 본 샘플의 이동이 이보다 크면(샘플이 빠짐) 그 통과의 켜짐 길이는 버린다
MATCH_TOL = 69           #: 대차 번호 매칭 허용 (피치 139 의 절반)
WINDOW = 20              #: 현재 상태 = 최근 20번 통과 (대차당 약 3시간)
MIN_PASSES = 8           #: 이보다 적게 지나간 칸은 판정하지 않는다
MIN_WAGONS = 40          #: 센서 하나에서 판정 가능한 대차가 이보다 적으면 그 센서는 아직 판정하지 않는다
#                          (기동 직후 몇 대만 모였을 때 "재감지 대차 10대"·센서 이동 같은 센서 단위 판정이 틀어지지 않게.
#                           대차는 한 바퀴(약 9분)에 다 지나가므로 기다리는 시간은 한 바퀴 이하)
DAY_MIN_PASSES = 20      #: 하루 마감 때 이보다 적은 칸은 기록하지 않는다 (배치와 같음)
HISTORY_DAYS = 60        #: 가동일 기록 보관 일수 ("언제부터" 표시와 승인·복귀 판정)
SPEED_WINDOW_SEC = 1800  #: 속도 비율은 최근 30분 정속 구간으로 본다
SPEED_MAX_DT = 2.0       #: 이 간격 미만 샘플을 쓴다. 1초 미만만 쓰면 스로틀로 빠진 구간이 같이 빠져 속도가 낮게 치우친다
SPEED_MIN_HZ = 300       #: 생산 속도(30 Hz) 이상만 (0.1 Hz). 15 Hz 에서는 비율이 0.271 ~ 0.273 (슬립)
SPEED_SKIP_AFTER_RUN = 60    #: 재가동 뒤 이 초 동안의 샘플은 버린다 (가속 중)
SPEED_MIN_STEADY_SEC = 300   #: 창 안 정속 시간이 이보다 짧으면 판정하지 않는다
SPEED_GRACE_SEC = 300        #: 재가동(또는 시작) 뒤 이 초 동안은 판정하지 않는다


def position_increment(diffs):
    """카운터 변화량들 → 이번 샘플 이동량.

    2차 리셋은 카운터를 조금(−5 ~ −20) 떨어뜨린다. 이걸 중앙값에 넣으면 같은 샘플에 다른 카운터가 1차 리셋으로
    빠졌을 때 (+6, −8) 처럼 남아 위치를 잃는다. 그래서 다수 방향(정회전이면 0 이상) 값만으로 중앙값을 잡는다.
    """
    d = [x for x in diffs if x is not None]
    if not d:
        return 0.0
    pos = [x for x in d if x >= 0]
    neg = [x for x in d if x < 0]
    return float(np.median(pos if len(pos) >= len(neg) else neg))


def _iqr(x):
    x = np.asarray(x, dtype=float)
    x = x[np.isfinite(x)]
    return float(np.subtract(*np.percentile(x, [75, 25]))) if len(x) >= 5 else np.nan


def _num(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if np.isnan(f) else f


def to_kst_naive(t):
    t = pd.Timestamp(t)
    return t.tz_convert("Asia/Seoul").tz_localize(None) if t.tzinfo is not None else t


class CoreRealtime:
    """코어 6개 센서. update(t, row) 를 tick 마다, evaluate(t) 를 판정 주기마다."""

    def __init__(self, history_path=None, window=WINDOW, min_passes=MIN_PASSES, day_start_hour=6):
        self.history_path = history_path
        self.window, self.min_passes, self.day_start_hour = window, min_passes, day_start_hour
        self.last = {}                                   # 태그 → 마지막 값 (유닛도 이걸 읽는다)
        self.prev = {}                                   # 지난 tick 카운터 값
        self.prev_bit = {}
        self.pos, self.seg, self.last_t, self.new_seg = 0.0, 0, None, True
        self.ref = deque(maxlen=160)                     # P00140 통과 (pos, seg, wagon) — 한 바퀴 반
        self.open = {}                                   # 센서 → 진행 중 통과 {pos, seg, rearm, width}
        self.recent = defaultdict(lambda: deque(maxlen=self.window))   # (센서, 대차) → 최근 통과
        self.today = defaultdict(list)                   # (센서, 대차) → 오늘 통과 전부
        self.history = []                                # 지난 가동일 칸 (dict)
        self.day = None
        self.hz_hist = deque(maxlen=20)
        self.run_hist = deque(maxlen=20)
        self.speed = deque()                             # (t, dpos, dt*hz/10, dt)
        self.run_on_t = None                             # 마지막 재가동(운전 OFF→ON) 또는 모델 시작 시각
        self.result = self.empty()
        if history_path and os.path.exists(history_path):
            self.load_history(history_path)

    # ------------------------------------------------------------------ #
    # 전처리 (tick)
    # ------------------------------------------------------------------ #
    def update(self, t, row):
        t = to_kst_naive(t)
        day = (t - timedelta(hours=self.day_start_hour)).date()
        if self.day is None:
            self.day = day
        elif day != self.day:
            self.close_day()
            self.day = day
        prev_run = self.last.get(RUN_TAG)
        for k, v in row.items():
            f = _num(v)
            if f is not None:
                self.last[k] = f
        if self.run_on_t is None or (prev_run or 0) <= 0 < (self.last.get(RUN_TAG) or 0):
            self.run_on_t = t                            # 재가동(또는 모델 시작) — 구동부 유예·가속 구간 기준

        dt = None if self.last_t is None else (t - self.last_t).total_seconds()
        self.new_seg = dt is None or dt > GAP_SEC
        if self.new_seg:
            self.seg += 1
            for s in list(self.open):                    # 끊긴 통과는 그대로 마감
                self._close_pass(s)
        cur = {c: self.last.get(c) for c in COUNTERS}
        inc = 0.0
        if not self.new_seg:
            d = [cur[c] - self.prev[c] for c in COUNTERS
                 if cur[c] is not None and self.prev.get(c) is not None and abs(cur[c] - self.prev[c]) < RESET_DROP]
            inc = position_increment(d)
            self.pos += inc
        # 1·2차 리셋
        for s, cfg in SENSORS.items():
            v, pv = cur.get(cfg["counter"]), self.prev.get(cfg["counter"])
            if self.new_seg or v is None or pv is None:
                continue
            dv = v - pv
            if dv < -RESET_DROP:
                self._close_pass(s)
                p = self.pos - (v - 1)
                self.open[s] = dict(pos=p, seg=self.seg, rearm=0, width=None)
                if s == "P00140":
                    w = self.last.get(WAGON_TAG)
                    if w is not None:
                        self.ref.append((p, self.seg, int(w)))
            elif dv < 0 and s in self.open:
                self.open[s]["rearm"] += 1
        # 켜짐 길이: 원본 비트가 꺼진 순간
        for s in SENSORS:
            b = self.last.get(s)
            if b is None:
                continue
            b = b > 0
            if self.prev_bit.get(s) and not b and not self.new_seg and s in self.open:
                o = self.open[s]
                w = self.pos - o["pos"]
                if o["width"] is None and 0 < w <= WIDTH_WIN and o["seg"] == self.seg:
                    # 이번 샘플 이동이 크면 꺼진 위치를 모른다 (샘플 빠짐) → 버린다. NaN 은 칸 중앙값에서 빠진다
                    o["width"] = w if inc <= WIDTH_STEP_MAX else np.nan
            self.prev_bit[s] = b
        # 속도 비율 (정속 구간: Hz 지령과 운전이 둘 다 20샘플(약 10초) 그대로)
        # 운전이 막 켜진 직후는 지령은 그대로여도 인버터가 가속 중이라 실제 속도가 낮다. 운전 ON/OFF 가 잦은
        # 시간대(휴식·인칭 운전)에 이 구간이 섞이면 속도 비율이 −1% 까지 내려가 구동부 오탐이 난다.
        # 그래서 재가동 뒤 SPEED_SKIP_AFTER_RUN 초는 버리고, 생산 속도(SPEED_MIN_HZ 이상)만 쓴다.
        # 간격은 SPEED_MAX_DT(2초) 미만까지 쓴다 — 1초 미만만 쓰면 스로틀로 빠진 구간(약 1초)이 같이 빠져서
        # 남는 샘플이 "시각 간격이 길게 찍힌 것"으로 치우치고 속도가 낮게 나온다 (엣지 입력 평균 −0.04%).
        hz = self.last.get(HZ_TAG)
        self.hz_hist.append(hz)
        self.run_hist.append((self.last.get(RUN_TAG) or 0) > 0)
        steady = (hz is not None and hz >= SPEED_MIN_HZ and len(self.hz_hist) == self.hz_hist.maxlen
                  and len(set(self.hz_hist)) == 1 and all(self.run_hist))
        if (steady and not self.new_seg and dt is not None and dt < SPEED_MAX_DT and inc > 0
                and (t - self.run_on_t).total_seconds() >= SPEED_SKIP_AFTER_RUN):
            self.speed.append((t, inc, dt * hz / 10, dt))
        while self.speed and (t - self.speed[0][0]).total_seconds() > SPEED_WINDOW_SEC:
            self.speed.popleft()
        self.prev = {c: v for c, v in cur.items() if v is not None}
        self.last_t = t

    def _close_pass(self, s):
        """통과 마감 — 대차 번호를 붙여 칸에 넣는다."""
        o = self.open.pop(s, None)
        if o is None:
            return
        if s == "P00140":
            match = next(((p, w) for p, sg, w in reversed(self.ref) if p == o["pos"] and sg == o["seg"]), None)
            if match is None:
                return
            wagon, dist = match[1], 0.0
        else:
            target = o["pos"] - SENSORS[s]["D"]
            best = None
            for p, sg, w in self.ref:
                if sg != o["seg"]:
                    continue
                gap = abs(target - p)
                if gap < MATCH_TOL and (best is None or gap < best[0]):
                    best = (gap, p, w)
            if best is None:
                return
            wagon, dist = best[2], o["pos"] - best[1]
        rec = (dist, np.nan if o["width"] is None else o["width"], o["rearm"])
        self.recent[(s, wagon)].append(rec)
        self.today[(s, wagon)].append(rec)

    # ------------------------------------------------------------------ #
    # 하루 마감 · 기록
    # ------------------------------------------------------------------ #
    @staticmethod
    def _cell(day, s, w, recs):
        a = np.array(recs, dtype=float)
        width = a[:, 1][np.isfinite(a[:, 1])]
        return dict(day=pd.Timestamp(day), sensor=s, wagon=int(w), n=len(a), dist=float(np.median(a[:, 0])),
                    width=float(np.median(width)) if len(width) else np.nan, width_n=len(width),
                    rearm=float(a[:, 2].mean()), jitter=_iqr(a[:, 0]))

    def close_day(self):
        rows = [self._cell(self.day, s, w, recs) for (s, w), recs in self.today.items() if len(recs) >= DAY_MIN_PASSES]
        self.today = defaultdict(list)
        if not rows:
            return
        self.history = [r for r in self.history if r["day"] != pd.Timestamp(self.day)] + rows
        days = sorted({r["day"] for r in self.history})[-HISTORY_DAYS:]
        self.history = [r for r in self.history if r["day"] in days]
        if self.history_path:
            self.save_history(self.history_path)

    def save_history(self, path):
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump([{**r, "day": r["day"].strftime("%Y-%m-%d")} for r in self.history], f, ensure_ascii=False)
        os.replace(tmp, path)

    def load_history(self, path):
        with open(path, encoding="utf-8") as f:
            self.history = [{**r, "day": pd.Timestamp(r["day"])} for r in json.load(f)]

    def seed_history(self, daily):
        """배치 일별 표(cache/conv_feat/wagon_sensor_daily.pkl)로 기록을 채운다 — 처음 배포할 때 한 번."""
        d = daily[daily.n >= DAY_MIN_PASSES]
        d = d[d.day.isin(sorted(d.day.unique())[-HISTORY_DAYS:])]
        cols = ["day", "sensor", "wagon", "n", "dist", "width", "width_n", "rearm", "jitter"]
        self.history = [{**r, "day": pd.Timestamp(r["day"]), "wagon": int(r["wagon"])} for r in d[cols].to_dict("records")]

    # ------------------------------------------------------------------ #
    # 판정
    # ------------------------------------------------------------------ #
    def current_table(self):
        return [self._cell(self.day, s, w, recs) for (s, w), recs in self.recent.items() if len(recs) >= self.min_passes]

    def evaluate(self, t):
        cur = self.current_table()
        if not cur:
            self.result = self.empty()
            return self.result
        today = pd.Timestamp(self.day)
        J = judge(pd.DataFrame([r for r in self.history if r["day"] < today] + cur))
        now = J[J.day == today]
        _, lv = sensor_level(now, pd.DataFrame([{"sensor": s, **v} for s, v in LONG_REF.items()]))
        self.result = self._build(J, now, lv)
        return self.result

    def _speed(self):
        # 표본 "개수"(예전 100개 ≈ 재가동 뒤 1분 반)가 아니라 정속 "시간"으로 본다 — 입력 주기가 바뀌어도 같은 뜻.
        # 재가동 직후 몇 분은 창이 작고 가속 꼬리가 남아 0.3% 문턱을 자주 넘었다 (C-06)
        if (self.last_t is None or self.run_on_t is None
                or (self.last_t - self.run_on_t).total_seconds() < SPEED_GRACE_SEC
                or sum(x[3] for x in self.speed) < SPEED_MIN_STEADY_SEC):
            return dict(sev=None, status=None, reason=None)
        a = np.array([(x[1], x[2]) for x in self.speed])
        dev = a[:, 0].sum() / a[:, 1].sum() / K_SPEED - 1
        sev = 3 if abs(dev) > SPEED_TOL else 0
        return dict(sev=sev, status=SEV_NAME[sev],
                    reason="정상" if sev == 0 else "속도 비율이 기준에서 벗어남 → 구동부·체인 점검")

    @staticmethod
    def _since(J, s, w, sev_now):
        g = J[(J.sensor == s) & (J.wagon == w)].sort_values("day")
        sev = [cell_sev(a, b) for a, b in zip(g.state, g.accepted)]
        k = len(sev) - 1
        while k > 0 and sev[k - 1] == sev_now:
            k -= 1
        return g.day.iloc[k].strftime("%m-%d") if len(g) else None

    def _build(self, J, now, lv):
        """센서별·대차별 결과. 스테이션으로 묶는 건 ConveyorRealtimeGroup 가 한다."""
        sensors = self._blank_sensors()
        wagons = {f"{w:02d}": dict(sev=None, items=[]) for w in range(1, N_WAGON + 1)}
        n_wagons = now.groupby("sensor").wagon.nunique()
        ready = {s for s in SENSORS if n_wagons.get(s, 0) >= MIN_WAGONS}
        flags = {r.sensor: r.flag.split(", ") for r in lv.itertuples() if r.flag and r.sensor in ready}
        for r in now[now.sensor.isin(ready)].itertuples():
            # 센서 재감지로 묶인 칸은 센서 쪽 플래그 하나로만 낸다 (대차에는 정상으로)
            sev = 0 if absorbed_by_sensor(r.state, flags.get(r.sensor, ())) else cell_sev(r.state, r.accepted)
            se, wk = sensors[r.sensor], wagons[f"{int(r.wagon):02d}"]
            se["sev"] = max(se["sev"] or 0, sev)
            wk["sev"] = max(wk["sev"] or 0, sev)
            if sev > 0:
                item = dict(wagon=int(r.wagon), sensor=r.sensor, name=se["name"], unit=se["unit"], sev=sev,
                            status=SEV_NAME[sev], state=r.state,
                            reason=cell_reason(r.state, r.accepted),
                            since=self._since(J, r.sensor, int(r.wagon), sev))
                se["cells"].append(item)
                wk["items"].append(item)
        for s, fs in flags.items():
            se = sensors[s]
            for f in fs:
                se["flags"].append(dict(sensor=s, name=se["name"], flag=f, action=FLAG_ACTION.get(f, "")))
            se["sev"] = max(se["sev"] or 0, 2)
        for se in sensors.values():
            se["cells"].sort(key=lambda x: (-x["sev"], x["wagon"]))
        for wk in wagons.values():
            wk["items"].sort(key=lambda x: -x["sev"])
        w = self.last.get(WAGON_TAG)
        return dict(ready=True, carrier=None if w is None else int(w), sensors=sensors, wagons=wagons, drive=self._speed())

    @staticmethod
    def _blank_sensors():
        return {s: dict(name=c["label"], unit=c["unit"], sev=None, flags=[], cells=[]) for s, c in SENSORS.items()}

    def empty(self):
        w = self.last.get(WAGON_TAG) if hasattr(self, "last") else None
        return dict(ready=False, carrier=None if w is None else int(w), sensors=self._blank_sensors(),
                    wagons={f"{w:02d}": dict(sev=None, items=[]) for w in range(1, N_WAGON + 1)},
                    drive=dict(sev=None, status=None, reason=None))
