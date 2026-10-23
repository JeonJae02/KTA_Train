"""대차 통과 센서 실시간 판정 — 백엔드 ConveyorMonitor.read() 를 이걸로 바꾼다 (1차 배포).

    mon = ConveyorRealtime(history_path="data/conveyor_history.json")   # 반복문 밖에서 한 번
    while True:
        out = mon.read(live_data)        # live_data: {태그명: 원시 PLC 값} (이번 tick 에 들어온 것만이어도 됨)

판정 규칙은 conv_rules.py (conv_02 배치와 같은 함수). 이 파일은 tick 을 받아서 그 규칙에 넣을 표를 만든다.

## tick 하나에 하는 일 (대당 수십 µs)

1. 카운터 6개(R00030 · R00040 · R00055 · R00060 · R00070 · R00080)의 변화량 중앙값으로 체인 위치(카운트)를 늘린다.
   샘플 간격이 GAP_SEC 보다 길면 그 사이 이동을 모르므로 위치를 이어 붙이지 않는다(seg 증가).
2. 카운터가 50 넘게 떨어지면 그 센서의 상승엣지(대차 통과)다. 위치 = 지금 위치 − (값 − 1).
   조금 떨어지면 같은 대차에서 다시 켜진 것(2차 리셋)이라 그 통과의 rearm 을 센다.
3. 원본 비트가 꺼지면 켜짐 길이 = 꺼진 위치 − 통과 위치 (40카운트 안일 때만).
4. 통과는 같은 센서의 다음 통과가 올 때 마감한다. 대차 번호는 "같은 대차가 P00140 을 지난 위치"
   (= 이 통과 위치 − 센서 간 거리 D) 에 가장 가까운 P00140 통과에서 가져온다.

## 판정 (EVAL_SEC 마다)

- **현재 상태:** (센서, 대차)마다 최근 WINDOW 번 통과의 중앙값으로 오늘 칸을 만들고, 지난 가동일 기록과 합쳐
  conv_rules.judge 에 넣는다. 히스테리시스·3일 승인·복귀 같은 시간 규칙이 그대로 적용된다.
- **하루 마감:** 한국시간 오전 6시(탱크 실시간 모델과 같은 기준)에 그날 통과 전부로 일별 칸을 만들어 기록에 넣는다.
  기록은 history_path 에 저장한다 — 재시작해도 승인·복귀 판정이 이어진다.

## 출력 (read 의 반환)

숫자 점수는 내지 않는다. 상태 4단계(정상 · 관찰 · 점검 추천 · 경고)와 근거 문구만 낸다.
  carrier  : 지금 주입부에 있는 대차 ("대차번호-05", 기존 목업과 같은 키)
  ready    : 판정할 만큼 데이터가 모였는지
  line     : 라인 전체 상태 (가장 나쁜 스테이션) + 구동부(속도 비율)
  stations : 스테이션 6곳 (주입부 · 클로징 · 오프닝 · 전진 · 후진 진입) 상태, 센서 단위 문제, 문제 대차 목록
  wagons   : 대차 42대 상태와 걸린 스테이션
"""

import json
import os
from collections import defaultdict, deque
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from .conv_rules import (SEV_NAME, FLAG_ACTION, SPEED_TOL, cell_reason, cell_sev, judge, sensor_level)

# --------------------------------------------------------------------------- #
# 설정 — 값은 conv_02 (09-10 – 10-19, 31 가동일) 에서 나온 것
# --------------------------------------------------------------------------- #
N_WAGON = 42
RESET_DROP = 50          #: 카운터가 이만큼 떨어지면 통과(1차 리셋), 그보다 적게 떨어지면 2차 리셋
GAP_SEC = 5.0            #: 샘플 간격이 이보다 길면 위치를 이어 붙이지 않는다
WIDTH_WIN = 40           #: 통과 뒤 이 카운트 안의 첫 하강을 같은 펄스로 본다
MATCH_TOL = 69           #: 대차 번호 매칭 허용 (피치 139 의 절반)
WINDOW = 20              #: 현재 상태 = 최근 20번 통과 (대차당 약 3시간)
MIN_PASSES = 8           #: 이보다 적게 지나간 칸은 판정하지 않는다
DAY_MIN_PASSES = 20      #: 하루 마감 때 이보다 적은 칸은 기록하지 않는다 (배치와 같음)
HISTORY_DAYS = 60        #: 가동일 기록 보관 일수 ("언제부터" 표시와 승인·복귀 판정에 쓴다)
EVAL_SEC = 60            #: 판정 주기
DAY_START_HOUR = 6       #: 하루 경계 (한국시간)
K_SPEED = 0.2744         #: 초당 카운트 ÷ (Hz_In_Conv/10). 이에서 SPEED_TOL 넘게 벗어나면 구동부 경고
SPEED_WINDOW_SEC = 1800  #: 속도 비율은 최근 30분 정속 구간으로 본다

COUNTERS = ["R00030", "R00040", "R00055", "R00060", "R00070", "R00080"]

#: 센서: 카운터, 센서 간 거리 D (같은 대차가 P00140 에서 이 센서까지 가는 카운트), 스테이션
SENSORS = {
    "P00140": dict(name="대차 진입", counter="R00040", D=0.0, station="pour"),
    "P00141": dict(name="대차 쉬프트", counter="R00080", D=19.0, station="pour"),
    "P00154": dict(name="클로징 존 진입", counter="R00030", D=295.0, station="closing"),
    "P00164": dict(name="오프닝 존 진입", counter="R00070", D=3709.0, station="opening"),
    "P00451": dict(name="전진 유닛 진입", counter="R00055", D=3758.0, station="fwd"),
    "P00459": dict(name="후진 유닛 진입", counter="R00060", D=5728.0, station="rev"),
}
STATIONS = {
    "pour": "주입부", "closing": "클로징 존", "opening": "오프닝 존", "fwd": "전진 유닛", "rev": "후진 유닛",
}
#: 장기 기준 (09-10 – 09-19 중앙값, 카운터 6개로 만든 위치). 센서 이동·감도·흔들림 판정에 쓴다
LONG_REF = {
    "P00140": dict(D=0.0, W0=8.0, J0=0.0), "P00141": dict(D=19.0, W0=8.0, J0=0.0),
    "P00154": dict(D=295.0, W0=8.5, J0=0.0), "P00164": dict(D=3709.0, W0=9.0, J0=0.0),
    "P00451": dict(D=3758.0, W0=9.0, J0=0.0), "P00459": dict(D=5728.0, W0=8.0, J0=0.0),
}
WAGON_TAG = "Now_Actual_Wagon_Num"
RUN_TAG = "Conv_Run_Out"
HZ_TAG = "Hz_In_Conv"
REQUIRED_TAGS = COUNTERS + list(SENSORS) + [WAGON_TAG, RUN_TAG, HZ_TAG]


def position_increment(diffs):
    """카운터 변화량들 → 이번 샘플 이동량.

    2차 리셋(같은 대차에서 센서가 다시 켜짐)은 카운터를 조금 떨어뜨린다(−5 ~ −20). 이걸 이동량 중앙값에
    넣으면, 같은 샘플에 다른 카운터가 1차 리셋으로 빠졌을 때 남은 값이 (+6, −8) 처럼 되어 위치를 잃는다.
    그래서 다수 방향(정회전이면 0 이상)의 값만으로 중앙값을 잡는다. 역회전이면 음수 쪽을 쓴다.
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


class ConveyorRealtime:
    """대차 통과 센서 6개의 실시간 판정. read(live_data) 를 tick 마다 부른다."""

    def __init__(self, history_path=None, window=WINDOW, min_passes=MIN_PASSES, eval_sec=EVAL_SEC,
                 day_start_hour=DAY_START_HOUR, sensors=None):
        self.sensors = sensors or SENSORS
        self.history_path = history_path
        self.window, self.min_passes, self.eval_sec = window, min_passes, eval_sec
        self.day_start_hour = day_start_hour
        self.last = {}                                   # 태그 → 마지막 값
        self.prev = {}                                   # 지난 tick 카운터 값
        self.prev_bit = {}
        self.pos, self.seg, self.last_t = 0.0, 0, None
        self.ref = deque(maxlen=160)                     # P00140 통과 (pos, seg, wagon) — 한 바퀴 반
        self.open = {}                                   # 센서 → 진행 중 통과 {pos, seg, rearm, width}
        self.recent = defaultdict(lambda: deque(maxlen=self.window))   # (센서, 대차) → 최근 통과
        self.today = defaultdict(list)                   # (센서, 대차) → 오늘 통과 전부
        self.history = []                                # 지난 가동일 칸 (dict)
        self.day = None
        self.hz_hist = deque(maxlen=20)
        self.speed = deque()                             # (t, dpos, dt*hz/10)
        self.last_eval = None
        self.result = self._empty()
        if history_path and os.path.exists(history_path):
            self.load_history(history_path)

    # ------------------------------------------------------------------ #
    # tick
    # ------------------------------------------------------------------ #
    def read(self, live_data=None, current_time=None):
        """백엔드 ConveyorMonitor.read 와 같은 모양: tick 마다 부르고 최신 판정을 돌려받는다."""
        if live_data:
            self.update(current_time or datetime.now(), live_data)
        return self.result

    def update(self, t, row):
        t = pd.Timestamp(t)
        if t.tzinfo is not None:
            t = t.tz_convert("Asia/Seoul").tz_localize(None)
        day = (t - timedelta(hours=self.day_start_hour)).date()
        if self.day is None:
            self.day = day
        elif day != self.day:
            self.close_day()
            self.day = day

        for k, v in row.items():
            f = _num(v)
            if f is not None:
                self.last[k] = f

        dt = None if self.last_t is None else (t - self.last_t).total_seconds()
        new_seg = dt is None or dt > GAP_SEC
        if new_seg:
            self.seg += 1
            for s in list(self.open):                    # 끊긴 통과는 그대로 마감
                self._close_pass(s)
        cur = {c: self.last.get(c) for c in COUNTERS}
        inc = 0.0
        if not new_seg:
            d = [cur[c] - self.prev[c] for c in COUNTERS
                 if cur[c] is not None and self.prev.get(c) is not None and abs(cur[c] - self.prev[c]) < RESET_DROP]
            inc = position_increment(d)
            self.pos += inc
        # 1·2차 리셋
        for s, cfg in self.sensors.items():
            c = cfg["counter"]
            v, pv = cur.get(c), self.prev.get(c)
            if new_seg or v is None or pv is None:
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
        for s in self.sensors:
            b = self.last.get(s)
            if b is None:
                continue
            b = b > 0
            pb = self.prev_bit.get(s)
            if pb and not b and not new_seg and s in self.open:
                o = self.open[s]
                w = self.pos - o["pos"]
                if o["width"] is None and 0 < w <= WIDTH_WIN and o["seg"] == self.seg:
                    o["width"] = w
            self.prev_bit[s] = b
        # 속도 비율 (정속 구간)
        hz = self.last.get(HZ_TAG)
        self.hz_hist.append(hz)
        steady = (hz is not None and hz > 0 and len(self.hz_hist) == self.hz_hist.maxlen
                  and len(set(self.hz_hist)) == 1)
        if steady and not new_seg and dt is not None and dt < 1.0 and inc > 0 and (self.last.get(RUN_TAG) or 0) > 0:
            self.speed.append((t, inc, dt * hz / 10))
        while self.speed and (t - self.speed[0][0]).total_seconds() > SPEED_WINDOW_SEC:
            self.speed.popleft()

        self.prev = {c: v for c, v in cur.items() if v is not None}
        self.last_t = t
        if self.last_eval is None or (t - self.last_eval).total_seconds() >= self.eval_sec:
            self.evaluate(t)
            self.last_eval = t

    # ------------------------------------------------------------------ #
    # 통과 마감 · 하루 마감
    # ------------------------------------------------------------------ #
    def _close_pass(self, s):
        o = self.open.pop(s, None)
        if o is None:
            return
        if s == "P00140":
            match = next(((p, w) for p, sg, w in reversed(self.ref) if p == o["pos"] and sg == o["seg"]), None)
            if match is None:
                return
            wagon, dist = match[1], 0.0
        else:
            target = o["pos"] - self.sensors[s]["D"]
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

    @staticmethod
    def _cell(day, s, w, recs):
        a = np.array(recs, dtype=float)
        width = a[:, 1][np.isfinite(a[:, 1])]
        return dict(day=pd.Timestamp(day), sensor=s, wagon=int(w), n=len(a), dist=float(np.median(a[:, 0])),
                    width=float(np.median(width)) if len(width) else np.nan, width_n=len(width),
                    rearm=float(a[:, 2].mean()), jitter=_iqr(a[:, 0]))

    def close_day(self):
        """하루 마감: 그날 통과 전부로 칸을 만들어 기록에 넣고 저장한다."""
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
        out = [{**r, "day": r["day"].strftime("%Y-%m-%d")} for r in self.history]
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False)
        os.replace(tmp, path)

    def load_history(self, path):
        with open(path, encoding="utf-8") as f:
            rows = json.load(f)
        self.history = [{**r, "day": pd.Timestamp(r["day"])} for r in rows]

    def seed_history(self, daily):
        """배치 일별 표(conv_02 wagon_sensor_daily)로 기록을 채운다 — 처음 배포할 때 한 번."""
        d = daily[daily.n >= DAY_MIN_PASSES]
        days = sorted(d.day.unique())[-HISTORY_DAYS:]
        d = d[d.day.isin(days)]
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
            self.result = self._empty(t)
            return
        today = pd.Timestamp(self.day)
        table = pd.DataFrame([r for r in self.history if r["day"] < today] + cur)
        J = judge(table)
        now = J[J.day == today]
        lr = pd.DataFrame([{"sensor": s, **v} for s, v in LONG_REF.items()])
        _, lv = sensor_level(now, lr)
        self.result = self._build(t, J, now, lv)

    def _speed(self):
        if len(self.speed) < 100:
            return None, None
        a = np.array([(x[1], x[2]) for x in self.speed])
        r = a[:, 0].sum() / a[:, 1].sum()
        dev = r / K_SPEED - 1
        return r, (3 if abs(dev) > SPEED_TOL else 0)

    def _since(self, J, s, w, sev_now):
        g = J[(J.sensor == s) & (J.wagon == w)].sort_values("day")
        sev = [cell_sev(a, b) for a, b in zip(g.state, g.accepted)]
        k = len(sev) - 1
        while k > 0 and sev[k - 1] == sev_now:
            k -= 1
        return g.day.iloc[k].strftime("%m-%d") if len(g) else None

    def _build(self, t, J, now, lv):
        flags = {r.sensor: r.flag for r in lv.itertuples() if r.flag}
        stations = {sid: dict(name=name, sensors=[s for s, c in self.sensors.items() if c["station"] == sid],
                              sev=None, status=None, sensor_flags=[], wagons=[]) for sid, name in STATIONS.items()}
        wagons = {f"{w:02d}": dict(sev=None, status=None, stations=[]) for w in range(1, N_WAGON + 1)}
        for r in now.itertuples():
            sid = self.sensors[r.sensor]["station"]
            sev = cell_sev(r.state, r.accepted)
            st, wk = stations[sid], wagons[f"{int(r.wagon):02d}"]
            st["sev"] = max(st["sev"] or 0, sev)
            wk["sev"] = max(wk["sev"] or 0, sev)
            if sev > 0:
                item = dict(wagon=int(r.wagon), sensor=r.sensor, sev=sev, status=SEV_NAME[sev], state=r.state,
                            reason=cell_reason(r.state, r.rise_off, r.width_ex, r.rearm, r.accepted),
                            since=self._since(J, r.sensor, int(r.wagon), sev))
                st["wagons"].append(item)
                wk["stations"].append(dict(station=sid, name=STATIONS[sid], sensor=r.sensor, sev=sev,
                                           status=SEV_NAME[sev], state=r.state, reason=item["reason"], since=item["since"]))
        for s, fs in flags.items():
            st = stations[self.sensors[s]["station"]]
            for f in fs.split(", "):
                st["sensor_flags"].append(dict(sensor=s, name=self.sensors[s]["name"], flag=f, action=FLAG_ACTION.get(f, "")))
            st["sev"] = max(st["sev"] or 0, 2)
        for st in stations.values():
            st["wagons"].sort(key=lambda x: (-x["sev"], x["wagon"]))
            st["status"] = None if st["sev"] is None else SEV_NAME[st["sev"]]
        for wk in wagons.values():
            wk["status"] = None if wk["sev"] is None else SEV_NAME[wk["sev"]]
        ratio, ssev = self._speed()
        drive = dict(sev=ssev, status=None if ssev is None else SEV_NAME[ssev],
                     reason=None if ratio is None else ("정상" if ssev == 0 else "속도 비율이 기준에서 벗어남 → 구동부·체인 점검"))
        sevs = [s["sev"] for s in stations.values() if s["sev"] is not None] + ([ssev] if ssev is not None else [])
        line_sev = max(sevs) if sevs else None
        w = self.last.get(WAGON_TAG)
        return dict(carrier=None if w is None else f"대차번호-{int(w):02d}", updated=t.isoformat(timespec="seconds"),
                    ready=True, line=dict(sev=line_sev, status=None if line_sev is None else SEV_NAME[line_sev], drive=drive),
                    stations=stations, wagons=wagons)

    def _empty(self, t=None):
        w = self.last.get(WAGON_TAG) if hasattr(self, "last") else None
        return dict(carrier=None if w is None else f"대차번호-{int(w):02d}",
                    updated=None if t is None else pd.Timestamp(t).isoformat(timespec="seconds"),
                    ready=False, line=dict(sev=None, status=None, drive=dict(sev=None, status=None, reason=None)),
                    stations={sid: dict(name=n, sensors=[s for s, c in SENSORS.items() if c["station"] == sid], sev=None,
                                        status=None, sensor_flags=[], wagons=[]) for sid, n in STATIONS.items()},
                    wagons={f"{w:02d}": dict(sev=None, status=None, stations=[]) for w in range(1, N_WAGON + 1)})
