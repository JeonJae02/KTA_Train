"""[유닛 · 전처리 + 실시간 판정] 에어 주입 유닛 하나 — UnitRealtime. 전진·후진·에어백이 같은 클래스, 설정만 다르다.

ConveyorRealtimeGroup 가 tick 마다 CoreRealtime.update 다음에 update(t, core) 를 부른다.
유닛은 코어에서 두 가지만 빌린다: 마지막 태그 값(core.last), 체인 위치(core.pos — 거리 지표용).

## 전처리 (tick 마다) — conv_cyl.cycle_table 과 같은 계산을 한 tick 씩

1. 신호가 바뀐 시각 = 바뀌기 전 샘플과 바뀐 샘플의 중간 (위치도 같은 방식).
   샘플 간격이 MAX_GAP(1.2초) 보다 길면 그 변화의 시각을 버린다.
   왜 P 비트끼리: DB 수집기가 입력·출력(P)을 한 순간에 읽고 카운터는 0.26초 뒤에 읽는다.
2. 사이클 = 스톱퍼 SOL ON 부터 다음 스톱퍼 SOL ON 전까지. 사이클 시작 바로 전 샘플의 변화도 포함한다 (배치와 같음).
3. 사이클이 끝나면 지표마다 "시작 신호 첫 변화 → 끝 신호 첫 변화" 시간(초) 또는 거리(카운트)를 잰다.
   운전 중이 아니었거나 30초 넘게 걸린 사이클(중간에 멈춤)은 버린다.
4. 지표를 오늘 합계에 더한다. 사이클 하나는 0.5초 샘플이라 0 또는 0.5 로만 나오므로, **하루 평균**으로 본다.

## 판정 (evaluate)

오늘 사이클이 MIN_CYCLES_TODAY 이상 모이면 오늘 평균을, 아직이면 마지막 가동일 기록을 판정한다.
(하루 평균의 표준오차는 약 4 ms. 사이클이 적으면 잡음이 σ 와 비슷해져 거짓 경보가 난다)
기록 + 오늘 → unit_judge.cyl_judge (UNIT_REF 의 장기 기준·σ 로) → 지표별 상태.
PLC 에러·알람은 다루지 않는다 — 정상 가동 중 동작 시간의 변화만 본다.
하루 마감(오전 6시): 그날 평균을 기록에 더하고 저장한다.

출력 (result): {ready, sev, basis, day, cycles_today, items: [{metric, relay, sev, state, reason, since}]}
  items 는 sev > 0 인 지표만. relay 는 그 판정이 뜨는 부위 주소(unit_params.metric_part), 거리·에어 주입 시간 지표는 None.
  판정 전이면 ready=False, sev=None.
"""

import json
import os
from datetime import timedelta

import numpy as np
import pandas as pd

from .core_params import RUN_TAG
from .unit_judge import STATE_REASON, cyl_judge, item_reason, item_sev
from .unit_params import MAX_CYCLE, MAX_GAP, METRICS, UNIT_REF, bits, metric_columns, metric_part

MIN_CYCLES_DAY = 1000     #: 하루 마감 때 이보다 적은 날은 기록하지 않는다 (배치와 같음)
MIN_CYCLES_TODAY = 2000   #: 오늘 평균으로 판정하려면 이만큼 모여야 한다 (약 7–8시간). 그 전에는 마지막 가동일로
HISTORY_DAYS = 365        #: 하루 한 줄이라 길게 둔다 (승인된 변화가 기록에서 빠지지 않게)


def _num(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if np.isnan(f) else f


class UnitRealtime:
    """에어 주입 유닛 하나. update(t, core) 를 tick 마다, evaluate() 를 판정 주기마다."""

    def __init__(self, unit, history_path=None, day_start_hour=6):
        self.unit = unit
        self.history_path = history_path
        self.day_start_hour = day_start_hour
        self.cols = metric_columns(unit)
        self.sig = [(k, j, b) for k in ("stop_sol", "nozzle_sol", "return_sol", "open_sol", "org", "flt", "stop_fwd",
                                        "stop_rev", "nozzle_rev", "rev_latch") for j, b in enumerate(bits(unit, k))]
        self.prev_bit = {}
        self.prev_t = self.prev_pos = None
        self.t0 = None
        self.prev_edges = []                 # 지난 tick 의 변화 (사이클 시작 바로 전 샘플 포함용)
        self.cyc = None                      # 진행 중 사이클
        self.day = None
        self.sum = {c: 0.0 for c in self.cols}
        self.cnt = {c: 0 for c in self.cols}
        self.cycles = 0
        self.history = []
        self.result = self.empty()
        if history_path and os.path.exists(history_path):
            self.load_history(history_path)

    # ------------------------------------------------------------------ #
    # 전처리 (tick)
    # ------------------------------------------------------------------ #
    def update(self, t, core):
        """core: CoreRealtime (같은 tick 에 이미 update 된 것). core.last 의 태그 값과 core.pos 를 쓴다."""
        day = (t - timedelta(hours=self.day_start_hour)).date()
        if self.day is None:
            self.day = day
        elif day != self.day:
            self.close_day()
            self.day = day
        if self.t0 is None:
            self.t0 = t
        ts = (t - self.t0).total_seconds()
        pos = core.pos
        last = core.last
        edges = []
        if self.prev_t is not None:
            gap = ts - self.prev_t
            tm = (ts + self.prev_t) / 2 if gap <= MAX_GAP else np.nan
            pm = (pos + self.prev_pos) / 2 if gap <= MAX_GAP else np.nan
            for k, j, b in self.sig:
                v = last.get(b)
                if v is None:
                    continue
                cur, pb = v > 0, self.prev_bit.get(b)
                if pb is not None and cur != pb:
                    edges.append((k, j, 1 if cur else -1, tm, pm))
        for k, j, b in self.sig:
            v = last.get(b)
            if v is not None:
                self.prev_bit[b] = v > 0
        started = any(k == "stop_sol" and d > 0 for k, _, d, _, _ in edges)
        if started:
            if self.cyc is not None:
                self._close_cycle(ts)
            self.cyc = dict(start=ts, run=(last.get(RUN_TAG) or 0) > 0, first={})
            for e in self.prev_edges + edges:
                self.cyc["first"].setdefault(e[:3], e[3:])
        elif self.cyc is not None:
            for e in edges:
                self.cyc["first"].setdefault(e[:3], e[3:])
        self.prev_edges = edges
        self.prev_t, self.prev_pos = ts, pos

    def _close_cycle(self, ts_next):
        c, self.cyc = self.cyc, None
        if not c["run"] or ts_next - c["start"] > MAX_CYCLE:
            return                                        # 운전 중이 아니었거나 중간에 멈춘 사이클
        f = c["first"]
        for name, s_sig, s_dir, e_sig, e_dir, unit_, _ in METRICS:
            n = len(bits(self.unit, e_sig))
            for j in range(n):
                col = name if n == 1 else f"{name}_{j + 1}"
                st, en = f.get((s_sig, 0, s_dir)), f.get((e_sig, j, e_dir))
                if st is None or en is None:
                    continue
                v = (en[0] - st[0]) if unit_ == "s" else (en[1] - st[1])
                if np.isfinite(v) and v >= 0:
                    self.sum[col] += v
                    self.cnt[col] += 1
        self.cycles += 1

    # ------------------------------------------------------------------ #
    # 하루 마감 · 기록
    # ------------------------------------------------------------------ #
    def _day_row(self):
        row = dict(unit=self.unit, day=pd.Timestamp(self.day), cycles=self.cycles)
        row.update({c: (self.sum[c] / self.cnt[c]) if self.cnt[c] else np.nan for c in self.cols})
        return row

    def close_day(self):
        if self.cycles >= MIN_CYCLES_DAY:
            row = self._day_row()
            self.history = [r for r in self.history if r["day"] != row["day"]] + [row]
            self.history = sorted(self.history, key=lambda r: r["day"])[-HISTORY_DAYS:]
            if self.history_path:
                self.save_history(self.history_path)
        self.sum = {c: 0.0 for c in self.cols}
        self.cnt = {c: 0 for c in self.cols}
        self.cycles = 0

    def save_history(self, path):
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump([{**r, "day": r["day"].strftime("%Y-%m-%d")} for r in self.history], f, ensure_ascii=False)
        os.replace(tmp, path)

    def load_history(self, path):
        with open(path, encoding="utf-8") as f:
            self.history = [{**r, "day": pd.Timestamp(r["day"])} for r in json.load(f)]

    def seed_history(self, daily):
        """배치 일별 표(conv_cyl.cyl_daily)로 기록을 채운다 — 처음 배포할 때 한 번."""
        d = daily[daily.unit == self.unit]
        cols = ["unit", "day", "cycles"] + [c for c in self.cols if c in d]
        self.history = [{**r, "day": pd.Timestamp(r["day"])} for r in d[cols].to_dict("records")][-HISTORY_DAYS:]

    # ------------------------------------------------------------------ #
    # 판정
    # ------------------------------------------------------------------ #
    def evaluate(self):
        rows = [r for r in self.history if r["day"] < pd.Timestamp(self.day)] if self.day else list(self.history)
        basis = "전날까지"
        if self.cycles >= MIN_CYCLES_TODAY:
            rows = rows + [self._day_row()]
            basis = "오늘"
        if not rows:
            self.result = self.empty()
            return self.result
        D = pd.DataFrame(rows)
        J = cyl_judge(D[["unit", "day", "cycles"] + [c for c in self.cols if c in D]], refs=UNIT_REF)
        day = J.day.max()
        items, sev = [], 0
        for r in J[J.day == day].itertuples():
            s = item_sev(r.state, r.trend)
            if s > 0:
                # 단기 상태(느려짐·빨라짐·거리 변화)가 없이 걸린 항목은 장기 추세("장기 기준보다 느림")가 상태다
                items.append(dict(metric=r.metric, relay=metric_part(self.unit, r.metric)[2], sev=s,
                                  state=r.state if r.state in STATE_REASON else r.trend,
                                  reason=item_reason(r.metric, r.state, r.trend),
                                  since=self._since(J, r.metric, s)))
                sev = max(sev, s)
        items.sort(key=lambda x: -x["sev"])
        self.result = dict(ready=True, sev=sev, basis=basis, day=day.strftime("%m-%d"),
                           cycles_today=self.cycles, items=items)
        return self.result

    @staticmethod
    def _since(J, metric, sev_now):
        g = J[J.metric == metric].sort_values("day")
        sev = [item_sev(a, b) for a, b in zip(g.state, g.trend)]
        k = len(sev) - 1
        while k > 0 and sev[k - 1] == sev_now:
            k -= 1
        return g.day.iloc[k].strftime("%m-%d") if len(g) else None

    def empty(self):
        return dict(ready=False, sev=None, basis=None, day=None, cycles_today=0, items=[])
