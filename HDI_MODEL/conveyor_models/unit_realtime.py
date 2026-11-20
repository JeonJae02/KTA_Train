"""[유닛 · 전처리 + 실시간 판정] 에어 주입 유닛 하나 — UnitRealtime. 전진·후진·에어백이 같은 클래스, 설정만 다르다.

ConveyorRealtimeGroup 가 tick 마다 CoreRealtime.update 다음에 update(t, core) 를 부른다.
유닛은 코어에서 두 가지만 빌린다: 마지막 태그 값(core.last), 체인 위치(core.pos — 거리 지표용).

## 전처리 (tick 마다) — conv_cyl.cycle_table 과 같은 계산을 한 tick 씩

1. 신호가 바뀐 시각 = 바뀌기 전 샘플과 바뀐 샘플의 중간 (위치도 같은 방식).
   샘플 간격이 MAX_GAP(0.75초) 보다 길면 그 변화의 시각을 버린다 (메시지가 빠진 구간 — 시간이 길게 치우친다).
   왜 P 비트끼리: DB 수집기가 입력·출력(P)을 한 순간에 읽고 카운터는 0.26초 뒤에 읽는다.
2. 사이클 = 스톱퍼 SOL ON 부터 다음 스톱퍼 SOL ON 전까지. 사이클 시작 바로 전 샘플의 변화도 포함한다 (배치와 같음).
3. 사이클이 끝나면 지표마다 "시작 신호 첫 변화 → 끝 신호 첫 변화" 시간(초) 또는 거리(카운트)를 잰다.
   운전 중이 아니었거나 30초 넘게 걸린 사이클(중간에 멈춤)은 버린다.
4. 지표를 오늘 합계와 근무 구간 합계에 더한다. 사이클 하나는 0.5초 샘플이라 0 또는 0.5 로만 나오므로, **하루치 평균**으로 본다.

## 판정 (evaluate) — 최근 근무 구간 8개(= 하루치) 슬라이딩

근무 구간은 공유 근무표(operation.json shifts, 8개)다. 구간이 끝날 때마다 그 구간 합계를 최근 ROLL_BLOCKS 개 창에 넣는다
(휴게 중 사이클은 앞 구간, 사이클 BLOCK_MIN_CYCLES 미만 구간 — 주말·정지 — 은 넣지 않는다).
판정: 마감된 가동일 기록 + "최근 8구간 평균" 한 줄 → unit_judge.cyl_judge (UNIT_REF 의 장기 기준·σ 로) → 지표별 상태.
  왜 8구간: 하루치라 σ(하루 평균 기준)를 그대로 쓰고, 시간대 구성이 늘 같아 아침 편향이 없다.
  큰 변화는 창의 일부만 바뀌어도 문턱을 넘어 몇 시간 안에 뜨고, 문턱 근처 변화는 창이 찰 때(약 하루)까지 모인다.
  예전 "오늘 평균"(2,000사이클 넘으면)은 아침 구간이 많이 섞여 오경보가 났다 (C-07).
깜빡임 방지: 직전 판정에서 걸린 지표는 |z| 가 Z_CLEAR(4) 아래로 내려가야 푼다 (켜짐은 5).
창이 아직 안 찼으면(처음 배포, 근무표 없음) 마감된 가동일 기록만 판정한다.
PLC 에러·알람은 다루지 않는다 — 정상 가동 중 동작 시간의 변화만 본다.
하루 마감(오전 6시): 그날 평균을 기록에 더하고 저장한다. 승인(3 가동일)·장기 추세는 하루 기록으로 본다.

출력 (result): {ready, sev, basis, day, cycles_today, items: [{metric, relay, sev, state, reason, since}]}
  items 는 sev > 0 인 지표만. relay 는 그 판정이 뜨는 부위 주소(unit_params.metric_part), 거리·에어 주입 시간 지표는 None.
  판정 전이면 ready=False, sev=None.
"""

import json
import os
from collections import deque
from datetime import timedelta

import numpy as np
import pandas as pd

from config.operation_config import SHIFTS

from .core_params import RUN_TAG
from .unit_judge import STATE_REASON, cyl_judge, item_reason, item_sev
from .unit_params import MAX_CYCLE, MAX_GAP, METRICS, UNIT_REF, bits, metric_columns, metric_part

MIN_CYCLES_DAY = 1000     #: 하루 마감 때 이보다 적은 날은 기록하지 않는다 (배치와 같음)
HISTORY_DAYS = 365        #: 하루 한 줄이라 길게 둔다 (승인된 변화가 기록에서 빠지지 않게)
ROLL_BLOCKS = 8           #: 판정 창 = 최근 근무 구간 8개 (근무표 8구간 = 하루치)
BLOCK_MIN_CYCLES = 200    #: 이보다 적은 구간(주말·정지·짧은 가동)은 창에 넣지 않는다
Z_CLEAR = 4.0             #: 걸렸던 지표는 |z| 가 이 아래로 내려가야 푼다 (켜짐은 unit_judge.Z_TOL 5)
#: 근무 구간 시작 시각 (자정부터). 구간 = 이 시각부터 다음 시작 전까지 — 휴게 중 사이클은 앞 구간에 들어간다
BLOCK_STARTS = sorted(pd.Timedelta(hours=int(s[:2]), minutes=int(s[3:5])) for s, _ in SHIFTS)


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
        # 근무 구간 창 — 구간 합계 (재시작해도 창이 이어지게 파일로 남긴다)
        self.block = None                    # 지금 구간 시작 시각
        self.bsum = {c: 0.0 for c in self.cols}
        self.bcnt = {c: 0 for c in self.cols}
        self.bcycles = 0
        self.blocks = deque(maxlen=ROLL_BLOCKS)   # [{start, cycles, sum, cnt}]
        self.blocks_path = history_path.replace("_history.json", "_blocks.json") if history_path else None
        self.flagged = {}                    # 직전 판정에서 걸린 지표 → (item, z) — 깜빡임 방지
        self.result = self.empty()
        if history_path and os.path.exists(history_path):
            self.load_history(history_path)
        if self.blocks_path and os.path.exists(self.blocks_path):
            self.load_blocks(self.blocks_path)

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
        bk = self._block_start(t)
        if self.block is None:
            self.block = bk
        elif bk is not None and bk != self.block:
            self._close_block()
            self.block = bk
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
                    self.bsum[col] += v
                    self.bcnt[col] += 1
        self.cycles += 1
        self.bcycles += 1

    # ------------------------------------------------------------------ #
    # 근무 구간 창
    # ------------------------------------------------------------------ #
    @staticmethod
    def _block_start(t):
        """t 가 속한 근무 구간의 시작 시각. 근무표가 없으면 None (창을 쓰지 않는다)."""
        if not BLOCK_STARTS:
            return None
        d = pd.Timestamp(t).normalize()
        past = [d + s for s in BLOCK_STARTS if d + s <= t]
        return past[-1] if past else d - pd.Timedelta(days=1) + BLOCK_STARTS[-1]

    def _close_block(self):
        if self.bcycles >= BLOCK_MIN_CYCLES:
            self.blocks.append(dict(start=self.block.isoformat(), cycles=self.bcycles,
                                    sum=dict(self.bsum), cnt=dict(self.bcnt)))
            if self.blocks_path:
                self.save_blocks(self.blocks_path)
        self.bsum = {c: 0.0 for c in self.cols}
        self.bcnt = {c: 0 for c in self.cols}
        self.bcycles = 0

    def _roll_row(self):
        """최근 ROLL_BLOCKS 구간 평균 한 줄 (판정용, 기록에는 안 들어간다). 창이 덜 찼으면 None."""
        if len(self.blocks) < ROLL_BLOCKS:
            return None
        row = dict(unit=self.unit, day=pd.Timestamp(self.day), cycles=sum(b["cycles"] for b in self.blocks))
        for c in self.cols:
            n = sum(b["cnt"].get(c, 0) for b in self.blocks)
            row[c] = sum(b["sum"].get(c, 0.0) for b in self.blocks) / n if n else np.nan
        return row

    def save_blocks(self, path):
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(list(self.blocks), f, ensure_ascii=False)
        os.replace(tmp, path)

    def load_blocks(self, path):
        with open(path, encoding="utf-8") as f:
            self.blocks = deque(json.load(f), maxlen=ROLL_BLOCKS)

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
        roll = self._roll_row()
        if roll is not None:
            rows = rows + [roll]
            basis = f"최근 근무 {ROLL_BLOCKS}구간"
        if not rows:
            self.result = self.empty()
            return self.result
        D = pd.DataFrame(rows)
        J = cyl_judge(D[["unit", "day", "cycles"] + [c for c in self.cols if c in D]], refs=UNIT_REF)
        day = J.day.max()
        items, sev, flagged = [], 0, {}
        for r in J[J.day == day].itertuples():
            s = item_sev(r.state, r.trend)
            if s > 0:
                # 단기 상태(느려짐·빨라짐·거리 변화)가 없이 걸린 항목은 장기 추세("장기 기준보다 느림")가 상태다
                it = dict(metric=r.metric, relay=metric_part(self.unit, r.metric)[2], sev=s,
                          state=r.state if r.state in STATE_REASON else r.trend,
                          reason=item_reason(r.metric, r.state, r.trend),
                          since=self._since(J, r.metric, s))
            elif r.metric in self.flagged and np.isfinite(r.z) and abs(r.z) >= Z_CLEAR \
                    and np.sign(r.z) == np.sign(self.flagged[r.metric][1]):
                it = self.flagged[r.metric][0]            # 문턱 바로 아래(4 ≤ |z| < 5) — 직전 경고를 유지한다
            else:
                continue
            items.append(it)
            sev = max(sev, it["sev"])
            if it["state"] in STATE_REASON:
                flagged[r.metric] = (it, r.z)
        self.flagged = flagged
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
