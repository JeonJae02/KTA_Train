"""탱크 heat / exchanger / cool 실시간 진단.

`Tank_Diagnosis_Model.py` 는 기간을 정해 데이터를 한꺼번에 받아 판정하는 배치용이다.
이 파일은 같은 룰 3가지를 **샘플이 들어올 때마다** 판정한다. 하루치 데이터를 들고
있지 않고, 탱크마다 숫자 몇 개짜리 상태만 갖고 간다.

사용법 (`check_running.py` 와 같은 방식 — 반복문 밖에서 객체 생성, 안에서 update 만):

    mon = TankRealtimeGroup()                       # 반복문 밖에서 한 번
    while True:
        row = {...}                                 # 태그명 -> 원시 PLC 값 (이번 tick 에 들어온 것만)
        result = mon.update(current_time, row)      # {"relay_warning": {...}}

값은 **원시 PLC 값**(TK_Temp_* 는 0.1℃ 단위)을 그대로 넣는다. 이번 tick 에 안 들어온
태그는 빼거나 None 으로 두면 마지막 값을 이어서 쓴다.

## 배치 모델과 다른 점

1. **exchanger 사이클 끝 처리** — 꺼진 뒤 `MERGE_GAP_SEC`(90초) 동안은 "꺼짐대기"
   상태다. 이 동안은 시간을 켜짐으로 세지 않고, 그 안에 다시 켜지면 같은 사이클로
   이어서 센다(꺼져 있던 틈도 포함 — 배치의 on_duration_sec 과 같은 정의).
   90초가 지나면 cycle_end 를 **실제 꺼진 시각**으로 확정한다.
   경보는 켜져 있는 동안 cycle_start + MAX_ON_SEC 이 지나는 순간 바로 뜨므로
   90초 대기가 경보를 늦추지 않는다. 늦어지는 건 "10분 전에 꺼졌다가 10분 넘어서
   다시 켜진" 경우뿐인데, 이건 다시 켜지는 순간에야 알 수 있는 정보다.
2. **일별 최저 PV** — 하루치를 모으지 않고 min(PV - L_Set) 을 변수 하나로 갱신한다.
   하루 경계는 **한국시간 오전 6시**. L_Set 은 그날 중앙값 대신 그 순간의 값과 비교한다
   (하루 중간에 L_Set 이 바뀌어도 맞게 판정된다). PV 가 L_Set 밑으로 내려가는 순간
   그날 판정이 확정되므로 다음날까지 기다리지 않고 바로 경보를 낸다.
   오전 6시에 초기화할 때 최저 PV 는 **SV 에서 시작**한다(전날 최저값이 SV 보다 낮게 남아
   있어도 SV 로 올려서 새로 센다). heat 판정은 실제 PV 샘플로만 하므로 시작값이 판정을
   바꾸지는 않는다.
   글리치(허용범위 밖 PV)는 최저값에 **절대 넣지 않는다** — 한 번 들어가면 그날 하루가
   통째로 오판정되고 되돌릴 수 없다.
3. **켜진 도중부터 시작** — 배치는 이 사이클을 버리지만, 실시간은 첫 샘플 시각부터 센다.
   실제 지속시간은 그보다 길 수밖에 없으므로 10분을 넘으면 이상으로 봐도 틀리지 않는다
   (버리면 재시작 직후 고착된 exchanger 를 놓친다).

## 경보 이벤트와 유지

모델(`TankRealtime`)은 가동 여부와 상관없이 **항상 돌고**, 이상이 생길 때마다
`new_events` 에 이벤트를 낸다. 같은 이상은 한 번만 낸다 — exchanger/cooler/성능저하는
사이클당 한 번, heater 는 PV < L_Set 구간당 한 번(6시에 새로 센다).

## 가동 플래그 — 띄울지 말지는 최상위에서

`TankRealtimeGroup` 이 이벤트를 받아, 그 순간 `Auto_Running_PL`(PLC 자동운전 표시등)이
1 일 때만 띄운다. 띄운 경보는 **다음 오전 6시까지 유지**한다.

  - exchanger / cooler / 성능저하 -> 가동 중일 때만 띄운다. 실측(9/9~10/6, 8탱크)에서
    10분 초과 126건 중 103건이 비가동 중이었고, 대부분 생산 시작(06:50) 전 05~06시
    예열 구간이었다.
  - heater -> 항상 띄운다. 탱크 온도가 L_Set 밑으로 가는 건 가동과 무관하게 이상이다.

플래그는 원래 값 그대로 쓴다. 자동운전 시작 순간 1~2초 깜빡임이 있지만, 120초 유지
처리를 넣어도 결과가 거의 같았다(이상 24건 vs 23건).

`TankRealtime.warning()` 은 플래그를 거치지 않은 모델 자체 판정이다(확인·비교용).
"""

from collections import deque

import numpy as np
import pandas as pd

from Tank_Preprocess import MERGE_GAP_SEC, PV_VALID_RANGE, TANKS, TEMP_SCALE
from Tank_Diagnosis_Model import (MESSAGES, RELAY_IDS, WARNING_TYPE, _MESSAGE_PRIORITY,
                                  params)

# --------------------------------------------------------------------------- #
# 실시간 파라미터
# --------------------------------------------------------------------------- #
KST = "Asia/Seoul"
DAY_START_HOUR = 6      #: 하루 경계(한국시간). heat 최저값과 경보를 이때 초기화한다
HEAT_CONFIRM_SEC = 0    #: PV < L_Set 이 이만큼 이어져야 heat 이상. 0 이면 한 샘플로 확정(배치와 동일)

RUN_FLAG_TAG = "Auto_Running_PL"    #: 가동 플래그. 1 일 때만 GATED_REASONS 경보를 띄운다
GATED_REASONS = {"exchanger", "cooler", "exchanger_degrade"}   #: heater 는 항상 띄운다

OFF, ON, PENDING = "OFF", "ON", "꺼짐대기"


def to_kst(t):
    """시각 -> tz 없는 한국시간 Timestamp. epoch 초(float) 나 tz 붙은 시각도 받는다.

    tz 없는 시각은 이미 한국시간으로 본다(Log_Extractor 가 내주는 형식).
    """
    if isinstance(t, (int, float, np.integer, np.floating)):
        return pd.Timestamp(t, unit="s", tz="UTC").tz_convert(KST).tz_localize(None)
    t = pd.Timestamp(t)
    if t.tzinfo is not None:
        t = t.tz_convert(KST).tz_localize(None)
    return t


def sv_tag(tank):
    """탱크 온도 SV 태그. TK_Temp_* 라 0.1℃ 단위 (P1 실측 230 = 23.0℃)."""
    return f"TK_Temp_SV_{tank}"


def day_key(t):
    """오전 6시 경계 기준 '운영일'. 06:00 전은 전날로 친다."""
    return (t - pd.Timedelta(hours=DAY_START_HOUR)).date()


def _missing(v):
    return v is None or pd.isna(v)


def build_warning(tank, reasons):
    """{part: 사유 집합} -> relay_warning dict. 같은 ID 에 사유가 겹치면 우선순위로 하나만."""
    out = {}
    for part, rs in reasons.items():
        reason = next(r for r in _MESSAGE_PRIORITY if r in rs)
        out[RELAY_IDS[part][tank]] = {"type": WARNING_TYPE, "message": MESSAGES[reason]}
    return out


# --------------------------------------------------------------------------- #
# 탱크 1개
# --------------------------------------------------------------------------- #

class TankRealtime:
    """탱크 1개의 실시간 진단 상태.

    exchanger 상태는 OFF -> ON -> 꺼짐대기 -> (90초 경과) OFF 로 돈다.

        OFF ──(켜짐)──▶ ON ──(꺼짐)──▶ 꺼짐대기 ──(90초 경과)──▶ OFF  [cycle_end = 꺼진 시각]
                         ▲                │
                         └─(90초 안 켜짐)─┘  같은 사이클 계속
    """

    def __init__(self, tank, scale=TEMP_SCALE, valid_range=PV_VALID_RANGE,
                 gap_sec=MERGE_GAP_SEC, heat_confirm_sec=HEAT_CONFIRM_SEC):
        p = params(tank)
        self.tank = tank
        self.max_on = p["MAX_ON_SEC"]
        self.consec_n = p["CONSEC_COOL_N"]
        self.scale = scale
        self.valid_range = valid_range
        self.gap_sec = gap_sec
        self.heat_confirm_sec = heat_confirm_sec

        # 마지막 입력값 — 이번 tick 에 안 들어온 태그는 이걸 이어서 쓴다
        self.exch = self.cool = self.lset = self.sv = None

        # exchanger 사이클
        self.state = OFF
        self.cycle_start = None     # 사이클 시작. 꺼짐대기 중에도 유지
        self.off_t = None           # 꺼짐대기에 들어간 시각 = 확정되면 cycle_end
        self.truncated = False      # 켜진 도중부터 보기 시작한 사이클인가
        self.has_cool = False       # cycle_start ~ 마지막 꺼짐 사이에 cool 이 켜졌는가
        self.pending_cool = False   # 꺼짐대기 중에 켜진 cool. 다시 켜지면 합치고, 확정되면 버린다
        self.cool_run = 0           # 확정된 사이클 기준 cool 동반 연속 횟수 (규칙 ③)
        self.last_cycle = None      # 마지막으로 확정된 사이클

        # heat — 운영일(06:00~06:00) 단위
        self.day = None
        self.last_day = None        # 전 운영일 heat 요약
        self._reset_heat()

        # 경보
        self.reasons = {}                 # part -> 사유 집합. 플래그 안 거친 모델 판정, 다음 6시까지
        self.new_events = []              # 이번 update 에서 새로 난 이상 [(시각, part, 사유)]
        self.events = deque(maxlen=200)   # 최근 이벤트 기록 (확인용)
        self._cycle_raised = set()        # 이번 사이클에서 이미 낸 사유
        self._heat_raised = set()         # 이번 PV < L_Set 구간에서 이미 낸 사유

    # ------------------------------------------------------------------ #
    def update(self, t, exch=None, cool=None, pv=None, lset=None, sv=None):
        """샘플 1개 반영. 값은 원시 PLC 값.

        반환: {"relay_warning": {...}} — 플래그를 거치지 않은 모델 판정.
        이번에 새로 난 이상은 `self.new_events` 에 담긴다.
        """
        t = to_kst(t)
        self.new_events = []

        if not _missing(sv):
            self.sv = float(sv) / self.scale
        if not _missing(lset):
            self.lset = float(lset) / self.scale
        if not _missing(cool):
            self.cool = int(cool)
        first_exch = self.exch is None and not _missing(exch)
        if not _missing(exch):
            self.exch = int(exch)

        self._roll_day(t)                       # 입력을 먼저 받아야 초기화 때 최신 SV 를 쓴다
        if self.min_pv is None and self.sv is not None:
            self.min_pv = self.sv               # SV 가 초기화 뒤에 처음 들어온 경우

        self._update_exchanger(t, first_exch)
        if not _missing(pv):
            self._update_heat(t, float(pv) / self.scale)
        return {"relay_warning": self.warning()}

    def warning(self):
        """플래그를 거치지 않은 모델 판정 -> relay_warning dict."""
        return build_warning(self.tank, self.reasons)

    def status(self, now=None):
        """현재 상태 요약 (확인용)."""
        on_sec = None
        if self.cycle_start is not None:
            end = self.off_t if self.state == PENDING else (to_kst(now) if now is not None else None)
            if end is not None:
                on_sec = round((end - self.cycle_start).total_seconds(), 1)
        return {"tank": self.tank, "state": self.state, "cycle_start": self.cycle_start,
                "on_sec": on_sec, "has_cool": self.has_cool, "cool_run": self.cool_run,
                "day": self.day, "sv": self.sv, "min_pv": self.min_pv, "min_t": self.min_t,
                "min_lset": self.min_lset,
                "min_margin": None if np.isinf(self.min_margin) else round(self.min_margin, 2),
                "n_glitch": self.n_glitch, "reasons": {k: sorted(v) for k, v in self.reasons.items()}}

    # ------------------------------------------------------------------ #
    # 규칙 ② ③ exchanger / cool
    # ------------------------------------------------------------------ #
    def _update_exchanger(self, t, first_exch=False):
        # 1. 꺼짐대기가 90초를 넘겼으면 사이클 확정 (exch 샘플이 없는 tick 에도 해야 한다)
        if self.state == PENDING and (t - self.off_t).total_seconds() >= self.gap_sec:
            self._close_cycle()

        # 2. cool 이 켜져 있으면 지금 상태에 따라 반영
        if self.cool == 1:
            if self.state == ON:
                self.has_cool = True
            elif self.state == PENDING:
                self.pending_cool = True

        # 3. exchanger 전환
        e = self.exch
        if self.state == OFF and e == 1:
            self._open_cycle(t, truncated=first_exch)   # 첫 샘플부터 1 = 켜진 도중부터 봄
        elif self.state == ON and e == 0:
            self._check_on_time(t)               # 꺼지는 순간까지의 시간도 판정
            self.state, self.off_t = PENDING, t
        elif self.state == PENDING and e == 1:   # 90초 안에 다시 켜짐 -> 같은 사이클
            self.state, self.off_t = ON, None
            self.has_cool |= self.pending_cool
            self.pending_cool = False

        # 4. 켜져 있는 동안 판정
        if self.state == ON:
            self._check_on_time(t)
        if self.state != OFF:
            self._check_cool_run(t)

    def _open_cycle(self, t, truncated=False):
        self.truncated = truncated
        self.state, self.cycle_start, self.off_t = ON, t, None
        self.has_cool = self.cool == 1
        self.pending_cool = False
        self._cycle_raised = set()

    def _close_cycle(self):
        dur = (self.off_t - self.cycle_start).total_seconds()
        self.cool_run = self.cool_run + 1 if self.has_cool else 0
        self.last_cycle = {"cycle_start": self.cycle_start, "cycle_end": self.off_t,
                           "on_duration_sec": dur, "cool_overlap": self.has_cool,
                           "abnormal": dur > self.max_on, "truncated": self.truncated}
        self.state, self.cycle_start, self.off_t = OFF, None, None
        self.has_cool = self.pending_cool = self.truncated = False

    def _check_on_time(self, t):
        """규칙 ② — 켜진 뒤 MAX_ON_SEC 이 지나면 즉시 이상."""
        if (t - self.cycle_start).total_seconds() > self.max_on:
            self._raise("exchanger", "exchanger", t, self._cycle_raised)
            if self.has_cool:   # 둘이 같이 돌았는데도 실패 -> 양쪽 다
                self._raise("cooler", "cooler", t, self._cycle_raised)

    def _check_cool_run(self, t):
        """규칙 ③ — 진행 중 사이클까지 쳐서 cool 동반이 N회 연속이면 성능저하."""
        if self.cool_run + int(self.has_cool) >= self.consec_n:
            self._raise("exchanger", "exchanger_degrade", t, self._cycle_raised)

    # ------------------------------------------------------------------ #
    # 규칙 ① heat
    # ------------------------------------------------------------------ #
    def _reset_heat(self):
        # 최저 PV 는 SV 에서 시작한다 — 전날 최저값이 SV 보다 낮게 남아 있어도 SV 로 올려서
        # 새로 센다. SV 를 아직 모르면(시작 직후) 첫 SV 가 들어올 때 채운다(update 참고).
        self.min_pv = self.sv
        self.min_lset = None
        self.min_t = None           # None = 오늘 PV 가 아직 SV 밑으로 안 내려감
        self.min_margin = np.inf    # 오늘 min(PV - L_Set). heat 판정은 실제 샘플로만 한다
        self.below_since = None     # PV < L_Set 이 시작된 시각
        self._heat_raised = set()   # 6시 이후에도 L_Set 밑이면 새 운영일 이상으로 다시 낸다
        self.n_glitch = 0

    def _update_heat(self, t, pv):
        lo, hi = self.valid_range
        if not (lo <= pv <= hi):    # 글리치는 최저값에 절대 넣지 않는다
            self.n_glitch += 1
            return
        if self.min_pv is None or pv < self.min_pv:
            self.min_pv, self.min_lset, self.min_t = pv, self.lset, t
        if self.lset is None:
            return

        margin = pv - self.lset
        self.min_margin = min(self.min_margin, margin)

        if margin < 0:
            if self.below_since is None:
                self.below_since = t
            if (t - self.below_since).total_seconds() >= self.heat_confirm_sec:
                self._raise("heater", "heater", t, self._heat_raised)
        else:
            self.below_since = None
            self._heat_raised = set()

    def _roll_day(self, t):
        """오전 6시를 넘으면 heat 최저값과 경보를 초기화한다."""
        d = day_key(t)
        if d == self.day:
            return
        if self.day is not None:
            self.last_day = {"day": self.day, "min_pv": self.min_pv, "min_lset": self.min_lset,
                             "min_t": self.min_t, "n_glitch": self.n_glitch,
                             "heat_abnormal": bool(self.min_margin < 0)}
        self.day = d
        self._reset_heat()
        self.reasons = {}

    # ------------------------------------------------------------------ #
    def _raise(self, part, reason, t, seen):
        """seen : 같은 이상을 한 번만 내기 위한 집합 (사이클당 / heat 구간당)."""
        self.reasons.setdefault(part, set()).add(reason)
        if reason not in seen:
            seen.add(reason)
            self.new_events.append((t, part, reason))
            self.events.append((t, reason))


# --------------------------------------------------------------------------- #
# 여러 탱크
# --------------------------------------------------------------------------- #

class TankRealtimeGroup:
    """탱크 여러 개 + 가동 플래그. 최종 relay_warning 은 여기서 나간다.

    row 는 {태그명: 원시 PLC 값} — Tank_Preprocess.TANKS 의 태그명 + SV 태그(`sv_tag()`)
    + 가동 플래그(`RUN_FLAG_TAG`).

    모델은 플래그와 상관없이 돌고, 여기서는 새로 난 이상을 **그 순간 플래그가 1 일 때만**
    띄운다(GATED_REASONS 만. heater 는 항상). 띄운 경보는 다음 오전 6시까지 유지한다.
    플래그가 아직 한 번도 안 들어왔으면 가동 중으로 보고 띄운다 — 태그가 빠졌을 때 경보가
    조용히 사라지는 것보다 낫다.
    """

    def __init__(self, tanks=None, run_flag_tag=RUN_FLAG_TAG, **kw):
        self.monitors = {t: TankRealtime(t, **kw) for t in (tanks or TANKS)}
        self.run_flag_tag = run_flag_tag
        self.running = None
        self.day = None
        self.shown = {}                      # tank -> {part: 사유 집합}. 다음 6시까지
        self.suppressed = deque(maxlen=200)  # 비가동이라 안 띄운 이벤트 (확인용)

    def update(self, current_time, row):
        t = to_kst(current_time)
        flag = row.get(self.run_flag_tag)
        if not _missing(flag):
            self.running = int(flag)
        d = day_key(t)
        if d != self.day:
            self.day, self.shown = d, {}

        for tank, m in self.monitors.items():
            tags = TANKS[tank]
            m.update(t, exch=row.get(tags["exch"]), cool=row.get(tags["cool"]),
                     pv=row.get(tags["pv"]), lset=row.get(tags["lset"]),
                     sv=row.get(sv_tag(tank)))
            for et, part, reason in m.new_events:
                if reason in GATED_REASONS and self.running == 0:
                    self.suppressed.append((et, tank, reason))
                    continue
                self.shown.setdefault(tank, {}).setdefault(part, set()).add(reason)
        return {"relay_warning": self.warning()}

    def warning(self):
        merged = {}
        for tank, reasons in self.shown.items():
            merged.update(build_warning(tank, reasons))
        return merged

    def status(self, now=None):
        return {t: m.status(now) for t, m in self.monitors.items()}


# --------------------------------------------------------------------------- #
# 과거 데이터로 돌려보기
# --------------------------------------------------------------------------- #

def replay(tank, sig):
    """과거 원시 신호를 시간순으로 한 샘플씩 흘려 실시간 판정을 재현한다.

    sig : dict(exch, cool, pv, lset[, sv]) — Tank_Preprocess.load_signals 결과 형식.
          sv 가 없으면 최저 PV 를 SV 로 시작하지 않는다(판정 결과는 같다)
    반환 : (TankRealtime, 확정된 사이클 DataFrame)
    """
    frames = [s.rename(k) for k, s in sig.items()]
    df = pd.concat(frames, axis=1).sort_index()
    if "sv" not in df:
        df["sv"] = np.nan
    m, cycles = TankRealtime(tank), []
    for t, r in zip(df.index, df.itertuples(index=False)):
        prev = m.last_cycle
        m.update(t, exch=r.exch, cool=r.cool, pv=r.pv, lset=r.lset, sv=r.sv)
        if m.last_cycle is not prev:
            cycles.append(m.last_cycle)
    return m, pd.DataFrame(cycles)


if __name__ == "__main__":
    import json
    from Tank_Preprocess import load_signals

    sig = load_signals("P1", "2026-09-09 10:00:00", "now()")
    mon, cyc = replay("P1", sig)
    print(f"[P1] 확정 사이클 {len(cyc):,}개, 10분 초과 {int(cyc['abnormal'].sum())}개")
    print("최근 경보:", list(mon.events)[-5:])
    print(json.dumps({"relay_warning": mon.warning()}, ensure_ascii=False, indent=2))
