# tools_yeji/relay_models/tank_preprocess.py
"""탱크 실시간 전처리 — 원시 PLC 값을 판정에 쓸 상태로 정리한다. **판정은 하지 않는다.**

원본: DB_Log/HDI_MODEL/Tank_Realtime_Model.py 의 TankRealtime 중 입력·상태 부분.
판정(규칙 ①②③)은 tank_judge.py, 여러 탱크 + 가동 플래그는 tank_realtime.py.

## 하는 일
1. **입력 정리** — TK_Temp_* 는 0.1℃ 단위라 ℃ 로 바꾼다. 이번 tick 에 안 들어온 값은
   마지막 값을 이어서 쓴다. 가동 플래그(Auto_Running_PL)도 입력으로 받아 들고 있다
   (running, None = 아직 못 받음). 판정 쪽은 경보를 띄울지, 추세 쪽은 사이클을 쓸지 이걸로 정한다.
2. **exchanger 사이클** — 꺼진 뒤 `MERGE_GAP_SEC`(90초) 동안은 "꺼짐대기"다. 이 동안은
   켜짐으로 세지 않고, 그 안에 다시 켜지면 같은 사이클로 이어서 센다(꺼져 있던 틈도 포함 —
   배치의 on_duration_sec 과 같은 정의). 90초가 지나면 cycle_end 를 **실제 꺼진 시각**으로
   확정한다. 그래서 경보는 켜져 있는 동안 cycle_start + MAX_ON_SEC 이 지나는 순간 바로 뜨고,
   90초 대기가 경보를 늦추지 않는다.
3. **켜진 도중부터 시작** — 배치는 이 사이클을 버리지만, 실시간은 첫 샘플 시각부터 센다.
   실제 지속시간은 그보다 길 수밖에 없으므로 10분을 넘으면 이상으로 봐도 틀리지 않는다
   (버리면 재시작 직후 고착된 exchanger 를 놓친다).
   **데이터 공백도 재시작과 똑같이 본다.** 샘플이 `GAP_RESET_SEC`(90초)보다 오래 안 들어오면
   (통신 공백, 진단(ANALYZING) 중 예지 미구동) 그 사이 무슨 일이 있었는지 모르므로 사이클 상태를
   버리고 새로 센다. 안 그러면 공백 전에 켜져 있던 정상 사이클이 공백 시간만큼 켜진 것으로 계산돼
   재개 순간 10분 초과로 오판정된다(실측: 51분 공백 뒤 탱크 6개 동시 경보, 2026-10-08).
4. **운영일 최저 PV** — 하루치를 모으지 않고 min(PV - L_Set) 을 변수 하나로 갱신한다.
   하루 경계는 한국시간 오전 6시. L_Set 은 그 순간의 값과 비교한다(하루 중간에 바뀌어도 맞다).
   6시 초기화 때 최저 PV 는 **SV 에서 시작**한다. 글리치(허용범위 밖 PV)는 최저값에
   **절대 넣지 않는다** — 한 번 들어가면 그날 하루가 통째로 오판정되고 되돌릴 수 없다.

## 이번 tick 에 일어난 일 (update 마다 새로 채움)
판정과 추세 수집이 이걸 보고 움직인다.
  opened / turned_off / closed : 사이클 시작 / 꺼짐(꺼짐대기 진입) / 확정(last_cycle 갱신)
  day_rolled                   : 운영일이 바뀜 (last_day 갱신 — 첫 tick 포함)
  gap_reset                    : 데이터 공백이라 사이클 상태를 버리고 새로 시작함
  margin                       : 이번 PV - L_Set. 유효한 PV 와 L_Set 이 둘 다 있을 때만, 아니면 None
"""
import numpy as np

from tools_yeji.common.time_utils import day_key, is_missing
from tools_yeji.relay_models.tank_params import (GAP_RESET_SEC, MERGE_GAP_SEC, PV_VALID_RANGE,
                                                TEMP_SCALE)

OFF, ON, PENDING = "OFF", "ON", "꺼짐대기"


class TankPreprocessor:
    """탱크 1개의 전처리 상태.

    exchanger 상태는 OFF -> ON -> 꺼짐대기 -> (90초 경과) OFF 로 돈다.

        OFF ──(켜짐)──▶ ON ──(꺼짐)──▶ 꺼짐대기 ──(90초 경과)──▶ OFF  [cycle_end = 꺼진 시각]
                         ▲                │
                         └─(90초 안 켜짐)─┘  같은 사이클 계속
    """

    def __init__(self, tank, scale=TEMP_SCALE, valid_range=PV_VALID_RANGE, gap_sec=MERGE_GAP_SEC,
                 gap_reset_sec=GAP_RESET_SEC):
        self.tank = tank
        self.scale = scale
        self.valid_range = valid_range
        self.gap_sec = gap_sec
        self.gap_reset_sec = gap_reset_sec

        # 마지막 입력값 — 이번 tick 에 안 들어온 태그는 이걸 이어서 쓴다
        self.exch = self.cool = self.lset = self.sv = None
        self.running = None         # 가동 플래그. None = 한 번도 못 받음
        self.last_t = None          # 마지막 샘플 시각 — 공백 감지용

        # exchanger 사이클
        self.state = OFF
        self.cycle_start = None     # 사이클 시작. 꺼짐대기 중에도 유지
        self.off_t = None           # 꺼짐대기에 들어간 시각 = 확정되면 cycle_end
        self.truncated = False      # 켜진 도중부터 보기 시작한 사이클인가
        self.running_at_start = None    # 사이클이 열릴 때의 가동 플래그 (추세는 가동 중 시작한 것만 쓴다)
        self.has_cool = False       # cycle_start ~ 마지막 꺼짐 사이에 cool 이 켜졌는가
        self.pending_cool = False   # 꺼짐대기 중에 켜진 cool. 다시 켜지면 합치고, 확정되면 버린다
        self.cool_run = 0           # 확정된 사이클 기준 cool 동반 연속 횟수
        self.last_cycle = None      # 마지막으로 확정된 사이클

        # 운영일(06:00~06:00) 단위 PV 요약
        self.day = None
        self.last_day = None        # 전 운영일 요약
        self._reset_day()

        self._clear_tick()

    # ------------------------------------------------------------------ #
    def update(self, t, exch=None, cool=None, pv=None, lset=None, sv=None, running=None):
        """샘플 1개 반영. t 는 tz 없는 KST Timestamp, 값은 원시 PLC 값."""
        self._clear_tick()
        if self.last_t is not None and (t - self.last_t).total_seconds() > self.gap_reset_sec:
            self._forget_cycle()        # 입력을 받기 전에 — exch 를 비워야 아래 first_exch 가 맞게 잡힌다
        self.last_t = t

        if not is_missing(running):
            self.running = int(running)
        if not is_missing(sv):
            self.sv = float(sv) / self.scale
        if not is_missing(lset):
            self.lset = float(lset) / self.scale
        if not is_missing(cool):
            self.cool = int(cool)
        first_exch = self.exch is None and not is_missing(exch)
        if not is_missing(exch):
            self.exch = int(exch)

        self._roll_day(t)                       # 입력을 먼저 받아야 초기화 때 최신 SV 를 쓴다
        if self.min_pv is None and self.sv is not None:
            self.min_pv = self.sv               # SV 가 초기화 뒤에 처음 들어온 경우

        self._update_exchanger(t, first_exch)
        if not is_missing(pv):
            self._update_pv(t, float(pv) / self.scale)

    def elapsed(self, t):
        """사이클이 켜진 뒤 지난 시간 [초]. 꺼짐대기 중엔 꺼진 시각까지만. 사이클이 없으면 None."""
        if self.cycle_start is None:
            return None
        end = self.off_t if self.state == PENDING else t
        return (end - self.cycle_start).total_seconds()

    def status(self, now=None):
        """현재 상태 요약 (확인용). now 는 KST Timestamp."""
        on_sec = None
        if self.cycle_start is not None and (self.state == PENDING or now is not None):
            on_sec = round(self.elapsed(now), 1)
        return {"tank": self.tank, "state": self.state, "cycle_start": self.cycle_start,
                "on_sec": on_sec, "has_cool": self.has_cool, "cool_run": self.cool_run,
                "day": self.day, "sv": self.sv, "min_pv": self.min_pv, "min_t": self.min_t,
                "min_lset": self.min_lset,
                "min_margin": None if np.isinf(self.min_margin) else round(self.min_margin, 2),
                "n_glitch": self.n_glitch}

    # ------------------------------------------------------------------ #
    def _clear_tick(self):
        self.opened = self.turned_off = self.closed = self.day_rolled = self.gap_reset = False
        self.margin = None

    def _forget_cycle(self):
        """데이터 공백 — 그 사이 exchanger 가 어땠는지 모른다. 재시작과 똑같이 새로 센다.

        진행 중 사이클은 끝을 모르니 확정하지 않고 버린다(last_cycle 안 바뀜). cool 동반 연속
        횟수도 그 사이 몇 사이클이 지났는지 모르니 0 부터. exch / cool 을 비워 두면 다음 샘플이
        1 일 때 '켜진 도중부터'(truncated) 사이클로 열려 그 시각부터 센다.
        운영일 최저 PV · L_Set · SV · 가동 플래그는 실제 샘플로만 갱신되므로 그대로 둔다.
        """
        self.state, self.cycle_start, self.off_t = OFF, None, None
        self.has_cool = self.pending_cool = self.truncated = False
        self.running_at_start = None
        self.cool_run = 0
        self.exch = self.cool = None
        self.gap_reset = True

    # ------------------------------------------------------------------ #
    # exchanger / cool 사이클
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
            self.state, self.off_t = PENDING, t
            self.turned_off = True
        elif self.state == PENDING and e == 1:   # 90초 안에 다시 켜짐 -> 같은 사이클
            self.state, self.off_t = ON, None
            self.has_cool |= self.pending_cool
            self.pending_cool = False

    def _open_cycle(self, t, truncated=False):
        self.truncated = truncated
        self.running_at_start = self.running
        self.state, self.cycle_start, self.off_t = ON, t, None
        self.has_cool = self.cool == 1
        self.pending_cool = False
        self.opened = True

    def _close_cycle(self):
        self.cool_run = self.cool_run + 1 if self.has_cool else 0
        self.last_cycle = {"cycle_start": self.cycle_start, "cycle_end": self.off_t,
                           "on_duration_sec": (self.off_t - self.cycle_start).total_seconds(),
                           "cool_overlap": self.has_cool, "truncated": self.truncated,
                           "running_at_start": self.running_at_start}
        self.state, self.cycle_start, self.off_t = OFF, None, None
        self.has_cool = self.pending_cool = self.truncated = False
        self.running_at_start = None
        self.closed = True

    # ------------------------------------------------------------------ #
    # PV — 운영일 최저값과 이번 margin
    # ------------------------------------------------------------------ #
    def _reset_day(self):
        # 최저 PV 는 SV 에서 시작한다 — 전날 최저값이 SV 보다 낮게 남아 있어도 SV 로 올려서
        # 새로 센다. SV 를 아직 모르면(시작 직후) 첫 SV 가 들어올 때 채운다(update 참고).
        self.min_pv = self.sv
        self.min_lset = None
        self.min_t = None           # None = 오늘 PV 가 아직 SV 밑으로 안 내려감
        self.min_margin = np.inf    # 오늘 min(PV - L_Set). 실제 샘플로만 갱신한다
        self.n_glitch = 0

    def _update_pv(self, t, pv):
        lo, hi = self.valid_range
        if not (lo <= pv <= hi):    # 글리치는 최저값에 절대 넣지 않는다
            self.n_glitch += 1
            return
        if self.min_pv is None or pv < self.min_pv:
            self.min_pv, self.min_lset, self.min_t = pv, self.lset, t
        if self.lset is None:
            return
        self.margin = pv - self.lset
        self.min_margin = min(self.min_margin, self.margin)

    def _roll_day(self, t):
        """오전 6시를 넘으면 전 운영일을 요약해 두고 새로 센다."""
        d = day_key(t)
        if d == self.day:
            return
        if self.day is not None:
            self.last_day = {"day": self.day, "min_pv": self.min_pv, "min_lset": self.min_lset,
                             "min_t": self.min_t, "n_glitch": self.n_glitch,
                             "heat_abnormal": bool(self.min_margin < 0)}
        self.day = d
        self._reset_day()
        self.day_rolled = True
