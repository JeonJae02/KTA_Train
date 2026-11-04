# tools_yeji/relay_models/tank_judge.py
"""탱크 실시간 판정 — 전처리(TankPreprocessor) 상태를 읽어 규칙 3가지를 본다.

원본: DB_Log/HDI_MODEL/Tank_Realtime_Model.py 의 TankRealtime 중 판정 부분.
가동 여부와 상관없이 **항상 돈다.** 띄울지 말지는 tank_realtime.TankRealtimeGroup 이 정한다.

## 규칙
- ① heater          : PV < L_Set 이 HEAT_CONFIRM_SEC 이어짐 (0 이면 한 샘플로 확정)
- ② exchanger       : 켜진 뒤 MAX_ON_SEC 초과. cool 이 같이 켜졌으면 cooler 도
                      (켜져 있는 동안 + 꺼지는 순간에만 본다. 꺼짐대기는 켜짐으로 안 센다)
- ③ exchanger_degrade : 진행 중 사이클까지 쳐서 cool 동반이 CONSEC_COOL_N 회 연속

## 같은 이상은 한 번만 (new_events)
- exchanger / cooler / 성능저하 -> 사이클당 한 번 (6시에 새로 세지 않는다 — 6시에 걸친
  사이클이 다시 뜨지 않게)
- heater -> PV < L_Set 구간당 한 번. 6시에 새로 센다
아침 예열 때 막힌 경보 때문에 같은 날 가동 중 진짜 경보가 묻히지 않게 하루 단위가 아니라
이 단위로 낸다.

`asserted` 는 이번 tick 에 조건이 성립한 사유 전부다(새로 난 것 + 계속 성립 중인 것).
이미 띄운 경보를 이상이 이어지는 동안 계속 유지하는 데 쓴다.

`reasons` 는 플래그를 거치지 않은 모델 자체 판정이다(다음 6시까지, 확인·비교용).
"""
from collections import deque

from tools_yeji.relay_models.tank_params import (HEAT_CONFIRM_SEC, MESSAGES, RELAY_IDS,
                                                WARNING_TYPE, _MESSAGE_PRIORITY, params)
from tools_yeji.relay_models.tank_preprocess import OFF, ON


def build_warning(tank, reasons):
    """{part: 사유 집합} -> relay_warning dict. 같은 ID 에 사유가 겹치면 우선순위로 하나만."""
    out = {}
    for part, rs in reasons.items():
        reason = next(r for r in _MESSAGE_PRIORITY if r in rs)
        out[RELAY_IDS[part][tank]] = {"type": WARNING_TYPE, "message": MESSAGES[reason]}
    return out


def build_alert(tank, part, reason, show):
    """Relay_Monitor 로 넘기는 경보 1건 (헤드와 같은 형식).

    show     : True = 띄우기 시작해도 됨(새로 났고 가동 플래그도 통과) / False = 이미 떠 있으면 유지만
    priority : 같은 relay 에 사유가 겹칠 때 작은 값이 이긴다 (_MESSAGE_PRIORITY 순서)
    """
    return {"relay": RELAY_IDS[part][tank], "reason": reason,
            "priority": _MESSAGE_PRIORITY.index(reason),
            "type": WARNING_TYPE, "message": MESSAGES[reason], "show": show}


class TankJudge:
    """탱크 1개의 판정 상태. 매 tick 전처리 update 뒤에 update(t, pre) 를 부른다."""

    def __init__(self, tank, heat_confirm_sec=HEAT_CONFIRM_SEC):
        p = params(tank)
        self.tank = tank
        self.max_on = p["MAX_ON_SEC"]
        self.consec_n = p["CONSEC_COOL_N"]
        self.heat_confirm_sec = heat_confirm_sec

        self.reasons = {}                 # part -> 사유 집합. 다음 6시까지
        self.new_events = []              # 이번 update 에서 새로 난 이상 [(시각, part, 사유)]
        self.asserted = []                # 이번 update 에서 조건이 성립한 사유 전부 [(part, 사유)]
        self.events = deque(maxlen=200)   # 최근 이벤트 기록 (확인용)
        self._cycle_raised = set()        # 이번 사이클에서 이미 낸 사유
        self._heat_raised = set()         # 이번 PV < L_Set 구간에서 이미 낸 사유
        self.below_since = None           # PV < L_Set 이 시작된 시각

    # ------------------------------------------------------------------ #
    def update(self, t, pre):
        """pre : 이번 tick 을 반영한 TankPreprocessor. 새로 난 이상은 self.new_events 에."""
        self.new_events = []
        self.asserted = []
        if pre.day_rolled:          # 6시 — 모델 판정과 heat 구간을 새로 센다
            self.reasons = {}
            self.below_since = None
            self._heat_raised = set()   # 6시 이후에도 L_Set 밑이면 새 운영일 이상으로 다시 낸다
        if pre.opened:
            self._cycle_raised = set()

        if pre.state == ON or pre.turned_off:   # 꺼지는 순간까지의 시간도 판정
            self._check_on_time(t, pre)
        if pre.state != OFF:
            self._check_cool_run(t, pre)
        if pre.margin is not None:
            self._check_heat(t, pre.margin)

    def warning(self):
        """플래그를 거치지 않은 모델 판정 -> relay_warning dict."""
        return build_warning(self.tank, self.reasons)

    def status(self):
        return {"reasons": {k: sorted(v) for k, v in self.reasons.items()}}

    # ------------------------------------------------------------------ #
    def _check_on_time(self, t, pre):
        """규칙 ② — 켜진 뒤 MAX_ON_SEC 이 지나면 즉시 이상."""
        if pre.elapsed(t) > self.max_on:
            self._raise("exchanger", "exchanger", t, self._cycle_raised)
            if pre.has_cool:    # 둘이 같이 돌았는데도 실패 -> 양쪽 다
                self._raise("cooler", "cooler", t, self._cycle_raised)

    def _check_cool_run(self, t, pre):
        """규칙 ③ — 진행 중 사이클까지 쳐서 cool 동반이 N회 연속이면 성능저하."""
        if pre.cool_run + int(pre.has_cool) >= self.consec_n:
            self._raise("exchanger", "exchanger_degrade", t, self._cycle_raised)

    def _check_heat(self, t, margin):
        """규칙 ① — PV 가 L_Set 밑으로 HEAT_CONFIRM_SEC 이어지면 이상."""
        if margin < 0:
            if self.below_since is None:
                self.below_since = t
            if (t - self.below_since).total_seconds() >= self.heat_confirm_sec:
                self._raise("heater", "heater", t, self._heat_raised)
        else:
            self.below_since = None
            self._heat_raised = set()

    def _raise(self, part, reason, t, seen):
        """seen : 같은 이상을 한 번만 내기 위한 집합 (사이클당 / heat 구간당)."""
        self.asserted.append((part, reason))
        self.reasons.setdefault(part, set()).add(reason)
        if reason not in seen:
            seen.add(reason)
            self.new_events.append((t, part, reason))
            self.events.append((t, reason))
