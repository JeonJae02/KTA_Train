# tools_yeji/Relay_Monitor.py
import time

from tools_yeji.Fault_Registry import ActiveFaultRegistry
from tools_yeji.Relay_Trend import RelayTrend
from tools_yeji.common.time_utils import hold_until, to_kst, to_utc_iso
from tools_yeji.relay_models.head_injection import HeadInjection
from tools_yeji.relay_models.tank_realtime import TankRealtimeGroup


class RelayMonitor:
    """탱크·헤드 릴레이 경고. 판정은 relay_models, 얼마나 오래 띄울지(근무 단위 유지)는 여기서.

    check() 반환은 레지스트리 values() —
    [{time, relay, type, message, reason, active, last_detected, duration_sec, hold_until}].
    PredictiveHealth 가 그대로 `Relay_Data` 로 보낸다. 펌프는 Pump_Monitor 가 따로 보낸다.
    type 은 프론트 아이콘 키(warning-icons.ts): 탱크 thermal=온도계, 헤드 timing=삼각 경고.
    필드 설명은 shared/응답 예시/idle_data.md.
    """

    def __init__(self):
        self._registry = ActiveFaultRegistry()
        self._tank = TankRealtimeGroup()
        self._head = HeadInjection()
        self._trend = RelayTrend()
        #: (relay, 사유) -> {type, message, priority, expires, last, active}. 근무 단위 유지 (_apply_hold)
        self._hold = {}
        #: relay -> 처음 띄운 시각(epoch). 레지스트리 time 과 수명이 같다 — duration_sec 기준
        self._since = {}

    def check(self, live_data):
        """live_data 는 감시 태그(tag_config) 로 걸러진 {주소/태그명: 원시 PLC 값}.

        1. 탱크 / 헤드 모델 update — 모델은 원시값·주소 키를 그대로 받고, 가동 플래그까지
           거친 이번 tick 의 경보 목록을 낸다 (tank_judge.build_alert 형식)
        2. 근무 단위 유지 (_apply_hold) — 얼마나 오래 띄울지는 여기 한 곳에서만 정한다
        3. relay 마다 하나로 합쳐 레지스트리에 touch / sync
        (추세 기록은 Relay_Trend 구현 후 여기서 pre.closed / day_rolled 를 보고 넘긴다)

        유지되는 동안 매 틱 touch 하므로 레지스트리의 최초 발생 시각(time)이 그대로 유지된다.
        """
        now = time.time()
        t = to_kst(now)
        alerts = self._tank.update(now, live_data) + self._head.update(now, live_data)
        self._apply_hold(now, t, alerts)

        warnings = self._held_warnings(now)
        for rid, info in warnings.items():
            self._registry.touch(rid, relay=rid, **info)
        self._registry.sync(set(warnings))
        return self._registry.values()

    def _apply_hold(self, now, t, alerts):
        """경보를 "t 이후 처음 시작하는 근무가 끝날 때까지" 유지한다 (shared/operation.json).

        show=True  -> 띄우기 시작 (또는 유지 연장)
        show=False -> 이미 떠 있을 때만 유지 연장. 이상이 계속되는 동안은 안 꺼진다
        alerts 에 있으면 이번 tick 에 조건이 성립한 것이다 -> active=True, last=now.
        """
        until = hold_until(t)
        for h in self._hold.values():
            h["active"] = False
        for a in alerts:
            key = (a["relay"], a["reason"])
            if a["show"] or key in self._hold:
                self._hold[key] = {"type": a["type"], "message": a["message"],
                                   "priority": a["priority"], "expires": until,
                                   "last": now, "active": True}
        for key in [k for k, h in self._hold.items() if h["expires"] <= t]:
            del self._hold[key]

    def _held_warnings(self, now):
        """relay 마다 유지 중인 사유를 하나로 합친다 -> {relay: Relay_Data 필드}.

        type / message / reason : priority 가 가장 작은 사유
        active        : 사유 중 하나라도 이번 tick 에 성립했는가 (False = 해소됐지만 근무 끝까지 유지 중)
        last_detected : 사유들 중 마지막으로 성립한 시각
        duration_sec  : 처음 띄운 시각 ~ last_detected. 사유가 바뀌어도 처음 띄운 시각부터 센다
        hold_until    : 사유들 중 가장 늦은 만료 = 화면에서 내려가는 시각

        처음 띄운 시각(self._since)도 여기서 같이 갱신한다 — 레지스트리 touch / sync 와 같은 집합이다.
        """
        by_relay = {}
        for (rid, reason), h in self._hold.items():
            by_relay.setdefault(rid, []).append((reason, h))
        self._since = {rid: self._since.get(rid, now) for rid in by_relay}

        out = {}
        for rid, held in by_relay.items():
            reason, best = min(held, key=lambda rh: rh[1]["priority"])
            last = max(h["last"] for _, h in held)
            out[rid] = {"type": best["type"], "message": best["message"], "reason": reason,
                        "active": any(h["active"] for _, h in held),
                        "last_detected": to_utc_iso(last),
                        "duration_sec": round(last - self._since[rid], 1),
                        "hold_until": to_utc_iso(max(h["expires"] for _, h in held))}
        return out
