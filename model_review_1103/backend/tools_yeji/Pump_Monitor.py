# tools_yeji/Pump_Monitor.py
import time

from tools_yeji.Fault_Registry import ActiveFaultRegistry
from tools_yeji.common.time_utils import hold_until, to_kst, to_utc_iso
from tools_yeji.pump_models.pump_realtime import PumpRealtimeGroup

#: 진단 부품 → 프론트 3D 마커 키 (useFaultParts.ts: Pump→metering_pump, Motor→motor, Inverter→inverter).
#: 유량계·배관·헤드는 따로 마커가 없어 펌프에 띄운다 — 무엇인지는 message 가 말한다.
PART_DEVICE = {"flowmeter": "Pump", "pump": "Pump", "pressure": "Pump", "head": "Pump"}


class PumpMonitor:
    """펌프 14대 사이클 진단. 판정은 pump_models/pump_*, 얼마나 오래 띄울지(근무 단위 유지)는 여기서.

    check() 반환은 14개 전량 {pid: 판정 dict 또는 None} — PredictiveHealth 가 `Pump_Score` 로 내고,
    PredictiveEngine 이 `over` 인 것만 골라 `Pump_Anomaly` 로 프론트에 보낸다.

    - None: 판정 없음 (정지·대기 순환·점검·수동·학습 범위 밖, 아직 사이클 없음). '정상' 이 아니다
    - mse / threshold: 마지막 자동운전 사이클의 AE 점수 / 알림 임계 (사이클이 끝날 때만 바뀐다)
    - top: 마지막 사이클에서 많이 벗어난 피처 3개와 비중
    - over: **경보를 띄우는 중** — 운영일 첫 알림부터 그 근무 끝까지 (탱크·헤드 Relay_Monitor 와 같은 규칙).
      over 일 때 레지스트리 필드 {time, pump, device, type, message, reason, active, last_detected,
      duration_sec, hold_until} 가 같은 dict 에 붙는다
    """

    def __init__(self, pump_list=None):
        self._rt = PumpRealtimeGroup(pump_list)
        self.pump_list = list(pump_list or self._rt.pumps)
        self._faults = ActiveFaultRegistry()
        #: (pid, 사유) -> {device, type, message, priority, expires, last, active}. 근무 단위 유지 (_apply_hold)
        self._hold = {}
        #: pid -> 처음 띄운 시각(KST). 레지스트리 time 과 수명이 같다 — duration_sec 기준
        self._since = {}
        self._tags_checked = False

    def check(self, live_data, now=None):
        """live_data 는 감시 태그(tag_config) 로 걸러진 {주소/태그명: 원시 PLC 값}. now 는 재생 확인용."""
        now = time.time() if now is None else now
        t = to_kst(now)
        if not self._tags_checked:
            missing = self._rt.missing_tags(live_data.keys())
            if missing:
                print(f"⚠️ [PumpMonitor] 감시 태그 없음 (tag_config 확인): {missing}")
            self._tags_checked = True

        alerts = self._rt.update(now, live_data)
        self._apply_hold(t, alerts)
        held = self._held_warnings()

        scores = self._rt.pump_scores()
        out = {}
        for pid in self.pump_list:
            s = scores.get(pid)
            if pid in held:
                s = dict(s or {"mse": None, "threshold": None, "top": [], "wagon": None, "tick": None})
                s["over"] = True
                s.update(self._faults.touch(pid, pump=pid, **held[pid]))
            elif s is not None:
                s = dict(s, over=False)
            out[pid] = s
        self._faults.sync(set(held))
        return out

    def _apply_hold(self, t, alerts):
        """경보를 "t 이후 처음 시작하는 근무가 끝날 때까지" 유지한다 (shared/operation.json).

        show=True  -> 띄우기 시작 (또는 유지 연장)
        show=False -> 이미 떠 있을 때만 유지 연장. 이상이 계속되는 동안은 안 꺼진다
        헤드 판정은 POL·ISO 두 펌프(pumps)에 같이 띄운다.
        """
        until = hold_until(t)
        for h in self._hold.values():
            h["active"] = False
        for a in alerts:
            msg = a["message"] + (f" — 헤드 {a['label']}" if a["part"] == "head" else "")
            for pid in a["pumps"]:
                key = (pid, a["reason"])
                if a["show"] or key in self._hold:
                    self._hold[key] = {"device": PART_DEVICE.get(a["part"], "Pump"), "type": a["type"],
                                       "message": msg, "priority": a["priority"], "expires": until,
                                       "last": t, "active": True}
        for key in [k for k, h in self._hold.items() if h["expires"] <= t]:
            del self._hold[key]

    def _held_warnings(self):
        """펌프마다 유지 중인 사유를 하나로 합친다 -> {pid: 레지스트리 필드}. Relay_Monitor._held_warnings 와 같은 규칙."""
        by_pump = {}
        for (pid, reason), h in self._hold.items():
            by_pump.setdefault(pid, []).append((reason, h))
        self._since = {pid: self._since.get(pid, min(h["last"] for _, h in held)) for pid, held in by_pump.items()}

        out = {}
        for pid, held in by_pump.items():
            reason, best = min(held, key=lambda rh: rh[1]["priority"])
            last = max(h["last"] for _, h in held)
            out[pid] = {"device": best["device"], "type": best["type"], "message": best["message"], "reason": reason,
                        "active": any(h["active"] for _, h in held),
                        "last_detected": to_utc_iso(last),
                        "duration_sec": round((last - self._since[pid]).total_seconds(), 1),
                        "hold_until": to_utc_iso(max(h["expires"] for _, h in held))}
        return out
