# tools_yeji/pump_models/pump_diagnose.py
"""펌프 진단 — 창 판정 결과를 "어디가 왜" 로 해석해 경보 목록으로 낸다.

규칙표·문구표는 RULES. 경보 {reason, priority, type, message, show, pumps, part}
(+ 참고 필드 pump, head, level, evidence, label, relay, relay_fallback). 근무 단위 유지는 Pump_Monitor 가 한다.
relay(헤드 SOL·PT 릴레이 주소)는 채워만 둔다 — 나중에 헤드 판정을 헤드 SOL 마커에도 띄울 때 쓸 자리.
"""
# 자동 생성 — DB_Log/HDI_MODEL/relay_port/make_port.py. 원본: HDI_MODEL/cycle_ae/diagnose.py
from collections import deque

import numpy as np
import pandas as pd

from config.topology_config import RELAY_CONFIG
from tools_yeji.pump_models.pump_params import load_settings

E = 0.5                     # 판정 최소 증거 (정상 한계의 절반, 창 10사이클 중앙값 기준)
STRONG = 5.0                # 유량계 "고장" 으로 올리는 증거
COMMON_WINDOW_MIN = 30      # 여러 펌프 동시 판정 창
COMMON_MIN_PUMPS = 3


# --------------------------------------------------------------------------- #
# 규칙표 · 문구표
# --------------------------------------------------------------------------- #
RULES = {
    "flowmeter_fail": {"part": "flowmeter", "type": "flow", "priority": 0,
                       "message": "유량계 고장 의심 · 유량값 급감, 토출 압력은 정상",
                       "when": "PR ≤ −5 이고 RP ≥ −0.25 (압력은 안 내려감)"},
    "head_resistance_high": {"part": "head", "type": "pressure", "priority": 1,
                             "message": "헤드 토출 저항 증가 · POL·ISO 동시 압력 상승 (노즐·믹싱헤드 점검)",
                             "when": "같은 헤드 슬롯 POL·ISO 둘 다 RP 또는 P0 ≥ 0.5"},
    "head_resistance_low": {"part": "head", "type": "pressure", "priority": 1,
                            "message": "헤드 토출 저항 감소 · POL·ISO 동시 압력 하강 (노즐 마모·열림 점검)",
                            "when": "같은 헤드 슬롯 POL·ISO 둘 다 RP 또는 P0 ≤ −0.5"},
    "flowmeter_low": {"part": "flowmeter", "type": "flow", "priority": 2,
                      "message": "유량계 측정 이상 의심 · 같은 회전수 대비 유량값 감소, 토출 압력은 정상",
                      "when": "증거 −PR (RP ≥ −0.25 일 때)"},
    "pump_output_low": {"part": "pump", "type": "pump", "priority": 2,
                        "message": "펌프 토출 저하 · 같은 회전수 대비 유량·압력 함께 감소 (펌프 마모·흡입 점검)",
                        "when": "증거 min(−PR, −RP) (둘 다 ≤ −0.25)"},
    "line_resistance_high": {"part": "pressure", "type": "pressure", "priority": 3,
                             "message": "토출 배관·노즐 저항 증가 · 같은 유량 대비 압력 상승",
                             "when": "증거 max(RP, P0)"},
    "line_resistance_low": {"part": "pressure", "type": "pressure", "priority": 3,
                            "message": "토출 배관 저항 감소 · 같은 유량 대비 압력 하강 (노즐 열림·누설 의심)",
                            "when": "증거 max(−RP, −P0)"},
    "flow_reading_high": {"part": "flowmeter", "type": "flow", "priority": 4,
                          "message": "유량값 증가 · 같은 회전수 대비 유량 상승 (유량계·슬립 확인)",
                          "when": "증거 PR (|RP| < 0.5 일 때)"},
    "response_change": {"part": "pump", "type": "pump", "priority": 4,
                        "message": "펌프 응답 변화 · 증압·감속·압력해제 시간이 평소와 다름 (모터·인버터·밸브 점검)",
                        "when": "증거 max |응답 시간, 감속 반감, 압력 해제 반감|"},
    "flow_fluctuation": {"part": "flowmeter", "type": "flow", "priority": 5,
                         "message": "유량 흔들림 변화 · 유량 신호가 평소와 다르게 흔들림 (유량계·기포 확인)",
                         "when": "증거 |CV|"},
    "pattern": {"part": "pump", "type": "pump", "priority": 6,
                "message": "펌프 운전 패턴 변화 · 여러 지표의 관계가 평소와 다름",
                "when": "위 규칙 증거가 모두 0.5 미만 (AE 패턴 점수로만 알림)"},
}
COMMON_SUFFIX = " — 같은 시각 다른 펌프 {n}대도 같은 판정 (재료 온도·공정 조건 등 공통 원인 확인)"


def evidence(z):
    """z: {피처: 창 중앙 zn}. 반환 {규칙: 증거} (헤드·고장 규칙 제외 — 그건 상위에서)."""
    g = lambda k: float(z.get(k, 0.0)) if z.get(k) is not None and np.isfinite(z.get(k)) else 0.0  # noqa: E731
    PR, RP, P0, CV = g("pump_resid"), g("r_phys"), g("P0"), g("ft_cv")
    SH = max(abs(g("t_self")), abs(g("decel_half_sec")), abs(g("release_half_sec")))
    return {
        "flowmeter_low": max(0.0, -PR) if RP >= -E / 2 else 0.0,
        "pump_output_low": min(-PR, -RP) if (PR <= -E / 2 and RP <= -E / 2) else 0.0,
        "line_resistance_high": max(RP, P0, 0.0) if RP >= 0 or P0 >= E else 0.0,
        "line_resistance_low": max(-RP, -P0, 0.0) if RP <= 0 or P0 <= -E else 0.0,
        "flow_reading_high": max(0.0, PR) if abs(RP) < E else 0.0,
        "response_change": SH,
        "flow_fluctuation": abs(CV),
    }


def classify(z):
    """창 중앙 zn → (규칙 코드, 증거). 펌프 하나만 보고 정한다."""
    if not z:
        return "pattern", {}
    ev = evidence(z)
    if ev["flowmeter_low"] >= STRONG:
        return "flowmeter_fail", ev
    code = max(ev, key=ev.get)
    return (code if ev[code] >= E else "pattern"), ev


# --------------------------------------------------------------------------- #
# 릴레이 주소 — topology
# --------------------------------------------------------------------------- #
HEAD_SOL = {("HD1", 1): "Head_1 Injection SOL", ("HD1", 2): "Head_1-2 Injection SOL",
            ("HD2", 1): "Head_2 Injection 1 SOL", ("HD2", 2): "Head_2 Injection_2 SOL",
            ("HD3", 1): "Head_3 Injection SOL", ("HD3", 2): "Head_3-2 Injection SOL",
            ("HD4", 1): "Head_4 Injection SOL"}


class RelayResolver:
    def __init__(self, relays=None):
        self.relays = RELAY_CONFIG if relays is None else relays
        self.by_part = {v["partName"]: k for k, v in self.relays.items()}

    def __call__(self, part, pump, head_slot):
        """반환 (relay 주소, fallback 여부)."""
        pt = f"Scale_Out___PT_{pump}"
        if part == "pressure":
            return pt, False
        if part == "flowmeter":
            ft = f"Scale_Out___FT_{pump}"
            return (ft, False) if ft in self.relays else (pt, True)
        if part == "head":
            rid = self.by_part.get(HEAD_SOL.get(head_slot, ""))
            return (rid, False) if rid else (pt, True)
        return pt, True                                     # 펌프 본체·모터 — 릴레이 없음


# --------------------------------------------------------------------------- #
# 진단기
# --------------------------------------------------------------------------- #
class PumpDiagnoser:
    """펌프마다 최신 창 결과를 들고, alerts() 에서 헤드 묶기·동시 판정을 더해 릴레이 경보를 낸다."""

    def __init__(self, settings=None):
        s = settings or load_settings()
        self.slot = {p: (v["head"], v["slot"]) for p, v in s["pumps"].items()}
        self.name = {p: v["name"] for p, v in s["pumps"].items()}
        self.resolve = RelayResolver()
        self.state = {}                                     # pump -> {level, z, code, ev, show_pending, t}
        self.recent = deque(maxlen=500)                     # (t, pump, code) — 동시 판정

    def update(self, pump, w, t):
        """w: WindowTracker.update 결과. 알림 등급이 아니면 그 펌프 상태를 비운다."""
        t = pd.Timestamp(t)
        if w.get("pump_level") != "알림":
            self.state.pop(pump, None)
            return
        code, ev = classify(w.get("zn_med"))
        prev = self.state.get(pump, {})
        self.state[pump] = {"z": w.get("zn_med") or {}, "code": code, "ev": ev, "t": t,
                            "show_pending": prev.get("show_pending", False) or bool(w.get("notify"))}
        if w.get("notify"):
            self.recent.append((t, pump, code))

    def _partner(self, pump):
        other = ("I" if pump.startswith("P") else "P") + pump[1:]
        return other if other in self.slot and self.slot[other] == self.slot.get(pump) else None

    def alerts(self, t):
        """이번 tick 의 경보 목록 (릴레이 형식). show 는 운영일 첫 알림이 아직 안 나간 경우에만 True."""
        t = pd.Timestamp(t)
        out, done = [], set()
        for pump, st in sorted(self.state.items()):
            if pump in done:
                continue
            code, ev = st["code"], st["ev"]
            partner = self._partner(pump)
            ps = self.state.get(partner) if partner else None
            # 헤드 묶기: 짝도 알림 상태이고 둘 다 같은 방향 저항 변화
            if ps and code in ("line_resistance_high", "line_resistance_low") and ps["code"] == code:
                hcode = code.replace("line_", "head_")
                head = self.slot[pump]
                relay, fb = self.resolve("head", pump, head)
                pol, iso = (pump, partner) if pump.startswith("P") else (partner, pump)
                out.append(self._alert(hcode, relay, fb, pol, head, show=st["show_pending"] or ps["show_pending"],
                                       ev={pump: ev.get(code), partner: ps["ev"].get(code)},
                                       label=f"{head[0]}-{head[1]} {pol}·{iso}", pumps=[pol, iso]))
                st["show_pending"] = ps["show_pending"] = False
                done.update({pump, partner})
                continue
            relay, fb = self.resolve(RULES[code]["part"], pump, self.slot[pump])
            n_same = len({p for (tt, p, c) in self.recent
                          if c == code and p != pump and abs((tt - st["t"]).total_seconds()) <= COMMON_WINDOW_MIN * 60})
            out.append(self._alert(code, relay, fb, pump, self.slot[pump], show=st["show_pending"], ev=ev.get(code),
                                   common=n_same + 1 if n_same + 1 >= COMMON_MIN_PUMPS else 0))
            st["show_pending"] = False
            done.add(pump)
        return out

    def _alert(self, code, relay, fallback, pump, head, show, ev, label=None, common=0, pumps=None):
        r = RULES[code]
        msg = r["message"] + (COMMON_SUFFIX.format(n=common - 1) if common else "")
        return {"relay": relay, "reason": f"pump_{code}", "priority": r["priority"], "type": r["type"],
                "message": msg, "show": bool(show),
                "pump": pump, "pumps": pumps or [pump], "head": f"{head[0]}/{head[1]}", "part": r["part"], "level": "알림",
                "evidence": ev, "relay_fallback": fallback, "label": label or self.name.get(pump, pump)}
