# tools_yeji/pump_models/pump_diagnose.py
"""펌프 진단 — 창 판정 결과를 "어디가 왜" 로 해석해 경보 목록으로 낸다.

    classify_all (+ 헤드)   이 사이클 창에서 증거 E 이상인 사유 전부. 우선순위로 고르지 않는다
      └ PersistFilter       (펌프, 사유) 마다: 그날 누적 15분 이상 뜬 날이 생산일 3일 연속이면 확정 (pump_persist.py)
         └ alerts()         확정 사유 중 우선순위로 하나 → 아래 경보 형식 그대로. pattern 은 안 띄운다

지속 필터는 사유 이름만 보고 시간을 센다 — RULES·evidence 를 고쳐도 필터는 손댈 필요가 없다.
show=True 는 운영일마다 (펌프, 사유) 가 처음 나갈 때. 덧붙는 필드 codes (확정 사유 전부, 우선순위 순), streak (연속 생산일).

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
from tools_yeji.pump_models.pump_persist import PersistFilter, op_day

E = 0.5                     # 판정 최소 증거 (정상 한계의 절반, 창 10사이클 중앙값 기준)
STRONG = 5.0                # 유량계 "고장" 으로 올리는 증거
COMMON_WINDOW_MIN = 30      # 여러 펌프 동시 판정 창
COMMON_MIN_PUMPS = 3
#: 지속 필터 (pump_persist.py) — 그날 그 사유로 알림 누적 15분 이상인 날이 생산일 3일 연속이면 확정 (2026-11-23 한 달 재생)
PERSIST = {"min_minutes": 15, "days": 3, "min_cycles": 100, "max_gap_sec": 300, "bypass": ("flowmeter_fail",)}
HIDDEN = {"pattern"}        # 띄우지 않는 사유 — 무엇을 하라는 말이 없다


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
    """창 중앙 zn → (규칙 코드, 증거). 펌프 하나만 보고 증거가 가장 큰 것 하나 — 확인용 (경보는 classify_all)."""
    if not z:
        return "pattern", {}
    ev = evidence(z)
    if ev["flowmeter_low"] >= STRONG:
        return "flowmeter_fail", ev
    code = max(ev, key=ev.get)
    return (code if ev[code] >= E else "pattern"), ev


def classify_all(z):
    """창 중앙 zn → (증거 E 이상인 사유 전부, 증거). 하나로 고르지 않는다 — 우선순위 높은 사유가 낮은 사유의
    연속성을 가리지 않게, 지속 필터에는 전부 넘기고 고르는 건 띄울 때 한다. 아무것도 없으면 빈 집합 (pattern 없음)."""
    if not z:
        return set(), {}
    ev = evidence(z)
    codes = {k for k, v in ev.items() if v >= E}
    if ev["flowmeter_low"] >= STRONG:
        codes.add("flowmeter_fail")
    return codes, ev


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
    """사이클마다 ① 사유 전부 뽑기(classify_all + 헤드) → ② 지속 필터(PersistFilter) → alerts() 에서 ③ 우선순위로 하나 골라
    릴레이 경보 형식으로 낸다. ①·③ 은 규칙이 바뀌면 같이 바뀌고, ② 는 사유 이름만 보므로 그대로 둔다."""

    def __init__(self, settings=None, persist=None, persist_path=None):
        s = settings or load_settings()
        self.slot = {p: (v["head"], v["slot"]) for p, v in s["pumps"].items()}
        self.name = {p: v["name"] for p, v in s["pumps"].items()}
        self.resolve = RelayResolver()
        self.persist = persist or PersistFilter(**PERSIST, state_path=persist_path)
        self.state = {}                                     # pump -> {z, code, codes, confirmed, ev, t, od} (알림 등급일 때만)
        self.recent = deque(maxlen=500)                     # (t, pump, code) — 동시 판정 (띄운 경보 기준)
        self._shown = set()                                 # (운영일, 펌프들, 사유) — 그날 이미 show=True 로 낸 것

    def update(self, pump, w, t):
        """w: WindowTracker.update 결과. 자동운전 사이클마다 부른다 — 정상 사이클도 지속 필터의 시간·생산일 계산에 쓴다."""
        t = pd.Timestamp(t)
        alert = w.get("pump_level") == "알림"
        codes, ev = classify_all(w.get("zn_med")) if alert else (set(), {})
        if alert:
            # 헤드: 짝도 알림 상태이고 같은 방향 저항이 떠 있으면 헤드 사유를 더한다 (배관 사유도 그대로 둔다 — 띄울 때 헤드가 이긴다)
            partner = self._partner(pump)
            ps = self.state.get(partner) if partner else None
            for d in ("high", "low"):
                if ps and f"line_resistance_{d}" in codes and f"line_resistance_{d}" in ps["codes"]:
                    codes.add(f"head_resistance_{d}")
        confirmed = self.persist.update(pump, codes, t)
        if not alert:
            self.state.pop(pump, None)
            return
        self.state[pump] = {"z": w.get("zn_med") or {}, "code": classify(w.get("zn_med"))[0], "codes": codes,
                            "confirmed": confirmed, "ev": ev, "t": t, "od": op_day(t)}

    def _partner(self, pump):
        other = ("I" if pump.startswith("P") else "P") + pump[1:]
        return other if other in self.slot and self.slot[other] == self.slot.get(pump) else None

    def _pick(self, pump, st):
        """확정 사유 중 띄울 것 하나: 우선순위 → 같으면 오늘 오래 뜬 쪽. 없으면 None."""
        cand = [c for c in st["confirmed"] if c not in HIDDEN]
        if not cand:
            return None
        mins = self.persist.minutes(pump, st["od"])
        return min(cand, key=lambda c: (RULES[c]["priority"], -mins.get(c, 0)))

    def alerts(self, t):
        """이번 tick 의 경보 목록 (릴레이 형식). 알림 등급이면서 확정 사유가 있는 펌프만.
        show=True 는 운영일마다 (펌프, 사유) 가 처음 나갈 때 — Pump_Monitor 가 그때 띄우기 시작하고, 이후 show=False 로 유지 연장."""
        t = pd.Timestamp(t)
        od = op_day(t)
        # 확정은 그 운영일 것 — 마지막 사이클이 전 운영일인 펌프(멈춘 채 날이 바뀜)는 내지 않는다
        top = {p: (self._pick(p, st) if st["od"] == od else None) for p, st in self.state.items()}
        out, done = [], set()
        for pump, st in sorted(self.state.items()):
            code = top[pump]
            if pump in done or code is None:
                continue
            others = sorted((c for c in st["confirmed"] if c not in HIDDEN and c != code), key=lambda c: RULES[c]["priority"])
            info = {"codes": [code] + others, "streak": self.persist.streak(pump, code, st["od"])}
            if code.startswith("head_"):
                partner = self._partner(pump)
                ps = self.state.get(partner) if partner else None
                head = self.slot[pump]
                relay, fb = self.resolve("head", pump, head)
                pol, iso = (pump, partner) if pump.startswith("P") else (partner, pump)
                line = code.replace("head_", "line_")
                out.append(self._alert(code, relay, fb, pol, head, show=self._first(st["od"], (pol, iso), code, pump, t),
                                       ev={pump: st["ev"].get(line), partner: ps["ev"].get(line) if ps else None},
                                       label=f"{head[0]}-{head[1]} {pol}·{iso}", pumps=[pol, iso], info=info))
                done.add(pump)
                if top.get(partner) == code:
                    done.add(partner)
                continue
            relay, fb = self.resolve(RULES[code]["part"], pump, self.slot[pump])
            n_same = len({p for (tt, p, c) in self.recent
                          if c == code and p != pump and abs((tt - st["t"]).total_seconds()) <= COMMON_WINDOW_MIN * 60})
            out.append(self._alert(code, relay, fb, pump, self.slot[pump], show=self._first(st["od"], (pump,), code, pump, t),
                                   ev=st["ev"].get(code), common=n_same + 1 if n_same + 1 >= COMMON_MIN_PUMPS else 0,
                                   info=info))
            done.add(pump)
        return out

    def _first(self, od, pumps, code, pump, t):
        """그 운영일에 (펌프들, 사유) 가 처음이면 True 로 기록하고 동시 판정 목록에 넣는다."""
        key = (od, pumps, code)
        if key in self._shown:
            return False
        self._shown = {k for k in self._shown if k[0] >= od} | {key}
        self.recent.append((t, pump, code))
        return True

    def _alert(self, code, relay, fallback, pump, head, show, ev, label=None, common=0, pumps=None, info=None):
        r = RULES[code]
        msg = r["message"] + (COMMON_SUFFIX.format(n=common - 1) if common else "")
        return {"relay": relay, "reason": f"pump_{code}", "priority": r["priority"], "type": r["type"],
                "message": msg, "show": bool(show),
                "pump": pump, "pumps": pumps or [pump], "head": f"{head[0]}/{head[1]}", "part": r["part"], "level": "알림",
                "evidence": ev, "relay_fallback": fallback, "label": label or self.name.get(pump, pump), **(info or {})}
