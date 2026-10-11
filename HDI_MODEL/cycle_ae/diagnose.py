"""펌프 진단 — 모델(WindowTracker) 결과를 받아 "어디가 왜" 로 해석하고, 릴레이 경보 형식으로 낸다.

    diag = PumpDiagnoser()                                    # 프로세스 시작 때 하나
    diag.update(pump, w, t)                                   # 사이클마다: w = WindowTracker.update(...) 결과
    alerts = diag.alerts(t)                                   # 이번 tick 경보 [{relay, reason, priority, type, message, show, ...}]

    python HDI_MODEL/cycle_ae/diagnose.py --table             # 규칙표·문구표 → diagnose_rules.md

## 입력 — 모델이 이미 낸 것만 쓴다

| 입력 | 어디서 | 뜻 |
|---|---|---|
| `pump_level` | `WindowTracker.update` | 펌프 창 등급 (정상/주의/알림). **알림일 때만 진단한다** |
| `notify` | 〃 | 운영일 첫 알림 순간 → 경보 `show=True` |
| `zn_med` | 〃 | 최근 10사이클의 피처별 **부호 있는** 정규화 편차 중앙값 (z ÷ 그 피처의 정상 한계, ±1 = 한계) |
| 헤드·슬롯 | `pump_settings.json` | 같은 헤드 슬롯의 POL·ISO 짝 |
| 릴레이 주소 | `HDI_Relay/shared/topology.json` | 경보를 띄울 부품 |

## 판정 — 증거가 가장 큰 규칙 하나 (RULES)

피처 셋 + 응답 피처로 증거 점수를 매기고, 가장 큰 것을 고른다 (E = 0.5 이상일 때만, 아니면 `pattern`).
PR = 펌프 잔차, RP = 압력/기대, P0 = 시작 전 압력, CV = 유량 흔들림, SH = 응답 시간·반감 시각 중 최대 |값|.

- 유량계: PR 이 내려가는데 압력은 안 내려감 — 압력 기대값은 Hz 로 계산하므로, 진짜 유량이 줄면 압력도 같이 내려가야 한다
- 펌프 토출 저하: PR·RP 함께 내려감
- 배관·노즐 저항: RP 또는 P0 가 한쪽으로 — 펌프 잔차는 압력을 반영한 누설식이라 그대로
- 헤드: 같은 헤드 슬롯의 POL·ISO 가 **같은 방향**으로 저항 변화 → 펌프 둘이 아니라 헤드(노즐·믹싱) 하나로 묶어 경보
- 여러 펌프 동시(30분 안 3대 이상, 같은 판정): 공통 원인 표시 — 개별 경보는 그대로 두고 문구에 덧붙인다

근거(알림 상태일 때 창 중앙값, 2026-10-11): P7 유량계 전조 PR −0.6 · RP +0.4 / 고장 PR −41 · RP −0.1,
I1 노즐 저항 감소 RP −4.7 · P0 −1.4 / 증가 RP +3.8 · P0 +1.6, P2 배관 저항 RP +0.8 · P0 +1.1 (같은 때 I2 RP +0.8 · P0 +0.4),
I7 드리프트 RP −2.4 · P0 −0.8, 10/10 밤 P1·P4·I3 동시 PR +0.6~+1.1 — `diagnose_rules.md` 의 검증 표.

## 출력 — 릴레이 경보 형식 (HDI_Relay backend tools_yeji/Relay_Monitor 가 받는 것)

    {"relay", "reason", "priority", "type", "message", "show"}  + 참고 필드 {"pump", "head", "part", "level", "evidence", "relay_fallback"}

- `show=True` 는 운영일 첫 알림 순간에만. 알림 등급이 이어지는 동안은 `show=False` 로 계속 낸다 → Relay_Monitor 가
  근무 끝까지 유지한다 (탱크·헤드와 같은 규칙)
- `relay`: 부품의 topology 릴레이 주소. **유량계·펌프 본체는 topology 에 릴레이가 없어** 그 펌프의 토출 압력(PT) 릴레이로
  대신 띄우고 `relay_fallback=True`, `part` 에 원래 부품을 남긴다 → topology 에 `Scale_Out___FT_*` 등을 추가하면 자동으로 옮겨 간다
- `type`: 프론트 아이콘 키 (warning-icons.ts — pressure / level / thermal / vibration / trip). `flow`·`pump` 는 아직 아이콘이
  없어 삼각 경고로 뜬다
"""

import argparse
import json
import os
import sys
from collections import deque

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
MODEL_DIR = os.path.dirname(HERE)
sys.path.insert(0, MODEL_DIR)
sys.path.insert(0, HERE)

from new_pump_preprocessor import load_settings  # noqa: E402

TOPOLOGY_PATH = os.path.join(os.path.dirname(MODEL_DIR), "..", "HDI_Relay", "shared", "topology.json")
E = 0.5                     # 판정 최소 증거 (정상 한계의 절반, 창 10사이클 중앙값 기준)
STRONG = 5.0                # 유량계 "고장" 으로 올리는 증거
COMMON_WINDOW_MIN = 30      # 여러 펌프 동시 판정 창
COMMON_MIN_PUMPS = 3

# --------------------------------------------------------------------------- #
# 규칙표 · 문구표 — 문구는 탱크 경보와 같은 구조: [장치][동작] [증상] · [지표] [추세]
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
#: 헤드 슬롯 → 인젝션 SOL 부품명 (HDI_Relay tools_yeji/relay_models/head_injection.py 의 HEAD_SOL 과 같은 대응)
HEAD_SOL = {("HD1", 1): "Head_1 Injection SOL", ("HD1", 2): "Head_1-2 Injection SOL",
            ("HD2", 1): "Head_2 Injection 1 SOL", ("HD2", 2): "Head_2 Injection_2 SOL",
            ("HD3", 1): "Head_3 Injection SOL", ("HD3", 2): "Head_3-2 Injection SOL",
            ("HD4", 1): "Head_4 Injection SOL"}


class RelayResolver:
    def __init__(self, path=TOPOLOGY_PATH):
        topo = json.load(open(path, encoding="utf-8"))
        self.relays = topo["relays"]
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

    def __init__(self, settings=None, topology_path=TOPOLOGY_PATH):
        s = settings or load_settings()
        self.slot = {p: (v["head"], v["slot"]) for p, v in s["pumps"].items()}
        self.name = {p: v["name"] for p, v in s["pumps"].items()}
        self.resolve = RelayResolver(topology_path)
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
                                       label=f"{head[0]}-{head[1]} {pol}·{iso}"))
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

    def _alert(self, code, relay, fallback, pump, head, show, ev, label=None, common=0):
        r = RULES[code]
        msg = r["message"] + (COMMON_SUFFIX.format(n=common - 1) if common else "")
        return {"relay": relay, "reason": f"pump_{code}", "priority": r["priority"], "type": r["type"],
                "message": msg, "show": bool(show),
                "pump": pump, "head": f"{head[0]}/{head[1]}", "part": r["part"], "level": "알림",
                "evidence": ev, "relay_fallback": fallback, "label": label or self.name.get(pump, pump)}


# --------------------------------------------------------------------------- #
# 규칙표 문서
# --------------------------------------------------------------------------- #
def md_table(df):
    """DataFrame → 마크다운 표 (tabulate 없이)."""
    cols = list(df.columns)
    lines = ["| " + " | ".join(map(str, cols)) + " |", "|" + "---|" * len(cols)]
    for row in df.itertuples(index=False):
        lines.append("| " + " | ".join(str(v).replace("|", "/") for v in row) + " |")
    return "\n".join(lines)


def write_table(path):
    rows = ["| 코드 (`reason` = `pump_` + 코드) | 부품 → 릴레이 | 아이콘 `type` | 우선순위 | 조건 | 문구 |", "|---|---|---|---|---|---|"]
    part_relay = {"pressure": "토출 압력 PT (`Scale_Out___PT_{펌프}`)",
                  "flowmeter": "유량계 — topology 에 없음 → PT 릴레이로 대신",
                  "pump": "펌프 본체·모터 — topology 에 없음 → PT 릴레이로 대신",
                  "head": "헤드 인젝션 SOL (예: HD1/2 → `Head_1-2 Injection SOL`)"}
    for code, r in sorted(RULES.items(), key=lambda kv: kv[1]["priority"]):
        when = r["when"].replace("|", "\\|")
        rows.append(f"| `{code}` | {part_relay[r['part']]} | `{r['type']}` | {r['priority']} | {when} | {r['message']} |")
    doc = ("# 펌프 진단 규칙표 · 알림 문구표\n\n`diagnose.py --table` 로 생성 — 고칠 땐 `diagnose.py` 의 `RULES` 를 고치고 다시 생성.\n\n"
           f"- 입력: `WindowTracker` 의 펌프 등급(알림일 때만), 창 10사이클 피처별 부호 있는 정규화 편차 중앙값 `zn_med` (±1 = 정상 한계)\n"
           f"- PR 펌프 잔차 · RP 압력/기대 · P0 시작 전 압력 · CV 유량 흔들림. 증거가 가장 큰 규칙 하나, {E} 미만이면 `pattern`\n"
           f"- 같은 헤드 슬롯 POL·ISO 가 같은 방향 저항 판정이면 헤드 경보 하나로 묶는다\n"
           f"- {COMMON_WINDOW_MIN}분 안에 {COMMON_MIN_PUMPS}대 이상 같은 판정이면 문구에 공통 원인 표시\n\n"
           + "\n".join(rows) + "\n")
    with open(path, "w", encoding="utf-8") as f:
        f.write(doc)
    return path


# --------------------------------------------------------------------------- #
# 검증 — 알려진 이벤트·새 날을 모델 → 창 → 진단까지 흘려 본다
# --------------------------------------------------------------------------- #
def _prepare(df, meta):
    import model as cae
    F = meta["features"]
    df = df.copy()
    Zs = cae.zsigned(df, meta["stats"])
    for j, f in enumerate(F):
        df[f"zn_{f}"] = Zs[:, j]
    df = cae.add_levels(df.drop(columns=[c for c in ["alarm_ae", "alarm_z", "alarm", "alert", "level"] if c in df]),
                        meta["thresholds"])
    return cae.window_levels(df, meta)


def _run(O, meta):
    """창 결과 표 O 를 시간순으로 진단기에 흘린다. 반환 (알림 순간 경보 목록, 알림 상태 사이클별 판정 코드)."""
    F = meta["features"]
    diag = PumpDiagnoser()
    shown, codes = [], []
    O = O.sort_values("t_start")
    zc = [f"znmed_{f}" for f in F]
    for row in O[["pump", "t_start", "pump_level", "notify", "event"] + zc].itertuples(index=False):
        z = dict(zip(F, row[5:]))
        diag.update(row.pump, {"pump_level": row.pump_level, "notify": row.notify, "zn_med": z}, row.t_start)
        if row.pump_level == "알림":
            codes.append((row.pump, row.event, diag.state[row.pump]["code"]))
        if row.notify:
            for a in diag.alerts(row.t_start):
                if a["show"]:
                    shown.append({"t": row.t_start, "event": row.event, **a})
    return pd.DataFrame(shown), pd.DataFrame(codes, columns=["pump", "event", "code"])


def validate():
    import model as cae
    from dataset import EVAL_EVENTS, OUT_DIR
    meta = cae.CycleScorer.load().meta
    H = pd.read_pickle(os.path.join(OUT_DIR, "scores.pkl.xz"), compression="xz")
    H = H[H.in_scope & (~H.excluded | H.event.ne(""))]
    ev_pumps = sorted({p for p, *_ in EVAL_EVENTS})
    shown_h, codes_h = _run(_prepare(H[H.pump.isin(ev_pumps)], meta), meta)
    rows = []
    for p, a, b, name in EVAL_EVENTS:
        c = codes_h[(codes_h.pump == p) & (codes_h.event == name)].code.value_counts(normalize=True)
        s = shown_h[(shown_h.pump.isin([p, ("I" if p[0] == "P" else "P") + p[1:]])) & (shown_h.event == name)]
        rows.append({"이벤트": name, "알림 상태 판정 (상위 2)": ", ".join(f"{k} {v * 100:.0f}%" for k, v in c.head(2).items()),
                     "첫 알림 경보": "" if s.empty else f"{s.t.min():%m-%d %H:%M} `{s.iloc[0].reason}` → `{s.iloc[0].relay}`"})
    T1 = pd.DataFrame(rows)
    new_path = os.path.join(OUT_DIR, "new_1008_1011.pkl.xz")
    T2 = pd.DataFrame()
    if os.path.exists(new_path):
        N = pd.read_pickle(new_path, compression="xz")
        N = N[N.in_scope & ~N.flag_any].assign(event="")
        shown_n, _ = _run(_prepare(N, meta), meta)
        if len(shown_n):
            T2 = shown_n.assign(시각=shown_n.t.dt.strftime("%m-%d %H:%M"))[["시각", "label", "reason", "relay", "relay_fallback", "message"]]
    return T1, T2


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--table", action="store_true", help="규칙표·문구표 → diagnose_rules.md")
    ap.add_argument("--validate", action="store_true", help="이벤트·새 날 검증 표를 diagnose_rules.md 에 덧붙인다")
    a = ap.parse_args()
    if a.table or a.validate:
        path = write_table(os.path.join(HERE, "diagnose_rules.md"))
        if a.validate:
            T1, T2 = validate()
            with open(path, "a", encoding="utf-8") as f:
                f.write("\n## 검증 1 — 알려진 이벤트 (학습 기간·테스트)\n\n" + md_table(T1) + "\n")
                f.write("\n## 검증 2 — 새 날 10/8~10/11 에 사람에게 간 경보 (`show=True`)\n\n"
                        + (md_table(T2) if len(T2) else "(없음)") + "\n")
            print(T1.to_string(index=False)); print(T2.to_string(index=False))
        print("저장:", path)
