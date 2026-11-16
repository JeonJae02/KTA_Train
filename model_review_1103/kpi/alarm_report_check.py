"""PLC 알람 원인분석 보고서(report_logs/*.json) vs 실제 — 보고서별 분류표.

- 보고서 timestamp 는 **UTC** 다. +9h 해서 KST 로 쓴다
  (10-31 M00042 보고서 02:31·03:08 … = C03 의 KST 알람 11:30·12:07 …, M0008F 가 매일 06:51·11:31·20:10 에 뜸).
- 현장 정답이 있는 사건: C03 (M00042), C06 (11-12 06:30 당김줄). 정답은 con_check/cases/C03.md · C06.md.
- 가동 상태는 cache/raw/conveyor 의 Auto_Running_PL · Conv_Run_Out (보고서 30초 전 / 150초 안 재가동).
출력: kpi/alarm_report_check.csv
"""
import glob
import json
import re
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT = Path(__file__).resolve().parent

# 현장 정답 (KST 구간, 사건, 실제 위치, 대상 알람 — None 이면 구간 안 전부)
TRUTH = [
    ("2026-10-30 20:00", "2026-10-31 16:40", "C03", "M00042",
     "P00451 전진 진입 센서가 1번 대차 표식 앞 구조물을 다시 감지 → 1번 대차를 두 번 셈. 조치: 센서·브라켓 위치 조정 (10-31 16:29~16:39)"),
    ("2026-11-12 06:29", "2026-11-12 06:46", "C06", None,
     "06:30:23 작업자가 당김줄 P0015F 를 당김 (35번 캐리어 오프닝 앞쪽 슬라이드 탈선). 06:42~ 복구 중 Local 버튼·역방향 인칭"),
]


def load():
    rows = []
    for f in sorted(glob.glob(str(ROOT / "report_logs/*.json"))):
        j = json.load(open(f))
        g, a = j["global_state"], j["analysis_report"]
        t = pd.Timestamp(datetime.strptime(j["timestamp"], "%Y-%m-%d %H:%M:%S") + timedelta(hours=9))
        s, fv = a.get("summary") or {}, a.get("final_verdict") or {}
        cause = s.get("suspected_cause")
        devs = a.get("suspectDevices", [])
        top = next((d for d in devs if d["relay"] == cause), None)
        anchors = {}
        for d in devs:
            m = re.search(r"앵커 (\d)", d.get("trace", ""))
            if m:
                anchors[d["relay"]] = int(m.group(1))
        rows.append(dict(
            t=t, file=Path(f).name, trigger=g["initial_alarm"], target=g["current_target"],
            bundled="+".join(g.get("bundled_alarms", [])), cause=cause, cause_name=s.get("cause_name"),
            type=fv.get("type"), confidence=fv.get("confidence"), selected_by=s.get("selected_by"),
            used_health=s.get("used_health"), n_cand=len(devs),
            operator_input=bool(top and any("조작계" in x for x in top.get("facts", []))),
            anchors=anchors))
    return pd.DataFrame(rows)


def add_context(r):
    cache, auto, restart = {}, [], []
    for t in r.t:
        day = t.normalize()
        f = ROOT / f"cache/raw/conveyor/{day:%Y-%m-%d}.pkl.xz"
        if day not in cache:
            cache[day] = pd.read_pickle(f)[["Conv_Run_Out", "Auto_Running_PL"]] if f.exists() else None
        d = cache[day]
        if d is None:
            auto.append(None); restart.append(None); continue
        auto.append(d.Auto_Running_PL.asof(t - pd.Timedelta(seconds=30)))
        w = d.Conv_Run_Out[(d.index > t - pd.Timedelta(seconds=150)) & (d.index <= t)]
        restart.append(bool(((w == 1) & (w.shift() == 0)).any()))
    r["auto_on"] = auto
    r["conv_restart_150s"] = restart
    return r


def classify(r):
    out = []
    for _, x in r.iterrows():
        case = next((c for a, b, c, tgt, _ in TRUTH
                     if pd.Timestamp(a) <= x.t <= pd.Timestamp(b) and tgt in (None, x.target)), None)
        home = "Home_CHK_Err" in x.target and "Air_Supply" in x.target
        if case:
            cat = f"현장정답_{case}"
        elif x.type == "데이터 부족":
            cat = "데이터부족"
        elif home:
            sol = "P00470" if "Supply2" in x.target else "P00474"
            rev = "P00456" if "Supply2" in x.target else "P0045E"
            same = x.anchors.get(sol) == 1 and x.anchors.get(rev) == 0
            cat = "원위치_재가동" if same else "원위치_기타"
        elif x.operator_input and x.type == "부품 이상":
            cat = "조작계를_부품이상으로"
        elif x.cause in ("P00119", "P00129", "P00112", "P00114", "P00113", "P00123", "P0012B", "P00120", "P00110"):
            cat = "로봇_자기신호"
        else:
            cat = "미검증"
        out.append(cat)
    r["category"] = out
    return r


def main():
    r = classify(add_context(load()))
    r.drop(columns=["anchors"]).to_csv(OUT / "alarm_report_check.csv", index=False, encoding="utf-8-sig")
    print(r.category.value_counts().to_string())
    print("자동운전 중:", int((r.auto_on == 1).sum()), "/", len(r))
    return r


if __name__ == "__main__":
    main()
