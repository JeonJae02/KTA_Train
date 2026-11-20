"""단계별 성능 — 경고·케이스 수, 진짜 신호가 남았나, 정밀도.

진짜 판정 규칙 (엣지 라벨 '실제 TP · 강한 패턴 · 약한 패턴'에서 뽑음, con_check/패턴TP_가정.md)
  센서 경고  P00451 센서 재감지                       → 진짜 (C03 TP, 조치 뒤 깜빡임)
  대차 경고  P00451 대차                               → 진짜 (C03 잔여 · C06 대차 35)
             P00164 대차 20 · P00459 대차 9 포함        → 진짜 (현장 확인 요청)
             P00154 대차 9 포함, 10-27                  → 진짜 (약한 패턴, 하루짜리)
  기타 실제 변화  md_rev 스톱퍼 시간 · P00141 대차 18   → 따로 셈 (TP 도 FP 도 아님)
  나머지     → 잡음
케이스: 같은 증상이 72시간 안에 다시 뜨면 하나 (con_check/케이스_묶음.md 와 같은 규칙)
"""
import re, sys
import numpy as np, pandas as pd

ROOT = "/Users/jeonjae/Desktop/KTA/DB_Log/model_review_1103"
OUT = ROOT + "/con_check/stepwise/out"
START, END = pd.Timestamp("2026-10-26 06:00"), pd.Timestamp("2026-11-15 06:00")
WRE = re.compile(r"대차\s*([\d·]+)")


def wagons_of(msgs):
    s = set()
    for m in msgs:
        mm = WRE.search(m or "")
        if mm:
            s |= {int(x) for x in mm.group(1).split("·") if x}
    return s


def kind_of(conv, reasons):
    if conv.endswith(":wagons"):
        return "대차"
    if "drive_speed" in reasons:
        return "구동부"
    if any(r.startswith("sensor_") for r in reasons):
        return "센서"
    return "유닛"


def label_of(conv, reasons, W, onset):
    k = kind_of(conv, reasons)
    s = conv.split(":")[0]
    if k == "센서":
        return "진짜" if s == "P00451" and "sensor_rearm" in reasons else "잡음"
    if k == "대차":
        if s == "P00451" or (s == "P00164" and 20 in W) or (s == "P00459" and 9 in W):
            return "진짜"
        if s == "P00154" and 9 in W and onset.normalize() == pd.Timestamp("2026-10-27"):
            return "진짜"
        if s == "P00141" and 18 in W:
            return "기타변화"
        return "잡음"
    if k == "유닛" and conv == "P00474" and any(r.startswith("stop_") for r in reasons):
        return "기타변화"                                   # md_rev 스톱퍼 SOL — 9월 +0.01초 계단
    return "잡음"


def symptom(r):
    k = r.kind
    if k == "대차":
        return ("대차", r.conv.split(":")[0])
    if k == "센서":
        return ("센서", r.conv, tuple(sorted(r.reasons)))
    if k == "구동부":
        return ("구동부",)
    return ("유닛", r.conv, tuple(sorted({re.sub(r"_(slow|fast|distance|trend)$", "", x) for x in r.reasons})))


def cases(ep):
    """72시간 안에 이어지는 같은 증상 → 케이스. 대차는 같은 센서에서 대차가 겹칠 때만."""
    ep = ep.sort_values("onset").reset_index(drop=True)
    cid = [-1] * len(ep)
    open_ = []          # [case_id, symptom, wagons, last]
    n = 0
    for i, r in ep.iterrows():
        sym = symptom(r)
        hit = None
        for c in open_:
            if c[1] == sym and (r.onset - c[3]).total_seconds() <= 72 * 3600 and (sym[0] != "대차" or (c[2] & r.W)):
                hit = c
                break
        if hit is None:
            hit = [n, sym, set(r.W), r["last"]]
            open_.append(hit)
            n += 1
        else:
            hit[2] |= r.W
            hit[3] = max(hit[3], r["last"])
        cid[i] = hit[0]
    ep["case"] = cid
    return ep


def prep(ep):
    ep = ep[(ep.onset >= START) & (ep.onset < END)].copy()
    ep["W"] = ep.messages.map(wagons_of)
    ep["kind"] = [kind_of(c, r) for c, r in zip(ep.conv, ep.reasons)]
    ep["label"] = [label_of(c, r, w, o) for c, r, w, o in zip(ep.conv, ep.reasons, ep.W, ep.onset)]
    ep["hours"] = (ep["last"] - ep.onset).dt.total_seconds() / 3600
    return cases(ep)


def edge_episodes():
    e = pd.read_csv(ROOT + "/warning/episodes/warning_episodes.csv", parse_dates=["onset", "last_detected"])
    e = e[e.kind == "conveyor"]
    lab = pd.read_csv(ROOT + "/con_check/패턴TP_가정.csv")[["episode_id", "패턴"]]
    e = e.merge(lab, on="episode_id", how="left")
    return pd.DataFrame(dict(conv=e.key, onset=e.onset, last=e.last_detected,
                             reasons=e.reasons.fillna("").str.split(";"),
                             messages=[[m] + ([f"대차 {'·'.join(str(w).split(';'))}:"] if isinstance(w, str) else [])
                                       for m, w in zip(e.message_first, e.wagon_nos)],
                             wagon_nos=e.wagon_nos, 패턴=e["패턴"], episode_id=e.episode_id))


KNOWN = {
    "C03 센서 재감지 (TP)": lambda ep: ep[(ep.kind == "센서") & (ep.conv == "P00451") & (ep.onset < "2026-11-01")],
    "C06 대차 35 (TP)": lambda ep: ep[(ep.kind == "대차") & (ep.conv == "P00451:wagons") & ep.W.map(lambda w: 35 in w)
                                     & (ep.onset < "2026-11-12 07:00") & (ep["last"] > "2026-11-10")],
    "P00164 대차 20": lambda ep: ep[(ep.conv == "P00164:wagons") & ep.W.map(lambda w: 20 in w)],
    "P00459 대차 9": lambda ep: ep[(ep.conv == "P00459:wagons") & ep.W.map(lambda w: 9 in w)],
    "C03 잔여 P00451 대차": lambda ep: ep[(ep.conv == "P00451:wagons") & (ep["last"] > "2026-10-31 16:40")],
    "P00451 센서 깜빡임 (조치 뒤)": lambda ep: ep[(ep.kind == "센서") & (ep.conv == "P00451") & (ep.onset >= "2026-11-01")],
    "P00154 대차 9 (10-27)": lambda ep: ep[(ep.conv == "P00154:wagons") & ep.W.map(lambda w: 9 in w)
                                         & (ep.onset.dt.normalize() == pd.Timestamp("2026-10-27"))],
}


def summarize(name, ep):
    n = len(ep)
    real = (ep.label == "진짜").sum()
    other = (ep.label == "기타변화").sum()
    noise = (ep.label == "잡음").sum()
    c = ep.groupby("case").label.agg(lambda x: "진짜" if (x == "진짜").any() else ("기타변화" if (x == "기타변화").any() else "잡음"))
    row = dict(단계=name, 경고=n, 진짜=real, 기타변화=other, 잡음=noise,
               정밀도=f"{real / n:.0%}" if n else "-",
               케이스=len(c), 케이스_진짜=(c == "진짜").sum(), 케이스_잡음=(c == "잡음").sum(),
               케이스_정밀도=f"{(c == '진짜').sum() / len(c):.0%}" if len(c) else "-",
               잡음_대차=((ep.label == "잡음") & (ep.kind == "대차")).sum(),
               잡음_구동부=((ep.label == "잡음") & (ep.kind == "구동부")).sum(),
               잡음_유닛=((ep.label == "잡음") & (ep.kind == "유닛")).sum(),
               잡음_센서=((ep.label == "잡음") & (ep.kind == "센서")).sum(),
               경고시간_h=round(ep.hours.sum()))
    known = {k: len(f(ep)) for k, f in KNOWN.items()}
    c06 = KNOWN["C06 대차 35 (TP)"](ep)
    lead = (pd.Timestamp("2026-11-12 06:56") - c06.onset.min()).total_seconds() / 3600 if len(c06) else np.nan
    return row, known, lead


if __name__ == "__main__":
    steps = sys.argv[1:] or ["S0", "S1", "S2", "S3", "S4"]
    rows, knowns = [], {}
    E = prep(edge_episodes())
    # 규칙 확인: 엣지 라벨과 규칙이 맞나
    lab = E["패턴"].fillna("")
    lab_cls = np.where(lab.str.contains("실제 TP|강한|약한"), "진짜", np.where(lab.str.contains("기타"), "기타변화", "잡음"))
    print("규칙 vs 엣지 라벨 (엣지 경고, 같은 기간):")
    print(pd.crosstab(pd.Series(lab_cls, name="라벨"), E.label.rename("규칙")).to_string(), "\n")
    r, k, lead = summarize("엣지 실제", E)
    rows.append(r); knowns["엣지 실제"] = {**k, "C06 앞선 시간 h": round(lead, 1)}
    for s in steps:
        try:
            ep = prep(pd.read_pickle(f"{OUT}/ep_{s}_throttle.pkl"))
        except FileNotFoundError:
            continue
        r, k, lead = summarize(s, ep)
        rows.append(r); knowns[s] = {**k, "C06 앞선 시간 h": round(lead, 1)}
        ep.to_pickle(f"{OUT}/scored_{s}.pkl")
    print(pd.DataFrame(rows).set_index("단계").T.to_string(), "\n")
    print("진짜 신호 — 경고 수 (0 이면 놓침)")
    print(pd.DataFrame(knowns).to_string())
