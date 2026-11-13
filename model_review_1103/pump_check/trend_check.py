"""펌프 저항 수준·추세 점검 — 보고서 `pump_check/펌프_추세_검토.md` 의 숫자를 다시 만든다.

1. 일별 표 (14대 × 가동일): 순환 K·PT·FT, 증압 PT. `cache/raw/pump/` 일별 CSV 만 쓴다 (DB 조회 없음)
2. 지금 모델이 반응하는 저항 변화 크기 (meta.json 의 scale·zq·문턱 → r_phys %)
3. 압력 상승일 vs 7일 안 고장 (2라인 펌프 고장 3건)
4. 추세 방법 시험: EWMA · CUSUM · 최근 14 가동일 Theil-Sen 기울기 (일별 순환 K, 기준 09-16~10-03)

실행 (model_review_1103 에서): python pump_check/trend_check.py      # 처음 한 번 일별 표 계산 약 5분, 이후 캐시
"""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import fisher_exact, kendalltau, theilslopes

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from review_lib import pump_csv  # noqa: E402

PUMPS = ["P1", "P2", "P3", "P4", "P5", "P6", "P7", "I1", "I2", "I3", "I4", "I5", "I6", "I7"]
START, END = "2026-09-11", "2026-11-12"
CACHE = ROOT / "cache/replay/pump_daily_pressure.pkl"
SETTINGS = ROOT / "backend/models/pump_cycle/pump_settings.json"
META = ROOT / "backend/models/pump_cycle/meta.json"
REF = ("2026-09-16", "2026-10-03")          # 추세 기준 구간 (P2 09-11~15 상승 구간 뒤)
ELEV_PCT = 8.0                              # 압력 상승일: 순환 K 또는 증압 PT 가 자기 기준(25% 분위) 대비 +8% 이상
#: 2라인 펌프·압력 계통 고장 (일지). 09-17 P7 유량계, 10-27 유압, 10-29 니플은 압력 부하와 무관하거나 쪽 미상이라 뺐다
FAILURES = {("P7", "2026-09-14"): "P7 샤프트 파손 (압력 안 걸림)",
            ("P2", "2026-10-19"): "P2 펌프 고장 (압력 안 걸림)",
            ("P7", "2026-10-20"): "P7 POL 호스 터짐"}


def daily_table():
    if CACHE.exists():
        return pd.read_pickle(CACHE)
    circ_hz = {p: v["circ_hz"] for p, v in json.load(open(SETTINGS, encoding="utf-8"))["pumps"].items()}
    cols = [f"{x}_{p}" for p in PUMPS for x in ("Scale_Out___FT", "Scale_Out___PT", "Hz_Out", "Pump_BuildUp")]
    rows = []
    for d in pd.date_range(START, END):
        df = pump_csv.load(d, d + pd.Timedelta(days=1), cols)
        if df is None or not len(df):
            continue
        for p in PUMPS:
            ft, pt, hz, bu = (df[f"{x}_{p}"] for x in ("Scale_Out___FT", "Scale_Out___PT", "Hz_Out", "Pump_BuildUp"))
            hold = df.groupby((hz != hz.shift()).cumsum()).cumcount()
            ok = (ft > 300) & (pt < 60000) & (hold >= 20)      # 같은 Hz 20틱(약 10초) 넘게 유지된 틱만
            circ = ok & (bu == 0) & (hz == circ_hz[p])
            b = ok & (bu == 1)
            rows.append({"day": d, "pump": p, "circ_PT": pt[circ].median(), "circ_FT": ft[circ].median(),
                         "bu_PT": pt[b].median(), "bu_PT_p99": pt[b].quantile(.99) if b.any() else np.nan,
                         "bu_FT": ft[b].median(), "PT_max_run": pt[(ft > 300) & (pt < 60000)].max()})
    R = pd.DataFrame(rows)
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    R.to_pickle(CACHE)
    return R


def sensitivity():
    """r_phys 가 몇 % 움직여야 창 알림·사이클 알림·진단 증거 0.5 가 되나 (다른 피처는 그대로라고 볼 때)."""
    m = json.load(open(META, encoding="utf-8"))
    rows = []
    for g, s in m["stats"].items():
        if s.get("n_fit", 0) < 300:
            continue
        u = 100 * s["scale"]["r_phys"] * s["zq"]["r_phys"]         # zn = 1 에 해당하는 r_phys 변화 %
        t = m["thresholds"][g]["alert"]
        rows.append({"group": g, "n_fit": s["n_fit"], "진단 증거 0.5": 0.5 * u, "zn 1": u,
                     "창 알림 (zmax 중앙값)": u * t["zmax_roll"], "사이클 알림 (zmax)": u * t["zmax"]})
    return pd.DataFrame(rows).set_index("group").round(1)


def elevation_vs_failure(R):
    R = R.copy()
    R["K"] = R.circ_PT / (R.circ_FT / 1000) ** 2
    base = R.groupby("pump")[["K", "bu_PT"]].quantile(0.25)        # 펌프마다 낮은 쪽 정상 수준
    R = R.join(base, on="pump", rsuffix="_b")
    R["dK"] = (R.K / R.K_b - 1) * 100
    R["dPT"] = (R.bu_PT / R.bu_PT_b - 1) * 100
    R["elev"] = (R.dK >= ELEV_PCT) | (R.dPT >= ELEV_PCT)
    R = R[R.K.notna()].copy()
    fails = [(p, pd.Timestamp(d)) for p, d in FAILURES]
    R["fail7"] = [any(fp == p and d < fd <= d + pd.Timedelta(days=7) for fp, fd in fails)
                  for p, d in zip(R.pump, R.day)]
    R = R[R.day <= pd.Timestamp(END) - pd.Timedelta(days=7)]
    t = pd.crosstab(R.elev, R.fail7)
    odds, p = fisher_exact(t.values)
    # 3 가동일 이상 이어진 상승 구간
    segs = []
    for pump, g in R.sort_values("day").groupby("pump"):
        run = None
        for r in g.itertuples():
            if r.elev:
                run = run or {"pump": pump, "start": r.day.date(), "days": 0, "dK": [], "dPT": []}
                run["end"] = r.day.date()
                run["days"] += 1
                run["dK"].append(r.dK)
                run["dPT"].append(r.dPT)
            elif run:
                segs.append(run)
                run = None
        if run:
            segs.append(run)
    S = pd.DataFrame([{"pump": s["pump"], "start": s["start"], "end": s["end"], "days": s["days"],
                       "순환K %": np.nanmedian(s["dK"]), "증압PT %": np.nanmedian(s["dPT"])} for s in segs])
    S = S[S.days >= 3].copy()
    S["뒤 7일 안 고장"] = [next((v for (fp, fd), v in FAILURES.items() if fp == r.pump and
                              pd.Timestamp(r.start) < pd.Timestamp(fd) <= pd.Timestamp(r.end) + pd.Timedelta(days=7)), "")
                         for r in S.itertuples()]
    return t, odds, p, S.round(1)


def trend_tests(R, lam=0.3):
    R = R.copy()
    R["K"] = R.circ_PT / (R.circ_FT / 1000) ** 2
    out = []
    for p, g in R[R.K.notna()].groupby("pump"):
        s = g.set_index("day").K
        ref = s[REF[0]:REF[1]].median()
        x = (s / ref - 1) * 100
        # 날짜 간 흔들림 — 연속한 날 차이의 중앙값 (계단에 끌려가지 않게, 컨베이어 unit_judge 와 같은 방식)
        sig = max(1.4826 * np.median(np.abs(np.diff(x.values))) / np.sqrt(2), 0.5)
        after = x.index > pd.Timestamp(REF[1])
        e = x.ewm(alpha=lam, adjust=False).mean()
        lim = 3 * sig * np.sqrt(lam / (2 - lam))
        ew = e[(e.abs() > lim) & after]
        cp = cm = 0.0
        cus = None
        for t, v in x[after].items():
            cp, cm = max(0, cp + v - 0.5 * sig), max(0, cm - v - 0.5 * sig)
            if cus is None and (cp > 5 * sig or cm > 5 * sig):
                cus = f"{t.date()} {'+' if cp > 5 * sig else '-'}"
        last = x.iloc[-14:]
        tt = (last.index - last.index[0]).days.values
        out.append({"pump": p, "기준 K": round(ref, 1), "날짜 간 σ %": round(sig, 2),
                    "최근 5일 %": round(x.iloc[-5:].median(), 1),
                    "EWMA 첫 이탈": ew.index[0].date() if len(ew) else "", "CUSUM 첫 경보": cus or "",
                    "최근 14일 기울기 %/주": round(theilslopes(last.values, tt)[0] * 7, 2),
                    "Mann-Kendall p": round(kendalltau(tt, last.values)[1], 3),
                    "최대 하루 변화 %": round(x.diff().abs()[after].max(), 1),
                    "순환 PT 한 칸 %": round(100 / g.circ_PT.median(), 1)})
    return pd.DataFrame(out).set_index("pump")


def main():
    pd.set_option("display.width", 250)
    R = daily_table()
    R["day"] = pd.to_datetime(R["day"])
    print("== 1. 펌프별 절대 수준 (가동일 중앙값)")
    a = R.groupby("pump")[["circ_PT", "bu_PT", "bu_PT_p99", "PT_max_run", "bu_FT"]].median()
    print(a.round(1).sort_values("bu_PT", ascending=False).to_string())
    print("\n== 2. 모델이 반응하는 r_phys 변화 (%)")
    print(sensitivity().to_string())
    t, odds, p, S = elevation_vs_failure(R)
    print(f"\n== 3. 상승일 vs 7일 안 고장 (펌프-일)\n{t}\n오즈비 {odds:.1f}, Fisher p {p:.3f}"
          " — 같은 사건의 연속일이 겹쳐 있어 p 는 과소평가")
    print(S.to_string())
    print("\n== 4. 추세 방법 (일별 순환 K, 기준 %s~%s)" % REF)
    print(trend_tests(R).to_string())
    return 0


if __name__ == "__main__":
    sys.exit(main())
