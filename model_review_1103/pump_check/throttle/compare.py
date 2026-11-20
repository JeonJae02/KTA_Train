"""replay.py 결과 비교 — DB 입력 vs 스로틀 입력.

1. 사이클 짝짓기: 같은 펌프, 시작 시각 1.5초 안 (스로틀은 켜짐을 한 메시지 늦게 볼 수 있다)
2. 피처 차이: (스로틀 − DB) ÷ 정상 한계(묶음 scale × zq). ±1 = 그 피처의 정상 한계 전부
3. 짝이 없는 사이클: 어느 쪽에서 왜 빠졌나 (판정 mode)
4. 사이클 등급 · 경보: 이유별 수, 엣지 로그(pump_warning_mse.csv raised)와 겹침

사용: venv/bin/python model_review_1103/pump_check/throttle/compare.py
"""
import glob
import json
import pickle

import numpy as np
import pandas as pd

ROOT = "/Users/jeonjae/Desktop/KTA/DB_Log/model_review_1103"
OUT = ROOT + "/pump_check/throttle/out"
META = json.load(open(ROOT + "/v2_2_1/backend/models/pump_cycle/meta.json"))
FEATURES = META["features"]
TIMING = ["t_self", "decel_half_sec", "release_half_sec"]
pd.set_option("display.width", 220)
pd.set_option("display.max_columns", 30)


def load(mode):
    cy, al, n = [], [], {}
    for f in sorted(glob.glob(f"{OUT}/{mode}_*.pkl")):
        o = pickle.load(open(f, "rb"))
        c = o["cycles"].copy()
        c["day"] = o["day"]
        cy.append(c)
        if len(o["alerts"]):
            a = o["alerts"].copy()
            a["day"] = o["day"]
            al.append(a)
        n[o["day"]] = (o["n_in"], o["n_rows"])
    return pd.concat(cy, ignore_index=True), pd.concat(al, ignore_index=True), n


def limit(group, f):
    s = META["stats"].get(group)
    return s["scale"][f] * s["zq"][f] if s else np.nan


def pair(db, th):
    out = []
    for p in sorted(db.pump.unique()):
        a = db[db.pump == p].sort_values("t_start")
        b = th[th.pump == p].sort_values("t_start")
        m = pd.merge_asof(a, b, on="t_start", direction="forward", tolerance=pd.Timedelta(seconds=1.5),
                          suffixes=("_db", "_th"))
        out.append(m)
    return pd.concat(out, ignore_index=True)


def main():
    db, adb, ndb = load("db")
    th, ath, nth = load("throttle")
    days = sorted(ndb)
    print(f"가동일 {len(days)}: {days[0]} ~ {days[-1]}")
    print("스로틀이 넘긴 메시지 비율:", f"{sum(v[0] for v in nth.values()) / sum(v[1] for v in nth.values()):.3f}")

    # --- 사이클 수 · 판정 mode
    print("\n## 사이클 수 (판정 mode별)")
    t = pd.concat([db["mode"].value_counts().rename("DB"), th["mode"].value_counts().rename("스로틀")], axis=1)
    t["차이"] = t["스로틀"] - t["DB"]
    print(t)

    # --- 짝짓기
    m = pair(db, th)
    print(f"\nDB 사이클 {len(db)} 중 스로틀에 짝 있음 {m.mode_th.notna().sum()} ({m.mode_th.notna().mean():.3f})")
    lost = m[m.mode_th.isna()]
    print("짝 없는 DB 사이클의 DB mode:", lost.mode_db.value_counts().to_dict())
    both = m[(m.mode_db == "자동 운전") & m.mode_th.notna()]
    print("자동 운전 DB 사이클 짝의 스로틀 mode:", both.mode_th.value_counts().to_dict())
    same = both[both.mode_th == "자동 운전"].copy()
    same = same[same.group_db == same.group_th]

    # --- 피처 차이 (정상 한계 대비)
    print(f"\n## 피처 차이 — 둘 다 자동 운전·같은 묶음 {len(same)} 사이클, (스로틀 − DB) ÷ 정상 한계")
    rows = []
    for f in FEATURES:
        lim = same.group_db.map(lambda g: limit(g, f))
        d = (same[f"{f}_th"] - same[f"{f}_db"]) / lim
        ok = d.notna()
        nan_th = (same[f"{f}_db"].notna() & same[f"{f}_th"].isna()).mean()
        rows.append({"피처": f, "같음(차이 0)": (d[ok] == 0).mean(), "|차이|>0.1": (d[ok].abs() > 0.1).mean(),
                     "|차이|>0.5": (d[ok].abs() > 0.5).mean(), "|차이|>1": (d[ok].abs() > 1).mean(),
                     "평균(치우침)": d[ok].mean(), "p95 |차이|": d[ok].abs().quantile(0.95),
                     "스로틀만 결측": nan_th})
    print(pd.DataFrame(rows).set_index("피처").round(3))

    # 타이밍 피처: 원래 단위(초)
    print("\n타이밍 피처 차이 (초)")
    for f in TIMING:
        d = (same[f"{f}_th"] - same[f"{f}_db"]).dropna()
        print(f"  {f:18s} 평균 {d.mean():+.3f}  중앙 {d.median():+.3f}  p5 {d.quantile(.05):+.3f}  p95 {d.quantile(.95):+.3f}"
              f"  0이 아닌 비율 {(d != 0).mean():.3f}")

    # --- 사이클 등급 · AE 점수
    print("\n## 사이클 등급 (자동 운전)")
    for name, c in (("DB", db), ("스로틀", th)):
        a = c[c["mode"] == "자동 운전"]
        print(f"  {name}: 주의 이상 {(a.level != '정상').mean():.4f} · 알림 {(a.level == '알림').mean():.4f} · "
              f"AE 점수 중앙 {a.score.median():.3f} · p99 {a.score.quantile(.99):.2f} · zmax p99 {a.zmax.quantile(.99):.2f}")
    print("  같은 사이클 AE 점수 비 (스로틀/DB) 중앙", (same.score_th / same.score_db).median().round(3),
          "p95", (same.score_th / same.score_db).quantile(.95).round(3))

    # --- 경보
    print("\n## 사람에게 간 경보 (show=True) — 이유별")
    t = pd.concat([adb.reason.value_counts().rename("DB"), ath.reason.value_counts().rename("스로틀")], axis=1).fillna(0)
    t.loc["합계"] = t.sum()
    print(t.astype(int))

    # 엣지 로그와 비교 — (날짜, 펌프, 이유) 집합
    e = pd.read_csv(ROOT + "/pump_check/pump_warning_mse.csv")
    e = e[e.event == "raised"].copy()
    e["day"] = pd.to_datetime(e.ts).dt.strftime("%Y-%m-%d")
    e = e[e.day.isin(days) & e.reason.str.startswith("pump_")]
    key = lambda d: set(zip(d.day, d.pump, d.reason))  # noqa: E731
    ex = lambda a: a.explode("pumps").assign(pump=lambda x: x.pumps)  # noqa: E731  헤드 경보는 두 펌프에 뜬다
    E, D, T = key(e), key(ex(adb)), key(ex(ath))
    print(f"\n엣지 raised (같은 날들) {len(e)}건, (날짜·펌프·이유) {len(E)}개")
    for name, S in (("DB", D), ("스로틀", T)):
        print(f"  {name}: {len(S)}개 · 엣지와 겹침 {len(S & E)} · 엣지에만 {len(E - S)} · 재생에만 {len(S - E)}")
    print("  엣지에만 (스로틀 기준):", sorted(E - T)[:30])
    print("  스로틀에만:", sorted(T - E)[:30])
    pickle.dump({"pairs": m, "same": same}, open(f"{OUT}/pairs.pkl", "wb"))


if __name__ == "__main__":
    main()
