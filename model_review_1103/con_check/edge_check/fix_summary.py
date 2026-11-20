"""fix_check.py 결과 요약 — 켜짐 길이 경고가 줄었나, 진짜 신호(재감지·TP 모양 칸)는 남았나."""
import pickle
import pandas as pd

OUT = "/Users/jeonjae/Desktop/KTA/DB_Log/model_review_1103/con_check/edge_check/out"
RUNS = ["V0_throttle", "V1_throttle", "W6_throttle", "V0_db", "V1_db"]
WIDTH = {"이물(앞)", "이물(뒤)", "이물(기타)"}
R = {}
for r in RUNS:
    try:
        R[r] = pickle.load(open(f"{OUT}/fix_{r}.pkl", "rb"))
    except FileNotFoundError:
        pass


def prod_day(t):
    return (pd.Timestamp(t) - pd.Timedelta(hours=6)).normalize()


print("## 1. 경고 칸 수 (가동일 × 센서 × 대차, 점검 추천 이상)")
rows = []
for r, d in R.items():
    e = d["evals"].copy()
    e["day"] = e.t.map(prod_day)
    cell = e[e.kind == "cell"]
    w = cell[cell.state.isin(WIDTH)][["day", "sensor", "wagon"]].drop_duplicates()
    rr = cell[cell.state == "이물(분리)"][["day", "sensor", "wagon"]].drop_duplicates()
    fl = e[e.kind == "flag"][["day", "sensor", "state"]].drop_duplicates()
    rows.append(dict(run=r, 켜짐길이=len(w), 재감지=len(rr), 센서플래그=len(fl),
                     켜짐길이_하루평균=round(len(w) / e.day.nunique(), 1)))
print(pd.DataFrame(rows).to_string(index=False))

print("\n## 2. 진짜 신호가 남았나 (가동일 수)")
CHECK = [("P00164", 20, "P00164 대차 20 (현장확인)"), ("P00459", 9, "P00459 대차 9 (현장확인)"),
         ("P00451", 35, "P00451 대차 35 (C06)"), ("P00141", 18, "P00141 대차 18 (켜짐 길이 증가, 보류)")]
rows = []
for r, d in R.items():
    e = d["evals"].copy()
    e["day"] = e.t.map(prod_day)
    row = dict(run=r)
    for s, w, name in CHECK:
        g = e[(e.kind == "cell") & (e.sensor == s) & (e.wagon == w)]
        row[name] = f"{g.day.nunique()}일 " + "/".join(sorted(g.state.unique()))
    f = e[(e.kind == "flag") & (e.sensor == "P00451") & (e.state == "센서 재감지")]
    row["P00451 센서 재감지 (C03) 마지막 날"] = f.day.max().strftime("%m-%d") if len(f) else "-"
    rows.append(row)
print(pd.DataFrame(rows).set_index("run").T.to_string())

print("\n## 3. 하루 칸 켜짐 길이 — 스로틀(엣지) vs DB 같은 비율")
for v in ["V0", "V1"]:
    a, b = R.get(f"{v}_throttle"), R.get(f"{v}_db")
    if a is None or b is None:
        continue
    m = a["daily"].merge(b["daily"], on=["day", "sensor", "wagon"], suffixes=("_t", "_d"))
    dw = m.width_t - m.width_d
    print(f"{v}: 칸 {len(m)} · 같음 {(dw.abs() < .01).mean():.1%} · 엣지가 김 {(dw > .01).mean():.1%} · "
          f"엣지가 짧음 {(dw < -.01).mean():.1%} · 켜짐 길이 잡힌 비율 {(m.width_n_t / m.n_t).mean():.3f} / {(m.width_n_d / m.n_d).mean():.3f}")
