"""유닛 판정: 하루 마감 vs '최근 근무 구간 8개(= 하루치) 슬라이딩, 근무 구간 끝마다 갱신'.

근무 구간: operation.json shifts 8개 (06:50 · 09:00 · 11:30 · 13:40 · 15:30 · 17:40 · 20:10 · 22:20 시작).
휴게·중식 중 사이클은 앞 구간에 넣는다. 사이클 200개 미만 구간(주말·정지)은 창에 넣지 않는다.
기준·σ 는 HDI_MODEL 수정본 UNIT_REF. 판정 |z| ≥ 5.

1) 오경보: 09-22 ~ 11-14 실제 데이터에서 |z| ≥ 5 횟수 (기준 고정, 승인 규칙 없이)
2) 앞선 시간: 실제 사이클에 시각 T 뒤로 +kσ 계단을 넣고, T 부터 첫 |z| ≥ 5 까지 걸린 시간
"""
import glob, sys
import numpy as np, pandas as pd

sys.path.insert(0, "/Users/jeonjae/Desktop/KTA/DB_Log/HDI_MODEL")
import importlib.util
spec = importlib.util.spec_from_file_location("up", "/Users/jeonjae/Desktop/KTA/DB_Log/HDI_MODEL/conveyor_models/unit_params.py")
src = open(spec.origin).read().replace("from config.topology_config import RELAY_CONFIG", "RELAY_CONFIG = {}").split("def bits(")[0]
ns = {}; exec(src, ns); UNIT_REF = ns["UNIT_REF"]

OUT = "/Users/jeonjae/Desktop/KTA/DB_Log/model_review_1103/con_check/stepwise/out"
D = pd.concat([pd.read_pickle(f) for f in glob.glob(OUT + "/unit_cycles_*.pkl")]).sort_values("t").drop_duplicates(["unit", "t"])
D = D[(D.t >= "2026-09-22 06:00") & (D.t < "2026-11-15 06:00")].reset_index(drop=True)
STARTS = [(6, 50), (9, 0), (11, 30), (13, 40), (15, 30), (17, 40), (20, 10), (22, 20)]


def block_end(t):
    """그 사이클이 속한 근무 구간이 끝나는 시각(= 다음 구간 시작)."""
    day = t.normalize()
    cands = [day + pd.Timedelta(hours=h, minutes=m) for h, m in STARTS] + [day + pd.Timedelta(days=1, hours=6, minutes=50)]
    prev = day - pd.Timedelta(days=1) + pd.Timedelta(hours=22, minutes=20)
    for c in cands:
        if t < c:
            return c
    return cands[-1]


D["bend"] = D.t.map(block_end)
D["pday"] = (D.t - pd.Timedelta(hours=6)).dt.normalize()


def series(g, col, shift=None):
    """(판정 시각, z) — 하루 마감과 8구간 슬라이딩."""
    x = g[col].astype(float)
    if shift is not None:
        x = x + np.where(g.t >= shift[0], shift[1], 0.0)
    ref, sig = UNIT_REF[g.unit.iloc[0]][col]
    df = pd.DataFrame(dict(x=x.values, bend=g.bend.values, pday=g.pday.values)).dropna()
    # 하루 마감 (사이클 1000개 이상인 날, 다음 날 06:00 에 판정)
    d = df.groupby("pday").x.agg(["sum", "count"])
    d = d[d["count"] >= 1000]
    daily = pd.Series(((d["sum"] / d["count"]) - ref).values / sig, index=d.index + pd.Timedelta(days=1, hours=6))
    # 근무 구간 슬라이딩 (최근 8개 유효 구간)
    b = df.groupby("bend").x.agg(["sum", "count"])
    b = b[b["count"] >= 200]
    s, c = b["sum"].rolling(8).sum(), b["count"].rolling(8).sum()
    roll = ((s / c - ref) / sig).dropna()
    return daily, roll


METRICS = [(u, m) for u in UNIT_REF for m in UNIT_REF[u] if m in D.columns and D[D.unit == u][m].notna().sum() > 1000]

# 1) 오경보
fa = {"하루 마감": [0, 0], "8구간 슬라이딩": [0, 0]}
for u, m in METRICS:
    g = D[D.unit == u]
    daily, roll = series(g, m)
    fa["하루 마감"][0] += len(daily); fa["하루 마감"][1] += (daily.abs() >= 5).sum()
    fa["8구간 슬라이딩"][0] += len(roll); fa["8구간 슬라이딩"][1] += (roll.abs() >= 5).sum()
print("1) 실제 데이터, |z| ≥ 5")
for k, (n, a) in fa.items():
    print(f"   {k:10s} 판정 {n:5d}번 · 넘음 {a}번")

# 2) 앞선 시간 — 계단 크기 k σ, 계단 시각 여러 개
rng = np.random.default_rng(0)
days = sorted(D.pday.unique())[5:-3]
starts = []
for d in rng.choice(days, 12, replace=False):
    for hh in (8, 12, 17, 21):
        starts.append(pd.Timestamp(d) + pd.Timedelta(hours=hh, minutes=15))
rows = []
for u, m in [("md_fwd", "stop_rev_time"), ("md_rev", "nozzle_rev_time"), ("airbag", "stop_fwd_time"), ("md_fwd", "drag_cnt")]:
    g = D[D.unit == u]
    sig = UNIT_REF[u][m][1]
    for k in (5, 7, 10, 20, 50):
        for T in starts:
            daily, roll = series(g, m, shift=(T, k * sig))
            for name, z in (("하루 마감", daily), ("8구간 슬라이딩", roll)):
                hit = z[(z.index > T) & (z.abs() >= 5)]
                rows.append(dict(metric=f"{u}.{m}", k=k, how=name, T=T,
                                 delay_h=(hit.index[0] - T).total_seconds() / 3600 if len(hit) else np.nan))
R = pd.DataFrame(rows)
print("\n2) 계단 +kσ 를 넣고 첫 경고까지 (시간, 중앙값 · 최대 · 못 잡음)")
t = R.groupby(["k", "how"]).delay_h.agg(중앙값="median", 최대="max", 못잡음=lambda x: x.isna().sum()).round(1).unstack("how")
print(t.to_string())
R.to_pickle(OUT + "/rolling_delay.pkl")
