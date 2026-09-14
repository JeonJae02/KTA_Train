"""탱크별 변수 간 상관관계 분석 — 온도 / 수위 / heat / cool / exchanger / feeding.

**왜 단순 상관행렬을 그대로 쓰면 안 되는가.**
이 설비의 액추에이터는 온도를 보고 켜지는 **폐루프 제어**다. 그래서 같은 시점끼리
상관을 재면 인과가 거꾸로 나온다 — 실측으로 8개 탱크 전부 `corr(heat, 온도)` 가
음수(-0.11 ~ -0.84)다. "히터를 켜서 식었다"가 아니라 "식었으니까 히터를 켰다"인데,
상관계수는 이 둘을 구분하지 못한다. 같은 데이터로 `corr(heat, ΔT)` 를 재면 전부
양수(+0.07 ~ +0.73)로 부호가 뒤집힌다.

그래서 네 가지를 같이 낸다.

1. **수준(level) 상관 vs 변화량(ΔT) 상관을 나란히** — 위 함정을 눈으로 보게 한다.
   판단에 쓸 것은 변화량 쪽이다.
2. **시차 교차상관(CCF)** — corr(u(t-lag), ΔT(t)) 를 lag 을 바꿔가며 잰다. 열은
   즉시 전달되지 않으므로 상관이 최대가 되는 lag 이 곧 **열 지연 시간**이고,
   lag<0 쪽이 높으면 "온도가 먼저 움직여서 액추에이터가 반응한 것"(제어),
   lag>0 쪽이 높으면 "액추에이터가 먼저 움직여서 온도가 따라온 것"(물리)이다.
   **인과 방향을 구분하는 것이 이 분석의 핵심이다.**
3. **효과 크기(평균차 · Cohen's d)** — heat 처럼 98% 켜져 있는 이진 변수는 상관계수
   자체가 계급 불균형에 눌려 기계적으로 작아진다. "켜졌을 때와 꺼졌을 때 ΔT 평균이
   얼마나 다른가"가 훨씬 정직하다.
4. **편상관(partial correlation)** — 모든 변수가 제어 로직으로 엮여 있어서, 나머지를
   고정했을 때도 관계가 남는지 봐야 직접 관계와 간접 관계가 갈린다.

**표본 수 주의.** 5초 격자로 5만 행이 넘지만 인접 샘플은 거의 같은 값이라
독립 표본이 아니다. p-value 는 의미가 없어서 아예 내지 않는다. 상관계수의
**크기**만 보고, 유의성은 따지지 않는다.

정지 구간(9/13 종일 · 매일 00~07시)과 알람 구간은 ARX 모델과 동일하게 제외한다.
"""

import glob
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

matplotlib.rcParams["font.family"] = "Malgun Gothic"
matplotlib.rcParams["axes.unicode_minus"] = False

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from Tank_Temp_Factor_Analysis import TANKS, WAGON_TAG

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SAVE_DIR = os.path.join(BASE_DIR, "extracted_csv")
OUT_DIR = os.path.join(BASE_DIR, "analysis_out", "correlation")

GRID_SEC = 5
HORIZON_SEC = 60
STEPS = HORIZON_SEC // GRID_SEC
RUNNING_WINDOW_MIN = 30
EXCLUDE_HOURS = (0, 7)
LAG_MAX_SEC = 600           # 교차상관을 볼 시차 범위 (±10분)

VARS = ["T", "level", "heat", "cool", "exch", "feed"]


def load_clean(tank_key):
    """5초 격자 + 정지/알람 구간 제외. 수준값과 변화량을 같이 돌려준다."""
    cfg = TANKS[tank_key]
    path = sorted(glob.glob(os.path.join(SAVE_DIR, f"{tank_key}_Temp_vs_Factors_*.csv")))[-1]
    raw = pd.read_csv(path, index_col="Time", parse_dates=True).sort_index()

    num = pd.DataFrame({
        "T": raw[cfg["temp"]], "level": raw[cfg["level"]],
        "heat": raw[cfg["heat"]], "cool": raw[cfg["cool"]],
        "exch": raw[cfg["exchanger"]], "feed": raw[cfg["feeding"]],
        "wagon": raw[WAGON_TAG],
        "diag": (raw["source"] == "DIAGNOSTIC").astype(float),
    })
    g = num.resample(f"{GRID_SEC}s").ffill().dropna()

    moved = (g["wagon"].diff().abs() > 0).astype(float)
    running = moved.rolling(int(RUNNING_WINDOW_MIN * 60 / GRID_SEC), min_periods=1).max()
    hour = g.index.hour
    ok = (((hour < EXCLUDE_HOURS[0]) | (hour >= EXCLUDE_HOURS[1])) & (g["diag"] == 0)).astype(float) * running

    g = g[ok == 1].drop(columns=["wagon", "diag"])
    g["dT"] = g["T"].shift(-STEPS) - g["T"]
    g["dlevel"] = g["level"].shift(-STEPS) - g["level"]
    return g.dropna()


def partial_corr(df):
    """정밀도 행렬(공분산 역행렬)로 편상관을 구한다 — 나머지 변수를 고정한 관계."""
    c = df.corr().values
    p = np.linalg.pinv(c)
    d = np.sqrt(np.outer(np.diag(p), np.diag(p)))
    out = -p / d
    np.fill_diagonal(out, 1.0)
    return pd.DataFrame(out, index=df.columns, columns=df.columns)


def heatmap(ax, m, title):
    im = ax.imshow(m.values, cmap="RdBu_r", vmin=-1, vmax=1)
    ax.set_xticks(range(len(m.columns)))
    ax.set_xticklabels(m.columns, rotation=45, ha="right", fontsize=8)
    ax.set_yticks(range(len(m.index)))
    ax.set_yticklabels(m.index, fontsize=8)
    for i in range(len(m.index)):
        for j in range(len(m.columns)):
            v = m.values[i, j]
            ax.text(j, i, f"{v:.2f}", ha="center", va="center", fontsize=7,
                    color="white" if abs(v) > 0.55 else "black")
    ax.set_title(title, fontsize=10)
    return im


def ccf(x, y, max_lag_steps):
    """corr(x(t-lag), y(t)) 를 lag 별로. lag>0 이면 x 가 먼저 움직인 것."""
    lags = range(-max_lag_steps, max_lag_steps + 1)
    return [x.shift(l).corr(y) for l in lags], [l * GRID_SEC for l in lags]


def analyze(tank_key):
    d = load_clean(tank_key)
    cfg = TANKS[tank_key]

    lv = d[VARS]                                     # 수준
    ch = d[["dT", "dlevel", "heat", "cool", "exch", "feed"]]   # 변화량

    fig, axes = plt.subplots(1, 3, figsize=(17, 4.6))
    heatmap(axes[0], lv.corr(), f"{tank_key} — 수준 상관 (함정: 제어 때문에 부호 뒤집힘)")
    heatmap(axes[1], ch.corr(), f"{tank_key} — 변화량 상관 (이쪽을 보세요)")
    heatmap(axes[2], partial_corr(ch), f"{tank_key} — 변화량 편상관 (나머지 고정)")
    fig.tight_layout()
    os.makedirs(OUT_DIR, exist_ok=True)
    fig.savefig(os.path.join(OUT_DIR, f"{tank_key}_corr.png"), dpi=120)
    plt.close(fig)

    # 시차 교차상관 — 인과 방향 판별
    max_steps = LAG_MAX_SEC // GRID_SEC
    fig, ax = plt.subplots(figsize=(11, 4.2))
    for name, color in [("heat", "#d62728"), ("exch", "#2ca02c"),
                        ("feed", "#9467bd"), ("dlevel", "#ff7f0e")]:
        vals, lags = ccf(d[name], d["dT"], max_steps)
        ax.plot(np.array(lags) / 60, vals, color=color, linewidth=1.4, label=name)
    ax.axvline(0, color="#888888", linewidth=1, linestyle="--")
    ax.axhline(0, color="#888888", linewidth=0.8)
    ax.set_xlabel("시차 (분) —  오른쪽(+)=요소가 먼저 움직임(물리),  왼쪽(-)=온도가 먼저(제어 반응)")
    ax.set_ylabel("corr(요소(t-lag), ΔT(t))")
    ax.set_title(f"{tank_key} — 시차 교차상관")
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT_DIR, f"{tank_key}_ccf.png"), dpi=120)
    plt.close(fig)

    # 효과 크기 — 이진 변수는 상관계수보다 평균차가 정직하다
    rows = []
    for name in ["heat", "exch", "feed"]:
        on, off = d.loc[d[name] == 1, "dT"], d.loc[d[name] == 0, "dT"]
        if len(on) < 50 or len(off) < 50:
            rows.append({"탱크": tank_key, "요소": name, "ON%": round(d[name].mean() * 100, 1),
                         "ΔT_ON": None, "ΔT_OFF": None, "평균차": None, "Cohen_d": None})
            continue
        pooled = np.sqrt((on.var() * (len(on) - 1) + off.var() * (len(off) - 1)) / (len(on) + len(off) - 2))
        rows.append({"탱크": tank_key, "요소": name, "ON%": round(d[name].mean() * 100, 1),
                     "ΔT_ON": round(on.mean(), 3), "ΔT_OFF": round(off.mean(), 3),
                     "평균차": round(on.mean() - off.mean(), 3),
                     "Cohen_d": round((on.mean() - off.mean()) / pooled, 3) if pooled > 0 else None})

    # 최적 시차 (상관이 가장 큰 지점)
    best = {}
    for name in ["heat", "exch", "feed", "dlevel"]:
        vals, lags = ccf(d[name], d["dT"], max_steps)
        i = int(np.nanargmax(np.abs(vals)))
        best[name] = (lags[i], round(vals[i], 3))

    return rows, best, ch.corr(), len(d)


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    all_rows, best_rows = [], []

    for tk in TANKS:
        rows, best, cm, n = analyze(tk)
        all_rows.extend(rows)
        r = {"탱크": tk, "표본": n}
        for name, (lag, v) in best.items():
            r[f"{name}_최적시차초"] = lag
            r[f"{name}_최대상관"] = v
        r["corr(ΔT,Δlevel)"] = round(cm.loc["dT", "dlevel"], 3)
        best_rows.append(r)

    eff = pd.DataFrame(all_rows)
    bst = pd.DataFrame(best_rows)
    eff.to_csv(os.path.join(OUT_DIR, "효과크기.csv"), index=False, encoding="utf-8-sig")
    bst.to_csv(os.path.join(OUT_DIR, "최적시차.csv"), index=False, encoding="utf-8-sig")

    print("=== 요소별 효과 크기 (ΔT 60초 기준) ===")
    print(eff.to_string(index=False))
    print()
    print("=== 시차 교차상관이 최대가 되는 지점 (+면 요소가 먼저, -면 온도가 먼저) ===")
    print(bst.to_string(index=False))
    print(f"\n그림: {OUT_DIR}\\<탱크>_corr.png, <탱크>_ccf.png")
    return eff, bst


if __name__ == "__main__":
    main()
