"""cool 이 exchanger 랑 같이 켜졌을 때, 그 사이클의 온도가 다른(cool 안 겹친)
사이클보다 높았는지 비교한다 — "exchanger 혼자로는 못 잡아서 cool 까지
동원했다"는 가설을 데이터로 확인.

지표: 각 ON 사이클 구간 [cycle_start, cycle_end] 에서
  - max_temp   = 그 구간 동안의 최고온도
  - overshoot  = max_temp - SV  (SV 를 얼마나 초과했는지 — exchanger 가
    못 따라간 정도)

장기구간(09-09~지금, 1253개 사이클) 전체로 비교한다.

사용법 (TK_Temp_PV_P1/TK_Temp_SV_P1 장기구간 CSV 가 이미 받아져 있어야 함):
    PYTHONIOENCODING=utf-8 python Exchanger_Cool_Overlap_Temp_Comparison.py
"""

import glob
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy import stats

matplotlib.rcParams["font.family"] = "Malgun Gothic"
matplotlib.rcParams["axes.unicode_minus"] = False

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RAW_DIR = os.path.join(BASE_DIR, "parkkt", "extracted_csv")
OUT_DIR = os.path.join(BASE_DIR, "parkkt", "analysis_out", "exchanger_off_on_gap")
CYCLES_CSV = os.path.join(OUT_DIR, "cycles_long_range_09-09_to_now.csv")

TEMP_TAG = "TK_Temp_PV_P1"
SV_TAG = "TK_Temp_SV_P1"
TEMP_SCALE = 10.0

COLOR_WITH = "#D55E00"
COLOR_WITHOUT = "#0072B2"


def latest_csv_with(*tags):
    cands = sorted(glob.glob(os.path.join(RAW_DIR, "*_analysis.csv")))
    for p in reversed(cands):
        cols = pd.read_csv(p, nrows=0).columns
        if all(t in cols for t in tags):
            return p
    return None


def load_temp_sv():
    path = latest_csv_with(TEMP_TAG, SV_TAG)
    print(f"온도 CSV: {path}")
    raw = pd.read_csv(path, index_col="Time", parse_dates=True, usecols=["Time", TEMP_TAG, SV_TAG])
    raw = raw[~raw.index.duplicated(keep="last")].sort_index()
    temp = (raw[TEMP_TAG] / TEMP_SCALE).dropna()
    sv = (raw[SV_TAG] / TEMP_SCALE).dropna()
    return temp, sv


def load_cycles():
    return pd.read_csv(CYCLES_CSV, parse_dates=["cycle_start", "cycle_end", "next_on"])


def compute_overshoot(cycles, temp, sv):
    max_temps, svs, overshoots = [], [], []
    for _, r in cycles.iterrows():
        seg = temp.loc[r["cycle_start"]:r["cycle_end"]]
        if len(seg) == 0:
            max_temps.append(np.nan); svs.append(np.nan); overshoots.append(np.nan)
            continue
        sv_seg = sv.reindex(seg.index, method="ffill")
        mx = seg.max()
        sv_at_max = sv_seg.loc[seg.idxmax()]
        max_temps.append(mx)
        svs.append(sv_at_max)
        overshoots.append(mx - sv_at_max)
    cycles = cycles.copy()
    cycles["max_temp"] = max_temps
    cycles["sv"] = svs
    cycles["overshoot"] = overshoots
    return cycles


def report_and_plot(df):
    df = df.dropna(subset=["overshoot"])
    n_edge = int(df["edge_case"].sum())
    edge_rows = df[df["edge_case"]]
    print(f"첫가동/셧다운직전 경계케이스 {n_edge}개 제외 (그중 overshoot 상위: "
          f"{edge_rows['overshoot'].max():.1f}°C — 데이터 구간 경계에서 생긴 왜곡이라 뺀다)\n")
    df = df[~df["edge_case"]]

    with_cool = df[df["cool_overlap"]]
    without_cool = df[~df["cool_overlap"]]

    print(f"전체 {len(df)}개 — cool 겹침 {len(with_cool)}개, 안겹침 {len(without_cool)}개\n")

    print("=== overshoot(=최고온도-SV) 분포 비교 ===")
    for name, grp in (("cool 겹침", with_cool), ("cool 안겹침", without_cool)):
        d = grp["overshoot"]
        print(f"  {name}: n={len(d)} 중앙값={d.median():.3f}°C 평균={d.mean():.3f}°C "
              f"표준편차={d.std():.3f}°C 범위={d.min():.3f}~{d.max():.3f}°C")

    u, p_u = stats.mannwhitneyu(with_cool["overshoot"], without_cool["overshoot"], alternative="two-sided")
    t, p_t = stats.ttest_ind(with_cool["overshoot"], without_cool["overshoot"], equal_var=False)
    pooled = np.sqrt((with_cool["overshoot"].var() + without_cool["overshoot"].var()) / 2)
    d_cohen = (with_cool["overshoot"].mean() - without_cool["overshoot"].mean()) / pooled
    print(f"\n  Mann-Whitney p={p_u:.3g}, Welch t-test p={p_t:.3g}, Cohen's d={d_cohen:.2f}")

    # overshoot 기준으로 "cool 이 필요했을 법한" 비율을 가늠 — ROC 스타일 분리력 확인
    thr_candidates = np.percentile(df["overshoot"], [50, 75, 90, 95])
    print("\n  overshoot 임계값별 '그 이상'인 비율:")
    for thr in thr_candidates:
        rate_with = (with_cool["overshoot"] >= thr).mean() * 100
        rate_without = (without_cool["overshoot"] >= thr).mean() * 100
        print(f"    임계 {thr:.3f}°C 이상 — cool겹침군 {rate_with:.1f}% vs 안겹침군 {rate_without:.1f}%")

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 6))

    bins = np.linspace(df["overshoot"].min(), df["overshoot"].max(), 40)
    ax1.hist(without_cool["overshoot"], bins=bins, color=COLOR_WITHOUT, alpha=0.6, density=True,
             label=f"cool 안겹침 (n={len(without_cool)})")
    ax1.hist(with_cool["overshoot"], bins=bins, color=COLOR_WITH, alpha=0.6, density=True,
             label=f"cool 겹침 (n={len(with_cool)})")
    ax1.set_xlabel("overshoot = 최고온도 - SV (°C)")
    ax1.set_ylabel("밀도")
    ax1.set_title("overshoot 분포 비교 (정규화)", loc="left", fontsize=11)
    ax1.legend(frameon=False, fontsize=9)
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)

    bp = ax2.boxplot([without_cool["overshoot"], with_cool["overshoot"]], positions=[0, 1], widths=0.5,
                      showmeans=True, patch_artist=True)
    for patch, color in zip(bp["boxes"], [COLOR_WITHOUT, COLOR_WITH]):
        patch.set(facecolor=color, alpha=0.3, edgecolor=color, linewidth=1.3)
    for i, d in enumerate([without_cool["overshoot"], with_cool["overshoot"]]):
        x = np.random.default_rng(0).normal(i, 0.06, size=len(d))
        ax2.scatter(x, d, s=10, color=[COLOR_WITHOUT, COLOR_WITH][i], alpha=0.4, linewidths=0, zorder=3)
    ax2.set_xticks([0, 1])
    ax2.set_xticklabels([f"cool 안겹침\n(n={len(without_cool)})", f"cool 겹침\n(n={len(with_cool)})"])
    ax2.set_ylabel("overshoot (°C)")
    ax2.set_title("박스플롯 비교", loc="left", fontsize=11)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)

    fig.suptitle("cool 겹침 여부에 따른 overshoot(최고온도-SV) 비교", fontsize=13)
    fig.tight_layout()
    out_path = os.path.join(OUT_DIR, "cool_overlap_vs_overshoot.png")
    fig.savefig(out_path, dpi=140)
    plt.close(fig)
    return out_path


def main():
    temp, sv = load_temp_sv()
    cycles = load_cycles()
    cycles = compute_overshoot(cycles, temp, sv)
    out_path = report_and_plot(cycles)
    print(f"\n그래프 저장: {out_path}")

    csv_out = os.path.join(OUT_DIR, "cycles_long_range_with_overshoot.csv")
    cycles.to_csv(csv_out, index=False, encoding="utf-8-sig")
    print(f"결과 표 저장: {csv_out}")


if __name__ == "__main__":
    main()
