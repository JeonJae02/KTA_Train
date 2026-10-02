"""0(꺼짐) 상태가 얼마나 유지되는지(`off_duration_sec`) 범위를 다시 정리하고,
그 OFF 구간 동안 cool 이 같이 끼어 있었는지에 따라 차이가 있는지도 본다
(ON 쪽에서 했던 것과 같은 질문을 OFF 쪽에도 적용).

`merged_cycles_full.csv`(246개, 180초 임계값으로 채터링 제거) 기준.
09-28 05:21 시작 사이클(주말 지나고 생긴 특이 캐치업)은 그 사이클 **자신의**
off_duration_sec(그 다음 정상 OFF)과는 무관해서 여기선 제외하지 않는다 —
주말 예외는 ON 쪽 분석에서만 해당되는 문제였다.

사용법:
    PYTHONIOENCODING=utf-8 python Exchanger_Cooling_Overlap_Off_Duration.py
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
CYCLES_CSV = os.path.join(OUT_DIR, "merged_cycles_full.csv")

COOL_TAG = "워킹 Tank 1 BACK Cooling SOL"

#: 매일 밤 ~6.3~6.4시간짜리 셧다운 3건 — 이미 별도 카테고리로 확인된 것이라
#: "정상 냉각 OFF vs cool 겹침" 비교에서는 빼고 따로 본다.
SHUTDOWN_SEC = 3600

BUCKETS_SEC = [180, 300, 600, 900, 1200, 1800, 3600, 7200, 14400, float("inf")]
BUCKET_LABELS = ["3~5분", "5~10분", "10~15분", "15~20분", "20~30분",
                  "30~60분", "1~2시간", "2~4시간", "4시간 이상"]

COLOR_WITH = "#D55E00"
COLOR_WITHOUT = "#0072B2"


def latest_cool_csv():
    cands = sorted(glob.glob(os.path.join(RAW_DIR, "*_analysis.csv")))
    for p in reversed(cands):
        cols = pd.read_csv(p, nrows=0).columns
        if COOL_TAG in cols:
            return p
    raise FileNotFoundError(f"{COOL_TAG} 태그가 있는 CSV 가 없습니다 — "
                             "Exchanger_Cooling_Overlap_Analysis.py 먼저 실행")


def load_cool():
    path = latest_cool_csv()
    raw = pd.read_csv(path, index_col="Time", parse_dates=True, usecols=["Time", COOL_TAG])
    raw = raw[~raw.index.duplicated(keep="last")].sort_index()
    return raw[COOL_TAG].dropna()


def load_cycles():
    df = pd.read_csv(CYCLES_CSV, parse_dates=["cycle_start", "cycle_end", "next_on"])
    return df


def flag_overlap_during_off(cycles, cool):
    cool_sorted = cool.sort_index()
    overlaps = []
    for _, r in cycles.iterrows():
        seg = cool_sorted.loc[r["cycle_end"]:r["next_on"]]
        before = cool_sorted.loc[:r["cycle_end"]]
        start_val = before.iloc[-1] if len(before) else 0
        overlaps.append(bool((seg == 1).any() or start_val == 1))
    cycles = cycles.copy()
    cycles["cool_overlap_off"] = overlaps
    return cycles


def report(cycles):
    cycles["day"] = cycles["cycle_end"].dt.date
    cycles["bucket"] = pd.cut(cycles["off_duration_sec"], bins=BUCKETS_SEC, labels=BUCKET_LABELS,
                               right=False, include_lowest=True)

    print(f"전체 확정 OFF {len(cycles)}건\n")
    print("=== 날짜별 x 구간별 ===")
    table = pd.crosstab(cycles["day"], cycles["bucket"]).reindex(columns=BUCKET_LABELS, fill_value=0)
    table["합계"] = table.sum(axis=1)
    print(table.to_string())

    print("\n=== 전체 구간별 횟수/비율 ===")
    overall = cycles["bucket"].value_counts().reindex(BUCKET_LABELS, fill_value=0)
    pct = (overall / overall.sum() * 100).round(1)
    print(pd.DataFrame({"횟수": overall, "비율(%)": pct}).to_string())

    shutdown = cycles[cycles["off_duration_sec"] >= SHUTDOWN_SEC]
    normal = cycles[cycles["off_duration_sec"] < SHUTDOWN_SEC]
    print(f"\n정상 냉각 OFF {len(normal)}개(5~22분대) / 야간 셧다운(추정) {len(shutdown)}개(6시간대) — "
          f"이 둘은 성격이 달라서 아래 cool 비교에선 셧다운 {len(shutdown)}건을 뺀다.\n")

    with_cool = normal[normal["cool_overlap_off"]]
    without_cool = normal[~normal["cool_overlap_off"]]
    print(f"=== 정상 냉각 OFF 중 cool 겹침 비교 — 겹침 {len(with_cool)}개"
          f"({100*len(with_cool)/len(normal):.1f}%), 안겹침 {len(without_cool)}개"
          f"({100*len(without_cool)/len(normal):.1f}%) ===")
    for name, grp in (("cool 겹침", with_cool), ("cool 안겹침", without_cool)):
        if len(grp) == 0:
            print(f"  {name}: 0개")
            continue
        d = grp["off_duration_sec"] / 60.0
        print(f"  {name}: {len(grp)}개 — 중앙값 {d.median():.2f}분, 평균 {d.mean():.2f}분, "
              f"표준편차 {d.std():.2f}분, 범위 {d.min():.2f}~{d.max():.2f}분")

    if len(with_cool) >= 2 and len(without_cool) >= 2:
        u_stat, p_val = stats.mannwhitneyu(with_cool["off_duration_sec"], without_cool["off_duration_sec"],
                                            alternative="two-sided")
        t_stat, p_t = stats.ttest_ind(with_cool["off_duration_sec"], without_cool["off_duration_sec"],
                                       equal_var=False)
        pooled_std = np.sqrt((with_cool["off_duration_sec"].var() + without_cool["off_duration_sec"].var()) / 2)
        cohens_d = (with_cool["off_duration_sec"].mean() - without_cool["off_duration_sec"].mean()) / pooled_std
        print(f"\n=== 통계 검정 ===")
        print(f"  Mann-Whitney U p-value = {p_val:.4g}")
        print(f"  Welch t-test p-value   = {p_t:.4g}")
        print(f"  Cohen's d = {cohens_d:.2f}")

    return cycles, normal, shutdown


def plot(cycles, normal, shutdown):
    with_cool = normal[normal["cool_overlap_off"]]
    without_cool = normal[~normal["cool_overlap_off"]]

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(13, 9))

    ax1.scatter(without_cool["cycle_end"], without_cool["off_duration_sec"] / 60.0,
                s=22, c=COLOR_WITHOUT, alpha=0.7, linewidths=0, label="cool 안겹침(정상 OFF)")
    ax1.scatter(with_cool["cycle_end"], with_cool["off_duration_sec"] / 60.0,
                s=30, c=COLOR_WITH, alpha=0.9, linewidths=0, label="cool 겹침(정상 OFF)")
    ax1.scatter(shutdown["cycle_end"], shutdown["off_duration_sec"] / 60.0,
                s=30, c="#999999", alpha=0.9, linewidths=0, marker="x", label="야간 셧다운(제외)")
    ax1.set_yscale("log")
    ax1.set_ylabel("OFF 지속시간 (분, 로그축)")
    ax1.set_title("OFF 지속시간 — cool 겹침 여부별 시간순 산점도", loc="left", fontsize=12)
    ax1.grid(axis="y", which="both", alpha=0.2)
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)
    ax1.legend(loc="center right", frameon=False)

    bp_data = [without_cool["off_duration_sec"] / 60.0, with_cool["off_duration_sec"] / 60.0]
    bp = ax2.boxplot(bp_data, positions=[0, 1], widths=0.5, showmeans=True, patch_artist=True)
    for patch, color in zip(bp["boxes"], [COLOR_WITHOUT, COLOR_WITH]):
        patch.set(facecolor=color, alpha=0.3, edgecolor=color, linewidth=1.3)
    for i, d in enumerate(bp_data):
        x = np.random.default_rng(0).normal(i, 0.06, size=len(d))
        ax2.scatter(x, d, s=14, color=[COLOR_WITHOUT, COLOR_WITH][i], alpha=0.5, linewidths=0, zorder=3)
    ax2.set_xticks([0, 1])
    ax2.set_xticklabels([f"cool 안겹침 (n={len(without_cool)})", f"cool 겹침 (n={len(with_cool)})"])
    ax2.set_ylabel("OFF 지속시간 (분)")
    ax2.set_title("정상 냉각 OFF만 — 그룹별 분포 비교 (야간 셧다운 제외)", loc="left", fontsize=12)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)

    fig.tight_layout()
    out_path = os.path.join(OUT_DIR, "cool_overlap_off_duration.png")
    fig.savefig(out_path, dpi=140)
    plt.close(fig)
    return out_path


def main():
    cool = load_cool()
    cycles = load_cycles()
    cycles = flag_overlap_during_off(cycles, cool)
    cycles, normal, shutdown = report(cycles)
    out_path = plot(cycles, normal, shutdown)
    print(f"\n그래프 저장: {out_path}")

    csv_out = os.path.join(OUT_DIR, "cycles_with_cool_overlap_off.csv")
    cycles.to_csv(csv_out, index=False, encoding="utf-8-sig")
    print(f"결과 표 저장: {csv_out}")


if __name__ == "__main__":
    main()
