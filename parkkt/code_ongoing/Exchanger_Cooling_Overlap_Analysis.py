"""쿨러 신호("워킹 Tank 1 BACK Cooling SOL")가 exchanger ON 사이클 도중에 끼어
있었는지에 따라, 그 사이클의 ON 지속시간이 달라지는지 본다.

사용자 가설: cool 이 같이 켜져 있으면 ON 이 더 오래 유지될 것이다.

방법:
1. cool 태그를 exchanger 데이터와 같은 기간으로 InfluxDB 에서 받는다.
2. `merged_cycles_full.csv`(180초 임계값으로 채터링을 이어붙인 '진짜' exchanger
   ON 사이클)의 각 사이클 구간[cycle_start, cycle_end] 동안 cool==1 인 샘플이
   하나라도 있었으면 "겹침", 없으면 "안겹침"으로 나눈다.
3. 두 그룹의 on_duration_sec 분포를 비교한다(표 + 그래프 + 통계검정).

사용법:
    PYTHONIOENCODING=utf-8 python Exchanger_Cooling_Overlap_Analysis.py
"""

import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy import stats

matplotlib.rcParams["font.family"] = "Malgun Gothic"
matplotlib.rcParams["axes.unicode_minus"] = False

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT_DIR)
from Log_Extractor import LogExtractor

ENV_PATH = os.path.join(ROOT_DIR, ".env")
SAVE_DIR = os.path.join(ROOT_DIR, "parkkt", "extracted_csv")
OUT_DIR = os.path.join(ROOT_DIR, "parkkt", "analysis_out", "exchanger_off_on_gap")
CYCLES_CSV = os.path.join(OUT_DIR, "merged_cycles_full.csv")

COOL_TAG = "워킹 Tank 1 BACK Cooling SOL"

# merged_cycles_full.csv 를 만들 때 썼던 raw CSV 와 정확히 같은 구간으로 받는다.
START = "2026-09-28 00:00:00"
END = "2026-10-01 12:16:46"

COLOR_WITH = "#D55E00"
COLOR_WITHOUT = "#0072B2"


def fetch_cool():
    extractor = LogExtractor(env_path=ENV_PATH)
    df = extractor.get_data(start_time=START, end_time=END, target_tags=[COOL_TAG])
    os.makedirs(SAVE_DIR, exist_ok=True)
    extractor.save_to_csv(df, save_dir=SAVE_DIR)
    return df[COOL_TAG].dropna()


def load_cycles():
    df = pd.read_csv(CYCLES_CSV, parse_dates=["cycle_start", "cycle_end", "next_on"])
    df["on_duration_sec"] = (df["cycle_end"] - df["cycle_start"]).dt.total_seconds()
    return df


def flag_overlap(cycles, cool):
    cool_sorted = cool.sort_index()
    overlaps = []
    n_cool_segments = []
    for _, r in cycles.iterrows():
        seg = cool_sorted.loc[r["cycle_start"]:r["cycle_end"]]
        # 구간 시작 직전 마지막 값도 확인(그 시점에 이미 켜져 있었을 수 있음)
        before = cool_sorted.loc[:r["cycle_start"]]
        start_val = before.iloc[-1] if len(before) else 0
        any_on = (seg == 1).any() or start_val == 1
        overlaps.append(bool(any_on))
        # 그 구간 안에서 cool 이 몇 번이나 들어왔는지(디바운스 없이 raw 기준)
        seg_changes = seg[seg.diff().fillna(seg.iloc[0] if len(seg) else 0) != 0]
        n_cool_segments.append(int((seg_changes == 1).sum()))
    cycles = cycles.copy()
    cycles["cool_overlap"] = overlaps
    cycles["n_cool_on_segments"] = n_cool_segments
    return cycles


#: (이전 버전: 주말/연휴급 12시간 이상 공백 뒤만 제외 — 1건만 걸림.)
#: **업데이트**: 사용자 판단 — 하루 중 맨 처음 신호는 그 직전 상태(밤새 다른
#: 모드, 시작 시점 온도)가 불분명해서, 주말이 아니어도 시작 온도가 높으면 똑같이
#: 길어질 수 있다. 그래서 "주말급 공백 뒤"가 아니라 **그냥 그날의 첫 신호인지**
#: 로 기준을 바꾼다 — 날짜별 1건씩, 총 4건이 제외 대상이 된다(09-28 05:21,
#: 09-29 06:18, 09-30 06:23, 10-01 06:17 — Exchanger_Cycle_On_Duration_Analysis.py
#: 에서 이미 같은 기준으로 확인한 4건과 동일).
SHUTDOWN_OFF_SEC = 12 * 3600  # 더 이상 안 쓰지만 참고용으로 남겨둠


def flag_post_shutdown(cycles):
    """그날의 맨 처음 exchanger ON 사이클을 표시한다(= 전날 밤 상태가 불분명한
    기동 캐치업 — 주말 뒤든 평일 밤 뒤든 가리지 않고 날짜별 첫 신호는 전부 제외).

    판정: 직전 사이클의 off_duration_sec(=이 사이클 시작 전 OFF 길이)이
    SHUTDOWN_OFF_SEC 이상이거나, 데이터의 맨 첫 사이클(그 이전 OFF 길이를 데이터
    수집 구간 밖이라 알 수 없음 — 09-28 05:21 시작 사이클이 이 케이스: 주말 지나고
    첫 기동이라 cool 까지 같이 켜져서 46.7분간 온도를 크게 끌어내렸다. 사용자
    확인 완료)인 경우.

    (업데이트 — 실제 판정은 이제 날짜별 첫 신호 여부로 한다. 아래 참고.)
    """
    cycles = cycles.sort_values("cycle_start").reset_index(drop=True)
    day = cycles["cycle_start"].dt.date
    first_idx = cycles.groupby(day)["cycle_start"].idxmin()
    cycles["is_post_shutdown"] = cycles.index.isin(first_idx)
    return cycles


def report(cycles):
    n_excluded = int(cycles["is_post_shutdown"].sum())
    print(f"=== 기동 캐치업(야간 셧다운 직후) {n_excluded}건 제외 ===")
    print(cycles[cycles["is_post_shutdown"]][["cycle_start", "cycle_end", "on_duration_sec", "cool_overlap"]]
          .assign(on_min=lambda d: (d["on_duration_sec"] / 60).round(1)).drop(columns="on_duration_sec")
          .to_string(index=False))
    cycles = cycles[~cycles["is_post_shutdown"]].copy()
    print()

    with_cool = cycles[cycles["cool_overlap"]]
    without_cool = cycles[~cycles["cool_overlap"]]

    print(f"제외 후 분석 대상 {len(cycles)}개 — cool 겹침 {len(with_cool)}개"
          f"({100*len(with_cool)/len(cycles):.1f}%), 안겹침 {len(without_cool)}개"
          f"({100*len(without_cool)/len(cycles):.1f}%)")
    print("('안겹침' 은 cool_overlap==False 인 것만 — 겹친 사이클은 전부 제외하고 구함, 중복 없음)\n")

    print("=== 날짜별 겹침/안겹침 개수 ===")
    cycles["day"] = cycles["cycle_start"].dt.date
    table = pd.crosstab(cycles["day"], cycles["cool_overlap"])
    table.columns = ["안겹침", "겹침"] if False in table.columns and True in table.columns else table.columns
    print(table.to_string())

    print("\n=== ON 지속시간 그룹 비교 (분 단위) ===")
    for name, grp in (("cool 겹침", with_cool), ("cool 안겹침", without_cool)):
        if len(grp) == 0:
            print(f"  {name}: 0개")
            continue
        d = grp["on_duration_sec"] / 60.0
        print(f"  {name}: {len(grp)}개 — 중앙값 {d.median():.2f}분, 평균 {d.mean():.2f}분, "
              f"표준편차 {d.std():.2f}분, 범위 {d.min():.2f}~{d.max():.2f}분")

    if len(with_cool) >= 2 and len(without_cool) >= 2:
        u_stat, p_val = stats.mannwhitneyu(with_cool["on_duration_sec"], without_cool["on_duration_sec"],
                                            alternative="two-sided")
        t_stat, p_t = stats.ttest_ind(with_cool["on_duration_sec"], without_cool["on_duration_sec"],
                                       equal_var=False)
        pooled_std = np.sqrt((with_cool["on_duration_sec"].var() + without_cool["on_duration_sec"].var()) / 2)
        cohens_d = (with_cool["on_duration_sec"].mean() - without_cool["on_duration_sec"].mean()) / pooled_std
        print(f"\n=== 통계 검정 ===")
        print(f"  Mann-Whitney U p-value = {p_val:.4g}  (분포 자체가 다른지)")
        print(f"  Welch t-test p-value   = {p_t:.4g}  (평균 차이)")
        print(f"  Cohen's d = {cohens_d:.2f}  (0.2=작음 0.5=중간 0.8=큼)")

    print(f"\ncool 이 낀 사이클 안에서 cool ON 횟수(디바운스 없는 raw 기준) 분포:")
    print(with_cool["n_cool_on_segments"].describe().to_string())
    return cycles


def plot(cycles):
    with_cool = cycles[cycles["cool_overlap"]]
    without_cool = cycles[~cycles["cool_overlap"]]

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(13, 9))

    ax1.scatter(without_cool["cycle_start"], without_cool["on_duration_sec"] / 60.0,
                s=22, c=COLOR_WITHOUT, alpha=0.7, linewidths=0, label="cool 안겹침")
    ax1.scatter(with_cool["cycle_start"], with_cool["on_duration_sec"] / 60.0,
                s=30, c=COLOR_WITH, alpha=0.9, linewidths=0, label="cool 겹침")
    ax1.set_ylabel("ON 지속시간 (분)")
    ax1.set_title("exchanger ON 지속시간 — cool 겹침 여부별 시간순 산점도", loc="left", fontsize=12)
    ax1.grid(axis="y", alpha=0.2)
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)
    ax1.legend(loc="upper right", frameon=False)

    bp_data = [without_cool["on_duration_sec"] / 60.0, with_cool["on_duration_sec"] / 60.0]
    bp = ax2.boxplot(bp_data, positions=[0, 1], widths=0.5, showmeans=True, patch_artist=True)
    for patch, color in zip(bp["boxes"], [COLOR_WITHOUT, COLOR_WITH]):
        patch.set(facecolor=color, alpha=0.3, edgecolor=color, linewidth=1.3)
    for i, d in enumerate(bp_data):
        x = np.random.default_rng(0).normal(i, 0.06, size=len(d))
        ax2.scatter(x, d, s=14, color=[COLOR_WITHOUT, COLOR_WITH][i], alpha=0.5, linewidths=0, zorder=3)
    ax2.set_xticks([0, 1])
    ax2.set_xticklabels([f"cool 안겹침 (n={len(without_cool)})", f"cool 겹침 (n={len(with_cool)})"])
    ax2.set_ylabel("ON 지속시간 (분)")
    ax2.set_title("그룹별 분포 비교 (박스플롯 + 산점)", loc="left", fontsize=12)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)

    fig.tight_layout()
    out_path = os.path.join(OUT_DIR, "cool_overlap_on_duration.png")
    fig.savefig(out_path, dpi=140)
    plt.close(fig)
    return out_path


def main():
    cool = fetch_cool()
    cycles = load_cycles()
    cycles = flag_overlap(cycles, cool)
    cycles = flag_post_shutdown(cycles)

    filtered = report(cycles)  # 기동 캐치업 제외된 버전(통계/그래프용)
    out_path = plot(filtered)
    print(f"\n그래프 저장: {out_path}")

    # CSV 는 제외 여부를 알 수 있게 is_post_shutdown 플래그를 포함해 전체를 저장한다
    # (지워서 안 보이게 하는 대신, 플래그로 남겨서 나중에 검토 가능하게).
    csv_out = os.path.join(OUT_DIR, "cycles_with_cool_overlap.csv")
    cycles.to_csv(csv_out, index=False, encoding="utf-8-sig")
    print(f"결과 표 저장(전체, is_post_shutdown 플래그 포함): {csv_out}")


if __name__ == "__main__":
    main()
