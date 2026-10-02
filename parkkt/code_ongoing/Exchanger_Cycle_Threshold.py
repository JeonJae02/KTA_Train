"""OFF→ON 간격(`Exchanger_Off_On_Gap_Analysis.py` 결과)을 가지고 두 가지를 한다.

1. **실제 초 단위 분포를 시각화** — 구간(버킷)으로 뭉개지 않고 산점도/로그축
   히스토그램으로 "두 그룹"이 실측으로 얼마나 깨끗이 갈리는지 보여준다.
2. **디바운스 임계값(THRESHOLD_SEC)으로 raw ON 구간을 사이클 단위로 합친다** —
   OFF 가 짧으면(채터링) 이전 사이클에 이어붙이고, OFF 가 길면(진짜 꺼짐) 그
   지점에서 사이클을 끊는다. 경계값이 애매한 구간(= 실측 데이터가 실제로 걸쳐
   있는 구간)이 있으면 그 이벤트들의 정확한 시각을 출력해 사람이 원본을 볼 수
   있게 한다.

사용법 (먼저 Exchanger_Off_On_Gap_Analysis.py 를 한 번 실행해 CSV 를 만들어둘 것):
    PYTHONIOENCODING=utf-8 python Exchanger_Cycle_Threshold.py
"""

import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

matplotlib.rcParams["font.family"] = "Malgun Gothic"
matplotlib.rcParams["axes.unicode_minus"] = False

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
GAPS_CSV = os.path.join(BASE_DIR, "parkkt", "analysis_out", "exchanger_off_on_gap", "off_on_gaps.csv")
OUT_DIR = os.path.join(BASE_DIR, "parkkt", "analysis_out", "exchanger_off_on_gap")

# 디바운스 임계값(초). 아래 분석에서 확인한 "완전 공백 구간"(약 56~402초)
# 안이면 이번 주 데이터에 한해 어떤 값을 써도 분류 결과가 똑같다.
THRESHOLD_SEC = 180

# 색(Okabe-Ito, 프로젝트 기존 팔레트와 동일 — 색맹 안전, 고정 카테고리 순서)
COLOR_SHORT = "#0072B2"   # 짧은 그룹(채터링/순간 토글)
COLOR_LONG = "#D55E00"    # 긴 그룹(실제 냉각 OFF)


def load_gaps():
    df = pd.read_csv(GAPS_CSV, parse_dates=["off_time", "on_time"])
    return df.sort_values("off_time").reset_index(drop=True)


def find_dead_zone(gap_sec, threshold):
    """threshold 아래/위 각각 가장 가까운 실측값 — '경계가 애매한 실제 구간'을 보여준다."""
    below = gap_sec[gap_sec < threshold]
    above = gap_sec[gap_sec >= threshold]
    lo = below.max() if len(below) else None
    hi = above.min() if len(above) else None
    return lo, hi


def plot_two_groups(df, threshold):
    group = np.where(df["gap_sec"] < threshold, "짧은 그룹(채터링)", "긴 그룹(실제 OFF)")
    colors = np.where(df["gap_sec"] < threshold, COLOR_SHORT, COLOR_LONG)

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(13, 9))

    # --- 1) 산점도: 시간 흐름 x, 실제 간격(초, 로그축) y ---
    ax1.scatter(df["off_time"], df["gap_sec"], s=14, c=colors, alpha=0.6, linewidths=0)
    ax1.axhline(threshold, color="#999999", linewidth=1, linestyle="--", zorder=1)
    ax1.text(df["off_time"].iloc[0], threshold, f" 임계값 {threshold}초", va="bottom",
              fontsize=9, color="#666666")
    ax1.set_yscale("log")
    ax1.set_ylabel("OFF→ON 간격 (초, 로그축)")
    ax1.set_title("exchanger OFF→ON 간격 — 시간순 산점도 (실제 초)", loc="left", fontsize=12)
    ax1.grid(axis="y", which="both", alpha=0.2)
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)
    handles = [plt.Line2D([0], [0], marker="o", linestyle="", color=COLOR_SHORT, label="짧은 그룹(채터링)"),
               plt.Line2D([0], [0], marker="o", linestyle="", color=COLOR_LONG, label="긴 그룹(실제 OFF)")]
    ax1.legend(handles=handles, loc="upper right", frameon=False)

    # --- 2) 로그축 히스토그램: 분포 모양(이봉성) 확인용 ---
    log_vals = np.log10(df["gap_sec"].values)
    bins = np.linspace(log_vals.min(), log_vals.max(), 60)
    ax2.hist(log_vals[df["gap_sec"] < threshold], bins=bins, color=COLOR_SHORT, alpha=0.75,
              label="짧은 그룹(채터링)")
    ax2.hist(log_vals[df["gap_sec"] >= threshold], bins=bins, color=COLOR_LONG, alpha=0.75,
              label="긴 그룹(실제 OFF)")
    ax2.axvline(np.log10(threshold), color="#999999", linewidth=1, linestyle="--")
    tick_vals = [1, 2, 5, 10, 30, 60, 300, 600, 1800, 3600, 7200, 21600]
    ax2.set_xticks(np.log10(tick_vals))
    ax2.set_xticklabels([f"{v}s" if v < 60 else f"{v//60}m" for v in tick_vals])
    ax2.set_xlabel("OFF→ON 간격 (로그축)")
    ax2.set_ylabel("이벤트 수")
    ax2.set_title("같은 데이터의 로그축 히스토그램 — 두 봉우리 사이 완전 공백 구간 확인", loc="left", fontsize=12)
    ax2.legend(loc="upper right", frameon=False)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)

    fig.tight_layout()
    os.makedirs(OUT_DIR, exist_ok=True)
    out_path = os.path.join(OUT_DIR, "gap_two_groups.png")
    fig.savefig(out_path, dpi=140)
    plt.close(fig)
    return out_path


def merge_cycles(df, threshold):
    """raw OFF→ON 이벤트 연쇄를 threshold 기준으로 합쳐 '논리적 사이클'을 만든다.

    df 는 시간순 정렬된 off_time/on_time/gap_sec 행들 — 즉 원래 ON 구간들 사이의
    각 OFF 갭이다. gap_sec < threshold 인 OFF 는 "아직 같은 사이클"로 보고
    다음 ON 과 이어붙이고, gap_sec >= threshold 인 지점에서 사이클을 끊는다
    (그 OFF 시작 시각이 "사이클 종료 시점").

    반환 컬럼: cycle_end(= 그 사이클이 실제로 꺼진 시각), next_cycle_start,
    n_bridged(그 사이클 안에서 짧게 깜빡인 횟수), off_time/on_time 원본 보존.
    """
    rows = []
    n_bridged = 0
    for _, r in df.iterrows():
        if r["gap_sec"] < threshold:
            n_bridged += 1
            continue
        rows.append({"cycle_end": r["off_time"], "next_on": r["on_time"],
                      "off_duration_sec": r["gap_sec"], "n_bridged_short_offs": n_bridged})
        n_bridged = 0
    return pd.DataFrame(rows)


def main():
    df = load_gaps()
    print(f"로드: {len(df)}건 (이미 야간/비가동 제외된 상태)\n")

    lo, hi = find_dead_zone(df["gap_sec"], THRESHOLD_SEC)
    print(f"임계값 {THRESHOLD_SEC}초 기준 — 바로 아래 실측값: {lo:.1f}초, 바로 위 실측값: {hi:.1f}초")
    if lo is not None and hi is not None:
        print(f"→ 이 둘 사이({lo:.1f}~{hi:.1f}초)에는 실측 이벤트가 **전혀 없다** — "
              f"즉 이 구간 안이면 임계값을 어디로 잡아도 분류 결과가 동일하다 (애매한 경계 사례 없음).")

    print("\n임계값 바로 아래쪽 5개(짧은 그룹 쪽 끝) — 혹시 모를 확인용 정확한 시각:")
    print(df[df["gap_sec"] < THRESHOLD_SEC].nlargest(5, "gap_sec")[["off_time", "on_time", "gap_sec"]]
          .to_string(index=False))
    print("\n임계값 바로 위쪽 5개(긴 그룹 쪽 끝) — 혹시 모를 확인용 정확한 시각:")
    print(df[df["gap_sec"] >= THRESHOLD_SEC].nsmallest(5, "gap_sec")[["off_time", "on_time", "gap_sec"]]
          .to_string(index=False))

    out_path = plot_two_groups(df, THRESHOLD_SEC)
    print(f"\n그래프 저장: {out_path}")

    cycles = merge_cycles(df, THRESHOLD_SEC)
    csv_out = os.path.join(OUT_DIR, f"merged_cycles_thr{THRESHOLD_SEC}s.csv")
    cycles.to_csv(csv_out, index=False, encoding="utf-8-sig")
    print(f"\n임계값 {THRESHOLD_SEC}초로 합친 '진짜 종료' 사이클 {len(cycles)}개 저장: {csv_out}")
    print(f"(평균 {cycles['n_bridged_short_offs'].mean():.1f}회의 짧은 깜빡임이 사이클마다 다리이어졌습니다)")


if __name__ == "__main__":
    main()
