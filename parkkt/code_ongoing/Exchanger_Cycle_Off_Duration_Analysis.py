"""`merged_cycles_full.csv`(180초 임계값으로 채터링을 이어붙인 뒤 확정된 '진짜'
사이클)를 기준으로, 0(꺼짐)에서 1(켜짐)까지 얼마나 걸리는지(`off_duration_sec`)
를 날짜별/구간별로 센다.

이전 분석(raw 이벤트 2488개 기준)과 다른 점: 여기는 채터링이 전부 제거된
**사이클 단위** 246개만 본다 — 전부 180초 이상이라 "짧은 그룹"은 애초에 없고,
"정상 냉각 사이클 OFF"와 "야간 셧다운" 두 그룹으로 또 한 번 뚜렷하게 갈리는지를
본다.

사용법 (Exchanger_Cycle_Before_After.py 등으로 raw CSV 를 만들어둔 뒤):
    PYTHONIOENCODING=utf-8 python Exchanger_Cycle_Off_Duration_Analysis.py
"""

import glob
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

matplotlib.rcParams["font.family"] = "Malgun Gothic"
matplotlib.rcParams["axes.unicode_minus"] = False

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RAW_DIR = os.path.join(BASE_DIR, "parkkt", "extracted_csv")
OUT_DIR = os.path.join(BASE_DIR, "parkkt", "analysis_out", "exchanger_off_on_gap")
CYCLES_CSV = os.path.join(OUT_DIR, "merged_cycles_full.csv")

EXCH_TAG = "워킹 Tank 1 BACK Exchanger SOL"
THRESHOLD_SEC = 180

SHUTDOWN_SPLIT_SEC = 3600  # 1시간 — 정상 사이클(최대 22분) vs 야간 셧다운(6시간+) 사이 빈 구간 안

BUCKETS_SEC = [180, 300, 600, 900, 1200, 1800, 3600, 7200, 14400, float("inf")]
BUCKET_LABELS = ["3~5분", "5~10분", "10~15분", "15~20분", "20~30분",
                  "30~60분", "1~2시간", "2~4시간", "4시간 이상"]

COLOR_NORMAL = "#0072B2"
COLOR_SHUTDOWN = "#D55E00"


def latest_raw_csv():
    cands = sorted(glob.glob(os.path.join(RAW_DIR, "*_analysis.csv")))
    if not cands:
        raise FileNotFoundError("raw CSV 없음 — Exchanger_Off_On_Gap_Analysis.py 먼저 실행")
    return cands[-1]


def build_cycles():
    """raw에서 다시 계산 — 항상 최신 데이터 기준으로 맞춘다."""
    path = latest_raw_csv()
    raw = pd.read_csv(path, index_col="Time", parse_dates=True, usecols=["Time", EXCH_TAG])
    raw = raw[~raw.index.duplicated(keep="last")].sort_index()
    exch = raw[EXCH_TAG].dropna()

    s = exch[exch.diff() != 0]
    changes = pd.DataFrame({"val": s})
    changes["prev"] = changes["val"].shift(1)
    on_starts = changes.index[(changes["prev"].isna() | (changes["prev"] == 0)) & (changes["val"] == 1)]
    off_starts = changes.index[(changes["prev"] == 1) & (changes["val"] == 0)]

    segs, oi = [], 0
    for on_t in on_starts:
        while oi < len(off_starts) and off_starts[oi] <= on_t:
            oi += 1
        if oi >= len(off_starts):
            break
        segs.append((on_t, off_starts[oi]))

    rows = []
    cycle_start = segs[0][0]
    n_bridged = 0
    for i in range(len(segs) - 1):
        gap = (segs[i + 1][0] - segs[i][1]).total_seconds()
        if gap < THRESHOLD_SEC:
            n_bridged += 1
            continue
        rows.append({"cycle_start": cycle_start, "cycle_end": segs[i][1], "next_on": segs[i + 1][0],
                      "off_duration_sec": gap, "n_bridged_short_offs": n_bridged})
        cycle_start = segs[i + 1][0]
        n_bridged = 0

    df = pd.DataFrame(rows)
    os.makedirs(OUT_DIR, exist_ok=True)
    df.to_csv(CYCLES_CSV, index=False, encoding="utf-8-sig")
    return df


def report_table(df):
    df["day"] = df["cycle_end"].dt.date
    df["bucket"] = pd.cut(df["off_duration_sec"], bins=BUCKETS_SEC, labels=BUCKET_LABELS,
                           right=False, include_lowest=True)

    print(f"확정 사이클(= 0→1까지 걸린 시간이 기록된 건) {len(df)}개\n")

    print("=== 날짜별 x 구간별 ===")
    table = pd.crosstab(df["day"], df["bucket"]).reindex(columns=BUCKET_LABELS, fill_value=0)
    table["합계"] = table.sum(axis=1)
    print(table.to_string())

    print("\n=== 전체 구간별 횟수/비율 ===")
    overall = df["bucket"].value_counts().reindex(BUCKET_LABELS, fill_value=0)
    pct = (overall / overall.sum() * 100).round(1)
    print(pd.DataFrame({"횟수": overall, "비율(%)": pct}).to_string())

    print(f"\n=== 두 그룹 비교 (경계 {SHUTDOWN_SPLIT_SEC}초=1시간) ===")
    normal = df[df["off_duration_sec"] < SHUTDOWN_SPLIT_SEC]
    shutdown = df[df["off_duration_sec"] >= SHUTDOWN_SPLIT_SEC]
    for name, grp in (("정상 냉각 사이클 OFF", normal), ("야간 셧다운(추정)", shutdown)):
        if len(grp) == 0:
            print(f"  {name}: 0개")
            continue
        print(f"  {name}: {len(grp)}개 — 중앙값 {grp['off_duration_sec'].median()/60:.1f}분, "
              f"평균 {grp['off_duration_sec'].mean()/60:.1f}분, "
              f"범위 {grp['off_duration_sec'].min()/60:.1f}~{grp['off_duration_sec'].max()/60:.1f}분")

    lo = normal["off_duration_sec"].max() if len(normal) else None
    hi = shutdown["off_duration_sec"].min() if len(shutdown) else None
    if lo is not None and hi is not None:
        print(f"\n  두 그룹 사이 공백: {lo/60:.1f}분 ~ {hi/60:.1f}분 사이에 실측 이벤트 없음 "
              f"({hi/lo:.1f}배 차이) — 경계가 뚜렷함")
    return df


def plot(df):
    is_shutdown = df["off_duration_sec"] >= SHUTDOWN_SPLIT_SEC
    colors = np.where(is_shutdown, COLOR_SHUTDOWN, COLOR_NORMAL)

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(13, 9))

    ax1.scatter(df["cycle_end"], df["off_duration_sec"] / 60.0, s=22, c=colors, alpha=0.7, linewidths=0)
    ax1.axhline(SHUTDOWN_SPLIT_SEC / 60.0, color="#999999", linewidth=1, linestyle="--")
    ax1.set_yscale("log")
    ax1.set_ylabel("OFF 지속시간 (분, 로그축)")
    ax1.set_title("사이클 단위 OFF 지속시간 — 시간순 산점도 (채터링 제거 후)", loc="left", fontsize=12)
    ax1.grid(axis="y", which="both", alpha=0.2)
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)
    handles = [plt.Line2D([0], [0], marker="o", linestyle="", color=COLOR_NORMAL, label="정상 냉각 사이클 OFF"),
               plt.Line2D([0], [0], marker="o", linestyle="", color=COLOR_SHUTDOWN, label="야간 셧다운(추정)")]
    ax1.legend(handles=handles, loc="center right", frameon=False)

    log_vals = np.log10(df["off_duration_sec"].values / 60.0)
    bins = np.linspace(log_vals.min(), log_vals.max(), 40)
    ax2.hist(log_vals[~is_shutdown], bins=bins, color=COLOR_NORMAL, alpha=0.8, label="정상 냉각 사이클 OFF")
    ax2.hist(log_vals[is_shutdown], bins=bins, color=COLOR_SHUTDOWN, alpha=0.8, label="야간 셧다운(추정)")
    tick_vals = [3, 5, 10, 15, 20, 30, 60, 120, 240, 380]
    ax2.set_xticks(np.log10(tick_vals))
    ax2.set_xticklabels([f"{v}분" if v < 60 else f"{v//60}시간" for v in tick_vals])
    ax2.set_xlabel("OFF 지속시간 (분, 로그축)")
    ax2.set_ylabel("사이클 수")
    ax2.set_title("같은 데이터의 로그축 히스토그램 — 두 그룹 사이 공백 확인", loc="left", fontsize=12)
    ax2.legend(loc="upper left", frameon=False)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)

    fig.tight_layout()
    out_path = os.path.join(OUT_DIR, "cycle_off_duration_two_groups.png")
    fig.savefig(out_path, dpi=140)
    plt.close(fig)
    return out_path


def main():
    df = build_cycles()
    df = report_table(df)
    out_path = plot(df)
    print(f"\n그래프 저장: {out_path}")
    print(f"사이클 표 저장: {CYCLES_CSV}")


if __name__ == "__main__":
    main()
