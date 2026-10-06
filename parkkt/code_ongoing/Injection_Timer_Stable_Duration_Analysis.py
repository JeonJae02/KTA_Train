"""HD1_P1 / HD2_P2 / HD4_P1 인젝션 ON 타이머값이 "값이 안 변하고 유지된 시간"
(= 다음 사이클이 완료될 때까지 걸린 간격)을 구간별로 나눠서 분포를 본다.

배경: 이 타이머(ON_T)는 사이클이 완료될 때만 갱신되고 그 사이에는 직전 값을
그대로 유지한다(Injection_Timer_Threshold_Watch.py 참고). 설비가 한동안 멈춰
있다가 재가동되면 "멈춰 있던 구간"이 끝나는 순간 값이 평소보다 크게 튄다.
전처리(이상치 보정) 전에, 값이 안 변하고 유지된 구간이 실제로 얼마나 되는지
(5초 내외 / 4~5분 / 1시간 넘게 등) 와, 그 분포가 자연스러운 그룹(예: 5분
내외 그룹, 1시간 이상 그룹)으로 나뉘는지를 먼저 확인한다.

왼쪽/오른쪽 절단 구간 제외: 데이터 맨 앞에서 시작하는 구간은 실제로 언제부터
유지됐는지 모르고(왼쪽 절단), 데이터 맨 끝에서 아직 안 바뀐 구간은 언제
끝날지 모른다(오른쪽 절단). 둘 다 길이를 과소평가하게 되므로 분석에서 뺀다.

데이터: 기존 캐시 재사용 (parkkt/cache/injection_timer_sv_raw_20260910.pkl,
2026-09-10 ~ now, Injection_Timer_SV_Stats.py 가 만든 것).

사용법:
    PYTHONIOENCODING=utf-8 python Injection_Timer_Stable_Duration_Analysis.py
"""
import os
import pickle

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

matplotlib.rcParams["font.family"] = "Malgun Gothic"
matplotlib.rcParams["axes.unicode_minus"] = False

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CACHE_PATH = os.path.join(ROOT_DIR, "parkkt", "cache", "injection_timer_sv_raw_20260910.pkl")
OUT_DIR = os.path.join(ROOT_DIR, "parkkt", "analysis_out", "injection_timer_stable_duration")

TAGS = ["HD1_P1_인젝션_ON_T", "HD2_P2_인젝션_ON_T", "HD4_P1_인젝션_ON_T"]
TAG_COLORS = {
    "HD1_P1_인젝션_ON_T": "#0072B2",
    "HD2_P2_인젝션_ON_T": "#D55E00",
    "HD4_P1_인젝션_ON_T": "#009E73",
}

BUCKETS_SEC = [0, 5, 10, 30, 60, 120, 300, 600, 1800, 3600, 7200, float("inf")]
BUCKET_LABELS = ["5초 미만", "5~10초", "10~30초", "30초~1분", "1~2분", "2~5분",
                  "5~10분", "10~30분", "30~60분", "1~2시간", "2시간 이상"]


def load_raw():
    if not os.path.exists(CACHE_PATH):
        raise FileNotFoundError(
            f"캐시 없음: {CACHE_PATH}\n"
            "먼저 Injection_Timer_SV_Stats.py 를 한 번 실행해 캐시를 만들어두세요.")
    with open(CACHE_PATH, "rb") as f:
        return pickle.load(f)


def stable_durations(series):
    """값이 유지된 구간(런)마다 (start, value, duration_sec) 를 뽑는다."""
    s = series.dropna()
    changed = s[s.diff() != 0]  # diff 첫 값은 NaN -> True 취급, 첫 행도 포함됨
    if len(changed) < 3:
        return pd.DataFrame(columns=["start", "value", "duration_sec"])

    starts = changed.index[:-1]
    ends = changed.index[1:]
    vals = changed.to_numpy()[:-1]
    dur_sec = (ends - starts).total_seconds()

    df = pd.DataFrame({"start": starts, "value": vals, "duration_sec": dur_sec})
    return df.iloc[1:].reset_index(drop=True)  # 첫 구간(왼쪽 절단) 제외


def build_all(df_raw):
    rows = []
    for tag in TAGS:
        if tag not in df_raw.columns:
            print(f"⚠️ 태그 없음, 건너뜀: {tag}")
            continue
        d = stable_durations(df_raw[tag])
        d["tag"] = tag
        rows.append(d)
        print(f"{tag}: 유지 구간 {len(d):,}개 "
              f"(제외한 양끝 절단 구간 2개는 별도)")
    return pd.concat(rows, ignore_index=True)


def report_table(all_df):
    all_df["bucket"] = pd.cut(all_df["duration_sec"], bins=BUCKETS_SEC, labels=BUCKET_LABELS,
                               right=False, include_lowest=True)

    print(f"\n전체 유지 구간: {len(all_df):,}개 "
          f"(총 유지 시간 {all_df['duration_sec'].sum()/3600:.1f}시간)")

    print("\n=== 태그별 x 구간별 횟수 ===")
    table = pd.crosstab(all_df["tag"], all_df["bucket"]).reindex(columns=BUCKET_LABELS, fill_value=0)
    table["합계"] = table.sum(axis=1)
    print(table.to_string())

    print("\n=== 전체 구간별 횟수/비율 (어느 길이가 가장 흔한가) ===")
    cnt = all_df["bucket"].value_counts().reindex(BUCKET_LABELS, fill_value=0)
    cnt_pct = (cnt / cnt.sum() * 100).round(1)
    time_sum = all_df.groupby("bucket", observed=False)["duration_sec"].sum().reindex(BUCKET_LABELS, fill_value=0)
    time_pct = (time_sum / time_sum.sum() * 100).round(1)
    print(pd.DataFrame({
        "횟수": cnt, "횟수비율(%)": cnt_pct,
        "총시간(시간)": (time_sum / 3600).round(2), "총시간비율(%)": time_pct,
    }).to_string())

    return all_df


def find_histogram_gaps(log_vals, n_bins=80):
    """log10(duration_sec) 히스토그램에서, 양옆에 데이터가 있는데 가운데가
    완전히 빈 구간(= 자연스러운 그룹 경계 후보)을 찾아 (lo_sec, hi_sec) 로 반환."""
    counts, edges = np.histogram(log_vals, bins=n_bins)
    gaps = []
    i = 0
    while i < len(counts):
        if counts[i] == 0:
            j = i
            while j < len(counts) and counts[j] == 0:
                j += 1
            if i > 0 and j < len(counts):  # 양옆에 데이터가 있는 빈 구간만
                gaps.append((10 ** edges[i], 10 ** edges[j]))
            i = j
        else:
            i += 1
    return gaps


def fmt_sec(sec):
    if sec < 60:
        return f"{sec:.0f}초"
    if sec < 3600:
        return f"{sec/60:.1f}분"
    return f"{sec/3600:.1f}시간"


def check_natural_grouping(all_df):
    log_vals = np.log10(all_df["duration_sec"].to_numpy())

    print("\n=== 히스토그램상 빈 공백(= 자연 경계 후보) ===")
    gaps = find_histogram_gaps(log_vals)
    if not gaps:
        print("  뚜렷한 빈 공백 없음 — 길이가 연속적으로 이어져 있음(그룹 경계가 뚜렷하지 않음)")
    else:
        for lo, hi in gaps:
            print(f"  {fmt_sec(lo)} ~ {fmt_sec(hi)} 사이에 실측 구간 없음 ({hi/lo:.1f}배)")

    print("\n=== KMeans(로그축) 로 k개 그룹으로 나눴을 때 실루엣 점수 ===")
    X = log_vals.reshape(-1, 1)
    best_k, best_score = None, -1
    for k in range(2, 6):
        km = KMeans(n_clusters=k, n_init=10, random_state=0).fit(X)
        score = silhouette_score(X, km.labels_)
        print(f"  k={k}: 실루엣 {score:.3f}")
        if score > best_score:
            best_k, best_score = k, score

    print(f"\n  -> 가장 잘 나뉘는 그룹 수: k={best_k} (실루엣 {best_score:.3f})")
    km = KMeans(n_clusters=best_k, n_init=10, random_state=0).fit(X)
    all_df = all_df.copy()
    all_df["cluster"] = km.labels_
    centers = km.cluster_centers_.ravel()
    order = np.argsort(centers)
    print("\n  그룹별 범위 (중심값 오름차순):")
    for rank, c in enumerate(order):
        grp = all_df[all_df["cluster"] == c]
        print(f"    그룹 {rank+1}: {len(grp):,}개 — "
              f"{fmt_sec(grp['duration_sec'].min())} ~ {fmt_sec(grp['duration_sec'].max())}, "
              f"중앙값 {fmt_sec(grp['duration_sec'].median())}")
    return gaps, all_df


def plot(all_df, gaps):
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(13, 9))

    for tag in TAGS:
        sub = all_df[all_df["tag"] == tag]
        if sub.empty:
            continue
        ax1.scatter(sub["start"], sub["duration_sec"] / 60.0, s=10, alpha=0.5,
                    color=TAG_COLORS.get(tag, "#333333"), linewidths=0, label=tag)
    ax1.set_yscale("log")
    ax1.set_ylabel("유지 시간 (분, 로그축)")
    ax1.set_title("타이머 값이 변하지 않고 유지된 시간 — 시간순 산점도", loc="left", fontsize=12)
    ax1.grid(axis="y", which="both", alpha=0.2)
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)
    ax1.legend(loc="upper right", frameon=False, fontsize=9)

    log_vals_by_tag = {tag: np.log10(all_df.loc[all_df["tag"] == tag, "duration_sec"].to_numpy())
                        for tag in TAGS}
    all_log = np.log10(all_df["duration_sec"].to_numpy())
    bins = np.linspace(all_log.min(), all_log.max(), 60)
    for tag in TAGS:
        lv = log_vals_by_tag.get(tag)
        if lv is None or len(lv) == 0:
            continue
        ax2.hist(lv, bins=bins, color=TAG_COLORS.get(tag, "#333333"), alpha=0.6, label=tag)

    for lo, hi in gaps:
        ax2.axvspan(np.log10(lo), np.log10(hi), color="#999999", alpha=0.25)

    tick_vals = [1, 5, 10, 30, 60, 300, 600, 1800, 3600, 7200]
    ax2.set_xticks(np.log10(tick_vals))
    ax2.set_xticklabels([fmt_sec(v) for v in tick_vals], rotation=30, ha="right")
    ax2.set_xlabel("유지 시간 (로그축)")
    ax2.set_ylabel("구간 수")
    ax2.set_title("같은 데이터의 로그축 히스토그램 — 회색 음영 = 자연 공백(그룹 경계 후보)", loc="left", fontsize=12)
    ax2.legend(loc="upper right", frameon=False, fontsize=9)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)

    fig.tight_layout()
    out_path = os.path.join(OUT_DIR, "stable_duration_groups.png")
    fig.savefig(out_path, dpi=140)
    plt.close(fig)
    return out_path


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    df_raw = load_raw()
    print(f"원본: {len(df_raw):,}행 x {len(df_raw.columns)}컬럼 "
          f"({df_raw.index.min()} ~ {df_raw.index.max()})")

    all_df = build_all(df_raw)
    all_df = report_table(all_df)
    gaps, all_df = check_natural_grouping(all_df)

    csv_path = os.path.join(OUT_DIR, "stable_durations.csv")
    all_df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    out_path = plot(all_df, gaps)
    print(f"\n원자료 저장: {csv_path}")
    print(f"그래프 저장: {out_path}")


if __name__ == "__main__":
    main()
