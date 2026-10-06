"""Injection_Timer_Stable_Duration_Analysis.py 가 만든 stable_durations.csv 를
태그별로 쪼개서 더 자세히 본다.

계기: 전체(3개 태그 합산) 버킷표만 보면 "30~60분" 구간이 88건(0.1%)으로
너무 작아 보여서, 사용자가 육안으로 본 "50~60분대" 구간이 묻혀 보이지 않았다.
실제로는 태그마다 거의 매일 10시 40분대/19시 20분대에 47~60분 구간이 규칙적으로
나타난다(휴식시간으로 추정) — 합산 비율만 보면 놓치기 쉬운 패턴이라 태그별로
나누고, 10분 단위로 더 세분화한 버킷과 선형축 확대 히스토그램을 따로 그린다.

사용법:
    PYTHONIOENCODING=utf-8 python Injection_Timer_Stable_Duration_PerTag.py
"""
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

matplotlib.rcParams["font.family"] = "Malgun Gothic"
matplotlib.rcParams["axes.unicode_minus"] = False

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
OUT_DIR = os.path.join(ROOT_DIR, "parkkt", "analysis_out", "injection_timer_stable_duration")
CSV_PATH = os.path.join(OUT_DIR, "stable_durations.csv")

TAGS = ["HD1_P1_인젝션_ON_T", "HD2_P2_인젝션_ON_T", "HD4_P1_인젝션_ON_T"]
TAG_COLORS = {
    "HD1_P1_인젝션_ON_T": "#0072B2",
    "HD2_P2_인젝션_ON_T": "#D55E00",
    "HD4_P1_인젝션_ON_T": "#009E73",
}

# 10분 단위로 더 세분화한 버킷 (0~2시간) + 그 이상
FINE_BUCKETS_SEC = ([0, 5, 10, 30, 60] + list(range(300, 7200 + 1, 600)) + [float("inf")])
FINE_BUCKET_LABELS = (["5초 미만", "5~10초", "10~30초", "30초~1분"] +
                       [f"{m}~{m+10}분" for m in range(5, 120, 10)] +
                       ["2시간 이상"])

# 활발 가동 vs 정지 판정 경계값. 3개 태그 각각의 로그축 히스토그램에서 "실측값이
# 전혀 없는 빈 공백"을 따로 계산해보면(find_histogram_gaps), 35분(2100초)과
# 2시간(7200초) 둘 다 세 태그 모두의 빈 공백 안에 공통으로 들어간다 —
# 즉 이 값들은 임의로 고른 게 아니라 "이 부근에서는 어느 태그도 실제로 이
# 길이의 구간을 가진 적이 없다"는 데이터 근거가 있는 경계다.
#   HD1_P1 1번째 공백: 27.9~43.6분 / HD2_P2: 35.0~47.0분 / HD4_P1: 20.9~40.0분
#     -> 세 구간의 교집합이 대략 35~40분 -> 35분을 하한 경계로 사용
#   HD1_P1 2번째 공백: 58.7~347.9분 / HD2_P2: 84.9~372.5분 / HD4_P1: 76.6~387.6분
#     -> 2시간(120분)은 세 공백 모두의 내부 -> 2시간을 상한 경계로 사용
GROUP_BOUNDS_SEC = [0, 2100, 7200, float("inf")]
GROUP_LABELS = [
    "활발히 가동 중 (<35분, 정상 사이클 변동)",
    "정기 휴식/전환 추정 (35분~2시간)",
    "장시간 정지 (설비 중단 추정, 2시간 이상)",
]


def fmt_sec(sec):
    if sec < 60:
        return f"{sec:.0f}초"
    if sec < 3600:
        return f"{sec/60:.1f}분"
    return f"{sec/3600:.1f}시간"


def load():
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(
            f"{CSV_PATH} 없음 — Injection_Timer_Stable_Duration_Analysis.py 먼저 실행하세요.")
    df = pd.read_csv(CSV_PATH, parse_dates=["start"])
    return df


def per_tag_table(df):
    """태그 x 세부버킷 표 1개로 통합. 어느 태그든 하나라도 있으면 그 행은
    남기고(개별 태그만 보고 0인 행을 지우면 태그마다 표의 행이 달라져
    비교가 안 된다), 셋 다 0인 행만 뺀다."""
    df = df.copy()
    df["bucket"] = pd.cut(df["duration_sec"], bins=FINE_BUCKETS_SEC, labels=FINE_BUCKET_LABELS,
                           right=False, include_lowest=True)

    table = pd.crosstab(df["bucket"], df["tag"]).reindex(index=FINE_BUCKET_LABELS, columns=TAGS, fill_value=0)
    table = table[table.sum(axis=1) > 0]
    table["합계"] = table.sum(axis=1)

    print("\n=== 태그 x 유지시간 구간 (어느 태그든 1건 이상 있는 구간만) ===")
    print(table.to_string())


def meaningful_groups(df):
    """'활발히 가동 중' vs '정지'를 가르는, 데이터로 뒷받침되는 경계
    (GROUP_BOUNDS_SEC 주석 참고)로 나눠서 횟수/총시간을 본다."""
    df = df.copy()
    df["group"] = pd.cut(df["duration_sec"], bins=GROUP_BOUNDS_SEC, labels=GROUP_LABELS,
                          right=False, include_lowest=True)

    cnt = pd.crosstab(df["group"], df["tag"]).reindex(index=GROUP_LABELS, columns=TAGS, fill_value=0)
    cnt["합계"] = cnt.sum(axis=1)

    time_h = (pd.crosstab(df["group"], df["tag"], values=df["duration_sec"], aggfunc="sum")
              .reindex(index=GROUP_LABELS, columns=TAGS, fill_value=0) / 3600)
    time_h["합계"] = time_h.sum(axis=1)

    print("\n=== 의미있는 그룹으로 나눴을 때: 횟수 ===")
    print(cnt.to_string())
    print("\n=== 같은 그룹의 총 유지시간(시간) ===")
    print(time_h.round(1).to_string())

    total = cnt["합계"].sum()
    print(f"\n횟수 기준 비율: 활발 가동 {cnt.loc[GROUP_LABELS[0], '합계']/total*100:.2f}% / "
          f"정기 휴식 추정 {cnt.loc[GROUP_LABELS[1], '합계']/total*100:.2f}% / "
          f"장시간 정지 {cnt.loc[GROUP_LABELS[2], '합계']/total*100:.2f}%")
    total_h = time_h["합계"].sum()
    print(f"시간 기준 비율: 활발 가동 {time_h.loc[GROUP_LABELS[0], '합계']/total_h*100:.1f}% / "
          f"정기 휴식 추정 {time_h.loc[GROUP_LABELS[1], '합계']/total_h*100:.1f}% / "
          f"장시간 정지 {time_h.loc[GROUP_LABELS[2], '합계']/total_h*100:.1f}%")


def hour_of_day_pattern(df, min_minutes=30):
    """긴 유지 구간(>= min_minutes분)이 하루 중 특정 시각대에 몰리는지 확인."""
    long_df = df[df["duration_sec"] >= min_minutes * 60].copy()
    if long_df.empty:
        return
    long_df["hour"] = long_df["start"].dt.hour
    print(f"\n=== {min_minutes}분 이상 유지 구간의 시작 시각(시 단위) 분포 (태그 합산 {len(long_df)}건) ===")
    table = pd.crosstab(long_df["hour"], long_df["tag"]).reindex(columns=TAGS, fill_value=0)
    table["합계"] = table.sum(axis=1)
    print(table[table["합계"] > 0].to_string())


def plot_per_tag(df):
    fig, axes = plt.subplots(len(TAGS), 2, figsize=(14, 11))

    for row, tag in enumerate(TAGS):
        sub = df[df["tag"] == tag]
        color = TAG_COLORS[tag]
        ax_log, ax_zoom = axes[row]

        # 왼쪽: 로그축 전체 범위 히스토그램 (이 태그만)
        log_vals = np.log10(sub["duration_sec"].to_numpy())
        bins = np.linspace(log_vals.min(), log_vals.max(), 60)
        ax_log.hist(log_vals, bins=bins, color=color, alpha=0.85)
        tick_vals = [1, 5, 10, 30, 60, 300, 600, 1800, 3600, 7200, 21600]
        ax_log.set_xticks(np.log10(tick_vals))
        ax_log.set_xticklabels([fmt_sec(v) for v in tick_vals], rotation=30, ha="right", fontsize=8)
        ax_log.set_ylabel(f"{tag}\n구간 수", fontsize=9)
        if row == 0:
            ax_log.set_title("로그축 — 전체 범위", loc="left", fontsize=11)
        ax_log.spines["top"].set_visible(False)
        ax_log.spines["right"].set_visible(False)

        # 오른쪽: 0~180분 확대, y축은 로그 — 0 근처 수천 건에 50~60분대(수십 건)가
        # 묻히지 않게 한다 (선형 y축이면 사실상 안 보임).
        zoom = sub[sub["duration_sec"] < 180 * 60]
        zbins = np.arange(0, 180 + 2, 2)  # 2분 간격
        ax_zoom.hist(zoom["duration_sec"] / 60.0, bins=zbins, color=color, alpha=0.85)
        ax_zoom.axvspan(45, 60, color="#999999", alpha=0.2)
        ax_zoom.set_yscale("log")
        ax_zoom.set_xlim(0, 180)
        ax_zoom.set_ylabel("구간 수 (로그)", fontsize=9)
        if row == 0:
            ax_zoom.set_title("0~180분 확대, y축 로그 (회색=45~60분대)", loc="left", fontsize=11)
        ax_zoom.spines["top"].set_visible(False)
        ax_zoom.spines["right"].set_visible(False)

        if row == len(TAGS) - 1:
            ax_log.set_xlabel("유지 시간 (로그축)", fontsize=9)
            ax_zoom.set_xlabel("유지 시간 (분)", fontsize=9)

    fig.suptitle("태그별 유지 시간 히스토그램 (왼쪽: 전체 로그축 / 오른쪽: 0~180분 확대)", fontsize=13)
    fig.tight_layout(rect=(0, 0, 1, 0.97))
    out_path = os.path.join(OUT_DIR, "stable_duration_per_tag.png")
    fig.savefig(out_path, dpi=140)
    plt.close(fig)
    return out_path


def main():
    df = load()
    per_tag_table(df)
    meaningful_groups(df)
    hour_of_day_pattern(df, min_minutes=30)
    out_path = plot_per_tag(df)
    print(f"\n그래프 저장: {out_path}")


if __name__ == "__main__":
    main()
