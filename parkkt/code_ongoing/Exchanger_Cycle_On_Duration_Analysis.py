"""`merged_cycles_full.csv` 기준으로 exchanger 가 **켜져 있는(=1) 논리 사이클**이
얼마나 지속되는지(`cycle_start` ~ `cycle_end`)를 날짜별/구간별로 센다.

"논리" 라고 하는 이유: 180초 미만의 짧은 OFF(채터링)는 다리이어서 그 사이클이
계속 켜져 있던 것으로 본다 — 그래서 이 ON 지속시간에는 짧게 깜빡이며 꺼졌던
시간도 포함돼 있다(사용자가 정의한 전처리 규칙 그대로).

사용법 (Exchanger_Cycle_Off_Duration_Analysis.py 를 먼저 돌려 merged_cycles_full.csv
를 만들어둘 것):
    PYTHONIOENCODING=utf-8 python Exchanger_Cycle_On_Duration_Analysis.py
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
OUT_DIR = os.path.join(BASE_DIR, "parkkt", "analysis_out", "exchanger_off_on_gap")
CYCLES_CSV = os.path.join(OUT_DIR, "merged_cycles_full.csv")

#: 정상 사이클 중 가장 긴 축(258.8초)과 기동 직후 캐치업(336.9초) 사이 — 다른
#: 분석들의 17배짜리 공백만큼 뚜렷하진 않다(약 1.3배). 그래서 이건 "완전히
#: 분리된 두 그룹"이라기보단 "꼬리가 약간 두꺼운 분포 + 눈에 띄는 이상치 몇 개"로
#: 보는 게 더 정확하다 — 과장하지 않는다.
CATCHUP_SPLIT_SEC = 300

BUCKETS_SEC = [0, 120, 150, 180, 210, 240, 270, 300, 360, 600, float("inf")]
BUCKET_LABELS = ["~2분", "2~2.5분", "2.5~3분", "3~3.5분", "3.5~4분",
                  "4~4.5분", "4.5~5분", "5~6분", "6~10분", "10분 이상"]

COLOR_NORMAL = "#0072B2"
COLOR_LONG = "#D55E00"


def load():
    df = pd.read_csv(CYCLES_CSV, parse_dates=["cycle_start", "cycle_end", "next_on"])
    df["on_duration_sec"] = (df["cycle_end"] - df["cycle_start"]).dt.total_seconds()
    df["day"] = df["cycle_start"].dt.date
    df["bucket"] = pd.cut(df["on_duration_sec"], bins=BUCKETS_SEC, labels=BUCKET_LABELS,
                           right=False, include_lowest=True)
    # 그날 맨 처음 exchanger 신호 — 그 직전 상태(밤새 다른 모드, 온도가 얼마나
    # 올라 있었는지)가 불분명해서, 주말이 아니어도 시작점 온도가 높으면 똑같이
    # 길어질 수 있다(사용자 지적). 날짜별 최초 1건을 표시만 해두고, 제외 여부는
    # report() 에서 선택적으로 적용한다.
    first_idx = df.groupby("day")["cycle_start"].idxmin()
    df["is_first_of_day"] = df.index.isin(first_idx)
    return df


def report(df, exclude_first_of_day=False):
    if exclude_first_of_day:
        excluded = df[df["is_first_of_day"]]
        print(f"=== 하루 중 맨 처음 exchanger 신호 {len(excluded)}건 제외 ===")
        print(excluded[["cycle_start", "cycle_end", "on_duration_sec"]]
              .assign(on_min=lambda d: (d["on_duration_sec"] / 60).round(2)).drop(columns="on_duration_sec")
              .to_string(index=False))
        df = df[~df["is_first_of_day"]].copy()
        print()

    print(f"ON 사이클 {len(df)}개\n")
    print(df["on_duration_sec"].describe(percentiles=[.1, .25, .5, .75, .9, .95, .99])
          .rename(lambda s: s).to_string())

    print("\n=== 날짜별 x 구간별 ===")
    table = pd.crosstab(df["day"], df["bucket"]).reindex(columns=BUCKET_LABELS, fill_value=0)
    table["합계"] = table.sum(axis=1)
    print(table.to_string())

    print("\n=== 전체 구간별 횟수/비율 ===")
    overall = df["bucket"].value_counts().reindex(BUCKET_LABELS, fill_value=0)
    pct = (overall / overall.sum() * 100).round(1)
    print(pd.DataFrame({"횟수": overall, "비율(%)": pct}).to_string())

    long_df = df[df["on_duration_sec"] >= CATCHUP_SPLIT_SEC].sort_values("cycle_start")
    print(f"\n=== {CATCHUP_SPLIT_SEC}초(5분) 이상 — 눈에 띄는 긴 ON {len(long_df)}건 ===")
    print(long_df[["cycle_start", "cycle_end", "on_duration_sec", "n_bridged_short_offs"]]
          .assign(on_duration_min=lambda d: (d["on_duration_sec"] / 60).round(1))
          .drop(columns="on_duration_sec").to_string(index=False))
    print("  (전부 매일 밤 셧다운 직후 '기동 캐치업' 구간과 시간대가 겹칩니다 —")
    print("   정상 운전 중 짧게 반복되는 사이클과는 다른 성격으로 보는 게 맞습니다.)")
    return df


def plot(df, out_name="cycle_on_duration.png", title_suffix=""):
    is_long = df["on_duration_sec"] >= CATCHUP_SPLIT_SEC
    colors = np.where(is_long, COLOR_LONG, COLOR_NORMAL)

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(13, 9))

    ax1.scatter(df["cycle_start"], df["on_duration_sec"] / 60.0, s=22, c=colors, alpha=0.75, linewidths=0)
    ax1.axhline(CATCHUP_SPLIT_SEC / 60.0, color="#999999", linewidth=1, linestyle="--")
    ax1.set_ylabel("ON 지속시간 (분)")
    ax1.set_title(f"사이클 단위 ON 지속시간 — 시간순 산점도{title_suffix}", loc="left", fontsize=12)
    ax1.grid(axis="y", alpha=0.2)
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)
    handles = [plt.Line2D([0], [0], marker="o", linestyle="", color=COLOR_NORMAL, label="정상 ON 사이클(<5분)"),
               plt.Line2D([0], [0], marker="o", linestyle="", color=COLOR_LONG, label="기동 캐치업(>=5분, 셧다운 직후)")]
    ax1.legend(handles=handles, loc="upper right", frameon=False)

    bins = np.linspace(df["on_duration_sec"].min() / 60.0, df["on_duration_sec"].max() / 60.0, 40)
    ax2.hist(df.loc[~is_long, "on_duration_sec"] / 60.0, bins=bins, color=COLOR_NORMAL, alpha=0.85,
              label="정상 ON 사이클(<5분)")
    if is_long.any():
        ax2.hist(df.loc[is_long, "on_duration_sec"] / 60.0, bins=bins, color=COLOR_LONG, alpha=0.85,
                  label="기동 캐치업(>=5분)")
    ax2.set_xlabel("ON 지속시간 (분)")
    ax2.set_ylabel("사이클 수")
    ax2.set_title(f"ON 지속시간 히스토그램{title_suffix}", loc="left", fontsize=12)
    ax2.legend(loc="upper right", frameon=False)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)

    fig.tight_layout()
    out_path = os.path.join(OUT_DIR, out_name)
    fig.savefig(out_path, dpi=140)
    plt.close(fig)
    return out_path


def main():
    df = load()

    print("########## 전체(제외 없음) ##########")
    full = report(df.copy(), exclude_first_of_day=False)
    out1 = plot(full, out_name="cycle_on_duration.png")
    print(f"그래프 저장: {out1}")

    print("\n\n########## 하루 맨 처음 신호 제외 ##########")
    excl = report(df.copy(), exclude_first_of_day=True)
    out2 = plot(excl, out_name="cycle_on_duration_excl_first.png", title_suffix=" (하루 맨 처음 신호 제외)")
    print(f"그래프 저장: {out2}")


if __name__ == "__main__":
    main()
