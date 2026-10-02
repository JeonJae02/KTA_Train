"""OFF 지속시간(off_duration_sec)과 그 OFF 구간 동안의 최저 온도(PV) 사이의
상관관계를 본다. 사용자 가설: OFF 동안 최저 온도가 낮을수록 OFF 지속시간이
짧다.

`merged_cycles_full.csv`(246개, 채터링 제거) 기준으로, 각 행의 [cycle_end,
next_on] 구간에서 TK_Temp_PV_P1 최솟값을 구한다.

사용법 (temp CSV 가 이미 받아져 있어야 함 — Exchanger_SV_Error_Check.py 참고):
    PYTHONIOENCODING=utf-8 python Exchanger_Off_Duration_vs_MinTemp.py
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
CYCLES_CSV = os.path.join(OUT_DIR, "cycles_with_cool_overlap.csv")

TEMP_TAG = "TK_Temp_PV_P1"
TEMP_SCALE = 10.0

SHUTDOWN_OFF_SEC = 3600
COLOR_NORMAL = "#0072B2"
COLOR_SHUTDOWN = "#D55E00"


def latest_temp_csv():
    cands = sorted(glob.glob(os.path.join(RAW_DIR, "*_analysis.csv")))
    for p in reversed(cands):
        if TEMP_TAG in pd.read_csv(p, nrows=0).columns:
            return p
    raise FileNotFoundError(f"{TEMP_TAG} 포함된 CSV 없음 — Exchanger_SV_Error_Check.py 먼저 실행")


def load_temp():
    path = latest_temp_csv()
    raw = pd.read_csv(path, index_col="Time", parse_dates=True, usecols=["Time", TEMP_TAG])
    raw = raw[~raw.index.duplicated(keep="last")].sort_index()
    return (raw[TEMP_TAG] / TEMP_SCALE).dropna()


def load_cycles():
    return pd.read_csv(CYCLES_CSV, parse_dates=["cycle_start", "cycle_end", "next_on"])


def compute_min_temp(cycles, temp):
    mins = []
    for _, r in cycles.iterrows():
        seg = temp.loc[r["cycle_end"]:r["next_on"]]
        mins.append(seg.min() if len(seg) else np.nan)
    cycles = cycles.copy()
    cycles["off_min_temp"] = mins
    return cycles


def report_and_plot(df):
    df = df.dropna(subset=["off_min_temp", "off_duration_sec"])
    is_shutdown = df["off_duration_sec"] >= SHUTDOWN_OFF_SEC

    for label, sub in (("전체(셧다운 3건 포함)", df), ("정상 OFF만(셧다운 제외)", df[~is_shutdown])):
        if len(sub) < 3:
            continue
        r_p, p_p = stats.pearsonr(sub["off_duration_sec"], sub["off_min_temp"])
        r_s, p_s = stats.spearmanr(sub["off_duration_sec"], sub["off_min_temp"])
        print(f"=== {label} (n={len(sub)}) ===")
        print(f"  Pearson r  = {r_p:.3f} (p={p_p:.3g})")
        print(f"  Spearman ρ = {r_s:.3f} (p={p_s:.3g})")
        print()

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    ax1.scatter(df.loc[~is_shutdown, "off_min_temp"], df.loc[~is_shutdown, "off_duration_sec"] / 60.0,
                s=22, color=COLOR_NORMAL, alpha=0.7, linewidths=0, label="정상 OFF")
    ax1.scatter(df.loc[is_shutdown, "off_min_temp"], df.loc[is_shutdown, "off_duration_sec"] / 60.0,
                s=70, color=COLOR_SHUTDOWN, alpha=0.9, linewidths=2.2, marker="x", label="야간 셧다운")
    ax1.set_xlabel("OFF 구간 최저 온도 (°C)")
    ax1.set_ylabel("OFF 지속시간 (분)")
    ax1.set_yscale("log")
    ax1.set_title("전체 (로그축) — 셧다운 포함", loc="left", fontsize=11)
    ax1.legend(frameon=False, loc="center right")
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)

    sub = df[~is_shutdown]
    ax2.scatter(sub["off_min_temp"], sub["off_duration_sec"] / 60.0, s=24, color=COLOR_NORMAL, alpha=0.7,
                linewidths=0)
    if len(sub) >= 2:
        z = np.polyfit(sub["off_min_temp"], sub["off_duration_sec"] / 60.0, 1)
        xs = np.linspace(sub["off_min_temp"].min(), sub["off_min_temp"].max(), 50)
        ax2.plot(xs, np.polyval(z, xs), color="#D55E00", linewidth=1.5, linestyle="--")
    ax2.set_xlabel("OFF 구간 최저 온도 (°C)")
    ax2.set_ylabel("OFF 지속시간 (분)")
    ax2.set_title("정상 OFF만 확대 (선형축) + 추세선", loc="left", fontsize=11)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)

    fig.suptitle("OFF 지속시간 vs OFF 구간 최저 온도", fontsize=13)
    fig.tight_layout()
    out_path = os.path.join(OUT_DIR, "off_duration_vs_min_temp.png")
    fig.savefig(out_path, dpi=140)
    plt.close(fig)
    return out_path


def main():
    temp = load_temp()
    cycles = load_cycles()
    cycles = compute_min_temp(cycles, temp)
    out_path = report_and_plot(cycles)
    print(f"그래프 저장: {out_path}")

    csv_out = os.path.join(OUT_DIR, "off_duration_vs_min_temp.csv")
    cycles.to_csv(csv_out, index=False, encoding="utf-8-sig")
    print(f"결과 표 저장: {csv_out}")


if __name__ == "__main__":
    main()
