"""OFF 구간에서 히터가 얼마나 잘 작동하는지(= 바닥을 찍은 뒤 SV 위로 얼마나
빨리 회복하는지) 판정할 기준선을 만든다.

**왜 OFF 지속시간 하나만으론 기준이 안 되는가.** 이미 확인한 대로 OFF 지속시간과
최저온도는 강하게 음(-)의 상관(r=-0.89)이다 — 많이 떨어질수록 회복에 오래
걸리는 게 당연하다. 그래서 "10분 넘으면 이상" 같은 고정 시간 기준을 쓰면,
어쩌다 많이 떨어진 정상 사이클을 오탐하고, 조금만 떨어졌는데 회복이 느린
진짜 이상 사이클은 놓친다. **떨어진 폭(ΔT) 대비 회복 속도(기울기)**로 봐야
공정하게 비교된다.

방법:
1. 각 OFF 구간에서 최저온도 시각(t_min)부터 next_on(= SV 재도달 시각)까지를
   "회복 구간"으로 본다.
2. 기울기 = (SV - 최저온도) / 회복시간 [°C/분] — 클수록 히터가 빨리 데운 것.
3. 정상 243개 사이클의 기울기 분포에서 하위 퍼센타일(느린 쪽)을 기준선으로
   잡는다 — Exchanger_Relay_Check.py 에서 쓴 것과 같은 방식(대조군 분포의
   퍼센타일을 임계값으로).
4. 참고용으로, 그 기준선을 "ΔT 얼마당 몇 분"으로 환산해 직관적인 지속시간
   기준도 같이 보여준다.

사용법:
    PYTHONIOENCODING=utf-8 python Exchanger_Heater_Recovery_Slope.py
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
CYCLES_CSV = os.path.join(OUT_DIR, "cycles_with_cool_overlap.csv")

TEMP_TAG = "TK_Temp_PV_P1"
SV_TAG = "TK_Temp_SV_P1"
TEMP_SCALE = 10.0

SHUTDOWN_OFF_SEC = 3600
LOW_PCTS = [1, 5, 10, 25]  # 후보 임계 퍼센타일(기울기 하위 몇 %를 "느림"으로 볼지)

COLOR_NORMAL = "#0072B2"
COLOR_EDGE = "#999999"


def latest_csv_with(tag):
    cands = sorted(glob.glob(os.path.join(RAW_DIR, "*_analysis.csv")))
    for p in reversed(cands):
        if tag in pd.read_csv(p, nrows=0).columns:
            return p
    raise FileNotFoundError(f"{tag} 포함된 CSV 없음")


def load_temp_sv():
    path = latest_csv_with(TEMP_TAG)
    raw = pd.read_csv(path, index_col="Time", parse_dates=True, usecols=["Time", TEMP_TAG, SV_TAG])
    raw = raw[~raw.index.duplicated(keep="last")].sort_index()
    temp = (raw[TEMP_TAG] / TEMP_SCALE).dropna()
    sv = (raw[SV_TAG] / TEMP_SCALE).dropna()
    return temp, sv


def load_cycles():
    df = pd.read_csv(CYCLES_CSV, parse_dates=["cycle_start", "cycle_end", "next_on"])
    df["is_first_of_day"] = df["is_post_shutdown"].astype(bool)
    df["is_pre_shutdown"] = df["off_duration_sec"] >= SHUTDOWN_OFF_SEC
    df["edge_case"] = df["is_first_of_day"] | df["is_pre_shutdown"]
    return df


def compute_recovery(cycles, temp, sv):
    rows = []
    for _, r in cycles.iterrows():
        seg = temp.loc[r["cycle_end"]:r["next_on"]]
        if len(seg) < 3:
            continue
        t_min = seg.idxmin()
        v_min = seg.loc[t_min]
        sv_at_end = sv.reindex([r["next_on"]], method="ffill").iloc[0]
        recovery_sec = (r["next_on"] - t_min).total_seconds()
        delta_t = sv_at_end - v_min
        if recovery_sec <= 0 or delta_t <= 0:
            continue
        slope_per_min = delta_t / (recovery_sec / 60.0)
        rows.append({
            "cycle_end": r["cycle_end"], "next_on": r["next_on"], "t_min": t_min,
            "min_temp": v_min, "sv": sv_at_end, "delta_t": delta_t,
            "recovery_sec": recovery_sec, "slope_per_min": slope_per_min,
            "off_duration_sec": r["off_duration_sec"], "edge_case": r["edge_case"],
        })
    return pd.DataFrame(rows)


def report_and_plot(df):
    normal = df[~df["edge_case"]]
    edge = df[df["edge_case"]]

    print(f"회복구간 계산된 사이클 {len(df)}개 (정상 {len(normal)}, 경계케이스 {len(edge)})\n")
    print("=== 정상 사이클 회복 기울기(°C/분) 분포 ===")
    print(normal["slope_per_min"].describe(percentiles=[.01, .05, .1, .25, .5, .75, .9]).to_string())

    print("\n=== 후보 임계 퍼센타일 ===")
    for p in LOW_PCTS:
        thr = np.percentile(normal["slope_per_min"], p)
        # 참고용 환산: 중앙값 ΔT 기준으로 그 기울기면 회복에 몇 분 걸리는지
        median_dt = normal["delta_t"].median()
        equiv_min = median_dt / thr
        print(f"  하위 {p:>2d}%  임계기울기 = {thr:.3f} °C/분  "
              f"(ΔT={median_dt:.1f}°C 기준이면 회복에 {equiv_min:.1f}분 걸리는 셈)")

    # 회귀: recovery_sec ~ delta_t (기울기 역수 개념 확인용)
    z = np.polyfit(normal["delta_t"], normal["recovery_sec"] / 60.0, 1)
    print(f"\n참고 — 선형회귀: 회복시간(분) ≈ {z[0]:.3f} * ΔT(°C) + {z[1]:.3f}")
    print(f"  (기울기 {z[0]:.3f} 분/°C 의 역수 = {1/z[0]:.3f} °C/분 — 위 중앙값 기울기와 비슷한지 대조용)")

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    ax1.scatter(normal["delta_t"], normal["recovery_sec"] / 60.0, s=22, color=COLOR_NORMAL, alpha=0.7,
                linewidths=0, label="정상")
    if len(edge):
        ax1.scatter(edge["delta_t"], edge["recovery_sec"] / 60.0, s=50, color=COLOR_EDGE, alpha=0.9,
                    marker="^", linewidths=0, label="경계케이스(첫신호/종료직전)")
    xs = np.linspace(normal["delta_t"].min(), normal["delta_t"].max(), 50)
    ax1.plot(xs, np.polyval(z, xs), color="#D55E00", linewidth=1.5, linestyle="--", label="회귀선")
    thr5 = np.percentile(normal["slope_per_min"], 5)
    ax1.plot(xs, xs / thr5, color="#CC79A7", linewidth=1.5, linestyle=":", label="하위5% 기울기 기준선")
    ax1.set_xlabel("ΔT = SV - 최저온도 (°C)")
    ax1.set_ylabel("회복시간 (분)")
    ax1.set_title("ΔT 대비 회복시간 — 기준선 비교", loc="left", fontsize=11)
    ax1.legend(frameon=False, fontsize=8)
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)

    ax2.hist(normal["slope_per_min"], bins=30, color=COLOR_NORMAL, alpha=0.85)
    for p, ls in zip([5, 25], ["--", ":"]):
        thr = np.percentile(normal["slope_per_min"], p)
        ax2.axvline(thr, color="#D55E00", linestyle=ls, linewidth=1.3)
        ax2.text(thr, ax2.get_ylim()[1]*0.95, f" 하위{p}%", color="#D55E00", fontsize=8, rotation=90, va="top")
    ax2.set_xlabel("회복 기울기 (°C/분)")
    ax2.set_ylabel("사이클 수")
    ax2.set_title("회복 기울기 분포 + 후보 임계선", loc="left", fontsize=11)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)

    fig.suptitle("히터 회복 성능 기준선 — ΔT 대비 기울기 기반", fontsize=13)
    fig.tight_layout()
    out_path = os.path.join(OUT_DIR, "heater_recovery_slope.png")
    fig.savefig(out_path, dpi=140)
    plt.close(fig)
    return out_path


def main():
    temp, sv = load_temp_sv()
    cycles = load_cycles()
    df = compute_recovery(cycles, temp, sv)
    out_path = report_and_plot(df)
    print(f"\n그래프 저장: {out_path}")

    csv_out = os.path.join(OUT_DIR, "heater_recovery_slope.csv")
    df.to_csv(csv_out, index=False, encoding="utf-8-sig")
    print(f"결과 표 저장: {csv_out}")


if __name__ == "__main__":
    main()
