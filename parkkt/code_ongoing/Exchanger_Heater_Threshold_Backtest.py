"""히터 회복 기준선을 좁게(이번 주 데이터 퍼센타일 그대로) 썼을 때와, 조금
넓게(더 관대한 퍼센타일 + 더 긴 백스톱) 썼을 때를 **우리가 가진 데이터에
그대로 적용**해서 몇 건이 걸리는지 비교한다.

**왜 넓혀야 하는가.** 지금 임계값(하위5%/25%)은 전부 이번 주 239개 사이클
자체에서 뽑은 값이다 — 그 데이터에 그대로 테스트하면 정의상 딱 5%/25%가
걸린다(순환 논리). 미래 데이터는 이번 주보다 변동폭이 더 클 수 있으니, 지금
갖고 있는 건 "정답(=확실히 정상)" 데이터라고 보고 거기서 오탐이 최대한
안 나오게 여유를 더 주는 게 안전하다.

사용법:
    PYTHONIOENCODING=utf-8 python Exchanger_Heater_Threshold_Backtest.py
"""

import os

import numpy as np
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
OUT_DIR = os.path.join(BASE_DIR, "parkkt", "analysis_out", "exchanger_off_on_gap")
SLOPE_CSV = os.path.join(OUT_DIR, "heater_recovery_slope.csv")

# ---- BEFORE(참고용 — 이번 주 데이터에서 그대로 뽑은 좁은 값) ----
BEFORE = {
    "주의_pct": 25, "확인요망_pct": 5,
    "백스톱_min": 30,
}
# ---- ADOPTED(퍼센타일 기반, 2026-10-01 1차 확정 — 참고용으로 남겨둠) ----
ADOPTED = {
    "주의_pct": 10, "확인요망_pct": 1,
    "백스톱_min": 40,
}
# ---- USER(사용자 지정 고정값, 2026-10-01 최종 확정) ----
# 퍼센타일 기반(ADOPTED)도 정상 239개 중 24개(10%)가 걸려서 "분포가 너무 좁다"는
# 판단 — 퍼센타일 대신 **고정된 절대 기울기 값**으로 바꾼다. 0.251 은 정상
# 239개의 실측 최솟값과 같아서(사실상 "역대 가장 느렸던 것보다 느리면 주의"),
# 주의 단계에서 오탐이 거의 안 나온다. 0.200 은 그보다 확실히 더 아래라 여유가
# 크다.
USER_FIXED = {"warn": 0.251, "alert": 0.200, "백스톱_min": 40}


def classify(df, normal_slopes, warn_pct, alarm_pct, backstop_min):
    warn_thr = np.percentile(normal_slopes, warn_pct)
    alarm_thr = np.percentile(normal_slopes, alarm_pct)

    status = pd.Series("정상", index=df.index)
    status[df["slope_per_min"] < warn_thr] = "주의"
    status[df["slope_per_min"] < alarm_thr] = "확인요망"
    # 절대 시간 백스톱 — 기울기 판정과 무관하게 OFF 가 이 시간을 넘기면 확인요망
    status[df["off_duration_sec"] / 60.0 >= backstop_min] = "확인요망(백스톱)"
    return status, warn_thr, alarm_thr


def classify_fixed(df, warn_thr, alert_thr, backstop_min):
    """퍼센타일이 아니라 고정된 절대 기울기 값으로 판정 — 라벨도 주의/경고로."""
    status = pd.Series("정상", index=df.index)
    status[df["slope_per_min"] < warn_thr] = "주의"
    status[df["slope_per_min"] < alert_thr] = "경고"
    status[df["off_duration_sec"] / 60.0 >= backstop_min] = "경고(백스톱)"
    return status


def run_fixed_version(name, cfg, df):
    status = classify_fixed(df, cfg["warn"], cfg["alert"], cfg["백스톱_min"])
    df = df.copy()
    df[f"status_{name}"] = status

    print(f"=== {name} (주의 < {cfg['warn']} °C/분, 경고 < {cfg['alert']} °C/분, "
          f"백스톱 = {cfg['백스톱_min']}분) ===")

    normal_part = df[~df["edge_case"]]
    edge_part = df[df["edge_case"]]

    print("정상(239개, '진짜 정상'으로 알고 있는 데이터) 중:")
    print(normal_part[f"status_{name}"].value_counts().to_string())
    n_fp = (normal_part[f"status_{name}"] != "정상").sum()
    print(f"  → 오탐 {n_fp}개 ({100*n_fp/len(normal_part):.1f}%)")
    if n_fp:
        flagged = normal_part[normal_part[f"status_{name}"] != "정상"]
        print(flagged[["cycle_end", "min_temp", "delta_t", "recovery_min", "slope_per_min", f"status_{name}"]]
              .to_string(index=False))

    print("\n경계케이스(7개, 첫신호/종료직전) 중:")
    print(edge_part[["cycle_end", "off_duration_sec", f"status_{name}"]]
          .assign(off_min=lambda d: (d["off_duration_sec"] / 60).round(2))
          .drop(columns="off_duration_sec").to_string(index=False))
    print()
    return df


def run_version(name, cfg, df, normal_slopes):
    status, warn_thr, alarm_thr = classify(df, normal_slopes, cfg["주의_pct"], cfg["확인요망_pct"],
                                            cfg["백스톱_min"])
    df = df.copy()
    df[f"status_{name}"] = status

    print(f"=== {name} (주의=하위{cfg['주의_pct']}%={warn_thr:.3f}°C/분, "
          f"확인요망=하위{cfg['확인요망_pct']}%={alarm_thr:.3f}°C/분, 백스톱={cfg['백스톱_min']}분) ===")

    normal_part = df[~df["edge_case"]]
    edge_part = df[df["edge_case"]]

    print("정상(239개, '진짜 정상'으로 알고 있는 데이터) 중:")
    print(normal_part[f"status_{name}"].value_counts().to_string())
    n_fp = (normal_part[f"status_{name}"] != "정상").sum()
    print(f"  → 오탐(원래 정상인데 주의/확인요망 뜬 것) {n_fp}개 ({100*n_fp/len(normal_part):.1f}%)")

    print("\n경계케이스(7개, 첫신호/종료직전) 중:")
    print(edge_part[["cycle_end", "off_duration_sec", f"status_{name}"]]
          .assign(off_min=lambda d: (d["off_duration_sec"] / 60).round(2))
          .drop(columns="off_duration_sec").to_string(index=False))
    print()
    return df, status


def add_expected_baseline(df, normal_slopes):
    """중앙값 기울기를 '원래 이 정도면 됐어야 하는' 기준으로 써서,
    실제 회복시간과 비교할 수 있게 expected_min/초과분/초과비율을 붙인다."""
    median_slope = normal_slopes.median()
    df = df.copy()
    df["expected_min"] = df["delta_t"] / median_slope
    df["recovery_min"] = df["recovery_sec"] / 60.0
    df["초과분"] = df["recovery_min"] - df["expected_min"]
    df["초과비율"] = df["recovery_min"] / df["expected_min"]
    return df, median_slope


def main():
    df = pd.read_csv(SLOPE_CSV, parse_dates=["cycle_end", "next_on", "t_min"])
    normal_slopes = df.loc[~df["edge_case"], "slope_per_min"]

    df, median_slope = add_expected_baseline(df, normal_slopes)
    print(f"기준 중앙값 기울기: {median_slope:.4f} °C/분 "
          f"(= expected_min 계산에 쓴 '원래 이 정도면 됐어야 하는' 속도)\n")

    df, status_before = run_version("BEFORE", BEFORE, df, normal_slopes)
    df, status_adopted = run_version("ADOPTED", ADOPTED, df, normal_slopes)

    print("=== BEFORE vs ADOPTED 요약 비교 (정상 239개 기준 오탐률) ===")
    for name in ("BEFORE", "ADOPTED"):
        col = f"status_{name}"
        normal_part = df[~df["edge_case"]]
        n_fp = (normal_part[col] != "정상").sum()
        print(f"  {name}: 오탐 {n_fp}/{len(normal_part)} ({100*n_fp/len(normal_part):.1f}%)")

    detail_cols = ["cycle_end", "min_temp", "delta_t", "expected_min", "recovery_min", "초과분", "초과비율",
                   "slope_per_min"]
    normal_part = df[~df["edge_case"]]
    for status in ("확인요망", "주의"):
        sub = normal_part[normal_part["status_ADOPTED"] == status].sort_values("slope_per_min")
        print(f"\n=== ADOPTED 기준 '{status}' {len(sub)}건 — 예상 vs 실제 회복시간 ===")
        print(sub[detail_cols].round(2).to_string(index=False))

    df = run_fixed_version("USER", USER_FIXED, df)

    csv_out = os.path.join(OUT_DIR, "heater_threshold_backtest.csv")
    df.to_csv(csv_out, index=False, encoding="utf-8-sig")
    print(f"\n결과 표 저장: {csv_out}")


if __name__ == "__main__":
    main()
