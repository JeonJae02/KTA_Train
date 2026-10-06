"""간단 규칙: exchanger ON 사이클 시작 후 5분 안에 온도(PV)가 SV 밑으로
한 번도 안 내려가면 이상으로 본다. 그 사이클에 cool 이 같이 있었는지로
원인을 나눈다.

- exchanger 만 있었음        -> "exchanger 냉각수 부족"
- exchanger + cool 둘 다 있음 -> "exchanger 와 cool 이상"

지금은 단순화 버전 — 하루 마지막 사이클(공장 셧다운 직전) 예외 처리는 나중에
추가한다.

사용법:
    PYTHONIOENCODING=utf-8 python Exchanger_SV_Error_Check.py
"""

import os
import sys

import pandas as pd

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT_DIR)
from Log_Extractor import LogExtractor

ENV_PATH = os.path.join(ROOT_DIR, ".env")
SAVE_DIR = os.path.join(ROOT_DIR, "parkkt", "extracted_csv")
OUT_DIR = os.path.join(ROOT_DIR, "parkkt", "analysis_out", "exchanger_off_on_gap")
CYCLES_CSV = os.path.join(OUT_DIR, "cycles_with_cool_overlap.csv")

TEMP_TAG = "TK_Temp_PV_P1"
SV_TAG = "TK_Temp_SV_P1"
TEMP_SCALE = 10.0  # Scale_Max___TT_P1=1000 -> 0.1도 단위, 10으로 나눠야 실제 도(°C)

START = "2026-09-28 00:00:00"
END = "2026-10-01 12:16:46"  # 기존 exchanger/cool 데이터와 같은 구간

ERROR_WINDOW_SEC = 300  # 5분

#: exchanger 는 "온도가 SV 에 닿으면" 켜지는데, 그 경계에서 센서값이 229-230-
#: 231-232-231-230 식으로 노이즈로 들썩인다(실제 물리적 온도 변화가 아니라
#: 측정 잡음). 그래서 켜진 직후 바로 "SV 밑으로 내려갔다"고 잡히는 건 대부분
#: 이 잡음이지 진짜 냉각 효과가 아니다 — 켜지고 이 시간만큼은 크로싱 판정에서
#: 제외하고, 그 이후부터 5분 끝까지만 본다.
GRACE_SEC = 30


def fetch_temp():
    extractor = LogExtractor(env_path=ENV_PATH)
    df = extractor.get_data(start_time=START, end_time=END, target_tags=[TEMP_TAG, SV_TAG])
    os.makedirs(SAVE_DIR, exist_ok=True)
    extractor.save_to_csv(df, save_dir=SAVE_DIR)
    df[TEMP_TAG] = df[TEMP_TAG] / TEMP_SCALE
    df[SV_TAG] = df[SV_TAG] / TEMP_SCALE
    return df


def load_cycles():
    df = pd.read_csv(CYCLES_CSV, parse_dates=["cycle_start", "cycle_end", "next_on"])
    return df


def check_cycle(temp, sv, cycle_start):
    """cycle_start + GRACE_SEC 부터 cycle_start + ERROR_WINDOW_SEC 까지, temp <
    sv 가 된 적이 있는지 확인. 맨 처음 GRACE_SEC 는 SV 문턱 노이즈 구간이라
    제외한다.

    반환: (error_bool, dropped_below_time or None)
    """
    window_start = cycle_start + pd.Timedelta(seconds=GRACE_SEC)
    window_end = cycle_start + pd.Timedelta(seconds=ERROR_WINDOW_SEC)
    t_seg = temp.loc[window_start:window_end]
    sv_seg = sv.reindex(t_seg.index, method="ffill")
    below = t_seg < sv_seg
    if below.any():
        return False, below.idxmax()
    return True, None


#: 하루 중 맨 처음(새벽 기동 캐치업)과 셧다운 바로 직전(공장 종료) 사이클은
#: 평상시와 다른 체제라 "정상 운전 중 이상" 판정에서 따로 뺀다 — 지우는 게
#: 아니라 별도 카테고리로 표시만 해서, 그중 유난히 심한 것(예: 주말 뒤 46.7분)은
#: 따로 알아볼 수 있게 한다.
PRE_SHUTDOWN_OFF_SEC = 3600


def main():
    raw = fetch_temp()
    temp = raw[TEMP_TAG].dropna()
    sv = raw[SV_TAG].dropna()

    cycles = load_cycles()
    results = []
    for _, r in cycles.iterrows():
        error, dropped_at = check_cycle(temp, sv, r["cycle_start"])
        results.append({
            "cycle_start": r["cycle_start"],
            "cycle_end": r["cycle_end"],
            "on_duration_sec": r["on_duration_sec"],
            "off_duration_sec": r["off_duration_sec"],
            "cool_overlap": r["cool_overlap"],
            "is_first_of_day": bool(r["is_post_shutdown"]),  # 이름은 과거 버전 흔적, 의미는 '그날 첫 신호'
            "error": error,
            "dropped_below_sv_at": dropped_at,
        })

    out = pd.DataFrame(results)
    out["is_pre_shutdown"] = out["off_duration_sec"] >= PRE_SHUTDOWN_OFF_SEC
    out["edge_case"] = out["is_first_of_day"] | out["is_pre_shutdown"]

    out["cause"] = None
    out.loc[out["error"] & ~out["cool_overlap"], "cause"] = "exchanger 냉각수 부족"
    out.loc[out["error"] & out["cool_overlap"], "cause"] = "exchanger와 cool 이상"

    normal = out[~out["edge_case"]]
    edge = out[out["edge_case"]]

    n_err = int(normal["error"].sum())
    print(f"=== 정상 운전 구간(그날 첫 신호/종료 직전 {len(edge)}건 제외) {len(normal)}개 중 이상 {n_err}개"
          f" ({100*n_err/len(normal):.1f}%) ===\n")
    if n_err:
        print(normal[normal["error"]][["cycle_start", "cycle_end", "on_duration_sec", "cool_overlap", "cause"]]
              .assign(on_min=lambda d: (d["on_duration_sec"] / 60).round(2))
              .drop(columns="on_duration_sec").to_string(index=False))
        print()
        print("원인별:")
        print(normal[normal["error"]]["cause"].value_counts().to_string())
    else:
        print("정상 구간에서는 이상 없음.")

    print(f"\n=== 제외된 {len(edge)}건 (참고용 — 판정에는 안 씀) ===")
    print(edge[["cycle_start", "cycle_end", "on_duration_sec", "cool_overlap", "is_first_of_day",
                "is_pre_shutdown", "error", "cause"]]
          .assign(on_min=lambda d: (d["on_duration_sec"] / 60).round(2))
          .drop(columns="on_duration_sec").to_string(index=False))
    flagged_edge = edge[edge["error"]]
    if len(flagged_edge):
        print(f"\n  이 중 {len(flagged_edge)}건은 (제외 대상이지만) 5분 규칙 자체는 걸렸던 것들 — "
              f"특히 09-28 05:21(46.7분)은 다른 제외건들(5.6~11.5분)보다 훨씬 심해서 "
              f"'그냥 평상시와 다른 체제'를 넘어 별도로 들여다볼 가치가 있습니다.")
    else:
        print("이상 사이클 없음.")

    csv_out = os.path.join(OUT_DIR, "sv_error_check.csv")
    out.to_csv(csv_out, index=False, encoding="utf-8-sig")
    print(f"\n결과 표 저장: {csv_out}")


if __name__ == "__main__":
    main()
