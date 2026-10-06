"""
인젝션 ON_T/OFF_T 단순 임계값 감시 (SOL/PNP 무시).

사용자 의도: 사이클을 정확히 세는 게 목적이 아니라, 타이머 값이 알람 문턱(ON>20,
OFF>=10) 에 가까워지는 조기경보 구간(ON 17~19)을 잡아내는 것. ON_T/OFF_T 는
사이클이 완료될 때만 값이 바뀌는 값이므로(직전 값 유지), "값이 바뀐 시점"만
이벤트로 취급하면 같은 값이 유지되는 동안 중복 경보가 안 나온다.

기간: 2026-09-10 ~ now (기존 캐시 재사용, parkkt/cache/injection_cycle_raw_20260910.pkl)
"""
import os, sys, pickle
import pandas as pd

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT_DIR)
from Log_Extractor import LogExtractor

CACHE_PATH = os.path.join(ROOT_DIR, "parkkt", "cache", "injection_cycle_raw_20260909_1900.pkl")

PAIRS = ["HD1_P1", "HD1_P2", "HD2_P1", "HD2_P2", "HD3_P1", "HD3_P2", "HD4_P1"]

ON_WARN, ON_ALARM = 17, 20     # ON_T: 17~20 = 경고, >20 = 알람(컨베이어 정지)
OFF_ALARM = 10                  # OFF_T: >=10 = 알람


def load_raw():
    with open(CACHE_PATH, "rb") as f:
        return pickle.load(f)


def value_change_events(df, col, threshold, op=">="):
    """col 값이 바뀐 시점(직전 값과 다를 때)만 뽑아서, threshold 조건을 만족하는 것만 남긴다."""
    v = df[col]
    changed = v != v.shift(1)
    ev = df.loc[changed, [col]].rename(columns={col: "value"})
    if op == ">=":
        ev = ev[ev["value"] >= threshold]
    else:
        ev = ev[ev["value"] > threshold]
    return ev


def main():
    df = load_raw()
    print(f"원본: {len(df):,}행 ({df.index.min()} ~ {df.index.max()})")

    on_rows, off_rows = [], []
    for pid in PAIRS:
        on_t, off_t = f"{pid}_인젝션_ON_T", f"{pid}_인젝션_OFF_T"
        if on_t not in df.columns:
            continue
        sub = df[[on_t, off_t]].dropna()

        warn = value_change_events(sub, on_t, ON_WARN, ">=")
        warn = warn[warn["value"] <= ON_ALARM]  # 17~20 구간만 (20 초과는 아래 alarm에서 별도)
        for t, r in warn.iterrows():
            on_rows.append({"head": pid, "time": t, "value": r["value"], "type": "경고(17~20)"})

        alarm = value_change_events(sub, on_t, ON_ALARM, ">")
        for t, r in alarm.iterrows():
            on_rows.append({"head": pid, "time": t, "value": r["value"], "type": "알람(>20)"})

        off_alarm = value_change_events(sub, off_t, OFF_ALARM, ">=")
        for t, r in off_alarm.iterrows():
            off_rows.append({"head": pid, "time": t, "value": r["value"], "type": "알람(>=10)"})

    on_df = pd.DataFrame(on_rows, columns=["head", "time", "value", "type"]).sort_values("time")
    off_df = pd.DataFrame(off_rows, columns=["head", "time", "value", "type"]).sort_values("time")

    out_dir = os.path.join(ROOT_DIR, "parkkt", "analysis_out", "injection_threshold_watch")
    os.makedirs(out_dir, exist_ok=True)
    on_df.to_csv(os.path.join(out_dir, "on_t_events.csv"), index=False, encoding="utf-8-sig")
    off_df.to_csv(os.path.join(out_dir, "off_t_events.csv"), index=False, encoding="utf-8-sig")

    print(f"\n=== ON_T 17 이상 이벤트: 총 {len(on_df)}건 ===")
    print(on_df["head"].value_counts().rename("count").to_string())
    print(f"\n  경고(17~20): {(on_df['type']=='경고(17~20)').sum()}건")
    print(f"  알람(>20)  : {(on_df['type']=='알람(>20)').sum()}건")

    print(f"\n=== OFF_T >=10 이벤트: 총 {len(off_df)}건 ===")
    if not off_df.empty:
        print(off_df["head"].value_counts().rename("count").to_string())
    else:
        print("(없음)")

    print(f"\n저장 위치: {out_dir}")


if __name__ == "__main__":
    main()
