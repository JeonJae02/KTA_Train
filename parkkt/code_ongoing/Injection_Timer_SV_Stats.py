"""
HD1~4 x P1/P2 인젝션 ON/OFF 타이머 vs FLT_SV(임계값) 통계.

요청 범위: 2026-09-10 ~ now, 제외 규칙 없이 원본 그대로 (사용자 지정).
태그 3종 세트 (예: HD1_P1_인젝션_ON_T):
  - <base>            : 타이머 실측값
  - <base>_FLT        : 알람 비트 (PLC 가 SV 초과 시 세우는 것으로 추정)
  - <base>_FLT_SV     : SV(임계값)
"""
import os, sys, pickle
import numpy as np
import pandas as pd

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT_DIR)
from Log_Extractor import LogExtractor

HEADS = ["HD1", "HD2", "HD3", "HD4"]
PUMPS = ["P1", "P2"]
DIRS = ["ON", "OFF"]

START = "2026-09-10T00:00:00Z"
END = "now()"

CACHE_DIR = os.path.join(ROOT_DIR, "parkkt", "cache")
os.makedirs(CACHE_DIR, exist_ok=True)
CACHE_PATH = os.path.join(CACHE_DIR, "injection_timer_sv_raw_20260910.pkl")


def base_tags():
    out = []
    for hd in HEADS:
        for p in PUMPS:
            for d in DIRS:
                out.append(f"{hd}_{p}_인젝션_{d}_T")
    return out


def all_tags():
    tags = []
    for b in base_tags():
        tags += [b, f"{b}_FLT", f"{b}_FLT_SV"]
    return tags


def load_raw():
    if os.path.exists(CACHE_PATH):
        with open(CACHE_PATH, "rb") as f:
            return pickle.load(f)
    ex = LogExtractor(env_path=os.path.join(ROOT_DIR, ".env"))
    df = ex.get_data(start_time=START, end_time=END, target_tags=all_tags())
    with open(CACHE_PATH, "wb") as f:
        pickle.dump(df, f)
    return df


def exceed_episodes(df, flt_col, sv_col, timer_col):
    """FLT 비트가 1인 연속 구간을 (start, end, duration, sv, peak_timer) 로 묶는다."""
    flt = df[flt_col].fillna(0).astype(int)
    grp = (flt != flt.shift(fill_value=0)).cumsum()
    episodes = []
    for _, seg in df.assign(_flt=flt, _grp=grp).groupby("_grp"):
        if seg["_flt"].iloc[0] != 1:
            continue
        episodes.append({
            "start": seg.index.min(),
            "end": seg.index.max(),
            "duration_min": (seg.index.max() - seg.index.min()).total_seconds() / 60,
            "sv_at_start": seg[sv_col].iloc[0],
            "peak_timer": seg[timer_col].max(),
        })
    return pd.DataFrame(episodes)


def main():
    df = load_raw()
    print(f"원본: {len(df):,}행 x {len(df.columns)}컬럼 "
          f"({df.index.min()} ~ {df.index.max()})")

    summary_rows = []
    all_episodes = {}

    for b in base_tags():
        timer_col, flt_col, sv_col = b, f"{b}_FLT", f"{b}_FLT_SV"
        if not all(c in df.columns for c in (timer_col, flt_col, sv_col)):
            print(f"⚠️ 태그 없음, 건너뜀: {b}")
            continue

        sub = df[[timer_col, flt_col, sv_col]].dropna()
        sub[flt_col] = sub[flt_col].astype(int)

        for sv, g in sub.groupby(sv_col):
            summary_rows.append({
                "tag": b,
                "SV": sv,
                "n_ticks": len(g),
                "timer_mean": g[timer_col].mean(),
                "timer_max": g[timer_col].max(),
                "timer_min": g[timer_col].min(),
                "timer_std": g[timer_col].std(),
                "n_exceed_ticks": int(g[flt_col].sum()),
            })

        eps = exceed_episodes(sub, flt_col, sv_col, timer_col)
        if not eps.empty:
            all_episodes[b] = eps

    summary_df = pd.DataFrame(summary_rows).sort_values(["tag", "SV"])

    out_dir = os.path.join(ROOT_DIR, "parkkt", "analysis_out", "injection_timer_sv")
    os.makedirs(out_dir, exist_ok=True)
    summary_df.to_csv(os.path.join(out_dir, "sv_timer_summary.csv"),
                       index=False, encoding="utf-8-sig")

    with pd.ExcelWriter(os.path.join(out_dir, "exceed_episodes.xlsx")) as xw:
        for tag, eps in all_episodes.items():
            eps.to_excel(xw, sheet_name=tag[:31], index=False)

    print("\n=== SV 별 타이머 통계 ===")
    print(summary_df.round(3).to_string(index=False))

    print("\n=== 초과(FLT=1) 구간 개수 ===")
    for tag, eps in all_episodes.items():
        print(f"{tag}: {len(eps)}건, 총 {eps['duration_min'].sum():.1f}분")

    print(f"\n저장 위치: {out_dir}")


if __name__ == "__main__":
    main()
