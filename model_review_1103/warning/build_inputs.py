"""filtering.xlsx + warnings.jsonl* → 검토 입력 CSV.

    cd model_review_1103
    ../venv/bin/python warning/build_inputs.py

만드는 것 (전부 다시 만들 수 있다 — 손으로 고치지 않는다)
    warning/actions/actions_conveyor.csv   작업ID 한 행, 일지 원본 열 + action_map 열
    warning/actions/actions_pump.csv
    warning/actions/actions_etc.csv
    warning/actions/line2_unfiltered.csv   '전체' 시트의 2라인 기록 중 필터 시트·action_map 에 없는 것 (참고)
    warning/episodes/warning_events.csv    경고 이벤트 한 줄 = 한 행 (KST, 중복 제거)
    warning/episodes/warning_episodes.csv  경고 한 건 = 한 행

손으로 고치는 것
    warning/actions/action_map.csv         작업ID → 사건 묶음·설비키·모델범위·예상 사유
"""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from review_lib import actions as A                # noqa: E402
from review_lib import paths                       # noqa: E402
from review_lib import warnings_log as W           # noqa: E402


def build_actions():
    amap = pd.read_csv(paths.ACTIONS_DIR / "action_map.csv", dtype=str)
    filt = A.load_filtered()

    # action_map 에 있지만 필터 시트에 없는 작업ID → '전체' 시트에서 끌어온다 (예: 2라인 온조기)
    whole = A.read_sheet("전체")
    extra_ids = set(amap["작업ID"]) - set(filt["작업ID"])
    extra = whole[whole["작업ID"].isin(extra_ids)][A.KEEP_COLS].copy()
    extra["sheet_src"] = "전체"
    extra["sheet_domain"] = None
    df = pd.concat([filt, extra], ignore_index=True)

    unmapped = sorted(set(df["작업ID"]) - set(amap["작업ID"]))
    if unmapped:
        print(f"❗ action_map.csv 에 없는 작업ID {len(unmapped)}개 — 행을 추가하고 다시 돌린다: {unmapped}")

    df = A.add_derived(df).merge(amap, on="작업ID", how="left")
    df["domain"] = df["domain"].fillna(df["sheet_domain"])
    df = df.drop(columns=["sheet_domain"]).sort_values(["domain", "start_dt", "작업ID"])

    first = ["작업ID", "incident_id", "domain", "작업일", "start_dt", "end_dt", "journal", "sheet_src",
             "device_key", "sub_unit", "wagon_no", "model_scope", "expected_reasons"]
    df = df[first + [c for c in df.columns if c not in first]]
    for dom, part in df.groupby("domain"):
        f = paths.ACTIONS_DIR / f"actions_{dom}.csv"
        part.to_csv(f, index=False, encoding="utf-8-sig")
        print(f"  {f.relative_to(paths.ROOT)}: {len(part)}행, 사건 {part['incident_id'].nunique()}개")

    rest = A.load_unfiltered_line2(set(df["작업ID"]))
    rest = A.add_derived(rest)
    f = paths.ACTIONS_DIR / "line2_unfiltered.csv"
    rest.to_csv(f, index=False, encoding="utf-8-sig")
    print(f"  {f.relative_to(paths.ROOT)}: {len(rest)}행 (필터에서 빠진 2라인 기록 — 참고용)")


def build_warnings():
    paths.EPISODES_DIR.mkdir(parents=True, exist_ok=True)
    ev = W.load_events()
    f = paths.EPISODES_DIR / "warning_events.csv"
    ev.to_csv(f, index=False, encoding="utf-8-sig")
    print(f"  {f.relative_to(paths.ROOT)}: {len(ev)}행 ({ev['ts'].min()} ~ {ev['ts'].max()})")

    ep = W.build_episodes(ev)
    f = paths.EPISODES_DIR / "warning_episodes.csv"
    ep.to_csv(f, index=False, encoding="utf-8-sig")
    in_review = ep[ep["last_detected"] >= paths.REVIEW_START]
    print(f"  {f.relative_to(paths.ROOT)}: {len(ep)}건 (검토 기간에 걸친 것 {len(in_review)}건)")
    print(in_review.groupby(["domain", "kind"]).size().rename("에피소드").to_string())


if __name__ == "__main__":
    print("[조치]")
    build_actions()
    print("[경고]")
    build_warnings()
