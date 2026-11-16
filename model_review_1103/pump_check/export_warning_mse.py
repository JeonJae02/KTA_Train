"""펌프 경고 로그 한 줄마다 그때의 MSE(사이클 AE 복원 오차)를 붙여 CSV 로 저장한다.

입력  warning/episodes/warning_events.csv   (build_inputs.py 가 warnings.jsonl* 에서 만든 것)
      warning/episodes/warning_episodes.csv (경고 한 건 = episode_id)
      pump_check/labels_episodes.csv        (판정이 있으면 붙인다)
출력  pump_check/pump_warning_mse.csv

열의 뜻
- mse        : 그 로그 시각 직전 자동 운전 사이클 1개의 AE 점수 (운영 코드 pump_realtime.pump_scores 의 score)
- threshold  : 그 펌프 묶음의 '알림' 사이클 문턱 (meta.json thresholds[group].alert.cycle)
- mse_ratio  : mse / threshold. 1 이상이면 그 사이클 하나만으로 알림 수준
- level      : 펌프 창 등급 (최근 10사이클 중앙값 + 피처 z). 경고는 이 값으로 뜬다
- cycle_level: 그 사이클 하나의 등급
따라서 mse < threshold 인데 level 이 알림인 행이 정상적으로 있다 (창 중앙값·피처 z 로 뜬 경우).

실행: python pump_check/export_warning_mse.py   (model_review_1103 에서)
"""
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
EV = ROOT / "warning/episodes/warning_events.csv"
EP = ROOT / "warning/episodes/warning_episodes.csv"
LB = ROOT / "pump_check/labels_episodes.csv"
OUT = ROOT / "pump_check/pump_warning_mse.csv"


def top_cols(s):
    """top = [{"feature": .., "share": ..}, ...] → top1_feature, top1_share, ... top3."""
    out = {}
    items = json.loads(s) if isinstance(s, str) and s else []
    for i in range(3):
        d = items[i] if i < len(items) else {}
        out[f"top{i + 1}_feature"] = d.get("feature")
        out[f"top{i + 1}_share"] = d.get("share")
    return pd.Series(out)


def main():
    ev = pd.read_csv(EV, encoding="utf-8-sig", low_memory=False, parse_dates=["ts", "time"])
    ev = ev[ev["kind"] == "pump"].copy()
    ep = pd.read_csv(EP, encoding="utf-8-sig", parse_dates=["onset"])
    ep = ep[ep["kind"] == "pump"][["episode_id", "key", "onset"]]

    # 경고 한 건 = (kind, key, onset) — warnings_log.build_episodes 와 같은 기준
    ev = ev.merge(ep, left_on=["key", "time"], right_on=["key", "onset"], how="left")

    if LB.exists():
        lb = pd.read_csv(LB, encoding="utf-8-sig", dtype=str, keep_default_na=False)
        ev = ev.merge(lb[["episode_id", "cm", "matched_incident"]].rename(columns={"cm": "episode_cm"}),
                      on="episode_id", how="left")

    ev["mse_ratio"] = ev["mse"] / ev["threshold"]
    ev["mse_over_threshold"] = ev["mse"] > ev["threshold"]
    ev = pd.concat([ev, ev["top"].apply(top_cols)], axis=1)

    cols = ["ts", "episode_id", "event", "key", "device_key", "model_ver",
            "mse", "threshold", "mse_ratio", "mse_over_threshold", "level", "cycle_level",
            "reason", "type", "message",
            "top1_feature", "top1_share", "top2_feature", "top2_share", "top3_feature", "top3_share",
            "onset", "last_detected", "duration_sec", "episode_cm", "matched_incident", "_file"]
    out = ev[[c for c in cols if c in ev.columns]].rename(columns={"key": "pump", "_file": "source_file"})
    out = out.sort_values(["ts", "pump"]).reset_index(drop=True)
    out.to_csv(OUT, index=False, encoding="utf-8-sig", float_format="%.6g")

    print(f"{OUT}  {len(out)}행  {out.ts.min()} ~ {out.ts.max()}")
    print("episode 못 붙은 행:", int(out.episode_id.isna().sum()))
    print("mse 빈 행:", out.groupby("event").mse.apply(lambda s: int(s.isna().sum())).to_dict())
    s = out.dropna(subset=["mse"])
    print("mse > threshold 비율 (event별):",
          s.groupby("event").mse_over_threshold.mean().round(3).to_dict())
    print("level=알림 인데 mse <= threshold:",
          int(((s.level == "알림") & ~s.mse_over_threshold).sum()), "/", int((s.level == "알림").sum()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
