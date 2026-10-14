"""창 크기(ROLL) 비교 — 신경망은 그대로, 창 점수·임계값만 다시 계산해서 같은 잣대로 본다 (meta.json 은 안 바꾼다).

    python HDI_MODEL/cycle_ae/roll_sweep.py                 # ROLL = 5, 7, 10
    python HDI_MODEL/cycle_ae/roll_sweep.py --rolls 3 5 10

창 크기마다:
  - 창 점수(score_roll, zmax_roll)를 그 크기로 다시 굴리고 → calibrate.compute_thresholds (날짜 단위 K=4) → calibrate.evaluate
    (학습 기간 하루 빼고 / 새 날 정상 펌프 알림 / 알려진 이벤트 창 알림 비율·첫 알림)
  - 아침 워밍업: 새 날 아침마다 펌프의 첫 자동운전 사이클부터 어느 묶음이든 창이 처음 차기까지 [분]
바꾸기로 하면 train.py 의 ROLL 을 고치고 train.py → calibrate.py (train.py 는 신경망도 다시 학습하므로
임계값만 바꾸려면 scores.pkl 의 창 점수를 이 스크립트처럼 다시 굴려 calibrate 에 넣는 경로를 쓴다).
"""

import argparse
import os
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import calibrate as cal  # noqa: E402
from dataset import OUT_DIR  # noqa: E402


def reroll(df, roll):
    d = df.sort_values("t_start").copy()
    for src, dst in (("score", "score_roll"), ("zmax", "zmax_roll")):
        d[dst] = d.groupby("group")[src].transform(lambda s: s.rolling(roll, min_periods=roll).median())
    return d


def warmup(new, roll):
    """새 날 아침마다: 펌프 첫 사이클 → 어느 묶음이든 roll 번째 사이클까지 [분]."""
    out = []
    n = new.sort_values("t_start")
    n = n[n.t_start.dt.hour >= 6]
    for (p, day), g in n.groupby(["pump", n.t_start.dt.date]):
        k = g.groupby("group").cumcount() + 1
        full = g.t_start[k >= roll]
        if len(full):
            out.append((full.min() - g.t_start.min()).total_seconds() / 60)
    return np.median(out) if out else np.nan, np.max(out) if out else np.nan


def main(rolls, k):
    df = pd.read_pickle(os.path.join(OUT_DIR, "scores.pkl.xz"), compression="xz")
    new = cal.load_new()
    groups = sorted(df.group.unique())
    rows = {}
    for r in rolls:
        d = reroll(df, r)
        thr = cal.compute_thresholds(d, groups, k=k)
        ev = cal.evaluate(d, thr, r, new, k=k)
        wm, wx = warmup(new, r) if new is not None else (np.nan, np.nan)
        ev["아침 워밍업 분 (중앙/최대)"] = f"{wm:.0f} / {wx:.0f}"
        rows[f"ROLL={r}"] = ev
        print(f"ROLL={r} 끝", flush=True)
    t = pd.DataFrame(rows)
    print(t.to_string())
    t.to_csv(os.path.join(HERE, "artifacts", "eval_roll_sweep.csv"), encoding="utf-8-sig")
    return t


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rolls", type=int, nargs="+", default=[5, 7, 10])
    ap.add_argument("--k", type=float, default=cal.K)
    a = ap.parse_args()
    main(a.rolls, a.k)
