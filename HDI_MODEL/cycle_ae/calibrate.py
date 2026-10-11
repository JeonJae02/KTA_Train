"""임계값만 다시 잡기 — 신경망(model.pt)은 그대로, meta.json 의 thresholds 만 바꾼다.

    python HDI_MODEL/cycle_ae/calibrate.py            # 계산 + 평가 출력 + meta.json 저장
    python HDI_MODEL/cycle_ae/calibrate.py --dry-run  # 평가만
    python HDI_MODEL/cycle_ae/calibrate.py --k 5      # 더 느슨하게

입력: cache/pump_dataset/scores.pkl.xz (train.py 가 저장한 전 사이클 점수)
      cache/pump_dataset/new_*.pkl.xz  (있으면 — 학습 뒤 새로 채점한 날, 평가에만 씀)

## 방법 — 날짜 단위 (2026-10-11)

묶음(펌프 × 안정 출처 × 단일/다단)마다, 정상 날(학습 fit + calib 의 건강일) **하루하루의** 분위값을 구하고

    임계값 = 날마다 분위값의 중앙값 + K × 1.4826 × MAD(날마다 분위값)        K = 4

을 쓴다 (사이클 점수·사이클 수준·창 점수·창 수준 넷 다, 주의 0.995 / 알림 0.999 분위). 정상 날이 `MIN_DAYS` 보다 적은
묶음은 정상 날 전체를 합친 분위값으로 대신한다.

**왜** — 예전엔 calib 건강일(묶음당 1~3일)의 분위값 하나였다. 학습 기간 안의 날은 잘 맞았지만(하루 빼고 검증 0.35회/펌프·일),
학습 뒤 새 날(10/8~10)엔 정상 펌프도 1.2회/펌프·일로 늘었다. 날마다 흔들리는 폭을 임계값에 넣으면 이게 줄면서
알려진 이벤트는 그대로 잡힌다:

| | 예전 (calib 분위) | **날짜 단위 K=4** |
|---|---|---|
| 학습 기간 하루 빼고 — 알림/펌프·일 | 0.35 | **0.20** |
| 새 날 10/8~10 정상 펌프 10대 — 알림/펌프·일 | 1.17 | **0.50** |
| P7 유량계 전조 / 고장 첫 알림 | 22:25 / 06:56 | **22:27 / 06:56** |
| I1 노즐 저항 감소 / 증가 창 알림 | 98% / 92% | **98% / 100%** |
| P2 배관 저항 +7% 창 알림 | 98% | **83%** |
| I7 드리프트 재발 창 알림 | 60% | **75%** |

K 를 바꾸면 (시험 결과): K=3 은 새 날 0.73 · P2 93%, K=5 는 새 날 0.50 · P2 67%.
날마다의 최댓값을 쓰는 방식은 새 날 0.33 까지 내려가지만 P2 43%, I7 재발 34% 로 작은 변화를 놓쳐서 뺐다.
새 날 남은 알림은 펌프 여럿에 흩어진 하루 1칸 안팎의 작은 치우침이다 — 학습 뒤 실제로 조금 움직인 것.
"""

import argparse
import glob
import json
import os
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import model as cae  # noqa: E402
from dataset import EVAL_EVENTS, OUT_DIR  # noqa: E402

Q = {"warn": 0.995, "alert": 0.999}
K = 4.0
MIN_DAYS = 4            # 날짜 단위로 잡을 최소 정상 날 수
MIN_DAY_CYCLES = 30     # 하루 분위값을 믿을 최소 사이클 수
COLS = {"cycle": "score", "zmax": "zmax", "roll": "score_roll", "zmax_roll": "zmax_roll"}
KNOWN_PERSISTENT = ["P2", "I2", "I6", "I7"]   # 새 날 평가에서 따로 세는 펌프 (이미 아는 지속 상태)


def normal_rows(df):
    return df[df.role.isin(["fit", "calib"]) & df.in_scope & ~df.excluded]


def compute_thresholds(df, groups, k=K, q=Q, drop_day=None):
    """df: train.py 의 전 사이클 점수 표 (role, in_scope, excluded, group, day, score, zmax, score_roll, zmax_roll).
    groups: 임계값을 만들 묶음 목록 (meta['stats'] 의 키). drop_day: 검증용으로 뺄 날."""
    nr = normal_rows(df)
    if drop_day is not None:
        nr = nr[nr.day != drop_day]
    thr = {}
    for g in groups:
        rows = nr[nr.group == g]
        days = rows.groupby("day").size()
        days = days[days >= MIN_DAY_CYCLES].index
        use_days = len(days) >= MIN_DAYS
        t_all = {}
        for tier, qq in q.items():
            vals = {}
            daily = rows[rows.day.isin(days)].groupby("day")[list(COLS.values())].quantile(qq) if use_days else None
            for key, col in COLS.items():
                v = daily[col].dropna() if use_days else pd.Series(dtype=float)
                mad = 1.4826 * np.median(np.abs(v - v.median())) if len(v) else 0.0
                if use_days and len(v) >= MIN_DAYS and mad > 0:
                    vals[key] = float(v.median() + k * mad)
                else:
                    vals[key] = float(rows[col].quantile(qq)) if len(rows) else np.inf
            t_all[tier] = vals
        for key in COLS:                                   # 알림은 주의보다 낮을 수 없다
            t_all["alert"][key] = max(t_all["alert"][key], t_all["warn"][key])
        t_all.update({"n_days": int(len(days)), "method": f"day_median+{k:g}MAD" if use_days else "pooled_quantile"})
        thr[g] = t_all
    return thr


def relevel(df, thr, roll):
    df = df.drop(columns=[c for c in ["alarm_ae", "alarm_z", "alarm", "alert", "level"] if c in df]).copy()
    df = cae.add_levels(df, thr)
    return cae.window_levels(df, {"thresholds": thr, "roll": roll})


def evaluate(df, thr, roll, new=None, k=K, lodo=True):
    """① 학습 기간 하루 빼고 (lodo — 날짜 단위 방법으로 그날을 빼고 다시 계산) ② 새 날 ③ 알려진 이벤트. 반환 dict."""
    cols = ["pump", "group", "t_start", "score", "zmax", "top_feature", "top_dir", "day", "event", "role"]
    groups = list(thr)
    out = {}
    nr = normal_rows(df)
    n_alert, n_pd = 0, 0
    for d in (sorted(nr.day.unique()) if lodo else []):
        t = compute_thresholds(df, groups, k=k, drop_day=d)
        o = relevel(nr[nr.day == d][cols], t, roll)
        n_alert += o.notify.sum()
        n_pd += o.pump.nunique()
    out["학습기간 하루빼고 알림/펌프·일"] = round(n_alert / n_pd, 2) if n_pd else "-"
    if new is not None and len(new):
        o = relevel(new[cols[:-2]].assign(event="", role="new"), thr, roll)
        norm = o[~o.pump.isin(KNOWN_PERSISTENT)]
        pd_norm = norm.groupby(["pump", "day"]).ngroups
        out["새 날 정상펌프 알림/펌프·일"] = round(norm.notify.sum() / max(pd_norm, 1), 2)
        out["새 날 사이클 알림%"] = round((norm.level == "알림").mean() * 100, 2)
    h = relevel(df[df.in_scope & (~df.excluded | df.event.ne(""))][cols], thr, roll)
    for p, a, b, name in EVAL_EVENTS:
        g = h[(h.pump == p) & (h.event == name)]
        f = g[g.notify].t_start.min()
        out[name] = f"{(g.pump_level == '알림').mean() * 100:.0f}%" + ("" if pd.isna(f) else f" 첫 알림 {f:%m-%d %H:%M}")
    return out


def load_new():
    parts = []
    for f in sorted(glob.glob(os.path.join(OUT_DIR, "new_*.pkl.xz"))):
        n = pd.read_pickle(f, compression="xz")
        parts.append(n[n.in_scope & ~n.flag_any])
    if not parts:
        return None
    n = pd.concat(parts).drop_duplicates(["pump", "t_start"])
    n["day"] = n.t_start.dt.normalize()
    return n


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--k", type=float, default=K)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    meta_path = os.path.join(cae.ARTIFACT_DIR, "meta.json")
    meta = json.load(open(meta_path, encoding="utf-8"))
    df = pd.read_pickle(os.path.join(OUT_DIR, "scores.pkl.xz"), compression="xz")
    new = load_new()
    thr = compute_thresholds(df, list(meta["stats"]), k=a.k)
    old = meta.get("thresholds_prev", meta["thresholds"]) if "threshold_method" in meta else meta["thresholds"]
    print("예전 임계값:", json.dumps(evaluate(df, old, meta["roll"], new, k=a.k, lodo=False), ensure_ascii=False, indent=1))
    ev = evaluate(df, thr, meta["roll"], new, k=a.k)
    print(f"날짜 단위 K={a.k:g}:", json.dumps(ev, ensure_ascii=False, indent=1))
    print("방법별 묶음 수:", pd.Series([v["method"] for v in thr.values()]).value_counts().to_dict())
    if not a.dry_run:
        if meta["thresholds"] != thr:                      # 같은 값으로 다시 돌리면 예전 기록을 덮지 않는다
            meta["thresholds_prev"] = meta["thresholds"]
        meta["thresholds"] = thr
        meta["threshold_method"] = {"created": pd.Timestamp.now().isoformat(timespec="seconds"), "k": a.k, "q": Q,
                                    "min_days": MIN_DAYS, "min_day_cycles": MIN_DAY_CYCLES, "eval": ev,
                                    "by": "calibrate.py"}
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=1)
        pd.Series(ev).to_csv(os.path.join(cae.ARTIFACT_DIR, "eval_thresholds.csv"), header=["값"], encoding="utf-8-sig")
        print("저장:", meta_path)
