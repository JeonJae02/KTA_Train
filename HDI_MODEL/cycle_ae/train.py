"""사이클 AE 학습 · 임계값 · 평가.

    python HDI_MODEL/cycle_ae/dataset.py      # 먼저 (cache/pump_dataset/cycles.pkl.xz)
    python HDI_MODEL/cycle_ae/train.py        # → HDI_MODEL/cycle_ae/artifacts/

## 순서

1. `role == "fit"` 사이클로 묶음(펌프 × 안정 출처 × 단일/다단 Hz)별 표준화 통계 → 공용 AE 학습
   (펌프마다 같은 비중으로 뽑는다)
   조기 종료는 `calib` 손실로
2. 묶음별 임계값 — 두 가지 판정을 같이 쓴다 (2026-10-08 실험)
   - **AE 점수** (패턴): 피처 사이 관계가 평소와 다름. calib 점수 분위 (`THR_Q` — 주의 0.995, 알림 0.999)
   - **수준** (`zmax` = max 피처별 |z|/zq, model.py): 한 피처가 평소 범위를 벗어남. calib 상위 0.5%
   AE 는 평소에도 같이 움직이는 방향(예: 배관 저항↑ → P0·r_phys 함께↑)의 이동은 잘 복원해서 못 본다
   (I1 저항 증가: AE 24% / |z| 88%). 반대로 관계가 어긋난 이상은 AE 만 본다 (I7: AE 72% / |z| 10%).
   최종 사이클 경보 = 둘 중 하나. 10사이클 이동 중앙값(짧은 창) 임계값은 fit+calib 의 상위 0.5% —
   calib 만 쓰면 날짜가 1~3일뿐이라 날마다 다른 정상 변동을 못 담아 학습 구간에서도 9% 가 울렸다.
   calib 사이클이 MIN_CALIB 미만인 묶음은 사이클 임계값도 fit+calib 로 잡는다
3. 평가 — 구간별 경보율, 이벤트 검출률

## 출력

artifacts/model.pt, artifacts/meta.json (피처·통계·임계값·설정·학습 기록), artifacts/eval_pumps.csv, artifacts/eval_events.csv
cache/pump_dataset/scores.pkl.xz (전 사이클 점수 — 분석용)
"""

import json
import os
import sys
import time

import numpy as np
import pandas as pd
import torch

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from dataset import EVAL_EVENTS, OUT_DIR, OUT_PATH  # noqa: E402
from model import (ARTIFACT_DIR, CONDS, FEATURES, MIN_FIT, ZQ, CycleAE, CycleScorer, add_levels, errors,  # noqa: E402
                   explain, fit_stats, prepare, score_from_errors, window_levels, zlevel, zmax_of)

CONFIG = {"latent": 4, "hidden": [32, 16], "lr": 1e-3, "batch": 512, "epochs": 60, "patience": 6,
          "weight_decay": 1e-5, "seed": 0}
THR_Q = {"warn": 0.995, "alert": 0.999}   # 두 단계: 주의 = 정상 상위 0.5%, 알림 = 상위 0.1% (model.py 머리말)
ROLL = 10                # 짧은 창: 같은 묶음 최근 10사이클 점수 중앙값
MIN_CALIB = 100          # 이보다 적으면 사이클 임계값을 fit+calib 로


def train_model(X, M, C, w, Xv, Mv, Cv, cfg):
    torch.manual_seed(cfg["seed"])
    np.random.seed(cfg["seed"])
    model = CycleAE(latent=cfg["latent"], hidden=tuple(cfg["hidden"]))
    opt = torch.optim.Adam(model.parameters(), lr=cfg["lr"], weight_decay=cfg["weight_decay"])
    Xt, Mt, Ct = map(torch.from_numpy, (X, M, C))
    Xvt, Mvt, Cvt = map(torch.from_numpy, (Xv, Mv, Cv))
    sampler = torch.utils.data.WeightedRandomSampler(torch.as_tensor(w, dtype=torch.double), num_samples=len(X),
                                                     replacement=True, generator=torch.Generator().manual_seed(cfg["seed"]))
    idx_loader = torch.utils.data.DataLoader(np.arange(len(X)), batch_size=cfg["batch"], sampler=sampler)

    def masked(pred, x, m):
        return (((pred - x) ** 2) * m).sum() / m.sum().clamp(min=1)

    best, best_state, wait, hist = np.inf, None, 0, []
    for ep in range(cfg["epochs"]):
        model.train()
        tot = 0.0
        for idx in idx_loader:
            opt.zero_grad()
            loss = masked(model(Xt[idx], Ct[idx]), Xt[idx], Mt[idx])
            loss.backward()
            opt.step()
            tot += loss.item() * len(idx)
        model.eval()
        with torch.no_grad():
            vl = masked(model(Xvt, Cvt), Xvt, Mvt).item()
        hist.append({"epoch": ep + 1, "train": tot / len(X), "calib": vl})
        if vl < best - 1e-4:
            best, best_state, wait = vl, {k: v.clone() for k, v in model.state_dict().items()}, 0
        else:
            wait += 1
            if wait >= cfg["patience"]:
                break
    model.load_state_dict(best_state)
    return model, hist


def rolling_med(df, col):
    return df.sort_values("t_start").groupby("group")[col].transform(lambda s: s.rolling(ROLL, min_periods=ROLL).median())


def main():
    t0 = time.time()
    df = pd.read_pickle(OUT_PATH, compression="xz")
    stats = fit_stats(df[df.role == "fit"])
    df["in_scope"] = df.group.isin(stats)
    fit, cal = df[(df.role == "fit") & df.in_scope], df[(df.role == "calib") & df.in_scope]
    X, M, C = prepare(fit, stats)
    Xv, Mv, Cv = prepare(cal, stats)
    w = 1.0 / fit.pump.map(fit.pump.value_counts()).to_numpy()          # 펌프마다 같은 비중
    model, hist = train_model(X, M, C, w, Xv, Mv, Cv, CONFIG)
    print(f"학습 {len(hist)} epoch, calib 손실 {min(h['calib'] for h in hist):.4f} ({time.time() - t0:.0f}s)", flush=True)

    # 전 사이클 점수
    df = df.sort_values(["pump", "t_start"]).reset_index(drop=True)
    Xa, Ma, Ca = prepare(df, stats)
    E = errors(model, Xa, Ma, Ca)
    df["score"] = score_from_errors(E)
    Za = zlevel(df, stats)
    df["zmax"] = zmax_of(Za)
    df["score_roll"] = rolling_med(df, "score")
    df["zmax_roll"] = rolling_med(df, "zmax")

    # 임계값 — 사이클은 calib (적으면 fit+calib), 짧은 창은 fit+calib. 두 단계
    normal = df[df.role.isin(["fit", "calib"]) & df.in_scope]
    thr = {}
    for k, n in normal.groupby("group"):
        g = n[n.role == "calib"]
        src = g if len(g) >= MIN_CALIB else n
        thr[k] = {t: {"cycle": float(src.score.quantile(q)), "zmax": float(src.zmax.quantile(q)),
                      "roll": float(n.score_roll.dropna().quantile(q)), "zmax_roll": float(n.zmax_roll.dropna().quantile(q))}
                  for t, q in THR_Q.items()}
        thr[k].update({"n_calib": int(len(g)), "cycle_thr_from": "calib" if len(g) >= MIN_CALIB else "fit+calib"})
    df = add_levels(df, thr)
    df["top_feature"], df["top_dir"] = explain(Xa, Za, E, df.alarm_ae.to_numpy())
    meta_thr = {"thresholds": thr, "roll": ROLL}
    df = window_levels(df, meta_thr).sort_values(["pump", "t_start"])
    df["win_warn"] = df.window_level.isin(["주의", "알림"])
    df["win_alert"] = df.window_level == "알림"

    # 평가 — 구간별
    def rate(g, col):
        return round(float(g[col].mean() * 100), 2) if len(g) else np.nan
    seg = {"calib": df.role == "calib",
           "상태변화": (df.role == "eval") & (df.split != "test") & ~df.excluded,
           "test": df.split == "test"}
    cal_days = df[df.role == "calib"].groupby("pump").day.nunique()
    rows = []
    for p, g in df.groupby("pump", sort=False):
        r = {"pump": p, "범위 안%": round(float(g.in_scope.mean() * 100), 1)}
        g = g[g.in_scope]
        for name, m in seg.items():
            gg = g[m[g.index]]
            r[f"{name} 사이클주의%"] = rate(gg, "alarm")
            r[f"{name} 사이클알림%"] = rate(gg, "alert")
            r[f"{name} 창알림%"] = rate(gg, "win_alert")
        r["정상날 알림/일"] = round(float(g[g.role == "calib"].notify.sum() / max(cal_days.get(p, 1), 1)), 2)
        r["정상날 주의 창%"] = rate(g[g.role == "calib"], "win_warn")
        rows.append(r)
    ev_p = pd.DataFrame(rows)

    rows = []
    for p, a, b, name in EVAL_EVENTS:
        g = df[(df.pump == p) & (df.event == name) & df.in_scope]
        if not len(g):
            continue
        nt = g[g.notify]
        first = nt.t_start.min()
        rows.append({"pump": p, "event": name, "n": len(g), "사이클 주의%": rate(g, "alarm"), "사이클 알림%": rate(g, "alert"),
                     "창 주의%": rate(g, "win_warn"), "창 알림%": rate(g, "win_alert"),
                     "첫 알림": "" if pd.isna(first) else f"{first:%m-%d %H:%M} ({nt.notify_reason.iloc[0]})", "알림 수": int(len(nt)),
                     "주된 이유": {f"{k[0]} {k[1]}": v for k, v in
                                g[g.alarm].groupby(["top_feature", "top_dir"]).size().nlargest(2).items()}})
    ev_e = pd.DataFrame(rows)

    # 저장
    os.makedirs(ARTIFACT_DIR, exist_ok=True)
    torch.save(model.state_dict(), os.path.join(ARTIFACT_DIR, "model.pt"))
    ds_meta = json.load(open(os.path.join(OUT_DIR, "cycles_meta.json"), encoding="utf-8"))
    meta = {"created": pd.Timestamp.now().isoformat(timespec="seconds"), "features": FEATURES, "conds": CONDS,
            "config": CONFIG, "thr_q": THR_Q, "roll": ROLL, "min_fit": MIN_FIT, "min_calib": MIN_CALIB, "zq": ZQ, "stats": stats, "thresholds": thr, "history": hist,
            "dataset": ds_meta, "n_fit": int(len(fit)), "n_calib": int(len(cal))}
    with open(os.path.join(ARTIFACT_DIR, "meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=1)
    ev_p.to_csv(os.path.join(ARTIFACT_DIR, "eval_pumps.csv"), index=False, encoding="utf-8-sig")
    ev_e.to_csv(os.path.join(ARTIFACT_DIR, "eval_events.csv"), index=False, encoding="utf-8-sig")
    df.to_pickle(os.path.join(OUT_DIR, "scores.pkl.xz"), compression="xz")

    # 저장한 것을 다시 읽어 같은 점수가 나오는지 (실시간 채점기 경로 확인)
    sc = CycleScorer.load()
    chk = sc.score(df[df.in_scope].sample(2000, random_state=0))
    j = df.loc[chk.index]
    assert np.allclose(chk.score.to_numpy(), j.score.to_numpy(), equal_nan=True), "채점기 재현 실패 (점수)"
    assert (chk.alarm.to_numpy() == j.alarm.to_numpy()).all(), "채점기 재현 실패 (경보)"
    assert (chk.level.to_numpy() == j.level.to_numpy()).all(), "채점기 재현 실패 (등급)"
    assert (chk.top_feature.to_numpy() == j.top_feature.to_numpy()).all(), "채점기 재현 실패 (이유)"

    pd.set_option("display.width", 250)
    print(ev_p.to_string(index=False))
    print(ev_e.to_string(index=False))
    print(f"저장: {ARTIFACT_DIR}  ({time.time() - t0:.0f}s)")


if __name__ == "__main__":
    main()
