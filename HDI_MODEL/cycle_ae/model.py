"""사이클 AE — 펌프 14대 공용 조건부 오토인코더 + 채점기.

    scorer = CycleScorer.load()                           # artifacts/ 읽기
    out = scorer.score(cycles_df)                         # 배치: score, alarm, top_feature, err_* 컬럼
    out = scorer.score_one(cycle_dict, pump="P1")         # 실시간: PumpProcessor 의 "cycle" + 기준선 피처
    win = WindowTracker(scorer.meta)                      # 실시간 창 판정 (펌프·묶음별 최근 10사이클)
    w = win.update(pump, out["group"], out["score"], out["zmax"], t=out["t_start"],
                   reason=f'{out["top_feature"]} {out["top_dir"]}', level=out["level"])   # → pump_level, notify, reason

## 입력

- 복원 대상(FEATURES): 기준선 잔차와 곡선 모양. **묶음(group)별로 학습 구간 중앙값·MAD 로 표준화**해서 펌프 간 크기 차이를 없앤다
  묶음 = 펌프 × 안정 출처(clean/shot) × 단일/다단 Hz. 출처·다단 여부에 따라 압력 수준과 곡선 모양이 다르기 때문.
  학습 사이클이 MIN_FIT 미만인 묶음은 모델 범위 밖(`in_scope=False`, 점수 NaN)
- 조건(CONDS): 복원하지 않고 인코더·디코더에 같이 넣는다 — 레시피(Hz)·온도(점도)·원료형·안정 출처가 다를 때
  모양이 달라지는 것을 이상으로 보지 않게
- SV 기준 피처(FT_SV, t_cross 등)는 넣지 않는다 — Auto-Cal 마다 바뀐다 (new_pump_preprocessor.md 5절)

## 점수

- `score` (패턴) = 관측된 피처의 (복원 오차)² 평균. 결측 피처(복귀 반감 시각 등)는 입력 0(=중앙값), 오차 계산에서 뺀다
- `zmax` (수준) = max_f |z_f| / zq_f — 피처마다 |z| 를 그 묶음 학습 구간의 99.5% 값(zq, 최소 3)으로 나눠 최댓값.
  그냥 최대 |z| 를 쓰면 양자화로 MAD 가 아주 작은 피처(예: P7 다단 레시피 pump_resid, 척도 0.08%)가 평소에도
  |z| 20 을 넘어 임계값이 포화되고, 꼬리가 무거운 피처 하나가 판정을 독차지했다
- 패턴·수준 둘로 나누는 이유는 train.py 머리말

## 판정 — 두 단계 (2026-10-08)

| 등급 | 임계값 (묶음별, 정상 점수 분위) | 쓰임 |
|---|---|---|
| **주의** (`warn`) | 0.995 — 정상의 상위 0.5% | 대시보드·기록. 작은 드리프트는 ③ 추세에서 다시 본다 |
| **알림** (`alert`) | 0.999 — 정상의 상위 0.1% | 사람에게 알림 |

- 사이클 등급 `level` (정상/주의/알림): 그 사이클의 score 또는 zmax 가 등급 임계값을 넘음. `alarm` = 주의 이상, `alert` = 알림
- 창 등급 (`WindowTracker`): 같은 묶음 최근 10사이클의 score·zmax 중앙값을 창 임계값과 비교. 펌프 등급 = 그 펌프 묶음들 중 가장 높은 창 등급
- **사람에게 가는 알림 = 펌프 등급이 "알림" 으로 새로 올라간 순간 한 번** (`notify`). 이어지는 동안은 다시 울리지 않는다
- 알림을 보낸 펌프는 `COOLDOWN_MIN`(60분) 동안 다시 알리지 않는다 — 경계에서 오르내리며 한 시간에 네 번 울리던 것(P7 9/16 밤)
- **펌프마다 운영일(06시 경계)에 한 번만** 알린다 — 재가동 리셋은 창만 비우고 이 기억은 남긴다.
  (안 남기면 이미 아는 이상이 쉬는 시간 뒤 재가동마다 다시 울렸다: P2 하루 4~7회, 10/8~10.
  이유별로 한 번씩으로 하면, 지속 상태에선 이유가 "시작 전 압력 높음 / 압력/기대 높음 / 압력 해제 반감 높음" 으로
  바뀌며 다시 울렸다.) 그날 다른 문제가 생겨도 화면의 펌프 등급은 계속 보이므로, 알림(푸시)은 하루 한 번으로 둔다
- 알림 이유 `reason` = 그 펌프 최근 10사이클 중 주의 이상이었던 사이클들의 이유 최빈값 (사이클 하나의 이유는 흔들린다).
  그런 사이클이 없으면(창 중앙값만 임계를 넘은 경우) 창 전체 사이클의 이유 최빈값 — 예전엔 이 경우 이유가 비었다
- 펌프가 `RESET_GAP_MIN`(30분) 넘게 사이클이 없으면(정지·야간) 그 펌프의 창 상태를 비운다 — 아침 재가동 때의 이상을 새 알림으로
  (안 비우면 9/16 밤의 알림 상태가 이어져 9/17 아침 유량계 고장이 새 알림을 못 냈다)
- 정상 날 펌프당 하루 알림 약 0.6회(14대 약 8회), 주의는 약 2.7회 (cycle_ae_test.ipynb §8)
- 임계값 구조: `meta["thresholds"][묶음] = {"warn": {cycle, zmax, roll, zmax_roll}, "alert": {...}}`
"""

import json
import os
from collections import deque

import numpy as np
import pandas as pd
import torch
import torch.nn as nn

HERE = os.path.dirname(os.path.abspath(__file__))
ARTIFACT_DIR = os.path.join(HERE, "artifacts")

FEATURES = ["pump_resid", "r_phys", "t_self", "decel_half_sec", "release_half_sec", "ft_cv", "pt_std", "P0"]
FEATURE_KO = {"pump_resid": "펌프 잔차", "r_phys": "압력/기대", "t_self": "응답 시간", "decel_half_sec": "감속 반감",
              "release_half_sec": "압력 해제 반감", "ft_cv": "유량 흔들림", "pt_std": "압력 흔들림", "P0": "시작 전 압력"}
CONDS = ["mu_rel_z", "hz_rel", "dhz_rel", "is_iso", "is_shot", "is_multi"]
CLIP = 20.0
MIN_FIT = 300            # 묶음별 최소 학습 사이클
TIERS = ("warn", "alert")
LEVELS = ("정상", "주의", "알림")               # 0, 1, 2
RESET_GAP_MIN = 30
COOLDOWN_MIN = 60
DAY_START_HOUR = 6       # 운영일 경계 (탱크 모델과 같은 06시) — 펌프마다 알림은 운영일에 한 번
ZQ = 0.995               # 수준 판정: 피처별 정상 |z| 한계 (학습 구간 분위)


def group_key(pump, steady_src, n_hz_levels):
    return f"{pump}|{steady_src}|{'multi' if n_hz_levels > 1 else 'single'}"


def add_group(df):
    if "group" not in df:
        df = df.copy()
        df["group"] = [group_key(p, s, n) for p, s, n in zip(df.pump, df.steady_src, df.n_hz_levels.fillna(1))]
    return df


class CycleAE(nn.Module):
    def __init__(self, n_feat=len(FEATURES), n_cond=len(CONDS), latent=4, hidden=(32, 16)):
        super().__init__()
        h1, h2 = hidden
        self.enc = nn.Sequential(nn.Linear(n_feat + n_cond, h1), nn.ReLU(), nn.Linear(h1, h2), nn.ReLU(),
                                 nn.Linear(h2, latent))
        self.dec = nn.Sequential(nn.Linear(latent + n_cond, h2), nn.ReLU(), nn.Linear(h2, h1), nn.ReLU(),
                                 nn.Linear(h1, n_feat))

    def forward(self, x, c):
        z = self.enc(torch.cat([x, c], dim=1))
        return self.dec(torch.cat([z, c], dim=1))


# --------------------------------------------------------------------------- #
# 표준화
# --------------------------------------------------------------------------- #
def robust_scale(s):
    s = pd.Series(s).dropna()
    mad = 1.4826 * np.median(np.abs(s - s.median()))
    return float(max(mad, 0.25 * s.std(), 1e-9))     # 양자화로 MAD 가 0 인 피처(ft_cv 등) 보호


def fit_stats(df):
    """df: 학습(fit) 사이클. 묶음별 피처 중앙값·척도, 점도 중앙값·척도, 대표 Hz. 사이클 MIN_FIT 미만 묶음은 뺀다."""
    stats = {}
    for k, g in df.groupby("group"):
        if len(g) < MIN_FIT:
            continue
        stats[k] = {"n_fit": int(len(g)), "med": {f: float(g[f].median()) for f in FEATURES},
                    "scale": {f: robust_scale(g[f]) for f in FEATURES},
                    "mu_med": float(g.mu_rel.median()), "mu_scale": robust_scale(g.mu_rel),
                    "hz_med": float(g.Hz.median()), "material": g.material.iloc[0], "steady_src": g.steady_src.iloc[0],
                    "multi": bool((g.n_hz_levels > 1).iloc[0])}
        z = np.abs((g[FEATURES] - pd.Series(stats[k]["med"])) / pd.Series(stats[k]["scale"]))
        stats[k]["zq"] = {f: float(max(z[f].quantile(ZQ), 3.0)) for f in FEATURES}
    return stats


def prepare(df, stats):
    """→ X (표준화 피처, 결측 0), M (관측 마스크), C (조건). stats 에 없는 묶음의 행은 M 전부 0 (점수 NaN)."""
    n = len(df)
    X, M, C = np.zeros((n, len(FEATURES)), np.float32), np.zeros((n, len(FEATURES)), np.float32), \
        np.zeros((n, len(CONDS)), np.float32)
    keys = df.group.to_numpy()
    for k in np.unique(keys):
        if k not in stats:
            continue
        i = np.where(keys == k)[0]
        s, g = stats[k], df.iloc[i]
        for j, f in enumerate(FEATURES):
            v = ((g[f] - s["med"][f]) / s["scale"][f]).to_numpy(float)
            ok = np.isfinite(v)
            X[i, j] = np.clip(np.where(ok, v, 0.0), -CLIP, CLIP)
            M[i, j] = ok
        C[i, 0] = np.clip(np.nan_to_num((g.mu_rel.to_numpy(float) - s["mu_med"]) / s["mu_scale"]), -CLIP, CLIP)
        C[i, 1] = g.Hz.to_numpy(float) / s["hz_med"] - 1.0
        C[i, 2] = np.clip(np.nan_to_num(g.dHz_prev.to_numpy(float) / g.Hz.to_numpy(float)), -1, 1)
        C[i, 3] = float(s["material"] == "ISO")
        C[i, 4] = float(s["steady_src"] == "shot")
        C[i, 5] = float(s["multi"])
    return X, M, C


def zlevel(df, stats):
    """피처별 |z| / zq (자르지 않음). 결측·범위 밖은 NaN."""
    Z = np.full((len(df), len(FEATURES)), np.nan)
    keys = df.group.to_numpy()
    for k in np.unique(keys):
        if k not in stats:
            continue
        i = np.where(keys == k)[0]
        s, g = stats[k], df.iloc[i]
        for j, f in enumerate(FEATURES):
            Z[i, j] = np.abs((g[f].to_numpy(float) - s["med"][f]) / s["scale"][f]) / s["zq"][f]
    return Z


def zmax_of(Z):
    obs = ~np.isnan(Z)
    return np.where(obs.any(axis=1), np.max(np.where(obs, Z, -np.inf), axis=1), np.nan)


def errors(model, X, M, C, batch=65536):
    """피처별 (복원 오차)² — 결측은 NaN."""
    model.eval()
    out = []
    with torch.no_grad():
        for k in range(0, len(X), batch):
            x, c = torch.from_numpy(X[k:k + batch]), torch.from_numpy(C[k:k + batch])
            out.append(((model(x, c) - x) ** 2).numpy())
    e = np.concatenate(out) if out else np.zeros((0, len(FEATURES)), np.float32)
    return np.where(M > 0, e, np.nan)


def score_from_errors(E):
    """관측된 피처의 오차 평균. 피처가 하나도 없는 행(범위 밖)은 NaN."""
    obs = ~np.isnan(E)
    n = obs.sum(axis=1)
    return np.where(n > 0, np.where(obs, E, 0).sum(axis=1) / np.maximum(n, 1), np.nan)


def explain(X, Z, E, by_ae):
    """경보 이유 피처와 방향. AE 로 울린 사이클은 복원 오차가 가장 큰 피처, 수준으로 울린 사이클은 |z|/zq 가 가장 큰 피처."""
    ae_top = np.nanargmax(np.where(np.isnan(E), -1, E), axis=1)
    z_top = np.argmax(np.where(np.isnan(Z), -1, Z), axis=1)
    idx = np.where(np.asarray(by_ae, bool), ae_top, z_top)
    sign = np.sign(X[np.arange(len(X)), idx])
    return np.array(FEATURES)[idx], np.where(sign > 0, "높음", "낮음")


# --------------------------------------------------------------------------- #
# 등급
# --------------------------------------------------------------------------- #
def add_levels(df, thr):
    """사이클 등급. alarm_ae / alarm_z / alarm = 주의 이상, alert = 알림, level = 정상/주의/알림."""
    tm = lambda t, c: df.group.map({k: v[t][c] for k, v in thr.items()})  # noqa: E731
    df["alarm_ae"] = df.score > tm("warn", "cycle")
    df["alarm_z"] = df.zmax > tm("warn", "zmax")
    df["alarm"] = df.alarm_ae | df.alarm_z
    df["alert"] = (df.score > tm("alert", "cycle")) | (df.zmax > tm("alert", "zmax"))
    df["level"] = np.where(df.alert, LEVELS[2], np.where(df.alarm, LEVELS[1], LEVELS[0]))
    return df


class WindowTracker:
    """짧은 창 판정 — 실시간·배치 공용. 묶음별 최근 `roll` 사이클의 점수 중앙값으로 창 등급,
    펌프 등급 = 그 펌프 묶음들의 현재 창 등급 중 최고. 펌프 등급이 알림으로 새로 올라가면 notify=True (한 번만)."""

    def __init__(self, meta, reset_gap_min=RESET_GAP_MIN, cooldown_min=COOLDOWN_MIN):
        self.thr, self.roll = meta["thresholds"], meta["roll"]
        self.gap = pd.Timedelta(minutes=reset_gap_min)
        self.cool = pd.Timedelta(minutes=cooldown_min)
        self.hist, self.glevel, self.plevel, self.last_t, self.last_notify, self.reasons = {}, {}, {}, {}, {}, {}
        self.notified = {}       # pump -> 이미 알린 운영일

    def _reset(self, pump):
        """재가동 — 창과 등급만 비운다. 알림 기억(60분 대기, 운영일 이유)은 남긴다."""
        for g in [g for g in self.hist if g.startswith(pump + "|")]:
            del self.hist[g]
            self.glevel.pop(g, None)
        self.plevel[pump] = 0
        self.reasons.pop(pump, None)

    @staticmethod
    def op_day(t):
        return (t - pd.Timedelta(hours=DAY_START_HOUR)).date()

    def update(self, pump, group, score, zmax, t=None, reason=None, level=None):
        """reason·level: 이 사이클의 경보 이유와 사이클 등급 (score() 출력의 top_feature/top_dir, level) — 알림 이유 집계용."""
        if t is not None:
            t = pd.Timestamp(t)
            if pump in self.last_t and t - self.last_t[pump] > self.gap:
                self._reset(pump)
            self.last_t[pump] = t
        if group not in self.thr or not np.isfinite(score):
            return {"window_level": None, "pump_level": LEVELS[self.plevel.get(pump, 0)], "notify": False,
                    "score_med": np.nan, "zmax_med": np.nan, "reason": None}
        rq = self.reasons.setdefault(pump, deque(maxlen=self.roll))
        rq.append((reason, level in LEVELS[1:]))
        h = self.hist.setdefault(group, (deque(maxlen=self.roll), deque(maxlen=self.roll)))
        h[0].append(score)
        h[1].append(zmax)
        lv, ms, mz = 0, np.nan, np.nan
        if len(h[0]) == self.roll:
            ms, mz = float(np.median(h[0])), float(np.median(h[1]))
            th = self.thr[group]
            for i, tier in ((2, "alert"), (1, "warn")):
                if ms > th[tier]["roll"] or mz > th[tier]["zmax_roll"]:
                    lv = i
                    break
        self.glevel[group] = lv
        prev = self.plevel.get(pump, 0)
        cur = max(v for g, v in self.glevel.items() if g.startswith(pump + "|"))
        self.plevel[pump] = cur
        # 이유: 창 안의 주의 이상 사이클들 이유 최빈값. 그런 사이클이 없으면(창 중앙값만 넘은 경우) 창 전체 사이클의 최빈값
        rs = [r for r, ab in rq if r and ab] or [r for r, _ in rq if r]
        why = max(set(rs), key=rs.count) if rs else None
        notify = cur == 2 and prev < 2
        if notify and t is not None:
            od = self.op_day(t)
            if (pump in self.last_notify and t - self.last_notify[pump] < self.cool) or self.notified.get(pump) == od:
                notify = False
            else:
                self.last_notify[pump] = t
                self.notified[pump] = od
        return {"window_level": LEVELS[lv], "pump_level": LEVELS[cur], "notify": notify,
                "score_med": ms, "zmax_med": mz, "reason": why}


def window_levels(df, meta):
    """배치 — df 를 시간순으로 WindowTracker 에 흘린다 (실시간과 같은 결과). 컬럼 window_level, pump_level, notify 추가."""
    df = df.sort_values("t_start")
    wt = WindowTracker(meta)
    why = (df.top_feature + " " + df.top_dir) if "top_feature" in df else pd.Series(None, index=df.index)
    lv = df.level if "level" in df else pd.Series(None, index=df.index)
    out = [wt.update(p, g, s, z, t, r, l) for p, g, s, z, t, r, l in
           zip(df.pump, df.group, df.score, df.zmax, df.t_start, why, lv)]
    o = pd.DataFrame(out, index=df.index)
    return df.join(o[["window_level", "pump_level", "notify", "score_med", "zmax_med", "reason"]].rename(columns={"reason": "notify_reason"}))


# --------------------------------------------------------------------------- #
# 채점기 (학습 결과 읽어서 쓰기)
# --------------------------------------------------------------------------- #
class CycleScorer:
    def __init__(self, model, meta):
        self.model, self.meta = model, meta
        self.stats, self.thr = meta["stats"], meta["thresholds"]

    @classmethod
    def load(cls, path=ARTIFACT_DIR):
        meta = json.load(open(os.path.join(path, "meta.json"), encoding="utf-8"))
        model = CycleAE(latent=meta["config"]["latent"], hidden=tuple(meta["config"]["hidden"]))
        model.load_state_dict(torch.load(os.path.join(path, "model.pt"), map_location="cpu"))
        model.eval()
        return cls(model, meta)

    def score(self, df):
        """df: pump, steady_src, n_hz_levels, Hz, mu_rel, dHz_prev, material + FEATURES.
        반환: score·zmax·alarm·top_feature·in_scope·err_* 붙인 표. 범위 밖 묶음은 점수 NaN, alarm False."""
        df = add_group(df).copy()
        X, M, C = prepare(df, self.stats)
        E = errors(self.model, X, M, C)
        df["in_scope"] = df.group.isin(self.stats).to_numpy()
        df["score"] = score_from_errors(E)
        Z = zlevel(df, self.stats)
        df["zmax"] = zmax_of(Z)
        df = add_levels(df, self.thr)
        df["top_feature"], df["top_dir"] = explain(X, Z, E, df.alarm_ae.to_numpy())
        for j, f in enumerate(FEATURES):
            df[f"err_{f}"] = E[:, j]
        return df

    def score_one(self, cycle, pump):
        """실시간 한 사이클 — cycle 은 PumpProcessor 출력 + add_baseline_features 결과(dict)."""
        mat = next((v["material"] for k, v in self.stats.items() if k.startswith(pump + "|")), None)
        row = {"pump": pump, "material": mat, **cycle}
        return self.score(pd.DataFrame([row])).iloc[0].to_dict()
