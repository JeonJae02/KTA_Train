"""사이클 AE — 펌프 14대 공용 조건부 오토인코더 + 채점기.

    scorer = CycleScorer.load()                           # artifacts/ 읽기
    out = scorer.score(cycles_df)                         # 배치: score, alarm, top_feature, err_* 컬럼
    out = scorer.score_one(cycle_dict, pump="P1")         # 실시간: PumpProcessor 의 "cycle" + 기준선 피처

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
- 사이클 경보 `alarm` = score 또는 zmax 가 펌프별 임계값(calib 상위 0.5%)을 넘음. 이유는 train.py 머리말
- 임계값도 묶음별. 짧은 창(최근 10사이클 중앙값) 판정은 호출하는 쪽이 이력을 들고 `thresholds[묶음]["roll"/"zmax_roll"]` 과 비교
"""

import json
import os

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
        df["alarm_ae"] = df.score > df.group.map({k: v["cycle"] for k, v in self.thr.items()})
        df["alarm_z"] = df.zmax > df.group.map({k: v["zmax"] for k, v in self.thr.items()})
        df["alarm"] = df.alarm_ae | df.alarm_z
        df["top_feature"], df["top_dir"] = explain(X, Z, E, df.alarm_ae.to_numpy())
        for j, f in enumerate(FEATURES):
            df[f"err_{f}"] = E[:, j]
        return df

    def score_one(self, cycle, pump):
        """실시간 한 사이클 — cycle 은 PumpProcessor 출력 + add_baseline_features 결과(dict)."""
        mat = next((v["material"] for k, v in self.stats.items() if k.startswith(pump + "|")), None)
        row = {"pump": pump, "material": mat, **cycle}
        return self.score(pd.DataFrame([row])).iloc[0].to_dict()
