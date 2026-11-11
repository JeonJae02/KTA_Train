# tools_yeji/pump_models/pump_judge.py
"""펌프 사이클 판정 — 채점(CycleScorer)과 창 판정·알림(WindowTracker).

- CycleScorer.score_one: 사이클 1개 → 패턴 점수(AE 복원 오차)·수준 점수·사이클 등급(정상/주의/알림)·이유·피처별 편차
- WindowTracker.update: 펌프별 최근 10사이클 → 펌프 등급, 사람에게 알림(운영일 1회), 창 편차 중앙값
임계값은 meta.json 의 숫자 — DB_Log/HDI_MODEL/cycle_ae/calibrate.py 가 계산한다.
"""
# 자동 생성 — DB_Log/HDI_MODEL/relay_port/make_port.py. 원본: HDI_MODEL/cycle_ae/model.py
import json
import os
import warnings
from collections import deque

import numpy as np
import pandas as pd
import torch

from models_structure.Pump_Cycle_AE import CONDS, FEATURES, CycleAE  # noqa: F401
from tools_yeji.pump_models.pump_params import ASSET_DIR

FEATURE_KO = {"pump_resid": "펌프 잔차", "r_phys": "압력/기대", "t_self": "응답 시간", "decel_half_sec": "감속 반감",
              "release_half_sec": "압력 해제 반감", "ft_cv": "유량 흔들림", "pt_std": "압력 흔들림", "P0": "시작 전 압력"}
CLIP = 20.0
MIN_FIT = 300            # 묶음별 최소 학습 사이클
TIERS = ("warn", "alert")
LEVELS = ("정상", "주의", "알림")               # 0, 1, 2
RESET_GAP_MIN = 30
COOLDOWN_MIN = 60
DAY_START_HOUR = 6       # 운영일 경계 (탱크 모델과 같은 06시) — 펌프마다 알림은 운영일에 한 번
ZQ = 0.995               # 수준 판정: 피처별 정상 |z| 한계 (학습 구간 분위)


# --------------------------------------------------------------------------- #
# 표준화 · 점수
# --------------------------------------------------------------------------- #
def group_key(pump, steady_src, n_hz_levels):
    return f"{pump}|{steady_src}|{'multi' if n_hz_levels > 1 else 'single'}"


def add_group(df):
    if "group" not in df:
        df = df.copy()
        df["group"] = [group_key(p, s, n) for p, s, n in zip(df.pump, df.steady_src, df.n_hz_levels.fillna(1))]
    return df


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


def zsigned(df, stats):
    """피처별 z / zq — 부호 있는 정규화 편차. ±1 = 그 피처의 정상 한계(학습 구간 99.5%). 결측·범위 밖은 NaN.
    진단 규칙(diagnose.py)이 "어느 피처가 어느 쪽으로 얼마나" 를 보는 입력."""
    Z = np.full((len(df), len(FEATURES)), np.nan)
    keys = df.group.to_numpy()
    for k in np.unique(keys):
        if k not in stats:
            continue
        i = np.where(keys == k)[0]
        s, g = stats[k], df.iloc[i]
        for j, f in enumerate(FEATURES):
            Z[i, j] = (g[f].to_numpy(float) - s["med"][f]) / s["scale"][f] / s["zq"][f]
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
# 등급 · 창 판정
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
        self.zwin = {}           # pump -> 최근 roll 사이클의 피처별 zn (묶음 섞임 — 정규화돼 있어 같은 척도)

    def _reset(self, pump):
        """재가동 — 창과 등급만 비운다. 알림 기억(60분 대기, 운영일 이유)은 남긴다."""
        for g in [g for g in self.hist if g.startswith(pump + "|")]:
            del self.hist[g]
            self.glevel.pop(g, None)
        self.plevel[pump] = 0
        self.reasons.pop(pump, None)
        self.zwin.pop(pump, None)

    @staticmethod
    def op_day(t):
        return (t - pd.Timedelta(hours=DAY_START_HOUR)).date()

    def update(self, pump, group, score, zmax, t=None, reason=None, level=None, zn=None):
        """reason·level: 이 사이클의 경보 이유와 사이클 등급 (score() 출력의 top_feature/top_dir, level) — 알림 이유 집계용.
        zn: 이 사이클의 피처별 부호 있는 정규화 편차 {피처: z/zq} (score() 의 zn_*) — 창 중앙값을 진단(diagnose.py)에 넘긴다."""
        if t is not None:
            t = pd.Timestamp(t)
            if pump in self.last_t and t - self.last_t[pump] > self.gap:
                self._reset(pump)
            self.last_t[pump] = t
        if group not in self.thr or not np.isfinite(score):
            return {"window_level": None, "pump_level": LEVELS[self.plevel.get(pump, 0)], "notify": False,
                    "score_med": np.nan, "zmax_med": np.nan, "reason": None, "zn_med": None}
        rq = self.reasons.setdefault(pump, deque(maxlen=self.roll))
        rq.append((reason, level in LEVELS[1:]))
        if zn is not None:
            self.zwin.setdefault(pump, deque(maxlen=self.roll)).append([zn.get(f, np.nan) for f in FEATURES])
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
        zw = self.zwin.get(pump)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)          # 피처가 창 내내 결측이면 NaN
            zmed = dict(zip(FEATURES, np.nanmedian(np.array(zw, float), axis=0))) if zw else None
        return {"window_level": LEVELS[lv], "pump_level": LEVELS[cur], "notify": notify,
                "score_med": ms, "zmax_med": mz, "reason": why, "zn_med": zmed}


# --------------------------------------------------------------------------- #
# 채점기
# --------------------------------------------------------------------------- #
class CycleScorer:
    def __init__(self, model, meta):
        self.model, self.meta = model, meta
        self.stats, self.thr = meta["stats"], meta["thresholds"]

    @classmethod
    def load(cls, path=ASSET_DIR):
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
        Zs = zsigned(df, self.stats)
        Z = np.abs(Zs)
        df["zmax"] = zmax_of(Z)
        df = add_levels(df, self.thr)
        df["top_feature"], df["top_dir"] = explain(X, Z, E, df.alarm_ae.to_numpy())
        for j, f in enumerate(FEATURES):
            df[f"err_{f}"] = E[:, j]
            df[f"zn_{f}"] = Zs[:, j]
        return df

    def score_one(self, cycle, pump):
        """실시간 한 사이클 — cycle 은 PumpProcessor 출력 + add_baseline_features 결과(dict)."""
        mat = next((v["material"] for k, v in self.stats.items() if k.startswith(pump + "|")), None)
        row = {"pump": pump, "material": mat, **cycle}
        return self.score(pd.DataFrame([row])).iloc[0].to_dict()
