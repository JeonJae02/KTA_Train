"""여러 horizon(15/30/45/60/90초) 앞의 온도를 예측하는 모델.

**목표가 ARX_Temp_Model.py 와 다르다.** ARX_Temp_Model.py 는 "이 정도 명령이
나갔으면 온도가 이만큼 변해야 정상"을 검증하는 **고장 감지용** 모델이라 미래
액추에이터 상태(t~t+60s)를 이미 안다고 전제한다. 여기서는 **진짜 온도 예측**이
목적이라 두 입력 설계를 나란히 만들어 비교한다.

    PAST_ONLY   미래는 전혀 모른다고 가정 — 현재 T·level·직전 heat/exch/feed
                가동률만으로 t+h 를 맞힌다. 실전 모니터링에 그대로 쓸 수 있는 쪽.
    WITH_FUTURE t~t+h 구간 액추에이터 상태(heat/exch/feed)까지 안다고 가정 —
                ARX 와 동일 전제. 실전 예측기로는 못 쓰지만(미래 명령을 실제로는
                모르므로), "액추에이터 계획을 안다면 얼마나 더 좋아지는가"의
                상한선 역할을 한다.

**예측 대상은 온도 수준이 아니라 변화량 ΔT(t→t+h) 다.** 수준을 그대로 맞히면
"현재값 유지"만으로도 R² 가 0.9 를 넘어(짧은 horizon 일수록 심함) 모델이
액추에이터를 이해했는지 알 수 없다. 그래서 모든 지표는 **"ΔT=0(변화 없음)"
이라는 순박한 예측과 비교한 개선율**로 낸다 — 이게 이 모델이 "설득력이
있는가"에 대한 정직한 답이다. (수준 예측값은 어차피 `T(t) + ΔT예측`으로
그대로 복원되므로 잃는 정보가 없다.)

**heat/cool 은 완전 상보라 cool 은 넣지 않는다** (ARX_Temp_Model.py 와 동일한
이유 — 둘 다 ON/OFF 인 순간이 0건이라 다중공선성).

**세 가지 단위로 모델을 만든다** (탱크마다 물질이 달라 하나로 뭉치면 안 될 수도
있다는 가설을 확인하기 위해):
    탱크별   P1~P5, I1~I3 각각 따로 (8개)
    타입별   P(폴리올 계열 5개 탱크) / I(이소시아네이트 계열 3개 탱크) 각각 통합
    전체     8개 탱크 전부 통합
통합 모델에는 "어느 탱크인지" 원-핫 더미를 넣어 탱크별 기준선 차이를 흡수한다.
ΔT 자체는 절대온도 수준과 무관해서(수준이 달라도 변화량은 비교 가능) 통합이
가능하다 — 통합 시 탱크 간 절대온도를 맞추는 정규화는 필요 없다.

**두 알고리즘을 비교한다.**
    선형(Ridge)                 해석 가능, ARX_Temp_Model.py 와 같은 계열
    HistGradientBoostingRegressor  비선형/상호작용 포착. 선형보다 좋아지는
                                   정도가 "액추에이터-온도 관계가 얼마나
                                   비선형적인가"의 대리 지표가 된다.

사용법:
    python Temp_Prediction_Model.py
"""

import glob
import os
import sys

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.linear_model import Ridge

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from Tank_Temp_Factor_Analysis import TANKS, WAGON_TAG

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SAVE_DIR = os.path.join(BASE_DIR, "extracted_csv")
OUT_DIR = os.path.join(BASE_DIR, "analysis_out", "temp_prediction")

GRID_SEC = 5
HORIZONS_SEC = [15, 30, 45, 60, 90]
PAST_WINDOWS_SEC = [30, 60, 120]
PAST_FACTORS = ("heat", "exch", "feed")     # cool 제외 (heat 과 완전 상보)
FUTURE_FACTORS = ("heat", "exch", "feed")

EXCLUDE_HOURS = (0, 7)
RUNNING_WINDOW_MIN = 30
TEST_FRACTION = 0.3

TANK_TYPE = {tk: ("P" if tk.startswith("P") else "I") for tk in TANKS}
GROUPS = {
    "P타입(폴리올, 5탱크통합)": [tk for tk in TANKS if TANK_TYPE[tk] == "P"],
    "I타입(이소시아네이트, 3탱크통합)": [tk for tk in TANKS if TANK_TYPE[tk] == "I"],
    "전체(8탱크통합)": list(TANKS),
}


# --------------------------------------------------------------------------- #
# 데이터 / 특징
# --------------------------------------------------------------------------- #

def _latest_csv(tank_key):
    files = sorted(glob.glob(os.path.join(SAVE_DIR, f"{tank_key}_Temp_vs_Factors_*.csv")))
    return files[-1] if files else None


def build_tank_frame(tank_key):
    """탱크 CSV -> 5초 격자 + 과거/미래 특징 + 5개 horizon 타깃(ΔT).

    반환되는 표는 이미 '정지/알람/제외시간대'가 걸러진 상태다. 유효 조건은
    [t - past_needed, t + fut_needed] 구간 **전체**가 정상이어야 한다는 것 —
    구간 일부만 걸쳐도 롤링 특징이나 타깃이 오염되기 때문이다(ARX_Temp_Model.py
    와 동일한 원칙).
    """
    cfg = TANKS[tank_key]
    path = _latest_csv(tank_key)
    if path is None:
        return None

    raw = pd.read_csv(path, index_col="Time", parse_dates=True).sort_index()
    num = pd.DataFrame({
        "T": raw[cfg["temp"]], "level": raw["level_f"],
        "heat": raw[cfg["heat"]], "cool": raw[cfg["cool"]],
        "exch": raw[cfg["exchanger"]], "feed": raw[cfg["feeding"]],
        "wagon": raw[WAGON_TAG], "diag": (raw["source"] == "DIAGNOSTIC").astype(float),
    })
    g = num.resample(f"{GRID_SEC}s").ffill().dropna()

    moved = (g["wagon"].diff().abs() > 0).astype(float)
    running = moved.rolling(int(RUNNING_WINDOW_MIN * 60 / GRID_SEC), min_periods=1).max()
    hour = g.index.hour
    ok = (((hour < EXCLUDE_HOURS[0]) | (hour >= EXCLUDE_HOURS[1])) & (g["diag"] == 0)).astype(float) * running

    out = pd.DataFrame(index=g.index)
    out["T"] = g["T"]
    out["level"] = g["level"]
    out["dlevel_past60"] = g["level"].diff(60 // GRID_SEC)   # 최근 유입 활동의 대리 지표

    for name in PAST_FACTORS:
        for w in PAST_WINDOWS_SEC:
            steps = w // GRID_SEC
            # [t-w, t) — 현재 순간은 포함하지 않는다 (ARX_Temp_Model.py 와 동일 관례)
            out[f"{name}_past{w}"] = g[name].rolling(steps).mean().shift(1)

    for h in HORIZONS_SEC:
        steps = h // GRID_SEC
        out[f"y_{h}"] = g["T"].shift(-steps) - g["T"]
        for name in FUTURE_FACTORS:
            # [t, t+h) 미래 가동률 — WITH_FUTURE 변형에서만 쓴다
            out[f"{name}_fut{h}"] = g[name].rolling(steps).mean().shift(-(steps - 1))

    past_needed = max(PAST_WINDOWS_SEC) // GRID_SEC
    fut_needed = max(HORIZONS_SEC) // GRID_SEC
    back_ok = ok.rolling(past_needed + 1, min_periods=past_needed + 1).min()
    fwd_ok = ok[::-1].rolling(fut_needed + 1, min_periods=fut_needed + 1).min()[::-1]
    out["ok"] = back_ok * fwd_ok

    out = out.dropna()
    out = out[out["ok"] == 1].drop(columns="ok")
    out.insert(0, "tank", tank_key)
    out.insert(1, "type", TANK_TYPE[tank_key])
    return out


def past_only_features():
    feats = ["T", "level", "dlevel_past60"]
    feats += [f"{n}_past{w}" for n in PAST_FACTORS for w in PAST_WINDOWS_SEC]
    return feats


def with_future_features(h):
    return past_only_features() + [f"{n}_fut{h}" for n in FUTURE_FACTORS]


# --------------------------------------------------------------------------- #
# 학습 / 평가
# --------------------------------------------------------------------------- #

def _time_split(df, test_frac=TEST_FRACTION):
    """탱크별로(하나씩) 시간순 분할. 경계에는 최대 horizon 만큼 purge 를 둔다."""
    purge = max(HORIZONS_SEC) // GRID_SEC
    parts_tr, parts_te = [], []
    for tank_key, d in df.groupby("tank", sort=False):
        n = len(d)
        split = int(n * (1 - test_frac))
        if split - purge < 10 or n - split < 10:
            continue
        parts_tr.append(d.iloc[: split - purge])
        parts_te.append(d.iloc[split:])
    if not parts_tr:
        return None, None
    return pd.concat(parts_tr), pd.concat(parts_te)


def _design_matrix(df, features, pooled):
    X = df[features].copy()
    if pooled:
        X = pd.concat([X, pd.get_dummies(df["tank"], prefix="tank", dtype=float)], axis=1)
    return X.values, list(X.columns)


def fit_eval(train, test, features, target_col, algo, pooled):
    Xtr, cols = _design_matrix(train, features, pooled)
    Xte, _ = _design_matrix(test, features, pooled)
    ytr, yte = train[target_col].values, test[target_col].values

    if algo == "선형(Ridge)":
        model = Ridge(alpha=1.0)
    else:
        model = HistGradientBoostingRegressor(max_depth=6, max_iter=150, random_state=0)
    model.fit(Xtr, ytr)
    pred = model.predict(Xte)

    def rmse(a, b):
        return float(np.sqrt(np.mean((a - b) ** 2)))

    def r2(a, b):
        ss_res = np.sum((a - b) ** 2)
        ss_tot = np.sum((a - a.mean()) ** 2)
        return float(1 - ss_res / ss_tot) if ss_tot > 0 else np.nan

    rmse_model = rmse(yte, pred)
    rmse_naive = rmse(yte, 0.0)          # "변화 없음" 예측
    improve = (rmse_naive - rmse_model) / rmse_naive * 100 if rmse_naive > 0 else np.nan

    return {
        "n_train": len(train), "n_test": len(test),
        "RMSE_모델": round(rmse_model, 4), "RMSE_변화없음기준": round(rmse_naive, 4),
        "개선율%": round(improve, 1), "R2": round(r2(yte, pred), 4),
        "R2_변화없음기준": round(r2(yte, np.zeros_like(yte)), 4),
    }


def run_group(name, tank_keys, algo, variant):
    frames = [build_tank_frame(tk) for tk in tank_keys]
    frames = [f for f in frames if f is not None and len(f) > 0]
    if not frames:
        return []
    df = pd.concat(frames)
    pooled = len(tank_keys) > 1
    train, test = _time_split(df)
    if train is None:
        return []

    rows = []
    for h in HORIZONS_SEC:
        features = past_only_features() if variant == "PAST_ONLY" else with_future_features(h)
        target = f"y_{h}"
        res = fit_eval(train, test, features, target, algo, pooled)
        res.update({"그룹": name, "horizon초": h, "알고리즘": algo, "입력설계": variant})
        rows.append(res)
    return rows


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    all_rows = []

    targets = [(tk, [tk]) for tk in TANKS] + list(GROUPS.items())
    algos = ["선형(Ridge)", "비선형(GBR)"]
    variants = ["PAST_ONLY", "WITH_FUTURE"]

    for name, tank_keys in targets:
        for algo in algos:
            for variant in variants:
                all_rows.extend(run_group(name, tank_keys, algo, variant))
        print(f"완료: {name}")

    result = pd.DataFrame(all_rows)
    cols = ["그룹", "horizon초", "입력설계", "알고리즘", "n_train", "n_test",
            "RMSE_변화없음기준", "RMSE_모델", "개선율%", "R2", "R2_변화없음기준"]
    result = result[cols]
    out_path = os.path.join(OUT_DIR, "온도예측_horizon비교.csv")
    result.to_csv(out_path, index=False, encoding="utf-8-sig")

    print("\n=== 온도 예측 모델 비교 (전체) ===")
    print(result.to_string(index=False))
    print(f"\n저장: {out_path}")
    return result


if __name__ == "__main__":
    main()
