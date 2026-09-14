"""exchanger 의 "갭+정화" Δ기울기 방법을 Feeding_Model.py 와 똑같은 틀로 검증한다.

**왜 이 검증이 필요한가.** 지난번엔 이 방법을 SNR(평균/표준편차)로만 봤다
(I2 1.454, I3 1.539, P1 0.677). 그런데 SNR 이 좋다는 것과 "이동 기준선 +
CUSUM 으로 실제 고장을 몇 번 만에 잡아내는가"는 다른 질문이다 — SHAP
방법도 처음엔 그럴듯해 보였다가 가짜 고장을 주입해보고서야 진짜 성능이
드러났다(exchanger 적중률이 탱크마다 100%~13%로 요동쳤다). 같은 검증을
여기도 그대로 적용한다.

**방법 자체는 Feeding_Model.py 와 동일한 틀을 쓴다** — 이동 기준선(중앙값/MAD),
z 클리핑, z 중심 편향 보정, 학습/홀드아웃으로 임계값 산정, 그 다음 심각도별
가짜 고장 주입(25/50/100%)으로 몇 번째 이벤트에서 잡히는지 측정.

**Δ기울기 자체의 정의만 다르다:**
    기준 기울기 = [t0 - pre - post, t0 - post] 구간의 온도 기울기  (신호 켜지기
                  훨씬 전 — 직전 exchanger 이벤트의 잔향을 피한다)
    반응 기울기 = [t0, t0 + post] 구간의 온도 기울기
    Δ기울기     = 반응 기울기 - 기준 기울기
그리고 **"정화"** — 직전 exchanger 이벤트 종료 후 (pre+post)초가 안 지났으면
그 이벤트는 버린다(기준 구간이 이전 이벤트 효과에 오염되기 때문).

대상 탱크: I2, I3, P1 (SNR 이 좋게 나왔던 곳들).
"""

import glob
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from Tank_Temp_Factor_Analysis import TANKS, WAGON_TAG

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SAVE_DIR = os.path.join(BASE_DIR, "extracted_csv")

EXCLUDE_HOURS = (0, 7)
RUNNING_WINDOW_MIN = 30
MIN_EVENT_SEC = 5

WINDOW_FRAC = 0.15
MIN_PRE_SEC = 20
MAX_PRE_SEC = 120

BASELINE_WINDOW = 30
Z_CLIP = 4.0
CUSUM_K = 0.5
THRESH_MARGIN = 1.3
TRAIN_FRAC = 0.7

TARGET_TANKS = ["P1", "I2", "I3"]


# --------------------------------------------------------------------------- #
# 데이터 / 이벤트 (Tank_Temp_Factor_Analysis.TANKS 를 그대로 쓴다)
# --------------------------------------------------------------------------- #

def load(tank_key):
    cfg = TANKS[tank_key]
    path = sorted(glob.glob(os.path.join(SAVE_DIR, f"{tank_key}_Temp_vs_Factors_*.csv")))[-1]
    raw = pd.read_csv(path, index_col="Time", parse_dates=True,
                      usecols=["Time", "source", WAGON_TAG, cfg["temp"], cfg["exchanger"], cfg["heat"]]).sort_index()
    d = pd.DataFrame({"T": raw[cfg["temp"]], "exch": raw[cfg["exchanger"]], "heat": raw[cfg["heat"]],
                      "wagon": raw[WAGON_TAG], "diag": (raw["source"] == "DIAGNOSTIC").astype(float)})
    w = d["wagon"].resample("1min").last().ffill()
    run = (w.diff().abs() > 0).rolling(RUNNING_WINDOW_MIN, min_periods=1).max()
    d["running"] = run.reindex(d.index, method="ffill").fillna(0)
    h = d.index.hour
    d["ok"] = (((h < EXCLUDE_HOURS[0]) | (h >= EXCLUDE_HOURS[1])) & (d["diag"] == 0) & (d["running"] == 1))
    return d


def cycle_window(d):
    s = d["heat"][d["ok"]]
    on = s.index[(s == 1) & (s.shift(1) == 0)]
    if len(on) < 5:
        return MAX_PRE_SEC, MAX_PRE_SEC // 2
    period = float(pd.Series(on).diff().dt.total_seconds().median())
    total = period * WINDOW_FRAC
    pre = int(np.clip(total * 2 / 3, 0, MAX_PRE_SEC))
    return max(pre, MIN_PRE_SEC), max(pre, MIN_PRE_SEC) // 2


def _slope(seg):
    if len(seg) < 3:
        return np.nan
    x = (seg.index - seg.index[0]).total_seconds().values
    return np.polyfit(x, seg.values, 1)[0] * 60 if x[-1] > 0 else np.nan


def extract_events(tank_key):
    """갭+정화가 적용된 Δ기울기 이벤트 표.

    **정화(clean) 판정은 5초 미만 순간 튐을 빼고 난 "진짜" 이벤트끼리만 잰다.**
    exchanger 신호에는 5초 미만짜리 chattering 이 수천 번 섞여 있는데(P1 은
    5초 이상 388개 vs 전체 2486개), 이 튐까지 "직전 이벤트"로 세면 거의 모든
    이벤트가 "방금 전에도 뭔가 있었다"로 오염 판정을 받아 정화 필터를 통과하는
    이벤트가 10개 이하로 떨어진다. 튐을 먼저 걸러낸 뒤에 간격을 재야 한다.
    """
    d = load(tank_key)
    pre, post = cycle_window(d)

    s = (d["exch"] == 1) & d["ok"]
    grp = (s & ~s.shift(1, fill_value=False)).cumsum()[s]

    # 1) 5초 이상인 "진짜" 이벤트만 먼저 추린다 (순간 튐은 여기서 아예 제외)
    events = [(idx.iloc[0], idx.iloc[-1]) for _, idx in d.index[s].to_series().groupby(grp)
              if (idx.iloc[-1] - idx.iloc[0]).total_seconds() >= MIN_EVENT_SEC]

    # 2) 그 목록 안에서만 직전 이벤트와의 간격(정화)을 판정한다
    rows = []
    prev_end = None
    for t0, t1 in events:
        base_seg = d.loc[t0 - pd.Timedelta(seconds=pre + post): t0 - pd.Timedelta(seconds=post), "T"].dropna()
        resp_seg = d.loc[t0: t0 + pd.Timedelta(seconds=post), "T"].dropna()
        base_slope, resp_slope = _slope(base_seg), _slope(resp_seg)

        clean = prev_end is None or (t0 - prev_end).total_seconds() >= (pre + post)
        prev_end = t1

        if np.isnan(base_slope) or np.isnan(resp_slope) or not clean:
            continue

        rows.append({"start": t0, "dslope": resp_slope - base_slope})

    return pd.DataFrame(rows), (pre, post)


# --------------------------------------------------------------------------- #
# Feeding_Model.py 와 동일한 통계 틀 (이동 기준선 + z중심보정 + CUSUM)
# --------------------------------------------------------------------------- #

def _mad(base):
    med = np.median(base)
    scale = 1.4826 * np.median(np.abs(base - med))
    if scale < 1e-9:
        scale = np.std(base) if np.std(base) > 1e-9 else 1.0
    return med, scale


def rolling_z(v, window=BASELINE_WINDOW, clip=Z_CLIP):
    z = np.full(len(v), np.nan)
    for i in range(window, len(v)):
        med, scale = _mad(v[i - window:i])
        z[i] = (v[i] - med) / scale
    return np.clip(z, -clip, clip)


def cusum_down(z, k=CUSUM_K):
    s, out = 0.0, []
    for x in z:
        s = s if np.isnan(x) else max(0.0, s - x - k)
        out.append(s)
    return np.array(out)


def calibrate(train_vals):
    sign = np.sign(np.median(train_vals)) or 1.0
    z = rolling_z(train_vals * sign)
    bias = float(np.nanmean(z))
    cs = cusum_down(z - bias)
    thr = max(float(np.nanmax(cs)) * THRESH_MARGIN, 5.0)
    return sign, bias, thr


def fit_and_validate(e):
    v = e["dslope"].values.astype(float)
    n = len(v)
    n_tr = int(n * TRAIN_FRAC)
    if n_tr < BASELINE_WINDOW + 10 or n - n_tr < 10:
        return None

    sign, bias, thr = calibrate(v[:n_tr])
    z_all = rolling_z(v * sign) - bias
    cs_all = cusum_down(z_all)
    fa = int((cs_all[n_tr:] >= thr).sum())

    return {"n": n, "n_train": n_tr, "n_test": n - n_tr, "sign": sign, "bias": bias,
            "threshold": thr, "holdout_false_alarms": fa,
            "holdout_false_alarm_rate": fa / (n - n_tr)}


def injection(e, fit, severities=(0.25, 0.5, 1.0), n_inject=20, seed=0):
    """마지막 n_inject 개를 (1-심각도) 배로 약화시켜 몇 번째에 잡히는지 본다.

    **꼬리의 실제 관측값을 그대로 스케일하면 안 된다.** exchanger 의 개별 이벤트
    자체가 잡음이 커서(예: P1 정상 이벤트 중 하나가 +8.98 — 평소 중앙값 -1.6과
    부호까지 반대), 우연히 꼬리에 이런 이상치가 있으면 25%/50% 완화판은 그
    이상치가 줄어든 채로도 살아남아 "즉시 검출"되고, 100%(완전정지)는 오히려
    0으로 눌려서 그보다 덜 튀는 값이 되어 "미검출"되는 모순이 생긴다(실측으로
    확인됨). 그래서 꼬리의 실제값 대신, **학습 구간에서 재표본(bootstrap)한
    "전형적인 정상 반응"** 을 기준으로 심각도를 적용한다 — 그래야 심각도가
    커질수록 단조롭게 더 잘 잡혀야 정상이라는 전제가 성립한다.
    """
    v = e["dslope"].values.astype(float)
    if len(v) < BASELINE_WINDOW + n_inject:
        return {f"{int(s*100)}%": None for s in severities}

    n_tr = int(len(v) * TRAIN_FRAC)
    rng = np.random.default_rng(seed)
    typical = rng.choice(v[:n_tr], size=n_inject, replace=True)  # 학습 구간에서 재표본

    out = {}
    for sev in severities:
        vi = v.copy()
        vi[-n_inject:] = typical * (1 - sev)
        cs = cusum_down(rolling_z(vi * fit["sign"]) - fit["bias"])[-n_inject:]
        hit = int(np.argmax(cs >= fit["threshold"])) + 1 if (cs >= fit["threshold"]).any() else None
        out[f"{int(sev*100)}%"] = hit
    return out


def main():
    rows = []
    for tk in TARGET_TANKS:
        e, (pre, post) = extract_events(tk)
        fit = fit_and_validate(e)
        if fit is None:
            rows.append({"탱크": tk, "이벤트": len(e), "상태": "표본부족(정화 후)"})
            continue
        inj = injection(e, fit)
        rows.append({
            "탱크": tk, "창(전/후초)": f"{pre}/{post}", "정화후이벤트": fit["n"],
            "학습": fit["n_train"], "검증": fit["n_test"],
            "임계값": round(fit["threshold"], 2),
            "홀드아웃오경보": fit["holdout_false_alarms"],
            "오경보율%": round(fit["holdout_false_alarm_rate"] * 100, 1),
            "25%약화_검출": inj["25%"], "50%약화_검출": inj["50%"], "100%정지_검출": inj["100%"],
        })

    result = pd.DataFrame(rows)
    print("=== exchanger 갭+정화 Δ기울기 방법 — Feeding_Model 과 동일한 틀로 검증 ===")
    print(result.to_string(index=False))
    return result


if __name__ == "__main__":
    main()
