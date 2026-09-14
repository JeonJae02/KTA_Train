"""feeding 밸브/릴레이 진단 모델 — 학습 · 검증 · 판정.

**무엇을 판정하나.** feeding 명령이 나간 뒤 실제로 액체가 들어왔는지, 평소보다
약하지 않은지, 밸브가 늦게 열리지 않는지. 근거는 **수위**다 — 온도가 아니라.
온도로는 8개 중 4개 탱크만 잡혔지만 수위로는 8개 전부 잡힌다(Cohen's d 2.5~13.9).
수위는 feeding 의 결과가 직접 찍히는 계측이라 열 지연도 제어 역인과도 없다.

**수위는 정수 태그를 쓰지 않는다.** `TK_Level_PV` 는 정수로 반올림돼 ±0.5 오차가
실리는데 센서 실제 잡음은 0.04~0.08 이다. 원시 아날로그에서 다시 계산한
`level_f`(추출기가 만들어 둔다)를 쓰면 해상도가 38~67배 높다.

**유출 보정이 들어간다.** 수위 변화는 `유입 − 유출` 이라 순유량이지 밸브 유량이
아니다. feeding 직전 120초 수위 기울기로 유출률을 재서 빼낸다. 실측 유출률이
P5 −1.98/분, I3 −1.07/분 로 작지 않다. 분산 개선 자체는 P5(21%) 말고는 미미하지만,
**생산량이 바뀌면 유출률도 바뀌므로 보정 없는 지표는 밸브가 멀쩡해도 드리프트해
오경보를 낸다.** 정확도보다 견고성 때문에 넣는다.

**학습과 검증 기간을 반드시 나눈다.** 임계값을 뽑은 데이터에서 오경보를 세면
정의상 0 이 나온다(순환논법). 앞 기간으로 임계값을 정하고 **뒤 기간에서 실제
오경보를 센다.** 그 숫자가 이 모델을 믿어도 되는지에 대한 유일한 정직한 근거다.

**임계값은 목표 오경보 주기(ARL)로 정한다.** 예전의 "학습 구간 CUSUM 최대값
× 1.3" 은 극단값이라 표본이 늘수록 커져서, 데이터를 모을수록 모델이 둔해졌다.
지금은 학습 구간 z 를 재표본해 "정상인데 알람이 울릴 때까지 평균 몇 이벤트"를
시뮬레이션하고, 그 값이 TARGET_ARL 이 되는 임계값을 쓴다. **운영 중 판정
로직은 그대로다** — 여전히 `cusum >= 임계값` 하나만 본다.

사용법:
    python Feeding_Model.py fit                 # 학습 + 검증 (기본 분할: 앞 70%)
    python Feeding_Model.py fit --split "2026-09-13 00:00:00"
    python Feeding_Model.py check               # 저장된 파라미터로 최신 데이터 판정
"""

import argparse
import glob
import json
import os
import sys
from datetime import datetime

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from Tank_Temp_Factor_Analysis import TANKS, WAGON_TAG

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SAVE_DIR = os.path.join(BASE_DIR, "extracted_csv")
OUT_DIR = os.path.join(BASE_DIR, "analysis_out")
PARAM_PATH = os.path.join(BASE_DIR, "feeding_model_params.json")

EXCLUDE_HOURS = (0, 7)
RUNNING_WINDOW_MIN = 30       # wagon 이 이 시간 안에 안 움직이면 라인 정지
MIN_EVENT_SEC = 5             # I2/I3 펄스가 중앙값 7.3초라 10초로 잡으면 안 된다
TAIL_SEC = 60                 # 이벤트 종료 후 수위 반응을 더 보는 시간
OUTFLOW_PRE_SEC = 120         # 유출률을 재는 구간

BASELINE_WINDOW = 30          # 이동 기준선에 쓸 직전 이벤트 수
Z_CLIP = 4.0                  # 튀는 이벤트 하나로 CUSUM 이 폭발하는 걸 막는다
CUSUM_K = 0.5
OCCURRED_FRAC = 0.25          # 유입량이 평소 중앙값의 이 비율 미만이면 "미발생"

#: **임계값은 "목표 오경보 주기"로 정한다 (ARL 설계).**
#: 예전엔 `학습 구간 CUSUM 최대값 × 1.3` 을 썼는데, 최대값은 극단값 통계라
#: **표본이 많아질수록 저절로 커진다** — 데이터를 모을수록 모델이 둔해졌다
#: (실측: P1 학습 55개일 때 9.4 → 148개일 때 16.2, 검출이 3회에서 6회로 느려짐).
#: 게다가 탱크마다 기준이 제각각이 됐다 — 같은 방식으로 뽑은 임계값인데 실제
#: 오경보 주기가 P3 1.4일 ~ I1 59일로 42배 벌어져 있었다.
#: ARL(Average Run Length) = "정상 상태에서 알람이 잘못 울릴 때까지 평균 몇
#: 이벤트인가". 학습 구간 z 를 재표본해 가짜 정상 운전을 돌려서, 그 값이
#: TARGET_ARL 이 되는 가장 낮은 임계값을 찾는다. 분포의 성질이라 표본 크기에
#: 휘둘리지 않는다 (실측: P1 변동폭 1.83배 → 1.11배).
#: **운영 중 동작은 전혀 안 바뀐다** — 여전히 `cusum >= 임계값` 하나만 본다.
#: 500 은 탱크별 이벤트 간격 기준 3~16일에 한 번 오경보를 뜻한다.
TARGET_ARL = 500
ARL_SIM_CHAINS = 4000         # 시뮬레이션 체인 수 (많을수록 안정, 느려짐)
ARL_SIM_STEPS = 20000         # 체인당 최대 이벤트 수. TARGET_ARL 의 수십 배여야 한다
ARL_H_GRID = np.arange(1.0, 60.0, 0.1)
THRESH_FLOOR = 5.0            # 시뮬레이션이 이보다 낮게 주면 하한을 쓴다

MIN_EVENTS_TO_FIT = BASELINE_WINDOW + 20

#: **이력 없이 이벤트 하나만으로 "완전 미발생"을 판정하는 절대 기준.**
#: 밸브유량 CUSUM 은 이동 기준선(30개)이 쌓여야 도는데, P4·I2 는 그 전이라
#: 아예 감시가 안 됐다. 하지만 "아예 안 들어왔는지"는 순수 물리로 판단 가능
#: 하다 — 이벤트 구간의 수위 샘플 수를 알면, 순수 잡음만으로 우연히 나올 수
#: 있는 흔들림 크기(확산 근사: 잡음 × √표본수)를 계산할 수 있다. 실제 유입량이
#: 그 흔들림의 NOISE_MULT_MIN 배를 못 넘으면 "잡음과 구분이 안 된다" = 미발생.
#: 실측: 정상 이벤트들은 잡음의 30~65배라 여유가 크다 — 3배는 안전한 하한이다.
NOISE_MULT_MIN = 3.0

# --- CL(닫힘) 딜레이 조기 경보 ---
#: 알람은 이 딜레이가 3초를 넘으면 울리는 구조다(현재까지 한 번도 안 넘었음).
#: 2초는 그 직전 단계라 조기 신호가 될 수 있는데, 바로 알람으로 쓰지 않는다.
#: **한두 번은 그 밸브의 정상 변동일 수 있다** — 실측: P4 1건, I2 2건은 5일간 그
#: 정도 나오는 게 baseline 이었다(추세 없음, p=0.42~0.73). 그래서 누적 횟수가
#: MIN_COUNT 를 넘어야 "확인요망"으로 표시한다. P3/I1/I3 는 5일간 0건이라, 이
#: 셋 중 하나가 2초를 찍기 시작하는 것 자체가 이미 baseline 이탈이다.
VV_DELAY_PATTERN = "VV_Delay_Study_*.csv"
CL_DELAY_WINDOW_DAYS = 7
CL_DELAY_MIN_COUNT = 3


# --------------------------------------------------------------------------- #
# 데이터
# --------------------------------------------------------------------------- #

def load(tank_key):
    """탱크 CSV -> 정지/알람 구간 표시까지 붙인 원본 해상도 시계열."""
    cfg = TANKS[tank_key]
    path = sorted(glob.glob(os.path.join(SAVE_DIR, f"{tank_key}_Temp_vs_Factors_*.csv")))[-1]
    raw = pd.read_csv(path, index_col="Time", parse_dates=True,
                      usecols=["Time", "source", WAGON_TAG, "level_f", cfg["feeding"]]).sort_index()

    d = pd.DataFrame({"level": raw["level_f"], "feed": raw[cfg["feeding"]],
                      "wagon": raw[WAGON_TAG],
                      "diag": (raw["source"] == "DIAGNOSTIC").astype(float)})

    w = d["wagon"].resample("1min").last().ffill()
    run = (w.diff().abs() > 0).rolling(RUNNING_WINDOW_MIN, min_periods=1).max()
    d["running"] = run.reindex(d.index, method="ffill").fillna(0)

    h = d.index.hour
    d["ok"] = (((h < EXCLUDE_HOURS[0]) | (h >= EXCLUDE_HOURS[1]))
               & (d["diag"] == 0) & (d["running"] == 1))
    return d, os.path.basename(path)


def _slope_per_min(s):
    if len(s) < 3:
        return np.nan
    x = (s.index - s.index[0]).total_seconds().values
    return np.polyfit(x, s.values, 1)[0] * 60 if x[-1] > 0 else np.nan


def extract_events(tank_key):
    """이벤트별 지표 표. 여기까지는 판정이 안 들어간다 (학습/판정 공용)."""
    d, src = load(tank_key)
    lv = d["level"]
    noise = lv.diff().abs().median() * 1.4826 + 1e-9

    s = (d["feed"] == 1) & d["ok"]
    grp = (s & ~s.shift(1, fill_value=False)).cumsum()[s]

    rows = []
    for _, idx in d.index[s].to_series().groupby(grp):
        t0, t1 = idx.iloc[0], idx.iloc[-1]
        dur = (t1 - t0).total_seconds()
        if dur < MIN_EVENT_SEC:
            continue

        out_rate = _slope_per_min(lv.loc[t0 - pd.Timedelta(seconds=OUTFLOW_PRE_SEC): t0].dropna())
        a = lv.asof(t0 - pd.Timedelta(seconds=5))
        b = lv.asof(t1 + pd.Timedelta(seconds=TAIL_SEC))
        if np.isnan(out_rate) or pd.isna(a) or pd.isna(b):
            continue

        seg = lv.loc[t0: t1 + pd.Timedelta(seconds=TAIL_SEC)]
        over = seg[seg > a + 3 * noise]
        net = (b - a) / dur * 60

        # 이력 없이 즉시 판정 가능한 절대 기준 (아래 NOISE_MULT_MIN 설명 참고).
        # 확산 근사이므로 표본 수가 클수록 보수적(더 엄격)이 아니라 관대해지지
        # 않도록 sqrt 를 쓴다 — 순수 랜덤워크의 표준편차가 sqrt(n) 로 크는 것과 같다.
        noise_drift = noise * np.sqrt(max(len(seg), 1))
        noise_mult = abs(b - a) / noise_drift if noise_drift > 0 else np.inf

        rows.append({
            "start": t0, "지속초": round(dur, 1),
            "유입량": round(b - a, 3),
            "유출률": round(out_rate, 3),
            "순유량": round(net, 3),
            "밸브유량": round(net - out_rate, 3),     # ← 판정에 쓰는 값
            "지연초": round((over.index[0] - t0).total_seconds(), 1) if len(over) else np.nan,
            "잡음배수": round(noise_mult, 1),
            "발생_절대": bool(noise_mult >= NOISE_MULT_MIN),
        })

    return pd.DataFrame(rows), src, noise


# --------------------------------------------------------------------------- #
# 통계
# --------------------------------------------------------------------------- #

def _mad(base):
    med = np.median(base)
    scale = 1.4826 * np.median(np.abs(base - med))
    if scale < 1e-9:
        scale = np.std(base) if np.std(base) > 1e-9 else 1.0
    return med, scale


def rolling_z(v, window=BASELINE_WINDOW, clip=Z_CLIP):
    """직전 window 개를 기준선으로 한 로버스트 z.

    중앙값/MAD 를 쓰는 이유는 고장 이벤트 몇 개가 기준선을 끌어당기는 걸 막기
    위해서다. 고정 기준선이 아니라 이동 기준선인 이유는, 4.7일에 걸친 정상 공정
    변동까지 이상으로 쌓여 멀쩡한 탱크가 전부 고장으로 나오기 때문이다.
    릴레이 고장은 서서히가 아니라 계단형이라 직전 이벤트와 비교해도 놓치지 않는다.
    """
    z = np.full(len(v), np.nan)
    for i in range(window, len(v)):
        med, scale = _mad(v[i - window:i])
        z[i] = (v[i] - med) / scale
    return np.clip(z, -clip, clip)


def cusum_down(z, k=CUSUM_K, start=0.0):
    """반응이 기준보다 계속 작으면 쌓인다. 한 번 튄 건 다음에 0 으로 리셋된다.

    **넣기 전에 z 의 중심을 반드시 맞춰야 한다** (`fit` 이 학습 구간 z 평균을
    `z편향` 으로 저장한다). CUSUM 은 평균을 적분하는데, feeding 의 z 분포는
    왼쪽으로 치우쳐 있어서 — 가끔 크게 미달하는 이벤트가 꼬리를 만든다 —
    중앙값은 0 근처인데 평균은 음수다(P5: 중앙값 −0.013, 평균 −0.658).
    보정 없이 넣으면 정상 상태에서도 CUSUM 이 계속 떠올라(P5 평균 28) 임계값이
    무의미해진다. 실측: 보정하면 P5 CUSUM 평균 28.0 → 1.6, 홀드아웃 오경보 22 → 0.
    """
    s, out = start, []
    for x in z:
        s = s if np.isnan(x) else max(0.0, s - x - k)
        out.append(s)
    return np.array(out)


def simulate_arl(z_train, h_grid=None, n_chain=ARL_SIM_CHAINS,
                 t_max=ARL_SIM_STEPS, k=CUSUM_K, seed=0):
    """각 임계값 h 에 대해 "정상인데 알람이 울릴 때까지 걸리는 평균 이벤트 수".

    학습 구간 z 에서 복원추출해 가짜 정상 운전을 n_chain 개 돌린다. S 는
    단조증가하지 않지만 **"지금까지의 최고치"는 단조**라, 최고치가 갱신된
    순간만 기록해 두면 모든 h 에 대한 첫 도달 시각을 한 번의 시뮬레이션으로
    전부 구할 수 있다 (h 마다 다시 돌릴 필요가 없다).

    **한계**: 복원추출이라 z 의 자기상관을 무시한다. 실측 자기상관이 P1 0.28,
    나머지는 0.04~0.18 이다 — 자기상관이 있으면 실제 오경보가 시뮬레이션보다
    조금 잦다. P1 은 목표 500 이면 실제로는 400 정도로 보는 게 안전하다.
    """
    h_grid = ARL_H_GRID if h_grid is None else h_grid
    z = np.asarray(z_train, dtype=float)
    z = z[~np.isnan(z)]
    if len(z) < 5:
        return None
    rng = np.random.default_rng(seed)
    s = np.zeros(n_chain)
    runmax = np.zeros(n_chain)
    rec_i, rec_v, rec_t = [], [], []
    for t in range(1, t_max + 1):
        s = np.maximum(0.0, s - rng.choice(z, n_chain) - k)
        upd = s > runmax
        if upd.any():
            idx = np.flatnonzero(upd)
            rec_i.append(idx); rec_v.append(s[idx]); rec_t.append(np.full(len(idx), t))
            runmax[idx] = s[idx]
    ri = np.concatenate(rec_i); rv = np.concatenate(rec_v); rt = np.concatenate(rec_t)

    arl = np.empty(len(h_grid))
    for j, h in enumerate(h_grid):
        m = rv >= h
        rl = np.full(n_chain, float(t_max))       # 끝까지 안 넘으면 t_max 로 검열
        if m.any():
            ii, tt = ri[m], rt[m]
            order = np.argsort(tt)                 # 시간순 -> 각 체인의 첫 도달만 취함
            ii, tt = ii[order], tt[order]
            _, first = np.unique(ii, return_index=True)
            rl[ii[first]] = tt[first]
        arl[j] = rl.mean()
    return arl


def threshold_by_arl(z_train, target=TARGET_ARL, seed=0):
    """목표 ARL 을 만족하는 가장 낮은 임계값. 실패하면 (None, 사유)."""
    arl = simulate_arl(z_train, seed=seed)
    if arl is None:
        return THRESH_FLOOR, "z부족"
    ok = np.flatnonzero(arl >= target)
    if not len(ok):
        return float(ARL_H_GRID[-1]), "격자초과"
    h = float(ARL_H_GRID[ok[0]])
    return max(h, THRESH_FLOOR), "정상"


def score_events(e, params=None, bias=0.0):
    """이벤트 표에 z / cusum / 판정을 붙인다. params 가 없으면 임계값 없이 지표만."""
    e = e.copy()
    v = e["밸브유량"].values.astype(float)
    sign = np.sign(np.median(v)) or 1.0
    if params and "z편향" in params:
        bias = params["z편향"]
    e["z"] = rolling_z(v * sign)
    e["cusum"] = cusum_down(e["z"].values - bias)

    if params:
        e["발생"] = e["유입량"] >= params["유입량중앙"] * OCCURRED_FRAC
        thr = params["임계값"]
        e["판정"] = np.where(~e["발생"], "미발생",
                    np.where(e["cusum"] >= thr, "확정",
                    np.where(e["cusum"] >= thr * 0.6, "주의", "정상")))
        e["모드"] = "CUSUM"
    return e


def score_absolute(e):
    """이력(과거 이벤트) 없이, 이벤트 하나만으로 판정한다 — 표본부족 탱크용.

    CUSUM 을 못 돌리니 "평소보다 약한지"는 못 보고, "완전 미발생"만 잡는다.
    `extract_events` 가 이미 계산해 둔 `발생_절대`(잡음배수 기준)를 그대로 쓴다.
    """
    e = e.copy()
    e["판정"] = np.where(e["발생_절대"], "정상", "미발생")
    e["모드"] = "절대판정만"
    return e


# --------------------------------------------------------------------------- #
# CL(닫힘) 딜레이 조기 경보 — 단순 누적 카운트, CUSUM 아님
# --------------------------------------------------------------------------- #

def close_delay_report(window_days=CL_DELAY_WINDOW_DAYS, min_count=CL_DELAY_MIN_COUNT):
    """CL_Err_Delay 가 2초에 도달한 횟수를 센다.

    `TK_Feed_VV_Open/Close/*_Err_Delay_*` 태그는 메인 탱크 CSV 에 없다
    (`VV_Delay_Study_*.csv` 로 별도 추출). 없으면 안내만 하고 넘어간다 —
    이 플래그는 feeding 판정의 필수 요소가 아니라 보조 지표다.
    """
    files = sorted(glob.glob(os.path.join(SAVE_DIR, VV_DELAY_PATTERN)))
    if not files:
        print(f"⚠️ {VV_DELAY_PATTERN} 이 없어 CL 딜레이 확인을 건너뜁니다.")
        print("   Custom_Tag_Extractor.py 로 TK_Feed_VV_CL_Err_Delay_* 태그를 먼저 받으세요.")
        return None

    raw = pd.read_csv(files[-1], index_col="Time", parse_dates=True).sort_index()

    w = raw[WAGON_TAG].resample("1min").last().ffill()
    run = (w.diff().abs() > 0).rolling(RUNNING_WINDOW_MIN, min_periods=1).max()
    running = run.reindex(raw.index, method="ffill").fillna(0)
    h = raw.index.hour
    ok = ((h < EXCLUDE_HOURS[0]) | (h >= EXCLUDE_HOURS[1])) & (running == 1)
    if "source" in raw.columns:
        ok &= raw["source"] != "DIAGNOSTIC"

    cutoff = raw.index.max() - pd.Timedelta(days=window_days)
    rows = []
    for tk in TANKS:
        col = f"TK_Feed_VV_CL_Err_Delay_{tk}"
        if col not in raw.columns:
            continue
        s = raw[col][ok]
        # 2초에 "도달한 순간" 만 센다 (2초에 머물러 있는 매 샘플이 아니라 진입 엣지)
        hit_idx = s.index[(s >= 2) & (s.shift(1, fill_value=0) < 2)]
        recent = hit_idx[hit_idx >= cutoff]
        rows.append({
            "탱크": tk, "2초_전체건수": len(hit_idx),
            f"2초_최근{window_days}일": len(recent),
            "최근발생": str(recent.max()) if len(recent) else None,
            "판정": "확인요망" if len(recent) >= min_count else "정상",
        })

    out = pd.DataFrame(rows)
    print(f"\n=== CL(닫힘) 딜레이 2초 도달 — 최근 {window_days}일 {min_count}회 이상이면 확인요망 ===")
    print(out.to_string(index=False))

    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, "close_delay_확인.csv")
    out.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"💾 저장: {path}")
    return out


# --------------------------------------------------------------------------- #
# 학습 · 검증
# --------------------------------------------------------------------------- #

def fit(split=None, train_frac=0.7):
    """앞 기간으로 임계값을 정하고 **뒤 기간에서 실제 오경보를 센다.**"""
    params, report = {}, []

    for tk in TANKS:
        e, src, noise = extract_events(tk)
        if len(e) < MIN_EVENTS_TO_FIT:
            report.append({"탱크": tk, "전체이벤트": len(e), "상태": "표본부족",
                           "필요": MIN_EVENTS_TO_FIT})
            continue

        if split:
            cut = pd.Timestamp(split)
            n_tr = int((e["start"] < cut).sum())
        else:
            n_tr = int(len(e) * train_frac)

        if n_tr < MIN_EVENTS_TO_FIT or len(e) - n_tr < 10:
            report.append({"탱크": tk, "전체이벤트": len(e), "상태": "분할불가",
                           "학습": n_tr, "검증": len(e) - n_tr})
            continue

        tr, te = e.iloc[:n_tr], e.iloc[n_tr:]

        # --- 학습: z 중심 보정 -> 그 상태의 CUSUM 최대값으로 임계값 산정 ---
        v_tr = tr["밸브유량"].values.astype(float)
        sign_tr = np.sign(np.median(v_tr)) or 1.0
        bias = float(np.nanmean(rolling_z(v_tr * sign_tr)))
        z_tr = score_events(tr)["z"].values - bias
        cs_tr = cusum_down(z_tr)
        thr, arl_state = threshold_by_arl(z_tr)
        n_z = int(np.sum(~np.isnan(z_tr)))

        p = {"z편향": round(bias, 4),
             "유입량중앙": float(tr["유입량"].median()),
             "밸브유량중앙": float(tr["밸브유량"].median()),
             "유출률중앙": float(tr["유출률"].median()),
             "지연중앙": float(tr["지연초"].median()),
             "수위잡음": float(noise),
             "임계값": float(round(thr, 2)),
             "목표ARL": int(TARGET_ARL),
             "임계값산정": arl_state,
             "학습z개수": n_z,
             "학습이벤트": int(n_tr),
             "학습기간": [str(tr["start"].iloc[0]), str(tr["start"].iloc[-1])],
             "출처": src}
        params[tk] = p

        # --- 검증: 뒤 기간에서 그 임계값으로 오경보를 센다 (여기가 정직한 숫자) ---
        scored_all = score_events(e, p)
        val = scored_all.iloc[n_tr:]
        fa = int((val["cusum"] >= thr).sum())
        miss = int((~val["발생"]).sum())

        report.append({
            "탱크": tk, "전체이벤트": len(e), "상태": "학습완료",
            "학습": n_tr, "검증": len(te), "임계값": round(thr, 1),
            "검증_CUSUM최대": round(float(val["cusum"].max()), 1),
            "검증_오경보": fa, "검증_미발생": miss,
            "오경보율": f"{fa / len(te) * 100:.1f}%",
            "학습z": n_z,
            # ARL 은 z 의 분포를 시뮬레이션하므로, 못 믿는 경우는 "임계값이 낮다"가
            # 아니라 **분포를 추정할 z 자체가 적다** 는 것이다. 학습 이벤트에서
            # 앞 BASELINE_WINDOW 개는 z 가 안 나오므로 실제 z 는 그만큼 적다
            # (예: I1 은 학습 52개지만 z 는 22개뿐).
            "신뢰도": "잠정(z부족)" if n_z < 50 else "정상",
        })

    with open(PARAM_PATH, "w", encoding="utf-8") as f:
        json.dump(params, f, ensure_ascii=False, indent=2)

    rep = pd.DataFrame(report)
    print("=== 학습 + 홀드아웃 검증 ===")
    print(rep.to_string(index=False))
    print(f"\n💾 파라미터 저장: {PARAM_PATH}")
    return params, rep


def sensitivity(params, severities=(0.25, 0.5, 1.0), n_inject=20):
    """가짜 고장 주입 — 반응이 그만큼 약해지면 몇 번째 이벤트에 잡히나."""
    rows = []
    for tk, p in params.items():
        e, _, _ = extract_events(tk)
        v = e["밸브유량"].values.astype(float)
        sign = np.sign(np.median(v)) or 1.0
        gap = pd.Series(e["start"]).diff().dt.total_seconds().median()
        r = {"탱크": tk, "이벤트간격(분)": round(gap / 60, 1)}
        for sev in severities:
            vi = v.copy()
            vi[-n_inject:] = vi[-n_inject:] * (1 - sev)
            cs = cusum_down(rolling_z(vi * sign) - p.get("z편향", 0.0))[-n_inject:]
            hit = int(np.argmax(cs >= p["임계값"])) + 1 if (cs >= p["임계값"]).any() else None
            r[f"{int(sev*100)}%약화"] = hit
            r[f"{int(sev*100)}%_분"] = round(hit * gap / 60) if hit else None
        rows.append(r)
    out = pd.DataFrame(rows)
    print("\n=== 민감도: 고장 후 몇 번째 이벤트에 확정 알람 ===")
    print(out.to_string(index=False))
    return out


def check():
    """저장된 파라미터로 최신 데이터를 판정한다.

    파라미터가 없는(표본부족) 탱크도 건너뛰지 않는다 — CUSUM 은 못 돌려도
    `score_absolute` 로 "완전 미발생"만큼은 이력 없이 바로 감시한다.
    """
    if not os.path.exists(PARAM_PATH):
        print("⚠️ 먼저 `python Feeding_Model.py fit` 을 실행하세요.")
        return None

    with open(PARAM_PATH, encoding="utf-8") as f:
        params = json.load(f)

    os.makedirs(OUT_DIR, exist_ok=True)
    rows, details = [], []
    for tk in TANKS:
        e, src, _ = extract_events(tk)
        if len(e) == 0:
            rows.append({"탱크": tk, "이벤트": 0, "상태": "이벤트없음", "모드": "-"})
            continue

        p = params.get(tk)
        if p is not None and len(e) >= BASELINE_WINDOW + 1:
            s = score_events(e, p)
            s.insert(0, "탱크", tk)
            details.append(s)
            last = s.iloc[-1]
            rows.append({
                "탱크": tk, "이벤트": len(s), "상태": "정상감시", "모드": "CUSUM",
                "최근시각": str(last["start"]),
                "밸브유량": last["밸브유량"], "기준": round(p["밸브유량중앙"], 2),
                "지연초": last["지연초"], "기준지연": round(p["지연중앙"], 1),
                "CUSUM": round(float(last["cusum"]), 2), "임계값": p["임계값"],
                "미발생누적": int((~s["발생"]).sum()),
                "판정": last["판정"],
            })
        else:
            # 표본부족 -> CUSUM 은 못 돌려도 이력 없이 되는 절대판정은 돌린다
            s = score_absolute(e)
            s.insert(0, "탱크", tk)
            details.append(s)
            last = s.iloc[-1]
            rows.append({
                "탱크": tk, "이벤트": len(s),
                "상태": f"표본부족({len(e)}/{MIN_EVENTS_TO_FIT}, CUSUM 불가)", "모드": "절대판정만",
                "최근시각": str(last["start"]),
                "밸브유량": last["밸브유량"], "기준": None,
                "지연초": last["지연초"], "기준지연": None,
                "CUSUM": None, "임계값": None,
                "미발생누적": int((~s["발생_절대"]).sum()),
                "판정": last["판정"],
            })

    res = pd.DataFrame(rows)
    print("=== 현재 판정 ===")
    print(res.to_string(index=False))

    if details:
        allrows = pd.concat(details, ignore_index=True)
        path = os.path.join(OUT_DIR, "feeding_판정.csv")
        allrows.to_csv(path, index=False, encoding="utf-8-sig")
        flagged = allrows[allrows["판정"] != "정상"]
        print(f"\n💾 이벤트별 판정: {path}  (전체 {len(allrows)}건, 이상 {len(flagged)}건)")
        if len(flagged):
            print(flagged.tail(10)[["탱크", "start", "밸브유량", "z", "cusum", "판정"]].to_string(index=False))

    close_delay_report()
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("mode", choices=["fit", "check"])
    ap.add_argument("--split", default=None, help="학습/검증 경계 시각 (예: '2026-09-13 00:00:00')")
    ap.add_argument("--train-frac", type=float, default=0.7)
    a = ap.parse_args()

    if a.mode == "fit":
        params, _ = fit(split=a.split, train_frac=a.train_frac)
        if params:
            sensitivity(params)
    else:
        check()
