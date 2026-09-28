"""릴레이가 "이번에 실제로 작동했는가" — 이벤트 단위 실시간 판정기.

`Exchanger_시도_총정리.ipynb` 의 결론(이유 1~10)은 "지속적인 부분 열화"를
적응형 기준선(CUSUM)으로 잡으려던 모든 시도가 실패했다는 것이었다. 이 스크립트는
**다른 질문**을 푼다 — 열화가 아니라 "이번 ON 신호에 온도가 반응했는가"를
매 이벤트 독립적으로 본다. CUSUM 처럼 기준선이 누적되며 스스로 가라앉는 함정이
없다: 매 이벤트를 그 이벤트만 보고 판정한다.

## 방법

    켜지기 전 pre초  평균 기울기 = base_slope
    켜진 후 post초    평균 기울기 = resp_slope
    Δ기울기 = resp_slope - base_slope     (정상이면 음수 — 냉각 방향으로 꺾인다)

Δ기울기 하나만으로는 판단 기준이 없다. 그래서 **"신호가 전혀 없었다면 Δ기울기가
어떻게 나왔을까"** 를 실측 대조군으로 만든다 — exchanger 가 꺼져 있던 무작위
시점에서 똑같은 pre/post 창으로 잰 Δ기울기 분포다(자연스러운 온도 요동만 반영,
액추에이터 효과는 없음). ON 이벤트의 Δ기울기 분포와 이 "무반응" 분포가 얼마나
갈라지는지가 곧 "이 탱크에서, 이 창 길이로, 단일 이벤트를 판정할 수 있는가"의
답이다.

## 탱크별 pre/post 최적화

탱크마다 heat/cool 진동 주기, 토글 빈도, 냉각 능력이 달라(이유 5, 이유 9) 창
하나를 공통으로 쓰면 손해다. `PRE_CANDIDATES × POST_CANDIDATES` 그리드에서
"ON 분포 평균 Δ기울기의 절대값 / ON 분포 표준편차"(=SNR)를 최대화하는 조합을
탱크마다 고른다.

## 검증

임계값은 ON 분포에서 고르는 게 아니라 **무반응(대조) 분포의 p95** 로 고정한다
— "정상적으로 아무 냉각도 없을 때 그 정도 Δ기울기가 우연히 나올 확률이 5%"인
지점이다. 그 임계값으로:
  - 실제 ON 이벤트 중 몇 %가 "무반응처럼 보이는지"(= 미검출/정상작동 추정 실패율)
  - 대조군(진짜 무반응) 중 몇 %가 임계값을 넘는지(= 설계상 정확히 5%, 확인용)
을 보고한다. 겹침이 크면(ON 분포 대부분이 무반응 분포 안에 들어가 있으면) 그
탱크는 이 방법으로 단일 이벤트 판정이 못 미덥다는 뜻이고, 리포트에 그대로 나온다
— 좋아 보이게 포장하지 않는다.

사용법:
    python Exchanger_Relay_Check.py
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

#: 그리드서치 후보 (초). pre 는 too-길면 이전 이벤트 잔향/탱크 주기에 걸리고
#: (이유 5), too-짧으면 잡음이 커진다. post 도 마찬가지라 둘 다 스윕한다.
PRE_CANDIDATES = [15, 20, 30, 45, 60, 90, 120, 150, 180]
POST_CANDIDATES = [10, 15, 20, 30, 45, 60, 90]

MIN_EVENTS_FOR_WINDOW = 100   # 이보다 적은 창 조합은 그리드서치에서 제외
SMALL_SAMPLE_WARN = 200       # 이보다 적으면 리포트에 "표본 적음" 경고 붙임
N_CONTROL = 1500              # 대조군(무반응) 표본 뽑을 후보 시점 수
FLAG_PCT = 95                 # 대조군 분포의 이 퍼센타일을 임계값으로 쓴다
RNG_SEED = 0

#: 2026-09-10~09-21 (11일치) 데이터로 grid_search() 를 돌려 얻은 탱크별 최적값.
#: main() 을 다시 돌리면 재산출되지만, 매번 그리드서치(탱크당 9×7=63개 창 조합)를
#: 새로 돌릴 필요는 없어서 실전 사용(check_relay)은 이 상수를 바로 쓴다.
#: 데이터가 한 달 이상 더 쌓이면 재실행해서 갱신하는 걸 권장 — 특히 I2 는
#: 표본이 130개뿐이라(가동률 자체가 낮은 탱크) 다른 탱크보다 신뢰도가 낮다.
TUNED = {
    #        pre(초) post(초) 임계값(Δ기울기)  Cohen's d  미검출률%
    "P1": {"pre": 180, "post": 60, "thr": 2.949},   # d=0.67  미검출 4.6%
    "P2": {"pre": 60,  "post": 90, "thr": 0.870},   # d=1.32  미검출 7.2%
    "P3": {"pre": 15,  "post": 20, "thr": 5.585},   # d=0.40  미검출 4.0% (P3는 d가 작아 참고용)
    "P4": {"pre": 120, "post": 90, "thr": 0.167},   # d=5.84  미검출 0.1% — 가장 확실
    "P5": {"pre": 30,  "post": 90, "thr": 1.407},   # d=2.32  미검출 2.4%
    "I1": {"pre": 15,  "post": 90, "thr": 3.355},   # d=0.79  미검출 4.0%
    "I2": {"pre": 20,  "post": 90, "thr": 2.016},   # d=1.55  미검출 2.3% (표본 130개뿐)
    "I3": {"pre": 15,  "post": 90, "thr": 3.286},   # d=2.59  미검출 1.5%
}


# --------------------------------------------------------------------------- #
# 데이터 로드 (Exchanger_시도_총정리.ipynb 의 load() 와 동일한 정의)
# --------------------------------------------------------------------------- #

def load(tank_key):
    cfg = TANKS[tank_key]
    paths = sorted(glob.glob(os.path.join(SAVE_DIR, f"{tank_key}_Temp_vs_Factors_*.csv")))
    if not paths:
        raise FileNotFoundError(f"{tank_key} 추출 CSV 없음 — Tank_Temp_Factor_Analysis.py 먼저 실행")
    path = paths[-1]
    raw = pd.read_csv(path, index_col="Time", parse_dates=True,
                      usecols=["Time", "source", WAGON_TAG, cfg["temp"], cfg["exchanger"],
                              cfg["heat"], cfg["cool"], cfg["feeding"]]).sort_index()
    raw = raw[~raw.index.duplicated(keep="last")]
    d = pd.DataFrame({"T": raw[cfg["temp"]], "exch": raw[cfg["exchanger"]],
                      "heat": raw[cfg["heat"]], "cool": raw[cfg["cool"]], "feed": raw[cfg["feeding"]],
                      "wagon": raw[WAGON_TAG], "diag": (raw["source"] == "DIAGNOSTIC").astype(float)})
    w = d["wagon"].resample("1min").last().ffill()
    run = (w.diff().abs() > 0).rolling(RUNNING_WINDOW_MIN, min_periods=1).max()
    d["running"] = run.reindex(d.index, method="ffill").fillna(0)
    h = d.index.hour
    d["ok"] = (((h < EXCLUDE_HOURS[0]) | (h >= EXCLUDE_HOURS[1])) & (d["diag"] == 0) & (d["running"] == 1))
    return d


def slope(seg):
    if len(seg) < 3:
        return np.nan
    x = (seg.index - seg.index[0]).total_seconds().values
    return np.polyfit(x, seg.values, 1)[0] * 60 if x[-1] > 0 else np.nan


def raw_events(d):
    """모든 exch ON 이벤트의 (시작, 끝). 5초 미만 chattering 은 제외."""
    s = (d["exch"] == 1) & d["ok"]
    grp = (s & ~s.shift(1, fill_value=False)).cumsum()[s]
    return [(idx.iloc[0], idx.iloc[-1]) for _, idx in d.index[s].to_series().groupby(grp)
            if (idx.iloc[-1] - idx.iloc[0]).total_seconds() >= MIN_EVENT_SEC]


# --------------------------------------------------------------------------- #
# Δ기울기 표 — ON 이벤트용 / 대조군(무반응)용
# --------------------------------------------------------------------------- #

def on_dslope_table(d, events, pre, post):
    """갭+정화 적용: 기준 구간이 직전 이벤트 잔향에 안 걸친 것만 남긴다."""
    rows = []
    prev_end = None
    for t0, t1 in events:
        base = slope(d.loc[t0 - pd.Timedelta(seconds=pre): t0, "T"].dropna())
        resp = slope(d.loc[t0: t0 + pd.Timedelta(seconds=post), "T"].dropna())
        clean = prev_end is None or (t0 - prev_end).total_seconds() >= (pre + post)
        prev_end = t1
        if np.isnan(base) or np.isnan(resp) or not clean:
            continue
        rows.append({"t0": t0, "base_slope": base, "resp_slope": resp, "dslope": resp - base})
    return pd.DataFrame(rows)


def control_dslope_table(d, pre, post, n_target, rng):
    """exchanger 가 계속 꺼져 있던(직전 300초도 OFF) 무작위 시점에서 같은 창으로 잰
    Δ기울기 — '아무 일도 없었다면' 의 실측 분포."""
    idx = d.index[d["ok"] & (d["exch"] == 0)]
    if len(idx) == 0:
        return pd.DataFrame(columns=["t0", "dslope"])
    order = rng.permutation(len(idx))
    rows = []
    for i in order:
        t = idx[i]
        if d.loc[t - pd.Timedelta(seconds=300): t + pd.Timedelta(seconds=post), "exch"].max() > 0:
            continue
        base = slope(d.loc[t - pd.Timedelta(seconds=pre): t, "T"].dropna())
        resp = slope(d.loc[t: t + pd.Timedelta(seconds=post), "T"].dropna())
        if np.isnan(base) or np.isnan(resp):
            continue
        rows.append({"t0": t, "dslope": resp - base})
        if len(rows) >= n_target:
            break
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- #
# 탱크별 pre/post 그리드서치
# --------------------------------------------------------------------------- #

def grid_search(tank_key, d, events, rng):
    best = None
    for pre in PRE_CANDIDATES:
        for post in POST_CANDIDATES:
            on_tbl = on_dslope_table(d, events, pre, post)
            if len(on_tbl) < MIN_EVENTS_FOR_WINDOW:
                continue
            v = on_tbl["dslope"].values
            if np.median(v) >= 0 or v.std() == 0:
                continue
            snr = abs(v.mean()) / v.std()
            if best is None or snr > best["snr"]:
                best = {"pre": pre, "post": post, "snr": snr, "n_on": len(on_tbl), "on_tbl": on_tbl}
    return best


# --------------------------------------------------------------------------- #
# 임계값 산정 + 검증
# --------------------------------------------------------------------------- #

def evaluate(tank_key):
    d = load(tank_key)
    events = raw_events(d)
    rng = np.random.default_rng(RNG_SEED)

    best = grid_search(tank_key, d, events, rng)
    if best is None:
        return {"탱크": tank_key, "상태": "표본부족 — 어느 창도 이벤트 150개를 못 채움"}

    pre, post = best["pre"], best["post"]
    on_tbl = best["on_tbl"]
    ctl_tbl = control_dslope_table(d, pre, post, N_CONTROL, rng)
    if len(ctl_tbl) < 100:
        return {"탱크": tank_key, "상태": "대조군 표본부족"}

    thr = float(np.percentile(ctl_tbl["dslope"].values, FLAG_PCT))
    on_v = on_tbl["dslope"].values
    ctl_v = ctl_tbl["dslope"].values

    pooled_std = np.sqrt((on_v.var() + ctl_v.var()) / 2)
    cohens_d = (ctl_v.mean() - on_v.mean()) / pooled_std if pooled_std > 0 else np.nan

    miss_rate = float((on_v >= thr).mean())        # 실제 ON인데 "무반응처럼" 보이는 비율
    ctl_hit_rate = float((ctl_v >= thr).mean())     # 설계상 (100-FLAG_PCT)% 근처여야 정상

    return {
        "탱크": tank_key, "pre(초)": pre, "post(초)": post,
        "ON이벤트수": best["n_on"], "대조군수": len(ctl_tbl),
        "Cohen's d": round(float(cohens_d), 2),
        "임계값(Δ기울기)": round(thr, 3),
        "ON평균Δ기울기": round(float(on_v.mean()), 3),
        "ON표준편차": round(float(on_v.std()), 3),
        "대조군평균Δ기울기": round(float(ctl_v.mean()), 3),
        "미검출률%(ON인데 임계값 못넘음)": round(miss_rate * 100, 1),
        "대조군오탐률%(설계상 ~5)": round(ctl_hit_rate * 100, 1),
        "비고": "표본 적음" if best["n_on"] < SMALL_SAMPLE_WARN else "",
        "_pre": pre, "_post": post, "_thr": thr, "_on_tbl": on_tbl, "_ctl_tbl": ctl_tbl,
    }


def check_event(tank_key, t0, pre, post, thr):
    """단발성 체크: 특정 ON 이벤트 시작 시각 t0 에 대해 릴레이가 반응했는지 판정.

    반환: (판정, Δ기울기). 판정 True = 정상(냉각 반응 확인), False = 확인요망.
    """
    d = load(tank_key)
    base = slope(d.loc[t0 - pd.Timedelta(seconds=pre): t0, "T"].dropna())
    resp = slope(d.loc[t0: t0 + pd.Timedelta(seconds=post), "T"].dropna())
    if np.isnan(base) or np.isnan(resp):
        return None, np.nan
    dslope = resp - base
    return bool(dslope < thr), dslope


def check_relay(tank_key, t0):
    """실전용 단축 함수 — TUNED 표에 있는 탱크별 최적 창/임계값을 그대로 쓴다.

    사용 예:
        ok, dslope = check_relay("P4", pd.Timestamp("2026-09-20 14:03:10"))
        # ok=True  → 켜진 뒤 온도가 기대만큼 꺾였다 (정상)
        # ok=False → 꺾이지 않았다 (확인요망)
        # ok=None  → 그 구간 데이터가 부족해 판정 불가(전/후 데이터 결측)
    """
    if tank_key not in TUNED:
        raise ValueError(f"{tank_key} 는 TUNED 에 없음 — 지원 탱크: {list(TUNED)}")
    cfg = TUNED[tank_key]
    return check_event(tank_key, t0, cfg["pre"], cfg["post"], cfg["thr"])


def check_all_recent(tank_key, lookback_hours=24):
    """최근 lookback_hours 시간 안의 모든 ON 이벤트를 TUNED 값으로 일괄 판정해 표로 낸다."""
    cfg = TUNED[tank_key]
    d = load(tank_key)
    cutoff = d.index[-1] - pd.Timedelta(hours=lookback_hours)
    events = [(t0, t1) for t0, t1 in raw_events(d) if t0 >= cutoff]
    rows = []
    for t0, t1 in events:
        ok, dslope = check_event(tank_key, t0, cfg["pre"], cfg["post"], cfg["thr"])
        rows.append({"시작": t0, "종료": t1, "Δ기울기": round(dslope, 3) if not np.isnan(dslope) else None,
                     "판정": "정상" if ok else ("확인요망" if ok is False else "판정불가")})
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- #
# 가짜고장 주입 검증 — "그럴듯해 보이다가 실전에서 무너지는" 함정을 재확인
# --------------------------------------------------------------------------- #
#
# Exchanger_시도_총정리.ipynb 이유 8 은 SNR 좋아 보였던 방법(I2 1.45, I3 1.54)이
# 진짜 가짜고장을 주입하자 전부 실패했음을 보였다. 이번 방법(매 이벤트 독립 판정)도
# 같은 시험을 거쳐야 한다.
#
# **처음 시도했다가 버린 방식**: "exchanger 가 꺼져 있던 무관한 시점의 실측
# post-슬로프"를 실제 ON 이벤트의 base_slope 에 갖다붙이는 방식을 썼었다. 그런데
# 실측해보니 ON 이벤트의 base_slope(켜지기 직전, 설정값을 향해 가파르게 오르는
# 중 — P4 평균 +4.3)과 무관한 오프구간의 slope(평균 거의 0, 그런 가파른 상승이
# 애초에 없는 상태)는 **서로 다른 물리적 상황**이다. 둘을 섞으면 "무반응"을
# 흉내내려던 값이 오히려 "냉각이 아주 잘 됐다"처럼 보이는 가짜 결과가 나왔다
# (검출률이 역설적으로 0%에 가깝게 나옴 — 실측으로 확인, 버그였다).
#
# **고친 방식**: 다른 시점의 절대 슬로프를 섞어오지 않고, **그 이벤트 자신의
# 실측 dslope 를 그대로 스케일**한다.
#
#     dslope_주입(severity) = 실제_dslope × (1 - severity)
#
#     severity=0     원본 그대로 (정상)
#     severity=0.5   냉각효과 절반만(밸브 반쯤 열림 정도)
#     severity=1.0   냉각효과 전혀 없음(완전무반응, dslope=0)
#     severity=1.5   원래 냉각해야 할 크기의 절반만큼 오히려 승온(밸브가 막혀
#                    역행하는 최악의 경우) — 부호가 뒤집힌다
#
# 각 이벤트 고유의 base_slope/실측 변동성을 그대로 보존한 채 반응 크기만 줄이므로
# (또는 반전시키므로), 앞의 "무관한 시점 섞기" 버그가 생길 수 없다.

SEVERITIES = (0.0, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5)

#: 2026-09-21 실측 결과 (run_injection_validation, 11일치 데이터) — 요약.
#:
#:   완전무반응(dslope=0) 검출률은 **8개 탱크 전부 0%.** 이건 버그가 아니라
#:   구조적 사실이다 — "명령은 갔는데 냉각 효과가 정확히 0" 인 상태는, 애초에
#:   임계값을 정의할 때 쓴 "무반응(대조군)" 분포와 물리적으로 같은 신호다
#:   (둘 다 "이 구간엔 냉각이 없었다"). 그래서 원리적으로 구분이 안 된다 —
#:   Exchanger_시도_총정리.ipynb 의 결론(문턱 제어기가 신호를 스스로 지운다)과
#:   정확히 같은 벽에 다시 부딪힌 것.
#:
#:   반대로 "역행"(온도가 원래 꺾여야 할 크기의 일부만큼 오히려 계속 오름 —
#:   밸브가 막혀서 heat 는 못 이기고 온도가 계속 올라가는 경우)은 몇몇 탱크에서
#:   아주 잘 잡힌다: P4 99.9%, P5 86.7~92.4%, I3 68.5~77.7% (심각도 25%/50%).
#:   P2 는 50% 심각도부터(74.1%) 잡히고 25% 는 못 잡는다(8.9%). I1 은
#:   23~27% 로 절반도 못 미친다. P1·P3·I2 는 5% 미만으로 사실상 못 잡는다.
#:
#: **결론**: 이 방법이 실제로 잡을 수 있는 고장은 "밸브가 막혀 온도가 계속
#: 올라가는" 유형뿐이고(그것도 P4·P5·I3·P2 위주), "명령은 갔는데 조용히 아무
#: 일도 안 일어나는" 유형은 온도만으로는 원천적으로 못 잡는다. 실전에서는
#: "확인요망"이 하나도 안 뜬다고 릴레이가 정상이라고 보장할 수 없다는 뜻 —
#: 이 방법은 "온도가 이상하게 계속 오르는 조짐"의 조기경보로만 쓰고, 릴레이
#: 자체의 정상/고장 여부를 이걸로 최종 판정하면 안 된다.


def injection_validation(tank_key, res):
    """severity 별로 실제 dslope 를 스케일해 몇 %가 임계값을 넘는지(검출률) 잰다."""
    if "_thr" not in res:
        return {"탱크": tank_key, "상태": res.get("상태", "평가 실패")}

    thr = res["_thr"]
    real_dslopes = res["_on_tbl"]["dslope"].values

    out = {"탱크": tank_key, "pre/post(초)": f"{res['_pre']}/{res['_post']}", "임계값": round(thr, 3)}
    for sev in SEVERITIES:
        scaled = real_dslopes * (1 - sev)
        rate = float((scaled >= thr).mean() * 100)
        label = ("정상(원본)" if sev == 0 else
                 "완전무반응" if sev == 1 else
                 f"{int(sev*100)}%약화" if sev < 1 else
                 f"역행(원래크기의{int((sev-1)*100)}%로승온)")
        out[label] = round(rate, 1)
    return out


def run_injection_validation():
    rows = []
    for tk in TANKS:
        print(f"=== {tk} 가짜고장 주입 검증 중 ===")
        res = evaluate(tk)
        rows.append(injection_validation(tk, res))
    df = pd.DataFrame(rows)
    print("\n=== 가짜고장 주입 검증 — severity 별 검출률(%) ===")
    print(df.to_string(index=False))
    print("\n'정상(원본)' 열은 오탐률(낮을수록 좋음), 나머지는 검출률(높을수록 좋음 — 100%면 그 심각도를 다 잡음).")
    print("severity 가 커질수록(더 심한 고장일수록) 검출률이 단조증가해야 정상 — 안 그러면 방법 자체가 의심스럽다.")
    return df


def main():
    rows = []
    for tk in TANKS:
        print(f"=== {tk} 그리드서치 중 ===")
        rows.append(evaluate(tk))

    report_cols = ["탱크", "pre(초)", "post(초)", "ON이벤트수", "대조군수", "Cohen's d",
                    "임계값(Δ기울기)", "ON평균Δ기울기", "ON표준편차", "대조군평균Δ기울기",
                    "미검출률%(ON인데 임계값 못넘음)", "대조군오탐률%(설계상 ~5)", "비고"]
    df = pd.DataFrame(rows)
    print("\n=== 탱크별 최적 창 + 단일이벤트 판정 신뢰도 ===")
    if all(c in df.columns for c in report_cols):
        print(df[report_cols].to_string(index=False))
    else:
        print(df.to_string(index=False))
    print("\nCohen's d 해석: 0.2=작음 0.5=중간 0.8=큼 (ON 대조군 두 분포가 얼마나 갈라지는가).")
    print("미검출률이 높으면(예: 40%+) 그 탱크는 '무반응' 판정을 신뢰하기 어렵다 — 있는 그대로 보고.")
    return df


if __name__ == "__main__":
    main()
