"""탱크 신호 전처리.

진단 모델(`Tank_Diagnosis_Model.py`)에 넣기 전에 원시 PLC 신호를 정리한다.
여기서 하는 일은 네 가지다.

1. **온도 스케일 변환** — `TK_Temp_*` 는 0.1℃ 단위라 10 으로 나눠 ℃ 로 만든다.
2. **센서 글리치 제거** — PV 가 0℃ 나 327℃ 처럼 물리적으로 불가능한 값으로 튀는
   경우가 있다(2026-09-09 실측). 안 거르면 heat 진단이 오판정된다.
3. **exchanger 채터링 병합** — 핵심. 아래 설명 참고.
4. **cool 동반 여부 표시** — 사이클마다 cool 이 같이 켜졌는지.

## 채터링 병합을 왜 하는가

exchanger 는 온도가 SV 에 닿으면 켜지는데, **그 문턱에서 센서값이 229-230-231-230
식으로 들썩인다.** 그래서 원시 신호를 그대로 보면 0.5~3초짜리 ON/OFF 가 수십 번
반복되는 것처럼 보인다 — 실제로는 한 번의 냉각 동작인데 수십 개 조각으로 쪼개져 있다.

실측(P1, 2026-09-09~10-02)에서 OFF 길이 분포를 보면 두 덩어리로 완전히 갈린다.

    짧은 쪽 (채터링)   0.5 ~ 55.7초     90%
    [ 55.7초 ~ 402.1초 사이는 단 한 건도 없음 ]
    긴 쪽 (진짜 OFF)   402.1초 ~        10%

이 빈 구간 안이면 어떤 값을 임계로 잡아도 결과가 같다. 기본값은 **90초**.

## 잘린 첫 사이클

데이터가 **exchanger 가 이미 켜져 있는 도중부터** 시작하면, 그 사이클은 언제 시작했는지
알 수 없어 길이를 믿을 수 없다(실측에서 5.8시간짜리 가짜 사이클이 만들어졌다).
그래서 ON 시작은 **데이터 안에서 0→1 전환이 실제로 관측된 것만** 인정한다.
첫 샘플이 이미 1 이면 그 사이클은 자동으로 버려진다.
"""

import glob
import os

import numpy as np
import pandas as pd

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(ROOT_DIR, "parkkt", "extracted_csv")

# --------------------------------------------------------------------------- #
# 탱크별 태그
# --------------------------------------------------------------------------- #
#: 한글 태그는 탱크마다 공백 개수까지 제각각이라(예: "워킹 Tank 2-1 SOFT  Heater" 는
#: 스페이스 2개) InfluxDB 에 찍힌 그대로 적어야 한다. "정리"하면 태그를 못 찾는다.
#: I2 는 cool/exchanger 가 "Tank 2-2 MDI", heat/feeding 은 "Tank 2 MDI" 로 이름이 다르다.
_EXCH_COOL = {
    "P1": ("워킹 Tank 1 BACK Exchanger SOL",    "워킹 Tank 1 BACK Cooling SOL"),
    "P2": ("워킹 Tank 2-1 SOFT Exchanger SOL",  "워킹 Tank 2-1 SOFT Cooling SOL"),
    "P3": ("워킹 Tank 3-1 고탄성 Exchanger SOL", "워킹 Tank 3-1 고탄성 Cooling SOL"),
    "P4": ("워킹 Tank 2-2 HARD Exchanger SOL",  "워킹 Tank 2-2 HARD Cooling SOL"),
    "P5": ("워킹 Tank 3-2 CUSH Exchanger SOL",  "워킹 Tank 3-2 CUSH Cooling SOL"),
    "I1": ("워킹 Tank 1 ISO Exchanger SOL",     "워킹 Tank 1 ISO Cooling SOL"),
    "I2": ("워킹 Tank 2-2 MDI Exchanger SOL",   "워킹 Tank 2-2 MDI Cooling SOL"),
    "I3": ("워킹 Tank 3  ISO Exchanger SOL",    "워킹 Tank 3  ISO Cooling SOL"),
}

#: TK_Temp_* 는 탱크 키와 1:1 이라 자동 생성한다.
TANKS = {
    key: {"exch": exch, "cool": cool,
          "pv": f"TK_Temp_PV_{key}", "lset": f"TK_Temp_L_Set_{key}"}
    for key, (exch, cool) in _EXCH_COOL.items()
}

# --------------------------------------------------------------------------- #
# 전처리 파라미터
# --------------------------------------------------------------------------- #
TEMP_SCALE = 10.0               #: 0.1℃ 단위 -> ℃
MERGE_GAP_SEC = 90              #: 이보다 짧은 OFF 는 같은 사이클로 이어붙인다
PV_VALID_RANGE = (5.0, 60.0)    #: ℃. 이 범위 밖 PV 는 센서 글리치로 보고 버린다


# --------------------------------------------------------------------------- #
# 데이터 로드
# --------------------------------------------------------------------------- #

def _latest_csv_with(tag, raw_dir=RAW_DIR):
    for p in sorted(glob.glob(os.path.join(raw_dir, "*_analysis.csv")), reverse=True):
        try:
            cols = pd.read_csv(p, nrows=0).columns
        except Exception:
            continue
        if tag in cols:
            return p
    return None


def _read_csv_tag(path, tag):
    s = pd.read_csv(path, index_col="Time", parse_dates=True, usecols=["Time", tag])
    s = s[~s.index.duplicated(keep="last")].sort_index()
    return s[tag].dropna()


def load_signals(tank, start, end, use_cache=True, raw_dir=RAW_DIR, env_path=None):
    """탱크 1개의 원시 신호 4개를 dict(exch, cool, pv, lset) 으로 읽는다.

    캐시 CSV 가 있으면 쓰고, 없는 태그만 InfluxDB 에서 받는다.
    **이미 전처리된 데이터를 따로 갖고 있다면 이 함수는 안 써도 된다** —
    아래 `preprocess_signals()` 에 Series 4개를 직접 넘기면 된다.
    """
    want = TANKS[tank]
    out, missing = {}, []

    if use_cache:
        for key, tag in want.items():
            p = _latest_csv_with(tag, raw_dir)
            if p:
                out[key] = _read_csv_tag(p, tag)
            else:
                missing.append(tag)
    else:
        missing = list(want.values())

    if missing:
        import sys
        sys.path.insert(0, ROOT_DIR)
        from Log_Extractor import LogExtractor
        ex = LogExtractor(env_path=env_path or os.path.join(ROOT_DIR, ".env"))
        df = ex.get_data(start_time=start, end_time=end, target_tags=missing)
        ex.save_to_csv(df, save_dir=raw_dir)
        for key, tag in want.items():
            if tag in missing:
                out[key] = df[tag].dropna()

    s = pd.Timestamp(start)
    e = pd.Timestamp.now() if end == "now()" else pd.Timestamp(end)
    return {k: v.loc[s:e] for k, v in out.items()}


# --------------------------------------------------------------------------- #
# 1~2. 온도 스케일 + 글리치
# --------------------------------------------------------------------------- #

def clean_temperature(pv, lset, scale=TEMP_SCALE, valid_range=PV_VALID_RANGE):
    """0.1℃ -> ℃ 변환 후, PV 에서 물리적으로 불가능한 값을 버린다.

    반환: (pv_clean, lset_clean, 버린 개수)
    """
    pv = pv / scale
    lset = lset / scale
    lo, hi = valid_range
    n_before = len(pv)
    pv = pv[(pv >= lo) & (pv <= hi)]
    return pv, lset, n_before - len(pv)


# --------------------------------------------------------------------------- #
# 3. exchanger 채터링 병합
# --------------------------------------------------------------------------- #

def raw_segments(sig01):
    """0/1 신호에서 (ON 시작, OFF 시작) 구간 목록.

    ON 시작은 **0 -> 1 전환이 데이터 안에서 실제로 보인 것만** 인정한다.
    첫 샘플이 이미 1 이면(= 켜진 도중부터 데이터가 시작) 그 사이클은 시작 시각을
    알 수 없으므로 버린다.
    """
    s = sig01[sig01.diff() != 0]
    ch = pd.DataFrame({"val": s})
    ch["prev"] = ch["val"].shift(1)
    on_starts = ch.index[(ch["prev"] == 0) & (ch["val"] == 1)]
    off_starts = ch.index[(ch["prev"] == 1) & (ch["val"] == 0)]

    segs, oi = [], 0
    for on_t in on_starts:
        while oi < len(off_starts) and off_starts[oi] <= on_t:
            oi += 1
        if oi >= len(off_starts):
            break
        segs.append((on_t, off_starts[oi]))
    return segs


def merge_cycles(exch, gap_sec=MERGE_GAP_SEC):
    """채터링을 합쳐 '논리적 사이클' 표를 만든다.

    컬럼: cycle_start, cycle_end, next_on, off_duration_sec, n_bridged, on_duration_sec
      - n_bridged         : 이 사이클 안에서 이어붙인(=무시한) 짧은 OFF 개수
      - off_duration_sec  : 이 사이클이 꺼진 뒤 다음 ON 까지의 OFF 길이
    """
    segs = raw_segments(exch)
    cols = ["cycle_start", "cycle_end", "next_on", "off_duration_sec",
            "n_bridged", "on_duration_sec"]
    if not segs:
        return pd.DataFrame(columns=cols)

    rows, cycle_start, n_bridged = [], segs[0][0], 0
    for i in range(len(segs) - 1):
        gap = (segs[i + 1][0] - segs[i][1]).total_seconds()
        if gap < gap_sec:
            n_bridged += 1
            continue
        rows.append({"cycle_start": cycle_start, "cycle_end": segs[i][1],
                     "next_on": segs[i + 1][0], "off_duration_sec": gap,
                     "n_bridged": n_bridged})
        cycle_start, n_bridged = segs[i + 1][0], 0
    rows.append({"cycle_start": cycle_start, "cycle_end": segs[-1][1],
                 "next_on": pd.NaT, "off_duration_sec": np.nan, "n_bridged": n_bridged})

    df = pd.DataFrame(rows)
    df["on_duration_sec"] = (df["cycle_end"] - df["cycle_start"]).dt.total_seconds()
    return df[cols]


# --------------------------------------------------------------------------- #
# 4. cool 동반 여부
# --------------------------------------------------------------------------- #

def flag_cool(cycles, cool):
    """각 사이클 구간 동안 cool 이 한 번이라도 1 이었는가."""
    cool = cool.sort_index()
    flags = []
    for _, r in cycles.iterrows():
        seg = cool.loc[r["cycle_start"]:r["cycle_end"]]
        before = cool.loc[:r["cycle_start"]]
        start_val = before.iloc[-1] if len(before) else 0
        flags.append(bool((seg == 1).any() or start_val == 1))
    cycles = cycles.copy()
    cycles["cool_overlap"] = flags
    return cycles


# --------------------------------------------------------------------------- #
# 통합 진입점
# --------------------------------------------------------------------------- #

def preprocess_signals(exch, cool, pv, lset, gap_sec=MERGE_GAP_SEC,
                       scale=TEMP_SCALE, valid_range=PV_VALID_RANGE, verbose=False):
    """Series 4개 -> (사이클 표, 정리된 pv, 정리된 lset).

    이미 받아둔 데이터로 바로 돌릴 때 쓰는 진입점.
    """
    pv_c, lset_c, n_glitch = clean_temperature(pv, lset, scale, valid_range)
    cycles = merge_cycles(exch, gap_sec)
    cycles = flag_cool(cycles, cool)

    if verbose:
        first_val = exch.iloc[0] if len(exch) else None
        print(f"PV 글리치 제거 {n_glitch}개 (허용 {valid_range[0]}~{valid_range[1]}℃)")
        if first_val == 1:
            print("데이터가 exchanger 켜진 도중부터 시작 -> 첫 사이클 버림")
        print(f"raw ON {len(raw_segments(exch)):,}개 -> 병합 사이클 {len(cycles):,}개 "
              f"(cool 동반 {int(cycles['cool_overlap'].sum())}개)")
    return cycles, pv_c, lset_c


def preprocess(tank, start, end, use_cache=True, verbose=False, **kw):
    """태그 로드까지 포함한 전처리 진입점."""
    sig = load_signals(tank, start, end, use_cache=use_cache, **kw)
    cycles, pv, lset = preprocess_signals(sig["exch"], sig["cool"], sig["pv"],
                                          sig["lset"], verbose=verbose)
    return {"cycles": cycles, "pv": pv, "lset": lset,
            "exch": sig["exch"], "cool": sig["cool"]}


if __name__ == "__main__":
    data = preprocess("P1", "2026-09-09 10:00:00", "now()", verbose=True)
    print(data["cycles"].head().to_string())
