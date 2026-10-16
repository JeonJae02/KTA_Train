"""전진Unit 캐리어 스톱퍼CY — 사이클마다 전진감지 ON 길이와 전진→후진 전환 지연.

한 사이클(대차 1대, 약 13초):
  SOL ON → (0~0.5초) 전진감지 ON ── 약 2.6~3.1초 ── SOL OFF → (0~0.5초) 전진감지 OFF → (0~0.5초) 후진감지 ON ── 약 9.5초 ──
  후진감지 OFF → (0~0.5초) 다음 전진감지 ON

태그(공백 주의): 전진감지 = '전진Unit  캐리어  스톱퍼CY 전진감지', 후진감지 = '전진 Unit  캐리어 스톱퍼CY 후진감지'
(전진과 Unit 사이 공백), 명령 = '전진Unit 스톱퍼 SOL'. 받기: conv_tag_pull.py stopper_fwd

DB 는 약 0.52초 간격으로 기록하므로 시간은 ±0.5초 정밀도다. 길이 분포가 2.6 / 3.1 / 3.6초로 뭉치는 것은 이 샘플 간격 때문이다.
하루 평균을 내면 샘플 위상이 섞여서 0.5초보다 작은 변화도 보인다.

제외
  moving : 사이클(전진감지 ON 구간) 안에서 카운터가 2초 넘게 안 움직였으면 정지가 낀 것 → 제외
  db_gap : 사이클이나 전환 구간 안에서 DB 기록이 0.7초 넘게 비었으면 시각을 믿을 수 없음 → 제외
"""
import numpy as np
import pandas as pd

FWD = "전진Unit  캐리어  스톱퍼CY 전진감지"
BWD = "전진 Unit  캐리어 스톱퍼CY 후진감지"
SOL = "전진Unit 스톱퍼 SOL"
STALL_S = 2.0
DB_GAP_S = 0.7


def _edges(x):
    v = x.values.astype(int)
    d = np.diff(v)
    return x.index[1:][d == 1].values, x.index[1:][d == -1].values


def _after(arr, t):
    """t 이후(같은 시각 포함) 첫 값. 없으면 NaT."""
    k = np.searchsorted(arr, t)
    out = arr[np.clip(k, 0, len(arr) - 1)].copy()
    out[k >= len(arr)] = np.datetime64("NaT")
    return out


def stopper_cycles(S, df=None, cw=None):
    """사이클 1행 = 전진감지 ON 구간 1개.

    S  : 스톱퍼 태그 3개 원본(conv_tag_pull.load_days("stopper_fwd"))
    df : 카운터 원본(conv_wagon_reset_pull.load_days()) — 주면 정지/DB 끊김/대차 번호를 붙인다
    cw : conv_wagon_reset 모듈 (대차 번호용)
    """
    S = S[[FWD, BWD, SOL]].ffill().dropna()
    fr, ff = _edges(S[FWD]); br, bf = _edges(S[BWD]); sr, sf = _edges(S[SOL])
    i = np.searchsorted(ff, fr)
    ok = i < len(ff)
    r = pd.DataFrame({"on": fr[ok], "off": ff[i[ok]]})
    r = r[~r["off"].duplicated(keep="last")].reset_index(drop=True)
    r["on_s"] = (r["off"] - r["on"]).dt.total_seconds()                       # 전진감지 ON 길이
    r["bwd_on"] = _after(br, r["off"].values)
    r["fwd_off_to_bwd_on_s"] = (r["bwd_on"] - r["off"]).dt.total_seconds()    # 전진 OFF → 후진 ON
    r["bwd_off"] = _after(bf, r["bwd_on"].values)
    r["bwd_on_s"] = (r["bwd_off"] - r["bwd_on"]).dt.total_seconds()
    r["next_fwd_on"] = _after(fr, r["bwd_off"].values)
    r["bwd_off_to_fwd_on_s"] = (r["next_fwd_on"] - r["bwd_off"]).dt.total_seconds()
    k = np.searchsorted(sr, r["on"].values, side="right") - 1                 # 이 사이클을 연 SOL ON
    r["sol_on"] = sr[np.clip(k, 0, None)]
    r["sol_off"] = _after(sf, r["sol_on"].values)
    r["sol_s"] = (r["sol_off"] - r["sol_on"]).dt.total_seconds()
    r["sol_on_to_fwd_on_s"] = (r["on"] - r["sol_on"]).dt.total_seconds()
    r["sol_off_to_fwd_off_s"] = (r["off"] - r["sol_off"]).dt.total_seconds()
    r["date"] = r["on"].dt.strftime("%m-%d")
    if df is None:
        return r

    ti = df.index.values
    X = df[cw.CNT_TAGS].values
    mt = ti[np.r_[True, (np.diff(X, axis=0) != 0).any(axis=1)]]          # 카운터가 바뀐 시각
    allt = np.union1d(ti, S.index.values)                                # DB 에 기록이 있는 시각

    def max_gap(times, a, b):
        s = max(np.searchsorted(times, a) - 1, 0); e = np.searchsorted(times, b) + 1
        seg = np.r_[a, times[s:e], b] if times is mt else times[s:e]
        return (np.diff(seg) / np.timedelta64(1, "s")).max() if len(seg) > 1 else np.nan

    r["max_stall_s"] = [max_gap(mt, a, b) for a, b in zip(r["on"].values, r["off"].values)]
    r["db_gap_s"] = [max_gap(allt, a, b) for a, b in zip(r["on"].values, r["off"].values)]
    r["db_gap_trans_s"] = [max_gap(allt, a, b) if not pd.isna(b) else np.nan
                           for a, b in zip(r["off"].values, r["bwd_on"].values)]
    inside = (r["on"] >= df.index[0]) & (r["off"] <= df.index[-1])
    r["moving"] = inside & (r["max_stall_s"] <= STALL_S)
    r["ok_on"] = r["moving"] & (r["db_gap_s"] <= DB_GAP_S)
    r["ok_trans"] = r["moving"] & (r["db_gap_trans_s"] <= DB_GAP_S)
    # 대차 번호: 이 사이클 직전 전진Unit 진입감지 통과(실제 번호)
    cyc = cw.pour_cycles(df, cw.find_flats(df))
    b = cw.bursts(cw.counter_resets(df, "MD_CY_FWD", cyc)).sort_values("time")
    q = np.searchsorted(b["time"].values, r["on"].values) - 1
    r["wagon"] = np.where(q >= 0, b["wagon"].values[np.clip(q, 0, None)], -1)
    return r


def daily(r):
    """날짜별 평균·분위. 평균은 0.5초 샘플 위상이 섞여서 작은 변화도 보인다."""
    a = r[r["ok_on"]].groupby("date").agg(
        사이클=("on_s", "size"), ON_평균=("on_s", "mean"), ON_p99=("on_s", lambda x: x.quantile(.99)), ON_최대=("on_s", "max"),
        SOL_평균=("sol_s", "mean"), SOL에서_전진ON=("sol_on_to_fwd_on_s", "mean"), SOL에서_전진OFF=("sol_off_to_fwd_off_s", "mean"))
    b = r[r["ok_trans"]].groupby("date").agg(
        전진OFF_후진ON_평균=("fwd_off_to_bwd_on_s", "mean"), 전진OFF_후진ON_최대=("fwd_off_to_bwd_on_s", "max"))
    return a.join(b)
