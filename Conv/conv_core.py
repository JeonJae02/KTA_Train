"""컨베이어 분석 공용 함수 — 카운터로 절대 위치를 만들고 센서 엣지를 복원한다.

핵심 아이디어 (conv_01 §0 참고)
- DB 는 약 0.5초마다 바뀐 값만 기록한다. 0.5초보다 짧은 센서 펄스는 원본 비트로는 자주 놓친다.
- PLC 존 카운터(R000xx)는 엔코더 펄스마다 +1, 존 진입 센서의 상승엣지에서 1로 리셋된다.
  카운터는 거의 매 샘플 바뀌므로 DB 에 빠짐없이 남는다.
- 그래서 **카운터가 떨어진 샘플**을 찾으면 센서가 켜진 사실과 위치를 1카운트 정밀도로 되살릴 수 있다.
      리셋 위치 = 그 샘플의 절대 위치 − (카운터 값 − 1)

사용:
    from conv_core import load_group, add_position, counter_resets, bit_edges
    d = add_position(load_group("md_fwd", "2026-10-16"))
    ev = counter_resets(d, "R00055")          # 전진 진입 감지(P00451)의 모든 상승엣지
    ed = bit_edges(d, "P00452")               # 정위치 감지 원본 엣지(±0.5초 오차)
"""

import numpy as np
import pandas as pd

from extract_conv_cache import load_day, load_range

#: 존 카운터 → 리셋시키는 센서 (IL: Preparation.il, Air_Supply_Unit.il)
COUNTERS = {
    "R00040": ("P00140", "주입부 대차 진입 감지"),     # 실제 리셋은 Carrier_IN_CHK(P00140 0.3초 지속 후)
    "R00080": ("P00141", "주입부 대차 쉬프트 감지"),
    "R00070": ("P00164", "오프닝 존 진입 감지"),
    "R00030": ("P00154", "클로징 존 진입 감지"),
    "R00050": ("P00154", "에어백 유닛(클로징 센서 공유)"),
    "R00055": ("P00451", "전진 유닛 진입 감지"),
    "R00060": ("P00459", "후진 유닛 진입 감지"),
}

#: 카운터 값이 이만큼 이상 떨어지면 리셋으로 본다 (피치가 139 카운트라 정상 감소는 없다)
RESET_DROP = 50
#: 샘플 간격이 이보다 길면 DB 공백으로 보고 위치를 이어 붙이지 않는다
GAP_SEC = 5.0
#: 초당 카운트 = K_SPEED × Hz_In_Conv/10  (10/19 실측, 324–405 전 구간에서 일정)
K_SPEED = 0.2743


def load_group(group, start, end=None):
    """그룹 하루(또는 기간) 원본. 숫자 컬럼은 float64."""
    if end is None:
        df = load_day(group, start)
        if df is None:
            return pd.DataFrame()
        num = df.select_dtypes("number").columns
        df[num] = df[num].astype("float64")
        return df
    return load_range(group, start, end)


def add_position(df, counters=None):
    """절대 위치(pos, 카운트)와 연속 구간 번호(seg)를 붙인다.

    샘플마다 이동량 = 리셋이 아닌 카운터들의 변화량 중앙값. 역회전이면 음수가 된다.
    샘플 간격이 GAP_SEC 보다 길면 그 사이 이동량을 모르므로 seg 를 새로 시작한다.
    """
    df = df.copy()
    cs = [c for c in (counters or COUNTERS) if c in df.columns]
    d = df[cs].diff()
    inc = d.where(d.abs() < RESET_DROP).median(axis=1).fillna(0.0)
    dt = df.index.to_series().diff().dt.total_seconds()
    gap = (dt > GAP_SEC) | dt.isna()
    inc[gap] = 0.0
    df["seg"] = gap.cumsum().to_numpy()
    df["dpos"] = inc.to_numpy()
    df["pos"] = inc.cumsum().to_numpy()
    df["dt"] = dt.to_numpy()
    return df


def counter_resets(df, counter):
    """카운터 리셋 = 센서 상승엣지. 반환: 엣지마다 위치·추정 시각·대차번호·직전 피치.

    pos      : 센서가 켜진 절대 위치 (정확, 1카운트)
    t_est    : 추정 시각 = 리셋 직후 샘플 시각 − (값−1)/초당카운트
    pre_max  : 리셋 직전 마지막 샘플 값 (참고. 실제 피치는 pitch 사용)
    pitch    : 같은 센서 직전 엣지와의 거리 (같은 seg 안에서만)
    """
    x = df[counter]
    r = (x.diff() < -RESET_DROP) & (df["seg"] == df["seg"].shift(1))
    v = x[r]
    rate = (df["dpos"] / df["dt"]).rolling(20, min_periods=3).median()[r]
    out = pd.DataFrame({
        "t_row": df.index[r],
        "pos": (df["pos"][r] - (v - 1)).to_numpy(),
        "value": v.to_numpy(),
        "pre_max": x.shift(1)[r].to_numpy(),
        "seg": df["seg"][r].to_numpy(),
        "rate": rate.to_numpy(),
    })
    out["t_est"] = out["t_row"] - pd.to_timedelta((out["value"] - 1) / out["rate"], unit="s")
    if "Now_Actual_Wagon_Num" in df:
        out["wagon"] = df["Now_Actual_Wagon_Num"][r].to_numpy()
    same = out["seg"] == out["seg"].shift(1)
    out["pitch"] = out["pos"].diff().where(same)
    return out


def bit_edges(df, col):
    """원본 비트의 상승(+1)·하강(−1) 엣지. 0.5초 샘플링이라 짧은 펄스는 빠져 있다."""
    x = df[col].fillna(0).gt(0).astype(int)
    e = x.diff()
    m = e.isin([1, -1]) & (df["seg"] == df["seg"].shift(1))
    return pd.DataFrame({"t": df.index[m], "edge": e[m].astype(int).to_numpy(),
                         "pos": df["pos"][m].to_numpy(), "seg": df["seg"][m].to_numpy()})


def relative_to(events, ref_pos, max_dist=None):
    """events.pos 를 직전 기준 엣지(ref_pos, 정렬된 배열) 대비 거리로 바꾼다."""
    ref_pos = np.asarray(ref_pos)
    i = np.searchsorted(ref_pos, events["pos"].to_numpy(), side="right") - 1
    ok = i >= 0
    rel = np.full(len(events), np.nan)
    rel[ok] = events["pos"].to_numpy()[ok] - ref_pos[i[ok]]
    if max_dist is not None:
        rel[rel > max_dist] = np.nan
    return rel


def run_mask(df):
    """컨베이어 정회전 운전 중."""
    return df["Conv_Run_Out"].fillna(0).gt(0) & df["dpos"].ge(0)
