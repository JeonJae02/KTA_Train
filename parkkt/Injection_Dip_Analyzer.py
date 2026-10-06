# parkkt/Injection_Dip_Analyzer.py
"""사출(Shot_Injection_HD1_P1) 유무에 따른 압력(PT) 거동 분석.

`Cycle_Feature_Extractor.extract_cycle_features` 의 `Unexplained_PT_Dip_Count/_Max`
피처는 사이클 1행당 합계/최댓값만 남긴다. 여기서는 그 판정 로직을 **틱 단위로 그대로
재현**해서 - 언제, 몇 도씩, 어느 사이클에서 "사출 없이 압력이 떨어졌는지" 낱개로 뽑고,
사출이 실제로 일어날 때의 압력 하락(정상 거동)과 비교할 수 있게 한다.

판정 상수(DIP_THRESHOLD_PCT, INJECTION_DIP_TICKS, STEADY_TICK_MIN)는
Cycle_Feature_Extractor 와 반드시 같은 값을 써야 한다 - 갈라지면 여기서 뽑은 "이상"이
그 피처가 세는 것과 달라진다.
"""
import numpy as np
import pandas as pd

from Cycle_Feature_Extractor import (
    PUMP_MAP, DIP_THRESHOLD_PCT, INJECTION_DIP_TICKS, STEADY_TICK_MIN, RAMP_THRESHOLD_PCT,
)


def _tags(pump_id):
    info = PUMP_MAP[pump_id]
    return {
        "PT": f"Scale_Out___PT_{pump_id}",
        "FT": f"Scale_Out___FT_{pump_id}",
        "BU": f"Pump_BuildUp_{pump_id}",
        "Inj": f"Shot_Injection_{info['head']}_P{info['slot']}",
    }


def _foaming_cols(pump_id, df):
    """이 펌프가 속한 헤드의 `{head}_FOAMING_SHOT1~6` 중 실제 raw_df에 있는 것만.

    **헤드 공용**이다 - 같은 헤드의 다른 슬롯(P1/P2 등)이 쏠 때도 같이 켜진다.
    실측(HD1, P1/P2): 한 틱에 SHOT1~6 중 최대 1개만 켜지고(상호배타), 이 헤드의
    "사출 에피소드"(접근→사출→감쇠, 때로는 그 이후까지) 33,951건 중 95.9%가
    P1·P2 양쪽 사출을 한 에피소드 안에 같이 물고 있었다 - 즉 대부분 P1+P2가
    한 박자 차이로 같이 쏘는 결합 샷이라 "이 펌프 것"으로 못 가른다. 그래서
    사출 탐지 목적으로는 펌프별 `Shot_Injection_*` 대신 이걸 켜켜이 더 넓게
    "설명된 구간"으로 쓴다 (아래 Foaming_Active).
    """
    info = PUMP_MAP[pump_id]
    cols = [f"{info['head']}_FOAMING_SHOT{k}" for k in range(1, 7)]
    return [c for c in cols if c in df.columns]


def prep(raw_df, pump_id):
    """raw_df(=LogExtractor 결과류) -> Time 컬럼 있는 df, Cycle_ID, Tick_Index,
    Shot_Owned_Active, Foaming_Active 부여.

    `Cycle_Feature_Extractor.extract_cycle_features` 1~2단계와 동일한 규칙.
    """
    t = _tags(pump_id)
    df = raw_df.ffill().fillna(0).reset_index()
    df = df.rename(columns={df.columns[0]: "Time"})

    buildup = df[t["BU"]].astype(int)
    df["Cycle_ID"] = (buildup.diff().fillna(0) != 0).cumsum()

    inj = (df[t["Inj"]] != 0)
    spread = inj.copy()
    for k in range(1, INJECTION_DIP_TICKS + 1):
        spread |= inj.shift(k, fill_value=False)
    df["Shot_Owned_Active"] = spread.astype(int)

    fcols = _foaming_cols(pump_id, df)
    df["Foaming_Active"] = (df[fcols].sum(axis=1) > 0).astype(int) if fcols else 0
    df["_buildup"] = buildup
    return df, t


def unexplained_pt_dip_events(raw_df, pump_id):
    """사출로 설명 안 되는 PT 하락 틱을 낱개로 반환.

    반환 컬럼: Time, Date, Cycle_ID, Tick_Index, Ticks_From_End, PT_Now, PT_Prev_Roll,
    Drop_Abs, Drop_Pct(%), Foaming_Active_Now, SV_Changed_Recent, Near_Cycle_End,
    Strong_Candidate.

    **2026-09-28 수정**: 처음엔 "이 펌프 자신의 Shot_Injection_* 신호(+4틱)가 없다"만
    보고 15,548건을 "설명 안 됨"으로 잡았었다. 그런데 이 헤드의 `{head}_FOAMING_SHOT1~6`
    (접근→사출→감쇠를 아우르는, 헤드 공용 4단계 상태)를 겹쳐보니 그중 **99.2%가
    이미 이 FOAMING 단계 안에서 일어난 것**이었다 - 자기 펌프의 사출 신호 window
    밖이었을 뿐, 같은 헤드의 다른 슬롯(짝 펌프)이 쏘는 접근/사출/감쇠 여파였던 것.
    (판정 대상 모집단 전체의 FOAMING 기저비율은 79.6%인데, 걸린 것들은 99.2% - 즉
    FOAMING 중일 때 판정될 확률이 안 켜졌을 때보다 약 31배 높다.) 그래서 이제
    Foaming_Active도 Shot_Owned_Active와 동급으로 "설명된 구간"에 넣는다 - 이걸
    넣기 전엔 15,548건이었던 게 **127건**으로, 거기서 SV전환/사이클끝까지 마저 빼면
    **14건**(13일치 통틀어)까지 줄어든다. 즉 처음 잡았던 "누출 의심"은 사실상 전부
    같은 헤드를 쓰는 짝 펌프의 사출/감쇠였고, 진짜 남는 건 거의 없다.

    나머지 두 플래그(참고용, 위 Foaming_Active 배제 이후에는 영향이 작다):
      - SV_Changed_Recent: 직전 3틱 안에 SV(설정 유량) 자체가 바뀜 -> 레시피/구간 전환.
      - Near_Cycle_End: 이 사이클의 마지막 2틱 이내 -> 펌프가 꺼지기 직전, 감쇠가 이미
        시작된 구간.
    Strong_Candidate = Foaming_Active_Now 도 아니고 위 둘도 아닌 경우 - 최종적으로
    남는 진짜 "설명 안 되는" 후보.

    `Cum_FT_Error` 등과 달리 이건 **원시 판정 자체**라서, cyc 데이터프레임의
    Unexplained_PT_Dip_Count(이 모듈이 새로 고치기 전 정의)를 Cycle_ID 로
    groupby.size() 하면 Foaming_Active를 넣기 전 값과 일치해야 한다.
    """
    t = _tags(pump_id)
    df, _ = prep(raw_df, pump_id)

    df_on = df[df["_buildup"] == 1].sort_values("Time").copy()
    df_on["Tick_Index"] = df_on.groupby("Cycle_ID").cumcount()
    df_on["Cycle_Len"] = df_on.groupby("Cycle_ID")["Tick_Index"].transform("size")
    df_on["Ticks_From_End"] = df_on["Cycle_Len"] - 1 - df_on["Tick_Index"]

    pt_roll_cur = df_on.groupby("Cycle_ID")[t["PT"]].transform(
        lambda s: s.rolling(3, min_periods=1).mean())
    pt_roll = pd.Series(pt_roll_cur.values, index=df_on.index).groupby(df_on["Cycle_ID"]).shift(1)

    sv_tag = f"g_s_SV_{pump_id}"
    sv_changed = df_on.groupby("Cycle_ID")[sv_tag].transform(
        lambda s: (s.diff() != 0).rolling(3, min_periods=1).max())

    pt_drop_pct = (pt_roll - df_on[t["PT"]]) / pt_roll.replace(0, np.nan)
    no_shot = df_on["Shot_Owned_Active"] == 0
    no_foaming = df_on["Foaming_Active"] == 0
    steady_mask = df_on["Tick_Index"] >= STEADY_TICK_MIN
    unexplained = (pt_drop_pct >= DIP_THRESHOLD_PCT) & no_shot & no_foaming & steady_mask

    ev = df_on.loc[unexplained, ["Time", "Cycle_ID", "Tick_Index", "Ticks_From_End", t["PT"]]].copy()
    ev["PT_Prev_Roll"] = pt_roll[unexplained]
    ev["Drop_Pct"] = pt_drop_pct[unexplained] * 100
    ev = ev.rename(columns={t["PT"]: "PT_Now"})
    ev["Drop_Abs"] = ev["PT_Prev_Roll"] - ev["PT_Now"]
    ev["Date"] = ev["Time"].dt.date
    ev["Foaming_Active_Now"] = False  # 정의상 이미 전부 배제됨 - 참고용으로 명시만 해둔다
    ev["SV_Changed_Recent"] = sv_changed[unexplained].astype(bool).values
    # 사이클 끝나기 직전(마지막 2틱 이내)이면 이미 감쇠(Decay)가 시작된 거지 누출이 아닐 수 있다.
    ev["Near_Cycle_End"] = ev["Ticks_From_End"] <= 2
    ev["Strong_Candidate"] = ~(ev["SV_Changed_Recent"] | ev["Near_Cycle_End"])
    ev = ev.sort_values("Time").reset_index(drop=True)
    return ev[["Time", "Date", "Cycle_ID", "Tick_Index", "Ticks_From_End", "PT_Now",
               "PT_Prev_Roll", "Drop_Abs", "Drop_Pct", "Foaming_Active_Now",
               "SV_Changed_Recent", "Near_Cycle_End", "Strong_Candidate"]]


def unexplained_pt_dip_events_legacy(raw_df, pump_id):
    """Foaming_Active 배제를 넣기 전의 원래 판정(15,548건 쪽) - 비교/재현용으로만 남겨둔다."""
    t = _tags(pump_id)
    df, _ = prep(raw_df, pump_id)

    df_on = df[df["_buildup"] == 1].sort_values("Time").copy()
    df_on["Tick_Index"] = df_on.groupby("Cycle_ID").cumcount()
    df_on["Cycle_Len"] = df_on.groupby("Cycle_ID")["Tick_Index"].transform("size")
    df_on["Ticks_From_End"] = df_on["Cycle_Len"] - 1 - df_on["Tick_Index"]

    pt_roll_cur = df_on.groupby("Cycle_ID")[t["PT"]].transform(
        lambda s: s.rolling(3, min_periods=1).mean())
    pt_roll = pd.Series(pt_roll_cur.values, index=df_on.index).groupby(df_on["Cycle_ID"]).shift(1)

    sv_tag = f"g_s_SV_{pump_id}"
    sv_changed = df_on.groupby("Cycle_ID")[sv_tag].transform(
        lambda s: (s.diff() != 0).rolling(3, min_periods=1).max())

    pt_drop_pct = (pt_roll - df_on[t["PT"]]) / pt_roll.replace(0, np.nan)
    no_shot = df_on["Shot_Owned_Active"] == 0
    steady_mask = df_on["Tick_Index"] >= STEADY_TICK_MIN
    unexplained = (pt_drop_pct >= DIP_THRESHOLD_PCT) & no_shot & steady_mask

    ev = df_on.loc[unexplained, ["Time", "Cycle_ID", "Tick_Index", "Ticks_From_End", t["PT"]]].copy()
    ev["PT_Prev_Roll"] = pt_roll[unexplained]
    ev["Drop_Pct"] = pt_drop_pct[unexplained] * 100
    ev = ev.rename(columns={t["PT"]: "PT_Now"})
    ev["Drop_Abs"] = ev["PT_Prev_Roll"] - ev["PT_Now"]
    ev["Date"] = ev["Time"].dt.date
    foaming_now = df_on["Foaming_Active"].astype(bool)
    ev["Foaming_Active_Now"] = foaming_now[unexplained].values
    ev["SV_Changed_Recent"] = sv_changed[unexplained].astype(bool).values
    ev["Near_Cycle_End"] = ev["Ticks_From_End"] <= 2
    ev["Strong_Candidate"] = ~(ev["Foaming_Active_Now"] | ev["SV_Changed_Recent"] | ev["Near_Cycle_End"])
    ev = ev.sort_values("Time").reset_index(drop=True)
    return ev[["Time", "Date", "Cycle_ID", "Tick_Index", "Ticks_From_End", "PT_Now",
               "PT_Prev_Roll", "Drop_Abs", "Drop_Pct", "Foaming_Active_Now",
               "SV_Changed_Recent", "Near_Cycle_End", "Strong_Candidate"]]


def injection_events(raw_df, pump_id, before_ticks=3, after_ticks=INJECTION_DIP_TICKS):
    """사출 시작(0->on) 마다: 직전 PT 평균, 이후 최저 PT, 하락폭/하락률.

    사출 신호 자체 기준(전체 df, buildup 여부 무관 - 5-2절 실측 방식과 동일)으로 찾는다.

    **주의(before_ticks 민감도)**: 직전 몇 틱을 baseline으로 볼지에 따라 결과가 크게
    바뀐다. 실측(P1): before_ticks=1이면 "하락 없음"이 0.08%인데, before_ticks=5면
    34.6%까지 뛴다. 원인은 사이클 시작 직후(증압 중, Tick_Index가 낮을 때) 일어나는
    사출은 baseline 창이 아직 증압 중인(낮은) 값을 걸치게 되어 "하락"처럼 안 보이기
    때문 - `Tick_Index` 컬럼을 같이 반환하니 이 문제를 직접 확인/필터링할 수 있다.
    """
    df, t = prep(raw_df, pump_id)
    df_on = df[df["_buildup"] == 1].sort_values("Time").copy()
    df_on["Tick_Index"] = df_on.groupby("Cycle_ID").cumcount()
    tick_index_map = df_on.set_index("Time")["Tick_Index"]

    inj = (df[t["Inj"]] != 0).values
    pt_all = df[t["PT"]].values
    ft_all = df[t["FT"]].values
    times = df["Time"].values

    starts = np.where(inj & ~np.r_[False, inj[:-1]])[0]
    ends = np.where(inj & ~np.r_[inj[1:], False])[0]

    durations = np.empty(len(starts))
    for i, s in enumerate(starts):
        cand = ends[ends >= s]
        durations[i] = (cand[0] - s + 1) if len(cand) else 1

    pt_before = np.array([pt_all[max(0, s - before_ticks):s].mean() if s > 0 else pt_all[s]
                           for s in starts])
    ft_before = np.array([ft_all[max(0, s - before_ticks):s].mean() if s > 0 else ft_all[s]
                           for s in starts])
    pt_min_after = np.array([pt_all[s:min(len(pt_all), s + after_ticks + 1)].min() for s in starts])

    drop_abs = pt_before - pt_min_after
    drop_pct = np.where(pt_before != 0, drop_abs / pt_before * 100, 0.0)

    ev = pd.DataFrame({
        "Time": pd.to_datetime(times[starts]),
        "Signal_Ticks": durations,
        "PT_Before": pt_before,
        "FT_Before": ft_before,
        "PT_Min_After": pt_min_after,
        "Drop_Abs": drop_abs,
        "Drop_Pct": drop_pct,
    })
    ev["Date"] = ev["Time"].dt.date
    ev["Tick_Index"] = ev["Time"].map(tick_index_map).values
    return ev, starts, pt_all, ft_all, times


def settle_ticks(raw_df, pump_id):
    """SV(설정 유량) 목표가 바뀔 때마다(사이클 시작 포함) - 몇 틱만에 FT가 그 목표의
    RAMP_THRESHOLD_PCT(5%) 안으로 들어오는지.

    Kind:
      - "ramp_start": 사이클 첫 틱(Tick_Index==0) - 증압 완료까지.
      - "sv_change": 사이클 중간에 SV 자체가 바뀐 시점 - 레시피/구간 전환 후 재안정.
    Settled=False 면 사이클이 끝날 때까지 못 들어온 것(censored) - Ticks_To_Settle 은
    "사이클 끝까지 남은 틱 수"로 대체해 하한선만 알려준다.
    """
    t = _tags(pump_id)
    df, _ = prep(raw_df, pump_id)
    df_on = df[df["_buildup"] == 1].sort_values("Time").copy()
    df_on["Tick_Index"] = df_on.groupby("Cycle_ID").cumcount()

    sv_tag = f"g_s_SV_{pump_id}"
    rows = []
    for cid, seg in df_on.groupby("Cycle_ID", sort=True):
        seg = seg.reset_index(drop=True)
        sv = seg[sv_tag].values
        ft = seg[t["FT"]].values
        n = len(seg)

        change_idx = sorted(set([0] + list(np.where(np.diff(sv) != 0)[0] + 1)))
        for k in change_idx:
            target = sv[k] if sv[k] != 0 else (sv[k:].max() if n > k else 0)
            if not target:
                continue
            settled = np.where(np.abs(ft[k:] - target) / target <= RAMP_THRESHOLD_PCT)[0]
            kind = "ramp_start" if k == 0 else "sv_change"
            if len(settled):
                rows.append((cid, k, kind, target, int(settled[0]), True))
            else:
                rows.append((cid, k, kind, target, n - k, False))

    return pd.DataFrame(rows, columns=["Cycle_ID", "Tick_At_Change", "Kind", "Target_SV",
                                        "Ticks_To_Settle", "Settled"])


def injection_spacing(raw_df, pump_id):
    """사출과 사출 사이 틱 간격 + 사이클 내 사출 시작 위치(Tick_Index) + 사이클 길이.

    반환: dict(n_inj_per_cycle, gaps, start_tick_index, cycle_len) - 전부 numpy/Series.
    """
    t = _tags(pump_id)
    df, _ = prep(raw_df, pump_id)
    df_on = df[df["_buildup"] == 1].sort_values("Time").copy()
    df_on["Tick_Index"] = df_on.groupby("Cycle_ID").cumcount()
    df_on["Cycle_Len"] = df_on.groupby("Cycle_ID")["Tick_Index"].transform("size")

    inj = (df_on[t["Inj"]] != 0).astype(int)
    rising = (inj.diff().fillna(inj.iloc[0]) == 1)
    df_on = df_on.assign(inj_start=rising.values)

    n_inj_per_cycle = df_on.groupby("Cycle_ID")["inj_start"].sum()

    gaps = []
    for cid, g in df_on.groupby("Cycle_ID"):
        ticks = g.loc[g["inj_start"], "Tick_Index"].values
        if len(ticks) >= 2:
            gaps.extend(np.diff(ticks))

    single_cids = n_inj_per_cycle[n_inj_per_cycle == 1].index
    start_tick_index = df_on[df_on["Cycle_ID"].isin(single_cids) & df_on["inj_start"]]["Tick_Index"].values

    cycle_len = df_on.groupby("Cycle_ID")["Cycle_Len"].first()

    return {
        "n_inj_per_cycle": n_inj_per_cycle,
        "gaps": np.array(gaps),
        "start_tick_index": start_tick_index,
        "cycle_len": cycle_len,
    }


def injection_aligned_profile(pt_all, ft_all, starts, offsets=range(-3, 9)):
    """사출 시작 기준 틱 오프셋별 PT/FT 평균 곡선 + 개별 사례 몇 개(스파게티용)."""
    offs = list(offsets)
    pt_prof = np.array([pt_all[np.clip(starts + o, 0, len(pt_all) - 1)].mean() for o in offs])
    ft_prof = np.array([ft_all[np.clip(starts + o, 0, len(ft_all) - 1)].mean() for o in offs])
    return offs, pt_prof, ft_prof
