# parkkt/Shot_Feature_Extractor.py
"""Shot-level(= 사출 1회) 피처 추출.

사이클 단위 모델은 8초짜리 사이클을 숫자 하나로 요약하기 때문에, 그 안에서 일어난
**개별 사출의 이상**은 평균에 묻힌다. 이 모듈은 사출 한 건을 1행으로 만든다.

사출 시점은 `Shot_Injection_HD{head}_P{slot}` 의 0->1 전이로 잡는다.
(`Shot_P{n}_SEL_HD{m}` 은 "선택됨" 구간이라 실제 사출보다 2배 넓고,
 `HD{n}_FOAMING_SHOT{k}` 는 헤드 공용이라 어느 펌프 것인지 구분이 안 된다.)

실측한 사출 시 거동 (P1, 3,265건 평균):

    오프셋   PT       FT
      -1   105.9    2204.9     <- 기준선
       0   102.4    2316.4     사출 시작
      +1    96.5    2347.5     PT 최저 (-9.4)
      +2    99.8    2355.2
      +3   105.1    2349.8     거의 회복
      +4   105.7    2339.3     완전 회복

압력은 떨어졌다 3~4틱 만에 돌아오고, 유량은 반대로 올라간다(사출하니까).
딥이 유독 깊거나 회복이 느리면 노즐·밸브 쪽 이상을 의심할 수 있다.
"""
import numpy as np
import pandas as pd

from Cycle_Feature_Extractor import PUMP_MAP, _in_daily_exclude_window

#: 기준선을 잡을 때 사출 직전 몇 틱을 평균낼지
BASELINE_TICKS = 3
#: 딥과 회복을 관찰할 창 (실측상 +4틱이면 회복 완료)
DIP_WINDOW_TICKS = 8
#: 회복 판정 기준 - 기준선의 이 비율까지 돌아오면 회복으로 본다
RECOVERY_RATIO = 0.97


def extract_shot_features(raw_df, pump_id):
    """raw_df: LogExtractor.get_data() 결과. 반환: 사출 1건당 1행."""
    info = PUMP_MAP[pump_id]
    tag_PT = f"Scale_Out___PT_{pump_id}"
    tag_FT = f"Scale_Out___FT_{pump_id}"
    tag_SV = f"g_s_SV_{pump_id}"
    tag_BU = f"Pump_BuildUp_{pump_id}"
    tag_Hz = f"Hz_Out_{pump_id}"
    tag_Inj = f"Shot_Injection_{info['head']}_P{info['slot']}"

    df = raw_df.ffill().fillna(0).reset_index()
    df = df.rename(columns={df.columns[0]: "Time"})
    if tag_Inj not in df.columns:
        raise KeyError(f"{tag_Inj} 태그가 없습니다")

    bu = df[tag_BU].astype(int)
    df["Cycle_ID"] = (bu.diff().fillna(0) != 0).cumsum()

    inj = (df[tag_Inj] != 0).to_numpy()
    starts = np.where(inj & ~np.r_[False, inj[:-1]])[0]

    t = df["Time"].to_numpy()
    pt = df[tag_PT].to_numpy(dtype=float)
    ft = df[tag_FT].to_numpy(dtype=float)
    sv = df[tag_SV].to_numpy(dtype=float)
    hz = df[tag_Hz].to_numpy(dtype=float) if tag_Hz in df.columns else np.zeros(len(df))
    cid = df["Cycle_ID"].to_numpy()
    alarm = df["AL_Line_Bit"].to_numpy() if "AL_Line_Bit" in df.columns else np.zeros(len(df))

    rows = []
    prev_time_by_cycle = {}
    shot_no_by_cycle = {}

    for s in starts:
        lo = s - BASELINE_TICKS
        hi = s + DIP_WINDOW_TICKS
        if lo < 0 or hi >= len(df):
            continue

        base_pt = pt[lo:s].mean()
        base_ft = ft[lo:s].mean()
        if base_pt <= 0:
            continue

        win_pt = pt[s:hi + 1]
        win_ft = ft[s:hi + 1]

        dip_at = int(np.argmin(win_pt))
        dip_min = win_pt[dip_at]
        dip_size = base_pt - dip_min

        # 최저점 이후 기준선의 RECOVERY_RATIO 까지 돌아오는 데 몇 틱 걸리나
        rec_target = base_pt * RECOVERY_RATIO
        after = win_pt[dip_at:]
        rec_idx = np.where(after >= rec_target)[0]
        if len(rec_idx):
            rec_ticks = int(rec_idx[0])
            rec_sec = (t[min(s + dip_at + rec_ticks, len(t) - 1)] - t[s + dip_at]) / np.timedelta64(1, "s")
        else:
            # 창 안에 회복하지 못한 케이스(실측 12%)는 결측이 아니라 '창 끝까지 못 돌아옴'
            # 이라는 정보다. 결측으로 두면 dropna 에서 그 12% 가 통째로 날아간다.
            rec_ticks = DIP_WINDOW_TICKS - dip_at
            rec_sec = (t[min(hi, len(t) - 1)] - t[s + dip_at]) / np.timedelta64(1, "s")

        c = cid[s]
        shot_no_by_cycle[c] = shot_no_by_cycle.get(c, 0) + 1
        prev_t = prev_time_by_cycle.get(c)
        gap = (t[s] - prev_t) / np.timedelta64(1, "s") if prev_t is not None else np.nan
        prev_time_by_cycle[c] = t[s]

        # 틱 카운트는 DB 에 기록된 줄을 센 것이라 값이 7~9개뿐인 계단형이 된다.
        # 틱 간격이 불균일하므로(값이 바뀔 때만 기록) 실제 시간이 더 정확하고,
        # 연속값이라 오토인코더가 다루기도 낫다. 둘 다 저장하되 모델은 초를 쓴다.
        n_inj = int(inj[s:hi + 1].sum())
        rows.append({
            "Cycle_ID": int(c),
            "Shot_Time": pd.Timestamp(t[s]),
            "Shot_Index_In_Cycle": shot_no_by_cycle[c],
            # --- 사출 직전 상태 ---
            "PT_Baseline_Before": base_pt,
            "FT_Baseline_Before": base_ft,
            "Hz_at_Shot": hz[s],
            "DeltaP_over_Q_at_Shot": base_pt / base_ft if base_ft > 0 else np.nan,
            "SV_at_Shot": sv[s],
            # --- 딥의 모양 ---
            "PT_Dip_Size": dip_size,
            "PT_Dip_Depth_Pct": dip_size / base_pt * 100,
            "PT_Dip_At_Tick": dip_at,
            "PT_Dip_At_Sec": (t[s + dip_at] - t[s]) / np.timedelta64(1, "s"),
            "PT_Recovery_Ticks": rec_ticks,
            "PT_Recovery_Sec": rec_sec,
            "FT_Rise_Size": win_ft.max() - base_ft,
            "FT_Rise_Pct": (win_ft.max() - base_ft) / base_ft * 100 if base_ft > 0 else 0.0,
            # --- 사출 자체 ---
            "Inj_Duration_Ticks": n_inj,
            "Inj_Duration_Sec": (t[min(s + n_inj, len(t) - 1)] - t[s]) / np.timedelta64(1, "s"),
            "Time_Since_Last_Shot": gap,
            "Excluded_Alarm": bool(alarm[lo:hi + 1].max() > 0),
            "Excluded_DailyWindow": _in_daily_exclude_window(pd.Timestamp(t[s])),
        })

    shot_df = pd.DataFrame(rows)
    if shot_df.empty:
        return shot_df

    # 사이클 내 첫 사출은 직전 간격이 없다 - 사이클 시작으로부터의 경과로 채운다
    shot_df["Time_Since_Last_Shot"] = shot_df["Time_Since_Last_Shot"].fillna(-1.0)
    shot_df["Excluded"] = shot_df["Excluded_Alarm"] | shot_df["Excluded_DailyWindow"]
    return shot_df.set_index(shot_df["Shot_Time"])


#: 모델 입력으로 쓸 피처
#:
#: **주의 - 정의상 종속인 조합을 같이 넣으면 안 된다.**
#: 처음엔 PT_Dip_Depth_Pct(= Dip_Size/Baseline*100) 와 PT_Dip_Size_z(= Dip_Size 를
#: Baseline 으로 정규화) 와 PT_Baseline_Before 를 모두 넣었는데, 셋이 서로를 완전히
#: 결정해서(상관 1.000000) 오토인코더가 대수 항등식만 외우고 끝났다.
#: Valid Loss 가 0.0004(사이클 모델의 1/220)까지 떨어졌지만 이상 탐지 능력은 없었다.
#: 그래서 비율(_Pct) 대신 정규화된 _z 만 남긴다. Baseline 영향 제거에 더해
#: 흩어짐 보정까지 되어 있어 _Pct 보다 낫다.
#:
#: Shot_Index_In_Cycle 은 고유값이 2개뿐이고 Time_Since_Last_Shot 과 상관 0.999
#: (첫 사출이면 -1 이라 사실상 같은 정보)라 둘 다 뺀다.
#: 틱 카운트(고유값 5~9개)는 전부 초 단위로 바꿨다. 값이 몇 개뿐인 계단형 변수가
#: 기여도의 77% 를 차지하면서 검출률이 튀었기 때문이다(Test 7.39%, 분포는 오히려
#: 개선됐는데도). 초 단위는 연속값이라 표준화·재구성이 안정적이다.
SHOT_FEATURE_COLS = [
    # 사출 직전 상태 - 이 사출이 어떤 조건에서 시작했나
    "PT_Baseline_Before",
    "FT_Baseline_Before",
    "Hz_at_Shot",                # 모터 주파수 (Cycle 쪽 전류 지표의 재료)
    "DeltaP_over_Q_at_Shot",     # 유로 저항 (Cycle 에서 노즐 지표로 유효했던 것)
    "SV_at_Shot",
    # 딥의 모양 - 이 모델의 핵심
    "PT_Dip_Size",               # -> 정규화되어 PT_Dip_Size_z 로 들어간다
    "PT_Dip_At_Sec",
    "PT_Recovery_Sec",
    "FT_Rise_Size",              # -> FT_Rise_Size_z
    # 사출 자체
    "Inj_Duration_Sec",
]

#: 시작 조건에 끌려다니는 것들 - Cycle 쪽과 같은 표준화 잔차 방식
SHOT_NORMALIZE_SPECS = {
    "PT_Dip_Size":  ["PT_Baseline_Before"],
    "FT_Rise_Size": ["FT_Baseline_Before"],
}
