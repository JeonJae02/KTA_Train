# parkkt/Cycle_Feature_Extractor.py
"""Cycle-level(=Pump_BuildUp on~off 1회) 피처 추출.

train_pump.ipynb(기존)과의 핵심 차이:
  - 블록 리셋 기준이 Wagon이 아니라 Pump_BuildUp 단독.
  - BuildUp==0 구간을 통째로 버리지 않는다 - PreBuildUp_PT/FT, Decay_Slope는
    BuildUp이 꺼져 있는 순간의 값이 필요하기 때문에, 사이클 경계 바로 앞/뒤
    짧은 구간만 남겨서 같이 계산한다.
  - AL_Line_Bit==1인 순간이 하나라도 낀 사이클, 그리고 23:50~06:45 사이에
    시작하는 사이클은 통째로 제외한다.
"""
import numpy as np
import pandas as pd

# ==========================================
# 펌프 -> 헤드/로봇/로컬SEL슬롯 매핑 (실데이터로 검증된 것만)
# ==========================================
#: 각 펌프가 실제로 사출하는 순간은 `Shot_Injection_HD{head}_P{slot}` 이 알려준다.
#: (예전에 쓰던 `Shot_P{n}_SEL_HD{h}` 는 "선택됨" 구간이라 사출 순간보다 2배 넓다.)
#: 실측으로 확인한 로컬슬롯 -> 글로벌펌프 대응:
#:   HD1_P1->P1, HD1_P2->P2, HD2_P1->P3, HD2_P2->P4,
#:   HD3_P1->P5, HD3_P2->P6, HD4_P1->P7 (HD4_P2 는 이벤트 없음 = Pair 하나뿐)
#: **탱크 번호는 펌프 번호와 다르다.** 매핑표 기준:
#:   P1->Tank1 BACK / P2->Tank3-2 CUSH / P3->Tank2-1 SOFT / P4->Tank2-2 HARD
#:   P5->Tank3-1 고탄성 / P6->Tank1 BACK / P7->Tank3-2 CUSH
#: 탱크 태그는 TK_Temp_PV_P1~P5 까지만 존재한다(P6, P7 없음). 그래서 예전처럼
#: TK_Temp_PV_{PUMP_ID} 로 쓰면 P2/P3/P5 는 **조용히 다른 탱크 온도를 읽고**,
#: P6/P7 은 태그가 없어 죽는다. 반드시 이 표를 거쳐야 한다.
PUMP_MAP = {
    "P1": {"head": "HD1", "robot": "RB1", "slot": 1, "tank": "P1"},
    "P2": {"head": "HD1", "robot": "RB1", "slot": 2, "tank": "P5"},
    "P3": {"head": "HD2", "robot": "RB1", "slot": 1, "tank": "P2"},
    "P4": {"head": "HD2", "robot": "RB1", "slot": 2, "tank": "P4"},
    "P5": {"head": "HD3", "robot": "RB2", "slot": 1, "tank": "P3"},
    "P6": {"head": "HD3", "robot": "RB2", "slot": 2, "tank": "P1"},
    "P7": {"head": "HD4", "robot": "RB2", "slot": 1, "tank": "P5"},
}

RAMP_THRESHOLD_PCT = 0.05      # 증압완료 판정: |FT-SV|/SV <= 5%
DECAY_WINDOW_SEC = 3.0         # BuildUp off 직후 감쇠 기울기 계산 구간(초)
#: 감쇠 기울기를 믿으려면 최소 몇 점이 필요한가.
#: 실측(72시간, 6338 사이클): 구간 내 포인트가 4~5개인 경우가 98.5%, 2~3개가 1.5%.
#: 포인트 2개짜리는 두 값이 우연히 같으면 기울기가 정확히 0으로 나와 가짜 이상을 만든다
#: (실제로 그렇게 잡힌 케이스가 있었다). 3점 이상이면 그런 우연이 사라지고,
#: 버리는 양도 0.1% 뿐이다. 반대로 5점 전부 같은 값인 케이스는 압력이 정말 안 빠진
#: 진짜 신호이므로 살려둔다.
MIN_DECAY_POINTS = 3
DIP_THRESHOLD_PCT = 0.03       # 안정구간 dip 판정: 직전 롤링평균 대비 3% 이상 하락
#: 사출이 일으키는 압력 딥은 신호가 꺼진 뒤에도 이어진다. 실측(P1 사출 3265건 평균):
#:   사출시작 +0틱 PT -3.5 / +1틱 -9.4(최저) / +2틱 -6.2 / +3틱 -0.8 / +4틱 -0.2
#: Injection 신호 자체는 평균 2.4틱만 켜지므로, 신호 구간만 "설명됨"으로 보면
#: 회복 중 하락이 유출로 잘못 잡힌다. 사출 시작 후 이만큼은 설명된 구간으로 친다.
INJECTION_DIP_TICKS = 4
STEADY_TICK_MIN = 2            # Tick_Index >= 2 부터 Phase_Steady
DAILY_EXCLUDE_START = "23:50"
#: 06:45 로 잡았을 때 06시대(=06:45~07:00 에 시작한 사이클) 이상률이 7.77% 로
#: 다른 시간대(1~2.8%)의 3~5배였다. 그 사이클들의 범인이 Instant_FT_Error_Rate_mean
#: 13~15%(평소 1~2%), Steady_Std_FT 370~470 으로 토출이 확실히 안 잡힌 상태였다.
#: 야간 이상이 06:45 에 끝나지 않고 07:00 까지 여파가 남는다고 보고 경계를 옮겼다.
DAILY_EXCLUDE_END = "07:00"
COLD_START_GAP_SEC = 1800      # 직전 사이클 종료 후 30분 이상 지나면 콜드스타트 플래그


def _in_daily_exclude_window(ts):
    t = ts.time()
    start = pd.Timestamp(DAILY_EXCLUDE_START).time()
    end = pd.Timestamp(DAILY_EXCLUDE_END).time()
    # 23:50~06:45 처럼 자정을 넘어가는 구간
    return t >= start or t <= end


def extract_cycle_features(raw_df, pump_id):
    """raw_df: LogExtractor.get_data() 결과 (index=Time, KST naive).
    반환: (cycle_df, meta) - cycle_df는 Cycle 1행당 1row.
    """
    info = PUMP_MAP[pump_id]
    tag_SV = f"g_s_SV_{pump_id}"
    tag_PT = f"Scale_Out___PT_{pump_id}"
    tag_FT = f"Scale_Out___FT_{pump_id}"
    tag_Ana = f"Ana_Out_{pump_id}"
    tag_Temp = "TK_Temp_PV_" + info["tank"]   # 탱크 번호 != 펌프 번호 (PUMP_MAP 주석 참고)
    tag_BuildUp = f"Pump_BuildUp_{pump_id}"
    tag_Hz = f"Hz_Out_{pump_id}"
    tag_ShotInj = f"Shot_Injection_{info['head']}_P{info['slot']}"

    df = raw_df.ffill().fillna(0).copy()
    df = df.reset_index().rename(columns={df.index.name or "Time": "Time"})
    if "Time" not in df.columns:
        df = df.rename(columns={df.columns[0]: "Time"})

    # ==========================================
    # 1. 연속 시계열 위에서 Cycle_ID 부여 (Pump_BuildUp 단독 기준)
    # ==========================================
    buildup = df[tag_BuildUp].astype(int)
    buildup_changed = buildup.diff().fillna(0) != 0
    df["Cycle_ID"] = buildup_changed.cumsum()

    # ==========================================
    # 2. 연속 파생변수 (자르기 전에 계산)
    # ==========================================
    # Prev_SV: 직전 가동 사이클의 SV 최댓값
    shot_sv_max = df[buildup == 1].groupby("Cycle_ID")[tag_SV].max()
    prev_sv_map = shot_sv_max.shift(1)
    df["Prev_SV"] = df["Cycle_ID"].map(prev_sv_map).ffill().fillna(0)

    # PreBuildUp_PT/FT: 가장 최근 idle(BuildUp==0) 시점의 PT/FT
    idle_pt = df[tag_PT].where(buildup == 0)
    idle_ft = df[tag_FT].where(buildup == 0)
    df["PreBuildUp_PT"] = idle_pt.ffill().fillna(0)
    df["PreBuildUp_FT"] = idle_ft.ffill().fillna(0)

    # 안정구간 rolling 통계용 tick index (사이클 내부, 자르기 전엔 계산 불가하므로 임시)
    df["Tick_Index_tmp"] = df.groupby("Cycle_ID").cumcount()

    # 이 펌프가 실제로 사출한 순간 + 그 여파가 남는 구간까지를 "설명된 하락"으로 본다.
    # (사출 신호는 평균 2.4틱만 켜지는데 압력 딥은 +3틱까지 이어지므로 뒤로 늘려준다.)
    df["Shot_Owned_Active"] = 0
    if tag_ShotInj in df.columns:
        inj = (df[tag_ShotInj] != 0)
        spread = inj.copy()
        for k in range(1, INJECTION_DIP_TICKS + 1):
            spread |= inj.shift(k, fill_value=False)
        df["Shot_Owned_Active"] = spread.astype(int)

    # ==========================================
    # 3. Cycle 메타(시작/끝 시각, AL_Line_Bit, 제외 여부) 계산
    # ==========================================
    cycles = df[buildup == 1].groupby("Cycle_ID").agg(
        Start_Time=("Time", "first"),
        End_Time=("Time", "last"),
        AL_Line_Bit_Any=("AL_Line_Bit", "max") if "AL_Line_Bit" in df.columns else ("Time", "first"),
    )
    cycles["Excluded_DailyWindow"] = cycles["Start_Time"].apply(_in_daily_exclude_window)
    if "AL_Line_Bit" in df.columns:
        cycles["Excluded_Alarm"] = cycles["AL_Line_Bit_Any"] > 0
    else:
        cycles["Excluded_Alarm"] = False

    # 직전 사이클과의 갭(콜드스타트 플래그)
    cycles = cycles.sort_values("Start_Time")
    prev_end = cycles["End_Time"].shift(1)
    cycles["Gap_Since_Last_Cycle_Sec"] = (cycles["Start_Time"] - prev_end).dt.total_seconds()
    cycles["Cold_Start"] = (cycles["Gap_Since_Last_Cycle_Sec"] > COLD_START_GAP_SEC).fillna(False)

    # ==========================================
    # 4. 사이클별 피처 계산
    # ==========================================
    rows = []
    # 사이클마다 전체 테이블을 필터링하면 O(사이클수 x 전체행수)이라 9일치에서 매우 느리다.
    # 가동 구간만 한 번 잘라두고 groupby로 한 번에 순회한다.
    df_on = df[buildup == 1].sort_values("Time")
    all_cycle_ids = sorted(df_on["Cycle_ID"].unique())

    for cid, seg in df_on.groupby("Cycle_ID", sort=True):
        if seg.empty:
            continue
        seg = seg.reset_index(drop=True)
        seg["Tick_Index"] = np.arange(len(seg))
        steady = seg[seg["Tick_Index"] >= STEADY_TICK_MIN]
        if steady.empty:
            steady = seg  # 너무 짧은 사이클은 전체를 안정구간으로 대체

        sv_ref = seg[tag_SV].iloc[0] if seg[tag_SV].iloc[0] != 0 else seg[tag_SV].max()

        feat = {
            "Cycle_ID": cid,
            "Start_Time": seg["Time"].iloc[0],
            "End_Time": seg["Time"].iloc[-1],
            "N_Ticks": len(seg),
            "SV_mean": seg[tag_SV].mean(),
            "FT_mean": seg[tag_FT].mean(), "FT_max": seg[tag_FT].max(), "FT_std": seg[tag_FT].std(),
            "PT_mean": seg[tag_PT].mean(), "PT_max": seg[tag_PT].max(), "PT_std": seg[tag_PT].std(),
            "Ana_Out_mean": seg[tag_Ana].mean(),
            "Temp_mean": seg[tag_Temp].mean(),
            "Prev_SV": seg["Prev_SV"].iloc[0],
            "Prev_SV_Diff": sv_ref - seg["Prev_SV"].iloc[0],
            "PreBuildUp_PT": seg["PreBuildUp_PT"].iloc[0],
            "PreBuildUp_FT": seg["PreBuildUp_FT"].iloc[0],
            "PT_Jump_From_Pre": seg[tag_PT].iloc[0] - seg["PreBuildUp_PT"].iloc[0],
            "FT_Jump_From_Pre": seg[tag_FT].iloc[0] - seg["PreBuildUp_FT"].iloc[0],
            # 감쇠가 시작되는 지점의 값. Decay_Slope_* 가 이 값에 끌려다니므로
            # (PT 상관 -0.530, FT 상관 -0.888) 정규화의 기준 변수로 쓴다.
            "PT_at_End": seg[tag_PT].iloc[-1],
            "FT_at_End": seg[tag_FT].iloc[-1],
        }

        # Instant_FT_Error_Rate, Cum_FT_Error
        ft_err = sv_ref - seg[tag_FT] if sv_ref else (seg[tag_SV] - seg[tag_FT])
        err_rate = np.where(seg[tag_SV] > 0, (seg[tag_SV] - seg[tag_FT]) / seg[tag_SV] * 100, 0.0)
        feat["Instant_FT_Error_Rate_mean"] = float(np.mean(err_rate))
        feat["Instant_FT_Error_Rate_max"] = float(np.max(np.abs(err_rate)))
        feat["Cum_FT_Error"] = float((seg[tag_SV] - seg[tag_FT]).sum())

        # 증압소요시간: |FT-SV|/SV <= 임계값 첫 진입까지 경과시간
        if sv_ref:
            ok = (np.abs(seg[tag_SV] - seg[tag_FT]) / sv_ref) <= RAMP_THRESHOLD_PCT
        else:
            ok = pd.Series([False] * len(seg))
        if ok.any():
            first_ok_idx = ok.idxmax()
            feat["RampUp_Time_Sec"] = (seg.loc[first_ok_idx, "Time"] - seg["Time"].iloc[0]).total_seconds()
        else:
            feat["RampUp_Time_Sec"] = np.nan  # 목표치 근처에 아예 못 들어간 사이클

        # Δp/Q 비율, 전류추정치 I (안정구간 기준)
        steady_ft = steady[tag_FT].replace(0, np.nan)
        feat["DeltaP_over_Q"] = float((steady[tag_PT] / steady_ft).mean())
        hz = steady[tag_Hz].replace(0, np.nan) if tag_Hz in steady.columns else pd.Series([np.nan])
        feat["Current_Proxy_I"] = float((steady[tag_PT] * steady[tag_FT] / hz).mean())

        # 안정구간 기저변동성
        feat["Steady_Std_PT"] = float(steady[tag_PT].std())
        feat["Steady_Std_FT"] = float(steady[tag_FT].std())

        # Ramp_Negative_Slope_Count: 증압구간(Tick_Index < steady 시작)에서 FT diff<0 인 틱 수
        ramp = seg[seg["Tick_Index"] < STEADY_TICK_MIN + 3]  # 증압 초반 구간(러프하게 처음 몇 틱)
        # 더 정확히는 RampUp_Time_Sec 안쪽 구간을 써야 하지만, 우선 처음 STEADY_TICK_MIN+3틱으로 근사
        feat["Ramp_Negative_Slope_Count"] = int((ramp[tag_FT].diff() < 0).sum())

        # Unexplained dip: 안정구간에서 SHOT 신호 없이 PT/FT가 롤링평균 대비 떨어진 틱
        pt_roll = seg[tag_PT].rolling(3, min_periods=1).mean().shift(1)
        ft_roll = seg[tag_FT].rolling(3, min_periods=1).mean().shift(1)
        pt_drop_pct = (pt_roll - seg[tag_PT]) / pt_roll.replace(0, np.nan)
        ft_drop_pct = (ft_roll - seg[tag_FT]) / ft_roll.replace(0, np.nan)
        no_shot = seg["Shot_Owned_Active"] == 0
        steady_mask = seg["Tick_Index"] >= STEADY_TICK_MIN

        pt_unexplained = (pt_drop_pct >= DIP_THRESHOLD_PCT) & no_shot & steady_mask
        ft_unexplained = (ft_drop_pct >= DIP_THRESHOLD_PCT) & no_shot & steady_mask
        feat["Unexplained_PT_Dip_Count"] = int(pt_unexplained.sum())
        feat["Unexplained_PT_Dip_Max"] = float(pt_drop_pct[pt_unexplained].max()) if pt_unexplained.any() else 0.0
        feat["Unexplained_FT_Dip_Count"] = int(ft_unexplained.sum())
        feat["Unexplained_FT_Dip_Max"] = float(ft_drop_pct[ft_unexplained].max()) if ft_unexplained.any() else 0.0

        rows.append(feat)

    cycle_df = pd.DataFrame(rows)
    if cycle_df.empty:
        return cycle_df, cycles

    cycle_df = cycle_df.set_index("Cycle_ID")

    # ==========================================
    # 5. Decay_Slope_PT/FT: BuildUp off 직후 짧은 구간 (별도로 off 데이터에서 계산)
    # ==========================================
    # off 구간도 사이클마다 전체 스캔하면 느리다 - 시간순 정렬 후 searchsorted로 구간만 집는다.
    decay_rows = {}
    off_df = df[buildup == 0].sort_values("Time")
    off_times = off_df["Time"].values
    off_pt = off_df[tag_PT].values
    off_ft = off_df[tag_FT].values
    win = np.timedelta64(int(DECAY_WINDOW_SEC * 1000), "ms")

    for cid in all_cycle_ids:
        if cid not in cycle_df.index:
            continue
        end_time = np.datetime64(cycle_df.loc[cid, "End_Time"])
        lo = np.searchsorted(off_times, end_time, side="right")
        hi = np.searchsorted(off_times, end_time + win, side="right")
        if hi - lo < MIN_DECAY_POINTS:
            decay_rows[cid] = (np.nan, np.nan)
            continue
        t = (off_times[lo:hi] - end_time) / np.timedelta64(1, "s")
        pt_slope = np.polyfit(t, off_pt[lo:hi], 1)[0]
        ft_slope = np.polyfit(t, off_ft[lo:hi], 1)[0]
        decay_rows[cid] = (pt_slope, ft_slope)

    cycle_df["Decay_Slope_PT"] = [decay_rows.get(cid, (np.nan, np.nan))[0] for cid in cycle_df.index]
    cycle_df["Decay_Slope_FT"] = [decay_rows.get(cid, (np.nan, np.nan))[1] for cid in cycle_df.index]

    # ==========================================
    # 6. 제외 규칙 적용 (일자대 + AL_Line_Bit + 콜드스타트 갭 정보만 표시, 삭제는 안 함)
    # ==========================================
    cycle_df = cycle_df.join(cycles[["Excluded_DailyWindow", "Excluded_Alarm",
                                      "Gap_Since_Last_Cycle_Sec", "Cold_Start"]])
    cycle_df["Excluded"] = cycle_df["Excluded_DailyWindow"] | cycle_df["Excluded_Alarm"]

    return cycle_df, cycles
