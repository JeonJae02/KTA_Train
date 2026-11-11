"""[코어 · 규칙] 대차 통과 센서 판정 규칙 — 배치(conv_02)와 실시간(core_realtime)이 같이 쓴다.

입력은 (day, sensor, wagon) 한 칸에 dist · width · rearm · jitter (· pitch) 가 있는 표다.
  dist   : 같은 대차의 P00140 엣지에서 이 센서 엣지까지 카운트 (센서 간 거리 + 그 대차·센서 오프셋)
  width  : 켜짐 길이 (카운트)
  rearm  : 대차 한 번 지날 때 2차 리셋 수의 평균 (한 대차에서 다시 켜진 횟수)
  jitter : 같은 대차 dist 의 IQR
판정 결과(state)·수준(sev)·근거 문구(reason)는 이 파일의 정의를 프론트까지 그대로 쓴다.
"""

import numpy as np
import pandas as pd

# --------------------------------------------------------------------------- #
# 판정
# --------------------------------------------------------------------------- #
#: 판정 기준 (카운트). 코어 센서의 대차 간 흔들림이 std 0.5–0.7 이라 3 은 그 4–6배
W_TOL = 3.0             # 켜짐 길이가 정상보다 이만큼 길면 이물
T_TOL = 3.0             # 앞·뒤 끝이 기준보다 이만큼 움직이면 위치 변화
J_TOL = 3.0             # 대차 안 흔들림(IQR)이 이만큼 넘으면 흔들림
BACK_TOL = 1.5          # 이물 지수가 이 아래로 내려오면 복귀
N_ACCEPT = 3            # 위치 변화가 가동일 n일 유지되면 새 정상으로 승인
SENSOR_SHARE = 0.5      # 센서 하나에서 대차 절반 이상이 같은 판정이면 센서 문제로 올린다
SPEED_TOL = 0.003       # 속도 비율이 기준에서 0.3% 넘게 벗어나면 구동부
REARM_TOL = 0.5         # 대차 한 번 지날 때 2차 리셋이 평균 0.5 번 이상이면 재감지 (코어 센서는 0.01 안팎)
SENSOR_REARM_TOL = 0.1  # 대차 한 칸에서 통과당 2차 리셋이 0.1 번 이상이면 "재감지 대차"로 센다
SENSOR_REARM_WAGONS = 10  # 한 센서에서 재감지 대차가 10대 이상이면 센서 문제(센서 재감지)로 묶는다
# 근거 (9/10–10/24, 36 가동일): 재감지 대차 수가 전진 진입은 하루 15–26대, 나머지 센서는 0–4대.
# 재감지 때 먼저 켜지는 곳은 어느 센서든 표식 7·14카운트 앞(모든 대차에 있는 같은 자리)이라,
# 한두 대면 그 대차 쪽(휨·헐거움·이물), 여러 대면 센서가 그 자리까지 보는 것(감지 거리·위치)으로 본다.


def add_metrics(t):
    """물리적 정상(그날 그 센서 대차 대부분의 값) 대비 앞 끝·뒤 끝·켜짐 길이 편차와 센서별 허용폭."""
    t = t.copy()
    # 물리적 정상은 "깨끗한 대차"로 잡는다. 이물 대차가 많은 센서(전진 진입은 3분의 1)에서 전체 중앙값을 쓰면
    # 기준이 일찍 켜지는 쪽으로 끌려가 멀쩡한 대차가 위치 변화로 보인다.
    #   1) 재감지가 적은 대차로 정상 켜짐 길이 W0
    #   2) 그중 켜짐 길이도 정상인 대차로 센서 간 거리 D
    rearm = t["rearm"] if "rearm" in t else pd.Series(0.0, index=t.index)
    clean1 = rearm < REARM_TOL / 2
    w_clean = t.width.where(clean1)
    t["W0"] = w_clean.groupby([t.day, t.sensor]).transform("median")
    t["W0"] = t.W0.fillna(t.groupby(["day", "sensor"]).width.transform("median"))
    clean2 = clean1 & ((t.width - t.W0).abs() <= 1.5)
    t["D"] = t.dist.where(clean2).groupby([t.day, t.sensor]).transform("median")
    t["D"] = t.D.fillna(t.groupby(["day", "sensor"]).dist.transform("median"))   # 센서 간 고정 거리 (그날)
    t["J0"] = t.jitter.where(clean2).groupby([t.day, t.sensor]).transform("median")  # 평소 대차 안 흔들림
    t["J0"] = t.J0.fillna(t.groupby(["day", "sensor"]).jitter.transform("median"))
    t["n_clean"] = clean2.groupby([t.day, t.sensor]).transform("sum")
    t["rise_off"] = t.dist - t.D                         # 앞 끝 편차 (−: 일찍 켜짐)
    t["width_ex"] = t.width - t.W0                       # 이물 지수 (+: 길게 켜짐)
    t["fall_off"] = t.rise_off + t.width_ex              # 뒤 끝 편차
    # 허용폭: 센서 자신의 흔들림이 크면 그만큼 넓힌다 (코어는 J0≈0 이라 기본값 그대로)
    t["t_tol"] = np.maximum(T_TOL, 1.5 * t.J0)
    t["j_tol"] = np.maximum(J_TOL, 2.0 * t.J0)
    return t


def classify_cell(r):
    """대차×센서×날짜 한 칸의 상태. 우선순위: 재감지 > 이물 > 위치 변화 > 흔들림 > 정상."""
    if r.get("rearm", 0) >= REARM_TOL:
        return "이물(분리)"                               # 한 대차에서 두 번 켜짐 — 떨어진 이물 또는 채터링
    if r.width_ex >= W_TOL:
        if r.rise_off <= -(r.width_ex - 2):
            return "이물(앞)"                             # 앞만 당겨지고 뒤는 그대로
        if abs(r.rise_off) < 2:
            return "이물(뒤)"                             # 앞은 그대로, 뒤가 늘어남
        return "이물(기타)"
    if abs(r.rise_off) >= r.t_tol and abs(r.fall_off) >= r.t_tol:
        return "위치 변화"                                 # 앞뒤가 같이 이동, 길이는 정상
    if r.jitter >= r.j_tol:
        return "흔들림"                                   # 그 대차만 유난히 흔들림
    return "정상"


def judge(t, n_accept=N_ACCEPT):
    """칸마다 상태를 매기고, 단기 기준(승인된 위치)과 사건(새로 생김·복귀·승인)을 만든다.

    반환 t 에 추가되는 컬럼
      state     : 그날 물리 기준 대비 상태 (classify_cell)
      accepted  : 위치 변화가 n 가동일 이상 같은 값으로 유지돼 새 정상으로 승인된 상태인지
      event     : 그날 생긴 사건 — 새 이물 / 이물 복귀 / 위치 변화 점검 / 위치 승인 / 위치 복귀 / 흔들림 / ""
    """
    t = add_metrics(t)
    t["state"] = t.apply(classify_cell, axis=1)
    t = t.sort_values(["sensor", "wagon", "day"]).reset_index(drop=True)
    t["accepted"] = False
    t["event"] = ""
    for (_, _), g in t.groupby(["sensor", "wagon"], sort=False):
        prev_state, run, run_val, accepted_val = "정상", 0, None, 0.0
        for i, r in g.iterrows():
            s = r.state
            # 히스테리시스: 이물이었다면 이물 지수 < 1.5 이고 재감지 < 0.25 일 때만 복귀로 본다.
            # 그 사이 값이면 이물 상태를 이어간다 (기준 근처에서 날마다 뒤집히지 않게)
            recovered = (r.width_ex < BACK_TOL) and (r.get("rearm", 0) < REARM_TOL / 2)
            if prev_state.startswith("이물") and not s.startswith("이물") and not recovered:
                s = prev_state
                t.at[i, "state"] = s
            if s.startswith("이물") and not prev_state.startswith("이물"):
                t.at[i, "event"] = "새 이물"
            elif prev_state.startswith("이물") and not s.startswith("이물"):
                t.at[i, "event"] = "이물 복귀"
            if s == "위치 변화":
                same = run_val is not None and abs(r.rise_off - run_val) < r.t_tol
                run = run + 1 if same else 1
                run_val = r.rise_off if not same else run_val
                if abs(r.rise_off - accepted_val) < r.t_tol:
                    t.at[i, "accepted"] = True                # 이미 승인된 위치
                elif run == 1:
                    t.at[i, "event"] = "위치 변화 점검"
                elif run >= n_accept:
                    accepted_val = run_val
                    t.at[i, "accepted"] = True
                    t.at[i, "event"] = "위치 승인"
            else:
                run, run_val = 0, None
                if s == "정상":
                    if abs(accepted_val) >= r.t_tol:
                        t.at[i, "event"] = "위치 복귀"           # 승인했던 위치에서 원래 자리로 돌아옴
                    accepted_val = 0.0
            if s == "흔들림" and prev_state != "흔들림":
                t.at[i, "event"] = "흔들림"
            prev_state = s
    return t


def sensor_level(t, long_ref):
    """센서 단위 판정.

    share : 그날 그 센서에서 대차 절반 이상이 같은 상태 → 센서 문제로 올린다
    lv    : 장기 기준 대비 D·W0·J0 변화와 flag
            센서 이동 (|dD| ≥ 3) / 센서 감도·면 오염 (dW0 ≥ 3) / 센서 흔들림 (J0 ≥ 3 이고 기준보다 2 이상 큼)
            / 센서 재감지 (통과당 2차 리셋 ≥ 0.1 인 대차가 10대 이상)
    """
    share = (t[t.state != "정상"].groupby(["day", "sensor", "state"]).size()
             / t.groupby(["day", "sensor"]).size()).rename("share").reset_index()
    share = share[share.share >= SENSOR_SHARE]
    agg = dict(D=("D", "first"), W0=("W0", "first"), J0=("J0", "first"))
    if "rearm" in t:
        agg["rearm"] = ("rearm", "mean")
        agg["n_rearm"] = ("rearm", lambda x: int((x >= SENSOR_REARM_TOL).sum()))
    lv = t.groupby(["day", "sensor"]).agg(**agg).reset_index()
    lv = lv.merge(long_ref, on="sensor", suffixes=("", "_ref"))
    lv["dD"] = lv.D - lv.D_ref                           # 센서 이동(이 센서만) 또는 체인 늘어남(전 센서 비례)
    lv["dW0"] = lv.W0 - lv.W0_ref                        # 센서 감도·센서 면 오염 (전 대차 켜짐 길이)
    lv["dJ0"] = lv.J0 - lv.J0_ref                        # 센서 고정 상태 (전 대차 흔들림)
    n_rearm = lv["n_rearm"] if "n_rearm" in lv else pd.Series(0, index=lv.index)
    conds = [(lv.dD.abs() >= T_TOL, "센서 이동"), (lv.dW0 >= W_TOL, "센서 감도·면 오염"),
             ((lv.J0 >= J_TOL) & (lv.dJ0 >= 2), "센서 흔들림"), (n_rearm >= SENSOR_REARM_WAGONS, "센서 재감지")]
    # 여러 개가 동시에 걸리면 전부 남긴다 (", " 로 이음)
    lv["flag"] = [", ".join(name for c, name in conds if c.iloc[i]) for i in range(len(lv))]
    return share, lv


def wagon_level(t, long_ref_pitch, core_bits):
    """대차 단위 판정: 코어 센서 대부분에서 그 대차의 피치가 장기 기준과 다르면 대차 표식 이동."""
    p = t[t.sensor.isin(core_bits)].merge(long_ref_pitch, on=["sensor", "wagon"], suffixes=("", "_ref"))
    p["dpitch"] = p.pitch - p.pitch_ref
    w = p.groupby(["day", "wagon"]).agg(dpitch_med=("dpitch", "median"),
                                        n_sensor=("dpitch", lambda x: int((x.abs() >= 2).sum())),
                                        n_all=("dpitch", "count")).reset_index()
    w["표식 이동"] = (w.n_sensor >= 3) & (w.dpitch_med.abs() >= 2)
    return w


# --------------------------------------------------------------------------- #
# 수준 · 문구 (출력용)
# --------------------------------------------------------------------------- #
#: 0 정상 · 1 관찰 · 2 점검 추천 · 3 경고
SEV_NAME = ["정상", "관찰", "점검 추천", "경고"]


def absorbed_by_sensor(state, flags):
    """센서 쪽 문제로 묶여 대차에는 표시하지 않는 칸인지.

    센서 재감지(대차 10대 이상에서 다시 켜짐)가 켜진 센서는 그 센서의 대차 경고를 전부 센서 경고 하나로 묶는다.
    대차마다 가르지 않는다 — 센서를 고치면 나머지도 같이 고쳐질 가능성이 크고, 화면에서 우선순위가 분명해진다.
    센서를 고쳐 플래그가 꺼지면 대차 경고가 다시 대차 위에 뜬다.
    """
    return "센서 재감지" in flags


def cell_sev(state, accepted=False):
    """칸 상태 → 수준. 승인된 위치 변화는 정상, 위치 변화·흔들림은 관찰, 이물은 점검 추천."""
    if state is None or (isinstance(state, float) and np.isnan(state)):
        return None
    if str(state).startswith("이물"):
        return 2
    if state == "위치 변화":
        return 0 if accepted else 1
    if state == "흔들림":
        return 1
    return 0


def cell_reason(state, accepted=False):
    """칸 상태 → 사람이 읽는 근거 한 줄. 숫자는 넣지 않는다 — 최근 통과가 바뀔 때마다 문구가 바뀌면
    받는 쪽(uplink)이 경고가 바뀐 것으로 본다."""
    if state == "이물(분리)":
        return "한 대차에서 다시 켜짐 → 표식 앞 떨어진 이물 또는 채터링"
    if state == "이물(앞)":
        return "앞 끝·켜짐 길이 변화 → 표식 앞쪽 이물"
    if state == "이물(뒤)":
        return "켜짐 길이 변화, 앞 끝 제자리 → 표식 뒤쪽 이물"
    if state == "이물(기타)":
        return "앞 끝·켜짐 길이 변화 → 이물 또는 감지 간격 변화"
    if state == "위치 변화":
        return "앞뒤 끝 같이 이동, 길이 정상 → 표식 위치 변화" + (" (승인됨)" if accepted else "")
    if state == "흔들림":
        return "이 대차만 흔들림이 큼 → 표식 헐거움"
    return "정상"


#: 칸 상태 → 사유 키 (경고 reason). 같은 센서에서 같은 상태인 대차들이 경고 1건으로 묶인다
WAGON_REASON = {"이물(분리)": "wagon_rearm", "이물(앞)": "wagon_debris_front", "이물(뒤)": "wagon_debris_back",
                "이물(기타)": "wagon_debris", "위치 변화": "wagon_shift", "흔들림": "wagon_jitter"}


FLAG_ACTION = {
    "센서 이동": "센서 브래킷 위치 점검",
    "센서 감도·면 오염": "센서 면 청소·감지 거리 확인",
    "센서 흔들림": "센서 고정 상태 확인",
    "센서 재감지": "대차 여러 대에서 표식 앞까지 감지 → 센서 감지 거리·위치 확인",
}
#: 센서 플래그 → 사유 키 (경고 reason)
FLAG_REASON = {"센서 재감지": "sensor_rearm", "센서 이동": "sensor_shift",
               "센서 감도·면 오염": "sensor_dirty", "센서 흔들림": "sensor_jitter"}
