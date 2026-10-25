"""[유닛 · 규칙] 에어 주입 유닛 실린더 판정 규칙 — 배치(conv_03)와 실시간(unit_realtime)이 같이 쓴다.

입력은 (unit, day) 한 줄에 지표별 하루 평균이 있는 표다 (unit_spec.METRICS).
사이클 하나의 값은 0.5초 샘플 때문에 0 아니면 약 0.5초로만 나오지만, 샘플 위상이 사이클마다 무작위라
하루 평균은 치우치지 않는다 (하루 약 4,400 사이클 → 표준오차 약 4 ms). 그래서 판정은 일별 평균으로 한다.

판정: z = (그날 평균 − 단기 기준) ÷ σ
  느려짐     시간 지표 z ≥ +5 → 점검 추천. 자동 승인 안 함 (실린더·공압·밸브 노후 쪽)
  빨라짐     시간 지표 z ≤ −5 → 관찰. 3 가동일 같은 값이면 승인 (정비·압력 조정)
  거리 변화  거리 지표 |z| ≥ 5 → 점검 추천 1회. 3 가동일 같은 값이면 승인 (센서·이동대 조정)
  장기 기준 이탈  최근 7 가동일 중앙값이 장기 기준에서 3σ 넘게 → 추세 표시 (장기 기준은 갱신 안 함)
"""

import numpy as np
import pandas as pd

from .unit_spec import COUNT_METRICS, ERRORS, LABEL, TIME_METRICS

Z_TOL = 5.0             #: 일별 평균이 단기 기준에서 σ 의 5배 넘게 벗어나면 변화
Z_LONG = 3.0            #: 최근 7 가동일 중앙값이 장기 기준에서 3σ 넘게 벗어나면 장기 기준 이탈
N_ACCEPT_CYL = 3        #: 빨라짐·거리 변화가 가동일 3일 같은 값으로 유지되면 승인
N_REF = 3               #: 장기 기준 = 처음 3 가동일 중앙값 (refs 를 주지 않을 때)
SIGMA_FLOOR = {"s": 0.003, "cnt": 0.05}
LONG_WINDOW = 7         #: 장기 기준 비교에 쓰는 최근 가동일 수

SEV_NAME = ["정상", "관찰", "점검 추천", "경고"]


def base_metric(col):
    """노즐이 둘인 유닛의 컬럼(nozzle_rev_time_1)을 지표 이름(nozzle_rev_time)으로."""
    return col.rsplit("_", 1)[0] if col[-2:] in ("_1", "_2") else col


def metric_kind(col):
    return "s" if base_metric(col) in TIME_METRICS else "cnt"


def _sigma(x, kind):
    """날짜 간 흔들림. 연속한 날 차이의 중앙값으로 잡아 계단 변화에 끌려가지 않게 한다."""
    dx = np.abs(np.diff(x[np.isfinite(x)]))
    s = 1.4826 * np.median(dx) / np.sqrt(2) if len(dx) else np.nan
    return max(s, SIGMA_FLOOR[kind])


def cyl_judge(D, refs=None):
    """유닛 × 지표별 일별 판정. 반환: 긴 표 (unit, metric, day, value, level, z, z_long, state, event, trend, sev).

    D    : (unit, day, 지표 평균들, cycles, *_Err) 표
    refs : {unit: {지표: (장기 기준, σ)}} — 주면 그 값을 쓰고(실시간), 안 주면 D 에서 잡는다(배치).
    """
    out = []
    for unit, g in D.groupby("unit"):
        g = g.sort_values("day")
        for col in [c for c in g.columns if c not in ("unit", "day", "cycles") and not c.endswith("_Err")]:
            kind = metric_kind(col)
            x = g[col].to_numpy(dtype=float)
            fixed = (refs or {}).get(unit, {}).get(col)
            if fixed is None and np.isfinite(x).sum() < N_REF + 2:
                continue
            long_ref, sig = fixed if fixed is not None else (np.nanmedian(x[:N_REF]), _sigma(x, kind))
            level, run = long_ref, []
            for i, (day, v) in enumerate(zip(g.day, x)):
                z = (v - level) / sig
                recent = np.nanmedian(x[max(0, i - LONG_WINDOW + 1):i + 1])
                z_long = (recent - long_ref) / sig
                state, event, sev = "정상", "", 0
                if np.isfinite(z) and abs(z) >= Z_TOL:
                    up = z > 0
                    state = ("느려짐" if up else "빨라짐") if kind == "s" else "거리 변화"
                    same = bool(run) and abs(v - np.median(run)) < 3 * sig
                    run = run + [v] if same else [v]
                    accept_ok = not (kind == "s" and up)
                    if len(run) == 1:
                        event = f"{state} 점검" if state != "빨라짐" else "빨라짐"
                    if accept_ok and len(run) >= N_ACCEPT_CYL:
                        level, run = float(np.median(run)), []
                        event, state = "변화 승인", "정상(승인)"
                    sev = 2 if state in ("느려짐", "거리 변화") else (1 if state == "빨라짐" else 0)
                else:
                    run = []
                trend = ""
                if np.isfinite(z_long) and abs(z_long) >= Z_LONG:
                    trend = ("장기 기준보다 느림" if z_long > 0 else "장기 기준보다 빠름") if kind == "s" else "장기 기준과 거리 다름"
                out.append(dict(unit=unit, metric=col, kind=kind, day=day, value=v, level=level, sigma=sig,
                                long_ref=long_ref, z=z, z_long=z_long, state=state, event=event, trend=trend, sev=sev))
    return pd.DataFrame(out)


# --------------------------------------------------------------------------- #
# 출력용
# --------------------------------------------------------------------------- #
def item_sev(state, trend):
    """지표 한 줄 → 수준. 느려짐·거리 변화는 점검 추천, 빨라짐은 관찰, 장기 기준보다 느림도 관찰."""
    if state in ("느려짐", "거리 변화"):
        return 2
    if state == "빨라짐" or trend == "장기 기준보다 느림":
        return 1
    return 0


ACTION = {
    "느려짐": "실린더·공압(레귤레이터·필터)·솔밸브 점검",
    "빨라짐": "정비·압력 조정 이력 확인",
    "거리 변화": "정위치·끝단 센서 위치 또는 이동대 조정 이력 확인",
    "장기 기준보다 느림": "추세 관찰 (서서히 느려지는 중)",
}


def item_reason(col, state, trend, value, level, kind):
    """지표 한 줄 → 사람이 읽는 근거 (시간은 초, 거리는 카운트)."""
    label = LABEL.get(base_metric(col), col) + (f" (노즐 {col[-1]})" if col[-2:] in ("_1", "_2") else "")
    fmt = (lambda v: f"{v:.3f}초") if kind == "s" else (lambda v: f"{v:.1f}카운트")
    key = state if state in ACTION else trend
    if key not in ACTION:
        return f"{label} {fmt(value)}"
    return f"{label} {fmt(value)} (기준 {fmt(level)}) → {ACTION[key]}"


def error_reason(name, n):
    return f"PLC 에러: {ERRORS.get(name, name)} {n}회 (오늘, 운전 중)"
