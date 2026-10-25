"""코어 기준 격자 + 대차×센서 일별 표 + 원인 판정 규칙 (conv_02 참고).

1. 이벤트 (`day_events`)
   기준 센서 6개의 모든 상승엣지를 카운터 리셋으로 복원하고, 엣지마다 다음을 붙인다.
   - wagon : 대차 ID = 그 대차가 대차 진입 센서(P00140)를 지날 때의 메인 대차번호
   - dist  : 같은 대차가 P00140 을 지난 위치에서 이 센서까지의 거리 (카운트)
             = 센서 간 고정 거리 + 그 대차·센서의 고유 오프셋
   - width : 켜짐 길이 = 원본 비트 하강 위치 − 정확한 상승 위치 (하강은 ±0.5초 샘플링 오차)
   몇 대 전 대차인지(n_back)는 dist 분포가 가장 좁아지는 칸 수로 찾는다.
   모든 그룹 파일에 R00040 이 들어 있어서(COMMON) 같은 파일 안에서 P00140 엣지와 바로 비교할 수 있다.

2. 일별 표 (`daily_table`)
   (날짜, 센서, 대차)마다 dist·width 중앙값, 대차 안 흔들림(IQR), 피치(앞 대차와의 간격).

3. 판정 (`judge`)
   물리적 정상 = 그 센서 대차 대부분의 값. 앞 끝(rise)·뒤 끝(fall)·켜짐 길이로 원인을 가른다.
"""

import numpy as np
import pandas as pd

from conv_core import RESET_DROP, add_position, bit_edges, counter_resets, load_group
from core_spec import SENSOR_TABLE

#: 기준 센서 6개. core=True 는 대차에 무관하게 같은 타이밍에 켜지는 코어 센서
#: 센서 정의는 core_spec.py (실시간 모델과 같이 씀). ruler=True 는 "자" 역할이 확인된 센서
SENSORS = SENSOR_TABLE

N_WAGON = 42
WIDTH_WIN = 40          # 상승 후 이 카운트 안의 첫 하강을 같은 펄스로 본다


def _width(d, bit, ev_pos, ev_seg):
    """엣지마다 켜짐 길이. 펄스를 통째로 놓쳤으면 NaN."""
    be = bit_edges(d, bit)
    fall = be[be.edge == -1]
    fp, fs = fall.pos.to_numpy(), fall.seg.to_numpy()
    j = np.searchsorted(fp, ev_pos, side="right")
    w = np.full(len(ev_pos), np.nan)
    ok = j < len(fp)
    w[ok] = fp[j[ok]] - ev_pos[ok]
    w[ok & (fs[np.minimum(j, len(fp) - 1)] != ev_seg)] = np.nan
    w[(w <= 0) | (w > WIDTH_WIN)] = np.nan
    return w


def n_from_first_wagon(d, first_bit):
    """유닛의 1번 대차 감지 센서가 켜질 때 메인 대차번호 − 1 = 그 유닛이 P00140 에서 몇 대 뒤인지.

    1번 대차는 P00140 에서 메인 번호 1 을 받으므로, 그 대차가 유닛에 왔을 때 메인 번호가 k 면
    유닛은 k − 1 대 뒤에 있다. (10-01 – 10-17 검증: 후진 P00458 → 41, 전진 P00450 → 26, 메인 P00142 → 0)
    """
    be = bit_edges(d, first_bit)
    r = be[be.edge == 1]
    if len(r) == 0:
        return None
    lab = d.loc[r.t, "Now_Actual_Wagon_Num"].dropna()
    return int((lab.mode().iloc[0] - 1) % N_WAGON) if len(lab) else None


def _rematch(rp, rs, ep, es, D, n_iter=3):
    """각 이벤트를 "pos − D 에 가장 가까운 P00140 엣지"에 맞춘다. D 는 맞춘 거리의 중앙값으로 다듬는다."""
    for _ in range(n_iter):
        tgt = ep - D
        k = np.clip(np.searchsorted(rp, tgt), 1, len(rp) - 1)
        k = np.where(np.abs(tgt - rp[k - 1]) <= np.abs(tgt - rp[k]), k - 1, k)
        ok = (rs[k] == es) & (np.abs(tgt - rp[k]) < 69)
        dist = np.where(ok, ep - rp[k], np.nan)
        if not ok.any():
            break
        D = np.nanmedian(dist)
    return k, ok, dist


def _match_back(ref, ev, n=None):
    """이벤트마다 같은 대차의 P00140 엣지를 찾는다. 반환 (n_back, dist, wagon).

    n 이 주어지면(1번 대차 감지 센서로 확인) 그대로 쓴다. 없으면 0–41 을 모두 시도해
    "대차별 중앙값이 ±2 안에 드는 대차 비율"이 가장 큰 n 을 고른다 (동률이면 평균 편차가 작은 쪽).
    코어 센서는 정답 n 에서 이 비율이 0.9 안팎으로 뚜렷하다. 대차마다 위치가 다른 센서(전진 진입)는
    어떤 n 도 0.5 를 못 넘으므로 반드시 1번 대차 감지 센서로 n 을 준다.
    매칭은 "몇 번째 이전 엣지"가 아니라 가장 가까운 엣지로 한다 — 엣지가 P00140 엣지 근처에서 켜져도
    대차가 바뀌지 않게.
    """
    rp, rs, rw = ref.pos.to_numpy(), ref.seg.to_numpy(), ref.wagon.to_numpy()
    ep, es = ev.pos.to_numpy(), ev.seg.to_numpy()
    idx = np.searchsorted(rp, ep, side="right") - 1
    rem = np.median(ep[idx >= 0] - rp[idx[idx >= 0]])          # 피치 안에서의 위치
    pitch = np.median(np.diff(rp)[np.diff(rp) < 200])
    cands = [n] if n is not None else range(N_WAGON)
    best = None
    for c in cands:
        k, ok, dist = _rematch(rp, rs, ep, es, rem + pitch * c)
        if not ok.any():
            continue
        s = pd.Series(dist[ok]).groupby(rw[k[ok]]).median()
        dev = (s - s.median()).abs()
        key = ((dev <= 2).mean(), -dev.mean())
        if best is None or key > best[0]:
            best = (key, c, k, ok, dist)
    if best is None:
        return (n if n is not None else -1), np.full(len(ev), np.nan), np.full(len(ev), np.nan)
    _, c, k, ok, dist = best
    return c, dist, np.where(ok, rw[k], np.nan)


def load_merged(day, groups):
    """여러 그룹 파일을 시각으로 합친 하루치 표. 겹치는 컬럼은 처음 것만."""
    parts = []
    for g in groups:
        raw = load_group(g, day)
        if raw.empty:
            continue
        parts.append(raw[[c for c in raw.columns if all(c not in p for p in parts)]])
    if not parts:
        return pd.DataFrame()
    d = pd.concat(parts, axis=1).sort_index()
    return d[~d.index.duplicated()].ffill()


def day_events(day, sensors=SENSORS):
    """하루치 기준 센서 엣지 표. 가동하지 않은 날은 빈 표.

    필요한 그룹 파일을 시각으로 합쳐 **카운터 6개 전부로** 위치를 만든다 (실시간 모델과 같은 방식).
    그룹 파일 하나의 카운터 3개로만 만들면, 2차 리셋이 잦은 카운터(R00055)가 섞인 파일에서 위치가 틀어진다.
    """
    out = []
    groups = list(dict.fromkeys(["main"] + list(sensors.group)))
    raw = load_merged(day, groups)
    if raw.empty or "R00040" not in raw:
        return pd.DataFrame()
    d = add_position(raw)
    ref = counter_resets(d, "R00040").dropna(subset=["wagon"])
    if len(ref) < 200:                             # 대차 200대 미만이면 가동일로 보지 않는다
        return pd.DataFrame()
    for g, rows in sensors.groupby("group", sort=False):
        for bit, s in rows.iterrows():
            ev = ref.copy() if s.counter == "R00040" else counter_resets(d, s.counter)
            if len(ev) < 200:
                continue
            ev = ev.sort_values("pos").reset_index(drop=True)
            # 2차 리셋: 카운터가 조금(< RESET_DROP) 떨어짐 = 같은 대차가 지나가는 중에 센서가 다시 켜져 카운터가 1 로 돌아감.
            # 이물에 한 번, 원래 표식에 한 번 켜지거나 채터링일 때 생긴다. PLC 는 마지막 엣지를 기준으로 동작한다.
            dx = d[s.counter].diff()
            sec = (dx < 0) & (dx > -RESET_DROP) & (d.seg == d.seg.shift(1))
            sp = np.sort((d.pos[sec] - (d[s.counter][sec] - 1)).to_numpy())
            nxt = np.r_[ev.pos.to_numpy()[1:], np.inf]
            rearm = np.searchsorted(sp, nxt) - np.searchsorted(sp, ev.pos.to_numpy(), side="right")
            n_first = n_from_first_wagon(d, s["first"]) if isinstance(s["first"], str) and s["first"] in d else None
            n_use = n_first if n_first is not None else (None if pd.isna(s["n_fix"]) else int(s["n_fix"]))
            n, dist, wag = _match_back(ref, ev, n_use)
            e = pd.DataFrame({
                "day": pd.Timestamp(day), "sensor": bit, "t": ev.t_est.to_numpy(), "pos": ev.pos.to_numpy(),
                "seg": ev.seg.to_numpy(), "n_back": n, "n_src": "1번대차" if n_first is not None else ("고정" if n_use is not None else "패턴"),
                "dist": dist, "wagon": wag,
                "width": _width(d, bit, ev.pos.to_numpy(), ev.seg.to_numpy()),
                "rearm": rearm,
            })
            # 피치 = 앞 대차와의 간격. 바로 앞 대차(번호 −1)일 때만
            prev_w = (e.wagon.shift(1) % N_WAGON) + 1
            consecutive = (e.seg == e.seg.shift(1)) & (prev_w == e.wagon)
            e["pitch"] = e.pos.diff().where(consecutive)
            out.append(e.dropna(subset=["wagon"]))
    if not out:
        return pd.DataFrame()
    ev = pd.concat(out, ignore_index=True)
    ev["wagon"] = ev.wagon.astype(int)
    return ev


def _iqr(x):
    return np.subtract(*np.nanpercentile(x, [75, 25])) if np.isfinite(x).sum() >= 5 else np.nan


def daily_table(ev):
    """(날짜, 센서, 대차)별 중앙값. 대차 하나가 하루 약 100번 지나가므로 하강 오차가 평균된다."""
    g = ev.groupby(["day", "sensor", "wagon"])
    t = g.agg(n=("pos", "size"), dist=("dist", "median"), width=("width", "median"),
              width_n=("width", "count"), pitch=("pitch", "median"), rearm=("rearm", "mean")).reset_index()
    t["jitter"] = g.dist.apply(lambda x: _iqr(x.to_numpy())).to_numpy()
    return t


# --------------------------------------------------------------------------- #
# 판정 — 규칙은 core_rules.py (실시간 모델과 같이 쓴다). 예전 이름으로도 부를 수 있게 다시 내보낸다
# --------------------------------------------------------------------------- #
from core_rules import (W_TOL, T_TOL, J_TOL, BACK_TOL, N_ACCEPT, SENSOR_SHARE, SPEED_TOL,  # noqa: E402,F401
                        REARM_TOL, SENSOR_REARM_TOL, SENSOR_REARM_WAGONS, absorbed_by_sensor,
                        add_metrics, classify_cell, judge, sensor_level, wagon_level)
