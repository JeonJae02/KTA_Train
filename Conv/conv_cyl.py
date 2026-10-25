"""[유닛 · 배치 전처리] 하루치 캐시에서 유닛 사이클 표를 만든다 (conv_03 노트북용).

데이터 정의는 unit_spec.py, 판정 규칙은 unit_rules.py (실시간 unit_realtime 과 같이 씀).
여기는 캐시(그룹 파일)를 읽어 사이클을 자르고 지표를 재는 배치 전처리만 한다.

시간을 재는 원칙
- DB 수집기는 한 번에 K → M → P → D → R → T 블록 순서로 읽는다. 입력·출력 비트(P)는 같은 순간 값이고,
  카운터(R)는 약 0.26초 뒤에 읽힌다. 그래서 **실린더 시간은 P 비트끼리** 잰다.
- 비트가 바뀐 시각 = 바뀌기 전 샘플과 바뀐 샘플의 중간. 사이클 하나는 0.5초 샘플이라 못 재지만 하루 평균은 정확하다.
- 샘플 간격이 MAX_GAP 보다 긴 곳에서 바뀐 비트는 시각을 버린다.
- 위치(거리 지표)는 코어와 같은 방식 — 그룹 파일을 합쳐 카운터 6개로 만든다.
"""

import numpy as np
import pandas as pd

from conv_core import load_group, add_position
from conv_grid import load_merged
from unit_spec import (MAX_GAP, MAX_CYCLE, UNITS, METRICS, TIME_METRICS, COUNT_METRICS, LABEL,  # noqa: F401
                       ERRORS)
from unit_rules import (Z_TOL, Z_LONG, N_ACCEPT_CYL, N_REF, SIGMA_FLOOR, cyl_judge, _sigma)  # noqa: F401


def _edges(x, t, pos, gap):
    """비트 변화 (샘플 인덱스, 방향, 중간 시각, 중간 위치). 긴 간격 사이의 변화는 시각 NaN."""
    b = np.nan_to_num(x, nan=0.0) > 0
    e = np.diff(b.astype(int))
    i = np.where(e != 0)[0] + 1
    tm = (t[i] + t[i - 1]) / 2
    pm = (pos[i] + pos[i - 1]) / 2
    bad = gap[i] > MAX_GAP
    tm[bad] = np.nan
    pm[bad] = np.nan
    return i, e[i - 1], tm, pm


def cycle_table(day, unit, wagon_events=None):
    """하루치 사이클 표. 사이클 = 스톱퍼 SOL ON 부터 다음 스톱퍼 SOL ON 전까지.

    wagon_events : conv_grid.day_events 결과 중 이 유닛 진입 센서 이벤트(t, wagon). 있으면 대차 번호를 붙인다.
    """
    u = UNITS[unit]
    # 위치는 카운터 전부로 만든다 (conv_grid.day_events 와 같음). 유닛 그룹 파일만 쓰면 카운터가 3개라 2차 리셋에 흔들린다
    raw = load_merged(day, list(dict.fromkeys([u["group"], "main", "opening", "md_fwd", "md_rev"])))
    if raw.empty:
        return pd.DataFrame()
    d = add_position(raw)
    t = (d.index - d.index[0]).total_seconds().to_numpy()
    pos = d.pos.to_numpy()
    gap = np.r_[np.inf, np.diff(t)]
    t0 = d.index[0]

    sig = {k: [u[k]] if isinstance(u[k], str) else u[k]
           for k in ("stop_sol", "nozzle_sol", "return_sol", "open_sol", "org", "rev", "flt", "stop_fwd", "stop_rev", "nozzle_rev",
                     "rev_latch")}
    ed = {}
    for k, bits in sig.items():
        for j, b in enumerate(bits):
            if b in d:
                ed[(k, j)] = _edges(d[b].to_numpy(), t, pos, gap)

    # 사이클 경계: 스톱퍼 SOL ON (SOL 은 3초 넘게 켜져 있어 거의 빠짐없이 잡힌다)
    i_on, dirs, tm_on, pm_on = ed[("stop_sol", 0)]
    starts = i_on[dirs > 0]
    if len(starts) < 10:
        return pd.DataFrame()
    bounds = np.r_[starts, len(t)]
    cyc = pd.DataFrame({"start_i": starts, "end_i": bounds[1:]})
    cyc["t"] = t0 + pd.to_timedelta(t[starts], unit="s")
    cyc["run"] = d.Conv_Run_Out.to_numpy()[starts] > 0
    # 사이클 길이. 정상은 대차 한 대 간격(약 12–15초). 그보다 훨씬 길면 중간에 라인이 멈춘 사이클이다
    t_next = np.r_[t[starts[1:]], np.nan]
    cyc["cycle_s"] = t_next - t[starts]
    cyc["interrupted"] = ~(cyc.cycle_s <= MAX_CYCLE)

    def first(key, direction, lo, hi):
        """사이클 [lo, hi) 안의 첫 변화 (시각, 위치). 없으면 NaN."""
        out_t = np.full(len(lo), np.nan)
        out_p = np.full(len(lo), np.nan)
        if key not in ed:
            return out_t, out_p
        ii, dd, tt, pp = ed[key]
        m = dd == direction
        ii, tt, pp = ii[m], tt[m], pp[m]
        k = np.searchsorted(ii, lo)
        ok = (k < len(ii))
        ok[ok] = ii[k[ok]] < hi[ok]
        out_t[ok] = tt[k[ok]]
        out_p[ok] = pp[k[ok]]
        return out_t, out_p

    lo, hi = cyc.start_i.to_numpy(), cyc.end_i.to_numpy()
    lo_prev = np.maximum(lo - 1, 0)          # 시작 신호 자체(SOL ON)는 경계 샘플에서 바뀐다
    for name, s_sig, s_dir, e_sig, e_dir, unit_, _ in METRICS:
        for j in range(len(sig[e_sig])):
            col = name if len(sig[e_sig]) == 1 else f"{name}_{j + 1}"
            if len(sig[s_sig]) > 1:              # 시작 신호가 여러 개면 첫 번째로 본다 (노즐 SOL 은 하나)
                pass
            st_t, st_p = first((s_sig, 0), s_dir, lo_prev, hi)
            en_t, en_p = first((e_sig, j), e_dir, lo_prev, hi)
            v = (en_t - st_t) if unit_ == "s" else (en_p - st_p)
            v[v < 0] = np.nan
            v[cyc.interrupted.to_numpy()] = np.nan          # 멈췄다 이어진 사이클은 지표에서 뺀다
            cyc[col] = v
    # 비상(오버트래블) 감지 · 원점복귀 SOL 포착 여부
    fl_t, _ = first(("flt", 0), +1, lo_prev, hi)
    cyc["flt"] = np.isfinite(fl_t)
    # PLC 에러 비트 (사이클 안에서 새로 켜졌는지)
    for e in ERRORS:
        c = f"{u['prefix']}_{e}"
        if c in d:
            x = d[c].fillna(0).to_numpy() > 0
            on = np.where(x[1:] & ~x[:-1])[0] + 1
            k = np.searchsorted(on, lo)
            ok = k < len(on)
            ok[ok] = on[k[ok]] < hi[ok]
            cyc[e] = ok
    if wagon_events is not None and len(wagon_events):
        we = wagon_events.sort_values("t")
        j = np.searchsorted(we.t.to_numpy(), (cyc.t + pd.Timedelta(seconds=1.0)).to_numpy()) - 1
        ok = j >= 0
        w = np.full(len(cyc), np.nan)
        w[ok] = we.wagon.to_numpy()[j[ok]]
        cyc["wagon"] = w
    cyc.insert(0, "unit", unit)
    cyc.insert(1, "day", pd.Timestamp(day))
    return cyc.drop(columns=["start_i", "end_i"])


# --------------------------------------------------------------------------- #
# 사이클 이벤트 시각 (그림용) · 일별 표 · 판정
# --------------------------------------------------------------------------- #
#: 사이클 그림에 그릴 신호 (키, 표시 이름)
EVENT_SIGNALS = [("stop_sol", "스톱퍼 SOL"), ("stop_rev", "스톱퍼 후진감지"), ("stop_fwd", "스톱퍼 전진감지"),
                 ("org", "정위치"), ("nozzle_sol", "노즐 SOL"), ("nozzle_rev", "노즐 후진감지"),
                 ("open_sol", "노즐 오픈 SOL"), ("rev", "끝단 후진"), ("return_sol", "원점복귀 SOL")]


def cycle_events(day, unit):
    """사이클마다 신호별 첫 켜짐·꺼짐 시각 (스톱퍼 SOL ON 기준 초). P 비트끼리라 블록 시차가 없다."""
    u = UNITS[unit]
    raw = load_group(u["group"], day)
    if raw.empty:
        return pd.DataFrame()
    t = (raw.index - raw.index[0]).total_seconds().to_numpy()
    gap = np.r_[np.inf, np.diff(t)]
    pos = np.zeros(len(t))
    b0 = u["stop_sol"]
    i0, d0, t0_, _ = _edges(raw[b0].to_numpy(), t, pos, gap)
    on = d0 > 0
    starts, st = i0[on], t0_[on]
    nxt = np.r_[t[starts[1:]], np.inf]
    ok = (nxt - t[starts]) <= MAX_CYCLE
    rows = []
    for key, name in EVENT_SIGNALS:
        b = u[key] if isinstance(u[key], str) else u[key][0]
        if b not in raw:
            continue
        ii, dd, tt, _ = _edges(raw[b].to_numpy(), t, pos, gap)
        for direction, kind in [(+1, "ON"), (-1, "OFF")]:
            m = dd == direction
            ti, tm = ii[m], tt[m]
            k = np.searchsorted(ti, np.maximum(starts - 1, 0))
            valid = k < len(ti)
            valid[valid] = ti[k[valid]] < np.r_[starts[1:], len(t)][valid]
            rel = np.full(len(starts), np.nan)
            rel[valid] = tm[k[valid]] - st[valid]
            rel[~ok] = np.nan
            rows.append(pd.DataFrame({"signal": name, "edge": kind, "rel_s": rel}))
    return pd.concat(rows, ignore_index=True)


def cyl_daily(C, min_cycles=1000):
    """(유닛, 날짜)별 지표 평균과 사이클 수. 중단 사이클·정지 중 사이클은 뺀다."""
    C = C[C.run & ~C.interrupted]
    cols = [c for c in C.columns if any(c == m or c.startswith(m + "_") for m in TIME_METRICS + COUNT_METRICS)]
    g = C.groupby(["unit", "day"])
    D = g[cols].mean()
    D["cycles"] = g.size()
    errs = [c for c in C.columns if c.endswith("_Err")]
    D[errs] = g[errs].sum()
    return D[D.cycles >= min_cycles].reset_index()
