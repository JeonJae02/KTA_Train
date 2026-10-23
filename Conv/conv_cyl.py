"""에어 주입 유닛(전진·후진·에어백) 실린더 사이클 표 (conv_03 참고).

유닛 하나의 사이클 (IL: Air_Supply_Unit.il, 엑셀 Air_Supply2_정상사이클.xlsx)
  1 스톱퍼 SOL ON (진입 카운터가 설정값에 닿을 때)  → 스톱퍼 실린더 전진 (후진감지 OFF → 전진감지 ON)
  2 대차가 스톱퍼에 걸려 유닛 이동대가 끌려감          → 정위치 OFF  (체인이 끄는 구간: 카운트)
  3 노즐 SOL ON (정위치 OFF 순간)                    → 노즐 실린더 전진 (노즐 후진감지 OFF)
  4 노즐 후진감지 OFF 0.5초 뒤 노즐 오픈 SOL ON        → 에어 주입
  5 끝단 후진 ON (이동대가 끝까지 끌려감)              → 노즐 오픈 OFF, 0.5초 뒤 노즐 SOL OFF
  6 노즐 실린더 후진 (노즐 후진감지 ON)                → 스톱퍼 SOL OFF → 스톱퍼 후진 (전진감지 OFF → 후진감지 ON)
  7 스톱퍼 후진감지 ON 순간 원점복귀 SOL ON            → 이동대가 원점으로 (정위치 ON) → 복귀 SOL OFF

시간을 재는 원칙
- DB 수집기는 한 번에 K → M → P → D → R → T 블록 순서로 읽는다. 입력·출력 비트(P)는 같은 순간 값이고,
  카운터(R)는 약 0.26초 뒤에 읽힌다. 그래서 **실린더 시간은 P 비트끼리** 잰다.
- 비트가 바뀐 시각 = 바뀌기 전 샘플과 바뀐 샘플의 중간. 샘플 간격(약 0.5초)보다 짧은 동작은 사이클 하나로는
  못 재지만, **하루 평균은 치우침 없이 정확하다** (샘플 위상이 사이클마다 무작위이므로). 그래서 판정은 일별 평균으로 한다.
- 샘플 간격이 MAX_GAP 보다 긴 곳에서 바뀐 비트는 시각을 버린다.

짧은 신호는 PLC 래치로 대체한다 (사이클 중 잡히는 비율, 10-16)
- 끝단 후진 (P00453 · P0045B · P00132) 은 0.5초 안팎의 펄스라 66 – 76% 만 잡힌다.
  끝단 후진이 켜지는 순간 SET 되어 원점복귀까지 유지되는 `Air_Supply*_Rev_PX_CHK` 를 쓰면 100% 다.
  이 래치는 비상 감지·주입 시간 초과·에러 중 리셋으로도 켜지지만, 그런 사이클은 그 자체가 이상 사이클이다.
- 래치는 D 블록이라 P 비트보다 조금 늦게 읽힌다. 끌림 거리는 카운트(같은 행의 위치) 차이라 영향이 작다.
"""

import numpy as np
import pandas as pd

from conv_core import load_group, add_position

MAX_GAP = 1.2           # 초. 이보다 긴 샘플 간격 사이의 변화는 시각을 모른다고 본다
MAX_CYCLE = 30.0        # 초. 사이클(스톱퍼 SOL ON → 다음 ON)이 이보다 길면 중간에 라인이 멈춘 것으로 보고 지표에서 뺀다

#: 유닛별 비트. nozzle_rev 가 두 개면 노즐 1·2 를 따로 본다 (전진·후진은 입력 하나를 공유)
UNITS = {
    "md_fwd": dict(name="전진 유닛", group="md_fwd", counter="R00055",
                   stop_sol="P00470", nozzle_sol="P00471", return_sol="P00472", open_sol="P00473",
                   org="P00452", rev="P00453", flt="P00454", stop_fwd="P00455", stop_rev="P00456",
                   nozzle_rev=["P00457"], prefix="Air_Supply2", rev_latch="Air_Supply2_Rev_PX_CHK"),
    "md_rev": dict(name="후진 유닛", group="md_rev", counter="R00060",
                   stop_sol="P00474", nozzle_sol="P00475", return_sol="P00476", open_sol="P00477",
                   org="P0045A", rev="P0045B", flt="P0045C", stop_fwd="P0045D", stop_rev="P0045E",
                   nozzle_rev=["P0045F"], prefix="Air_Supply3", rev_latch="Air_Supply3_Rev_PX_CHK"),
    "airbag": dict(name="에어백 유닛", group="airbag", counter="R00050",
                   stop_sol="P0028A", nozzle_sol="P0028B", return_sol="P0028C", open_sol="P00243",
                   org="P00131", rev="P00132", flt="P00133", stop_fwd="P00134", stop_rev="P00135",
                   nozzle_rev=["P00136", "P00137"], prefix="Air_Supply", rev_latch="Air_Supply_Rev_PX_CHK"),
}

#: 사이클 지표: (이름, 시작 신호, 시작 변화, 끝 신호, 끝 변화, 단위, 설명)
#: 신호 이름은 UNITS 의 키. 변화 +1 = 켜짐, −1 = 꺼짐
METRICS = [
    ("stop_fwd_start", "stop_sol", +1, "stop_rev", -1, "s", "스톱퍼 전진 출발: SOL ON → 후진감지 OFF"),
    ("stop_fwd_time", "stop_sol", +1, "stop_fwd", +1, "s", "스톱퍼 전진 시간: SOL ON → 전진감지 ON"),
    ("stop_rev_start", "stop_sol", -1, "stop_fwd", -1, "s", "스톱퍼 후진 출발: SOL OFF → 전진감지 OFF"),
    ("stop_rev_time", "stop_sol", -1, "stop_rev", +1, "s", "스톱퍼 후진 시간: SOL OFF → 후진감지 ON"),
    ("nozzle_fwd_start", "nozzle_sol", +1, "nozzle_rev", -1, "s", "노즐 전진 출발: SOL ON → 후진감지 OFF"),
    ("nozzle_rev_time", "nozzle_sol", -1, "nozzle_rev", +1, "s", "노즐 후진 시간: SOL OFF → 후진감지 ON"),
    ("return_time", "stop_rev", +1, "org", +1, "s", "원점 복귀 시간: 스톱퍼 후진감지 ON(= 복귀 SOL ON) → 정위치 ON"),
    ("catch_cnt", "stop_fwd", +1, "org", -1, "cnt", "대차가 걸리기까지: 스톱퍼 전진감지 ON → 정위치 OFF"),
    ("drag_cnt", "org", -1, "rev_latch", +1, "cnt", "끌림 거리: 정위치 OFF → 끝단 후진 ON (이동대가 끌려간 거리)"),
    ("air_time", "open_sol", +1, "open_sol", -1, "s", "에어 주입 시간: 노즐 오픈 SOL ON → OFF"),
]


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
    raw = load_group(u["group"], day)
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
    for e in ("Stopper_CY_FWD_Err", "Stopper_CY_REV_Err", "Nozzle_CY_FWD_Err", "Nozzle_CY_REV_Err",
              "Return_Err", "Home_CHK_Err", "Rev_FLT_PX_CHK_Err"):
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


TIME_METRICS = [m[0] for m in METRICS if m[5] == "s"]
COUNT_METRICS = [m[0] for m in METRICS if m[5] == "cnt"]
LABEL = {m[0]: m[6].split(":")[0] for m in METRICS}

#: 판정 기준
Z_TOL = 5.0             # 일별 평균이 단기 기준에서 날짜 간 흔들림(σ)의 5배 넘게 벗어나면 변화
Z_LONG = 3.0            # 최근 7 가동일 중앙값이 장기 기준에서 3σ 넘게 벗어나면 장기 기준 이탈
N_ACCEPT_CYL = 3        # 위치·거리 변화가 가동일 3일 같은 값으로 유지되면 승인
N_REF = 3               # 장기 기준 = 처음 3 가동일 중앙값 (데이터 첫날 09-10 부터. 그 뒤의 계단 변화가 변화로 잡히게 짧게)
SIGMA_FLOOR = {"s": 0.003, "cnt": 0.05}


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


def _sigma(x, kind):
    """날짜 간 흔들림. 연속한 날 차이의 중앙값으로 잡아 계단 변화에 끌려가지 않게 한다."""
    dx = np.abs(np.diff(x[np.isfinite(x)]))
    s = 1.4826 * np.median(dx) / np.sqrt(2) if len(dx) else np.nan
    return max(s, SIGMA_FLOOR[kind])


def cyl_judge(D):
    """유닛 × 지표별 일별 판정. 반환: 긴 표 (unit, metric, day, value, level, z, z_long, state, event, sev).

    state
      정상        단기 기준 ±5σ 안
      느려짐      시간 지표가 +5σ 넘게 길어짐 → 점검 추천. 자동 승인 안 함 (실린더·공압·밸브 노후)
      빨라짐      시간 지표가 짧아짐 → 관찰. 3일 유지되면 승인
      거리 변화   카운트 지표가 ±5σ 넘게 변함 → 점검 1회, 3일 유지되면 승인 (센서·이동대 조정)
    장기 기준 이탈 (z_long ≥ 3, 최근 7일 중앙값 vs 처음 3일, 장기 기준은 갱신 안 함) 은 state 와 별도 컬럼 trend 로 남긴다.
    계단 변화를 승인해도 trend 에는 계속 남는다 — 느린 변화와 승인된 변화를 장기 그래프에서 같이 본다.
    """
    out = []
    for unit, g in D.groupby("unit"):
        g = g.sort_values("day")
        for col in [c for c in g.columns if c not in ("unit", "day", "cycles") and not c.endswith("_Err")]:
            base = col.rsplit("_", 1)[0] if col[-2:] in ("_1", "_2") else col
            kind = "s" if base in TIME_METRICS else "cnt"
            x = g[col].to_numpy(dtype=float)
            if np.isfinite(x).sum() < N_REF + 2:
                continue
            sig = _sigma(x, kind)
            long_ref = np.nanmedian(x[:N_REF])
            level, run = long_ref, []
            for i, (day, v) in enumerate(zip(g.day, x)):
                z = (v - level) / sig
                recent = np.nanmedian(x[max(0, i - 6):i + 1])
                z_long = (recent - long_ref) / sig
                state, event, sev = "정상", "", 0
                if np.isfinite(z) and abs(z) >= Z_TOL:
                    up = z > 0
                    if kind == "s":
                        state = "느려짐" if up else "빨라짐"
                    else:
                        state = "거리 변화"
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
