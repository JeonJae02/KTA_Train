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

#: 기준 센서 6개. core=True 는 대차에 무관하게 같은 타이밍에 켜지는 코어 센서
#: first = 그 위치의 1번 대차 감지 센서 (있으면 그날 몇 대 뒤인지 직접 확인)
#: n_fix = 1번 대차 센서가 없는 곳의 고정값 — 09-10 – 10-19 패턴 매칭 최빈값(31일 중 31일 같음)
SENSORS = pd.DataFrame([
    ("P00140", "대차 진입", "main", "R00040", True, "P00142", 0),
    ("P00141", "대차 쉬프트", "main", "R00080", True, None, 0),
    ("P00154", "클로징 진입", "closing", "R00030", True, None, 2),
    ("P00164", "오프닝 진입", "opening", "R00070", True, None, 26),
    ("P00451", "전진 진입", "md_fwd", "R00055", False, "P00450", 26),
    ("P00459", "후진 진입", "md_rev", "R00060", True, "P00458", 41),
], columns=["bit", "name", "group", "counter", "core", "first", "n_fix"]).set_index("bit")

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


def day_events(day, sensors=SENSORS):
    """하루치 기준 센서 엣지 표. 가동하지 않은 날은 빈 표."""
    out = []
    for g, rows in sensors.groupby("group", sort=False):
        raw = load_group(g, day)
        if raw.empty or "R00040" not in raw:
            continue
        d = add_position(raw)
        ref = counter_resets(d, "R00040").dropna(subset=["wagon"])
        if len(ref) < 200:                         # 대차 200대 미만이면 가동일로 보지 않는다
            continue
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
SENSOR_REARM_TOL = 0.1  # 센서 전체에서 통과당 2차 리셋이 0.1 번 넘으면 센서 단위 재감지(채터링)


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
            / 센서 재감지 (통과당 2차 리셋 ≥ 0.1 — 대차 여럿에서 두 번씩 켜짐)
    """
    share = (t[t.state != "정상"].groupby(["day", "sensor", "state"]).size()
             / t.groupby(["day", "sensor"]).size()).rename("share").reset_index()
    share = share[share.share >= SENSOR_SHARE]
    agg = dict(D=("D", "first"), W0=("W0", "first"), J0=("J0", "first"))
    if "rearm" in t:
        agg["rearm"] = ("rearm", "mean")
    lv = t.groupby(["day", "sensor"]).agg(**agg).reset_index()
    lv = lv.merge(long_ref, on="sensor", suffixes=("", "_ref"))
    lv["dD"] = lv.D - lv.D_ref                           # 센서 이동(이 센서만) 또는 체인 늘어남(전 센서 비례)
    lv["dW0"] = lv.W0 - lv.W0_ref                        # 센서 감도·센서 면 오염 (전 대차 켜짐 길이)
    lv["dJ0"] = lv.J0 - lv.J0_ref                        # 센서 고정 상태 (전 대차 흔들림)
    rearm = lv["rearm"] if "rearm" in lv else pd.Series(0.0, index=lv.index)
    flag = np.select(
        [lv.dD.abs() >= T_TOL, lv.dW0 >= W_TOL, (lv.J0 >= J_TOL) & (lv.dJ0 >= 2), rearm >= SENSOR_REARM_TOL],
        ["센서 이동", "센서 감도·면 오염", "센서 흔들림", "센서 재감지"], default="")
    lv["flag"] = flag
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
