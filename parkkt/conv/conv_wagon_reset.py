"""컨베이어 카운터 오작동 리셋 — 대차(wagon) 번호별 집계.

카운터는 존 센서가 켜질 때 리셋되고, 정상이면 한 번에 130~140 까지 올라간 뒤 리셋된다.
센서가 대차 한 대를 지나는 동안 두 번 이상 켜지면 카운트가 중간에 다시 떨어진다(예: 121→2→8→7→6→12).
이 '중간에 다시 떨어진 리셋'을 오작동 리셋이라고 부른다.

기준
  - Pour_Zone_CNT PV 의 리셋 = 새 대차가 들어온 순간. 이 리셋에서 다음 리셋까지를 Pour 사이클 1개로 보고,
    그 사이클의 Now_Actual_Wagon_Num 을 대차 번호로 붙인다(대차 번호도 이 순간 0~0.5초 안에 바뀐다).
  - 첫 Pour 리셋 이전과 마지막 미완성 사이클은 버린다.
  - 다섯 카운터 중 하나라도 30초 이상 값이 안 바뀐 구간(정지·DB 끊김)이 걸친 Pour 사이클은 통째로 뺀다.

리셋 판정
  - 리셋 = 카운트 감소. 직전 값이 ABN_MAX(100) 이하면 오작동 리셋(정상은 직전 값 ≥ 100 이며 대부분 130~145).
  - 정상 리셋 1개와 그 뒤에 이어지는 오작동 리셋들을 묶어 '통과 1회(burst)'로 본다 = 센서 앞을 대차 1대가 지나간 것.
    통과 1회 안에 리셋이 2개 이상이면 오작동 통과다.

통과를 어느 Pour 사이클(=대차 번호)에 붙이나
  - 통과의 첫 리셋(정상 리셋) 시각 + SHIFT_S[카운터] 가 들어가는 Pour 사이클.
  - MD_CY_FWD 는 정상 리셋이 Pour 리셋 ±1.5초 안에 생기는데, 대차마다 Pour 리셋 앞이기도 뒤이기도 하다.
    시각 그대로 붙이면 같은 대차의 통과가 앞뒤 두 번호로 갈라진다. 그래서 반 사이클(6초)을 밀어
    '그 통과와 가장 가까운 Pour 리셋'의 대차 번호를 붙인다. 다른 카운터는 리셋이 사이클 중간이라 밀지 않는다.

실제 대차 번호
  - 위에서 붙인 번호(pour_wagon)는 'Pour 리셋 때 막 들어온 대차' 번호다. 그 순간 다른 존 센서 앞에 있는 대차는
    다른 대차라서, 카운터마다 LABEL_OFFSET 만큼 빼서 실제 대차 번호(wagon)로 바꾼다.
      wagon = (pour_wagon - LABEL_OFFSET - 1) % 42 + 1
  - 확인 방법
      MD_CY_FWD / MD_CY_REV : '전진/후진Unit 캐리어 1번감지' 가 켜진 순간의 통과가 9/10~10/14 전부(1,477 / 1,705회)
                              pour_wagon 28 / 42 → 차이 27 / 41
      Opening              : Opening 통과 4.2초 뒤 같은 대차가 전진Unit 에 들어온다(현장 확인). pour_wagon 차이 +1 이 99.99%
                              → FWD 보다 1 작은 26
      Conv_Stop / Closing  : 대차 간격 무늬(연속 5~6대만 간격 142~143)를 Pour 와 맞춘 값(상관 0.89 / 0.95). 센서 확인은 없음
"""
import os

import numpy as np
import pandas as pd

WAGON = "Now_Actual_Wagon_Num"
N_WAGON = 42

#: 이름 → (카운터 태그, 리셋 센서 태그). 센서 태그명의 공백 2개 주의.
COUNTERS = {
    "Pour_Zone": ("Pour_Zone_CNT PV", "캐리어진입 Check"),
    "Conv_Stop": ("Conv_Stop_CNT PV", "캐리어진입 쉬프트 Check"),
    "MD_CY_FWD": ("MD_CY_FWD_Zone_CNT PV", "전진Unit  캐리어 진입감지"),
    "Opening":   ("Opening_CNT PV", "Opening Zone In Detect"),
    "Closing":   ("Closing_CNT PV", "Closing Zone In Detect"),
    "MD_CY_REV": ("MD_CY_REV_Zone_CNT PV", "후진Unit  캐리어  진입감지"),
}
#: 리셋 센서 후보로 같이 받아 보는 태그. 후진Unit 진입감지는 공백 수만 다른 태그가 두 개 있다(2+2칸 / 2+1칸).
EXTRA_SIG_TAGS = ["후진Unit  캐리어 진입감지"]
CNT_TAGS = [c for c, _ in COUNTERS.values()]
SIG_TAGS = [s for _, s in COUNTERS.values()]

FLAT_SEC = 30      # 이 시간 이상 값이 그대로면 멈춘 것
ABN_MAX = 100      # 리셋 직전 값이 이 이하면 오작동 리셋
SHIFT_S = {"MD_CY_FWD": 6.0}
#: Pour 기준 번호 → 그 센서 앞 실제 대차 번호 차이 (모듈 설명 '실제 대차 번호' 참고)
LABEL_OFFSET = {"Pour_Zone": 0, "Conv_Stop": 0, "MD_CY_FWD": 27, "Opening": 26, "Closing": 2, "MD_CY_REV": 41}


# ---------------------------------------------------------------------- #
# 데이터
# ---------------------------------------------------------------------- #

def pull(start, end, cache_path=None):
    """카운터·센서와 대차 번호를 받아 한 표로. 대차 번호는 바뀔 때만 적재돼서 따로 받아 ffill 한다."""
    if cache_path and os.path.exists(cache_path):
        return pd.read_pickle(cache_path)
    from Log_Extractor import LogExtractor
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    ex = LogExtractor(env_path=os.path.join(root, ".env"))   # 노트북을 parkkt/conv 에서 돌려도 .env 를 찾게
    raw = ex.get_data(start, end, CNT_TAGS + SIG_TAGS + EXTRA_SIG_TAGS)
    wag = ex.get_data(start, end, [WAGON])
    df = _clean(raw).join(_clean(wag)[[WAGON]], how="outer")
    df[WAGON] = df[WAGON].ffill()
    df = df[df[CNT_TAGS[0]].notna()]
    if cache_path:
        df.to_pickle(cache_path)
    return df


def _clean(df):
    df = df.copy()
    df.index = pd.to_datetime(df.index)
    return df[~df.index.duplicated(keep="first")].sort_index()


# ---------------------------------------------------------------------- #
# 사이클
# ---------------------------------------------------------------------- #

def find_flats(df, sec=FLAT_SEC):
    """카운터별로 값이 sec 초 이상 그대로인 구간. DB 가 끊긴 구간도 값이 안 바뀐 것이라 같이 잡힌다."""
    t = df.index
    rows = []
    for name, (cnt, _) in COUNTERS.items():
        x = df[cnt].values
        chg = np.flatnonzero(np.r_[True, x[1:] != x[:-1]])
        a = t[chg]
        b = t[np.r_[chg[1:], len(t) - 1]]
        d = (b - a).total_seconds()
        for s, e, dd in zip(a[d >= sec], b[d >= sec], d[d >= sec]):
            rows.append({"counter": name, "start": s, "end": e, "sec": dd})
    # 샘플 자체가 sec 초 넘게 빈 구간(받지 않은 날 사이 등). 앞뒤 값이 달라도 그 사이는 모른다.
    dt = np.diff(t.values) / np.timedelta64(1, "s")
    for i in np.flatnonzero(dt >= sec):
        rows.append({"counter": "(DB 빈 구간)", "start": t[i], "end": t[i + 1], "sec": dt[i]})
    return pd.DataFrame(rows, columns=["counter", "start", "end", "sec"])


def pour_cycles(df, flats):
    """Pour 리셋 → 다음 Pour 리셋. 대차 번호는 사이클 가운데 시각의 값(바뀌는 시각이 0~2초 늦을 때가 있다)."""
    t = df.index
    x = df[COUNTERS["Pour_Zone"][0]].values
    rp = np.flatnonzero(np.r_[False, x[1:] < x[:-1]])
    cyc = pd.DataFrame({"start": t[rp[:-1]], "end": t[rp[1:]]})
    cyc["dur_s"] = (cyc["end"] - cyc["start"]).dt.total_seconds()
    cyc["pour_max"] = [x[a:b].max() for a, b in zip(rp[:-1], rp[1:])]
    mid = cyc["start"] + (cyc["end"] - cyc["start"]) / 2
    w = df[WAGON].dropna()
    cyc["wagon"] = w.reindex(w.index.union(mid)).ffill().loc[mid].values.astype(int)
    fs, fe = flats["start"].values, flats["end"].values
    cyc["flat"] = [bool(((fs < b) & (fe > a)).any()) for a, b in zip(cyc["start"].values, cyc["end"].values)]
    cyc["date"] = cyc["start"].dt.date
    return cyc


# ---------------------------------------------------------------------- #
# 리셋 / 통과
# ---------------------------------------------------------------------- #

def counter_resets(df, name, cyc, abn_max=ABN_MAX, tol_ticks=1, sig=None):
    """카운터 1개의 리셋 전부. 통과 번호(burst)와 붙은 Pour 사이클(ci)·대차 번호 포함. sig 로 센서 태그를 바꿔 볼 수 있다."""
    cnt, sig0 = COUNTERS[name]
    sig = sig or sig0
    t = df.index
    x = df[cnt].values
    rp = np.flatnonzero(np.r_[False, x[1:] < x[:-1]])
    before = x[rp - 1]
    abn = before <= abn_max
    burst = np.cumsum(~abn)                     # 정상 리셋마다 새 통과. 첫 정상 리셋 전의 오작동은 burst 0
    on = (df[sig].fillna(0) != 0).astype(int)
    sig_near = on.rolling(2 * tol_ticks + 1, center=True, min_periods=1).max().values[rp].astype(bool)
    r = pd.DataFrame({"time": t[rp], "before": before, "after": x[rp], "abnormal": abn, "burst": burst,
                      "sig_near": sig_near, "pour_cnt": df[COUNTERS["Pour_Zone"][0]].values[rp]})
    r = r[r["burst"] > 0]
    # 통과의 첫 리셋 시각 + shift 로 Pour 사이클을 정한다
    t0 = r.groupby("burst")["time"].transform("first") + pd.Timedelta(seconds=SHIFT_S.get(name, 0.0))
    ci = np.searchsorted(cyc["start"].values, t0.values, side="right") - 1
    ok = (ci >= 0) & (t0.values < cyc["end"].values[np.clip(ci, 0, None)])
    r = r[ok].copy()
    r["ci"] = ci[ok]
    r["pour_wagon"] = cyc["wagon"].values[r["ci"]]
    r["wagon"] = (r["pour_wagon"] - LABEL_OFFSET.get(name, 0) - 1) % N_WAGON + 1
    r["flat"] = cyc["flat"].values[r["ci"]]
    r["counter"] = name
    return r.reset_index(drop=True)


def bursts(resets):
    """통과 1행. n_reset ≥ 2 면 오작동 통과."""
    g = resets.groupby("burst")
    b = pd.DataFrame({
        "counter": g["counter"].first(), "time": g["time"].first(), "ci": g["ci"].first(),
        "wagon": g["wagon"].first(), "pour_wagon": g["pour_wagon"].first(), "flat": g["flat"].first(),
        "n_reset": g.size(), "n_abn": g["abnormal"].sum(),
        "before_normal": g["before"].first(),
        "pour_cnt": g["pour_cnt"].first(),
    })
    b["abnormal"] = b["n_abn"] > 0
    return b.reset_index()


def per_cycle_check(cyc, b):
    """남긴 Pour 사이클마다 통과가 정확히 1번인지(사용자 전제: 한 사이클에 다른 카운터도 리셋 한 번)."""
    k = cyc[~cyc["flat"]]
    n = b[~b["flat"]].groupby("ci").size().reindex(k.index, fill_value=0)
    return n.value_counts().sort_index()


# ---------------------------------------------------------------------- #
# 요약
# ---------------------------------------------------------------------- #

def summary(cyc, B):
    """카운터별 오작동 통과 수·비율과 오작동 리셋 수."""
    k = cyc[~cyc["flat"]]
    rows = []
    for name, b in B.items():
        b = b[~b["flat"]]
        rows.append({"counter": name, "pour_cycles": len(k), "passes": len(b),
                     "abn_passes": int(b["abnormal"].sum()),
                     "abn_pct": round(b["abnormal"].mean() * 100, 2),
                     "abn_resets": int(b["n_abn"].sum()),
                     "wagons_hit": int(b.loc[b["abnormal"], "wagon"].nunique())})
    return pd.DataFrame(rows).set_index("counter")


def wagon_table(cyc, b):
    """대차 번호별 통과 수·오작동 통과 수·비율. p 는 '모든 대차가 같은 비율'일 때 이만큼 몰릴 확률(이항 단측)."""
    from scipy.stats import binomtest
    b = b[~b["flat"]]
    passes = b.groupby("wagon").size().reindex(range(1, N_WAGON + 1), fill_value=0)
    hit = b[b["abnormal"]].groupby("wagon").size().reindex(passes.index, fill_value=0)
    extra = b.groupby("wagon")["n_abn"].sum().reindex(passes.index, fill_value=0)
    p0 = hit.sum() / max(passes.sum(), 1)
    out = pd.DataFrame({"passes": passes, "abn_passes": hit, "abn_pct": (hit / passes.replace(0, np.nan) * 100).round(1),
                        "abn_resets": extra})
    out["p_value"] = [binomtest(int(h), int(n), p0, alternative="greater").pvalue if n and p0 > 0 else np.nan
                      for h, n in zip(hit, passes)]
    out.index.name = "wagon"
    return out


def wagon_by_date(b):
    """대차 × 날짜 오작동 비율(%) — 날마다 같은 대차인지 확인."""
    b = b[~b["flat"]].copy()
    b["date"] = b["time"].dt.date
    return (b.pivot_table(index="wagon", columns="date", values="abnormal", aggfunc="mean") * 100).round(1)


# ---------------------------------------------------------------------- #
# 대차별 상세 (통과 단위)
# ---------------------------------------------------------------------- #

def add_pass_context(df, cyc, b, gap_sec=0.9):
    """통과마다 첫 리셋 앞뒤 ±3샘플 안 최대 샘플 간격(max_dt)과 db_gap(> gap_sec) 을 붙인다.
    db_gap 인 정상 통과는 센서가 깜빡였어도 DB 가 못 봤을 수 있다."""
    t = df.index
    dt = np.r_[np.nan, np.diff(t.values) / np.timedelta64(1, "s")]
    pos = t.get_indexer(b["time"])
    b = b.copy()
    b["max_dt"] = [np.nanmax(dt[max(p - 3, 1):p + 4]) for p in pos]
    b["db_gap"] = b["max_dt"] > gap_sec
    return b


def _runs(a):
    """bool 배열의 (값, 길이) 연속 구간."""
    if len(a) == 0:
        return np.array([], bool), np.array([], int)
    edge = np.flatnonzero(np.r_[True, a[1:] != a[:-1], True])
    return a[edge[:-1]], np.diff(edge)


def wagon_detail(b, wagons, hot=50):
    """대차별 통과 단위 요약.

    전체 기간 / '걸린 날'(그 날 오작동 ≥ hot%) 안에서의 비율과, 걸린 날 안에서 정상 통과가 얼마나 끼었는지.
    - real_normal: 걸린 날 정상 통과 중 DB 끊김(db_gap) 아닌 것 = 실제로 깨끗하게 지나간 것
    - longest_normal: 걸린 날 안에서 정상 통과가 가장 길게 이어진 횟수
    - hours_100: 걸린 날 안 1시간(통과 4회 이상) 중 전부 오작동인 시간 비율
    """
    b = b[~b["flat"]]
    rows = []
    for w in wagons:
        x = b[b["wagon"] == w].sort_values("time")
        day = x["time"].dt.date
        dpct = x.groupby(day)["abnormal"].mean() * 100
        hot_days = dpct.index[dpct >= hot]
        y = x[day.isin(hot_days)]
        vals, lens = _runs(y["abnormal"].values)
        h = y.groupby(y["time"].dt.floor("h"))["abnormal"].agg(["size", "mean"])
        h = h[h["size"] >= 4]
        nm = y[~y["abnormal"]]
        rows.append({
            "wagon": w, "passes": len(x), "abn_pct": round(x["abnormal"].mean() * 100, 1),
            "hot_days": len(hot_days), "of_days": len(dpct),
            "first_hot": min(hot_days).strftime("%m-%d") if len(hot_days) else "",
            "last_hot": max(hot_days).strftime("%m-%d") if len(hot_days) else "",
            "hot_abn_pct": round(y["abnormal"].mean() * 100, 2) if len(y) else np.nan,
            "hot_normal": len(nm), "hot_real_normal": int((~nm["db_gap"]).sum()),
            "hot_real_abn_pct": round(100 - (~nm["db_gap"]).sum() / max(len(y), 1) * 100, 2) if len(y) else np.nan,
            "longest_normal": int(lens[~vals].max()) if (~vals).any() else 0,
            "hours_100": round((h["mean"] == 1).mean() * 100, 1) if len(h) else np.nan,
            "resets_2/3/4+": "/".join(str(int(v)) for v in [(x["n_reset"] == 2).sum(), (x["n_reset"] == 3).sum(), (x["n_reset"] >= 4).sum()]),
        })
    return pd.DataFrame(rows).set_index("wagon") if rows else pd.DataFrame()


def find_switches(B, win=15, jump=0.8, min_abn=30, stop_min=11.0):
    """대차별로 '켜짐(오작동 계속) ↔ 꺼짐' 이 바뀐 시점.

    같은 대차의 통과 순서에서, 앞 win 회와 뒤 win 회 오작동 비율 차이가 jump 이상인 곳.
    붙어 있는 후보(win 회 안)는 하나로 묶고 차이가 가장 큰 곳(같으면 직전 간격이 긴 곳)을 고른다.
    gap_min = 직전 통과와의 간격(분). 한 바퀴(42대) ≈ 9.1분이라 stop_min 보다 길면 그 사이에 정지가 있었다.
    반환: (switches, base_stop_pct) — base_stop_pct 는 아무 통과 사이에 정지가 낀 비율(비교 기준).
    """
    out, gaps = [], []
    for n, b in B.items():
        b = b[~b["flat"]]
        for w, x in b.groupby("wagon"):
            if x["abnormal"].sum() < min_abn:
                continue
            x = x.sort_values("time").reset_index(drop=True)
            a = x["abnormal"].values.astype(float)
            gap = x["time"].diff().dt.total_seconds().div(60).values
            gaps.append(gap[1:])
            cs = np.r_[0, np.cumsum(a)]
            idx = np.arange(win, len(a) - win + 1)
            d = (cs[idx + win] - cs[idx]) / win - (cs[idx] - cs[idx - win]) / win
            cand = idx[np.abs(d) >= jump]
            if len(cand) == 0:
                continue
            grp = np.r_[0, np.cumsum(np.diff(cand) >= win)]
            for gi in np.unique(grp):
                c = cand[grp == gi]
                dd = np.abs(d[c - win])
                best = c[dd == dd.max()]
                i = best[np.argmax(gap[best])]
                out.append({"counter": n, "wagon": w, "time": x["time"][i], "dir": "켜짐" if d[i - win] > 0 else "꺼짐",
                            "before_pct": round((cs[i] - cs[i - win]) / win * 100),
                            "after_pct": round((cs[i + win] - cs[i]) / win * 100),
                            "gap_min": round(gap[i], 1), "after_stop": bool(gap[i] > stop_min)})
    g = np.concatenate(gaps) if gaps else np.array([])
    return pd.DataFrame(out).sort_values("time").reset_index(drop=True), float((g > stop_min).mean() * 100) if len(g) else np.nan


# ---------------------------------------------------------------------- #
# 엔코더 위치 / 대차 간격 (해석용)
# ---------------------------------------------------------------------- #

def encoder(df):
    """여섯 카운터는 같은 엔코더로 올라간다(샘플 99.7% 에서 증가량 동일). 샘플마다 증가량 중앙값을 누적한 위치."""
    d = np.diff(df[CNT_TAGS].values, axis=0)
    with np.errstate(all="ignore"):
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            step = np.nanmedian(np.where(d > 0, d, np.nan), axis=1)
    return np.r_[0, np.cumsum(np.nan_to_num(step, nan=0.0))]


def pass_geometry(df, R_n, enc=None):
    """통과마다 리셋들의 엔코더 위치. 리셋 위치 = 그 샘플의 엔코더 - 리셋 뒤 카운트.
    pitch     : 직전 통과 마지막(정위치) 리셋 → 이번 통과 마지막 리셋 = 대차 간격
    early     : 이번 첫 리셋이 마지막 리셋보다 몇 카운트 일찍 났나(정상 통과 0)
    gaps      : 오작동 통과에서 리셋 사이 간격들(카운트)
    """
    enc = encoder(df) if enc is None else enc
    r = R_n[~R_n["flat"]].copy()
    r["enc"] = enc[df.index.get_indexer(r["time"])] - r["after"].values
    g = r.groupby("burst")
    p = pd.DataFrame({"wagon": g["wagon"].first(), "time": g["time"].first(), "n_reset": g.size(),
                      "first": g["enc"].first(), "last": g["enc"].last(),
                      "gaps": g["enc"].apply(lambda s: tuple(np.diff(s.values).round().astype(int)))}).sort_values("time")
    cont = p["time"].diff().dt.total_seconds() < 30
    p["pitch"] = p["last"].diff().where(cont)
    p["early"] = p["last"] - p["first"]
    p["abnormal"] = p["n_reset"] > 1
    return p.reset_index(drop=True)


# ---------------------------------------------------------------------- #
# 라인 알람(AL_Line_Word) 과 오작동 리셋
# ---------------------------------------------------------------------- #

def alarm_events(alarm_df, pour=None, word="AL_Line_Word"):
    """AL_Line_Word 가 0 이 아닌 값으로 바뀐 구간 = 알람 1건 (code, start, end, dur_s).
    pour(Pour 카운트 Series)를 주면 moving_before: 시작 전 10초 안에 컨베이어가 움직였나, since_move_s: 마지막으로 움직인 뒤 몇 초."""
    w = alarm_df[word].ffill()
    chg = w.index[w.ne(w.shift())]
    v = w.loc[chg].values
    rows = []
    for i, (t, c) in enumerate(zip(chg, v)):
        if pd.isna(c) or c == 0:
            continue
        end = chg[i + 1] if i + 1 < len(chg) else w.index[-1]
        rows.append({"code": int(c), "start": t, "end": end, "dur_s": (end - t).total_seconds()})
    E = pd.DataFrame(rows, columns=["code", "start", "end", "dur_s"])
    if pour is not None and len(E):
        mv = pour.index[np.r_[False, np.diff(pour.values) != 0]].values
        k = np.searchsorted(mv, E["start"].values) - 1
        E["since_move_s"] = np.where(k >= 0, (E["start"].values - mv[np.clip(k, 0, None)]) / np.timedelta64(1, "s"), np.nan)
        E["moving_before"] = E["since_move_s"] <= 10
    return E


def alarm_link(E, R, win_s=2.0):
    """알람마다 시작 직전 win_s 초 안에 오작동 리셋이 있었던 카운터와 그 대차.
    반환: (이벤트별 표, 코드×카운터 비율 %)"""
    rows = []
    for _, e in E.iterrows():
        row = {"code": e["code"], "start": e["start"], "dur_s": e["dur_s"]}
        for n, r in R.items():
            x = r[r["abnormal"] & (r["time"] <= e["start"]) & (r["time"] > e["start"] - pd.Timedelta(seconds=win_s))]
            row[n] = ",".join(str(w) for w in sorted(x["wagon"].unique())) if len(x) else ""
        rows.append(row)
    L = pd.DataFrame(rows)
    rate = (L.groupby("code")[list(R)].agg(lambda s: (s != "").mean() * 100)).round(1)
    rate.insert(0, "events", L.groupby("code").size())
    return L, rate


def baseline_rate(df, R, win_s=2.0, n=3000, seed=0):
    """컨베이어가 움직이는 아무 시각 n 개에서, 직전 win_s 초 안에 오작동 리셋이 있었던 비율 % (알람과 비교 기준)."""
    P = df[COUNTERS["Pour_Zone"][0]]
    mv = df.index[np.r_[False, np.diff(P.values) != 0]]
    t = np.sort(np.random.default_rng(seed).choice(mv.values, min(n, len(mv)), replace=False))
    out = {}
    for name, r in R.items():
        at = np.sort(r.loc[r["abnormal"], "time"].values)
        k = np.searchsorted(at, t, side="right") - 1
        lag = np.where(k >= 0, (t - at[np.clip(k, 0, None)]) / np.timedelta64(1, "s"), np.inf)
        out[name] = round((lag <= win_s).mean() * 100, 1)
    return pd.Series(out)
