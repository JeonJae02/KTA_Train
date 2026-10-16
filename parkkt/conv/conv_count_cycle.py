"""컨베이어 존 카운터 사이클 분석 — 센서 이상 지표용.

카운터(예: Pour_Zone_CNT PV)는 인코더 펄스마다 올라가고, PLC 로직상 **존 센서가 켜져야만**
리셋된다. 리셋 사이를 한 사이클로 보고 사이클마다 리셋 직전 최대 카운트와 소요시간을 잰다.

지표를 두 종류로 나눈다.

센서 상태 지표 (센서 이상 판단에 쓴다)
  - cnt_max   : 리셋 직전 최대 카운트 = 이전 센서에서 이 센서까지 벨트가 움직인 거리.
                센서가 늦게/일찍 잡히거나 위치가 틀어지면 이 값이 평소 범위를 벗어난다.
  - duration_s: 리셋까지 걸린 시간. 속도 영향을 받으므로 cnt_max 보다 약한 지표다.

  - 두 번 감지(double): 대차 한 대가 지날 때 센서가 약 0.5초 간격으로 두 번 켜져 리셋이 두 번
                난 경우. 카운트 SHORT_CNT 이하의 짧은 사이클이 생기고, 이웃 사이클과 더하면 정상값이
                된다. 사이클이 쪼개지므로 cnt_max 통계에서 빼고(main), 비율을 따로 센다.

데이터 품질 지표 (센서 이상 판단에 쓰면 안 된다)
  - 리셋은 됐는데 DB 에 센서 신호가 없는 경우. 리셋이 됐다는 것 자체가 PLC 에서 센서가
    켜졌다는 뜻이므로, 이건 서버 수집(약 0.5초 간격)이 짧은 펄스를 놓친 것이다.
    대부분 리셋 직전 샘플 간격이 평소(0.52초)보다 길다.

통계(평균/표준편차/범위)는 clean 사이클로만 낸다. 제외 조건:
  has_stop  : 사이클 안에 정지 시작~재가동 램프가 있음 (Conv_Run_Warning==0, Conv_Warning_Time!=30, RUN==0)
  has_gap   : 사이클 안에서 수집이 2초 넘게 끊김
  end_gap   : 리셋 직전 샘플 간격이 0.9초 초과 → 마지막 카운트를 놓쳐 cnt_max 가 낮게 찍힐 수 있음
  has_alarm : AL_Line_Bit == 1
  tod_window: 사출 시작(07:00~07:30, 23:50~00:20), 점심(10:50~11:30), 저녁(19:20~20:00).
              데이터로는 이 시간대의 카운트·두 번 감지율이 다른 시간과 같았지만(±0.2), 보수적으로 뺀다.
  off_day   : 주말(토·일)과 추석 연휴(2026-09-24~26). 사이클이 끝난 날짜 기준이라
              금요일 야간조가 토요일 0시를 넘긴 부분도 빠진다.
main = clean 이면서 두 번 감지로 쪼개진 사이클(짧은 사이클과 그 앞뒤)이 아닌 것. cnt_max 통계는 main 으로 낸다.

주의: 리셋 순간 DB 에 센서 신호가 보이는 비율은 센서마다 다르다. Pour_Zone 의 TMON_SIG 는
100% 보이지만, 존 감지 센서(Opening/Closing Zone In Detect 등)는 펄스가 짧아 55~75%만 보인다.
그래서 sig_lost 는 Pour_Zone 에서만 수집 품질 지표로 의미가 있다.
"""
import os

import numpy as np
import pandas as pd

RUN = "CONVEYOR RUN / STOP PL_HMI"
RUN_WARN = "Conv_Run_Warning"      # 정상 1. 0 으로 떨어지는 순간이 정지 시작
WARN_TIME = "Conv_Warning_Time"    # 정상 30. 정지 중 0, 재가동 때 약 3초간 0→30 으로 오름
ALARM = "AL_Line_Bit"
STATE_TAGS = [RUN, RUN_WARN, WARN_TIME, ALARM]

#: 카운터별 리셋 센서. 센서는 값이 0 이 아니면 켜진 것으로 본다.
COUNTERS = {
    "Pour_Zone": ("Pour_Zone_CNT PV", ["캐리어진입 Check", "대차진입 TMON_SIG"]),
    "Conv_Stop": ("Conv_Stop_CNT PV", ["캐리어진입 쉬프트 Check"]),
    "MD_CY_FWD": ("MD_CY_FWD_Zone_CNT PV", ["전진Unit  캐리어 진입감지"]),
    "Opening":   ("Opening_CNT PV", ["Opening Zone In Detect"]),
    "Closing":   ("Closing_CNT PV", ["Closing Zone In Detect"]),
    "Air_Bag":   ("Air_Bag_Zone_CNT PV", ["Closing Zone In Detect"]),
}
CNT, SIGS = COUNTERS["Pour_Zone"]

#: 샘플 간격이 이보다 크면 사이클 안에서 데이터가 끊긴 것으로 본다(정상 ~0.52초, 스냅샷 300초).
GAP_SEC = 2.0
#: 리셋 직전 샘플 간격이 이보다 크면 DB 누락으로 본다(정상 0.52초, 99.9% 분위 0.65초).
END_GAP_SEC = 0.9
#: 통계에서 뺄 요일(월=0 … 일=6)과 휴일.
OFF_WEEKDAYS = (5, 6)
HOLIDAYS = {pd.Timestamp(d).date() for d in ("2026-09-24", "2026-09-25", "2026-09-26")}  # 추석 연휴

#: 통계에서 뺄 시간대(KST, 시작~끝). 끝이 시작보다 이르면 자정을 넘는 구간이다.
#: 정지 자체는 has_stop 으로 이미 빠지고, 여기는 그 앞뒤 여유분이다.
EXCLUDE_TOD = {
    "사출 시작(주간)": ("07:00", "07:30"),
    "사출 시작(야간)": ("23:50", "00:20"),
    "점심": ("10:50", "11:30"),   # 실측 정지 10:50~11:00, 재가동 11:00~11:20
    "저녁": ("19:20", "20:00"),   # 실측 정지 19:20~19:50, 재가동 19:30~19:50
}

#: 리셋 직전 최대 카운트가 이 이하면 두 번 감지로 생긴 짧은 사이클로 본다(정상 ~137).
SHORT_CNT = 20


def _prep(raw_df, cols):
    df = raw_df[[c for c in cols if c in raw_df.columns]].copy()
    df.index = pd.to_datetime(df.index)
    return df[~df.index.duplicated(keep="first")].sort_index()


def is_off_day(ts):
    """주말·휴일이면 True. ts 는 Series[datetime]."""
    return ts.dt.weekday.isin(OFF_WEEKDAYS) | ts.dt.date.isin(HOLIDAYS)


def tod_window(ts, windows=None):
    """시각이 EXCLUDE_TOD 구간에 들면 그 이름, 아니면 "" 를 돌려준다. ts 는 Series[datetime]."""
    windows = EXCLUDE_TOD if windows is None else windows
    minute = ts.dt.hour * 60 + ts.dt.minute
    out = pd.Series("", index=ts.index, dtype=object)
    for name, (a, b) in windows.items():
        ha, ma = map(int, a.split(":")); hb, mb = map(int, b.split(":"))
        lo, hi = ha * 60 + ma, hb * 60 + mb
        hit = (minute >= lo) & (minute < hi) if lo < hi else (minute >= lo) | (minute < hi)
        out[hit & (out == "")] = name
    return out


def extract_count_cycles(raw_df, cnt=CNT, sigs=SIGS, tol_ticks=1, drop_off_days=True, drop_tod=True):
    """리셋(카운트 감소) 지점으로 사이클을 나눠 표로 돌려준다.

    반환: (cycles, resets)
      cycles : 사이클 1행 = 리셋 한 번에서 다음 리셋까지
      resets : 리셋 1행 = 리셋 순간의 센서 확인 결과와 직전 샘플 간격
    """
    df = _prep(raw_df, [cnt, *sigs, *STATE_TAGS])
    df = df[df[cnt].notna()]
    t = df.index.to_series()
    dt = t.diff().dt.total_seconds()
    c = df[cnt]

    sig = pd.Series(False, index=df.index)
    for s in sigs:
        if s in df:
            sig |= df[s].fillna(0) != 0
    # 리셋 순간 ± tol_ticks 샘플 안에 한 번이라도 켜졌으면 DB 에서 확인된 것으로 본다.
    sig_near = sig.astype(int).rolling(2 * tol_ticks + 1, center=True, min_periods=1).max().astype(bool)

    is_reset = (c.diff() < 0) & (dt <= GAP_SEC)
    rp = np.flatnonzero(is_reset.values)

    resets = pd.DataFrame({
        "reset_time": t.iloc[rp].values,
        "count_before": c.shift(1).iloc[rp].values,
        "count_after": c.iloc[rp].values,
        "dt_before": dt.iloc[rp].values,
        "sig_near_reset": sig_near.iloc[rp].values,
    })
    for s in sigs:
        if s in df:
            resets[s] = df[s].iloc[rp].values
    # 리셋은 PLC 에서 센서가 켜졌다는 뜻 → DB 에 신호가 없으면 수집 누락이다.
    resets["sig_lost"] = ~resets["sig_near_reset"]
    resets["sig_lost_db_gap"] = resets["sig_lost"] & (resets["dt_before"] > END_GAP_SEC)

    # 정지·알람 행 누적합 → 사이클 구간에 포함됐는지 O(1)
    stop_row = np.zeros(len(df), dtype=bool)
    if RUN_WARN in df:
        stop_row |= (df[RUN_WARN] == 0).values
    if WARN_TIME in df:
        stop_row |= (df[WARN_TIME] != 30).values
    if RUN in df:
        stop_row |= (df[RUN] == 0).values
    alarm_row = (df[ALARM] == 1).values if ALARM in df else np.zeros(len(df), dtype=bool)
    cs_stop = np.concatenate([[0], np.cumsum(stop_row)])
    cs_alarm = np.concatenate([[0], np.cumsum(alarm_row)])
    big_gap = (dt > GAP_SEC).values
    cs_gap = np.concatenate([[0], np.cumsum(big_gap)])

    rows = []
    for a, b in zip(rp[:-1], rp[1:]):
        seg = c.iloc[a:b]                       # 리셋 직후 ~ 다음 리셋 직전
        rows.append({
            "start_time": t.iloc[a],
            "end_time": t.iloc[b],
            "duration_s": (t.iloc[b] - t.iloc[a]).total_seconds(),
            "n_samples": len(seg),
            "cnt_first": seg.iloc[0],
            "cnt_mean": seg.mean(),
            "cnt_max": seg.max(),
            "cnt_max_time": seg.idxmax(),
            "end_dt": dt.iloc[b],
            "has_gap": bool(cs_gap[b] - cs_gap[a + 1] > 0),
            "end_gap": bool(dt.iloc[b] > END_GAP_SEC),
            "has_stop": bool(cs_stop[b + 1] - cs_stop[a] > 0),
            "has_alarm": bool(cs_alarm[b + 1] - cs_alarm[a] > 0),
        })
    cycles = pd.DataFrame(rows)
    if cycles.empty:
        return cycles, resets
    cycles["off_day"] = is_off_day(cycles["end_time"])
    cycles["clean"] = ~(cycles["has_gap"] | cycles["end_gap"] | cycles["has_stop"] | cycles["has_alarm"])
    if drop_off_days:
        cycles["clean"] &= ~cycles["off_day"]
    # 시작·끝 중 하나라도 제외 시간대에 걸리면 그 사이클을 뺀다
    w_start, w_end = tod_window(cycles["start_time"]), tod_window(cycles["end_time"])
    cycles["tod_window"] = w_end.where(w_end != "", w_start)
    if drop_tod:
        cycles["clean"] &= cycles["tod_window"] == ""
    cycles["short"] = cycles["cnt_max"] <= SHORT_CNT
    cycles["split"] = cycles["short"] | cycles["short"].shift(1, fill_value=False) | cycles["short"].shift(-1, fill_value=False)
    cycles["main"] = cycles["clean"] & ~cycles["split"]
    # k_clean: 마지막으로 제외된 사이클 이후 몇 번째 clean 사이클인지(재가동 직후 확인용)
    grp = (~cycles["clean"]).cumsum()
    cycles["k_clean"] = cycles["clean"].groupby(grp).cumsum().where(cycles["clean"], 0)
    cycles["date"] = cycles["end_time"].dt.date
    cycles["hour"] = cycles["end_time"].dt.hour
    return cycles, resets


def extract_stop_events(raw_df, resets, cnt=CNT, move_window_s=3.0):
    """Conv_Run_Warning 1→0 = 정지 시작. 그 순간의 카운트와 직전 리셋 후 경과시간.

    재가동 램프 중에도 Run_Warning 이 0 으로 깜빡이는데(카운트가 안 움직임), 그건 정지 시작이 아니다.
    직전 move_window_s 초 동안 카운트가 실제로 올라가고 있던 것만 정지 시작으로 본다.
    """
    df = _prep(raw_df, [cnt, *STATE_TAGS])
    w, c, idx = df[RUN_WARN], df[cnt], df.index
    fall_pos = np.flatnonzero(((w.shift(1) == 1) & (w == 0)).values)
    reset_times = pd.DatetimeIndex(resets["reset_time"])
    up = (c.diff() > 0).astype(int)

    rows = []
    for p in fall_pos:
        t = idx[p]
        lo = idx.searchsorted(t - pd.Timedelta(seconds=move_window_s))
        if up.iloc[lo:p + 1].mean() < 0.5:
            continue
        j = reset_times.searchsorted(t) - 1
        t_reset = reset_times[j] if j >= 0 else pd.NaT
        wt_after = df[WARN_TIME].iloc[p:]
        back = wt_after[wt_after == 30]
        t_restart = back.index[0] if len(back) else pd.NaT
        seg = c.iloc[p:idx.searchsorted(t_restart if pd.notnull(t_restart) else idx[-1]) + 1]
        still = (seg.diff() == 0).values          # 첫 행의 diff 는 NaN → False
        frozen = seg.iloc[still.argmax()] if still.any() else np.nan
        rows.append({
            "stop_time": t,
            "cnt_at_stop": c.iloc[p],
            "since_reset_s": (t - t_reset).total_seconds() if pd.notnull(t_reset) else np.nan,
            "cnt_frozen": frozen,
            # 감속 중에 리셋이 끼면 frozen < cnt_at_stop 이라 감속량을 못 구한다 → NaN
            "cnt_coast": (frozen - c.iloc[p]) if frozen >= c.iloc[p] else np.nan,
            "restart_time": t_restart,
            "stopped_s": (t_restart - t).total_seconds() if pd.notnull(t_restart) else np.nan,
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------- #
# 요약 표
# ---------------------------------------------------------------------- #

def health_summary(cycles, resets, name=""):
    """카운터 1개의 센서 상태 지표 + 데이터 품질 지표를 한 행으로."""
    cl = cycles[cycles["clean"]]
    c = cycles[cycles["main"]]
    q = c["cnt_max"].quantile([0.005, 0.995])
    return pd.Series({
        "counter": name,
        "cycles": len(cycles),
        "clean": len(cl),
        "double_detect": int(cl["short"].sum()),
        "double_pct": cl["short"].mean() * 100,
        "main": len(c),
        "excl_stop": int(cycles["has_stop"].sum()),
        "excl_gap": int((cycles["has_gap"] | cycles["end_gap"]).sum()),
        "excl_alarm": int(cycles["has_alarm"].sum()),
        "excl_off_day": int(cycles["off_day"].sum()),
        "excl_tod": int((cycles["tod_window"] != "").sum()),
        "cnt_max_mean": c["cnt_max"].mean(),
        "cnt_max_std": c["cnt_max"].std(),
        "cnt_max_mode": c["cnt_max"].mode().iloc[0] if len(c) else np.nan,
        "cnt_max_min": c["cnt_max"].min(),
        "cnt_max_min_time": c.loc[c["cnt_max"].idxmin(), "end_time"] if len(c) else pd.NaT,
        "cnt_max_max": c["cnt_max"].max(),
        "cnt_max_max_time": c.loc[c["cnt_max"].idxmax(), "end_time"] if len(c) else pd.NaT,
        "cnt_max_p0.5": q.iloc[0],
        "cnt_max_p99.5": q.iloc[1],
        "dur_median": c["duration_s"].median(),
        "dur_p90": c["duration_s"].quantile(0.9),
        "resets": len(resets),
        "sig_lost": int(resets["sig_lost"].sum()),
        "sig_lost_db_gap": int(resets["sig_lost_db_gap"].sum()),
    })


def value_table(cycles, col="cnt_max"):
    """clean 사이클의 값별 개수/비율과, 그 값이 처음·마지막으로 나온 시각."""
    c = cycles[cycles["clean"]]
    g = c.groupby(col)["end_time"]
    out = pd.DataFrame({"n": g.size(), "first": g.min(), "last": g.max()})
    out["pct"] = out["n"] / out["n"].sum() * 100
    out["cum_pct"] = out["pct"].cumsum()
    return out


def extreme_cycles(cycles, n=10, col="cnt_max"):
    """main 사이클 중 값이 가장 작은/큰 n 개 (시각 포함)."""
    c = cycles[cycles["main"]]
    cols = ["end_time", col, "duration_s", "end_dt", "k_clean"]
    return c.nsmallest(n, col)[cols], c.nlargest(n, col)[cols]


def group_stats(cycles, by):
    """날짜/시간대별 요약. 두 번 감지 비율은 clean 기준, 나머지는 main 기준. by: 'date' | 'hour' """
    cl = cycles[cycles["clean"]]
    dbl = cl.groupby(by)["short"].agg(clean_n="size", double_n="sum")
    dbl["double_pct"] = dbl["double_n"] / dbl["clean_n"] * 100
    c = cycles[cycles["main"]]
    return dbl.join(c.groupby(by).agg(n=("duration_s", "size"),
                             dur_med=("duration_s", "median"),
                             dur_p90=("duration_s", lambda x: x.quantile(0.9)),
                             max_mean=("cnt_max", "mean"),
                             max_std=("cnt_max", "std"),
                             max_min=("cnt_max", "min"),
                             max_max=("cnt_max", "max")))


# ---------------------------------------------------------------------- #
# 그래프
# ---------------------------------------------------------------------- #

def _font():
    import matplotlib.pyplot as plt
    plt.rc("font", family="Malgun Gothic" if os.name == "nt" else "AppleGothic")
    plt.rc("axes", unicode_minus=False)
    return plt


def plot_value_hist(cycles_by_name, fig_path=None):
    """카운터별 리셋 직전 최대 카운트 값의 개수 막대그래프(clean 만)."""
    plt = _font()
    names = list(cycles_by_name)
    fig, ax = plt.subplots(len(names), 1, figsize=(15, 3.6 * len(names)))
    ax = np.atleast_1d(ax)
    for a, name in zip(ax, names):
        vt = value_table(cycles_by_name[name])
        a.bar(vt.index, vt["n"], width=0.8, color="#2a6fdb")
        a.set_yscale("log")
        a.set_title(f"{name}: 리셋 직전 최대 카운트 값별 사이클 수 (clean {int(vt['n'].sum())}개, 로그축)", fontweight="bold")
        a.set_ylabel("사이클 수")
        a.grid(alpha=0.3, ls=":")
    fig.tight_layout()
    if fig_path:
        fig.savefig(fig_path, dpi=100)
    return fig


def plot_stop_start(stops, cycles, fig_path=None):
    """정지 시작 순간의 카운트/경과시간이 사이클 안 어디쯤인지."""
    plt = _font()
    clean = cycles[cycles["main"]]
    fig, ax = plt.subplots(1, 3, figsize=(18, 5))
    top = int(clean["cnt_max"].median())
    ax[0].hist(stops["cnt_at_stop"], bins=np.arange(0, top + 8, 8), color="#d62728", alpha=0.75, density=True, label=f"정지 시작 ({len(stops)}건)")
    ax[0].axhline(1 / top, color="k", ls="--", label="무작위 시점이라면(균등)")
    ax[0].set_xlabel("정지 시작 순간의 카운트"); ax[0].set_ylabel("밀도")
    ax[0].set_title("정지는 사이클의 어느 지점에서 시작하나", fontweight="bold"); ax[0].legend()
    s2 = stops[stops["since_reset_s"] <= 20]
    med = clean["duration_s"].median()
    ax[1].hist(s2["since_reset_s"], bins=np.arange(0, 14, 1), color="#d62728", alpha=0.75, density=True, label=f"정지 시작 ({len(s2)}건)")
    ax[1].axhline(1 / med, color="k", ls="--", label="균등")
    ax[1].axvline(med, color="#2a6fdb", label=f"평소 사이클 길이 {med:.1f}s")
    ax[1].set_xlabel("직전 리셋 후 경과시간 (s)")
    ax[1].set_title("정지 시작까지 걸린 시간 (리셋 기준)", fontweight="bold"); ax[1].legend()
    ax[2].hist(stops["cnt_coast"].dropna(), bins=20, color="#ff7f0e", alpha=0.8)
    ax[2].set_xlabel("정지 시작 후 더 올라간 카운트(감속량)")
    ax[2].set_title("감속량 분포", fontweight="bold")
    for a in ax:
        a.grid(alpha=0.3, ls=":")
    fig.tight_layout()
    if fig_path:
        fig.savefig(fig_path, dpi=110)
    return fig


def plot_by_date_hour(cycles, title="", fig_path=None):
    """날짜별 / 시간대별 상자그림과 날짜×시간 히트맵(main 만)."""
    plt = _font()
    c = cycles[cycles["main"]].copy()
    c["d"] = c["end_time"].dt.strftime("%m-%d")
    dates = sorted(c["d"].unique())
    fig, ax = plt.subplots(3, 2, figsize=(18, 15))
    for col, (val, lab) in enumerate([("duration_s", "리셋까지 걸린 시간 (s)"), ("cnt_max", "리셋 직전 최대 카운트")]):
        ax[0][col].boxplot([c.loc[c["d"] == d, val] for d in dates], labels=dates, showfliers=False)
        ax[0][col].set_title(f"{title} 날짜별 {lab}", fontweight="bold"); ax[0][col].tick_params(axis="x", rotation=90)
        data = [c.loc[c["hour"] == h, val] for h in range(24)]
        ax[1][col].boxplot([d if len(d) else [np.nan] for d in data], labels=range(24), showfliers=False)
        ax[1][col].set_title(f"{title} 시간대별 {lab}", fontweight="bold"); ax[1][col].set_xlabel("시(KST)")
        piv = c.pivot_table(index="d", columns="hour", values=val, aggfunc="median").reindex(columns=range(24))
        im = ax[2][col].imshow(piv.values, aspect="auto", cmap="viridis")
        ax[2][col].set_yticks(range(len(piv.index))); ax[2][col].set_yticklabels(piv.index, fontsize=7)
        ax[2][col].set_xticks(range(24)); ax[2][col].set_title(f"{title} 날짜×시간 중앙값: {lab}", fontweight="bold")
        fig.colorbar(im, ax=ax[2][col])
        for r in (0, 1):
            ax[r][col].grid(alpha=0.3, ls=":")
    fig.tight_layout()
    if fig_path:
        fig.savefig(fig_path, dpi=100)
    return fig


def plot_double_detect(cycles_by_name, fig_path=None):
    """카운터별 두 번 감지 비율 — 날짜별 / 시간대별."""
    plt = _font()
    fig, ax = plt.subplots(1, 2, figsize=(18, 5))
    for name, cy in cycles_by_name.items():
        cl = cy[cy["clean"]].copy()
        d = cl.groupby("date")["short"].agg(["size", "mean"])
        d = d[d["size"] >= 300]                       # 비가동일(표본 적음) 제외
        ax[0].plot(pd.to_datetime(d.index), d["mean"] * 100, ".-", label=name)
        h = cl.groupby("hour")["short"].agg(["size", "mean"])
        h = h[h["size"] >= 300]
        ax[1].plot(h.index, h["mean"] * 100, ".-", label=name)
    ax[0].set_title("날짜별 두 번 감지 비율 (사이클 300개 미만인 날 제외)", fontweight="bold")
    ax[1].set_title("시간대별 두 번 감지 비율 (사이클 300개 미만인 시간 제외)", fontweight="bold")
    ax[1].set_xlabel("시(KST)")
    for a in ax:
        a.set_ylabel("% (clean 사이클 중)"); a.set_yscale("symlog", linthresh=1); a.grid(alpha=0.3, ls=":"); a.legend()
    fig.tight_layout()
    if fig_path:
        fig.savefig(fig_path, dpi=100)
    return fig


# ---------------------------------------------------------------------- #
# 이상 판정 모델 (규칙 + 통계 기준선)
# ---------------------------------------------------------------------- #

class CounterHealthModel:
    """카운터 1개의 정상 기준을 학습하고, 사이클마다 이상 여부를 판정한다.

    고장 라벨이 없어서 학습형(ML) 모델 대신 정상 분포에서 기준을 뽑는다.
    규칙과 기준값은 아래와 같고, 모두 main 사이클(정지·끊김·알람·휴일·두 번 감지 제외)로 학습한다.

      no_reset   [오류] 카운트가 over_cnt 를 넘음 = 센서가 대차를 못 잡아 리셋이 안 됨
                        over_cnt = 학습 구간 최대 카운트 + 10
      cnt_single [경고] 리셋 직전 카운트가 [cnt_lo, cnt_hi] 밖 (사이클 1개)
                        cnt_lo = p0.05 - 3, cnt_hi = p99.95 + 3
      cnt_shift  [오류] 리셋 직전 카운트의 최근 W 사이클 중앙값이 [roll_lo, roll_hi] 밖
                        = 센서 감지 위치가 계속 틀어짐. 학습 구간 이동중앙값 min-1, max+1
      dur_long   [경고] 정지 없이 리셋까지 dur_hi 초 넘게 걸림 (p99.9 × 1.25)
      dur_shift  [경고] 최근 W 사이클 소요시간 중앙값이 roll_dur_hi 초과 (학습 최대 + 1)
      double     [경고] 최근 100 사이클 중 두 번 감지 수가 dbl_hi 이상
                        dbl_hi = 중앙값 + max(2, 3·√중앙값)  (포아송 3σ 근사)
    """
    W = 20
    DBL_W = 100

    def __init__(self, name=""):
        self.name = name
        self.th = {}

    def fit(self, cycles, raw_cnt_max=None):
        m = cycles[cycles["main"]]
        x, d = m["cnt_max"], m["duration_s"]
        rm = x.rolling(self.W).median().dropna()
        rd = d.rolling(self.W).median().dropna()
        dbl = cycles.loc[cycles["clean"], "short"].astype(int).rolling(self.DBL_W).sum().dropna()
        med_dbl = float(dbl.median())
        top = float(raw_cnt_max if raw_cnt_max is not None else x.max())
        self.th = {
            "over_cnt": top + 10,
            "cnt_lo": float(x.quantile(0.0005)) - 3,
            "cnt_hi": float(x.quantile(0.9995)) + 3,
            "roll_lo": float(rm.min()) - 1,
            "roll_hi": float(rm.max()) + 1,
            "dur_hi": round(float(d.quantile(0.999)) * 1.25, 2),
            "roll_dur_hi": round(float(rd.max()) + 1, 2),
            "dbl_median": med_dbl,
            "dbl_hi": med_dbl + max(2.0, 3 * np.sqrt(med_dbl)),
            "cnt_median": float(x.median()),
            "train_from": str(m["end_time"].min()),
            "train_to": str(m["end_time"].max()),
            "train_n": int(len(m)),
        }
        return self

    def score(self, cycles):
        """사이클마다 규칙별 True/False. 정지·끊김·알람·휴일 사이클은 판정하지 않는다(NaN 대신 False)."""
        th = self.th
        c = cycles.copy()
        ok = c["clean"]
        valid = c["main"]
        c["no_reset"] = c["cnt_max"] > th["over_cnt"]          # 정지 여부와 무관하게 본다
        c["cnt_single"] = valid & ((c["cnt_max"] < th["cnt_lo"]) | (c["cnt_max"] > th["cnt_hi"]))
        # 이동중앙값은 main 사이클만 이어 붙여 계산한다(두 번 감지가 잦으면 행 기준 창에 main 이 모자라다).
        rm = c.loc[valid, "cnt_max"].rolling(self.W).median().reindex(c.index)
        c["cnt_shift"] = valid & ((rm < th["roll_lo"]) | (rm > th["roll_hi"]))
        c["dur_long"] = ok & ~c["short"] & (c["duration_s"] > th["dur_hi"])
        rd = c.loc[valid, "duration_s"].rolling(self.W).median().reindex(c.index)
        c["dur_shift"] = valid & (rd > th["roll_dur_hi"])
        dbl = c.loc[ok, "short"].astype(int).rolling(self.DBL_W).sum().reindex(c.index)
        c["double"] = ok & (dbl >= th["dbl_hi"])
        c["error"] = c["no_reset"] | c["cnt_shift"]
        c["warning"] = c["cnt_single"] | c["dur_long"] | c["dur_shift"] | c["double"]
        return c

    RULES = ["no_reset", "cnt_shift", "cnt_single", "dur_long", "dur_shift", "double"]

    @staticmethod
    def events(scored, merge_min=10):
        """규칙별로 붙어 있는 판정을 하나의 이벤트로 묶는다(merge_min 분 안이면 같은 이벤트)."""
        out = []
        for rule in CounterHealthModel.RULES:
            t = scored.loc[scored[rule], "end_time"]
            if t.empty:
                continue
            new = t.diff() > pd.Timedelta(minutes=merge_min)
            for _, g in t.groupby(new.cumsum()):
                out.append({"rule": rule, "start": g.iloc[0], "end": g.iloc[-1], "n_cycles": len(g)})
        return pd.DataFrame(out, columns=["rule", "start", "end", "n_cycles"])

    def save(self, path):
        import json
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"name": self.name, "W": self.W, "DBL_W": self.DBL_W, "th": self.th}, f, ensure_ascii=False, indent=2)

    @classmethod
    def load(cls, path):
        import json
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
        m = cls(d["name"])
        m.W, m.DBL_W, m.th = d["W"], d["DBL_W"], d["th"]
        return m


def find_no_reset(raw_df, cnt, over_cnt):
    """센서가 대차를 못 잡으면 리셋이 안 돼서 사이클이 끝나지 않는다 → 원시 카운트로 직접 본다.

    카운트가 over_cnt 를 넘은 구간(연속 행 묶음)마다 시작·끝 시각과 최대값을 돌려준다.
    실시간에서는 '현재 카운트 > over_cnt' 한 줄이면 된다.
    """
    df = _prep(raw_df, [cnt])
    over = df[cnt] > over_cnt
    if not over.any():
        return pd.DataFrame(columns=["start", "end", "cnt_peak"])
    grp = (over != over.shift()).cumsum()[over]
    g = df.loc[over, cnt].groupby(grp)
    return pd.DataFrame({"start": g.apply(lambda s: s.index[0]), "end": g.apply(lambda s: s.index[-1]),
                         "cnt_peak": g.max()}).reset_index(drop=True)


SHORT_CLASSES = [
    "두 번 감지 (합 정상)",
    "세 번 이상 감지 (합 정상)",
    "합 부족 (숨은 리셋 추정)",
    "합 초과",
    "정지 중 감지 (합 정상)",
    "정지 중 (합 안 맞음)",
    "수집 끊김 (판단 불가)",
]


def classify_short_cycles(cycles, lo=128, hi=146):
    """짧은 사이클(cnt_max ≤ SHORT_CNT)을 묶음 단위로 분류한다.

    묶음 = 연속된 짧은 사이클들 + 바로 앞 사이클(일찍 끊긴 사이클).
    두 번 감지라면 묶음 합이 정상 한 사이클 거리(lo~hi)와 같아야 한다.
    반환: 묶음 1행 (start=첫 짧은 사이클 시각, n_short, prev, sum, situation, category)
    """
    cy = cycles.reset_index(drop=True)
    s = cy["short"].values
    v = cy["cnt_max"].values
    rows = []
    i = 1
    while i < len(cy):
        if s[i] and not s[i - 1]:
            j = i
            while j < len(cy) and s[j]:
                j += 1
            seg = cy.iloc[i - 1:j]
            total = v[i - 1:j].sum()
            if seg["has_stop"].any():
                situation = "정지 포함"
            elif (seg["has_gap"] | seg["end_gap"]).any():
                situation = "수집 끊김"
            else:
                situation = "정상 운전"
            ok = lo <= total <= hi
            if situation == "수집 끊김":
                cat = "수집 끊김 (판단 불가)"
            elif situation == "정지 포함":
                cat = "정지 중 감지 (합 정상)" if ok else "정지 중 (합 안 맞음)"
            elif ok:
                cat = "두 번 감지 (합 정상)" if j - i == 1 else "세 번 이상 감지 (합 정상)"
            else:
                cat = "합 부족 (숨은 리셋 추정)" if total < lo else "합 초과"
            rows.append({"start": cy.at[i, "start_time"], "n_short": j - i, "prev": v[i - 1], "sum": total,
                         "off_day": bool(seg["off_day"].any()), "situation": situation, "category": cat})
            i = j
        else:
            i += 1
    return pd.DataFrame(rows, columns=["start", "n_short", "prev", "sum", "off_day", "situation", "category"])


def short_class_table(groups):
    """분류 결과를 짧은 사이클 수와 % 로 요약한다(묶음 안 짧은 사이클 수로 센다)."""
    t = groups.groupby("category")["n_short"].sum().reindex(SHORT_CLASSES).fillna(0).astype(int)
    return pd.DataFrame({"짧은 사이클": t, "%": (t / t.sum() * 100).round(1)})


def find_reset_gaps(raw_df, cnt, thr=END_GAP_SEC, moving_only=True, drop_diagnostic=True):
    """리셋(카운트 감소) 직전 두 샘플 안에 수집 끊김(간격 > thr 초)이 있었던 경우를 모두 돌려준다.

    끊김이 GAP_SEC(2초) 이하면 리셋으로 인정되고 그 사이클은 end_gap, 넘으면 리셋으로 세지 않아
    앞뒤 사이클이 합쳐지고 has_gap 이 된다. 어느 쪽이든 통계에서는 빠진다.
    moving_only: 끊김 시작이나 리셋 순간에 정지 상태(Conv_Run_Warning==0, RUN==0,
                 Conv_Warning_Time!=30)였던 것은 뺀다 — 정지·재가동 중이라 리셋 직전 값 비교가 의미 없다.
    drop_diagnostic: 끊김 직전 행이나 리셋 행의 source 가 PREDICTIVE 가 아니면(DIAGNOSTIC) 뺀다.
                 DIAGNOSTIC 행은 예외 없이 다음 행까지 0.9초 넘게 끊긴다 — 서버가 진단 수집을 하느라 생긴 끊김이다.
    """
    df = _prep(raw_df, [cnt, *STATE_TAGS, "source"])
    df = df[df[cnt].notna()]
    t = df.index.to_series()
    dt = t.diff().dt.total_seconds()
    x = df[cnt]
    src = df["source"].astype(str).values if "source" in df else np.full(len(df), "PREDICTIVE")
    pos = np.flatnonzero(((x.diff() < 0) & ((dt > thr) | (dt.shift(1) > thr))).values)
    stop_row = np.zeros(len(df), dtype=bool)
    if RUN_WARN in df:
        stop_row |= (df[RUN_WARN] == 0).values
    if RUN in df:
        stop_row |= (df[RUN] == 0).values
    if WARN_TIME in df:
        stop_row |= (df[WARN_TIME] != 30).values
    rows = []
    for p in pos:
        # 두 샘플 중 더 크게 끊긴 쪽을 끊김으로 본다
        g = p if dt.iloc[p] >= dt.iloc[p - 1] else p - 1
        if moving_only and (stop_row[g - 1] or stop_row[p]):
            continue
        if drop_diagnostic and (src[g - 1] != "PREDICTIVE" or src[p] != "PREDICTIVE"):
            continue
        gap = dt.iloc[g]
        rows.append({
            "끊김_시작": t.iloc[g - 1],              # 마지막으로 받은 시각
            "끊김_끝": t.iloc[g],                   # 다시 받은 시각
            "끊김_초": round(gap, 3),
            "리셋_시각": t.iloc[p],
            "끊김_전_카운트": x.iloc[p - 1],          # DB 에 보인 리셋 직전 값
            "리셋_후_카운트": x.iloc[p],
            # 리셋 인정 여부는 extract_count_cycles 와 같은 규칙: 리셋 줄과 바로 앞 줄 간격이 GAP_SEC(2초) 이하
            "리셋_줄_간격_초": round(dt.iloc[p], 3),
            "구분": "리셋 인정(end_gap)" if dt.iloc[p] <= GAP_SEC else "리셋 미인정(has_gap)",
            "RUN": df[RUN].iloc[p] if RUN in df else np.nan,
            "Run_Warning": df[RUN_WARN].iloc[p] if RUN_WARN in df else np.nan,
            "Warning_Time": df[WARN_TIME].iloc[p] if WARN_TIME in df else np.nan,
            "끊김_전_source": src[g - 1],
            "리셋_source": src[p],
        })
    out = pd.DataFrame(rows)
    if len(out):
        out["날짜"] = out["리셋_시각"].dt.date
        out["요일"] = out["리셋_시각"].dt.day_name(locale=None).str[:3]
        out["휴일"] = is_off_day(out["리셋_시각"])
        out["제외_시간대"] = tod_window(out["리셋_시각"])
    return out
