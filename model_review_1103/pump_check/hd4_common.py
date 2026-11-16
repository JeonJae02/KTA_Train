"""P02·P03·P04 (4번 헤드 HD4 = P7 POL · I7 ISO) 노트북이 같이 쓰는 데이터·지표.

- 원시 데이터: cache/raw/pump/ 일별 CSV (pump_check/extract_raw_csv.py). 없는 태그만 DB(하루 캐시):
  재생용 사출 신호·알람 비트·가동 플래그(review_lib.pump_csv.REPLAY_AUX), HD4 릴레이(HD4_AUX).
- 재생: 운영 펌프 모델(v2.1 — 펌프 코드는 v2.2 와 같음)에 P7·I7 을 10-06 ~ 11-12 하루씩 흘린다.
  하루 약 1.5분. `python pump_check/hd4_common.py` 로 날짜별 병렬 실행하면 전 기간 약 10분
  (cache/replay/hd4_days/), 합친 결과는 cache/replay/hd4_P7I7_1006_1112.pkl.
"""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from review_lib import db, paths, pump_csv, replay  # noqa: E402

PUMPS = ["P7", "I7"]
START, END = pd.Timestamp("2026-10-06"), pd.Timestamp("2026-11-13")
REPLAY_CACHE = paths.CACHE_DIR / "replay" / "hd4_P7I7_1006_1112.pkl"
#: 순환 운전 Hz 지령 (pump_settings circ_hz: P7 25 Hz, I7 30 Hz) 와 원료 탱크
CIRC_HZ = {"P7": 2500, "I7": 3000}
TANK = {"P7": "P5", "I7": "I3"}
#: CSV 에 없는 HD4 릴레이 — 트립 · 인버터 FWD(가동 지령) · 압력 스위치 · 헤드 인젝션 SOL (shared/topology.json)
HD4_AUX = {"P00038": "Trip_P7", "P00039": "Trip_I7", "P00209": "FWD_P7", "P0020C": "FWD_I7",
           "P00062": "PSW_P7", "P00063": "PSW_I7", "P00280": "INJ_SOL_HD4"}
CSV_COLS = (pump_csv.pump_cols(PUMPS)
            + ["HD4_P1_인젝션_ON_T", "HD4_P1_인젝션_OFF_T", "Pump_Run_P7", "Pump_Run_I7", "P00241", "P00242",
               "AL_Line_Word", "AL_MA_Word", "AL_ETC_Word"])
SHORT = {"Scale_Out___": "", "Pump_BuildUp_": "BU_", "Hz_Out_": "Hz_", "g_s_SV_": "SV_", "HD4_P1_인젝션_": "INJ_",
         "TK_Temp_PV_": "TT_"}
EVENTS = {   # 일지 (warning/actions/actions_pump.csv)
    "P02 호스 교체": (pd.Timestamp("2026-10-20 00:10"), pd.Timestamp("2026-10-20 01:20")),
    "P03 믹싱헤드 오링": (pd.Timestamp("2026-10-27 10:40"), pd.Timestamp("2026-10-27 11:30")),
    "P04 OUT 니플": (pd.Timestamp("2026-10-29 19:40"), pd.Timestamp("2026-10-29 20:10")),
}


def short(df):
    def f(c):
        for a, b in SHORT.items():
            c = c.replace(a, b)
        return c
    return df.rename(columns=f)


def raw(start=START, end=END, aux=True):
    """HD4 원시 (짧은 열 이름). PT 65535 근처(0 밑을 부호 없는 16비트로 읽은 값)는 비운다.
    TT 는 ℃ 로 바꾼다(0.1℃ 원값 ÷ 10). 나머지는 원값."""
    df = pump_csv.load(start, end, CSV_COLS)
    if aux:
        days = pd.date_range(pd.Timestamp(start).normalize(), (pd.Timestamp(end) - pd.Timedelta(seconds=1)).normalize())
        a = pd.concat([db.fetch_day(d, list(HD4_AUX), group="pump_HD4_aux") for d in days]).sort_index()
        a = a[list(HD4_AUX)].rename(columns=HD4_AUX)
        a = a[~a.index.duplicated(keep="last")]
        a = a[(a.index >= df.index[0]) & (a.index < pd.Timestamp(end))]
        idx = df.index.union(a.index[a.index > df.index[0]])
        df = df.reindex(idx).ffill().join(a.reindex(a.index.union(idx)).ffill().reindex(idx))
        df.index.name = "Time"
    df = short(df)
    for p in PUMPS:
        df[f"PT_{p}"] = df[f"PT_{p}"].where(df[f"PT_{p}"] < 60000)
        df[f"TT_{TANK[p]}"] = df[f"TT_{TANK[p]}"] / 10
    return df


def steady(df, p, buildup, hz=None, min_hold=20):
    """같은 Hz 지령이 min_hold 틱(약 10초) 넘게 이어진 틱. buildup=0 순환, 1 증압(자동 운전). K = PT/(FT/1000)²."""
    hold = df.groupby((df[f"Hz_{p}"] != df[f"Hz_{p}"].shift()).cumsum()).cumcount()
    m = (df[f"FT_{p}"] > 300) & df[f"PT_{p}"].notna() & (hold >= min_hold) & (df[f"BU_{p}"] == buildup)
    if hz is not None:
        m &= df[f"Hz_{p}"] == hz
    s = df.loc[m, [f"FT_{p}", f"PT_{p}", f"Hz_{p}", f"SV_{p}"]].copy()
    s.columns = ["FT", "PT", "Hz", "SV"]
    s["K"] = s.PT / (s.FT / 1000) ** 2
    s["FT_Hz"] = s.FT / s.Hz
    s["day"] = (s.index - pd.Timedelta(hours=6)).date          # 06시 기준 가동일
    return s


def daily_K(df, p):
    """일별 고정 조건 저항 — 순환(circ Hz)과 자동 운전 최빈 Hz."""
    c = steady(df, p, 0, CIRC_HZ[p])
    a = steady(df, p, 1)
    hz = a.Hz.value_counts().index[0]
    a = a[a.Hz == hz]
    out = c.groupby("day").agg(n_circ=("K", "size"), FT_circ=("FT", "median"), PT_circ=("PT", "median"),
                               K_circ=("K", "median"))
    out = out.join(a.groupby("day").agg(n_auto=("K", "size"), FT_auto=("FT", "median"), PT_auto=("PT", "median"),
                                        K_auto=("K", "median")), how="outer")
    out.index = pd.to_datetime(out.index)
    return out, hz


def hz_table(df, p, buildup=1, top=4):
    """일별 × Hz 단계별 K 중앙값 (가장 흔한 Hz top 개). 레시피가 바뀌어도 같은 Hz 끼리 이어 본다."""
    s = steady(df, p, buildup)
    hz = s.Hz.value_counts().index[:top]
    t = s[s.Hz.isin(hz)].groupby(["day", "Hz"]).K.median().unstack()
    t.index = pd.to_datetime(t.index)
    return t[sorted(hz)]


def before_after(df, p, t0, t1, hours=4, buildup=1):
    """작업 [t0, t1] 앞 hours 시간 vs 뒤 hours 시간 — 같은 Hz 단계끼리 K·PT·FT 중앙값."""
    s = steady(df, p, buildup, CIRC_HZ[p] if buildup == 0 else None)
    pre = s[(s.index >= t0 - pd.Timedelta(hours=hours)) & (s.index < t0)].assign(구간="전")
    post = s[(s.index > t1) & (s.index <= t1 + pd.Timedelta(hours=hours))].assign(구간="후")
    if pre.empty or post.empty:
        return pd.DataFrame()                                   # 한쪽 구간에 데이터가 없음 (정지 중 등)
    x = pd.concat([pre, post])
    t = x.groupby(["Hz", "구간"]).agg(n=("K", "size"), FT=("FT", "median"), PT=("PT", "median"),
                                    K=("K", "median")).unstack("구간")
    t = t[(t[("n", "전")] >= 20) & (t[("n", "후")] >= 20)]
    t[("K", "변화%")] = (t[("K", "후")] / t[("K", "전")] - 1) * 100
    return t


def injection_cycles(df):
    """헤드 인젝션 SOL(P00280) 켜짐 한 번 = 한 행. on_s = SOL 켜진 시간,
    ON_T/OFF_T = 그 사이클이 끝난 뒤 PLC 실측 타이머(HD4_P1_인젝션_ON_T·OFF_T, 사이클 끝날 때만 갱신)."""
    sol = df.INJ_SOL_HD4
    on = sol[(sol == 1) & (sol.shift() == 0)].index
    off = sol[(sol == 0) & (sol.shift() == 1)].index
    i = off.searchsorted(on)
    keep = i < len(off)
    e = pd.DataFrame({"on": on[keep], "off": off[i[keep]]})
    e["on_s"] = (e.off - e.on).dt.total_seconds()
    e["next_on"] = e.on.shift(-1)
    e["ON_T"] = df.INJ_ON_T.reindex(e.off + pd.Timedelta(seconds=3), method="ffill").values
    e["OFF_T"] = df.INJ_OFF_T.reindex(e.next_on.fillna(e.off) + pd.Timedelta(seconds=3), method="ffill").values
    e["day"] = pd.to_datetime((e.on - pd.Timedelta(hours=6)).dt.date)
    return e[e.on_s < 60].set_index("on")


def mark(ax, which=None, label=False, span=False):
    """그림에 일지 작업을 표시. span=True 면 작업 구간 회색 띠(시간 단위 그림), 아니면 점선(일 단위 그림)."""
    for k, (a, b) in EVENTS.items():
        if which is None or k.startswith(which):
            if span:
                ax.axvspan(a, b, color="gray", alpha=0.25)
            else:
                ax.axvline(a, color="k", ls="--", lw=0.8)
            if label:
                ax.text(a, ax.get_ylim()[1], " " + k[:3], fontsize=8, va="top")


DAY_CACHE = paths.CACHE_DIR / "replay" / "hd4_days"


def replay_day(d):
    """하루 재생 → cache/replay/hd4_days/{날짜}.pkl. 하루씩 독립이라 병렬로 돌릴 수 있다
    (python pump_check/hd4_common.py — 프로세스 여럿으로 전 기간)."""
    d = pd.Timestamp(d)
    f = DAY_CACHE / f"{d:%Y-%m-%d}.pkl"
    if f.exists():
        return pd.read_pickle(f)
    r = pump_csv.replay_input(d, d + pd.Timedelta(days=1), PUMPS)
    c = replay.pump_cycles(r, pumps=PUMPS, progress=False, keep_raw=True)
    f.parent.mkdir(parents=True, exist_ok=True)
    c.to_pickle(f)
    return c


def replay_cycles():
    """P7·I7 재생 사이클 (자동 운전 판정·zn_*·raw_*). 캐시가 없으면 만든다 (하루 약 1.5분, 순서대로)."""
    if REPLAY_CACHE.exists():
        C = pd.read_pickle(REPLAY_CACHE)
    else:
        parts = [replay_day(d) for d in pd.date_range(START, END - pd.Timedelta(days=1))]
        C = pd.concat([p for p in parts if not p.empty], ignore_index=True)
        C["t"] = pd.to_datetime(C.t_start)
        C["day"] = pd.to_datetime((C.t - pd.Timedelta(hours=6)).dt.date)
        REPLAY_CACHE.parent.mkdir(parents=True, exist_ok=True)
        C.to_pickle(REPLAY_CACHE)
    return C


def daily_features(C, p):
    """자동 운전 사이클 일별 중앙값 — 모델 피처 원값·z, 알림 창 비율."""
    A = C[(C["mode"] == "자동 운전") & (C.pump == p)]
    return A.groupby("day").agg(
        n=("t", "size"), 알림창비율=("pump_level", lambda s: (s == "알림").mean()),
        PT=("raw_PT", "median"), P0=("raw_P0", "median"), FT_SV=("raw_FT_SV", "median"),
        pump_resid=("raw_pump_resid", "median"), r_phys=("raw_r_phys", "median"),
        z_r=("zn_r_phys", "median"), z_P0=("zn_P0", "median"), z_resid=("zn_pump_resid", "median"),
        score=("score", "median"))


def episodes(keys=("P7", "I7")):
    ep = pd.read_csv(paths.EPISODES_DIR / "warning_episodes.csv", parse_dates=["onset", "last_detected", "cleared"])
    return ep[ep.key.isin(keys)]


def actions(incident):
    a = pd.read_csv(paths.ACTIONS_DIR / "actions_pump.csv")
    return a[a.incident_id == incident]


if __name__ == "__main__":
    # 재생을 날짜별로 병렬 실행해 캐시를 만든다
    from concurrent.futures import ProcessPoolExecutor
    days = [d for d in pd.date_range(START, END - pd.Timedelta(days=1))
            if not (DAY_CACHE / f"{d:%Y-%m-%d}.pkl").exists()]
    print(f"재생할 날 {len(days)}개", flush=True)
    with ProcessPoolExecutor(max_workers=12) as ex:
        for d, c in zip(days, ex.map(replay_day, days)):
            print(f"{d:%m-%d} 사이클 {len(c)}", flush=True)
    print("합치기:", replay_cycles().shape)
