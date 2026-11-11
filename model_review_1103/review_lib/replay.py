"""운영 모델에 원시 데이터를 다시 흘려 '모델이 본 피처'를 얻는다 (backend 코드를 그대로 호출).

경고 로그에는 상태 변화만 남는다. 조치 전후로 어떤 피처가 움직였는지,
놓친 경고(FN)에서 점수가 문턱 아래에서 움직였는지는 재생으로만 보인다.

- 입력: db.fetch_range() 결과 (KST naive 인덱스, 태그별 열, ffill 된 원시값).
  DB 행 간격이 실측 약 0.5초라 행 하나를 틱 하나로 넣는다.
- 재생 결과는 엣지와 코드는 같지만, 엣지의 재시작·수집 공백까지 똑같지는 않다.
- 펌프 창 판정(최근 10사이클)은 재생 시작 직후 비어 있다. 보고 싶은 시각보다 최소 반나절 앞에서 시작한다.
"""
from contextlib import redirect_stdout
from io import StringIO

import pandas as pd

from . import backend


def pump_cycles(raw, pumps=None, progress=True, version="v2.1"):
    """펌프 사이클 한 행 = 한 판정. zn_<피처> 열이 오토인코더 입력 피처의 정규화 값.

    mode: 자동 운전 / 판정 범위 밖 / 수집 간격 / 점검·수동 / 알람 / 유량 없음
    (자동 운전만 창 판정·경보로 이어진다)
    """
    backend.use(version)                      # 펌프 코드는 두 버전이 같다
    with redirect_stdout(StringIO()):
        from tools_yeji.pump_models.pump_realtime import PumpRealtimeGroup
        grp = PumpRealtimeGroup(pumps=pumps, keep_cycles=None)
    cols = [c for c in raw.columns if c != "source"]
    vals = raw[cols].to_numpy(dtype=object)
    n = len(raw)
    step = max(n // 10, 1)
    for i, (t, v) in enumerate(zip(raw.index, vals)):
        row = {c: x for c, x in zip(cols, v) if x is not None and x == x}
        grp.update(t, row)
        if progress and i % step == 0:
            print(f"\r재생 {i / n:4.0%}  {t}", end="", flush=True)
    if progress:
        print(f"\r재생 완료 — 사이클 {len(grp.cycles)}개")
    df = pd.DataFrame(list(grp.cycles))
    if df.empty:
        return df
    if "zn" in df:
        zn = pd.json_normalize(df.pop("zn").apply(lambda d: d if isinstance(d, dict) else {}))
        df = pd.concat([df, zn.add_prefix("zn_")], axis=1)
    return df


def conveyor_core(raw, version="v2.2", history=None, eval_every="15min", progress=True, units=()):
    """컨베이어 코어(대차 통과 센서 6개) 재생. 통과 하나 = 한 행인 표와, 주기마다의 판정 결과를 돌려준다.

    운영 CoreRealtime 를 상속해 기록만 덧붙인다 (판정 코드는 그대로).
    - passes: t(통과 시작 시각), sensor, wagon, dist(P00140 기준 앞 끝 위치), width(켜짐 길이), rearm(재감지 수)
    - evals : 판정 시각마다 (sensor, wagon) 칸의 상태·수준 + 센서 플래그. 모델 화면에 떴을 내용이다.
    - core.unit_cycles: units=('md_rev', …) 를 주면 유닛 사이클 한 행 = 한 사이클 (t = 스톱퍼 SOL ON, 지표 값).
      운영 모델은 하루 합계만 남기므로 사이클 값은 여기서 따로 계산한다 (unit_realtime._close_cycle 과 같은 식).
    history: 지난 가동일 칸 목록(가동일 기록 파일 내용). 재생 시작일 이전 날만 넘긴다.
    """
    backend.use(version)
    with redirect_stdout(StringIO()):
        from tools_yeji.conveyor_models.core_realtime import CoreRealtime, to_kst_naive
        from tools_yeji.conveyor_models.unit_params import METRICS, MAX_CYCLE, bits
        from tools_yeji.conveyor_models.unit_realtime import UnitRealtime

    unit_cycles = []

    class LoggingUnit(UnitRealtime):
        def _close_cycle(self, ts_next):
            c = self.cyc
            if c is not None and c["run"] and ts_next - c["start"] <= MAX_CYCLE:
                rec = {"unit": self.unit, "t": self.t0 + pd.Timedelta(seconds=c["start"])}
                f = c["first"]
                for name, s_sig, s_dir, e_sig, e_dir, unit_, _ in METRICS:
                    n = len(bits(self.unit, e_sig))
                    for j in range(n):
                        col = name if n == 1 else f"{name}_{j + 1}"
                        st, en = f.get((s_sig, 0, s_dir)), f.get((e_sig, j, e_dir))
                        if st is not None and en is not None:
                            v = (en[0] - st[0]) if unit_ == "s" else (en[1] - st[1])
                            rec[col] = v if v == v and v >= 0 else None
                unit_cycles.append(rec)
            super()._close_cycle(ts_next)

    unit_objs = [LoggingUnit(u) for u in units]

    passes = []

    class _Log(list):
        def __init__(self, key, owner):
            super().__init__()
            self.key, self.owner = key, owner

        def append(self, rec):
            passes.append((self.owner._pass_t, self.key[0], self.key[1], *rec))
            super().append(rec)

    class _LogDict(dict):
        def __init__(self, owner):
            super().__init__()
            self.owner = owner

        def __missing__(self, key):
            v = self[key] = _Log(key, self.owner)
            return v

    class LoggingCore(CoreRealtime):
        def __init__(self):
            super().__init__(history_path=None)
            self._pass_t = None
            self.today = _LogDict(self)

        def update(self, t, row):
            super().update(t, row)
            now = to_kst_naive(t)
            for o in self.open.values():
                o.setdefault("t", now)               # 통과가 열린 tick 의 시각

        def _close_pass(self, s):
            self._pass_t = self.open.get(s, {}).get("t")
            super()._close_pass(s)

        def close_day(self):
            super().close_day()
            self.today = _LogDict(self)

    core = LoggingCore()
    if history:
        core.history = [{**r, "day": pd.Timestamp(r["day"])} for r in history]
    cols = [c for c in raw.columns if c != "source"]
    vals = raw[cols].to_numpy(dtype=object)
    step_eval = pd.Timedelta(eval_every)
    next_eval = raw.index[0] + step_eval
    evals = []
    n = len(raw)
    for i, (t, v) in enumerate(zip(raw.index, vals)):
        core.update(t, {c: x for c, x in zip(cols, v) if x is not None and x == x})
        for u in unit_objs:
            u.update(to_kst_naive(t), core)
        if t >= next_eval:
            next_eval = t + step_eval
            r = core.evaluate(t)
            for s, se in r.get("sensors", {}).items():
                flags = ", ".join(f["flag"] for f in se.get("flags", []))
                evals.append(dict(t=t, sensor=s, wagon=None, state=None, sev=se.get("sev"), flags=flags))
            for wk, w in r.get("wagons", {}).items():
                for it in w.get("items", []):
                    evals.append(dict(t=t, sensor=it["sensor"], wagon=int(wk), state=it["state"], sev=it["sev"],
                                      flags=None))
        if progress and i % max(n // 10, 1) == 0:
            print(f"\r재생 {i / n:4.0%}  {t}", end="", flush=True)
    if progress:
        print(f"\r재생 완료 — 통과 {len(passes)}개")
    P = pd.DataFrame(passes, columns=["t", "sensor", "wagon", "dist", "width", "rearm"])
    core.unit_cycles = pd.DataFrame(unit_cycles)
    return P, pd.DataFrame(evals), core


#: 유닛 → 그 유닛의 캐리어 진입감지 센서 (코어 센서). 유닛 사이클을 대차에 붙일 때 쓴다
UNIT_ENTRY = {"md_fwd": "P00451", "md_rev": "P00459"}


def attach_wagon(cycles, passes, unit, max_lag="60s"):
    """유닛 사이클마다 직전 진입감지 통과의 대차 번호를 붙인다 (lag = 통과 → 스톱퍼 SOL ON).

    에어백 유닛은 코어에 진입 센서가 없어 붙이지 않는다. lag 분포가 한 덩어리인지 먼저 확인한다.
    """
    c = cycles[cycles["unit"] == unit].sort_values("t").copy()
    p = passes[passes["sensor"] == UNIT_ENTRY[unit]].sort_values("t")[["t", "wagon"]].rename(columns={"t": "t_pass"})
    c["t"] = c["t"].astype("datetime64[ns]")
    p["t_pass"] = p["t_pass"].astype("datetime64[ns]")
    out = pd.merge_asof(c, p, left_on="t", right_on="t_pass", direction="backward",
                        tolerance=pd.Timedelta(max_lag))
    out["lag_s"] = (out["t"] - out["t_pass"]).dt.total_seconds()
    return out


def pump_daily(cyc, feats=None):
    """사이클 → 펌프·날짜별 피처 중앙값과 판정 모드 비율. 조치 전후 비교 표에 쓴다."""
    cyc = cyc.copy()
    cyc["date"] = pd.to_datetime(cyc["t_start"]).dt.date
    auto = cyc[cyc["mode"] == "자동 운전"]
    feats = feats or [c for c in cyc.columns if c.startswith("zn_")]
    med = auto.groupby(["pump", "date"])[feats + ["score"]].median()
    mode = cyc.groupby(["pump", "date"])["mode"].value_counts(normalize=True).unstack(fill_value=0).add_prefix("mode_")
    return med.join(mode, how="outer")
