"""펌프 일별 CSV(cache/raw/pump/2026-*.csv, pump_check/extract_raw_csv.py 로 받음) 읽기.

- 기간: 09-11 00:00 ~ 11-13 11:00. 열 124개(펌프 14대 PT·FT·Hz·BuildUp·SV, 헤드 인젝션 ON/OFF_T,
  탱크 온도, Pump_Run, Calibration Nozzle SOL, AL_*_Word, P0025D). 인덱스 Time = naive KST, ffill 된 원시값.
- load(start, end, cols): 필요한 열만 읽어 이어 붙인다 (하루 55 MB 라 열을 고르는 게 빠르다).
- replay_input(start, end, pumps): 운영 펌프 모델 재생 입력. CSV 에 없는 태그(사출 신호·알람 비트·가동 플래그)만
  db.fetch_day 로 하루씩 받아(cache/raw/pump_replay_aux/) outer merge 한다.
"""
from pathlib import Path

import pandas as pd

from . import db, paths

CSV_DIR = paths.CACHE_DIR / "raw" / "pump"
HEADS = ["HD1_P1", "HD1_P2", "HD2_P1", "HD2_P2", "HD3_P1", "HD3_P2", "HD4_P1"]
#: CSV 에 없는데 펌프 모델이 읽는 태그 (탱크 온도 TT 는 TK_Temp_PV 별칭으로 들어간다)
REPLAY_AUX = [f"Shot_Injection_{h}" for h in HEADS] + ["AL_Line_Bit", "AL_MA_Bit", "AL_Tank_Bit", "AL_ETC_Bit",
                                                       "Auto_Running_PL"]


def _files(start, end):
    start, end = pd.Timestamp(start), pd.Timestamp(end)
    out = []
    for p in sorted(CSV_DIR.glob("2026-*.csv")):
        d = pd.Timestamp(p.stem[:10])
        if d + pd.Timedelta(days=1) > start and d < end:
            out.append(p)
    return out


def _read(p, cols=None):
    usecols = None if cols is None else ["Time", *cols]
    df = pd.read_csv(p, usecols=lambda c: usecols is None or c in usecols, encoding="utf-8-sig")
    df["Time"] = pd.to_datetime(df["Time"], format="ISO8601")
    return df.set_index("Time")


def load(start, end, cols=None):
    """[start, end) 구간. cols=None 이면 전 열."""
    start, end = pd.Timestamp(start), pd.Timestamp(end)
    parts = [_read(p, cols) for p in _files(start, end)]
    if not parts:
        return pd.DataFrame()
    df = pd.concat(parts).sort_index()
    df = df[~df.index.duplicated(keep="last")]
    return df[(df.index >= start) & (df.index < end)]


def pump_tank(pump):
    """펌프 → 원료 탱크 (pump_settings.json, 예: P7 → P5)."""
    import json
    s = json.load(open(paths.BACKEND / "models" / "pump_cycle" / "pump_settings.json", encoding="utf-8"))
    return s["pumps"][pump]["tank"]


def pump_cols(pumps):
    pre = ("Scale_Out___PT_", "Scale_Out___FT_", "Hz_Out_", "Pump_BuildUp_", "g_s_SV_")
    return sorted({f"{x}{p}" for p in pumps for x in pre} | {f"TK_Temp_PV_{pump_tank(p)}" for p in pumps})


def replay_input(start, end, pumps):
    """펌프 모델 재생용 원시 데이터 = CSV(펌프 태그) ∪ DB(REPLAY_AUX, 하루 캐시). 둘 다 앞 값으로 채운다."""
    start, end = pd.Timestamp(start), pd.Timestamp(end)
    base = load(start, end, pump_cols(pumps))
    days = pd.date_range(start.normalize(), (end - pd.Timedelta(seconds=1)).normalize(), freq="D")
    aux = pd.concat([db.fetch_day(d, REPLAY_AUX, group="pump_replay_aux") for d in days]).sort_index()
    aux = aux[[c for c in REPLAY_AUX if c in aux.columns]]
    aux = aux[~aux.index.duplicated(keep="last")]
    idx = base.index.union(aux.index[(aux.index > base.index[0]) & (aux.index <= base.index[-1])])
    a = aux.reindex(aux.index.union(idx)).ffill().reindex(idx)
    out = base.reindex(idx).ffill().join(a)
    out.index.name = "Time"
    return out
