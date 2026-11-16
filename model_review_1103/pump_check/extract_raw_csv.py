"""펌프 원시 태그를 하루 단위 CSV 로 받는다 → model_review_1103/cache/raw/pump/

    python model_review_1103/pump_check/extract_raw_csv.py                  # 09-11 00:00 ~ 11-13 11:00
    python model_review_1103/pump_check/extract_raw_csv.py --start 2026-10-01 --end "2026-10-03 00:00"

태그 (124개)
- 펌프 14대(P1~P7, I1~I7): Scale_Out___PT_, Scale_Out___FT_, Hz_Out_, Pump_BuildUp_, g_s_SV_
- 헤드 7개(HD1_P1 … HD4_P1): {헤드}_인젝션_ON_T / _OFF_T
- 탱크 8개: TK_Temp_PV_{P1~P5, I1~I3} — P6·P7·I4~I7 탱크 온도는 DB 에 없다 (11-12 확인)
- 기타 (변할 때 + 5분 스냅샷에만 기록): P0025D (Oval Conveyor Break MC), AL_Line/MA/ETC_Word,
  Pump_Run_{펌프} 14, Calibration Nozzle SOL 14 (CAL_SOL)

현장 DB 보호: 하루씩 요청(LogExtractor 가 다시 6시간 청크로 나눔), 요청 사이 --sleep 초 휴식,
실패 시 30/60/120초 뒤 재시도, 청크 하나라도 실패하면 그 날은 저장하지 않는다. 이미 있는 날은 건너뛴다.

값은 PLC 원값 그대로(단위 변환 없음, TK_Temp_PV 는 0.1℃ 단위). 인덱스 Time = naive KST, ffill 된 상태.
"""
import argparse
import contextlib
import io
import os
import sys
import time
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]                     # KTA_Train/ (.env, Log_Extractor.py)
OUT_DIR = Path(__file__).resolve().parents[1] / "cache" / "raw" / "pump"
sys.path.insert(0, str(ROOT))

PUMPS = [f"{a}{i}" for i in range(1, 8) for a in "PI"]
HEADS = ["HD1_P1", "HD1_P2", "HD2_P1", "HD2_P2", "HD3_P1", "HD3_P2", "HD4_P1"]
TANKS = ["P1", "P2", "P3", "P4", "P5", "I1", "I2", "I3"]
#: 펌프 외 추가 태그. 이미 받은 CSV 에는 merge_tag_csv.py 로 열을 붙인다.
#: Calibration Nozzle SOL (Auto-Cal) — shared/topology.json 의 PUMP_* cal_nozzle
CAL_SOL = {"P1": "P00239", "I1": "P0023A", "P2": "P00244", "I2": "P00245", "P3": "P0023B", "I3": "P0023C",
           "P4": "P0023D", "I4": "P0023E", "P5": "P0023F", "I5": "P00240", "P6": "P00246", "I6": "P00247",
           "P7": "P00241", "I7": "P00242"}
EXTRA_TAGS = (["P0025D",                                        # Oval Conveyor Break MC
               "AL_Line_Word", "AL_MA_Word", "AL_ETC_Word"]
              + [f"Pump_Run_{p}" for p in PUMPS]
              + [CAL_SOL[p] for p in PUMPS])
TAGS = ([f"{pre}{p}" for pre in ("Scale_Out___PT_", "Scale_Out___FT_", "Hz_Out_", "Pump_BuildUp_", "g_s_SV_")
         for p in PUMPS]
        + [f"{h}_인젝션_{k}_T" for h in HEADS for k in ("ON", "OFF")]
        + [f"TK_Temp_PV_{t}" for t in TANKS]
        + EXTRA_TAGS)
RETRY_WAITS = (30, 60, 120)


def out_path(s, e):
    full = e - s == pd.Timedelta(days=1) and s == s.normalize()
    return OUT_DIR / (f"{s:%Y-%m-%d}.csv" if full else f"{s:%Y-%m-%d_%H%M}-{e:%H%M}.csv")


def fetch(ex, s, e):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df = ex.get_data(f"{s:%Y-%m-%d %H:%M:%S}", f"{e:%Y-%m-%d %H:%M:%S}", TAGS)
    if "실패" in buf.getvalue():
        raise RuntimeError("청크 실패")
    if df is None or df.empty:
        raise RuntimeError("빈 결과")
    df = df[[t for t in TAGS if t in df.columns]]               # source 등 태그 아닌 열 제거, 순서 고정
    df = df[(df.index >= s) & (df.index < e)]
    missing = [t for t in TAGS if t not in df.columns]
    return df, missing


def main(start, end, sleep_sec):
    start, end = pd.Timestamp(start), pd.Timestamp(end)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    spans = []
    s = start
    while s < end:
        e = min(s.normalize() + pd.Timedelta(days=1), end)
        spans.append((s, e))
        s = e
    todo = [(s, e) for s, e in spans if not out_path(s, e).exists()]
    print(f"구간 {len(spans)}개 중 받을 것 {len(todo)}개 → {OUT_DIR}", flush=True)

    from Log_Extractor import LogExtractor
    with contextlib.redirect_stdout(io.StringIO()):
        ex = LogExtractor(env_path=str(ROOT / ".env"))

    t_all = time.time()
    for i, (s, e) in enumerate(todo, 1):
        t0, note = time.time(), ""
        for wait in (0,) + RETRY_WAITS:
            if wait:
                print(f"   재시도 {wait}초 대기 ({note})", flush=True)
                time.sleep(wait)
            try:
                df, missing = fetch(ex, s, e)
                p = out_path(s, e)
                df.to_csv(p.with_suffix(".tmp"), float_format="%.10g", encoding="utf-8-sig")
                os.replace(p.with_suffix(".tmp"), p)
                note = f"없는 태그 {missing}" if missing else ""
                status = "ok"
                break
            except Exception as ex_:                            # noqa: BLE001
                note, status = f"{type(ex_).__name__}: {ex_}"[:200], "FAIL"
        el = time.time() - t_all
        print(f"[{i}/{len(todo)}] {s:%m-%d %H:%M}~{e:%m-%d %H:%M} {status} "
              f"{len(df) if status == 'ok' else 0:>8,}행 {time.time() - t0:5.1f}s "
              f"(경과 {el / 60:.1f}분, 남은 {el / i * (len(todo) - i) / 60:.1f}분) {note}", flush=True)
        time.sleep(sleep_sec)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", default="2026-09-11 00:00")
    ap.add_argument("--end", default="2026-11-13 11:00")
    ap.add_argument("--sleep", type=float, default=5.0)
    a = ap.parse_args()
    main(a.start, a.end, a.sleep)
