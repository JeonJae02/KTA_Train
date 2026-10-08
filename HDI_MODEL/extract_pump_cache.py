"""펌프 원시 데이터를 하루 단위로 받아 캐시에 쌓는다 — 학습·검증·테스트 공용 원본.

    python HDI_MODEL/extract_pump_cache.py                         # 전 펌프, 2026-09-10 ~ 지금
    python HDI_MODEL/extract_pump_cache.py --pumps P1 I1           # 일부 펌프만
    python HDI_MODEL/extract_pump_cache.py --start 2026-10-01 --end 2026-10-03
    python HDI_MODEL/extract_pump_cache.py --status                # 받은 현황만 출력

저장: cache/pump_raw/{펌프}/{YYYY-MM-DD}.pkl.xz   (오늘처럼 덜 끝난 날은 {날짜}_partial.pkl.xz)
      cache/pump_raw/_extract_log.csv                (요청마다 한 줄: 행수·소요·상태)
      cache/pump_raw/_manifest.json                  (펌프별 태그 목록·설정 버전)
읽기: load_day(pump, "2026-09-15") / load_range(pump, start, end)

## 현장 DB 를 터뜨리지 않는 원칙

- **한 번에 펌프 하나, 하루치만.** LogExtractor 가 그 안에서 다시 6시간 청크로 나누고, 태그 필터를
  피벗 앞에 둔다(정규식 pushdown). 펌프를 섞어 받으면 틱 개수도 달라진다(SV_유량_추종_분석 노트북).
- **요청 사이에 쉰다** — 날짜 사이 `--sleep`(기본 3초), 펌프 사이 `--pump-sleep`(기본 10초).
- **이미 받은 날은 건너뛴다.** 중단돼도 다시 실행하면 이어서 받는다. 오늘(_partial)만 매번 다시 받는다.
- **실패는 기다렸다 재시도** (30 → 60 → 120초). 청크 하나라도 실패하면 그 날 전체를 실패로 본다 —
  구멍 난 데이터를 정상으로 저장하지 않기 위해서다. 3번 실패하면 로그만 남기고 다음 날로 넘어간다.

## 저장 형식

값은 전부 정수라(PLC 원값) float32 로 바꿔도 손실이 없고, xz 압축으로 하루 약 0.6 MB(원래 12 MB).
인덱스는 LogExtractor 그대로 naive KST, 앞 600초 패딩으로 ffill 해 둔 상태다.
"""

import argparse
import contextlib
import csv
import io
import json
import os
import sys
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)
sys.path.insert(0, HERE)

from new_pump_preprocessor import load_settings, required_tags  # noqa: E402

CACHE_DIR = os.path.join(ROOT, "cache", "pump_raw")
LOG_PATH = os.path.join(CACHE_DIR, "_extract_log.csv")
MANIFEST_PATH = os.path.join(CACHE_DIR, "_manifest.json")
DEFAULT_START = "2026-09-10"           # 09-09 15:53 에 수집 태그 집합이 바뀌었다 (new_vis 0-2)
RETRY_WAITS = (30, 60, 120)
#: 펌프 순서 — 같은 헤드 쌍끼리 붙여서 받는다
ORDER = ["P1", "I1", "P2", "I2", "P3", "I3", "P4", "I4", "P5", "I5", "P6", "I6", "P7", "I7"]


# --------------------------------------------------------------------------- #
# 경로 · 읽기
# --------------------------------------------------------------------------- #
def day_path(pump, day, partial=False):
    return os.path.join(CACHE_DIR, pump, f"{day}{'_partial' if partial else ''}.pkl.xz")


def load_day(pump, day):
    """하루치 원본. 완성본이 없으면 _partial 을 읽는다. 없으면 None."""
    for partial in (False, True):
        p = day_path(pump, day, partial)
        if os.path.exists(p):
            return pd.read_pickle(p, compression="xz")
    return None


def load_range(pump, start, end):
    """start ~ end(포함) 날짜를 이어 붙인다. float32 → float64 로 되돌려 준다."""
    days = pd.date_range(start, end, freq="D").strftime("%Y-%m-%d")
    parts = [d for d in (load_day(pump, x) for x in days) if d is not None]
    if not parts:
        return pd.DataFrame()
    df = pd.concat(parts).sort_index()
    df = df[~df.index.duplicated(keep="first")]
    num = df.select_dtypes("number").columns
    df[num] = df[num].astype("float64")
    return df


# --------------------------------------------------------------------------- #
# 받기
# --------------------------------------------------------------------------- #
def _compact(df):
    df = df.copy()
    num = df.select_dtypes("number").columns
    vals = df[num].to_numpy()
    finite = vals[np.isfinite(vals)]
    if finite.size and (np.any(finite % 1 != 0) or np.abs(finite).max() > 2 ** 24):
        raise ValueError("정수가 아니거나 float32 로 정확히 못 담는 값이 있다 — 형식 확인 필요")
    df[num] = df[num].astype("float32")
    for c in df.columns.difference(num):
        df[c] = df[c].astype("category")
    return df


def _log(row):
    new = not os.path.exists(LOG_PATH)
    with open(LOG_PATH, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(row))
        if new:
            w.writeheader()
        w.writerow(row)


def fetch_day(ex, pump, tags, day, end_dt):
    """하루치 받기. 반환 (DataFrame, 메모). 청크 실패·빈 결과는 예외."""
    start = f"{day} 00:00:00"
    end = min(pd.Timestamp(day) + pd.Timedelta(days=1), end_dt).strftime("%Y-%m-%d %H:%M:%S")
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df = ex.get_data(start, end, tags)
    out = buf.getvalue()
    if "실패" in out:
        raise RuntimeError("청크 실패: " + " | ".join(l.strip() for l in out.splitlines() if "실패" in l)[:300])
    if df is None or df.empty:
        raise RuntimeError("빈 결과")
    missing = [t for t in tags if t not in df.columns]
    note = f"없는 태그 {missing}" if missing else ""
    return df, note


def extract(pumps, start, end, sleep_sec, pump_sleep_sec, dry_run=False):
    settings = load_settings()
    os.makedirs(CACHE_DIR, exist_ok=True)
    now = datetime.now()
    end_dt = pd.Timestamp(now if end in (None, "now") else pd.Timestamp(end) + pd.Timedelta(days=1)).floor("min")
    today = now.strftime("%Y-%m-%d")
    days = pd.date_range(start, (end_dt - pd.Timedelta(seconds=1)).normalize(), freq="D").strftime("%Y-%m-%d")

    manifest = {"settings_version": settings["version"], "start": start, "updated": now.isoformat(timespec="seconds"),
                "pumps": {p: required_tags(p, settings) for p in pumps}}
    if os.path.exists(MANIFEST_PATH):
        old = json.load(open(MANIFEST_PATH, encoding="utf-8"))
        old["pumps"].update(manifest["pumps"])
        manifest["pumps"] = old["pumps"]
    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=1)

    todo = []
    for p in pumps:
        for d in days:
            partial = d == today or pd.Timestamp(d) + pd.Timedelta(days=1) > end_dt
            if not partial and os.path.exists(day_path(p, d)):
                continue
            todo.append((p, d, partial))
    print(f"받을 것 {len(todo)}건 (펌프 {len(pumps)}대 × {len(days)}일, 이미 있는 것 제외)", flush=True)
    if dry_run or not todo:
        return

    from Log_Extractor import LogExtractor
    with contextlib.redirect_stdout(io.StringIO()):
        ex = LogExtractor(env_path=os.path.join(ROOT, ".env"))

    t_all, prev_pump = time.time(), None
    for i, (p, d, partial) in enumerate(todo, 1):
        if prev_pump is not None and p != prev_pump:
            time.sleep(pump_sleep_sec)
        prev_pump = p
        tags = required_tags(p, settings)
        os.makedirs(os.path.join(CACHE_DIR, p), exist_ok=True)
        status, note, rows, t0 = "fail", "", 0, time.time()
        for attempt, wait in enumerate((0,) + RETRY_WAITS, 1):
            if wait:
                time.sleep(wait)
            try:
                df, note = fetch_day(ex, p, tags, d, end_dt)
                df = _compact(df)
                path = day_path(p, d, partial)
                df.to_pickle(path + ".tmp", compression="xz")
                os.replace(path + ".tmp", path)
                if not partial and os.path.exists(day_path(p, d, True)):
                    os.remove(day_path(p, d, True))
                status, rows = ("partial" if partial else "ok"), len(df)
                break
            except Exception as e:                         # noqa: BLE001 — 실패는 기록하고 다음으로
                note = f"{type(e).__name__}: {e}"[:300]
        sec = time.time() - t0
        _log({"time": datetime.now().isoformat(timespec="seconds"), "pump": p, "day": d, "status": status,
              "attempts": attempt, "rows": rows, "sec": round(sec, 1), "note": note})
        el = time.time() - t_all
        eta = el / i * (len(todo) - i)
        print(f"[{i}/{len(todo)}] {p} {d} {status:7s} {rows:>8,}행 {sec:5.1f}s  "
              f"(경과 {el / 60:.1f}분, 남은 예상 {eta / 60:.1f}분){'  ' + note if note else ''}", flush=True)
        time.sleep(sleep_sec)


def status():
    if not os.path.exists(CACHE_DIR):
        print("캐시 없음")
        return
    rows = []
    for p in ORDER:
        d = os.path.join(CACHE_DIR, p)
        if not os.path.isdir(d):
            continue
        files = sorted(os.listdir(d))
        full = [f for f in files if f.endswith(".pkl.xz") and "_partial" not in f]
        part = [f for f in files if "_partial" in f]
        size = sum(os.path.getsize(os.path.join(d, f)) for f in files) / 1e6
        rows.append({"펌프": p, "완성일": len(full), "partial": len(part),
                     "첫날": full[0][:10] if full else "", "끝날": full[-1][:10] if full else "", "MB": round(size, 1)})
    print(pd.DataFrame(rows).to_string(index=False))
    if os.path.exists(LOG_PATH):
        lg = pd.read_csv(LOG_PATH)
        bad = lg.groupby(["pump", "day"]).status.last()
        bad = bad[bad == "fail"]
        print(f"\n마지막 시도가 실패인 날: {len(bad)}건" + (f" — {list(bad.index)[:10]}" if len(bad) else ""))


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pumps", nargs="*", default=None, help="기본: 설정의 전 펌프")
    ap.add_argument("--start", default=DEFAULT_START)
    ap.add_argument("--end", default="now", help="마지막 날짜(포함) 또는 now")
    ap.add_argument("--sleep", type=float, default=3.0, help="요청(하루치) 사이 쉬는 초")
    ap.add_argument("--pump-sleep", type=float, default=10.0, help="펌프 바뀔 때 쉬는 초")
    ap.add_argument("--dry-run", action="store_true", help="받을 목록만 센다")
    ap.add_argument("--status", action="store_true", help="받은 현황만 출력")
    a = ap.parse_args()
    if a.status:
        status()
    else:
        pumps = a.pumps or [p for p in ORDER if p in load_settings()["pumps"]]
        extract(pumps, a.start, a.end, a.sleep, a.pump_sleep, a.dry_run)
