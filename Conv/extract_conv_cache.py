"""컨베이어 센서 원시 데이터를 그룹별·하루 단위로 받아 캐시에 쌓는다 — 분석·학습 공용 원본.

    python Conv/extract_conv_cache.py                                  # 전 그룹, 2026-09-10 ~ 지금
    python Conv/extract_conv_cache.py --groups md_fwd closing          # 일부 그룹만
    python Conv/extract_conv_cache.py --start 2026-10-01 --end 2026-10-03
    python Conv/extract_conv_cache.py --status                         # 받은 현황만 출력

저장: cache/conv_raw/{그룹}/{YYYY-MM-DD}.pkl.xz   (덜 끝난 날은 {날짜}_partial.pkl.xz)
      cache/conv_raw/_extract_log.csv               (요청마다 한 줄: 행수·소요·상태)
      cache/conv_raw/_manifest.json                 (그룹별 태그 목록)
읽기: load_day(group, "2026-09-15") / load_range(group, start, end)
그룹·태그 정의는 Conv/conv_tags.py.

펌프 추출기(HDI_MODEL/extract_pump_cache.py)와 같은 원칙을 따른다.
- 한 번에 그룹 하나, 하루치만 요청한다. LogExtractor 가 6시간 청크로 나누고 정규식 필터를 피벗 앞에 둔다.
- 요청 사이에 쉬고, 이미 받은 날은 건너뛰며, 실패하면 30 → 60 → 120초 기다렸다 재시도한다.
- 청크 하나라도 실패하면 그 날 전체를 실패로 본다.

## 리샘플하지 않는다

컨베이어 분석은 PLC 카운터(R000xx)가 떨어지는 순간으로 센서 엣지를 복원한다. 그래서 DB 의
원래 타임스탬프(약 0.5초 간격)를 그대로 둔다. LogExtractor 는 피벗 + ffill 만 하고 리샘플하지 않는다.
값은 PLC 정수라 float32 로 손실 없이 줄인다(정수가 아닌 컬럼만 float64 로 남긴다).
"""

import argparse
import contextlib
import csv
import io
import json
import os
import sys
import time
from datetime import datetime

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)
sys.path.insert(0, HERE)

from conv_tags import ORDER, group_tags  # noqa: E402

CACHE_DIR = os.path.join(ROOT, "cache", "conv_raw")
LOG_PATH = os.path.join(CACHE_DIR, "_extract_log.csv")
MANIFEST_PATH = os.path.join(CACHE_DIR, "_manifest.json")
DEFAULT_START = "2026-09-10"           # 09-09 15:53 에 수집 태그 집합이 바뀌었다
RETRY_WAITS = (30, 60, 120)


# --------------------------------------------------------------------------- #
# 경로 · 읽기
# --------------------------------------------------------------------------- #
def day_path(group, day, partial=False):
    return os.path.join(CACHE_DIR, group, f"{day}{'_partial' if partial else ''}.pkl.xz")


def load_day(group, day):
    """하루치 원본. 완성본이 없으면 _partial 을 읽는다. 없으면 None."""
    for partial in (False, True):
        p = day_path(group, day, partial)
        if os.path.exists(p):
            return pd.read_pickle(p, compression="xz")
    return None


def load_range(group, start, end):
    """start ~ end(포함) 날짜를 이어 붙인다. float32 → float64 로 되돌려 준다."""
    days = pd.date_range(start, end, freq="D").strftime("%Y-%m-%d")
    parts = [d for d in (load_day(group, x) for x in days) if d is not None]
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
    """정수 컬럼은 float32, 아닌 컬럼은 float64 로 둔다. 문자열은 category."""
    df = df.copy()
    for c in df.select_dtypes("number").columns:
        v = df[c].to_numpy(dtype="float64")
        f = v[np.isfinite(v)]
        exact = not f.size or (np.all(f % 1 == 0) and np.abs(f).max() <= 2 ** 24)
        df[c] = v.astype("float32" if exact else "float64")
    for c in df.columns.difference(df.select_dtypes("number").columns):
        df[c] = df[c].astype("category")
    return df


def _log(row):
    new = not os.path.exists(LOG_PATH)
    with open(LOG_PATH, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(row))
        if new:
            w.writeheader()
        w.writerow(row)


def fetch_day(ex, tags, day, end_dt):
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


def extract(groups, start, end, sleep_sec, group_sleep_sec, dry_run=False):
    os.makedirs(CACHE_DIR, exist_ok=True)
    now = datetime.now()
    end_dt = pd.Timestamp(now if end in (None, "now") else pd.Timestamp(end) + pd.Timedelta(days=1)).floor("min")
    today = now.strftime("%Y-%m-%d")
    days = pd.date_range(start, (end_dt - pd.Timedelta(seconds=1)).normalize(), freq="D").strftime("%Y-%m-%d")

    manifest = {"start": start, "updated": now.isoformat(timespec="seconds"),
                "groups": {g: group_tags(g) for g in groups}}
    if os.path.exists(MANIFEST_PATH):
        old = json.load(open(MANIFEST_PATH, encoding="utf-8"))
        old["groups"].update(manifest["groups"])
        manifest["groups"] = old["groups"]
    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=1)

    todo = []
    for g in groups:
        for d in days:
            partial = d == today or pd.Timestamp(d) + pd.Timedelta(days=1) > end_dt
            if not partial and os.path.exists(day_path(g, d)):
                continue
            todo.append((g, d, partial))
    print(f"받을 것 {len(todo)}건 (그룹 {len(groups)}개 × {len(days)}일, 이미 있는 것 제외)", flush=True)
    if dry_run or not todo:
        return

    from Log_Extractor import LogExtractor
    with contextlib.redirect_stdout(io.StringIO()):
        ex = LogExtractor(env_path=os.path.join(ROOT, ".env"))

    t_all, prev = time.time(), None
    for i, (g, d, partial) in enumerate(todo, 1):
        if prev is not None and g != prev:
            time.sleep(group_sleep_sec)
        prev = g
        tags = group_tags(g)
        os.makedirs(os.path.join(CACHE_DIR, g), exist_ok=True)
        status, note, rows, t0 = "fail", "", 0, time.time()
        for attempt, wait in enumerate((0,) + RETRY_WAITS, 1):
            if wait:
                time.sleep(wait)
            try:
                df, note = fetch_day(ex, tags, d, end_dt)
                df = _compact(df)
                path = day_path(g, d, partial)
                df.to_pickle(path + ".tmp", compression="xz")
                os.replace(path + ".tmp", path)
                if not partial and os.path.exists(day_path(g, d, True)):
                    os.remove(day_path(g, d, True))
                status, rows = ("partial" if partial else "ok"), len(df)
                break
            except Exception as e:                         # noqa: BLE001 — 실패는 기록하고 다음으로
                note = f"{type(e).__name__}: {e}"[:300]
        sec = time.time() - t0
        _log({"time": datetime.now().isoformat(timespec="seconds"), "group": g, "day": d, "status": status,
              "attempts": attempt, "rows": rows, "sec": round(sec, 1), "note": note})
        el = time.time() - t_all
        eta = el / i * (len(todo) - i)
        print(f"[{i}/{len(todo)}] {g} {d} {status:7s} {rows:>8,}행 {sec:5.1f}s  "
              f"(경과 {el / 60:.1f}분, 남은 예상 {eta / 60:.1f}분){'  ' + note if note else ''}", flush=True)
        time.sleep(sleep_sec)


def status():
    if not os.path.exists(CACHE_DIR):
        print("캐시 없음")
        return
    rows = []
    for g in ORDER:
        d = os.path.join(CACHE_DIR, g)
        if not os.path.isdir(d):
            continue
        files = sorted(os.listdir(d))
        full = [f for f in files if f.endswith(".pkl.xz") and "_partial" not in f]
        part = [f for f in files if "_partial" in f]
        size = sum(os.path.getsize(os.path.join(d, f)) for f in files) / 1e6
        rows.append({"그룹": g, "완성일": len(full), "partial": len(part),
                     "첫날": full[0][:10] if full else "", "끝날": full[-1][:10] if full else "", "MB": round(size, 1)})
    print(pd.DataFrame(rows).to_string(index=False))
    if os.path.exists(LOG_PATH):
        lg = pd.read_csv(LOG_PATH)
        bad = lg.groupby(["group", "day"]).status.last()
        bad = bad[bad == "fail"]
        print(f"\n마지막 시도가 실패인 날: {len(bad)}건" + (f" — {list(bad.index)[:10]}" if len(bad) else ""))


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--groups", nargs="*", default=None, help=f"기본: 전 그룹 {ORDER}")
    ap.add_argument("--start", default=DEFAULT_START)
    ap.add_argument("--end", default="now", help="마지막 날짜(포함) 또는 now")
    ap.add_argument("--sleep", type=float, default=3.0, help="요청(하루치) 사이 쉬는 초")
    ap.add_argument("--group-sleep", type=float, default=10.0, help="그룹 바뀔 때 쉬는 초")
    ap.add_argument("--dry-run", action="store_true", help="받을 목록만 센다")
    ap.add_argument("--status", action="store_true", help="받은 현황만 출력")
    a = ap.parse_args()
    if a.status:
        status()
    else:
        extract(a.groups or ORDER, a.start, a.end, a.sleep, a.group_sleep, a.dry_run)
