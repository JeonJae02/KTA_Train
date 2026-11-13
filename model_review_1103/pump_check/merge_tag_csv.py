"""이미 받은 일별 펌프 CSV(cache/raw/pump/2026-*.csv)에 추가 태그 열을 덧붙인다.

    python model_review_1103/pump_check/merge_tag_csv.py            # extract_raw_csv.EXTRA_TAGS 전부

1) 추가 태그만 전체 기간을 한 번에 받는다(비트·워드라 가볍다) → cache/raw/pump/_extra_tags.csv
2) 일별 CSV 마다 outer merge — 원래 행 시각 ∪ 추가 태그가 바뀐 시각을 모두 행으로 두고 앞 값으로 채운다.
   LogExtractor 로 처음부터 같이 받은 것과 같은 결과이고, 0.5초보다 짧은 펄스도 빠지지 않는다.
   그래서 행 수가 조금 늘어난다(새 행의 펌프 값은 직전 값).
추가 태그가 이미 전부 있는 파일은 건너뛴다.
"""
import argparse
import contextlib
import io
import os
import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
OUT_DIR = HERE.parent / "cache" / "raw" / "pump"
EXTRA_CSV = OUT_DIR / "_extra_tags.csv"
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(HERE))
from extract_raw_csv import EXTRA_TAGS  # noqa: E402


def _read(path):
    df = pd.read_csv(path, index_col="Time", encoding="utf-8-sig")
    df.index = pd.to_datetime(df.index, format="ISO8601")
    return df


def fetch_extra(tags, start, end):
    if EXTRA_CSV.exists():
        df = _read(EXTRA_CSV)
        if all(t in df.columns for t in tags):
            return df[tags]
    from Log_Extractor import LogExtractor
    with contextlib.redirect_stdout(io.StringIO()):
        ex = LogExtractor(env_path=str(ROOT / ".env"))
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df = ex.get_data(f"{start:%Y-%m-%d %H:%M:%S}", f"{end:%Y-%m-%d %H:%M:%S}", tags)
    if "실패" in buf.getvalue() or df.empty:
        raise RuntimeError("추가 태그 추출 실패:\n" + buf.getvalue()[-1500:])
    missing = [t for t in tags if t not in df.columns]
    if missing:
        raise RuntimeError(f"DB 에 없는 태그: {missing}")
    df = df[tags]
    df.to_csv(EXTRA_CSV, float_format="%.10g", encoding="utf-8-sig")
    print(f"추가 태그 {len(tags)}개: {len(df):,}행 → {EXTRA_CSV.name}", flush=True)
    return df


def merge_one(p, extra, tags):
    df = _read(p)
    df = df.drop(columns=[t for t in tags if t in df.columns])
    t0, t1 = df.index[0], df.index[-1]
    n = len(df)
    # 추가 태그가 바뀐 시각 중 이 파일의 첫 행 ~ 마지막 행 사이. 그 밖의 값은 첫 행에서 ffill 로 들어온다
    new_t = extra.index[(extra.index > t0) & (extra.index <= t1)]
    idx = df.index.union(new_t)
    ext = extra.reindex(extra.index.union(idx)).ffill().reindex(idx)
    out = df.reindex(idx).ffill().join(ext)
    out.index.name = "Time"
    na = int(out[tags].isna().sum().sum())
    tmp = p.with_suffix(".tmp")
    out.to_csv(tmp, float_format="%.10g", encoding="utf-8-sig")
    os.replace(tmp, p)
    return n, len(out), na


def main(start, end):
    tags = EXTRA_TAGS
    files = sorted(OUT_DIR.glob("2026-*.csv"))
    extra = fetch_extra(tags, pd.Timestamp(start), pd.Timestamp(end)).sort_index()
    extra = extra[~extra.index.duplicated(keep="last")]
    for p in files:
        head = pd.read_csv(p, nrows=0, encoding="utf-8-sig").columns
        if all(t in head for t in tags):
            print(f"{p.name}: 이미 있음")
            continue
        n0, n1, na = merge_one(p, extra, tags)
        print(f"{p.name}: {n0:,} → {n1:,}행{f'  (빈 값 {na})' if na else ''}", flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", default="2026-09-11 00:00")
    ap.add_argument("--end", default="2026-11-13 11:00")
    a = ap.parse_args()
    main(a.start, a.end)
