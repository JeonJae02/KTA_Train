"""9/10 ~ 오늘, 평일(추석 9/24·25 제외)을 하루씩 받아 캐시한다. 이미 받은 날은 건너뛴다.

    PYTHONIOENCODING=utf-8 python parkkt/conv/conv_wagon_reset_pull.py
"""
import os
import sys

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path[:0] = [HERE, ROOT]
import conv_wagon_reset as cw

CACHE = os.path.join(ROOT, "parkkt", "cache", "conv_wagon_reset", "daily")
HOLIDAYS = {"2026-09-24", "2026-09-25"}          # 추석 (9/26 은 토요일)


def work_days(start="2026-09-10", end=None):
    end = end or pd.Timestamp.now().strftime("%Y-%m-%d")
    days = pd.date_range(start, end, freq="D")
    return [d for d in days if d.weekday() < 5 and d.strftime("%Y-%m-%d") not in HOLIDAYS]


def load_days(days=None):
    """캐시된 날을 이어 붙인다."""
    days = days or work_days()
    parts = [pd.read_pickle(p) for d in days if os.path.exists(p := os.path.join(CACHE, f"{d:%Y%m%d}.pkl"))]
    return pd.concat(parts).sort_index()


if __name__ == "__main__":
    os.chdir(ROOT)                  # Log_Extractor 가 .env 를 상대경로로 읽는다
    os.makedirs(CACHE, exist_ok=True)
    now = pd.Timestamp.now()
    for d in work_days():
        path = os.path.join(CACHE, f"{d:%Y%m%d}.pkl")
        today = d.normalize() == now.normalize()
        if os.path.exists(path) and not today:
            continue
        end = now if today else d + pd.Timedelta(days=1)
        df = cw.pull(f"{d:%Y-%m-%d} 00:00:00", f"{end:%Y-%m-%d %H:%M:%S}")
        df = df[(df.index >= d) & (df.index < d + pd.Timedelta(days=1))]
        df.to_pickle(path)
        print(f"== {d:%Y-%m-%d} {len(df)}행, 없는 태그: {[c for c in cw.CNT_TAGS + cw.SIG_TAGS + [cw.WAGON] if c not in df or df[c].isna().all()]}", flush=True)
