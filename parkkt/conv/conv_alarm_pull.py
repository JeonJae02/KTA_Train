"""라인 알람(AL_Line_Word = 에러 번호, AL_Line_Bit) 을 conv_wagon_reset_pull 과 같은 날짜로 하루씩 받아 캐시한다.

    PYTHONIOENCODING=utf-8 python parkkt/conv/conv_alarm_pull.py
"""
import os
import sys

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path[:0] = [HERE, ROOT]
import conv_wagon_reset_pull as cp

ALARM_TAGS = ["AL_Line_Word", "AL_Line_Bit"]
CACHE = os.path.join(ROOT, "parkkt", "cache", "conv_wagon_reset", "alarm_daily")


def load_days(days=None):
    days = days or cp.work_days()
    parts = [pd.read_pickle(p) for d in days if os.path.exists(p := os.path.join(CACHE, f"{d:%Y%m%d}.pkl"))]
    return pd.concat(parts).sort_index()


if __name__ == "__main__":
    os.chdir(ROOT)
    from Log_Extractor import LogExtractor
    ex = LogExtractor()
    os.makedirs(CACHE, exist_ok=True)
    now = pd.Timestamp.now()
    for d in cp.work_days():
        path = os.path.join(CACHE, f"{d:%Y%m%d}.pkl")
        today = d.normalize() == now.normalize()
        if os.path.exists(path) and not today:
            continue
        end = now if today else d + pd.Timedelta(days=1)
        df = ex.get_data(f"{d:%Y-%m-%d} 00:00:00", f"{end:%Y-%m-%d %H:%M:%S}", ALARM_TAGS)
        df.index = pd.to_datetime(df.index)
        df = df[~df.index.duplicated()].sort_index()
        df = df[(df.index >= d) & (df.index < d + pd.Timedelta(days=1))]
        df.to_pickle(path)
        print(f"== {d:%Y-%m-%d} {len(df)}행 코드: {sorted(df['AL_Line_Word'].dropna().unique().astype(int).tolist()) if 'AL_Line_Word' in df else '없음'}", flush=True)
