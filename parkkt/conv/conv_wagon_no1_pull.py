"""전진/후진 Unit '캐리어 1번감지' 센서를 conv_wagon_reset_pull 과 같은 날짜로 하루씩 받아 캐시한다.

1번 대차가 그 Unit 을 지날 때 켜지는 센서다. 펄스가 짧아 DB 에는 일부만 찍힌다.
    PYTHONIOENCODING=utf-8 python parkkt/conv/conv_wagon_no1_pull.py
"""
import os
import sys

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path[:0] = [HERE, ROOT]
import conv_wagon_reset_pull as cp

NO1_TAGS = {"MD_CY_FWD": "전진Unit  캐리어 1번감지", "MD_CY_REV": "후진Unit  캐리어 1번감지"}
CACHE = os.path.join(ROOT, "parkkt", "cache", "conv_wagon_reset", "no1_daily")


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
        df = ex.get_data(f"{d:%Y-%m-%d} 00:00:00", f"{end:%Y-%m-%d %H:%M:%S}", list(NO1_TAGS.values()))
        df.index = pd.to_datetime(df.index)
        df = df[~df.index.duplicated()].sort_index()
        df = df[(df.index >= d) & (df.index < d + pd.Timedelta(days=1))]
        df.to_pickle(path)
        print(f"== {d:%Y-%m-%d} {len(df)}행", flush=True)
