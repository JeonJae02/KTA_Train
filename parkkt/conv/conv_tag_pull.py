"""임의 태그 묶음을 conv_wagon_reset_pull 과 같은 날짜(평일, 추석 제외)로 하루씩 받아 캐시한다.

    PYTHONIOENCODING=utf-8 python parkkt/conv/conv_tag_pull.py stopper_fwd
그룹 이름과 태그는 GROUPS 에 적는다. 캐시: parkkt/cache/conv_wagon_reset/<그룹>/YYYYMMDD.pkl
"""
import os
import sys

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path[:0] = [HERE, ROOT]
import conv_wagon_reset_pull as cp

#: 태그명 공백 주의 — 후진감지는 '전진 Unit'(전진과 Unit 사이 공백)이다.
GROUPS = {
    "stopper_fwd": ["전진Unit  캐리어  스톱퍼CY 전진감지", "전진 Unit  캐리어 스톱퍼CY 후진감지", "전진Unit 스톱퍼 SOL"],
    "air_supply2_stopper": ["Air_Supply2_Stopper_CY", "Air_Supply2_Stopper_FWD_CS", "Air_Supply2_Stopper_REV_CS"],
    "air_supply2_home": ["Air_Supply2_Home_CHK_Err", "Conv_Run_Warning", "Air_Supply2_In_CHK_P", "Air_Supply2_In_CHK_PX"],
    "air_supply2_home_state": ["Air_Supply2_Home_CHK", "Air_Supply2_Home_Off_CHK", "Air_Supply2_ORG_PX", "Air_Supply2_Rev_PX",
                               "Air_Supply2_In_CHK_Stopper_FWD", "Air_Supply2_Nozzle_REV_CHK"],
    "air_supply2_nozzle": ["Air_Supply2_Nozzle_CY"],
    "air_supply2_nozzle_cs": ["Air_Supply2_Nozzle1_REV_CS", "Air_Supply2_Nozzle2_REV_CS"],
}


def cache_dir(group):
    return os.path.join(ROOT, "parkkt", "cache", "conv_wagon_reset", group)


def load_days(group, days=None):
    days = days or cp.work_days()
    d0 = cache_dir(group)
    parts = [pd.read_pickle(p) for d in days if os.path.exists(p := os.path.join(d0, f"{d:%Y%m%d}.pkl"))]
    df = pd.concat(parts).sort_index()
    return df[~df.index.duplicated()]


def pull_days(group):
    from Log_Extractor import LogExtractor
    ex = LogExtractor()
    d0 = cache_dir(group); os.makedirs(d0, exist_ok=True)
    now = pd.Timestamp.now()
    for d in cp.work_days():
        path = os.path.join(d0, f"{d:%Y%m%d}.pkl")
        today = d.normalize() == now.normalize()
        if os.path.exists(path) and not today:
            continue
        end = now if today else d + pd.Timedelta(days=1)
        df = ex.get_data(f"{d:%Y-%m-%d} 00:00:00", f"{end:%Y-%m-%d %H:%M:%S}", GROUPS[group])
        df.index = pd.to_datetime(df.index)
        df = df[~df.index.duplicated()].sort_index()
        df = df[(df.index >= d) & (df.index < d + pd.Timedelta(days=1))]
        df.to_pickle(path)
        print(f"== {d:%Y-%m-%d} {len(df)}행", flush=True)


if __name__ == "__main__":
    os.chdir(ROOT)
    pull_days(sys.argv[1])
