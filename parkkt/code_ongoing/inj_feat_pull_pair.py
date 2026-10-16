"""짝펌프(P2, HD1 slot2) 사출 신호만 따로 일별 추출 -> 생산 틱에 시간 asof 로 붙여 쓴다.
(같이 받으면 틱이 늘어나므로 따로 받는다.) 저장소 루트에서:
python parkkt/code_ongoing/inj_feat_pull_pair.py 2026-09-10 2026-10-06
"""
import os, sys
sys.path.insert(0, '.')
import pandas as pd
from Log_Extractor import LogExtractor

TAGS = ['Shot_Injection_HD1_P2']
OUT = 'parkkt/cache/inj_feat_daily/pair'

if __name__ == '__main__':
    os.makedirs(OUT, exist_ok=True)
    ex = LogExtractor('.env')
    for d in pd.date_range(sys.argv[1], sys.argv[2]):
        p = f'{OUT}/p2inj_{d:%Y%m%d}.pkl'
        if os.path.exists(p):
            continue
        df = ex.get_data(f'{d:%Y-%m-%d} 07:00:00', f'{d:%Y-%m-%d} 23:50:00', TAGS)
        if df is None or len(df) == 0:
            print(f'{d:%Y-%m-%d}: 없음'); continue
        df.to_pickle(p); print(f'{d:%Y-%m-%d}: {len(df)}행')
