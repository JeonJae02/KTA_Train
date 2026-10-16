"""P1 사출-압력 분석용 일별 데이터 추출 (inj_dip_signal_pull.py 와 같은 태그 구성 — 틱 밀도가 같아야 비교 가능).
저장소 루트에서 실행: python parkkt/code_ongoing/inj_dip_pull_daily.py 2026-09-10 2026-10-06
캐시: parkkt/cache/inj_dip_daily/p1_YYYYMMDD.pkl (이미 있으면 건너뜀, 오늘 날짜는 항상 다시 받음)
"""
import os, sys
sys.path.insert(0, '.')
import pandas as pd
from Log_Extractor import LogExtractor

TAGS = ['Scale_Out___PT_P1', 'Scale_Out___FT_P1', 'Pump_BuildUp_P1', 'Shot_Injection_HD1_P1',
        'Shot_Injection_On_HD1'] + [f'HD1_FOAMING_SHOT{k}' for k in range(1, 5)] + ['g_s_SV_P1']
OUT = 'parkkt/cache/inj_dip_daily'
DAY_START, DAY_END = '07:00:00', '23:50:00'   # Cycle_Feature_Extractor 의 DAILY_EXCLUDE 와 같은 운영시간

if __name__ == '__main__':
    os.makedirs(OUT, exist_ok=True)
    d0, d1 = pd.Timestamp(sys.argv[1]), pd.Timestamp(sys.argv[2])
    today = pd.Timestamp.now().normalize()
    ex = LogExtractor('.env')
    for d in pd.date_range(d0, d1):
        p = f'{OUT}/p1_{d:%Y%m%d}.pkl'
        if os.path.exists(p) and d < today:
            continue
        end = 'now()' if d == today else f'{d:%Y-%m-%d} {DAY_END}'
        df = ex.get_data(f'{d:%Y-%m-%d} {DAY_START}', end, TAGS)
        if df is None or len(df) == 0:
            print(f'{d:%Y-%m-%d}: 데이터 없음'); continue
        df.to_pickle(p); print(f'{d:%Y-%m-%d}: {len(df)}행 저장')
