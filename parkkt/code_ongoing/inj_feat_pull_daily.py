"""사출 피처 검증용 일별 추출 — **생산 AE 태그 8개 + 사출 태그 6개**.
저장소 루트에서: python parkkt/code_ongoing/inj_feat_pull_daily.py 2026-09-10 2026-10-06
캐시: parkkt/cache/inj_feat_daily/p1_YYYYMMDD.pkl

사출 태그를 같이 받으면 '사출 태그만 바뀐 스캔'이 행으로 끼어든다 (1시간에 약 34행, 0.5%).
생산 AE 에는 없는 틱이므로 inj_feat_lib.to_prod_ticks() 로 걷어내야 생산 틱과 정확히 같아진다
(2026-10-06 09~10시 6,598행 일치 확인, inj_feat_tick_check.py).
"""
import os, sys
sys.path.insert(0, '.')
import pandas as pd
from Log_Extractor import LogExtractor

PROD = ['Pump_BuildUp_P1', 'RB1_Robot_Num', 'g_s_SV_P1', 'Ana_Out_P1', 'Scale_Out___PT_P1',
        'Scale_Out___FT_P1', 'TK_Temp_PV_P1', 'TK_Level_PV_P1']          # train_all_models.py 와 동일
INJ = ['Shot_Injection_HD1_P1', 'Shot_Injection_On_HD1'] + [f'HD1_FOAMING_SHOT{k}' for k in range(1, 5)]
OUT = 'parkkt/cache/inj_feat_daily'

if __name__ == '__main__':
    os.makedirs(OUT, exist_ok=True)
    d0, d1 = pd.Timestamp(sys.argv[1]), pd.Timestamp(sys.argv[2])
    today = pd.Timestamp.now().normalize()
    ex = LogExtractor('.env')
    for d in pd.date_range(d0, d1):
        p = f'{OUT}/p1_{d:%Y%m%d}.pkl'
        if os.path.exists(p) and d < today:
            continue
        df = ex.get_data(f'{d:%Y-%m-%d} 07:00:00', f'{d:%Y-%m-%d} 23:50:00', PROD + INJ)
        if df is None or len(df) == 0:
            print(f'{d:%Y-%m-%d}: 데이터 없음'); continue
        df.to_pickle(p); print(f'{d:%Y-%m-%d}: {len(df)}행 저장')
