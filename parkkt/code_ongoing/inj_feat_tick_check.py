"""사출 태그를 생산 AE 태그 구성에 추가해도 '틱'(행)이 그대로인지 확인.
생산(train_all_models.py) 8태그 / 생산 8태그 + 사출 6태그 / 이번 분석용 10태그 를 같은 구간으로 받아 비교한다.
결과는 parkkt/cache/inj_feat_tick_check.pkl
"""
import sys
sys.path.insert(0, '.')
import pandas as pd
from Log_Extractor import LogExtractor

PROD = ['Pump_BuildUp_P1', 'RB1_Robot_Num', 'g_s_SV_P1', 'Ana_Out_P1', 'Scale_Out___PT_P1',
        'Scale_Out___FT_P1', 'TK_Temp_PV_P1', 'TK_Level_PV_P1']
INJ = ['Shot_Injection_HD1_P1', 'Shot_Injection_On_HD1'] + [f'HD1_FOAMING_SHOT{k}' for k in range(1, 5)]
MINE = ['Scale_Out___PT_P1', 'Scale_Out___FT_P1', 'Pump_BuildUp_P1', 'g_s_SV_P1'] + INJ

if __name__ == '__main__':
    a, b = sys.argv[1], sys.argv[2]
    ex = LogExtractor('.env')
    out = {k: ex.get_data(a, b, tags) for k, tags in
           {'prod': PROD, 'prod_inj': PROD + INJ, 'mine': MINE}.items()}
    pd.to_pickle(out, 'parkkt/cache/inj_feat_tick_check.pkl')
    for k, d in out.items():
        print(k, len(d))
