import sys; sys.path.insert(0,'.')  # 저장소 루트에서 실행
from Log_Extractor import LogExtractor
ex=LogExtractor('.env')
tags=['Scale_Out___PT_P1','Scale_Out___FT_P1','Pump_BuildUp_P1','Shot_Injection_HD1_P1','Shot_Injection_On_HD1']+[f'HD1_FOAMING_SHOT{k}' for k in range(1,5)]+['g_s_SV_P1']
for name,(a,b) in {'fri':('2026-10-02 07:00:00','2026-10-02 11:30:00'),'today':('2026-10-06 07:00:00','now()')}.items():
    df=ex.get_data(a,b,tags); df.to_pickle(f'parkkt/cache/inj_dip_p1_{name}.pkl'); print(name, df.shape, df.index.min(), df.index.max())
