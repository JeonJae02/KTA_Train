"""틱 AE 피처 제안 — 참조 구현 (P1 기준).

`parkkt/AE_사출피처_최종정리.ipynb` 에서 정리한 피처를 원본 로그에서 만드는 방법을 코드로 고정해 둔다.
생산 전처리(Ana_Preprocess.VirtualSensorPreprocessor)에 옮길 때 이 함수의 결과와 같아야 한다.

입력
  raw_main : LogExtractor 결과 (생산 8태그 + Shot_Injection_HD1_P1 + Shot_Injection_On_HD1 + HD1_FOAMING_SHOT1~4)
  raw_pair : LogExtractor 결과 (Shot_Injection_HD1_P2 만 따로 받은 것)

틱 / 구간 정의
  - 틱 = 생산 태그 8개 중 하나라도 값이 바뀐 스캔. 사출 태그만 바뀐 스캔은 버린다 (to_prod_ticks).
  - 샷 = BuildUp 0->1 또는 가동 중 RB1_Robot_Num 변경으로 시작. Tick_Index 는 샷마다 0 부터.
  - 꺼짐(Decay) 틱 = BuildUp 1->0 이후 DECAY_TICKS 개. 그 뒤 대기 틱은 AE 입력에서 뺀다.
  - 차분(dPT1, dPT2)은 '샷 + 그 뒤 꺼짐 틱' 안에서 이어서 계산한다 (꺼질 때의 압력 하락이 보이도록).
    새 샷 첫 틱은 이전 값이 없으므로 0.
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import numpy as np, pandas as pd
from inj_feat_lib import to_prod_ticks

DECAY_TICKS = 6          # OFF 후 PT 는 1틱, FT 는 5~6틱에 걸쳐 내려간다
INJ_AFTER_TICKS = 2      # S1 이 꺼진 뒤 이만큼은 '사출 직후' (회복 또는 늦은 하락)
PT_SETTLE = {'on': 1, 'sv': 2}   # 압력: 켜진 뒤 tick>=1, SV 변경 후 2틱 지나면 안정
FT_SETTLE = {'on': 4, 'sv': 3}   # 유량: 켜진 뒤 tick>=4, SV 변경 후 3틱 지나면 안정

FEATURES = ['Inj_Flag', 'Inj_After', 'Pair_Inj_Flag', 'dPT1', 'dPT2', 'dSV2',
            'BuildUp', 'Phase_Decay', 'PT_Settled', 'FT_Settled']


def build_features(raw_main, raw_pair, pump='P1', pair_tag='Shot_Injection_HD1_P2', inj_tag='Shot_Injection_HD1_P1'):
    df = to_prod_ticks(raw_main).copy()
    t = df.index
    p2 = raw_pair[pair_tag].ffill().fillna(0)
    df['P2'] = pd.merge_asof(pd.DataFrame({'Time': t}), p2.rename('P2').rename_axis('T2').reset_index(),
                             left_on='Time', right_on='T2', direction='backward')['P2'].fillna(0).values
    bu = (df[f'Pump_BuildUp_{pump}'] != 0).values
    rb = df['RB1_Robot_Num'].values
    n = len(df)
    new_shot = bu & (np.r_[True, ~bu[:-1]] | np.r_[False, rb[1:] != rb[:-1]])
    shot = np.cumsum(new_shot)                          # OFF 틱은 직전 샷 번호를 이어받는다
    tick = pd.Series(np.where(bu, 1, 0)).groupby(shot).cumsum().values - 1
    off_k = pd.Series((~bu).astype(int)).groupby(shot).cumsum().values - 1   # 샷 안에서 OFF 몇 번째
    keep = bu | ((~bu) & (off_k < DECAY_TICKS) & (shot > 0))

    pt = df[f'Scale_Out___PT_{pump}'].values.astype(float)
    sv = df[f'g_s_SV_{pump}'].values.astype(float)
    s1 = (df[inj_tag] != 0).values.astype(int)
    out = pd.DataFrame({'Time': t, 'shot': shot, 'Tick_Index': np.where(bu, tick, -1), 'Off_Index': np.where(bu, -1, off_k),
                        'PT': pt, 'FT': df[f'Scale_Out___FT_{pump}'].values, 'SV': sv, 'S1': s1,
                        'P2': (df.P2.values != 0).astype(int), 'BuildUp': bu.astype(int)})
    out = out[keep].reset_index(drop=True)
    g = out.groupby('shot')
    out['dPT1'] = (out.PT - g.PT.shift(1)).fillna(0)
    out['dPT2'] = (out.PT - g.PT.shift(2)).fillna(out.dPT1)
    # dSV2: 같은 샷의 가동 틱 안에서만 (꺼짐 틱은 0)
    on = out[out.BuildUp == 1]; go = on.groupby('shot')
    d = (on.SV - go.SV.shift(2)).fillna(on.SV - go.SV.shift(1)).fillna(0)
    out['dSV2'] = 0.0; out.loc[on.index, 'dSV2'] = d
    chg = (on.SV != go.SV.shift(1)) & go.SV.shift(1).notna()
    c = chg.groupby(on.shot).cumsum()
    since = on.groupby([on.shot, c]).cumcount().where(c > 0, 99)
    out['since_sv'] = 99; out.loc[on.index, 'since_sv'] = since
    out['Inj_Flag'] = out.S1
    prev = sum(g.S1.shift(k).fillna(0) for k in range(1, INJ_AFTER_TICKS + 1))
    out['Inj_After'] = ((out.S1 == 0) & (prev > 0)).astype(int)
    out['Pair_Inj_Flag'] = out.P2
    out['Phase_Decay'] = (out.BuildUp == 0).astype(int)
    out['PT_Settled'] = ((out.BuildUp == 1) & (out.Tick_Index >= PT_SETTLE['on']) & (out.since_sv >= PT_SETTLE['sv'])).astype(int)
    out['FT_Settled'] = ((out.BuildUp == 1) & (out.Tick_Index >= FT_SETTLE['on']) & (out.since_sv >= FT_SETTLE['sv'])).astype(int)
    return out


def load_day_features(day, cache='parkkt/cache/inj_feat_daily'):
    d = pd.Timestamp(day)
    raw = pd.read_pickle(f'{cache}/p1_{d:%Y%m%d}.pkl')
    pair = pd.read_pickle(f'{cache}/pair/p2inj_{d:%Y%m%d}.pkl')
    return build_features(raw, pair)
