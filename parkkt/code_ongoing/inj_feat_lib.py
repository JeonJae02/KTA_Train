"""틱 AE 용 '사출 플래그 + 압력 차분' 피처 검증 라이브러리 (P1).

틱/샷 정의는 생산 전처리(Ana_Preprocess.VirtualSensorPreprocessor)와 같게 맞춘다:
  - 틱 = 생산 태그 8개 중 하나라도 값이 바뀐 스캔 (사출 태그만 바뀐 스캔은 제거)
  - 샷 시작 = BuildUp 0->1 또는 가동 중 RB1_Robot_Num 변경, Tick_Index 는 샷마다 0부터
  - PT 이력은 샷마다 초기화 -> 차분은 같은 샷 안에서만 (첫 틱은 없음)
  - Phase_Start = Tick_Index<=2, Phase_Transition = 직전 틱 대비 SV 변경 (생산과 같이 샷 경계에서도 비교)

후보 사출 플래그 (모두 0/1 디지털, 같은 틱 t 의 값):
  S1 = Shot_Injection_HD1_P1, S2 = Shot_Injection_On_HD1, S3 = HD1_FOAMING_SHOT1~4 중 하나라도 ON,
  S4 = S2 AND (FOAMING 1~4 중 하나) -> S2 가 항상 FOAMING 안에 있으면 S2 와 똑같아진다(확인용).
"""
import os, sys, glob
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import numpy as np, pandas as pd

CACHE = 'parkkt/cache/inj_feat_daily'
PROD = ['Pump_BuildUp_P1', 'RB1_Robot_Num', 'g_s_SV_P1', 'Ana_Out_P1', 'Scale_Out___PT_P1',
        'Scale_Out___FT_P1', 'TK_Temp_PV_P1', 'TK_Level_PV_P1']
INJ = ['Shot_Injection_HD1_P1', 'Shot_Injection_On_HD1'] + [f'HD1_FOAMING_SHOT{k}' for k in range(1, 5)]
HOLIDAY = pd.Timestamp('2026-09-24')
BIG = 5      # '큰 하락' 기준(눈금). 사출 없는 흔들림 P99 ≈ 4.5~5.5 눈금 (inj_dip_rule_eval.py)
SIGNALS = ['S1', 'S2', 'S3', 'S4']
SIG_NAME = {'S1': 'S1 Shot_Injection_HD1_P1', 'S2': 'S2 Shot_Injection_On_HD1',
            'S3': 'S3 FOAMING1~4 (아무거나)', 'S4': 'S4 On_HD1 ∧ FOAMING'}


def to_prod_ticks(raw):
    df = raw.drop(columns='source', errors='ignore')
    same_prod = (df[PROD].diff().fillna(1) == 0).all(axis=1)
    inj_chg = (df[INJ].diff().fillna(0) != 0).any(axis=1)
    return df[~(same_prod & inj_chg)].copy()


def features(raw):
    """생산 틱 + 샷/틱 인덱스 + 차분 + 후보 플래그. BuildUp=1 틱만 반환."""
    df = to_prod_ticks(raw)
    n_removed = len(raw) - len(df)
    bu = (df.Pump_BuildUp_P1 != 0).values
    rb = df.RB1_Robot_Num.values
    new_shot = bu & (np.r_[True, ~bu[:-1]] | np.r_[False, rb[1:] != rb[:-1]])
    shot = np.cumsum(new_shot)
    out = pd.DataFrame({'Time': df.index, 'bu': bu, 'shot': shot,
                        'PT': df.Scale_Out___PT_P1.values.astype(float),
                        'FT': df.Scale_Out___FT_P1.values.astype(float),
                        'SV': df.g_s_SV_P1.values.astype(float)})
    sv = out.SV.values
    out['Phase_Transition'] = np.r_[0, (sv[1:] != sv[:-1]).astype(int)]
    out = out[out.bu].copy()
    g = out.groupby('shot')
    out['Tick_Index'] = g.cumcount()
    out['dPT1'] = out.PT - g.PT.shift(1)            # PT(t) - PT(t-1)
    out['dPT2'] = out.PT - g.PT.shift(2)            # PT(t) - PT(t-2)  (= 생산 Rolling_PT_Diff_3, tick>=2)
    out['dPT1_pct'] = out.dPT1 / g.PT.shift(1) * 100
    # 최근 3틱 안에 SV 변경 (생산 Phase_Transition 은 그 틱만 1 이지만, 압력은 1~2틱 뒤에 따라 내려간다)
    out['SV_chg_recent'] = g.Phase_Transition.transform(lambda s: s.rolling(4, min_periods=1).max()).astype(bool)
    pos = out.index.values          # out 은 df 와 같은 순서의 RangeIndex 에서 BuildUp 행만 남긴 것 = df 위치
    S1 = (df.Shot_Injection_HD1_P1 != 0).values[pos]
    S2 = (df.Shot_Injection_On_HD1 != 0).values[pos]
    F = (df[[f'HD1_FOAMING_SHOT{k}' for k in range(1, 5)]] != 0).any(axis=1).values[pos]
    out['S1'], out['S2'], out['S3'], out['S4'] = S1.astype(int), S2.astype(int), F.astype(int), (S2 & F).astype(int)
    out.attrs['n_removed'] = n_removed
    return out.reset_index(drop=True)


def add_lags(t):
    """정렬 변형: lag1 = 한 틱 전 신호, hold1/hold2 = 최근 1/2틱 안에 신호가 있었음(샷 안에서)."""
    g = t.groupby('shot')
    for s in SIGNALS:
        p1 = g[s].shift(1).fillna(0); p2 = g[s].shift(2).fillna(0)
        t[f'{s}_lag1'] = p1.astype(int)
        t[f'{s}_hold1'] = ((t[s] + p1) > 0).astype(int)
        t[f'{s}_hold2'] = ((t[s] + p1 + p2) > 0).astype(int)
    return t


def load_all(force=False):
    p_out = f'{CACHE}/_ticks.pkl'
    if os.path.exists(p_out) and not force:
        return pd.read_pickle(p_out)
    parts, removed = [], {}
    for p in sorted(glob.glob(f'{CACHE}/p1_*.pkl')):
        raw = pd.read_pickle(p)
        if (raw.Pump_BuildUp_P1 != 0).mean() < 0.05 or (raw.Shot_Injection_HD1_P1 != 0).sum() == 0:
            continue
        t = features(raw); day = pd.Timestamp(os.path.basename(p)[3:11])
        t['day'] = day; t['shot'] = t.shot.astype(np.int64) + int(day.strftime('%Y%m%d')) * 100000
        removed[day] = t.attrs['n_removed']
        parts.append(add_lags(t))
    T = pd.concat(parts, ignore_index=True)
    T['era'] = np.where(T.day < HOLIDAY, '연휴 전', '연휴 후')
    T.attrs['removed'] = removed
    pd.to_pickle(T, p_out)
    return T


def judged(T, col='dPT1'):
    """판정 대상: 차분이 정의되는 틱. 증압 구간(Phase_Start, tick<=2)은 따로 본다."""
    return T[T[col].notna() & (T.Tick_Index >= 3)]


def flag_metrics(T, flag, col='dPT1', big=BIG):
    J = judged(T, col)
    f = J[flag] == 1; d = J[col]
    drop = d <= -big
    p1, p0 = drop[f].mean(), drop[~f].mean()
    return {
        '플래그 ON 비율%': f.mean() * 100,
        'ON 평균': d[f].mean(), 'OFF 평균': d[~f].mean(),
        'ON P10': d[f].quantile(.1), 'OFF P10': d[~f].quantile(.1),
        f'ON 큰하락%(≤-{big})': p1 * 100, f'OFF 큰하락%': p0 * 100,
        '배율(ON/OFF)': p1 / p0 if p0 else np.nan,
        '큰하락 중 ON 비율%': f[drop].mean() * 100,
        'MI(bits)': mutual_info(f.values, d.values),
    }


def mutual_info(flag, d, bins=(-np.inf, -9, -7, -5, -3, -1, 0.5, 2.5, 4.5, 6.5, np.inf)):
    """플래그(0/1)와 차분(구간화) 사이 상호정보량. 플래그가 차분에 대해 알려주는 정보의 양."""
    b = np.digitize(d, bins[1:-1])
    pxy = pd.crosstab(flag, b, normalize=True).values
    px = pxy.sum(1, keepdims=True); py = pxy.sum(0, keepdims=True)
    nz = pxy > 0
    return float((pxy[nz] * np.log2(pxy[nz] / (px @ py)[nz])).sum())


def aligned_profile(T, s, offsets=range(-3, 7), col='dPT1'):
    """신호 s 상승 엣지(샷 안, Tick_Index>=3) 기준 오프셋별 차분 평균과 신호 ON 비율."""
    T = T.reset_index(drop=True)
    on = T[s].values; shot = T.shot.values; ti = T.Tick_Index.values
    edges = np.where((on == 1) & np.r_[True, on[:-1] == 0] & np.r_[False, shot[1:] == shot[:-1]] & (ti >= 3))[0]
    rows = []
    for o in offsets:
        j = edges + o; ok = (j >= 0) & (j < len(T)); j = j[ok]; j = j[shot[j] == shot[edges[ok]]]
        rows.append({'offset': o, 'n': len(j), 'mean': T[col].values[j].mean() if len(j) else np.nan,
                     'p10': np.nanpercentile(T[col].values[j], 10) if len(j) else np.nan,
                     'big%': np.mean(T[col].values[j] <= -BIG) * 100 if len(j) else np.nan,
                     'on%': on[j].mean() * 100 if len(j) else np.nan})
    return pd.DataFrame(rows).set_index('offset'), len(edges)
