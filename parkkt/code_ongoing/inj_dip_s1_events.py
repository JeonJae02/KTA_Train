"""S1(Shot_Injection_HD1_P1) 이벤트 단위 표 — '사출했는데 안 떨어짐'이 어떤 조건에서 생기는지 보기 위한 속성 부여."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import numpy as np, pandas as pd
import inj_dip_signal_base as A
from inj_dip_signal_base import segs

A.MIN_TICK = 1; PRE = 2; POST = 2
PT, FT = 'Scale_Out___PT_P1', 'Scale_Out___FT_P1'


def prep_raw(raw):
    """LogExtractor 원본(DatetimeIndex) -> ffill, 사이클/틱 부여 (inj_dip_signal_base.load 와 같은 규칙)."""
    df = raw.drop(columns='source', errors='ignore').ffill().fillna(0)
    t = df.index.values
    df = df.reset_index(drop=True); df['Time'] = t
    bu = (df.Pump_BuildUp_P1 != 0).values
    df['bu'] = bu; df['cyc'] = np.cumsum(np.r_[True, bu[1:] != bu[:-1]])
    df['tick'] = df.groupby('cyc').cumcount()
    return df


def s1_events(df):
    v = df[PT].values.astype(float); ft = df[FT].values.astype(float); bu = df.bu.values
    cyc = df.cyc.values; tick = df.tick.values; n = len(v)
    S1 = df.Shot_Injection_HD1_P1.values != 0; S2 = df.Shot_Injection_On_HD1.values != 0
    ph = sum(k * (df[f'HD1_FOAMING_SHOT{k}'].values != 0) for k in range(1, 5))
    cyc_len = df.groupby('cyc').tick.transform('size').values
    # 사이클 안에서 몇 번째 S1 인지
    rows = []; seen = {}
    for s, e in segs(S1):
        r = A.event_drop(v, bu, cyc, tick, s, e)
        k = seen.get(cyc[s], 0) + 1; seen[cyc[s]] = k
        if not r:
            continue
        base, pct, L = r
        resp = np.arange(s, min(e + POST, n - 1) + 1); resp = resp[bu[resp] & (cyc[resp] == cyc[s])]
        i_min = resp[np.argmin(v[resp])]
        # 사이클 '안정 기준' = 이 사이클 BU 중 tick>=1 의 중앙값 — 기준 2틱이 이미 내려가 있었는지 확인용
        cyc_mask = (cyc == cyc[s]) & bu & (tick >= 1)
        rows.append(dict(
            Time=pd.Timestamp(df.Time.iloc[s]), cyc=cyc[s], start_tick=tick[s], s1_len=e - s + 1,
            nth_s1=k, phase=ph[s], cyc_len=cyc_len[s], ticks_to_end=cyc_len[s] - 1 - tick[s],
            resp_len=L, base=base, drop_pct=pct, drop_abs=base - v[i_min], min_at=i_min - s,
            pre_s2=bool(S2[s - PRE:s].any()),                      # 기준 2틱 안에 이미 S2(다른 사출)가 켜져 있었나
            base_vs_cyc=(base / np.median(v[cyc_mask]) - 1) * 100,  # 기준이 사이클 중앙값보다 몇 % 높/낮았나
            sv=df.g_s_SV_P1.values[s], ft_dev=(ft[s] / df.g_s_SV_P1.values[s] - 1) * 100 if df.g_s_SV_P1.values[s] else np.nan,
        ))
    return pd.DataFrame(rows)


if __name__ == '__main__':
    out = []
    for name in ['fri', 'today']:
        e = s1_events(prep_raw(pd.read_pickle(f'parkkt/cache/inj_dip_p1_{name}.pkl'))); e['day'] = name; out.append(e)
    E = pd.concat(out, ignore_index=True)
    E['nodrop'] = E.drop_pct < 5
    pd.set_option('display.width', 200)
    print('전체 n', len(E), ' 하락<5% 비율', E.nodrop.mean().round(3))
    for col, bins in [('start_tick', None), ('phase', None), ('s1_len', None), ('nth_s1', None), ('pre_s2', None),
                      ('ticks_to_end', [-1, 2, 4, 6, 8, 99]), ('base_vs_cyc', [-99, -2, -0.5, 0.5, 2, 99]),
                      ('sv', [0, 2200, 2400, 2600, 2800, 9999]), ('base', [0, 95, 105, 115, 999]), ('resp_len', None),
                      ('ft_dev', [-999, -10, -3, 3, 999])]:
        g = E.groupby(pd.cut(E[col], bins) if bins else E[col], observed=True)
        t = g.agg(n=('drop_pct', 'size'), drop_med=('drop_pct', 'median'), nodrop=('nodrop', 'mean'))
        t = t[t.n >= 20]
        print(f'\n--- {col}'); print(t.round(2).T.to_string())
    print('\n--- day x phase'); print(E.pivot_table(index='day', columns='phase', values='nodrop', aggfunc='mean').round(2))
    print('\n--- start_tick x pre_s2'); print(E.pivot_table(index='start_tick', columns='pre_s2', values='drop_pct', aggfunc=['median', 'size']).round(2))
    print('\n--- min_at (최저점이 S1 시작 몇 틱 뒤)'); print(E.groupby('nodrop').min_at.value_counts(normalize=True).unstack().round(2))
