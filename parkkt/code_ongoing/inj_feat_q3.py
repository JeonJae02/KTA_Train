"""사용자 질문 3가지 + 피처 예시 (P1, 생산 틱).
Q1 S1(사출) 이벤트마다 압력이 실제로 떨어졌나, 얼마나
Q2 사출했는데 안 떨어진 비율, 짝펌프(P2) 사출·SV 변경 등으로 설명되나
Q3 사출 없는데 떨어진 비율, 원인, 크기 분포 (AE 노이즈 바닥)
EX 한 샷의 틱별 피처 벡터 예시 (Phase / PT·FT 안정 분리 / dSV2)
"""
import os, sys, glob
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import numpy as np, pandas as pd
from inj_feat_lib import load_all, to_prod_ticks, CACHE

pd.set_option('display.width', 250); pd.set_option('display.max_columns', 30)
SVB_EDGES = [0, 2100, 2200, 2400, 2600, 9999]
SVB_LABELS = ['SV ≤2100', '2101~2200', '2201~2400', '2401~2600', '>2600']
NOISE = -3    # 사출 없는 안정 틱 dPT1 하위 5% = -4 -> '-3 이상'은 노이즈 범위 안 = 하락 없음


def attach_pair(T):
    parts = []
    for d, g in T.groupby('day'):
        p = f'{CACHE}/pair/p2inj_{d:%Y%m%d}.pkl'
        s = pd.read_pickle(p)['Shot_Injection_HD1_P2'].ffill().fillna(0)
        m = pd.merge_asof(g[['Time']].reset_index(), s.rename('P2').reset_index().rename(columns={'Time': 'T2', s.index.name or 'index': 'T2'}),
                          left_on='Time', right_on='T2', direction='backward')
        g = g.copy(); g['P2'] = (m['P2'].fillna(0).values != 0).astype(int); parts.append(g)
    return pd.concat(parts)


def add_cols(T):
    g = T.groupby('shot')
    T['dSV2'] = T.SV - g.SV.shift(2)
    T['dSV2'] = T.dSV2.fillna(T.SV - g.SV.shift(1)).fillna(0)
    T['svchg_tick'] = (T.SV != g.SV.shift(1)) & g.SV.shift(1).notna()
    # 샷 안 마지막 SV 변경 이후 틱 수 (변경 없으면 큰 값)
    c = T.groupby('shot').svchg_tick.cumsum()
    T['since_sv'] = T.groupby([T.shot, c]).cumcount().where(c > 0, 99)
    T['S1_prev'] = (g.S1.shift(1).fillna(0) + g.S1.shift(2).fillna(0) > 0).astype(int)
    T['svb'] = pd.cut(T.SV, SVB_EDGES, labels=SVB_LABELS)
    return T


def s1_events(T):
    """S1 ON 연속 구간 = 이벤트. ON 틱들에서의 dPT1/dPT2 최저값 (AE 가 '플래그=1' 틱에서 보는 값)."""
    T = T.reset_index(drop=True)
    on = T.S1.values; shot = T.shot.values
    start = (on == 1) & ~(np.r_[False, on[:-1] == 1] & np.r_[False, shot[1:] == shot[:-1]])
    eid = np.cumsum(start) * on
    E = T[on == 1].assign(eid=eid[on == 1]).groupby('eid').agg(
        day=('day', 'first'), era=('era', 'first'), svb=('svb', 'first'), SV=('SV', 'first'),
        start_tick=('Tick_Index', 'first'), n_on=('S1', 'size'),
        min_d1=('dPT1', 'min'), min_d2=('dPT2', 'min'),
        P2_overlap=('P2', 'max'), svchg_recent=('since_sv', lambda x: (x <= 3).any()))
    # 시작 직전 틱에 이미 P2 가 켜져 있었나
    first_idx = np.where(start)[0]
    E['P2_before'] = T.P2.values[np.clip(first_idx - 1, 0, None)][:len(E)] == 1
    return E


def example_rows(day, center, n_after_off=6):
    raw = pd.read_pickle(f'{CACHE}/p1_{pd.Timestamp(day):%Y%m%d}.pkl')
    df = to_prod_ticks(raw).reset_index().rename(columns={'index': 'Time'})
    df = df.rename(columns={df.columns[0]: 'Time'})
    i0 = int(np.argmin(np.abs(df.Time.values - np.datetime64(pd.Timestamp(center)))))
    bu = (df.Pump_BuildUp_P1 != 0).values
    s = i0
    while s > 0 and bu[s - 1]: s -= 1
    e = i0
    while e < len(df) - 1 and bu[e + 1]: e += 1
    sub = df.iloc[max(s - 2, 0): e + 1 + n_after_off].copy()
    b = (sub.Pump_BuildUp_P1 != 0).values
    pt = sub.Scale_Out___PT_P1.values; sv = sub.g_s_SV_P1.values
    tick_on = np.where(b, np.cumsum(b) - 1, -1)
    off_k = np.zeros(len(sub), int); k = -1
    for i in range(len(sub)):
        if b[i]: k = -1
        else: k += 1
        off_k[i] = k if not b[i] else -1
    rows = []
    last_chg = -99
    for i in range(len(sub)):
        on_t = tick_on[i]
        if b[i] and on_t >= 1 and sv[i] != sv[i - 1]: last_chg = on_t
        since = on_t - last_chg if b[i] else None
        d1 = pt[i] - pt[i - 1] if b[i] and on_t >= 1 else 0
        d2 = pt[i] - pt[i - 2] if b[i] and on_t >= 2 else (d1 if b[i] else 0)
        dsv2 = (sv[i] - sv[i - 2]) if b[i] and on_t >= 2 else ((sv[i] - sv[i - 1]) if b[i] and on_t >= 1 else 0)
        ramp = int(b[i] and on_t <= 3)
        decay = int((not b[i]) and 0 <= off_k[i] <= 5 and i >= 2 and tick_on[:i].max() >= 0)
        rows.append({'시각': pd.Timestamp(sub.Time.iloc[i]).strftime('%H:%M:%S.%f')[:-4],
                     'BuildUp': int(b[i]), 'PT': pt[i], 'FT': sub.Scale_Out___FT_P1.values[i], 'SV': sv[i],
                     'Inj_Flag(S1)': int(raw.loc[sub.Time.iloc[i], 'Shot_Injection_HD1_P1'] != 0) if sub.Time.iloc[i] in raw.index else 0,
                     'dPT1': d1 if b[i] else 0, 'dPT2': d2 if b[i] else 0, 'dSV2': dsv2,
                     'Phase_Ramp': ramp, 'Phase_Steady': int(b[i] and on_t >= 4), 'Phase_Decay': decay,
                     'PT_Settled': int(b[i] and on_t >= 1 and (since is None or since >= 2)),
                     'FT_Settled': int(b[i] and on_t >= 4 and (since is None or since >= 3)),
                     '(ON틱/OFF틱)': on_t if b[i] else f'off{off_k[i]}'})
    return pd.DataFrame(rows)


if __name__ == '__main__':
    T = add_cols(attach_pair(load_all()))
    print('S2 == S1 OR P2 ?', ((T.S2 == 1) == ((T.S1 == 1) | (T.P2 == 1))).mean().round(4),
          ' S1&P2 동시 ON 틱 비율(S1 ON 중):', T[T.S1 == 1].P2.mean().round(3))
    T.to_pickle(f'{CACHE}/_ticks_q3.pkl')
    E = s1_events(T)
    E.to_pickle(f'{CACHE}/_s1_events_q3.pkl')
    print(f'\n[Q1] S1 이벤트 {len(E):,}개  (ON 틱 수 분포 {E.n_on.value_counts().sort_index().to_dict()})')
    for col in ['min_d1', 'min_d2']:
        print(f'  {col}:', E.groupby('era')[col].describe(percentiles=[.1, .25, .5, .75, .9]).round(1).to_string())
        print('   하락 단계별 비율%:\n', E.groupby('era')[col].apply(lambda x: pd.Series({
            '≤-5 (큰 하락)': (x <= -5).mean() * 100, '-4~-5 사이 아님': np.nan, '≤-4': (x <= -4).mean() * 100,
            '-3~-1 (작은 하락)': ((x <= -1) & (x > -4)).mean() * 100, '≥0 (전혀 안 떨어짐)': (x >= 0).mean() * 100})).unstack().drop(columns='-4~-5 사이 아님').round(1).to_string())
    print('\n  SV 구간별 min_d2 중앙 / ≥-3(하락없음) 비율%')
    print(E.groupby(['era', 'svb'], observed=True).min_d2.agg(['size', 'median', lambda x: (x > NOISE).mean() * 100]).round(1).to_string())

    print('\n[Q2] 하락 없음(ON 틱 dPT2 최저 > -3) 이벤트의 조건')
    E['nodrop'] = E.min_d2 > NOISE
    for f in ['P2_overlap', 'P2_before', 'svchg_recent']:
        t = E.groupby(['era', f]).nodrop.agg(['size', 'mean'])
        t['mean'] = (t['mean'] * 100).round(1); print(f'  -- {f}\n', t.unstack().to_string())
    E['short'] = E.n_on == 1; E['early'] = E.start_tick <= 3
    for f in ['short', 'early']:
        t = E.groupby(['era', f]).nodrop.agg(['size', 'mean']); t['mean'] = (t['mean'] * 100).round(1)
        print(f'  -- {f}\n', t.unstack().to_string())

    print('\n[Q3] S1 OFF 틱 (Tick_Index>=1) 의 dPT1 <= -5 원인')
    J = T[(T.S1 == 0) & T.dPT1.notna() & (T.Tick_Index >= 1)]
    big = J[J.dPT1 <= -5]
    cat = np.select([big.Tick_Index <= 3, big.since_sv <= 1, big.P2 == 1, big.S1_prev == 1],
                    ['증압 구간(tick 1~3)', 'SV 변경 틱/다음 틱', '짝펌프 P2 사출 중', 'S1 꺼진 직후 1~2틱'], '설명 안 됨')
    for er in ['연휴 전', '연휴 후']:
        m = (big.era == er).values; n_off = (J.era == er).sum()
        vc = pd.Series(cat[m]).value_counts()
        print(f'  {er}: OFF 틱 {n_off:,} 중 큰하락 {m.sum():,} ({m.sum()/n_off*100:.2f}%)  ->', (vc / n_off * 100).round(2).to_dict())
    un = big[cat == '설명 안 됨']
    print('  설명 안 되는 하락 크기 분포:', un.dPT1.value_counts().sort_index().to_dict())
    hours = T.groupby('day').size().sum() * 0.523 / 3600
    for th in [-5, -6, -8, -10]:
        k = (un.dPT1 <= th).sum(); print(f'   ≤{th}: {k}건  (가동 1시간당 {k/hours:.2f}건, 하루 {k/T.day.nunique():.1f}건)')
    # 같은 조건에서 '평범한 노이즈 틱' 이 얼마나 많은가 (AE 입장의 정상 분포)
    q = J[(J.Tick_Index > 3) & (J.since_sv > 1) & (J.P2 == 0) & (J.S1_prev == 0)]
    print('  설명변수 다 0 인 OFF 틱의 dPT1 분위:', {p: q.dPT1.quantile(p) for p in [.001, .005, .01, .05, .5, .95, .99]}, ' n=', len(q))

    print('\n[EX] 2단 사출 샷 + OFF 직후 피처 예시 (9/10 07:44:46 샷)')
    print(example_rows('2026-09-10', '2026-09-10 07:44:50').to_string(index=False))
