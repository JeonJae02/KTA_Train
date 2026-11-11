import glob, pandas as pd, numpy as np
c = pd.read_pickle('cache/conv_feat/cyl_cycles.pkl')
c = c[c.run & ~c.interrupted & (c.cycle_s < 30)]
out = []
for unit in ['md_fwd', 'md_rev', 'airbag']:
    hz = []
    for f in sorted(glob.glob(f'cache/conv_raw/{unit}/*.pkl.xz')):
        d = pd.read_pickle(f)
        if 'Hz_In_Conv' not in d: continue
        x = d[['Hz_In_Conv']].copy()
        if 'time' in d: x.index = pd.to_datetime(d['time'])
        hz.append(x.dropna())
    hz = pd.concat(hz).sort_index()
    hz = hz[~hz.index.duplicated()]
    u = c[c.unit == unit].sort_values('t')
    hz.index = hz.index.astype('datetime64[ns]'); u = pd.merge_asof(u, hz.rename_axis('t').reset_index(), on='t', direction='backward')
    out.append(u)
u = pd.concat(out)
u['v'] = 0.2744 * u.Hz_In_Conv / 10                    # 카운트/초
u['air_cnt'] = u.air_time * u.v                         # 카운트로 바꾼 에어 주입
for unit, g in u.groupby('unit'):
    nf = 'nozzle_fwd_start' if unit != 'airbag' else 'nozzle_fwd_start_1'
    g = g[g.Hz_In_Conv.isin([324, 365, 385, 405])]
    t = g.groupby('Hz_In_Conv').agg(air_time=('air_time', 'mean'), air_cnt=('air_cnt', 'mean'),
                                    drag_cnt=('drag_cnt', 'mean'), nf=(nf, 'mean'), v=('v', 'first'))
    t['pred'] = t.drag_cnt / t.v - t.nf - 0.5           # 끌림 시간 − 노즐 전진 출발 − 0.5초 지연
    t['resid'] = t.air_time - t.pred
    print(unit); print(t.round(3).to_string())
