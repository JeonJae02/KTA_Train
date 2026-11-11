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
print(u.Hz_In_Conv.describe())
print(u.groupby('unit').Hz_In_Conv.value_counts(normalize=True).groupby(level=0).head(5).round(3))
cols = ['stop_fwd_time','stop_rev_time','nozzle_rev_time','return_time','air_time','catch_cnt','drag_cnt','cycle_s']
for unit, g in u.groupby('unit'):
    top = g.Hz_In_Conv.value_counts()
    top = top[top > 3000].index.sort_values()
    print(f'\n{unit}: Hz 단계별 사이클 평균 (사이클 수 3000 이상인 Hz만)')
    t = g[g.Hz_In_Conv.isin(top)].groupby('Hz_In_Conv')[cols].mean()
    t['n'] = g[g.Hz_In_Conv.isin(top)].groupby('Hz_In_Conv').size()
    print(t.round(3).to_string())

print('\n기간', u.t.min(), u.t.max())
for unit, g in u.groupby('unit'):
    d = g.groupby('day').agg(share_low=('Hz_In_Conv', lambda x: (x < 400).mean()),
                             air=('air_time', 'mean'), ret=('return_time', 'mean'), catch=('catch_cnt', 'mean'),
                             n=('air_time', 'size'))
    d405 = g[g.Hz_In_Conv == 405].groupby('day')[['air_time', 'return_time', 'catch_cnt']].mean().add_suffix('_405')
    d = d.join(d405)
    d = d[d.n > 1000]
    print(f'\n{unit}: 일별 저속 비율 vs 하루 평균 (상관)')
    print('  저속비율 범위', d.share_low.min().round(3), '~', d.share_low.max().round(3))
    for col in ['air', 'ret', 'catch']:
        print(f'  {col}: corr(저속비율, 하루평균)={d.share_low.corr(d[col]):.2f}  하루평균 std={d[col].std():.4f}  405Hz만 std={d[col.replace("air","air_time").replace("ret","return_time").replace("catch","catch_cnt")+"_405"].std():.4f}')
