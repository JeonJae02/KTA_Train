"""피처 설계 질문 3가지 확인 (P1, inj_feat_daily 캐시 = 생산 틱).
1) '큰 하락' 기준(5눈금)의 근거와 기준을 바꿔도 신호 순위가 같은지
2) 샷 중간 SV 변경 후 압력(PT)·유량(FT)이 몇 틱 뒤에 반응하나 -> Phase_Transition 을 어떻게 넣을지
3) BuildUp ON 직후 증압 / 안정 / OFF 직후 감쇠가 각각 몇 틱인가 -> 상태 피처를 어떻게 넣을지
"""
import os, sys, glob
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import numpy as np, pandas as pd
from inj_feat_lib import load_all, flag_metrics, to_prod_ticks, CACHE

pd.set_option('display.width', 250)
T = load_all()

# ---------- 1) 큰 하락 기준
J = T[T.dPT1.notna() & (T.Tick_Index >= 3)].copy()
g = J.groupby('shot')
near_s2 = sum(g.S2.shift(k).fillna(0) for k in range(-2, 3)) > 0
quiet = J[~near_s2 & ~J.SV_chg_recent]
print('[1] 사출(S2)·SV변경 없는 안정 틱의 dPT1 분포  n=', len(quiet))
print('   ', {f'P{q}': quiet.dPT1.quantile(q / 100) for q in [1, 2.5, 5, 50, 95, 97.5, 99]})
print('   하락 쪽(음수) 분위: 하위 5% =', quiet.dPT1.quantile(.05), ' 하위 1% =', quiet.dPT1.quantile(.01))
rows = {}
for big in [3, 4, 5, 6, 8]:
    for s in ['S1', 'S2', 'S3']:
        for er in ['연휴 전', '연휴 후']:
            m = flag_metrics(T[T.era == er], s, 'dPT1', big=big)
            rows[(big, er, s)] = m['배율(ON/OFF)']
r = pd.Series(rows).unstack(); print('   기준별 ON/OFF 배율:\n', r.round(2).to_string())

# ---------- 2) 샷 중간 SV 변경 응답
T2 = T.reset_index(drop=True)
sv = T2.SV.values; shot = T2.shot.values; ti = T2.Tick_Index.values
chg = np.where((np.r_[False, sv[1:] != sv[:-1]]) & np.r_[False, shot[1:] == shot[:-1]] & (ti >= 2))[0]
print(f'\n[2] 샷 중간 SV 변경 {len(chg)}회 (샷 {T2.shot.nunique()}개 중 {len(np.unique(shot[chg]))}개 샷)')
for name, sel in [('SV 하향', sv[chg] < sv[chg - 1]), ('SV 상향', sv[chg] > sv[chg - 1])]:
    c = chg[sel]; out = []
    for o in range(-2, 7):
        j = c + o; ok = (j < len(T2)) & (shot[np.clip(j, 0, len(T2) - 1)] == shot[c]); j = j[ok]
        dsv = (sv[c[ok]] - sv[c[ok] - 1])
        out.append({'offset': o, 'n': len(j), 'dPT1 평균': T2.dPT1.values[j].mean(),
                    'dPT1/ΔSV(%)': np.nanmean(T2.dPT1.values[j] / dsv * 100),
                    'FT-SV 오차%': np.nanmean((T2.FT.values[j] - sv[j]) / sv[j] * 100),
                    '사출S1 ON%': T2.S1.values[j].mean() * 100})
    print(f'  {name} ({sel.sum()}회, ΔSV 중앙 {np.median(sv[c] - sv[c - 1]):.0f})')
    print(pd.DataFrame(out).set_index('offset').round(2).T.to_string())

# ---------- 3) 증압 / 안정 / 감쇠  (BuildUp OFF 틱이 필요하므로 원본에서 몇 날만)
print('\n[3] BuildUp ON 직후 / OFF 직후 틱별 압력·유량')
on_rows, off_rows = [], []
for p in sorted(glob.glob(f'{CACHE}/p1_*.pkl'))[::4]:
    raw = pd.read_pickle(p)
    if (raw.Pump_BuildUp_P1 != 0).mean() < 0.05: continue
    df = to_prod_ticks(raw).reset_index()
    bu = (df.Pump_BuildUp_P1 != 0).values; pt = df.Scale_Out___PT_P1.values; ft = df.Scale_Out___FT_P1.values
    sv_ = df.g_s_SV_P1.values; t = df.iloc[:, 0].values
    seg = np.cumsum(np.r_[True, bu[1:] != bu[:-1]])
    for sid in np.unique(seg):
        ix = np.where(seg == sid)[0]
        if bu[ix[0]]:
            ref = np.median(pt[ix[3:]]) if len(ix) > 5 else np.nan
            for k, i in enumerate(ix[:8]):
                on_rows.append((k, pt[i] / ref * 100, (ft[i] - sv_[i]) / sv_[i] * 100 if sv_[i] else np.nan))
        else:
            if ix[0] == 0: continue
            prev = np.where(seg == sid - 1)[0]; ref = np.median(pt[prev[3:]]) if len(prev) > 5 else np.nan
            gap = (t[ix[-1]] - t[ix[0]]) / np.timedelta64(1, 's')
            for k, i in enumerate(ix[:10]):
                off_rows.append((k, pt[i], pt[i] / ref * 100, ft[i], len(ix), gap))
on = pd.DataFrame(on_rows, columns=['tick', 'PT/안정PT%', 'FT오차%'])
print(on.groupby('tick').agg(PT_med=('PT/안정PT%', 'median'), PT_p10=('PT/안정PT%', lambda x: x.quantile(.1)),
                             FT오차_med=('FT오차%', 'median'), FT오차_abs_p90=('FT오차%', lambda x: x.abs().quantile(.9))).round(1).T.to_string())
off = pd.DataFrame(off_rows, columns=['tick', 'PT', 'PT/직전안정%', 'FT', 'off_len', 'off_sec'])
print(off.groupby('tick').agg(PT_med=('PT', 'median'), PT_pct=('PT/직전안정%', 'median'), FT_med=('FT', 'median'), n=('PT', 'size')).round(1).T.to_string())
o1 = off.drop_duplicates(['off_len', 'off_sec'])
print('   OFF 구간 길이(틱) 분포:', o1.off_len.describe(percentiles=[.1, .5, .9]).round(0).to_dict())
print('   OFF 구간 길이(초) 중앙:', o1.off_sec.median(), '  60초 이상 비율', (o1.off_sec > 60).mean().round(3))
