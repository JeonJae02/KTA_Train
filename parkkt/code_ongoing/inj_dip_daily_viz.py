"""일별(9/10~) 사출-압력 분석 그림. 데이터는 inj_dip_daily_lib.process_all()."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import numpy as np, pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from inj_dip_daily_lib import process_all, load_day, THRS
from inj_dip_signal_base import segs
from inj_dip_signal_viz import C  # 같은 색 체계

plt.rcParams.update({'font.family': 'Malgun Gothic', 'axes.unicode_minus': False,
                     'axes.spines.top': False, 'axes.spines.right': False,
                     'axes.edgecolor': '#8a8984', 'axes.labelcolor': '#52514e',
                     'xtick.color': '#52514e', 'ytick.color': '#52514e',
                     'axes.grid': True, 'grid.color': '#e6e5e0', 'grid.linewidth': 0.6,
                     'figure.facecolor': '#fcfcfb', 'axes.facecolor': '#fcfcfb'})
HOLIDAY = (pd.Timestamp('2026-09-24'), pd.Timestamp('2026-09-27'))
SVB_EDGES = [0, 2100, 2200, 2400, 2600, 9999]
SVB_LABELS = ['SV ≤2100', '2101~2200', '2201~2400', '2401~2600', '>2600']
# 순서형(magnitude) 이므로 한 색상 계열의 밝기 단계
SVB_COLORS = ['#0d366b', '#1f5ba8', '#2a78d6', '#6fa6e8', '#b3d0f5']


def era(day):
    return np.where(pd.to_datetime(day) < HOLIDAY[0], '연휴 전', '연휴 후')


def _day_axis(a, days):
    a.set_xticks(range(len(days)))
    a.set_xticklabels([f'{d:%m/%d}\n{"월화수목금토일"[d.dayofweek]}' for d in days], fontsize=8)
    k = np.searchsorted(days, HOLIDAY[0])
    a.axvline(k - 0.5, color='#8a8984', lw=1, ls='--')
    return k


# ======================= 노트북 A: 사출 창 밖 하락 =======================
def fig_outside_daily(days_df, window='W_S2', per='cycles'):
    """일별 창 밖 하락 건수. 진한 막대 = SV 변경 직후 제외, 테두리 = 제외 안 했을 때."""
    d = days_df; days = list(d.index)
    fig, axs = plt.subplots(len(THRS), 1, figsize=(15, 8.5), sharex=True, gridspec_kw={'hspace': 0.25})
    for a, g in zip(axs, THRS):
        raw = d[f'{window}_out_pct{g}_with_svchg']; ex = d[f'{window}_out_pct{g}']
        scale = (1000 / d.cycles).values if per == 'cycles' else np.ones(len(d))
        x = np.arange(len(days))
        a.bar(x, raw * scale, width=0.7, color='none', edgecolor='#b5b4ad', lw=1.2, label='SV 변경 직후 포함')
        a.bar(x, ex * scale, width=0.7, color=C['S2'] if window == 'W_S2' else C['S1'], label='SV 변경 직후 제외')
        for i, (r_, e_) in enumerate(zip(raw, ex)):
            a.text(i, e_ * scale[i], f'{e_}', ha='center', va='bottom', fontsize=8, color='#0b0b0b')
        a.set_ylabel(f'≥{g}% 하락\n(1000사이클당)', fontsize=9)
        k = _day_axis(a, days)
        if g == THRS[0]:
            a.legend(frameon=False, fontsize=9, loc='lower right', bbox_to_anchor=(1, 1.0), ncol=2)
            a.text(k - 0.4, a.get_ylim()[1] * 0.92, '추석 연휴 (9/24~27 비가동)', fontsize=8, color='#52514e')
    name = 'S2 On_HD1' if window == 'W_S2' else 'S1 HD1_P1'
    fig.suptitle(f'{name} 창 밖에서 생긴 압력 하락 — 일별 (막대 위 숫자 = 하루 건수)', x=0.07, ha='left', fontsize=13)
    return fig


def fig_outside_where(alarms, thr=6):
    a = alarms[(~alarms.sv_chg) & (alarms.d_pct >= thr)].copy()
    fig, axs = plt.subplots(1, 2, figsize=(15, 3.6), gridspec_kw={'width_ratios': [2.2, 1], 'wspace': 0.2})
    tk = a.tick.clip(upper=20)
    vc = tk.value_counts().sort_index()
    axs[0].bar(vc.index, vc.values, color=C['S2'], width=0.7)
    axs[0].set_xticks(range(1, 21)); axs[0].set_xticklabels([str(i) if i < 20 else '20+' for i in range(1, 21)])
    axs[0].set_xlabel('사이클 내 틱 위치'); axs[0].set_ylabel('건수 (20일 합)')
    axs[0].set_title(f'S2 창 밖 ≥{thr}% 하락은 사이클 어디서 나나', loc='left', fontsize=11)
    sv = pd.cut(a.sv, SVB_EDGES, labels=SVB_LABELS).value_counts().reindex(SVB_LABELS)
    axs[1].barh(range(len(sv)), sv.values, color=SVB_COLORS, height=0.6)
    axs[1].set_yticks(range(len(sv))); axs[1].set_yticklabels(SVB_LABELS); axs[1].invert_yaxis()
    axs[1].set_title('레시피(SV)별 건수', loc='left', fontsize=11); axs[1].set_xlabel('건수')
    return fig


def fig_around(day, center, before=2, after=2, thr=6, title=None, off_keep=4):
    """center 시각이 속한 사이클 앞뒤 before/after 사이클을 압력 + 신호 레인으로."""
    df, t = load_day(day)
    i0 = int(np.argmin(np.abs(pd.to_datetime(t.Time).values - np.datetime64(pd.Timestamp(center)))))
    on_cyc = np.sort(df[df.bu].cyc.unique())
    c = t.cyc.iloc[i0]
    j = np.searchsorted(on_cyc, c) if c in on_cyc else np.searchsorted(on_cyc, c) - 1
    sel_c = on_cyc[max(j - before, 0): j + after + 1]
    m = df.cyc.between(sel_c[0], sel_c[-1] + 1)
    sub_i = df.index[m & (df.bu | (df.groupby('cyc').cumcount() < off_keep))]
    sub = df.loc[sub_i]; ts = t.loc[sub_i]; x = np.arange(len(sub))
    fig, (a1, a2) = plt.subplots(2, 1, figsize=(15, 5.8), sharex=True, gridspec_kw={'height_ratios': [2, 1.6], 'hspace': 0.06})
    for s, e in segs(~sub.bu.values):
        for a in (a1, a2): a.axvspan(s - 0.5, e + 0.5, color='#efeeea', zorder=0, lw=0)
    for i in np.where(np.diff(sub.index.values) > 1)[0]:
        for a in (a1, a2): a.axvline(i + 0.5, color='#8a8984', lw=0.8, ls=':')
    for i in np.where(np.r_[False, (sub.g_s_SV_P1.values[1:] != sub.g_s_SV_P1.values[:-1]) & (sub.cyc.values[1:] == sub.cyc.values[:-1])])[0]:
        a1.axvline(i - 0.5, color='#eda100', lw=1.4)   # 사이클 중간 SV 변경 지점
    a1.plot(x, ts.PT.values, color='#3a3936', lw=1.6); a1.scatter(x, ts.PT.values, s=10, color='#3a3936')
    flag = (ts.ok & (ts.d_pct >= thr)).values
    kinds = [('S2 창 밖 · SV변경 아님 (이상 후보)', flag & ~ts.W_S2.values & ~ts.sv_chg.values, '#e34948'),
             ('S2 창 밖 · SV변경 직후', flag & ~ts.W_S2.values & ts.sv_chg.values, '#eda100'),
             ('S2 창 안 (사출로 설명됨)', flag & ts.W_S2.values, '#8a8984')]
    for lab, mk, col in kinds:
        a1.scatter(x[mk], ts.PT.values[mk], s=100, color=col, edgecolor='#fcfcfb', lw=1.5, zorder=3, label=lab)
    a1.legend(loc='lower left', bbox_to_anchor=(0, 1.02), ncol=3, frameon=False, fontsize=9,
              title=f'● 직전 2틱 평균 대비 ≥{thr}% 하락 틱', title_fontsize=9)
    a1.set_ylabel('P1 압력 (PT)')
    a1.set_title(title or f'{pd.Timestamp(center):%m/%d %H:%M:%S} 주변', loc='left', fontsize=12, pad=44)
    # SV(목표유량)는 이중축 대신 아래 레인에 변경 지점으로만 표시
    lanes = [('S1  HD1_P1', sub.Shot_Injection_HD1_P1.values != 0, C['S1']),
             ('S2  On_HD1', sub.Shot_Injection_On_HD1.values != 0, C['S2'])] + \
            [(f'FOAMING {k}', sub[f'HD1_FOAMING_SHOT{k}'].values != 0, C['F']) for k in range(1, 5)] + \
            [('BuildUp', sub.bu.values, '#52514e')]
    for i, (nm, mk, col) in enumerate(lanes):
        y = len(lanes) - i
        for s, e in segs(mk):
            a2.broken_barh([(s - 0.45, e - s + 0.9)], (y - 0.32, 0.64), color=col, lw=0)
            if nm != 'BuildUp':
                a2.broken_barh([(e + 0.55, 1.9)], (y - 0.32, 0.64), color=col, alpha=0.28, lw=0)
    # SV 변경 지점
    svc = np.where(np.r_[False, (sub.g_s_SV_P1.values[1:] != sub.g_s_SV_P1.values[:-1]) & (sub.cyc.values[1:] == sub.cyc.values[:-1])])[0]
    for i in svc:
        a2.annotate(f'SV {sub.g_s_SV_P1.values[i-1]:.0f}→{sub.g_s_SV_P1.values[i]:.0f}', (i, -0.35), xytext=(i, -0.35), fontsize=7.5, color='#9a6a00', ha='center')
        a2.axvline(i - 0.5, color='#eda100', lw=1.4)
    starts = np.where(np.r_[True, sub.cyc.values[1:] != sub.cyc.values[:-1]] & sub.bu.values)[0]
    for i in starts:
        a2.text(i, 0.1, f'SV {sub.g_s_SV_P1.values[i]:.0f}', fontsize=7.5, color='#52514e')
    a2.set_yticks(range(1, len(lanes) + 1)); a2.set_yticklabels([l[0] for l in lanes][::-1], fontsize=9)
    a2.set_ylim(-0.6, len(lanes) + 0.6); a2.grid(False)
    a2.set_xticks(starts); a2.set_xticklabels([pd.Timestamp(sub.Time.iloc[i]).strftime('%H:%M:%S') for i in starts], fontsize=8)
    a2.set_xlabel('진한 막대 = 신호 ON, 연한 = 뒤 2틱 응답창, 회색 = BuildUp OFF(4틱만 표시, 점선 = 생략), 노란 선 = 사이클 중간 SV 변경 · x축 = 사이클 시작 시각')
    return fig


# ======================= 노트북 B: 사출했는데 안 떨어짐 =======================
def svb(sv):
    return pd.cut(sv, SVB_EDGES, labels=SVB_LABELS)


def fig_s1_trend(E):
    g = E.assign(svb=svb(E.sv)).groupby(['day', 'svb'], observed=True).drop_pct
    med = g.median().unstack(); q25 = g.quantile(.25).unstack()
    days = list(med.index); x = np.arange(len(days))
    fig, a = plt.subplots(figsize=(15, 4.8))
    for lab, col in zip(SVB_LABELS, SVB_COLORS):
        if lab not in med: continue
        a.plot(x, med[lab].values, color=col, lw=2, marker='o', ms=5, label=lab)
    a.axhline(5, color='#0b0b0b', lw=1, ls='--'); a.text(len(days) - 0.6, 5.2, '5%', fontsize=8)
    a.axhline(3, color='#0b0b0b', lw=0.8, ls=':'); a.text(len(days) - 0.6, 3.2, '3%', fontsize=8)
    k = _day_axis(a, days)
    a.text(k - 0.4, a.get_ylim()[1] * 0.95, '← 연휴 전 | 연휴 후 →', fontsize=9, color='#52514e', ha='center')
    a.set_ylabel('S1 이벤트 하락 % (일별 중앙값)')
    a.legend(frameon=False, fontsize=9, ncol=5, loc='upper left', bbox_to_anchor=(0, 1.12))
    a.set_title('S1 사출 때 압력이 떨어지는 폭 — 레시피(SV)별 일별 추이', loc='left', fontsize=12, pad=30)
    return fig


def fig_s1_dist(E):
    E = E.assign(svb=svb(E.sv), era=era(E.day))
    fig, axs = plt.subplots(1, 2, figsize=(15, 4.6), sharey=True, gridspec_kw={'wspace': 0.05})
    for a, er in zip(axs, ['연휴 전', '연휴 후']):
        data = [E[(E.era == er) & (E.svb == l)].drop_pct.clip(-5, 30).values for l in SVB_LABELS]
        bp = a.boxplot(data, widths=0.55, patch_artist=True, showfliers=False, medianprops=dict(color='#0b0b0b', lw=1.6))
        for p, col in zip(bp['boxes'], SVB_COLORS):
            p.set_facecolor(col); p.set_edgecolor('#52514e')
        for i, dd in enumerate(data):
            a.text(i + 1, -4.5, f'n={len(dd)}\n<5%: {(dd < 5).mean()*100:.0f}%', ha='center', fontsize=8, color='#52514e')
        a.axhline(5, color='#0b0b0b', lw=1, ls='--'); a.axhline(3, color='#0b0b0b', lw=0.8, ls=':')
        a.set_xticks(range(1, 6)); a.set_xticklabels(SVB_LABELS, fontsize=9)
        a.set_title(f'{er} ({"9/10~9/23" if er == "연휴 전" else "9/28~10/06"})', loc='left', fontsize=11)
        a.set_ylim(-7, 26)
    axs[0].set_ylabel('S1 이벤트 하락 %  (상자 = P25~P75, 수염 = 1.5 IQR)')
    fig.suptitle('같은 레시피라도 연휴 후 사출 하락이 작아졌다 — 점선 = 3%, 파선 = 5%', x=0.07, ha='left', fontsize=12)
    return fig


def fig_s1_factors(E, thr=5):
    """'안 떨어짐'(하락 < thr%) 비율을 조건별로 — 연휴 전/후 나눠서."""
    E = E.assign(era=era(E.day), nodrop=E.drop_pct < thr)
    facs = [('nth_s1', '사이클 내 몇 번째 S1', lambda s: s.map({1: '첫 번째', 2: '두 번째(2단 사출)'}), ['첫 번째', '두 번째(2단 사출)']),
            ('s1_len', 'S1 신호 길이(틱)', lambda s: s.clip(upper=4).astype(int).astype(str).replace({'4': '4+'}), ['1', '2', '3', '4+']),
            ('start_tick', 'S1 시작 틱 위치', lambda s: pd.cut(s, [-1, 3, 5, 7, 99], labels=['≤3', '4~5', '6~7', '8+']).astype(str), ['≤3', '4~5', '6~7', '8+']),
            ('base_vs_cyc', '기준 2틱이 사이클 중앙값 대비', lambda s: pd.cut(s, [-99, -1, 1, 99], labels=['1%↓ 낮음', '±1%', '1%↑ 높음']).astype(str), ['1%↓ 낮음', '±1%', '1%↑ 높음'])]
    fig, axs = plt.subplots(1, len(facs), figsize=(16, 3.8), sharey=True, gridspec_kw={'wspace': 0.12})
    for a, (col, lab, f, order) in zip(axs, facs):
        g = E.assign(k=f(E[col])).groupby(['k', 'era']).nodrop.agg(['mean', 'size']).reset_index()
        g = g[g['size'] >= 30]
        cats = [c for c in order if c in set(g.k)]
        for j, (er, colr) in enumerate([('연휴 전', '#6fa6e8'), ('연휴 후', '#1f5ba8')]):
            gg = g[g.era == er].set_index('k').reindex(cats)
            xs = np.arange(len(cats)) + (j - 0.5) * 0.36
            a.bar(xs, gg['mean'] * 100, width=0.34, color=colr, edgecolor='#fcfcfb', lw=2, label=er)
            for xx, m_, n_ in zip(xs, gg['mean'], gg['size']):
                if pd.notna(m_): a.text(xx, m_ * 100 + 1, f'{m_*100:.0f}', ha='center', fontsize=8)
        a.set_xticks(range(len(cats))); a.set_xticklabels(cats, fontsize=8.5); a.set_title(lab, loc='left', fontsize=10)
    axs[0].set_ylabel(f'하락 <{thr}% 인 S1 비율 %'); axs[0].legend(frameon=False, fontsize=9)
    axs[0].set_ylim(0, 108)
    fig.suptitle(f'어떤 S1 에서 압력이 안 떨어지나 (하락 <{thr}%)', x=0.07, ha='left', fontsize=12, y=1.03)
    return fig


def s1_rule_table(E):
    E = E.assign(svb=svb(E.sv), era=era(E.day))
    t = E.groupby(['era', 'svb'], observed=True).agg(
        S1이벤트=('drop_pct', 'size'), 하루평균=('day', lambda s: len(s) / s.nunique()),
        하락중앙=('drop_pct', 'median'), 하락P10=('drop_pct', lambda s: s.quantile(.1)),
        **{'<3% 비율': ('drop_pct', lambda s: (s < 3).mean() * 100), '<5% 비율': ('drop_pct', lambda s: (s < 5).mean() * 100)})
    t['<3% 하루 알람'] = t['하루평균'] * t['<3% 비율'] / 100
    return t.round(1)
