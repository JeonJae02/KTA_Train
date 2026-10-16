"""사출 플래그 + 압력 차분 피처 검증 그림 (inj_feat_lib 기반)."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import numpy as np, pandas as pd
import matplotlib.pyplot as plt
from inj_feat_lib import *

plt.rcParams.update({'font.family': 'Malgun Gothic', 'axes.unicode_minus': False,
                     'axes.spines.top': False, 'axes.spines.right': False,
                     'axes.edgecolor': '#8a8984', 'axes.labelcolor': '#52514e',
                     'xtick.color': '#52514e', 'ytick.color': '#52514e',
                     'axes.grid': True, 'grid.color': '#e6e5e0', 'grid.linewidth': 0.6,
                     'figure.facecolor': '#fcfcfb', 'axes.facecolor': '#fcfcfb'})
COL = {'S1': '#2a78d6', 'S2': '#eb6834', 'S3': '#1baf7a', 'S4': '#e87ba4', 'OFF': '#8a8984'}
SVB_EDGES = [0, 2100, 2200, 2400, 2600, 9999]
SVB_LABELS = ['SV ≤2100', '2101~2200', '2201~2400', '2401~2600', '>2600']


def _days_axis(a, days):
    a.set_xticks(range(len(days)))
    a.set_xticklabels([f'{d:%m/%d}' for d in days], fontsize=8, rotation=0)
    k = int(np.searchsorted(np.array(days, dtype='datetime64[ns]'), np.datetime64(HOLIDAY)))
    a.axvline(k - 0.5, color='#8a8984', lw=1, ls='--')
    return k


def fig_profile(T, sigs=('S1', 'S2', 'S3'), offsets=range(-3, 7)):
    fig, axs = plt.subplots(3, len(sigs), figsize=(16, 8.4), sharex=True, sharey='row',
                            gridspec_kw={'hspace': 0.12, 'wspace': 0.06, 'height_ratios': [1.2, 1.2, 0.8]})
    for j, s in enumerate(sigs):
        for er, ls in [('연휴 전', '-'), ('연휴 후', '--')]:
            p, n = aligned_profile(T[T.era == er], s, offsets)
            x = p.index.values
            axs[0, j].plot(x, p['mean'], color=COL[s], lw=2, ls=ls, marker='o', ms=4, label=f'{er} (엣지 {n:,})')
            axs[1, j].plot(x, p['big%'], color=COL[s], lw=2, ls=ls, marker='o', ms=4)
            if er == '연휴 후':
                axs[2, j].bar(x, p['on%'], color=COL[s], width=0.7)
        axs[0, j].axhline(0, color='#52514e', lw=0.8)
        axs[0, j].set_title(SIG_NAME[s], loc='left', fontsize=11)
        axs[0, j].legend(frameon=False, fontsize=8, loc='upper right')
        for a in axs[:, j]:
            a.axvspan(-0.5, 0.5, color='#efeeea', zorder=0, lw=0)
        axs[2, j].set_xlabel('신호 상승 엣지 기준 틱 오프셋 (0 = 신호가 처음 켜진 틱)')
        axs[2, j].set_xticks(list(offsets))
    axs[0, 0].set_ylabel('dPT1 평균 (눈금)\nPT(t) - PT(t-1)')
    axs[1, 0].set_ylabel(f'큰 하락 비율 %\n(dPT1 ≤ -{BIG})')
    axs[2, 0].set_ylabel('그 틱에 신호가\n켜져 있는 비율 %')
    fig.suptitle('신호가 켜지는 순간을 0 으로 맞춰 놓고 본 압력 차분 — 하락이 신호 ON 틱과 겹치는가', x=0.07, ha='left', fontsize=13)
    return fig


def fig_onoff_hist(T, sigs=('S1', 'S2', 'S3'), col='dPT1'):
    J = judged(T, col)
    fig, axs = plt.subplots(2, len(sigs), figsize=(16, 6.4), sharex=True, sharey=True, gridspec_kw={'hspace': 0.3, 'wspace': 0.06})
    bins = np.arange(-20.5, 21, 1)
    for i, er in enumerate(['연휴 전', '연휴 후']):
        Je = J[J.era == er]
        for j, s in enumerate(sigs):
            a = axs[i, j]; on = Je[s] == 1
            a.hist(Je.loc[~on, col].clip(-20, 20), bins=bins, density=True, color=COL['OFF'], alpha=0.45, label='신호 OFF')
            a.hist(Je.loc[on, col].clip(-20, 20), bins=bins, density=True, histtype='step', color=COL[s], lw=2.2, label='신호 ON')
            a.axvline(0, color='#52514e', lw=0.8); a.axvline(-BIG, color='#0b0b0b', lw=1, ls='--')
            a.text(0.03, 0.95, f"ON  평균 {Je.loc[on, col].mean():+.1f}, ≤-{BIG}: {(Je.loc[on, col] <= -BIG).mean()*100:.0f}%\n"
                               f"OFF 평균 {Je.loc[~on, col].mean():+.1f}, ≤-{BIG}: {(Je.loc[~on, col] <= -BIG).mean()*100:.1f}%\n"
                               f"ON 틱 비율 {on.mean()*100:.0f}%",
                   transform=a.transAxes, va='top', fontsize=8.5, color='#0b0b0b')
            if i == 0: a.set_title(SIG_NAME[s], loc='left', fontsize=11)
            if j == 0: a.set_ylabel(f'{er}\n밀도')
            if i == 1: a.set_xlabel(f'{col} (눈금)')
    axs[0, -1].legend(frameon=False, fontsize=9, loc='upper right')
    fig.suptitle(f'신호 ON 틱 vs OFF 틱의 압력 차분 분포 (Tick_Index ≥ 3, 파선 = -{BIG})', x=0.07, ha='left', fontsize=13)
    return fig


def off_drop_breakdown(T, s, col='dPT1'):
    """신호 OFF 인데 큰 하락(≤-BIG)이 난 틱이 무엇이었나."""
    J = judged(T, col)
    J = J.assign(prev_on=J.groupby('shot')[s].shift(1).fillna(0).astype(int) |
                 J.groupby('shot')[s].shift(2).fillna(0).astype(int))
    x = J[(J[s] == 0) & (J[col] <= -BIG)]
    cat = np.select([x.SV_chg_recent, x.prev_on == 1, (x.S2 == 1) if s == 'S1' else np.zeros(len(x), bool)],
                    ['SV 변경 직후 (2단 사출 등)', '신호 꺼진 직후 1~2틱 (하락 꼬리)', 'S2 ON (짝펌프 사출 등)'], '그 외 (설명 안 됨)')
    return pd.Series(cat).value_counts(), len(x), len(J[J[s] == 0])


def fig_off_breakdown(T, sigs=('S1', 'S2', 'S3')):
    order = ['SV 변경 직후 (2단 사출 등)', '신호 꺼진 직후 1~2틱 (하락 꼬리)', 'S2 ON (짝펌프 사출 등)', '그 외 (설명 안 됨)']
    colors = ['#eda100', '#b5b4ad', COL['S2'], '#e34948']
    fig, a = plt.subplots(figsize=(15, 2.8))
    for y, s in enumerate(sigs):
        vc, n, n_off = off_drop_breakdown(T, s)
        left = 0
        for k, c in zip(order, colors):
            w = vc.get(k, 0) / n_off * 100
            a.barh(y, w, left=left, color=c, edgecolor='#fcfcfb', lw=2, height=0.6, label=k if y == 0 else None)
            if w > 0.12: a.text(left + w / 2, y, f'{w:.1f}', ha='center', va='center', fontsize=8.5, color='#0b0b0b')
            left += w
        a.text(left + 0.05, y, f'  합 {left:.1f}%  ({n:,}틱)', va='center', fontsize=9, color='#52514e')
    a.set_yticks(range(len(sigs))); a.set_yticklabels([SIG_NAME[s] for s in sigs]); a.invert_yaxis()
    a.set_xlabel(f'신호 OFF 틱 중 큰 하락(dPT1 ≤ -{BIG}) 비율 %  — 원인별'); a.grid(False)
    a.legend(frameon=False, fontsize=9, ncol=4, loc='lower left', bbox_to_anchor=(0, 1.0))
    return fig


def daily_metrics(T, s='S1', col='dPT1'):
    rows = []
    for d, g in T.groupby('day'):
        m = flag_metrics(g, s, col); m['day'] = d; rows.append(m)
    return pd.DataFrame(rows).set_index('day')


def fig_daily(T, sigs=('S1', 'S2', 'S3')):
    fig, axs = plt.subplots(2, 1, figsize=(15, 6.4), sharex=True, gridspec_kw={'hspace': 0.12})
    for s in sigs:
        dm = daily_metrics(T, s); days = list(dm.index); x = np.arange(len(days))
        axs[0].plot(x, dm[f'ON 큰하락%(≤-{BIG})'], color=COL[s], lw=2, marker='o', ms=4, label=f'{s} ON')
        axs[0].plot(x, dm['OFF 큰하락%'], color=COL[s], lw=1.4, ls=':', marker='o', ms=3, label=f'{s} OFF')
        axs[1].plot(x, dm['MI(bits)'], color=COL[s], lw=2, marker='o', ms=4, label=SIG_NAME[s])
    axs[0].set_ylabel(f'큰 하락(≤-{BIG}) 비율 %'); axs[0].legend(frameon=False, fontsize=8.5, ncol=6, loc='upper right')
    axs[1].set_ylabel('상호정보량 (bits)\n플래그 ↔ dPT1'); axs[1].legend(frameon=False, fontsize=9, ncol=3, loc='upper right')
    for a in axs: k = _days_axis(a, days)
    axs[0].text(k - 0.4, axs[0].get_ylim()[1] * 0.92, '추석 연휴', fontsize=8, color='#52514e')
    fig.suptitle('날마다 같은 관계가 유지되나 (실선 = 신호 ON 틱, 점선 = OFF 틱)', x=0.07, ha='left', fontsize=13)
    return fig


def sv_table(T, s='S1', col='dPT1'):
    J = judged(T, col).assign(svb=lambda d: pd.cut(d.SV, SVB_EDGES, labels=SVB_LABELS))
    g = J.groupby(['era', 'svb', J[s].map({1: 'ON', 0: 'OFF'})], observed=True)[col]
    t = pd.DataFrame({'평균': g.mean(), f'≤-{BIG} %': g.apply(lambda x: (x <= -BIG).mean() * 100), 'n': g.size()}).unstack()
    t[('배율', 'ON/OFF')] = t[(f'≤-{BIG} %', 'ON')] / t[(f'≤-{BIG} %', 'OFF')]
    return t.round(2)
