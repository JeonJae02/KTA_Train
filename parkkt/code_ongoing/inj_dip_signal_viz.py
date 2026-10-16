"""사출신호(S1/S2/FOAMING)별 P1 압력하락 시각화 — inj_dip_signal_compare.py 와 같은 정의."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import numpy as np, pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from matplotlib.lines import Line2D
import inj_dip_signal_base as A
from inj_dip_signal_base import load, segs, auc

plt.rcParams.update({'font.family': 'Malgun Gothic', 'axes.unicode_minus': False,
                     'axes.spines.top': False, 'axes.spines.right': False,
                     'axes.edgecolor': '#8a8984', 'axes.labelcolor': '#52514e',
                     'xtick.color': '#52514e', 'ytick.color': '#52514e',
                     'axes.grid': True, 'grid.color': '#e6e5e0', 'grid.linewidth': 0.6,
                     'figure.facecolor': '#fcfcfb', 'axes.facecolor': '#fcfcfb'})
A.MIN_TICK = 1; PRE = 2; POST = 2
PT = 'Scale_Out___PT_P1'
C = {'S1': '#2a78d6', 'S2': '#eb6834', 'F': '#1baf7a', 'none': '#8a8984', 'null': '#b5b4ad'}
CAT_LABEL = {'S1': 'S1 창 안 (P1 사출)', 'S2': 'S1 밖 · S2 창 안 (짝펌프 사출)',
             'F': 'S2 밖 · FOAMING 창만', 'none': '어떤 창에도 없음'}
DAYS = {'fri': '10/02(금) 07:00~11:30', 'today': '10/06(화) 07:00~18:38'}


def prepare(name):
    """틱 단위 하락(d_t), 안정틱(ok), 비교군 틱(quiet), 신호별 응답창(W_*), 이벤트 표를 만든다."""
    df = load(name)
    df['Time'] = pd.read_pickle(f'parkkt/cache/inj_dip_p1_{name}.pkl').index.values
    v = df[PT].values.astype(float); bu = df.bu.values; cyc = df.cyc.values; tick = df.tick.values; n = len(v)
    S1 = df.Shot_Injection_HD1_P1.values != 0; S2 = df.Shot_Injection_On_HD1.values != 0
    F = {k: df[f'HD1_FOAMING_SHOT{k}'].values != 0 for k in range(1, 5)}
    base = np.full(n, np.nan); base[2:] = (v[:-2] + v[1:-1]) / 2
    df['d_t'] = (base - v) / base * 100
    ok = np.zeros(n, bool)
    for t in range(2, n):
        ok[t] = bu[t-2:t+1].all() and cyc[t-2] == cyc[t] and tick[t-2] >= A.MIN_TICK
    df['ok'] = ok
    cs = np.r_[0, np.cumsum(S2)]
    df['quiet'] = [ok[t] and cs[min(t+POST+1, n)] - cs[max(t-PRE-2, 0)] == 0 for t in range(n)]

    def resp(masks):
        r = np.zeros(n, bool)
        for m in masks:
            for s, e in segs(m):
                r[s:min(e+POST, n-1)+1] = True
        return r
    df['W_S1'] = resp([S1]); df['W_S2'] = resp([S2]); df['W_F'] = resp(list(F.values()))
    sigs = {'S1 HD1_P1': [S1], 'S2 On_HD1': [S2], 'S3 FOAMING1~4': list(F.values()),
            'S4 On_HD1∧SHOTk': [S2 & F[k] for k in F]}
    for k in F: sigs[f'S3 SHOT{k}'] = [F[k]]
    ev = {}
    for nm, ms in sigs.items():
        rows = []
        for m in ms:
            for s, e in segs(m):
                r = A.event_drop(v, bu, cyc, tick, s, e)
                if r: rows.append((s, e) + r)
        ev[nm] = pd.DataFrame(rows, columns=['s', 'e', 'base', 'drop', 'L'])
    # 응답창이 BuildUp OFF 경계를 넘는 이벤트(모든 신호): 잘랐을 때 vs 안 잘랐을 때 하락
    cross = {}
    for nm, ms in sigs.items():
        rows = []
        for m in ms:
            for s, e in segs(m):
                if s < PRE or not bu[s-PRE:s].all() or tick[s-PRE] < A.MIN_TICK: continue
                r = np.arange(s, min(e+POST, n-1)+1)
                if (~bu[r]).any():
                    b = v[s-PRE:s].mean(); rin = r[bu[r] & (cyc[r] == cyc[s])]
                    rows.append(((b - v[rin].min())/b*100 if len(rin) else np.nan, (b - v[r].min())/b*100))
        cross[nm] = np.array(rows).reshape(-1, 2)
    df.attrs['cross'] = cross
    return df, ev


def null_events(dfs):
    """비교군 이벤트: 창 [s-2, s+L-1+2] 전체에 S2 가 없는 위치에 같은 방식으로 잰 하락."""
    out = {}
    for L in range(1, 13):
        x = []
        for df in dfs:
            v = df[PT].values.astype(float); bu = df.bu.values; cyc = df.cyc.values; tick = df.tick.values
            S2 = df.Shot_Injection_On_HD1.values != 0; cs = np.r_[0, np.cumsum(S2)]; n = len(v)
            for s in range(PRE, n-L-POST):
                if cs[s+L+POST] - cs[s-PRE] > 0: continue
                r = A.event_drop(v, bu, cyc, tick, s, s+L-1)
                if r and r[2] == L+POST: x.append(r[1])
        out[L+POST] = np.array(x)
    return out


def categorize(df, thr):
    """유의 하락 틱을 S1 -> S2 -> FOAMING -> 없음 우선순위로 귀속."""
    sig = df.ok & (df.d_t >= thr)
    cat = np.where(df.W_S1, 'S1', np.where(df.W_S2, 'S2', np.where(df.W_F, 'F', 'none')))
    return pd.Series(np.where(sig, cat, ''), index=df.index)


def fig_timeline(df, thr, thr_big, title, n_cyc=6, pick='max'):
    """연속 n_cyc 사이클을 골라 압력 + 신호 레인을 겹쳐 보여준다.
    pick='max': 강한 하락이 가장 많이 몰린 구간, pick=정수: 그 사이클 번째부터."""
    cat = categorize(df, thr)
    big = (df.ok & (df.d_t >= thr_big)).groupby(df.cyc).sum()
    on_cycles = df[df.bu].cyc.unique()
    if pick == 'max':
        best = max(range(len(on_cycles)-n_cyc), key=lambda i: big.reindex(on_cycles[i:i+n_cyc]).sum())
    else:
        best = int(pick)
    c0, c1 = on_cycles[best], on_cycles[best+n_cyc-1]
    # BuildUp OFF 대기구간은 앞 OFF_KEEP 틱만 남겨 압축한다(펌프가 꺼져 압력이 떨어지는 모습만 보이게)
    OFF_KEEP = 4
    sel = df[(df.cyc >= c0) & (df.cyc <= c1 + 1)]
    sel = sel[sel.bu | (sel.groupby('cyc').cumcount() < OFF_KEEP)]
    sub = sel
    x = np.arange(len(sub))
    fig, (a1, a2) = plt.subplots(2, 1, figsize=(15, 6.4), sharex=True,
                                 gridspec_kw={'height_ratios': [2.2, 1.6], 'hspace': 0.06})
    for s, e in segs(~sub.bu.values):
        for a in (a1, a2): a.axvspan(s-0.5, e+0.5, color='#efeeea', zorder=0, lw=0)
    a1.plot(x, sub[PT].values, color='#3a3936', lw=1.6, zorder=2)
    a1.scatter(x, sub[PT].values, s=10, color='#3a3936', zorder=2)
    cs = cat.loc[sub.index].values
    for k in ['S1', 'S2', 'F', 'none']:
        m = cs == k
        a1.scatter(x[m], sub[PT].values[m], s=90, color=C[k], edgecolor='#fcfcfb', lw=1.5, zorder=3, label=CAT_LABEL[k])
    a1.set_ylabel('P1 압력 (PT)')
    a1.legend(loc='lower left', bbox_to_anchor=(0, 1.06), ncol=4, frameon=False, fontsize=9,
              title=f'● 유의 하락 틱 (직전 2틱 평균 대비 ≥{thr:.1f}%) 이 어느 창 안이었나', title_fontsize=9)
    a1.set_title(title, loc='left', fontsize=12, color='#0b0b0b', pad=48)
    lanes = [('S1  HD1_P1', sub.Shot_Injection_HD1_P1.values != 0, C['S1']),
             ('S2  On_HD1', sub.Shot_Injection_On_HD1.values != 0, C['S2'])] + \
            [(f'FOAMING {k}', sub[f'HD1_FOAMING_SHOT{k}'].values != 0, C['F']) for k in range(1, 5)] + \
            [('BuildUp', sub.bu.values, '#52514e')]
    for i, (nm, m, col) in enumerate(lanes):
        y = len(lanes) - 1 - i
        for s, e in segs(m):
            a2.broken_barh([(s-0.45, e-s+0.9)], (y-0.32, 0.64), color=col, lw=0)
            if nm != 'BuildUp':
                a2.broken_barh([(e+0.55, POST-0.1)], (y-0.32, 0.64), color=col, alpha=0.28, lw=0)
    a2.set_yticks(range(len(lanes))); a2.set_yticklabels([l[0] for l in lanes][::-1], fontsize=9)
    a2.grid(False)
    a2.set_xlabel('진한 막대 = 신호 ON, 연한 막대 = 뒤 2틱 응답창, 회색 = BuildUp OFF (분석 제외, 4틱만 표시 · 점선 = 생략)   ·   x축 = 사이클 시작 시각, 틱 약 0.52초')
    starts = np.where(np.r_[True, sub.cyc.values[1:] != sub.cyc.values[:-1]] & sub.bu.values)[0]
    a2.set_xticks(starts)
    a2.set_xticklabels([pd.Timestamp(sub.Time.iloc[i]).strftime('%H:%M:%S') for i in starts], fontsize=8)
    for i in np.where(np.diff(sub.index.values) > 1)[0]:  # 압축으로 잘라낸 자리 표시
        for a in (a1, a2): a.axvline(i + 0.5, color='#8a8984', lw=0.8, ls=':')
    return fig


def fig_tick_profile(dfs, thr):
    fig, axs = plt.subplots(3, 2, figsize=(15, 9), sharex=True, gridspec_kw={'hspace': 0.12, 'wspace': 0.08})
    for j, (name, df) in enumerate(dfs.items()):
        on = df[df.bu].copy()
        on['ptn'] = on[PT] / on.groupby('cyc')[PT].transform('median') * 100
        cat = categorize(df, thr).loc[on.index]
        g = on.groupby('tick'); sz = g.size(); keep = sz[sz >= 100].index
        tk = np.array(keep)
        a = axs[0, j]
        a.fill_between(tk, g.ptn.quantile(.1)[keep], g.ptn.quantile(.9)[keep], color='#d9d8d2', lw=0, label='P10~P90')
        a.plot(tk, g.ptn.median()[keep], color='#3a3936', lw=2, label='중앙값')
        a.plot(tk, g.ptn.quantile(.1)[keep], color='#3a3936', lw=1, ls='--', label='P10')
        a.set_ylim(85, 106); a.set_title(DAYS[name], loc='left', fontsize=11)
        if j == 0:
            a.set_ylabel('압력 (사이클 중앙값=100)'); a.legend(frameon=False, fontsize=8, loc='lower right')
        a = axs[1, j]; bottom = np.zeros(len(tk))
        for k in ['S1', 'S2', 'F', 'none']:
            r = (cat == k).groupby(on.tick).mean().reindex(keep).values * 100
            a.bar(tk, r, bottom=bottom, color=C[k], width=0.8, edgecolor='#fcfcfb', lw=1, label=CAT_LABEL[k])
            bottom += r
        if j == 0:
            a.set_ylabel(f'유의 하락 틱 비율 %\n(≥{thr:.1f}%)'); a.legend(frameon=False, fontsize=8, loc='upper right')
        a = axs[2, j]
        a.plot(tk, g.Shot_Injection_HD1_P1.apply(lambda s: (s != 0).mean())[keep]*100, color=C['S1'], lw=2, marker='o', ms=4, label='S1 HD1_P1')
        a.plot(tk, g.Shot_Injection_On_HD1.apply(lambda s: (s != 0).mean())[keep]*100, color=C['S2'], lw=2, marker='o', ms=4, label='S2 On_HD1')
        fon = on[[f'HD1_FOAMING_SHOT{k}' for k in range(1, 5)]].ne(0).any(axis=1)
        a.plot(tk, fon.groupby(on.tick).mean().reindex(keep)*100, color=C['F'], lw=2, marker='o', ms=4, label='FOAMING 1~4 (아무거나)')
        a.set_xlabel('사이클 내 틱 위치 (BuildUp ON 이후)')
        if j == 0:
            a.set_ylabel('신호 ON 비율 %'); a.legend(frameon=False, fontsize=8, loc='upper right')
        for k in range(3): axs[k, j].set_xticks(tk)
    fig.suptitle('사이클 안 어느 위치에서 압력이 떨어지고, 그때 어떤 신호가 켜져 있나', x=0.07, ha='left', fontsize=13)
    return fig


def auc_matched(ev, nullL, min_n=50):
    """응답창 길이(L)가 같은 비교군끼리 AUC 를 구해 이벤트 수로 가중평균.
    비교군이 min_n 개 미만인 긴 L 은 가장 긴 사용가능 L 의 비교군을 쓴다(긴 창 쪽에 유리 = 보수적)."""
    usable = sorted(L for L, x in nullL.items() if len(x) >= min_n)
    Ls = ev['L'].astype(int).clip(usable[0], usable[-1]).values; d = ev['drop'].values
    return sum(auc(d[Ls == L], nullL[L]) * (Ls == L).sum() for L in np.unique(Ls)) / len(d)


def fig_auc(evs, nullL, thr_ev):
    names = ['S1 HD1_P1', 'S2 On_HD1', 'S3 FOAMING1~4', 'S4 On_HD1∧SHOTk', 'S3 SHOT3']
    colors = [C['S1'], C['S2'], C['F'], C['S2'], C['F']]
    null_all = np.concatenate([nullL[3], nullL[4]])
    fig, axs = plt.subplots(2, len(names), figsize=(17, 6.6), sharex=True, sharey='row',
                            gridspec_kw={'hspace': 0.35, 'wspace': 0.08})
    bins = np.arange(-6, 20.5, 1)
    for i, (day, ev) in enumerate(evs.items()):
        for j, (nm, col) in enumerate(zip(names, colors)):
            a = axs[i, j]; d = ev[nm]['drop'].clip(-6, 20)
            a.hist(null_all.clip(-6, 20), bins=bins, density=True, color=C['null'], alpha=0.55)
            a.hist(d, bins=bins, density=True, histtype='step', color=col, lw=2.2)
            a.axvline(thr_ev, color='#0b0b0b', lw=1, ls='--')
            au = auc_matched(ev[nm], nullL); above = (ev[nm]['drop'] >= thr_ev).mean()*100
            a.text(0.97, 0.95, f'AUC {au:.2f}\n≥{thr_ev:.1f}%: {above:.0f}%\nn={len(d)}', transform=a.transAxes,
                   ha='right', va='top', fontsize=9, color='#0b0b0b')
            if i == 0: a.set_title(nm, fontsize=10, color='#0b0b0b', loc='left')
            if j == 0: a.set_ylabel(f'{DAYS[day]}\n밀도', fontsize=9)
            if i == 1: a.set_xlabel('이벤트 하락 % (앞2틱 평균 대비 최저)')
    h = [Patch(color=C['null'], alpha=0.55, label=f'비교군: 앞뒤 2틱 안에 S2(사출)가 없는 위치에서 똑같이 잰 하락 (n={len(null_all)})'),
         Line2D([], [], color='#52514e', lw=2.2, label='해당 신호 이벤트의 하락'),
         Line2D([], [], color='#0b0b0b', lw=1, ls='--', label=f'유의 기준 {thr_ev:.1f}% (비교군 P95)')]
    fig.legend(handles=h, loc='upper left', ncol=3, frameon=False, fontsize=9, bbox_to_anchor=(0.06, 1.0))
    return fig


def fig_share(dfs, thr, thr_big):
    fig, axs = plt.subplots(1, 2, figsize=(15, 2.9), sharey=True, gridspec_kw={'wspace': 0.05})
    for a, (lab, th) in zip(axs, [(f'유의 하락 틱 (≥{thr:.1f}%)', thr), (f'강한 하락 틱 (≥{thr_big:.1f}%)', thr_big)]):
        for y, (name, df) in enumerate(dfs.items()):
            cat = categorize(df, th); vc = cat[cat != ''].value_counts(normalize=True) * 100
            left = 0
            for k in ['S1', 'S2', 'F', 'none']:
                w = vc.get(k, 0)
                a.barh(y, w, left=left, color=C[k], edgecolor='#fcfcfb', lw=2, height=0.6, label=CAT_LABEL[k] if y == 0 else None)
                if w >= 4:
                    a.text(left + w/2, y, f'{w:.0f}%', ha='center', va='center', color='#ffffff', fontsize=9, fontweight='bold')
                left += w
            a.text(101, y, f"n={int((cat != '').sum())}", va='center', fontsize=8, color='#52514e')
        a.set_yticks([0, 1]); a.set_yticklabels([DAYS[k] for k in dfs]); a.set_xlim(0, 108)
        a.set_title(lab, loc='left', fontsize=11); a.grid(False); a.set_xticks([])
    axs[0].legend(loc='upper left', bbox_to_anchor=(0, -0.05), ncol=4, frameon=False, fontsize=9)
    fig.suptitle('유의 하락이 일어난 틱은 누구의 창 안에 있었나 (겹치면 S1 → S2 → FOAMING 순으로 귀속)',
                 x=0.07, ha='left', fontsize=12, y=1.04)
    return fig
