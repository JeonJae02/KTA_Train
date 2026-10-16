"""사출 하락 크기(dPT1/dPT2) · '전혀 안 떨어짐' 검증 · 사출 없는 하락 깔때기 · 설명 안 되는 하락의 시점.
입력: inj_feat_q3.py 가 만든 parkkt/cache/inj_feat_daily/_ticks_q3.pkl (생산 틱 + P2 + dSV2 등)
"""
import os, sys, glob
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import numpy as np, pandas as pd
import matplotlib.pyplot as plt
from inj_feat_lib import to_prod_ticks, CACHE, HOLIDAY, mutual_info

plt.rcParams.update({'font.family': 'Malgun Gothic', 'axes.unicode_minus': False,
                     'axes.spines.top': False, 'axes.spines.right': False,
                     'axes.edgecolor': '#8a8984', 'axes.labelcolor': '#52514e',
                     'xtick.color': '#52514e', 'ytick.color': '#52514e',
                     'axes.grid': True, 'grid.color': '#e6e5e0', 'grid.linewidth': 0.6,
                     'figure.facecolor': '#fcfcfb', 'axes.facecolor': '#fcfcfb'})
ERA_COL = {'연휴 전': '#6fa6e8', '연휴 후': '#0d366b'}
C_S1, C_P2, C_SV, C_UN = '#2a78d6', '#eb6834', '#eda100', '#e34948'
SVB_LABELS = ['SV ≤2100', '2101~2200', '2201~2400', '2401~2600', '>2600']
BIG = 5
MAG_BINS = [-np.inf, -10, -8, -7, -6, -5]
MAG_LABELS = ['≤-10', '-9~-8', '-7', '-6', '-5']
CATS = ['짝펌프 P2 사출 (Pair_Inj_Flag)', 'SV 변경 틱/다음 틱 (dSV2≠0)', '증압 중 tick 1~3 (FT_Settled=0)',
        'S1 꺼진 직후 1~2틱 (피처 없음)', '설명 안 됨']
CAT_COL = [C_P2, C_SV, '#1baf7a', '#b5b4ad', C_UN]


# ------------------------------------------------------------------ 데이터
def load_ticks():
    p = f'{CACHE}/_ticks_q4.pkl'
    if os.path.exists(p):
        return pd.read_pickle(p)
    T = pd.read_pickle(f'{CACHE}/_ticks_q3.pkl').reset_index(drop=True)
    ph = []
    for d in T.day.unique():
        raw = to_prod_ticks(pd.read_pickle(f'{CACHE}/p1_{pd.Timestamp(d):%Y%m%d}.pkl'))
        f = sum(k * (raw[f'HD1_FOAMING_SHOT{k}'] != 0) for k in range(1, 5))
        ph.append(pd.DataFrame({'Time': raw.index, 'F_phase': f.values}))
    T = T.merge(pd.concat(ph), on='Time', how='left')
    T['hour'] = pd.to_datetime(T.Time).dt.hour
    T.to_pickle(p)
    return T


def s1_events(T):
    """S1 ON 연속 구간마다: ON 틱 dPT1/dPT2 최저, 앞뒤 2틱의 dPT1 최저 (하락이 신호 밖으로 밀렸는지 확인)."""
    on = T.S1.values; shot = T.shot.values; d1 = T.dPT1.values; d2 = T.dPT2.values; pt = T.PT.values
    same_prev = np.r_[False, shot[1:] == shot[:-1]]; same_next = np.r_[shot[1:] == shot[:-1], False]
    S = np.where((on == 1) & ~((np.r_[0, on[:-1]] == 1) & same_prev))[0]
    E = np.where((on == 1) & ~((np.r_[on[1:], 0] == 1) & same_next))[0]
    rows = []
    n = len(T)
    for s, e in zip(S, E):
        sh = shot[s]
        pre = [i for i in (s - 2, s - 1) if i >= 0 and shot[i] == sh]
        post = [i for i in (e + 1, e + 2) if i < n and shot[i] == sh]
        win = pre + list(range(s, e + 1)) + post
        rows.append((s, e, e - s + 1, np.nanmin(d1[s:e + 1]), np.nanmin(d2[s:e + 1]),
                     np.nanmin(d1[pre]) if pre else np.nan, np.nanmin(d1[post]) if post else np.nan,
                     (pt[s - 1] - pt[win].min()) if s >= 1 and shot[s - 1] == sh else np.nan))
    ev = pd.DataFrame(rows, columns=['s', 'e', 'n_on', 'min_d1', 'min_d2', 'pre_min_d1', 'post_min_d1', 'win_drop'])
    for c in ['day', 'era', 'svb', 'SV', 'Tick_Index', 'Time', 'F_phase', 'hour']:
        ev[c] = T[c].values[ev.s.values]
    ev['P2_on_win'] = [T.P2.values[max(s - 2, 0):e + 3].max() for s, e in zip(ev.s, ev.e)]
    ev['svchg_win'] = [(T.since_sv.values[max(s - 2, 0):e + 3] <= 1).any() for s, e in zip(ev.s, ev.e)]
    return ev


def off_ticks(T):
    J = T[(T.S1 == 0) & T.dPT1.notna() & (T.Tick_Index >= 1)].copy()
    J['cat'] = np.select([J.P2 == 1, J.since_sv <= 1, J.Tick_Index <= 3, J.S1_prev == 1], CATS[:4], CATS[4])
    J['mag'] = pd.cut(J.dPT1, MAG_BINS + [np.inf], labels=MAG_LABELS + ['정상(>-5)'], right=True)
    return J


# ------------------------------------------------------------------ 1. 하락 크기
def fig_drop_dist(ev):
    fig, axs = plt.subplots(1, 2, figsize=(15, 4.4), sharey=True, gridspec_kw={'wspace': 0.06})
    bins = np.arange(-25.5, 8, 1)
    for a, col, lab in zip(axs, ['min_d1', 'min_d2'], ['dPT1 = PT(t)-PT(t-1)', 'dPT2 = PT(t)-PT(t-2)']):
        for er in ['연휴 전', '연휴 후']:
            x = ev.loc[ev.era == er, col].clip(-25, 7)
            a.hist(x, bins=bins, density=True, histtype='stepfilled' if er == '연휴 전' else 'step',
                   color=ERA_COL[er], alpha=0.55 if er == '연휴 전' else 1, lw=2.2,
                   label=f'{er}  중앙 {x.median():.0f}, ≤-5 {(x <= -5).mean()*100:.0f}%, ≥0 {(x >= 0).mean()*100:.1f}%')
        a.axvline(-BIG, color='#0b0b0b', lw=1, ls='--'); a.axvline(0, color='#52514e', lw=0.8)
        a.set_title(f'S1 이벤트마다 ON 틱 중 가장 큰 하락 — {lab}', loc='left', fontsize=11)
        a.set_xlabel('눈금'); a.legend(frameon=False, fontsize=9, loc='upper left')
    axs[0].set_ylabel('이벤트 밀도')
    return fig


def fig_drop_by_sv(ev):
    fig, axs = plt.subplots(1, 2, figsize=(15, 4.2), sharey=True, gridspec_kw={'wspace': 0.06})
    for a, col in zip(axs, ['min_d1', 'min_d2']):
        pos = 0; ticks = []
        for lab in SVB_LABELS:
            for j, er in enumerate(['연휴 전', '연휴 후']):
                x = ev[(ev.svb == lab) & (ev.era == er)][col].dropna()
                bp = a.boxplot([x.clip(-25, 8)], positions=[pos + j * 0.42], widths=0.36, patch_artist=True,
                               showfliers=False, medianprops=dict(color='#0b0b0b'))
                bp['boxes'][0].set_facecolor(ERA_COL[er])
            ticks.append(pos + 0.21); pos += 1.2
        a.set_xticks(ticks); a.set_xticklabels(SVB_LABELS, fontsize=9)
        a.axhline(-BIG, color='#0b0b0b', lw=1, ls='--'); a.axhline(0, color='#52514e', lw=0.8)
        a.set_title(f'{col.replace("min_", "최저 ")} — 레시피별 (연한 = 연휴 전, 진한 = 연휴 후)', loc='left', fontsize=11)
    axs[0].set_ylabel('눈금')
    return fig


def dpt1_vs_dpt2_table(T, ev):
    rows = []
    J = T[T.dPT1.notna() & T.dPT2.notna() & (T.Tick_Index >= 3)]
    for er in ['연휴 전', '연휴 후']:
        j = J[J.era == er]; e = ev[ev.era == er]
        f = j.S1.values
        b1 = np.digitize(j.dPT1, [-9, -7, -5, -3, -1, 0.5, 2.5, 4.5, 6.5]); b2 = np.digitize(j.dPT2, [-9, -7, -5, -3, -1, 0.5, 2.5, 4.5, 6.5])
        rows.append({'구간': er,
                     '이벤트 ≤-5 (dPT1)%': (e.min_d1 <= -5).mean() * 100, '이벤트 ≤-5 (dPT2)%': (e.min_d2 <= -5).mean() * 100,
                     '이벤트 ≤-3 (dPT1)%': (e.min_d1 <= -3).mean() * 100, '이벤트 ≤-3 (dPT2)%': (e.min_d2 <= -3).mean() * 100,
                     '이벤트 ≥0 (dPT1)%': (e.min_d1 >= 0).mean() * 100, '이벤트 ≥0 (dPT2)%': (e.min_d2 >= 0).mean() * 100,
                     'MI 플래그↔dPT1': mutual_info(f, j.dPT1.values), 'MI 플래그↔dPT2': mutual_info(f, j.dPT2.values),
                     'MI 플래그↔(dPT1,dPT2)': mutual_info(f, b1 * 10 + b2, bins=(-np.inf,) + tuple(np.arange(0.5, 100, 1)) + (np.inf,))})
    return pd.DataFrame(rows).set_index('구간').round(3)


def on_tick_profile(T, ev):
    """S1 ON 틱 순서별(1번째, 2번째...) dPT1 평균 — 하락이 몇 번째 틱에 오는지."""
    rows = []
    d1 = T.dPT1.values
    for er in ['연휴 전', '연휴 후']:
        e = ev[(ev.era == er) & (ev.n_on >= 2)]
        for k in range(-2, 5):
            idx = e.s.values + k
            ok = (idx >= 0) & (idx < len(T)) & (T.shot.values[np.clip(idx, 0, len(T) - 1)] == T.shot.values[e.s.values])
            v = d1[idx[ok]]
            rows.append({'구간': er, '위치': f'S1 {"시작" if k == 0 else ("전" if k < 0 else "후")}{"" if k == 0 else f" {abs(k)}"}',
                         'k': k, 'dPT1 평균': np.nanmean(v), '≤-5 %': np.nanmean(v <= -5) * 100,
                         'S1 ON %': T.S1.values[idx[ok]].mean() * 100})
    return pd.DataFrame(rows)


# ------------------------------------------------------------------ 2. 전혀 안 떨어짐
def fig_nodrop_daily(ev):
    g = ev.groupby('day')
    d = pd.DataFrame({'≥0 (dPT1)': g.min_d1.apply(lambda x: (x >= 0).mean() * 100),
                      '≥0 (dPT2)': g.min_d2.apply(lambda x: (x >= 0).mean() * 100),
                      '창 전체 무하락': g.apply(lambda x: ((x.min_d1 >= 0) & (x.pre_min_d1.fillna(0) >= 0) & (x.post_min_d1.fillna(0) >= 0)).mean() * 100)})
    by_sv = ev.assign(nd=ev.min_d2 >= 0).pivot_table(index='day', columns='svb', values='nd', aggfunc='mean', observed=True) * 100
    fig, axs = plt.subplots(2, 1, figsize=(15, 7.2), sharex=True, gridspec_kw={'hspace': 0.15})
    x = np.arange(len(d)); days = list(d.index)
    for c, col in zip(d.columns, ['#6fa6e8', '#0d366b', C_UN]):
        axs[0].plot(x, d[c], lw=2, marker='o', ms=4, color=col, label=c)
    axs[0].set_ylabel('S1 이벤트 중 비율 %'); axs[0].legend(frameon=False, fontsize=9, loc='upper left')
    axs[0].set_title('\"사출했는데 전혀 안 떨어짐\" 비율 — 날짜별 (창 전체 = 앞 2틱 ~ 뒤 2틱까지 dPT1 이 한 번도 음수가 아님)', loc='left', fontsize=11)
    cols = ['#0d366b', '#1f5ba8', '#2a78d6', '#6fa6e8', '#b3d0f5']
    for lab, col in zip(SVB_LABELS, cols):
        if lab in by_sv: axs[1].plot(x, by_sv[lab].values, lw=2, marker='o', ms=4, color=col, label=lab)
    axs[1].set_ylabel('ON 틱 dPT2 최저 ≥ 0 비율 %'); axs[1].legend(frameon=False, fontsize=9, ncol=5, loc='upper left')
    axs[1].set_title('레시피별', loc='left', fontsize=11)
    k = int(np.searchsorted(np.array(days, dtype='datetime64[ns]'), np.datetime64(HOLIDAY)))
    for a in axs: a.axvline(k - 0.5, color='#8a8984', lw=1, ls='--')
    axs[1].set_xticks(x); axs[1].set_xticklabels([f'{dd:%m/%d}' for dd in days], fontsize=8)
    return fig, d.round(1), by_sv.round(1)


def fig_samples(T, centers, titles, ncol=6, before=4, after=6, suptitle=''):
    """작은 그림 여러 장: center 틱 기준 PT. 파랑 배경 = S1 ON, 주황 = P2 ON, 노란 선 = SV 변경."""
    n = len(centers); nrow = int(np.ceil(n / ncol))
    fig, axs = plt.subplots(nrow, ncol, figsize=(2.75 * ncol, 2.15 * nrow), squeeze=False)
    shot = T.shot.values; pt = T.PT.values; s1 = T.S1.values; p2 = T.P2.values; sv = T.SV.values; ti = T.Tick_Index.values
    for ax, c, tt in zip(axs.flat, centers, titles):
        idx = np.arange(c - before, c + after + 1); idx = idx[(idx >= 0) & (idx < len(T))]; idx = idx[shot[idx] == shot[c]]
        x = idx - c
        for xi, i in zip(x, idx):
            if s1[i]: ax.axvspan(xi - 0.5, xi + 0.5, color=C_S1, alpha=0.18, lw=0)
            if p2[i]: ax.axvspan(xi - 0.5, xi + 0.5, color=C_P2, alpha=0.18, lw=0)
            if i > 0 and shot[i - 1] == shot[i] and sv[i] != sv[i - 1]: ax.axvline(xi - 0.5, color=C_SV, lw=1.6)
        ax.plot(x, pt[idx], color='#3a3936', lw=1.4, marker='o', ms=2.5)
        ax.scatter([0], [pt[c]], color=C_UN, s=26, zorder=3)
        ax.set_title(tt, fontsize=7.5, loc='left'); ax.tick_params(labelsize=7); ax.set_xticks([-4, -2, 0, 2, 4, 6])
        lo, hi = pt[idx].min(), pt[idx].max(); pad = max(3, (hi - lo) * 0.15); ax.set_ylim(lo - pad, hi + pad)
    for ax in list(axs.flat)[n:]: ax.set_visible(False)
    fig.suptitle(suptitle, x=0.01, ha='left', fontsize=11)
    fig.tight_layout(rect=[0, 0, 1, 0.975])
    return fig


# ------------------------------------------------------------------ 3. 사출 없는데 떨어짐
def funnel_table(J):
    rows = []
    for er in ['연휴 전', '연휴 후']:
        j = J[J.era == er]; n = len(j)
        big = j[j.dPT1 <= -BIG]
        r = {'구간': er, 'S1 OFF 틱': n, '정상(>-5)': (j.dPT1 > -BIG).sum(), '큰 하락(≤-5)': len(big),
             '큰 하락 %': len(big) / n * 100}
        left = big
        for c in CATS[:4]:
            left = left[left.cat != c]; r[f'- {c.split(" (")[0]} 빼면'] = len(left)
        r['남은 % (OFF 틱 대비)'] = len(left) / n * 100
        r['남은 비율 (큰 하락 대비)%'] = len(left) / len(big) * 100
        rows.append(r)
    return pd.DataFrame(rows).set_index('구간').round(2)


def mag_cat_table(J):
    big = J[J.dPT1 <= -BIG]
    t = big.pivot_table(index=['era', 'mag'], columns='cat', values='dPT1', aggfunc='size', observed=True, fill_value=0)
    t = t.reindex(columns=CATS, fill_value=0)
    t['합'] = t.sum(axis=1)
    norm = J.groupby('era').size()
    t['OFF 틱 1만 개당'] = [t.loc[i, '합'] / norm[i[0]] * 1e4 for i in t.index]
    return t.round(1)


def fig_funnel(J):
    fig, axs = plt.subplots(1, 2, figsize=(15, 4.4), gridspec_kw={'wspace': 0.25})
    for a, er in zip(axs, ['연휴 전', '연휴 후']):
        j = J[J.era == er]; big = j[j.dPT1 <= -BIG]
        steps = [('큰 하락 전체', len(big))]
        left = big
        for c in CATS[:4]:
            left = left[left.cat != c]; steps.append((f'- {c.split(" (")[0]}', len(left)))
        labs, vals = zip(*steps)
        cols = ['#52514e'] + CAT_COL[:4]
        a.barh(range(len(vals)), vals, color=['#52514e', C_P2, C_SV, '#1baf7a', '#b5b4ad'], height=0.6)
        a.barh(len(vals) - 1, vals[-1], color=C_UN, height=0.6)
        for i, v in enumerate(vals):
            a.text(v, i, f'  {v:,}  ({v/len(j)*100:.2f}% of OFF)', va='center', fontsize=8.5)
        a.set_yticks(range(len(vals))); a.set_yticklabels(labs, fontsize=9); a.invert_yaxis()
        a.set_title(f'{er}: S1 OFF 틱 {len(j):,}개 중 dPT1 ≤ -5 — 피처를 하나씩 넣으면', loc='left', fontsize=10.5)
        a.set_xlim(0, vals[0] * 1.55); a.grid(False); a.set_xticks([])
    fig.text(0.07, -0.02, '초록 막대까지 = 제안 피처(Pair_Inj_Flag, dSV2, FT_Settled)로 줄어든 결과. 빨강 = S1 꺼진 직후(대응 피처 없음)까지 마저 뺀 뒤 남은 "설명 안 됨".', fontsize=9, color='#52514e')
    return fig


# ------------------------------------------------------------------ 4. 설명 안 되는 하락의 시점
def rate_by(J, key, order=None):
    base = J[J.cat == '설명 안 됨']           # 설명변수가 다 0 인 OFF 틱 (모집단)
    un = base[base.dPT1 <= -BIG]
    t = pd.DataFrame({'모집단 틱': base.groupby([key, 'era']).size(), '설명안된 하락': un.groupby([key, 'era']).size()}).fillna(0)
    t['1만 틱당'] = t['설명안된 하락'] / t['모집단 틱'] * 1e4
    t = t.unstack('era')
    if order is not None: t = t.reindex(order)
    return t


def fig_when(J, T):
    base = J[J.cat == '설명 안 됨']
    un = base[base.dPT1 <= -BIG].copy()
    # S1 기준 위치: 다음 S1 시작까지 틱 수
    s1 = T.S1.values; shot = T.shot.values
    nxt = np.full(len(T), 99); last = 99
    for i in range(len(T) - 1, -1, -1):
        if i == len(T) - 1 or shot[i + 1] != shot[i]: last = 99
        if s1[i]: last = 0
        else: last = min(last + 1, 99) if last != 99 else 99
        nxt[i] = last
    T = T.assign(to_next_s1=nxt)
    J = J.assign(to_next_s1=T.to_next_s1.values[J.index])
    J['to_next_cat'] = pd.cut(J.to_next_s1, [-1, 1, 2, 3, 5, 98, 100], labels=['1', '2', '3', '4~5', '6+', '이 샷에 S1 더 없음'])
    fig, axs = plt.subplots(2, 3, figsize=(17, 8.2), gridspec_kw={'hspace': 0.45, 'wspace': 0.22})
    specs = [('Tick_Index', '샷 안 틱 위치 (Tick_Index)', list(range(4, 22))),
             ('to_next_cat', '다음 S1 사출까지 남은 틱', ['1', '2', '3', '4~5', '6+', '이 샷에 S1 더 없음']),
             ('F_phase', 'FOAMING 단계 (0 = 없음)', [0, 1, 2, 3, 4]),
             ('hour', '시각 (시)', list(range(7, 24))),
             ('svb', '레시피 SV', SVB_LABELS),
             ('day', '날짜', None)]
    tabs = {}
    for a, (key, lab, order) in zip(axs.flat, specs):
        t = rate_by(J, key, order)
        tabs[key] = t
        x = np.arange(len(t))
        for j, er in enumerate(['연휴 전', '연휴 후']):
            if ('1만 틱당', er) not in t: continue
            a.bar(x + (j - 0.5) * 0.4, t[('1만 틱당', er)].values, width=0.38, color=ERA_COL[er], label=er)
        a.axhline(len(un) / len(base) * 1e4, color=C_UN, lw=1, ls='--')
        a.set_xticks(x)
        if key == 'day':
            a.set_xticklabels([f'{d:%m/%d}' for d in t.index], fontsize=7, rotation=90)
        else:
            a.set_xticklabels([str(v) for v in t.index], fontsize=8, rotation=0 if key != 'svb' else 20)
        a.set_title(lab, loc='left', fontsize=10.5)
    axs[0, 0].set_ylabel('설명 안 된 하락\n(모집단 1만 틱당)'); axs[1, 0].set_ylabel('설명 안 된 하락\n(모집단 1만 틱당)')
    axs[0, 0].legend(frameon=False, fontsize=9)
    fig.suptitle('설명 안 되는 하락(dPT1 ≤ -5)은 언제 생기나 — 같은 조건의 틱 수로 나눈 발생률 (빨간 파선 = 전체 평균)', x=0.07, ha='left', fontsize=12)
    return fig, tabs, un


def per_shot_cluster(J):
    base = J[J.cat == '설명 안 됨']
    un = base[base.dPT1 <= -BIG]
    k = un.groupby('shot').size()
    n_shots = base.shot.nunique()
    lam = len(un) / n_shots
    from math import exp, factorial
    obs = k.value_counts().sort_index()
    rows = [{'샷당 건수': 0, '관측 샷 수': n_shots - len(k), '무작위(포아송) 기대': n_shots * exp(-lam)}]
    for c in range(1, 5):
        rows.append({'샷당 건수': c, '관측 샷 수': int(obs.get(c, 0)), '무작위(포아송) 기대': n_shots * exp(-lam) * lam ** c / factorial(c)})
    rows.append({'샷당 건수': '5+', '관측 샷 수': int(obs[obs.index >= 5].sum()), '무작위(포아송) 기대': np.nan})
    return pd.DataFrame(rows).round(1)
