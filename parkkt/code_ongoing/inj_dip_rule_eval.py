"""'사출 신호 창 밖에서 압력이 떨어지면 이상' 규칙이 쓸 만한지 — 창 선택 x 임계값(% / 절대값)별 오탐률.

이 두 날(10/02, 10/06)은 정상 운전이라고 가정하고, 창 밖 하락 = 오탐으로 센다.
임계값을 비교군 분위로 잡으면 'S2 창 밖 = 비교군' 이라 순환논리가 되므로 고정값 격자로 본다.
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import numpy as np, pandas as pd
from inj_dip_signal_viz import prepare, PT, DAYS, segs
import inj_dip_signal_base as A


def add_abs(df):
    v = df[PT].values.astype(float); base = np.full(len(v), np.nan); base[2:] = (v[:-2] + v[1:-1]) / 2
    df['d_abs'] = base - v
    df['base'] = base
    return df


def event_drop_abs(df, masks):
    v = df[PT].values.astype(float); bu = df.bu.values; cyc = df.cyc.values; tick = df.tick.values
    out = []
    for m in masks:
        for s, e in segs(m):
            r = A.event_drop(v, bu, cyc, tick, s, e)
            if r: out.append((r[0], r[1], r[0] * r[1] / 100))   # base, pct, abs
    return pd.DataFrame(out, columns=['base', 'pct', 'abs'])


if __name__ == '__main__':
    dfs = {n: add_abs(prepare(n)[0]) for n in ['fri', 'today']}
    hours = {n: (pd.Timestamp(d.Time.iloc[-1]) - pd.Timestamp(d.Time.iloc[0])).total_seconds() / 3600 for n, d in dfs.items()}

    # ---- 1) 사출 없는 위치의 흔들림: 압력 레벨에 따라 % 와 절대값 중 어느 쪽이 일정한가
    q = pd.concat([pd.DataFrame(d[d.quiet].to_dict('list')) for d in dfs.values()])
    q['lvl'] = pd.cut(q.base, [0, 95, 110, 125, 999], labels=['<95', '95~110', '110~125', '>125'])
    print('[비교군 틱 흔들림 vs 압력 레벨]  (레벨에 따라 덜 변하는 쪽이 임계값으로 적합)')
    print(q.groupby('lvl', observed=True).agg(n=('d_t', 'size'),
          pct_P95=('d_t', lambda x: x.quantile(.95)), pct_P99=('d_t', lambda x: x.quantile(.99)),
          abs_P95=('d_abs', lambda x: x.quantile(.95)), abs_P99=('d_abs', lambda x: x.quantile(.99))).round(2).to_string())
    print(f'   전체: pct P95 {q.d_t.quantile(.95):.2f}% P99 {q.d_t.quantile(.99):.2f}%  |  abs P95 {q.d_abs.quantile(.95):.1f} P99 {q.d_abs.quantile(.99):.1f}')

    # ---- 2) 창 밖 하락 = 오탐. 창 선택별
    for n, d in dfs.items():
        ok = d.ok.values
        W = {'S1 창': d.W_S1.values, 'S2 창 (=S1∪S2)': d.W_S2.values, 'FOAMING 창': d.W_F.values}
        print(f'\n===== {DAYS[n]}  ({hours[n]:.1f}h, 펌프 가동 안정틱 {ok.sum()})')
        for metric, col, grid in [('%', 'd_t', [3, 4, 5, 6, 8, 10]), ('절대', 'd_abs', [3, 4, 5, 6, 8, 10])]:
            x = d[col].values
            print(f'  [{metric}]  창 밖 하락 알람 건수/시간 (괄호: 창 안 대비 창 밖 발생률 비)')
            hdr = '  '.join(f'≥{g}{"%" if metric=="%" else ""}'.rjust(13) for g in grid)
            print(f'   {"창":16s} 밖시간  {hdr}')
            for wn, w in W.items():
                out_t = ok & ~w; in_t = ok & w
                cells = []
                for g in grid:
                    a_out = (out_t & (x >= g)).sum(); a_in = (in_t & (x >= g)).sum()
                    r_out = a_out / max(out_t.sum(), 1); r_in = a_in / max(in_t.sum(), 1)
                    cells.append(f'{a_out/hours[n]:6.1f} ({r_out/r_in if r_in else np.nan:4.2f})'.rjust(13))
                print(f'   {wn:16s} {out_t.sum()/ok.sum()*100:4.1f}%  ' + '  '.join(cells))

    # ---- 3) '사출했는데 안 떨어짐' 쪽: S1 이벤트 중 하락이 임계 미만인 비율
    print('\n[S1 이벤트 중 하락이 임계값 미만인 비율 — "사출했는데 안 떨어짐"을 이상으로 보면 이만큼 알람]')
    for n, d in dfs.items():
        e = event_drop_abs(d, [d.Shot_Injection_HD1_P1.values != 0])
        print(f'   {DAYS[n]}: n={len(e)}  ' + '  '.join(f'<{g}%:{(e.pct < g).mean()*100:.0f}%' for g in [3, 5]) + '   ' +
              '  '.join(f'<{g}:{(e["abs"] < g).mean()*100:.0f}%' for g in [3, 5]))
