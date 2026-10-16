"""9/10~ 일별 캐시(inj_dip_pull_daily.py)를 같은 정의로 처리하는 벡터화 버전.

정의는 inj_dip_signal_viz.prepare 와 동일하다:
  - 틱 하락 = 직전 2틱 평균 대비 현재 틱 (%, 절대값 둘 다)
  - 판정 대상(ok) = 기준 2틱 + 현재 틱이 모두 BuildUp=1, 같은 사이클, 기준 첫 틱이 tick>=1
  - 신호 창 = 각 신호 ON 구간 [시작, 끝+2틱]
"""
import os, sys, glob
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import numpy as np, pandas as pd
from inj_dip_signal_base import segs
from inj_dip_s1_events import prep_raw, s1_events, PT, FT

CACHE = 'parkkt/cache/inj_dip_daily'
POST = 2
THRS = [5, 6, 8]
SV_CHG_TICKS = 3   # 직전 3틱 안에 SV(목표유량)가 바뀌었으면 '설명된 하락' (2단 사출 레시피의 SV 하향 등)


def day_files():
    return sorted(glob.glob(f'{CACHE}/p1_*.pkl'))


def window(mask, n):
    r = np.zeros(n, bool)
    for s, e in segs(mask):
        r[s:min(e + POST, n - 1) + 1] = True
    return r


def ticks(df):
    """틱 단위 표: 하락값, ok, 창 소속."""
    v = df[PT].values.astype(float); n = len(v)
    bu = df.bu.values; cyc = df.cyc.values; tick = df.tick.values
    base = np.full(n, np.nan); base[2:] = (v[:-2] + v[1:-1]) / 2
    ok = np.zeros(n, bool)
    ok[2:] = bu[2:] & bu[1:-1] & bu[:-2] & (cyc[2:] == cyc[:-2]) & (tick[:-2] >= 1)
    S1 = df.Shot_Injection_HD1_P1.values != 0; S2 = df.Shot_Injection_On_HD1.values != 0
    F = np.zeros(n, bool)
    for k in range(1, 5):
        F |= window(df[f'HD1_FOAMING_SHOT{k}'].values != 0, n)
    return pd.DataFrame({'Time': df.Time.values, 'cyc': cyc, 'tick': tick, 'PT': v, 'base': base,
                         'd_pct': (base - v) / base * 100, 'd_abs': base - v, 'ok': ok,
                         'W_S1': window(S1, n), 'W_S2': window(S2, n), 'W_F': F,
                         'sv': df.g_s_SV_P1.values, 'ft': df[FT].values,
                         'sv_chg': sv_changed(df.g_s_SV_P1.values, cyc)})


def sv_changed(sv, cyc, k=SV_CHG_TICKS):
    """[t-k, t] 안에서 같은 사이클 내 SV 가 바뀌었는가 (사이클 시작 때의 SV 세팅은 제외)."""
    n = len(sv); chg = np.zeros(n, bool)
    chg[1:] = (sv[1:] != sv[:-1]) & (cyc[1:] == cyc[:-1])
    out = chg.copy()
    for j in range(1, k + 1):
        out[j:] |= chg[:-j]
    return out


def process_all(force=False):
    """일별 요약 / 창 밖 알람 목록 / S1 이벤트 표를 만들어 캐시."""
    out_p = f'{CACHE}/_summary.pkl'
    if os.path.exists(out_p) and not force:
        return pd.read_pickle(out_p)
    days, alarms, events = [], [], []
    for p in day_files():
        day = pd.Timestamp(os.path.basename(p)[3:11])
        df = prep_raw(pd.read_pickle(p))
        if (df.bu.mean() < 0.05) or (df.Shot_Injection_HD1_P1 != 0).sum() == 0:
            continue                       # 비가동일(일요일·연휴)
        t = ticks(df)
        on_h = t.ok.sum() * np.median(np.diff(t.Time.values).astype('timedelta64[ms]').astype(float)) / 3.6e6
        row = {'day': day, 'ok_ticks': int(t.ok.sum()), 'pump_on_h': on_h,
               'cycles': int(df[df.bu].cyc.nunique())}
        row['sv_chg_frac'] = (t.ok & t.sv_chg).sum() / t.ok.sum()
        for wn in ['W_S1', 'W_S2', 'W_F']:
            out = t.ok & ~t[wn] & ~t.sv_chg
            row[f'{wn}_out_frac'] = out.sum() / t.ok.sum()
            for g in THRS:
                row[f'{wn}_out_pct{g}'] = int((out & (t.d_pct >= g)).sum())
                row[f'{wn}_out_abs{g}'] = int((out & (t.d_abs >= g)).sum())
                row[f'{wn}_in_pct{g}'] = int((t.ok & t[wn] & ~t.sv_chg & (t.d_pct >= g)).sum())
                row[f'{wn}_out_pct{g}_with_svchg'] = int((t.ok & ~t[wn] & (t.d_pct >= g)).sum())
        days.append(row)
        row['W_S2_out_ticks'] = int((t.ok & ~t.W_S2 & ~t.sv_chg).sum())
        a = t[t.ok & ~t.W_S2 & (t.d_pct >= min(THRS))].copy(); a['day'] = day
        a['in_S1'] = a.W_S1; a['in_F'] = a.W_F
        alarms.append(a)
        e = s1_events(df); e['day'] = day; events.append(e)
    res = {'days': pd.DataFrame(days).set_index('day'), 'alarms': pd.concat(alarms, ignore_index=True),
           'events': pd.concat(events, ignore_index=True)}
    pd.to_pickle(res, out_p)
    return res


def load_day(day):
    """특정 날짜 원본 + 틱 표 (그림용)."""
    df = prep_raw(pd.read_pickle(f'{CACHE}/p1_{pd.Timestamp(day):%Y%m%d}.pkl'))
    return df, ticks(df)


if __name__ == '__main__':
    r = process_all(force=True)
    d = r['days']
    pd.set_option('display.width', 250)
    cols = ['cycles', 'sv_chg_frac', 'W_S2_out_frac'] + [f'W_S2_out_pct{g}_with_svchg' for g in THRS] + [f'W_S2_out_pct{g}' for g in THRS] + [f'W_S1_out_pct{g}' for g in THRS] + [f'W_S2_out_abs{g}' for g in THRS]
    print(d[cols].round(2).to_string())
    print('alarms', len(r['alarms']), 'events', len(r['events']))
