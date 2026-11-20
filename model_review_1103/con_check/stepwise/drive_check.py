"""구동부 판정 후보 비교 — 20일, 60초마다 판정, |편차| > 0.3% 가 몇 번·몇 덩어리인가.

A  지금 (샘플 간격 < 1초만, 표본 100개)
B  S2 (A + 재가동 뒤 5분 유예 + 창 길이 5분 — 창 길이에 정지 전 샘플이 섞임)
C  간격 < 2초 샘플도 씀(스로틀이 건너뛴 구간 포함 → 치우침 없음) + 재가동 뒤 60초 샘플은 버림 + 창 안 정속 시간 5분 이상
D  C + 재가동 뒤 5분 유예
"""
import sys
from collections import deque
import numpy as np, pandas as pd
sys.path.insert(0, "/Users/jeonjae/Desktop/KTA/DB_Log/model_review_1103/con_check/stepwise")
import harness as H
cr = H.cr
mode = sys.argv[1]
tags = H.db.model_tags("conveyor", "v2.2")


class C(cr.CoreRealtime):
    def __init__(s):
        super().__init__(history_path=None)
        s.sp2 = deque(); s.prev_t2 = None; s.run_on = None
    def update(s, t, row):
        prev_run = s.last.get(H.RUN_TAG); pos0 = s.pos
        super().update(t, row)
        if s.run_on is None or ((prev_run or 0) <= 0 < (s.last.get(H.RUN_TAG) or 0)):
            s.run_on = t
        dt = None if s.prev_t2 is None else (t - s.prev_t2).total_seconds()
        s.prev_t2 = t
        hz = s.last.get(cr.HZ_TAG); inc = s.pos - pos0
        steady = (hz is not None and hz > 0 and len(s.hz_hist) == s.hz_hist.maxlen and len(set(s.hz_hist)) == 1 and all(s.run_hist))
        if steady and not s.new_seg and dt is not None and dt < 2.0 and inc > 0 and (t - s.run_on).total_seconds() >= 60:
            s.sp2.append((t, inc, dt * hz / 10, dt))
        while s.sp2 and (t - s.sp2[0][0]).total_seconds() > cr.SPEED_WINDOW_SEC:
            s.sp2.popleft()


def dev(entries):
    a = np.array([(x[1], x[2]) for x in entries]); return a[:, 0].sum() / a[:, 1].sum() / cr.K_SPEED - 1

c = C(); kept = None; last = None; rec = []
for day in pd.date_range("2026-10-26", "2026-11-14"):
    for t, row in H.rows_of(day, tags):
        if mode == "throttle":
            if kept is not None and (t - kept).total_seconds() < 0.5: continue
            kept = t
        c.update(t, row)
        if last is None or (t - last).total_seconds() >= 60:
            last = t
            since = (t - c.run_on).total_seconds()
            a = dev(c.speed) if len(c.speed) >= 100 else np.nan
            b = a if (since >= 300 and c.speed and (c.speed[-1][0] - c.speed[0][0]).total_seconds() >= 300) else np.nan
            steady_s = sum(x[3] for x in c.sp2)
            cc = dev(c.sp2) if steady_s >= 300 else np.nan
            d = cc if since >= 300 else np.nan
            rec.append((t, a, b, cc, d))
R = pd.DataFrame(rec, columns=["t", "A", "B", "C", "D"]).set_index("t")
R.to_pickle(f"{H.OUT}/drive_{mode}.pkl")
for k in "ABCD":
    x = R[k].dropna(); over = x.abs() > cj_tol if (cj_tol := 0.003) else None
    o = over[over]
    blocks = (o.index.to_series().diff().dt.total_seconds().fillna(1e9) > 3 * 3600).sum() if len(o) else 0
    print(mode, k, f"판정 {len(x)} · 편차 std {x.std()*100:.3f}% · 평균 {x.mean()*100:+.3f}% · 0.3% 넘음 {len(o)}번 · 덩어리(3h) {blocks}")
