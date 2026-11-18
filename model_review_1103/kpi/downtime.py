"""2라인 다운타임: 시스템 설치(2026-10-16) 전후 비교.

정의
- 가동 = Auto_Running_PL == 1 (PLC 자동운전 표시등, 백엔드 RUN_FLAG_TAG).
- 생산일 = 그날 자동운전이 3시간 이상인 날. 일요일·추석·정비청소일(9/27, 11/7 등)은 빠진다.
- 계획가동시간 = 생산일들에서 "그 분(minute)에 자동운전이 켜져 있던 날의 비율" ≥ 50% 인 분.
  (근무표 shifts 안이지만 휴게·식사 몇 분 전 선정지는 매일 하므로 계획가동에서 빠진다.)
- 다운타임 = 생산일의 계획가동시간 중 자동운전이 꺼진 시간.
- 계획정지 = 일지로 확인된 비고장 정지(건강검진 등) — PLANNED 에 적는다. 다운타임에서 따로 뺀다.
- 데이터 결측(기록 간격 > 330초)은 계획가동·다운타임 양쪽에서 뺀다.

입력: cache/raw/conveyor (9/10~), cache/raw/runflag (9/7~9/9, 9/25~9/30). 인덱스 KST.
"""
import glob
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT = Path(__file__).resolve().parent
INSTALL = pd.Timestamp("2026-10-16")
#: DB는 9/7 14시부터 있지만 9/9 15시까지 자동운전 태그가 0 고정(일지상 생산 중) — 수집 안정화 전이라 뺀다
DATA_START = pd.Timestamp("2026-09-10")
GAP_SEC = 330          # 스냅샷 300초 + 여유. 이보다 긴 간격은 결측
PROD_DAY_MIN_H = 3.0
EVENT_MIN_SEC = 60     # 이보다 짧은 꺼짐은 '깜빡임'으로 따로 센다

# 일지로 확인한 계획 정지 (KST). 다운타임에서 뺀다.
PLANNED = [
    ("2026-11-04 07:30", "2026-11-04 10:50", "건강검진 (일지 M 11-04 08:00~10:30)"),
    ("2026-11-11 07:30", "2026-11-11 10:50", "건강검진 (일지 M 11-11 08:00~10:30)"),
]

# 5분 이상 정지의 일지 매칭 (시각이 겹치는 기록만). 키 = 정지 시작 (KST, 분 단위)
CAUSES = {
    "2026-09-14 07:01": "컴프레서 에러 — 라인 정지 07:00~07:06 (일지 발포)",
    "2026-10-05 14:11": "1-1 POL IN 호스 터짐 → 교체 14:10~14:20",
    "2026-10-12 22:35": "제품 크러싱 장비 제품 걸림 — 비가동 22:36~23:00",
    "2026-11-03 16:58": "행거 구동부 체인 이탈 (C05, 일지 17:00~18:26)",
    "2026-11-03 17:40": "행거 구동부 체인 이탈 (C05, 위와 같은 정지 — 휴게로 나뉨)",
    "2026-11-03 20:45": "행거 구동부 체인 이탈 재발 (C05, 일지 20:45)",
    "2026-11-12 06:50": "35번 캐리어 탈선 → 하차·슬라이드 교체 06:50~07:03 (C06, 모델 TP)",
    "2026-11-13 08:20": "4번 헤드 유압 OUT 호스 누수 → 교체 08:20~08:33",
    "2026-11-14 12:45": "3번 헤드 POL IN 호스 터짐 → 교체 12:45~13:05 (P05)",
}


def load_day(f):
    d = pd.read_pickle(f)["Auto_Running_PL"].astype(float)
    d = d[~d.index.duplicated()].dropna()
    day = d.index[0].normalize()
    sec = pd.date_range(day, day + pd.Timedelta(days=1), freq="1s", inclusive="left")
    # 결측 마스크: 직전 기록과의 간격이 GAP_SEC 넘는 초
    last = pd.Series(d.index, index=d.index).reindex(sec, method="ffill")
    known = ((sec - last.values) <= pd.Timedelta(seconds=GAP_SEC)) & last.notna().values
    s = d.reindex(d.index.union(sec)).ffill().reindex(sec)
    s[~known] = np.nan
    return day, s


def main():
    files = {Path(f).name: f for f in sorted(glob.glob(str(ROOT / "cache/raw/runflag/*.pkl.xz")))}
    files.update({Path(f).name: f for f in sorted(glob.glob(str(ROOT / "cache/raw/conveyor/*.pkl.xz")))})
    days = {}
    for name in sorted(files):
        day, s = load_day(files[name])
        if day >= DATA_START:
            days[day] = s

    # 분 단위 가동 비율 (결측 분은 NaN)
    mins = pd.DataFrame({d: s.resample("1min").mean().values for d, s in days.items()}).T
    auto_h = mins.sum(axis=1) / 60
    prod = auto_h >= PROD_DAY_MIN_H
    profile = mins[prod].mean()
    planned_min = (profile >= 0.5).values            # 계획가동 분 (1440,)

    planned_mask = []
    rows, events = [], []
    for day, s in days.items():
        m = np.repeat(planned_min, 60)               # 초 단위
        known = s.notna().values
        off = (s.values == 0)
        excl = np.zeros(len(s), bool)
        for a, b, _ in PLANNED:
            a, b = pd.Timestamp(a), pd.Timestamp(b)
            if a.normalize() == day:
                excl |= (s.index >= a) & (s.index < b)
        base = m & known
        r = dict(day=day.date(), dow=day.day_name()[:3], prod_day=bool(prod[day]),
                 period="after" if day >= INSTALL else "before",
                 auto_h=round(auto_h[day], 2),
                 missing_min=round((~known & m).sum() / 60, 1),
                 planned_min=round(base.sum() / 60, 1),
                 planned_stop_min=round((base & excl & off).sum() / 60, 1),
                 down_min=round((base & ~excl & off).sum() / 60, 1))
        rows.append(r)
        if not prod[day]:
            continue
        # 정지 이벤트: 계획가동 안의 연속 꺼짐 (계획정지 제외)
        x = base & ~excl & off
        dx = np.diff(np.r_[0, x.astype(int), 0])
        st, en = np.where(dx == 1)[0], np.where(dx == -1)[0]
        for a, b in zip(st, en):
            events.append(dict(day=day.date(), period=r["period"], start=s.index[a], end=s.index[b - 1] + pd.Timedelta(seconds=1),
                               sec=int(b - a)))
    daily = pd.DataFrame(rows)
    ev = pd.DataFrame(events)
    daily.to_csv(OUT / "downtime_daily.csv", index=False, encoding="utf-8-sig")
    ev.to_csv(OUT / "downtime_events.csv", index=False, encoding="utf-8-sig")

    # 계획가동 창 출력
    w, prev = [], False
    for i, v in enumerate(planned_min):
        if v and not prev: a = i
        if not v and prev: w.append((a, i))
        prev = v
    if prev: w.append((a, 1440))
    summarize(daily, ev)
    print("계획가동 창:", ", ".join(f"{a//60:02d}:{a%60:02d}~{b//60:02d}:{b%60:02d}" for a, b in w),
          f"= {planned_min.sum()}분/일")
    return daily, ev


def summarize(daily, ev):
    p = daily[daily.prod_day]
    ev = ev.copy()
    ev["bucket"] = pd.cut(ev.sec, [0, 60, 300, 900, 3600, 1e9], right=False,
                          labels=["<1분", "1~5분", "5~15분", "15~60분", "≥60분"])
    g = p.groupby("period")
    s = pd.DataFrame({
        "생산일": g.size(),
        "계획가동_h": g.planned_min.sum() / 60,
        "다운타임_min": g.down_min.sum(),
        "일평균_min": g.down_min.mean(),
        "일중앙값_min": g.down_min.median(),
        "1분이상정지_min": ev[ev.sec >= 60].groupby("period").sec.sum() / 60,
        "1분이상정지_건": ev[ev.sec >= 60].groupby("period").size(),
        "계획정지_min": g.planned_stop_min.sum(),
    })
    s["가동률_%"] = 100 * (1 - s["다운타임_min"] / (s["계획가동_h"] * 60))
    s = s.loc[["before", "after"]].T.round(2)
    s.to_csv(OUT / "downtime_summary.csv", encoding="utf-8-sig")
    print(s.to_string())
    major = ev[ev.sec >= 300].copy()
    major["min"] = (major.sec / 60).round(1)
    major["cause"] = major.start.dt.strftime("%Y-%m-%d %H:%M").map(CAUSES).fillna("일지 매칭 없음")
    major[["day", "period", "start", "end", "min", "cause"]].to_csv(
        OUT / "downtime_major.csv", index=False, encoding="utf-8-sig")
    print(major[["start", "min", "cause"]].to_string(index=False))


if __name__ == "__main__":
    main()
