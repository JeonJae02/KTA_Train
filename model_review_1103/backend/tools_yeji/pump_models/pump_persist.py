# tools_yeji/pump_models/pump_persist.py
"""지속·반복 필터 — "며칠 연속, 하루에 얼마나 오래" 뜬 사유만 통과시킨다.

진단 규칙(pump_diagnose.py 의 RULES·evidence)이 바뀌어도 이 필터는 그대로 쓴다. 사유 코드는 이름일 뿐이고,
필터는 그 뜻을 모른다 — (설비, 사유) 마다 시간만 센다.

    pf = PersistFilter()                                     # 프로세스 시작 때 하나
    confirmed = pf.update(unit, codes, t)                    # 판정 대상 사이클마다 (정상이면 codes = set())
    pf.streak(unit, code)                                    # 오늘까지 연속 생산일 수

## 규칙 (2026-11-23, 한 달 재생 10-15 ~ 11-21 로 정함)

- 운영일: 06:00 ~ 다음날 06:00 (`DAY_START_HOUR`)
- 그날 그 사유가 **누적 `min_minutes`분 이상** 떠 있었으면 그 사유의 "뜬 날"
  - 시간 = 사이클 사이 간격을 앞 사이클의 사유에 붙인다 (`max_gap_sec` 초까지 — 정지·공백은 안 센다)
  - 사이클 수가 아니라 시간으로 세는 이유: 하루 사이클이 펌프마다 390 ~ 3200 개로 달라 같은 개수가 다른 뜻이 된다
  - 15 ~ 30분이 같은 결과 (평탄 구간) → 15분
- **생산일 `days`일 연속** 뜬 날이면 확정. 오늘이 뜬 날이 되는 순간부터 그날 끝까지 확정
  - 생산일 = 그날 판정 사이클 `min_cycles`개 이상(또는 그 사유가 뜬 날). 생산 안 한 날(일요일 등)은 끊지도 세지도 않는다
- `bypass` 사유는 연속 판단 없이 바로 확정 (예: 유량계 고장 — 3일 기다릴 일이 아니다)
- 우선순위는 여기서 보지 않는다 — 모든 사유를 따로 센다. 고르는 건 띄우는 쪽(pump_diagnose.PumpDiagnoser.alerts) 일

## 재시작

연속 판단은 며칠치 기억이 필요하다. `state_path` 를 주면 `save_every_sec` 마다·운영일이 바뀔 때 JSON 으로 저장하고,
시작할 때 읽는다 (Pump_Monitor 의 data_dir → `pump_persist_history.json`, 컨베이어 history 와 같은 자리).
없으면 처음 `days`일은 아무것도 확정되지 않는다.
"""

import json
import os

import pandas as pd

DAY_START_HOUR = 6
KEEP_DAYS = 21              # 설비마다 들고 있는 운영일 기록 수


def op_day(t, start_hour=DAY_START_HOUR):
    """시각 → 운영일 'YYYY-MM-DD' (06시 이전은 전날)."""
    return (pd.Timestamp(t) - pd.Timedelta(hours=start_hour)).strftime("%Y-%m-%d")


class PersistFilter:
    def __init__(self, min_minutes=15.0, days=3, min_cycles=100, max_gap_sec=300.0, bypass=(),
                 day_start_hour=DAY_START_HOUR, state_path=None, save_every_sec=300.0):
        self.min_sec = float(min_minutes) * 60
        self.days = int(days)
        self.min_cycles = int(min_cycles)
        self.max_gap = float(max_gap_sec)
        self.bypass = set(bypass)
        self.start_hour = day_start_hour
        self.state_path = state_path
        self.save_every = save_every_sec
        #: unit -> {"hist": {운영일: {"n": 사이클 수, "sec": {사유: 누적 초}}}, "last_t": 시각, "last_codes": [사유]}
        self.units = {}
        self._saved_at = None
        if state_path and os.path.exists(state_path):
            self.load(state_path)

    # ------------------------------------------------------------------ #
    def update(self, unit, codes, t):
        """판정 대상 사이클 하나. codes: 이 사이클에 뜬 사유 전부 (우선순위 무관, 없으면 빈 집합).
        반환: 지금 확정된 사유 집합."""
        t = pd.Timestamp(t)
        u = self.units.setdefault(unit, {"hist": {}, "last_t": None, "last_codes": []})
        od = op_day(t, self.start_hour)
        if u["last_t"] is not None:
            dt = (t - u["last_t"]).total_seconds()
            if 0 < dt:
                day = self._day(u, op_day(u["last_t"], self.start_hour))
                for c in u["last_codes"]:
                    day["sec"][c] = day["sec"].get(c, 0.0) + min(dt, self.max_gap)
        new_day = od not in u["hist"]
        self._day(u, od)["n"] += 1
        u["last_t"], u["last_codes"] = t, sorted(set(codes))
        if new_day:
            for k in sorted(u["hist"])[:-KEEP_DAYS]:
                del u["hist"][k]
        self._autosave(t, force=new_day)
        return self.confirmed(unit, od)

    def confirmed(self, unit, od=None):
        """운영일 od (없으면 마지막 사이클의 운영일) 에 확정된 사유."""
        u = self.units.get(unit)
        if not u or u["last_t"] is None:
            return set()
        od = od or op_day(u["last_t"], self.start_hour)
        today = u["hist"].get(od, {"sec": {}})
        out = {c for c in self.bypass if c in u["last_codes"] or today["sec"].get(c, 0) > 0}
        for c in today["sec"]:
            if c not in out and self.streak(unit, c, od) >= self.days:
                out.add(c)
        return out

    def streak(self, unit, code, od=None):
        """od 까지 그 사유가 뜬 생산일이 몇 일 연속인지 (od 가 뜬 날이 아니면 0)."""
        u = self.units.get(unit)
        if not u or u["last_t"] is None:
            return 0
        od = od or op_day(u["last_t"], self.start_hour)
        n = 0
        for d in sorted((k for k in u["hist"] if k <= od), reverse=True):
            day = u["hist"][d]
            hit = day["sec"].get(code, 0) >= self.min_sec
            if d == od:
                if not hit:
                    return 0
                n += 1
            elif hit:
                n += 1
            elif day["n"] >= self.min_cycles:
                break                                       # 생산했는데 안 뜬 날 — 끊긴다
            # 생산 안 한 날 — 건너뛴다
        return n

    def minutes(self, unit, od=None):
        """운영일 od 의 사유별 누적 분 (확인용)."""
        u = self.units.get(unit) or {"hist": {}, "last_t": None}
        od = od or (op_day(u["last_t"], self.start_hour) if u["last_t"] is not None else None)
        return {c: round(s / 60, 1) for c, s in u["hist"].get(od, {"sec": {}})["sec"].items()}

    @staticmethod
    def _day(u, od):
        return u["hist"].setdefault(od, {"n": 0, "sec": {}})

    # ------------------------------------------------------------------ #
    def to_dict(self):
        return {"units": {k: {"hist": v["hist"], "last_t": None if v["last_t"] is None else v["last_t"].isoformat(),
                              "last_codes": v["last_codes"]} for k, v in self.units.items()}}

    def from_dict(self, d):
        self.units = {k: {"hist": v["hist"], "last_t": None if v["last_t"] is None else pd.Timestamp(v["last_t"]),
                          "last_codes": list(v["last_codes"])} for k, v in d.get("units", {}).items()}

    def save(self, path=None):
        path = path or self.state_path
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, ensure_ascii=False)
        os.replace(tmp, path)

    def load(self, path=None):
        with open(path or self.state_path, encoding="utf-8") as f:
            self.from_dict(json.load(f))

    def _autosave(self, t, force=False):
        if not self.state_path:
            return
        if force or self._saved_at is None or (t - self._saved_at).total_seconds() >= self.save_every:
            self.save()
            self._saved_at = t
