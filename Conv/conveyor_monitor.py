"""[묶기 · 출력] ConveyorMonitor — 백엔드가 tick 마다 read(live_data) 로 부르는 자리.

    mon = ConveyorMonitor(data_dir="./data")         # 반복문 밖에서 한 번
    out = mon.read(live_data, t)                     # tick 마다

tick 하나:  CoreGrid.update  →  UnitMonitor.update × 3 (코어의 위치·태그 값을 빌려 씀)
판정 (EVAL_SEC 마다):  CoreGrid.evaluate  +  UnitMonitor.evaluate × 3  →  화면 단위로 묶어 출력

코어와 유닛은 서로 독립이다. units=[] 로 만들면 코어만 돈다.

## 출력 — 화면에 띄우는 자리 단위

숫자 점수는 내지 않는다. 상태 4단계(0 정상 · 1 관찰 · 2 점검 추천 · 3 경고)와 근거 문구만.
  carrier   지금 주입부에 있는 대차 ("대차번호-15")
  ready     코어가 판정할 만큼 통과가 모였는지
  line      라인 전체 (마커·대차 중 가장 나쁜 것)
  markers   그림 위 마커 — 그 센서·유닛 자체의 문제
              코어 센서 6 (kind=sensor) · 유닛 실린더 3 (kind=unit) · 구동부 (kind=drive)
              unit 은 현장 센서 시트의 묶음 (Zone 진입 센서 · 전진 Unit · 후진 Unit · Air 주입 Unit) + 구동부
  wagons    대차 42대 — 코어 센서가 그 대차에서 본 문제
마커·대차마다 message 는 가장 나쁜 항목 1줄, items 는 전체 근거.
"""

import os
from datetime import datetime
from zoneinfo import ZoneInfo

from core_realtime import CoreGrid, to_kst_naive
from core_spec import REQUIRED_TAGS as CORE_TAGS
from core_spec import SENSORS
from unit_realtime import UnitMonitor
from unit_spec import UNITS, metric_sensor, required_tags

KST = ZoneInfo("Asia/Seoul")
EVAL_SEC = 60
SEV_NAME = ["정상", "관찰", "점검 추천", "경고"]
#: 화면 묶음 (현장 센서 시트의 구분 + 구동부). 마커는 이 순서로 나온다
UNIT_GROUPS = {"zone": "Zone 진입 센서", "md_fwd": "전진 Unit", "md_rev": "후진 Unit", "airbag": "Air 주입 Unit",
               "drive": "구동부"}


def all_required_tags(units=tuple(UNITS)):
    return list(dict.fromkeys(CORE_TAGS + [t for u in units for t in required_tags(u)]))


def _status(sev):
    return dict(sev=sev, status=None if sev is None else SEV_NAME[sev])


class ConveyorMonitor:
    def __init__(self, data_dir=None, units=tuple(UNITS), eval_sec=EVAL_SEC, day_start_hour=6):
        path = (lambda name: os.path.join(data_dir, name)) if data_dir else (lambda name: None)
        self.core = CoreGrid(history_path=path("conveyor_core_history.json"), day_start_hour=day_start_hour)
        self.units = {u: UnitMonitor(u, history_path=path(f"conveyor_unit_{u}_history.json"), day_start_hour=day_start_hour)
                      for u in units}
        self.eval_sec = eval_sec
        self.last_eval = None
        self.result = self._assemble(None)

    def read(self, live_data=None, t=None):
        if live_data:
            self.update(t if t is not None else datetime.now(KST), live_data)
        return self.result

    def update(self, t, row):
        t = to_kst_naive(t)
        self.core.update(t, row)
        for m in self.units.values():
            m.update(t, self.core)
        if self.last_eval is None or (t - self.last_eval).total_seconds() >= self.eval_sec:
            self.evaluate(t)
            self.last_eval = t

    def evaluate(self, t):
        self.core.evaluate(t)
        for m in self.units.values():
            m.evaluate()
        self.result = self._assemble(t)
        return self.result

    # ------------------------------------------------------------------ #
    # 출력
    # ------------------------------------------------------------------ #
    def _sensor_marker(self, s):
        """코어 센서 마커 — 센서 자체 문제(sensor_flags)만. 대차에서 본 문제는 wagons 로 간다."""
        core = self.core.result
        se, unit = core["sensors"][s], SENSORS[s]["unit"]
        items = [dict(flag=f["flag"], action=f["action"]) for f in se["flags"]]
        sev = None if se["sev"] is None else (2 if items else 0)        # None: 이 센서는 아직 판정 전
        return dict(kind="sensor", unit=unit, unit_name=UNIT_GROUPS[unit], name=se["name"], **_status(sev),
                    message=f"{items[0]['flag']}: {items[0]['action']}" if items else "", items=items)

    def _unit_marker(self, u):
        """유닛 실린더 마커 — 지표마다 어느 센서로 잰 값인지 붙인다."""
        r = self.units[u].result
        items = []
        for it in r["items"]:
            bit, label = metric_sensor(u, it["metric"])
            items.append(dict(it, sensor=bit, sensor_name=label))
        return dict(kind="unit", unit=u, unit_name=UNIT_GROUPS[u], name=f"{UNIT_GROUPS[u]} 실린더", **_status(r["sev"]),
                    message=items[0]["reason"] if items else "", basis=r["basis"], day=r["day"],
                    cycles_today=r["cycles_today"], items=items)

    def _drive_marker(self):
        d = self.core.result["drive"]
        return dict(kind="drive", unit="drive", unit_name=UNIT_GROUPS["drive"], name="컨베이어 구동부", **_status(d["sev"]),
                    message=d["reason"] if d["sev"] else "", items=[])

    def _assemble(self, t):
        core = self.core.result
        markers = {}
        for g in UNIT_GROUPS:
            for s, c in SENSORS.items():
                if c["unit"] == g:
                    markers[s] = self._sensor_marker(s)
            if g in self.units:
                markers[g] = self._unit_marker(g)
        markers["drive"] = self._drive_marker()

        wagons = {}
        for k, w in core["wagons"].items():
            items = [dict(sensor=i["sensor"], name=i["name"], unit=i["unit"], unit_name=UNIT_GROUPS[i["unit"]],
                          sev=i["sev"], status=i["status"], state=i["state"], reason=i["reason"], since=i["since"])
                     for i in w["items"]]
            wagons[k] = dict(**_status(w["sev"]), message=f"{items[0]['name']}: {items[0]['reason']}" if items else "",
                             items=items)

        sevs = [x["sev"] for x in list(markers.values()) + list(wagons.values()) if x["sev"] is not None]
        line = max(sevs) if sevs else None
        return dict(carrier=None if core["carrier"] is None else f"대차번호-{core['carrier']:02d}",
                    updated=None if t is None else t.isoformat(timespec="seconds"), ready=core["ready"],
                    line=_status(line), markers=markers, wagons=wagons)
