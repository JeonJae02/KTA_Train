"""[묶기 · 출력] ConveyorMonitor — 백엔드가 tick 마다 read(live_data) 로 부르는 자리.

    mon = ConveyorMonitor(data_dir="./data")         # 반복문 밖에서 한 번
    out = mon.read(live_data, t)                     # tick 마다

tick 하나:  CoreGrid.update  →  UnitMonitor.update × 3 (코어의 위치·태그 값을 빌려 씀)
판정 (EVAL_SEC 마다):  CoreGrid.evaluate  +  UnitMonitor.evaluate × 3  →  스테이션으로 묶어 출력

코어와 유닛은 서로 독립이다. units=[] 로 만들면 코어만 돈다.

## 출력

숫자 점수는 내지 않는다. 상태 4단계(0 정상 · 1 관찰 · 2 점검 추천 · 3 경고)와 근거 문구만.
  carrier   지금 주입부에 있는 대차 ("대차번호-15")
  ready     코어가 판정할 만큼 통과가 모였는지
  line      라인 전체 (가장 나쁜 스테이션) + drive (구동부: 속도 비율)
  stations  주입부 · 클로징 존 · 오프닝 존 · 전진 유닛 · 후진 유닛 · 에어백 유닛
            각 스테이션 = 코어 센서 결과(sensor_flags · wagons) + 유닛 결과(unit: 실린더 지표)
  wagons    대차 42대 상태와 걸린 센서
"""

import os
from datetime import datetime
from zoneinfo import ZoneInfo

from core_realtime import CoreGrid, to_kst_naive
from core_spec import REQUIRED_TAGS as CORE_TAGS
from core_spec import SENSORS
from unit_realtime import UnitMonitor
from unit_spec import UNITS, required_tags

KST = ZoneInfo("Asia/Seoul")
EVAL_SEC = 60
SEV_NAME = ["정상", "관찰", "점검 추천", "경고"]
#: 화면 스테이션: (id, 이름, 유닛 키)
STATIONS = [("pour", "주입부", None), ("closing", "클로징 존", None), ("opening", "오프닝 존", None),
            ("fwd", "전진 유닛", "md_fwd"), ("rev", "후진 유닛", "md_rev"), ("airbag", "에어백 유닛", "airbag")]


def all_required_tags(units=tuple(UNITS)):
    return list(dict.fromkeys(CORE_TAGS + [t for u in units for t in required_tags(u)]))


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

    def _assemble(self, t):
        core = self.core.result
        stations = {}
        for sid, name, unit in STATIONS:
            bits = [s for s, c in SENSORS.items() if c["station"] == sid]
            flags = [f for s in bits for f in core["sensors"][s]["flags"]]
            wagons = sorted([c for s in bits for c in core["sensors"][s]["cells"]], key=lambda x: (-x["sev"], x["wagon"]))
            sevs = [core["sensors"][s]["sev"] for s in bits if core["sensors"][s]["sev"] is not None]
            u = self.units[unit].result if unit in self.units else None
            if u is not None and u["sev"] is not None:
                sevs.append(u["sev"])
            sev = max(sevs) if sevs else None
            stations[sid] = dict(name=name, sensors=bits, sev=sev, status=None if sev is None else SEV_NAME[sev],
                                 sensor_flags=flags, wagons=wagons, unit=u)
        wagons = {}
        for k, w in core["wagons"].items():
            wagons[k] = dict(sev=w["sev"], status=None if w["sev"] is None else SEV_NAME[w["sev"]], stations=w["items"])
        drive = core["drive"]
        sevs = [s["sev"] for s in stations.values() if s["sev"] is not None] + ([drive["sev"]] if drive["sev"] is not None else [])
        line = max(sevs) if sevs else None
        return dict(carrier=None if core["carrier"] is None else f"대차번호-{core['carrier']:02d}",
                    updated=None if t is None else t.isoformat(timespec="seconds"), ready=core["ready"],
                    line=dict(sev=line, status=None if line is None else SEV_NAME[line], drive=drive),
                    stations=stations, wagons=wagons)
