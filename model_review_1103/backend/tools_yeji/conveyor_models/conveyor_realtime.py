"""컨베이어 실시간 진단 — 코어(대차 통과 센서 6개) + 유닛 3곳(전진·후진·Air 주입 Unit 실린더).
tools_yeji/Conveyor_Monitor 가 하나 갖고 매 tick 부른다.

    mon = ConveyorRealtimeGroup(data_dir="./data")   # 한 번 (가동일 기록 읽기)
    print(mon.missing_tags(live_data.keys()))        # 처음 한 번 — 빠진 감시 태그
    alerts = mon.update(t, live_data)                # 매 tick — 점검 추천 이상 경보 목록
    snap   = mon.snapshot()                          # 키오스크 그림용 전체 상태 (아래 "출력")
    judged = mon.judged()                            # 지금 판정이 있는 conv (재시작 직후 코어 센서는 빠짐)

tick 하나:  CoreRealtime.update  →  UnitRealtime.update × 3 (코어의 위치·태그 값을 빌려 씀)
판정 (EVAL_SEC 마다):  CoreRealtime.evaluate  +  UnitRealtime.evaluate × 3  →  화면 단위로 묶고 경보를 만든다
**얼마나 오래 띄울지**(근무 단위 유지)는 Conveyor_Monitor 가 정한다.

코어와 유닛은 서로 독립이다. units=[] 로 만들면 코어만 돈다.

## 경보 — {conv, reason, type, unit, unit_name, name, sev, message, since}

sev ≥ WARN_SEV 인 것만. conv 가 uplink 키(CONV:{conv})가 된다.
  마커는 마커 하나가 conv. 대차는 센서·상태별로 묶어 `{센서}:wagons` 하나 —
  센서 하나가 나쁘면 대차 수십 대에 같이 떠서, 대차마다 내면 할 일이 수십 장 생긴다.

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

from .core_judge import FLAG_REASON, WAGON_REASON
from .core_params import REQUIRED_TAGS as CORE_TAGS
from .core_params import SENSORS
from .core_realtime import CoreRealtime, to_kst_naive
from .unit_judge import STATE_REASON
from .unit_params import UNITS, metric_sensor, required_tags
from .unit_realtime import UnitRealtime

EVAL_SEC = 60
SEV_NAME = ["정상", "관찰", "점검 추천", "경고"]
#: 이 단계 이상만 경보로 낸다 (모바일 할 일이 된다). 관찰(1)은 키오스크 그림에만
WARN_SEV = 2
#: 화면 묶음 (현장 센서 시트의 구분 + 구동부). 마커는 이 순서로 나온다
UNIT_GROUPS = {"zone": "Zone 진입 센서", "md_fwd": "전진 Unit", "md_rev": "후진 Unit", "airbag": "Air 주입 Unit",
               "drive": "구동부"}


def all_required_tags(units=tuple(UNITS)):
    return list(dict.fromkeys(CORE_TAGS + [t for u in units for t in required_tags(u)]))


def _status(sev):
    return dict(sev=sev, status=None if sev is None else SEV_NAME[sev])


class ConveyorRealtimeGroup:
    def __init__(self, data_dir=None, units=tuple(UNITS), eval_sec=EVAL_SEC, day_start_hour=6):
        path = (lambda name: os.path.join(data_dir, name)) if data_dir else (lambda name: None)
        self.core = CoreRealtime(history_path=path("conveyor_core_history.json"), day_start_hour=day_start_hour)
        self.units = {u: UnitRealtime(u, history_path=path(f"conveyor_unit_{u}_history.json"), day_start_hour=day_start_hour)
                      for u in units}
        self.eval_sec = eval_sec
        self.last_eval = None
        self.result = self._assemble(None)

    def missing_tags(self, keys):
        keys = set(keys)
        return [t for t in all_required_tags(tuple(self.units)) if t not in keys]

    def update(self, t, row):
        """row: {주소/태그명: 원시 PLC 값}. 빈 row(수신 없음)는 판정을 건드리지 않는다. 반환은 이번 판정의 경보 목록."""
        if row:
            t = to_kst_naive(t)
            self.core.update(t, row)
            for m in self.units.values():
                m.update(t, self.core)
            if self.last_eval is None or (t - self.last_eval).total_seconds() >= self.eval_sec:
                self.evaluate(t)
                self.last_eval = t
        return self.alerts

    def snapshot(self):
        return self.result

    def judged(self):
        return self._judged

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
        self.alerts = _alerts(markers, wagons)
        self._judged = _judged(markers)
        return dict(carrier=None if core["carrier"] is None else f"대차번호-{core['carrier']:02d}",
                    updated=None if t is None else t.isoformat(timespec="seconds"), ready=core["ready"],
                    line=_status(line), markers=markers, wagons=wagons)


def _alerts(markers, wagons):
    """화면 마커·대차 → sev ≥ WARN_SEV 경보 (머리말 "경보")."""
    alerts = []
    for key, m in markers.items():
        if m["sev"] is None or m["sev"] < WARN_SEV:
            continue
        meta = dict(conv=key, type=m["kind"], unit=m["unit"], unit_name=m["unit_name"], name=m["name"])
        if m["kind"] == "sensor":
            for it in m["items"]:
                alerts.append(dict(meta, reason=FLAG_REASON.get(it["flag"], "sensor"), sev=m["sev"],
                                   message=f"{it['flag']}: {it['action']}", since=None))
        elif m["kind"] == "unit":
            for it in m["items"]:
                if it["sev"] >= WARN_SEV:
                    alerts.append(dict(meta, reason=f"{it['metric']}_{STATE_REASON.get(it['state'], 'trend')}",
                                       sev=it["sev"], message=it["reason"], since=it["since"]))
        else:
            alerts.append(dict(meta, reason="drive_speed", sev=m["sev"], message=m["message"], since=None))

    groups = {}
    for wk, w in sorted(wagons.items()):
        for it in w["items"]:
            if it["sev"] >= WARN_SEV:
                groups.setdefault((it["sensor"], it["state"]), []).append((wk, it))
    for (sensor, state), rows in groups.items():
        it = max((i for _, i in rows), key=lambda i: i["sev"])
        alerts.append(dict(conv=f"{sensor}:wagons", type="wagon", unit=it["unit"], unit_name=it["unit_name"],
                           name=it["name"], reason=WAGON_REASON.get(state, "wagon"), sev=it["sev"],
                           message=f"{it['name']} 대차 {'·'.join(wk for wk, _ in rows)}: {it['reason']}",
                           since=min(i["since"] for _, i in rows)))
    return alerts


def _judged(markers):
    """지금 판정이 있는 conv. 대차 묶음은 그 센서를 따른다."""
    judged = {k for k, m in markers.items() if m["sev"] is not None}
    judged |= {f"{k}:wagons" for k, m in markers.items() if m["kind"] == "sensor" and m["sev"] is not None}
    return judged
