"""컨베이어 실시간 진단 — 코어(대차 통과 센서 6개) + 유닛 3곳(전진·후진·Air 주입 Unit 실린더).
tools_yeji/Conveyor_Monitor 가 하나 갖고 매 tick 부른다.

    mon = ConveyorRealtimeGroup(data_dir="./data")   # 한 번 (가동일 기록 읽기)
    print(mon.missing_tags(live_data.keys()))        # 처음 한 번 — 빠진 감시 태그
    alerts = mon.update(t, live_data)                # 매 tick — 점검 추천 이상 경보 목록
    snap   = mon.snapshot()                          # 키오스크 그림용 상태 (아래 "출력")
    judged = mon.judged()                            # 지금 판정이 있는 부위 (재시작 직후 코어 센서는 빠짐)

tick 하나:  CoreRealtime.update  →  UnitRealtime.update × 3 (코어의 위치·태그 값을 빌려 씀)
판정 (EVAL_SEC 마다):  CoreRealtime.evaluate  +  UnitRealtime.evaluate × 3  →  부위로 묶고 경보를 만든다
**얼마나 오래 띄울지**(근무 단위 유지)는 Conveyor_Monitor 가 정한다.

코어와 유닛은 서로 독립이다. units=[] 로 만들면 코어만 돈다.

## 부위 — 판정이 뜨는 자리. shared/topology.json 의 device·group 체계를 따른다

키는 주소가 있으면 주소(topology relays 키), 없으면 `{device}:{group}`.
  코어 센서 6   센서 주소 — 센서 자체의 문제. 대차에서 본 문제는 wagons 로 간다
  유닛          UnitRealtime 항목의 relay (실린더 SOL). 거리·에어 주입 시간 지표는 `{device}:slide_sensor`
  구동부        `CONVEYOR:drive`

## 경보 — {conv, type, relay | device·group, reason, sev, message, since}

sev ≥ WARN_SEV 인 것만. conv 는 부위 키이고 uplink 키(CONV:{conv})가 된다.
  대차는 센서·상태별로 묶어 `{센서}:wagons` 하나 — 센서 하나가 나쁘면 대차 수십 대에 같이 떠서,
  대차마다 내면 할 일이 수십 장 생긴다.
  대차 묶음의 relay 는 그 대차를 본 센서다(문제는 대차 표식). 키오스크는 이 경고를 카드로 띄우지 않고 그림의 대차 카드로
  보여 주며, 모바일·경고 이력용이다. 문구는 "대차 02·03: 사유 (센서 감지)"
주소가 있으면 relay 만 (이름·device·group 은 topology 에서), 없으면 device·group (펌프 경고의 pump·device 처럼).
  type  sensor 코어 센서 자체 · timing 동작 시간(실린더, 에어 주입 = 이동대 끌림) · distance 이동대 거리 · drive 구동부 · wagon 대차

## 출력 (스냅샷) — 그림 색칠용. 경보 목록으로 알 수 없는 것만

숫자 점수는 내지 않는다. 상태 4단계(0 정상 · 1 관찰 · 2 점검 추천 · 3 경고)만, 0 은 보내지 않는다 (없으면 정상).
  carrier   지금 주입부에 있는 대차 ("대차번호-15")
  updated   마지막 판정 시각
  ready     코어가 판정할 만큼 통과가 모였는지
  parts     {부위 키: 수준} — 0 이 아닌 것만. null = 판정 전 (재시작 직후 코어 센서, 기록 없는 유닛)
  wagons    {"05": {sev, message}} — 관찰 이상인 대차만. 코어 센서가 그 대차에서 본 문제, message 는 가장 나쁜 1줄
"""

import os

from .core_judge import FLAG_REASON, WAGON_REASON
from .core_params import REQUIRED_TAGS as CORE_TAGS
from .core_realtime import CoreRealtime, to_kst_naive
from .unit_judge import STATE_REASON, metric_kind
from .unit_params import SLIDE_GROUP, UNITS, metric_columns, metric_part, required_tags
from .unit_realtime import UnitRealtime

EVAL_SEC = 60
#: 이 단계 이상만 경보로 낸다 (모바일 할 일이 된다). 관찰(1)은 키오스크 그림에만
WARN_SEV = 2
#: 구동부 (topology device, group). 주소가 없다
DRIVE = ("CONVEYOR", "drive")


def all_required_tags(units=tuple(UNITS)):
    return list(dict.fromkeys(CORE_TAGS + [t for u in units for t in required_tags(u)]))


def part_key(device, group, relay):
    """부위 키. 주소가 있으면 주소, 없으면 `{device}:{group}`."""
    return relay or f"{device}:{group}"


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
    def _assemble(self, t):
        """판정 → 부위별 수준(0·판정 전 포함 전체) + 경보 + 대차. 스냅샷에는 0 을 뺀 것만 낸다."""
        core = self.core.result
        levels, alerts = {}, []
        self._core_sensors(core["sensors"], levels, alerts)
        for u, m in self.units.items():
            self._unit(u, m.result, levels, alerts)
        self._drive(core["drive"], levels, alerts)
        wagons = _wagons(core["wagons"], alerts)

        self.alerts = alerts
        # 정상(0)인 부위도 판정이 있는 것 — 근무 유지가 해소를 판단하려면 0 까지 봐야 한다
        self._judged = {k for k, v in levels.items() if v is not None}
        self._judged |= {f"{s}:wagons" for s, se in core["sensors"].items() if se["sev"] is not None}
        return dict(carrier=None if core["carrier"] is None else f"대차번호-{core['carrier']:02d}",
                    updated=None if t is None else t.isoformat(timespec="seconds"), ready=core["ready"],
                    parts={k: v for k, v in levels.items() if v != 0}, wagons=wagons)

    @staticmethod
    def _core_sensors(sensors, levels, alerts):
        """코어 센서 자체 문제(센서 플래그)만. 센서 플래그가 있으면 점검 추천, se["sev"] None 은 아직 판정 전."""
        for s, se in sensors.items():
            levels[s] = None if se["sev"] is None else (WARN_SEV if se["flags"] else 0)
            for f in se["flags"]:
                alerts.append(dict(conv=s, type="sensor", relay=s, reason=FLAG_REASON.get(f["flag"], "sensor"),
                                   sev=WARN_SEV, message=f"{f['flag']}: {f['action']}", since=None))

    @staticmethod
    def _unit(u, r, levels, alerts):
        """유닛 지표 → 부위. 항목의 relay 가 부위이고, relay 가 없으면(거리 지표) 그 유닛의 이동대 센서 묶음."""
        device = UNITS[u]["device"]
        for k in dict.fromkeys(part_key(*metric_part(u, c)) for c in metric_columns(u)):
            levels[k] = None if r["sev"] is None else 0
        for it in r["items"]:
            k = part_key(device, SLIDE_GROUP, it["relay"])
            levels[k] = max(levels[k], it["sev"])
            if it["sev"] >= WARN_SEV:
                where = dict(relay=it["relay"]) if it["relay"] else dict(device=device, group=SLIDE_GROUP)
                alerts.append(dict(conv=k, type="timing" if metric_kind(it["metric"]) == "s" else "distance", **where,
                                   reason=f"{it['metric']}_{STATE_REASON.get(it['state'], 'trend')}",
                                   sev=it["sev"], message=it["reason"], since=it["since"]))

    @staticmethod
    def _drive(d, levels, alerts):
        k = part_key(*DRIVE, None)
        levels[k] = d["sev"]
        if d["sev"] is not None and d["sev"] >= WARN_SEV:
            alerts.append(dict(conv=k, type="drive", device=DRIVE[0], group=DRIVE[1], reason="drive_speed",
                               sev=d["sev"], message=d["reason"], since=None))


def _wagons(wagons, alerts):
    """대차 42대 → 관찰 이상인 대차만 {sev, message}. 점검 추천 이상은 센서·상태별로 묶어 경보 1건 (머리말 "경보").

    문구는 사유를 앞에, 본 센서는 뒤 괄호에 — 문제는 대차 표식이라 센서 이름으로 시작하면 센서 고장처럼 읽힌다
    """
    out, groups = {}, {}
    for wk, w in sorted(wagons.items()):
        if w["sev"]:
            it = w["items"][0]
            out[wk] = dict(sev=w["sev"], message=f"{it['reason']} ({it['name']} 감지)")
        for it in w["items"]:
            if it["sev"] >= WARN_SEV:
                groups.setdefault((it["sensor"], it["state"]), []).append((wk, it))
    for (sensor, state), rows in groups.items():
        it = max((i for _, i in rows), key=lambda i: i["sev"])
        alerts.append(dict(conv=f"{sensor}:wagons", type="wagon", relay=sensor,
                           reason=WAGON_REASON.get(state, "wagon"), sev=it["sev"],
                           message=f"대차 {'·'.join(wk for wk, _ in rows)}: {it['reason']} ({it['name']} 감지)",
                           since=min(i["since"] for _, i in rows)))
    return out
