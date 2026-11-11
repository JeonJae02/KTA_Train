#### 목적

- `uplink` 컨테이너가 Relay 소켓(`system_mode`, `report_data`, `idle_data`)을 구독해 만든 이벤트를 모바일 서버(`HDI_Mobile_Server`)로 보내는 계약
- raw 태그, MSE 점수, 컨베이어 그림용 스냅샷(`idle_data.conveyor`)은 보내지 않음
- `Relay_Data` 탱크·헤드 릴레이 경고는 기본으로 보냄. `UPLINK_INCLUDE_RELAY_WARNINGS=false` 로 끔
- `Conveyor_Anomaly` 컨베이어 경고는 항상 보냄 (스위치 없음)

#### 전송 규칙

- 연결 방향은 **엣지 → 서버** 만 존재 (서버는 엣지에 명령하지 않음)
- 이벤트는 로컬 SQLite outbox 에 먼저 적재하고 `seq` 순으로 최대 100건씩 전송
- 실패 시 1초 → 최대 60초 백오프로 재시도. 서버는 `event_id` 로 중복 제거
- `incident.*` 은 즉시, `warning.*` 은 최대 1초 모아서 전송
- 인증: `Authorization: Bearer <UPLINK_TOKEN>`

---

#### POST /ingest/v1/events

```json
{
  "edge_id": "hdi-line2",
  "events": [
    {
      "event_id": "5b0c...uuid4",
      "edge_id": "hdi-line2",
      "seq": 10231,
      "type": "incident.updated",
      "occurred_at": "2026-09-14T05:24:39.120Z",
      "payload": {}
    }
  ]
}
```

| 필드          | 설명                                                                   |
| ------------- | ---------------------------------------------------------------------- |
| `event_id`    | 전역 고유 ID (uuid4)                                                   |
| `seq`         | outbox 자동 증가 값. 순서 복원용                                       |
| `occurred_at` | uplink 수신 시각 (UTC, `Z`). `report_data.timestamp` 는 tz 가 없어 미사용 |

응답: `{ "accepted": 1, "duplicates": 0, "last_seq": 10231 }`

#### 이벤트 종류

| type                 | 만드는 조건                                                         |
| -------------------- | ------------------------------------------------------------------- |
| `edge.config`        | uplink 시작 시 1회. `shared/topology.json`, `alarm_mapping.json` 본문 |
| `incident.opened`    | 열린 진단이 없을 때 `report_data` 수신 (보통 stage 0 미리보기)         |
| `incident.updated`   | 이후 `report_data` 중 모바일 관련 필드가 바뀐 경우만                  |
| `incident.completed` | `global_state.system_status == "COMPLETED"`                         |
| `incident.aborted`   | 완료 없이 `system_mode: IDLE` / 다른 알람으로 교체 / 10분간 리포트 없음 |
| `alarm.resolved`     | Relay `alarm_episode` 소켓 이벤트 중 `event: "resolved"` — 하위 알람 1→0 확정 |
| `warning.raised`     | `idle_data` 에서 같은 key 가 **2초 이상** 연속 관측                  |
| `warning.updated`    | 활성 경고의 type·message·reason·active 변경. key 당 10초에 1회       |
| `warning.cleared`    | key 가 **5초 이상** 이어진 `idle_data` 에서 계속 빠짐                 |

- 해소는 `idle_data` 수신 기준으로 판정 → ANALYZING 중 `idle_data` 가 멈추면 경고는 동결
- key 형식: `PUMP:{pump}`, `RELAY:{relay}`, `CONV:{conv}` (주소 없는 부위 `CONV:CONVEYOR_MD_FWD:slide_sensor`·대차 묶음 `CONV:P00164:wagons` 은 콜론이 하나 더 있음)

#### incident payload (opened / updated / completed 공통)

```json
{
  "incident_id": "uuid4",
  "alarm_key": "Pump_Trip_P1",
  "bundled_alarms": ["Pump_Trip_P1", "Pump_Trip_P3"],
  "episode_id": "Pump_Trip_P1#1789000000000",
  "stage": 3,
  "current_target": "Pump_Trip_P1",
  "suspected_relay": "P00220",
  "summary_type": "level",
  "loop_count": 1,
  "recovered": false
}
```

| 필드              | 원본                                                     |
| ----------------- | -------------------------------------------------------- |
| `alarm_key`       | `global_state.initial_alarm` (없으면 `bundled_alarms[0]`) |
| `suspected_relay` | `analysis_report.summary.suspected_cause`                |
| `summary_type`    | `analysis_report.summary.type`                           |
| `recovered`       | opened 만. stage 0 이 아닌 상태에서 열리면 true            |

- `"-"`, `""` 자리표시 값은 `null` 로 보냄
- completed 에만 추가: `final_verdict` { `action`, `type`, `recommendation`, `confidence`, `coverage` }, `suspect_relays` (상위 3개)
- aborted: `{ "incident_id", "alarm_key", "reason": "idle" | "replaced" | "stale" }`
- `episode_id`: 이 진단이 속한 알람 발생 1건 (`global_state.episode_id`). 같은 알람이라도 꺼졌다 다시 켜지면 바뀐다. 예전 Relay 는 보내지 않아 `null`

#### alarm payload

진단은 알람 발생(에피소드)당 1회만 돈다. 알람이 켜진 채로 남아 있으면 다시 진단하지 않고,
꺼졌을 때 `alarm.resolved` 한 건이 나간다. 발생은 `incident.opened` 가 알리므로 따로 보내지 않는다.

```json
{
  "episode_id": "Pump_Trip_P1#1789000000000",
  "alarm_key": "Pump_Trip_P1",
  "started_at": "2026-09-10T00:26:40.000Z",
  "resolved_at": "2026-09-10T00:27:22.500Z",
  "duration_sec": 42.5,
  "resolved_during_run": true,
  "diagnosed": true
}
```

| 필드                  | 설명                                                                  |
| --------------------- | --------------------------------------------------------------------- |
| `alarm_key`           | 하위 알람 이름. incident 의 `alarm_key` 와 같은 공간                  |
| `resolved_at`         | 처음 0 을 관측한 시각 (확정은 2틱 뒤지만 실제로 꺼진 건 이때)           |
| `resolved_during_run` | 진단이 도는 도중(알람 묶기·진단 단계)에 꺼졌는지                       |
| `diagnosed`           | 이 발생에 대해 진단이 돌았는지. 부팅 때 이미 켜져 있던 알람은 `false` |

- 서버는 같은 `alarm_key` 의 해소 전 incident(이 이벤트보다 먼저 열린 것)를 해소 처리하고 고장 카드를 내린다 (`fault_remove`)
- 진단받지 못한 알람의 해소도 보낸다. 서버에 남아 있던 같은 알람의 옛 카드를 정리하는 근거가 된다

#### warning payload

펌프 경고(`source: "PUMP"`, `idle_data.Pump_Anomaly`)

```json
{
  "warning_id": "uuid4",
  "key": "PUMP:P1",
  "source": "PUMP",
  "pump": "P1",
  "device": "Pump",
  "type": "pressure",
  "message": "토출 배관·노즐 저항 증가 · 같은 유량 대비 압력 상승",
  "first_seen": "2026-10-08T04:12:03.120Z",
  "reason": "pump_line_resistance_high",
  "active": true,
  "last_detected": "2026-10-08T04:12:05.120Z",
  "abnormal_sec": 2.0
}
```

릴레이 경고(`source: "RELAY"`, `idle_data.Relay_Data`)는 `pump`·`device` 대신 `relay` 를 갖는다

```json
{
  "warning_id": "uuid4",
  "key": "RELAY:P00270",
  "source": "RELAY",
  "relay": "P00270",
  "type": "thermal",
  "message": "히터 가열 응답 지연 · 설정 온도 도달 시간 증가 추세",
  "first_seen": "2026-10-08T04:12:03.120Z",
  "reason": "heater",
  "active": true,
  "last_detected": "2026-10-08T04:12:05.120Z",
  "abnormal_sec": 2.0
}
```

컨베이어 경고(`source: "CONVEYOR"`, `idle_data.Conveyor_Anomaly`)는 경고 1건이 부위 1개다. key 는 `conv`(부위 키)이고,
부위는 주소가 있으면 `relay`(릴레이 경고처럼), 없으면 `device`·`group`(펌프 경고의 `pump`·`device` 처럼)으로 온다.
이름은 보내지 않는다 — 서버가 `edge.config` 의 topology(`relays[relay].partName`, `devices[].name`)에서 찾는다.

```json
{
  "warning_id": "uuid4",
  "key": "CONV:P00474",
  "source": "CONVEYOR",
  "conv": "P00474",
  "type": "timing",
  "message": "스톱퍼 전진 시간 느려짐 → 실린더·공압(레귤레이터·필터)·솔밸브 점검",
  "first_seen": "2026-10-21T08:24:18.000Z",
  "reason": "stop_fwd_time_slow",
  "active": true,
  "last_detected": "2026-10-21T09:00:00.000Z",
  "abnormal_sec": 2142.0,
  "relay": "P00474",
  "sev": 2,
  "since": "10-21"
}
```

주소 없는 부위는 `relay` 대신 `"device": "CONVEYOR_MD_REV", "group": "slide_sensor"` (key `CONV:CONVEYOR_MD_REV:slide_sensor`).

| 필드        | 설명                                                                                          |
| ----------- | --------------------------------------------------------------------------------------------- |
| `conv`      | 부위 키. 주소가 있으면 주소, 없으면 `{device}:{group}`, 대차 묶음은 `{센서}:wagons`. 전체 목록은 `shared/응답 예시/idle_data.md` "컨베이어 부위 전체 목록" |
| `type`      | `sensor` 코어 센서 자체 · `timing` 실린더 동작 시간 · `distance` 이동대 거리 · `drive` 구동부 · `wagon` 그 센서가 대차에서 본 문제 (센서·상태별로 대차를 묶어 1건) |
| `relay`     | 부위 주소 (실린더는 그 SOL, 대차 묶음은 그 센서). topology `relays` 키                          |
| `device`, `group` | `relay` 가 없을 때만. 이동대 센서 묶음 `CONVEYOR_{MD_FWD·MD_REV·AIRBAG}` / `slide_sensor`, 구동부 `CONVEYOR` / `drive` |
| `sev`       | 2 점검 추천 · 3 경고. 2 미만(관찰)은 키오스크에만 그리고 보내지 않음                              |
| `since`     | 모델이 본 그 상태의 시작 가동일 (MM-DD, 06:00 경계). `first_seen`(백엔드가 처음 띄운 시각)과 다름. 센서·구동부 경고는 `null` |

- 11-04 변경: 예전에는 유닛 하나가 key 하나(`CONV:md_rev`)였고 `unit_name`·`name` 을 실었다. 이제 부위 단위(실린더·이동대 센서 묶음)로 나뉘어 한 유닛에서 여러 건이 동시에 뜰 수 있고, `unit_name`·`name` 은 오지 않는다. 구동부 key 는 `CONV:drive` → `CONV:CONVEYOR:drive`

- `message` 에는 숫자를 넣지 않는다 (값이 움직일 때마다 updated 가 나가지 않게). 대차 묶음은 대차 번호가 들어가서 대차가 늘거나 줄면 updated 가 나간다
- 판정은 60초마다 바뀐다. 마지막 감지 뒤 90초(판정 2번) 지나야 `active: false`
- 코어 센서는 백엔드 재시작 뒤 1–2시간 판정이 없다. 그동안 떠 있던 경고는 재시작 전 상태 그대로 유지된다 (해소로 보내지 않음)
- 서버가 `CONVEYOR` 를 모르면 raised 는 검증 실패로 투영만 빠지고 배치는 정상 처리된다. 서버에 반영한 뒤 uplink 를 한 번 재시작해야 이미 떠 있던 경고가 다시 raised 로 들어간다

- updated: `{ "warning_id", "type", "message", "reason", "active", "last_detected", "abnormal_sec" }`
- cleared: `{ "warning_id", "key", "duration_sec", "last_detected", "abnormal_sec" }` — `duration_sec` 는 `idle_data` 에 떠 있던 기간(uplink 관측)

아래 유지 필드는 펌프·릴레이·컨베이어 공통이다. 백엔드는 경고를 발생 후 처음 시작하는 근무가 끝날 때까지 유지하므로,
경고 1건(raised~cleared) 안에서 실제 이상이 이어진 구간은 따로 표시한다. 예전 Relay 는 이 필드를 보내지 않아 빠질 수 있음

| 필드            | 실리는 이벤트             | 설명                                                                     |
| --------------- | ------------------------- | ------------------------------------------------------------------------ |
| `reason`        | raised, updated           | 사유 키. 펌프 `pump_{규칙}`(`pump_line_resistance_high`·`pump_flowmeter_fail` 등, `pump_diagnose.RULES`), 탱크 `heater`·`exchanger`·`cooler`·`exchanger_degrade`, 헤드 `injection_time`, 컨베이어는 아래 표 |
| `active`        | raised, updated           | 지금도 이상이면 `true`. `false` 는 이상은 해소됐지만 키오스크에 아직 표시 중 |
| `last_detected` | raised, updated, cleared  | 마지막으로 이상이 성립한 시각. `active: false` 면 해소 시각                |
| `abnormal_sec`  | raised, updated, cleared  | 실제 이상 지속(초) = `last_detected` − `first_seen`. `idle_data` 의 `duration_sec` |

- updated 는 `type`·`message`·`reason`·`active` 가 바뀔 때만 보냄. `last_detected`·`abnormal_sec` 는 이상이 이어지는 동안 계속 바뀌므로 updated 를 일으키지 않고, 보낼 때 그 순간 값을 같이 실음
- cleared 의 `last_detected`·`abnormal_sec` 는 마지막 관측값. 경고 1건의 최종 이상 지속은 이 값을 쓴다
- `idle_data` 의 `hold_until` 은 보내지 않음 (해소는 cleared 가 알림)

컨베이어 `reason` (`tools_yeji/conveyor_models/conveyor_realtime.py`). 한 conv 에 사유가 겹치면 sev 가 높은 것 하나

| type     | reason                                                                                                   |
| -------- | -------------------------------------------------------------------------------------------------------- |
| `sensor`   | `sensor_rearm` 재감지 · `sensor_shift` 이동 · `sensor_dirty` 감도·면 오염 · `sensor_jitter` 흔들림          |
| `timing`   | `{지표}_slow`. 지표: `stop_fwd_start`·`stop_fwd_time`·`stop_rev_start`·`stop_rev_time`(스톱퍼 실린더) · `nozzle_fwd_start`·`nozzle_rev_time`(노즐 실린더, 에어 주입 유닛은 `_1`·`_2`) · `return_time`(로드레스 실린더) · `air_time`(이동대 끌림 — 주소 없이 `device`·`group: slide_sensor`). 가장 긴 것 `nozzle_fwd_start_1_slow` 23자 |
| `distance` | `catch_cnt_distance` 걸림 거리 · `drag_cnt_distance` 끌림 거리 (이동대 센서 묶음)                           |
| `drive`    | `drive_speed` 속도 비율이 기준에서 벗어남                                                                  |
| `wagon`    | `wagon_rearm` 이물(분리) · `wagon_debris_front` 이물(앞) · `wagon_debris_back` 이물(뒤) · `wagon_debris` 이물(기타) |

빨라짐(`_fast`)·대차 위치 변화(`wagon_shift`)·흔들림(`wagon_jitter`)은 관찰(sev 1)이라 키오스크에만 그리고 보내지 않는다.

---

#### POST /ingest/v1/heartbeat

outbox 를 거치지 않고 15초마다 직접 보냄. 실패하면 버림

```json
{
  "edge_id": "hdi-line2",
  "sent_at": "2026-09-14T05:24:45.000Z",
  "system_mode": "IDLE",
  "open_incident_id": null,
  "active_warning_keys": ["PUMP:I1", "CONV:P00474"],
  "outbox_backlog": 0,
  "last_relay_msg_at": "2026-09-14T05:24:44.980Z"
}
```

- 서버는 45초간 heartbeat 가 없으면 `engineConnected: false`
- 서버는 `active_warning_keys` 에 없는 활성 경고를 정리 (uplink 재시작·누락 보정)
- `outbox_backlog == 0` 이고 `open_incident_id == null` 이면 남은 ANALYZING 진단을 중단 처리
