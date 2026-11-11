#### 목적

- 알람 감지부터 진단 완료까지 이어지는 이벤트
- WARNING 모달 구간(초반) + 보전 4단계(후반)를 같은 이벤트 흐름에서 전달

#### 이벤트 흐름

알람은 **하위 알람**을 직접 감시해 잡는다. 상위 알람(`AL_*` 12개)을 먼저 받고 하위로
내려가던 드릴다운은 없어졌고, 단계 이름만 예전 순서대로 유지한다.

| 순서 | `system_mode` 이벤트 | `report_data` | 비고 |
| ---- | -------------------- | ------------- | ---- |
| 1 | `IDLE` | - | 평시 |
| 2 | `GATHERING_ALARMS` | - | **머물지 않음(0틱).** 순서 호환용으로 알리기만 하고 곧바로 3 |
| 3 | `DRILL_DOWN` | `system_mode: "DRILL_DOWN"`, `stage: 0` | 하위 알람을 3틱 동안 묶음. 새 알람이 잡힐 때마다 갱신. 첫 건은 알람이 잡힌 틱에 바로 전송 |
| 4 | `ANALYZING` | `system_mode: "ANALYZING"`, `stage: 1~4` | 보전 4단계 |
| 5 | `IDLE` | - | 진단 종료 |

- 같은 알람이 켜진 채로 있으면 다시 진단하지 않는다. 꺼졌다가 다시 켜져야 새로 진단한다
- `bundled_alarms`에는 이번에 새로 뜬 알람만 들어간다. 이미 진단한 뒤 켜진 채로 남은 알람은 빠진다

#### 수신 규칙

- 대시보드 프론트는 **`report_data.system_mode`만 본다.** `system_mode` 이벤트는 구독하지 않는다
  - `"DRILL_DOWN"` → WARNING 모달, `"ANALYZING"` → 보전 4단계, 그 외 → 숨김
- `GATHERING_ALARMS`는 `report_data`에 실리지 않는다 (`system_mode` 이벤트로만 나감)
- `stage: 0`은 프론트 검증 범위(1~4) 밖이라 stage만 떼고 받는다. 화면은 `system_mode`로 정해진다
- system_mode: **ANALYZING**에는 **stage: 1 | 2 | 3 | 4**를 전달
- 한 번 채워진 앞 단계 데이터는 이후 단계에서도 유지 (화면 전환 안정)
- uplink 는 `system_mode` 이벤트 중 `IDLE`에만 반응한다 (열린 incident 중단 처리)
- `global_state.episode_id`: 이 진단이 속한 알람 발생 1건. stage 0 부터 실린다. 없으면 키 자체가 없다

#### alarm_episode 이벤트

하위 알람 하나가 켜지거나(`opened`) 꺼진(`resolved`) 순간 나간다. 꺼짐은 2틱 연속 0 을 본 뒤 확정한다.

```json
{
  "event": "resolved",
  "episode_id": "Conv_Trip#1790578124129",
  "alarm": "Conv_Trip",
  "started_at": 1790578124.129,
  "resolved_at": 1790578166.5,
  "duration_sec": 42.4,
  "resolved": true,
  "resolved_during_run": true,
  "edge_observed": true,
  "runs": 1,
  "active_alarms": ["Tension_Trip"]
}
```

- `active_alarms`: 이 시점에 켜져 있는 하위 알람 전체. 받는 쪽은 상태를 따로 들지 않고 "내 묶음이 전부 꺼졌나" 를 판단한다
- 대시보드: 현재 창의 `bundled_alarms` 가 `active_alarms` 에 하나도 없으면 창을 내린다
- uplink: `resolved` 를 `alarm.resolved` 로 바꿔 모바일 서버에 보낸다 (`shared/uplink_events.md`)
- `edge_observed: false` 는 부팅 때 이미 켜져 있던 알람이다. 알람 이전 구간이 없어 진단하지 않는다

---

#### JSON 형식

```json
{
  "system_mode": "ANALYZING",
  "stage": 4,

  "global_state": {
    "bundled_alarms": [
      "Pump_Trip_P1",
      "Pump_Trip_P3",
      "Pump_Trip_I3",
      "Pump_Trip_I1"
    ],
    "current_target": "Pump_Trip_P1",
    "loop_count": 1,
    "max_loops": 3,
    "system_status": "ANALYZING"
  },

  "analysis_report": {
    "suspectDevices": [
      { "rank": 1, "relay": "P00220" },
      { "rank": 2, "relay": "P00227" },
      { "rank": 3, "relay": "P00324" }
    ],
    "summary": {
      "suspected_cause": "P00220",
      "type": "level"
    },
    "final_verdict": {
      "action": "CONCLUDE",
      "recommendation": "사용자에게 띄우는 문제 해결 안내 리포트"
    }
  }
}
```

| 필드                               | 타입     | 설명                                                  |
| ---------------------------------- | -------- | ----------------------------------------------------- |
| `system_mode`                      | string   | **DRILL_DOWN** (stage 0) / **ANALYZING** (stage 1~4)  |
| `stage`                            | number?  | **ANALYZING**에서 1~4                                 |
| `global_state`                     | object   | global state                                          |
| `global_state.bundled_alarms`      | string[] | stage 0, stage 1                                      |
| `global_state.current_target`      | string   | stage 2                                               |
| `analysis_report`                  | object   |                                                       |
| `analysis_report.suspectDevices[]` | array    | stage 2 · 의심 릴레이 후보군                          |
| `analysis_report.summary`          | object   | stage 3 · 릴레이 특정                                 |
| `analysis_report.final_verdict`    | object   | stage 4 · 최종 결론                                   |

#### global_state

| 필드             | 타입     | 설명                                  |
| ---------------- | -------- | ------------------------------------- |
| `bundled_alarms` | string[] | 알람 발생 시 발생된 알람 리스트       |
| `current_target` | string   | 현재 탐색을 하고자 하는 alarm         |
| `loop_count`     | number   | 현재 진단 사이클을 몇 번 돌았는지     |
| `max_loops`      | number   | 최대 루프 수                          |
| `system_status`  | string   | **ANALYZING** / **COMPLETED**         |

#### analysis_report

| 필드                           | 타입   | 설명                                                |
| ------------------------------ | ------ | --------------------------------------------------- |
| `suspectDevices[].rank`        | number | 의심 디바이스 후보 순위                             |
| `suspectDevices[].relay`       | string | 의심 릴레이 (FE에서는 릴레이로 디바이스 찾기)       |
| `summary.suspected_cause`      | string | 릴레이 특정                                         |
| `summary.type`                 | string | 위험 유형                                           |
| `final_verdict.action`         | string | **CONCLUDE** / **PIVOT_ALARM** / **PIVOT_DEVICE**   |
| `final_verdict.recommendation` | string | 해결 방법 / 권고 문구                               |

`final_verdict.action` 값별 의미

| 값               | 의미                                             |
| ---------------- | ------------------------------------------------ |
| **CONCLUDE**     | 명확한 원인 파악됨 / **종료**                    |
| **PIVOT_ALARM**  | 알람 꺼짐 or 근본 원인 아님 / **stage 2로 back** |
| **PIVOT_DEVICE** | 릴레이 선택 잘못됨 / **stage 3로 back**          |

---

### 메모

- **suspected_cause**는 릴레이 물리 주소 → **topology.relays**에서 설비/그룹/표시명 조회
- suspectDevices는 릴레이만 전달하고 상세는 topology.devices로 조회
