#### 목적

- 0.5초 주기로 수신
- 예지 화면에 필요한 **현재 활성 경고 목록** + 화면에 표시할 **컨베이어 상태**만 전달

#### 수신 규칙

- 진단이 시작되면(`system_mode` = **ANALYZING** 이 되면) **idle_data**는 멈춤
- `Relay_Data`, `Pump_Anomaly`, `Conveyor_Anomaly`는 변경분(diff)이 아니라 **현재 활성 경고 전체를 매번 전달**

---

#### JSON 형식

```json
{
  "system_mode": "IDLE",
  "Relay_Data": [
    // 탱크, 헤드 경고. 경고는 발생 후 처음 시작하는 근무가 끝날 때까지 유지(shared/operation.json)
    // { "time": 처음 띄운 시각, "relay" : 릴레이 주소, "type" : 경고 유형, "message" : 경고 사유,
    //   "reason": 사유 키, "active": 지금도 이상인지, "last_detected": 마지막 감지 시각,
    //   "duration_sec": 이상 지속 시간, "hold_until": 화면에서 내려가는 시각 }
    {
      "time": "2026-10-08T04:12:03.120Z",
      "relay": "P00270",
      "type": "thermal",
      "message": "히터 가열 응답 지연 · 설정 온도 도달 시간 증가 추세",
      "reason": "heater",
      "active": true,
      "last_detected": "2026-10-08T04:40:11.520Z",
      "duration_sec": 1688.4,
      "hold_until": "2026-10-08T06:25:00.000Z"
    },
    {
      "time": "2026-10-08T01:05:40.000Z",
      "relay": "P00279",
      "type": "timing",
      "message": "헤드1 P1 인젝션 (로드) 작동 시간이 길어지고 있습니다.",
      "reason": "injection_time",
      "active": false,
      "last_detected": "2026-10-08T01:21:10.500Z",
      "duration_sec": 930.5,
      "hold_until": "2026-10-08T04:30:00.000Z"
    }
  ],
  "Pump_Anomaly": [
    // 펌프 경고
    // { "time": 발생 시각, "pump" : 펌프 ID, "device": 고장 설비, "type" : 경고 유형, "message" : 경고 사유 }
    // device는 예시라서 topology 따르면 됨
    {
      "time": "2026-07-29 06:44:24",
      "pump": "I1",
      "device": "Pump",
      "type": "Trip",
      "message": "펌프 정지"
    },
    {
      "time": "2026-07-29 06:44:24",
      "pump": "I2",
      "device": "Inverter",
      "type": "OverCurrent",
      "message": "과전류"
    },
    {
      "time": "2026-07-29 06:44:24",
      "pump": "I3",
      "device": "Motor",
      "type": "OverVoltage",
      "message": "과전압"
    }
  ],
  "Conveyor_Anomaly": [
    // 컨베이어 경고 (점검 추천 이상). Relay_Data 와 같은 근무 단위 유지 규칙
    // 키오스크 경고 카드·uplink(모바일)·경고 이력이 같이 쓴다. 경고 1건 = 부위 1개
    // 부위는 주소가 있으면 relay (Relay_Data 처럼), 없으면 device·group (Pump_Anomaly 의 pump·device 처럼)
    {
      "time": "2026-10-21T08:24:18.000Z",
      "conv": "P00474",
      "type": "timing",
      "message": "스톱퍼 전진 시간 느려짐 → 실린더·공압(레귤레이터·필터)·솔밸브 점검",
      "reason": "stop_fwd_time_slow",
      "active": true,
      "last_detected": "2026-10-21T09:00:00.000Z",
      "duration_sec": 2142.0,
      "hold_until": "2026-10-21T13:00:00.000Z",
      // 여기부터 컨베이어만 갖는 필드
      "relay": "P00474",
      "sev": 2,
      "since": "10-21"
    },
    {
      "time": "2026-10-21T06:10:00.000Z",
      "conv": "CONVEYOR_MD_FWD:slide_sensor",
      "type": "distance",
      "message": "끌림 거리 거리 변화 → 정위치·끝단 센서 위치 또는 이동대 조정 이력 확인",
      "reason": "drag_cnt_distance",
      "active": true,
      "last_detected": "2026-10-21T09:00:00.000Z",
      "duration_sec": 10200.0,
      "hold_until": "2026-10-21T13:00:00.000Z",
      "device": "CONVEYOR_MD_FWD",
      "group": "slide_sensor",
      "sev": 2,
      "since": "10-21"
    },
    {
      "time": "2026-10-20T22:43:58.000Z",
      "conv": "P00164:wagons",
      "type": "wagon",
      "message": "대차 20: 한 대차에서 다시 켜짐 → 표식 앞 떨어진 이물 또는 채터링 (Opening Zone In Detect 감지)",
      "reason": "wagon_rearm",
      "active": true,
      "last_detected": "2026-10-21T09:00:00.000Z",
      "duration_sec": 36962.0,
      "hold_until": "2026-10-21T13:00:00.000Z",
      "relay": "P00164",
      "sev": 2,
      "since": "10-20"
    }
  ],
  "conveyor": {
    // 그림 색칠용. 판정 그대로(근무 유지 없음). 0(정상)은 보내지 않는다 — 없으면 정상
    "carrier": "대차번호-23",
    "updated": "2026-10-21T18:00:00",
    "ready": true,
    "parts": { "P00474": 2, "CONVEYOR_MD_FWD:slide_sensor": 2, "P0028C": 1 },
    "wagons": { "20": { "sev": 2, "message": "한 대차에서 다시 켜짐 → 표식 앞 떨어진 이물 또는 채터링 (Opening Zone In Detect 감지)" } }
  }
}
```

| 필드                    | 타입   | 설명                                          |
| ----------------------- | ------ | --------------------------------------------- |
| `system_mode`           | string | 항상 **IDLE**                                 |
| `Relay_Data`            | array  | 현재 경고 설비 목록(없으면 **[]**, 최대 22건) |
| `Relay_Data[].relay`    | string | 경고가 난 위치(릴레이 주소)                   |
| `Pump_Anomaly[].pump`   | string | 경고 발생한 펌프 ID                           |
| `Pump_Anomaly[].device` | string | 펌프 상세 고장 설비                           |

| `Relay_Data[].time`,
`Pump_Anomaly[].time` | string | 경고가 발생한 시각 (UTC 형식 필요) |
| `Relay_Data[].type`,
`Pump_Anomaly[].type` | string | 경고 유형. 아이콘 선택에 사용 |
| `Relay_Data[].message`,
`Pump_Anomaly[].message` | string | 경고 사유 1줄(설비 단위 표시 문구) |
| `Relay_Data[].reason` | string | 사유 키. 탱크 `heater`·`exchanger`·`cooler`·`exchanger_degrade`, 헤드 `injection_time`. 한 릴레이에 사유가 겹치면 우선순위가 높은 것 하나 (`type`·`message` 도 그 사유 기준) |
| `Relay_Data[].active` | boolean | 이번 판정에서도 이상 조건이 성립하면 **true**. **false** 는 이상은 해소됐지만 `hold_until` 까지 표시를 유지하는 중 |
| `Relay_Data[].last_detected` | string | 마지막으로 이상이 성립한 시각 (UTC). `active` 가 true 면 거의 지금 |
| `Relay_Data[].duration_sec` | number | 이상 지속 시간(초) = `last_detected` − `time`. 백엔드가 계산하므로 클라이언트 시계와 무관 |
| `Relay_Data[].hold_until` | string | 이 시각에 목록에서 빠짐 (UTC). 발생 후 처음 시작하는 근무의 끝. 이상이 계속되면 새 근무가 시작될 때마다 늦춰짐 |
| `Conveyor_Anomaly` | array | 컨베이어 활성 경고 (없으면 **[]**). `time`·`message`·`reason`·`active`·`last_detected`·`duration_sec`·`hold_until` 은 `Relay_Data` 와 같은 뜻 |
| `Conveyor_Anomaly[].conv` | string | 부위 키. 주소가 있으면 주소(`P00474`), 없으면 `{device}:{group}`(`CONVEYOR_MD_FWD:slide_sensor`·`CONVEYOR:drive`), 대차 묶음은 `{센서}:wagons`. 아래 "컨베이어 부위 전체 목록" |
| `Conveyor_Anomaly[].type` | string | `sensor` 코어 센서 자체 · `timing` 실린더 동작 시간 · `distance` 이동대 거리 · `drive` 구동부 · `wagon` 대차 |
| `Conveyor_Anomaly[].relay` | string | 부위 주소. 있으면 `shared/topology.json` `relays` 에서 설비·부품명을 찾는다 (Relay_Data 와 같음). 대차 묶음은 그 센서 주소 |
| `Conveyor_Anomaly[].device`, `group` | string | `relay` 가 없을 때만. topology device ID·group ID (이동대 센서 묶음·구동부) |
| `Conveyor_Anomaly[].sev` | number | 2 점검 추천 · 3 경고 |
| `Conveyor_Anomaly[].since` | string | 모델이 본 그 상태의 시작 가동일 (MM-DD). 센서·구동부 경고는 `null` |
| `conveyor` | object | 컨베이어 그림용 상태. 경고 목록으로 알 수 없는 것만 |
| `conveyor.carrier` | string | 지금 주입부에 있는 대차 (`대차번호-23`). 대차번호 태그를 아직 못 받았으면 `null` |
| `conveyor.updated` | string | 마지막 판정 시각 (KST). 기동 직후 판정 전이면 `null` |
| `conveyor.ready` | boolean | 코어가 판정할 만큼 통과가 모였는지 |
| `conveyor.parts` | object | {부위 키: 수준}. **0 이 아닌 것만** — 키가 없으면 정상. 1 관찰 · 2 점검 추천 · 3 경고 · `null` 판정 전 |
| `conveyor.wagons` | object | {`"01"`~`"42"`: {`sev`, `message`}}. **관찰 이상인 대차만**. 1 위치 변화·흔들림 · 2 이물 |

---

#### 컨베이어 부위 전체 목록

판정은 `tools_yeji/conveyor_models` (지표 → 부위는 `unit_params.METRIC_PART`). 설비·group 은 `shared/topology.json`.

| 무엇을 그리나 | 어디서 읽나 | 값 |
| ------------- | ----------- | -- |
| 점·영역 색 | `conveyor.parts[키]` | 키가 없으면 정상 · 1 관찰 · 2·3 위험 · `null` 판정 전 |
| 경고 카드 | `Conveyor_Anomaly[]` | 1건 = 카드 1장. `relay` 있으면 topology `relays[relay]`, 없으면 `device`·`group` |
| 대차 색·hover | `conveyor.wagons` | `{sev, message}` |

`parts` 에는 아래 19개 키만 온다.

**컨베이어 벨트 (`CONVEYOR`)**

| 키 | 화면 이름 | group | 그림 값 | 경고 type | 경고 reason |
| -- | --------- | ----- | ------- | --------- | ----------- |
| `P00140` | 캐리어 진입 Check | carrier_sensor | null · 2 | sensor | `sensor_rearm` · `sensor_shift` · `sensor_dirty` · `sensor_jitter` |
| `P00141` | 캐리어진입 쉬프트 Check | carrier_sensor | null · 2 | sensor | 〃 |
| `P00154` | Closing Zone In Detect | carrier_sensor | null · 2 | sensor | 〃 |
| `P00164` | Opening Zone In Detect | carrier_sensor | null · 2 | sensor | 〃 |
| `CONVEYOR:drive` | 컨베이어 구동부 ※ | drive | null · 3 | drive | `drive_speed` |

**전진 유닛 (`CONVEYOR_MD_FWD`) / 후진 유닛 (`CONVEYOR_MD_REV`)**

| 키 (전진 / 후진) | 화면 이름 | group | 그림 값 | 경고 type | 경고 reason |
| ---------------- | --------- | ----- | ------- | --------- | ----------- |
| `P00451` / `P00459` | 캐리어 진입감지 | carrier_sensor | null · 2 | sensor | `sensor_*` (위와 같음) |
| `P00470` / `P00474` | 스톱퍼 실린더 | stopper_rod | null · 1 · 2 | timing | `stop_fwd_start_slow` · `stop_fwd_time_slow` · `stop_rev_start_slow` · `stop_rev_time_slow` |
| `P00471` / `P00475` | 노즐 실린더 | nozzle_rod | null · 1 · 2 | timing | `nozzle_fwd_start_slow` · `nozzle_rev_time_slow` |
| `P00472` / `P00476` | 로드레스 실린더 | slide_rod | null · 1 · 2 | timing | `return_time_slow` |
| `CONVEYOR_MD_FWD:slide_sensor` / `CONVEYOR_MD_REV:slide_sensor` | 이동대 센서 (정위치·후진) ※ | slide_sensor | null · 1 · 2 | distance · timing | `catch_cnt_distance` · `drag_cnt_distance` · `air_time_slow` (에어 주입 시간 = 이동대 끌림) |

**에어 주입 유닛 (`CONVEYOR_AIRBAG`)** — 진입감지 없음 (P00130 은 실물이 없고 PLC 가 클로징 신호 타이머로 대신함)

| 키 | 화면 이름 | group | 그림 값 | 경고 type | 경고 reason |
| -- | --------- | ----- | ------- | --------- | ----------- |
| `P0028A` | 스톱퍼 실린더 | stopper_rod | null · 1 · 2 | timing | 전진·후진 유닛과 같음 |
| `P0028B` | 노즐 실린더 (노즐 1·2 공용) | nozzle_rod | null · 1 · 2 | timing | `nozzle_fwd_start_1_slow` · `_2_slow` · `nozzle_rev_time_1_slow` · `_2_slow` (message 에 "(노즐 1)" 처럼 표시) |
| `P0028C` | 로드레스 실린더 | slide_rod | null · 1 · 2 | timing | `return_time_slow` |
| `CONVEYOR_AIRBAG:slide_sensor` | 이동대 센서 (정위치·후진) ※ | slide_sensor | null · 1 · 2 | distance · timing | `catch_cnt_distance` · `drag_cnt_distance` · `air_time_slow` |

- ※ 주소가 없는 부위. `relays` 에 없어 화면 이름은 topology `groupNames` 에 둔다 (프론트·모바일 서버가 같이 읽는다)
- 실린더 3종의 topology 부품명은 SOL(`전진Unit 스톱퍼 SOL`)이다. 문제는 실린더 전체(실린더·공압·SOL)라 화면 제목은 위 이름을 권장
- 에어 주입 시간은 주입 SOL(`P00473`·`P00477`·`P00243`)이 아니라 이동대 묶음으로 온다. 주입 SOL 은 PLC 가 켜고(노즐 후진감지 OFF + 0.5초) 끄므로(끝단 후진 도착) 그 시간은 밸브 응답이 아니라 이동대가 끌려간 시간이다
- 실린더 동작 시간은 SOL 에 붙인다 — 헤드 인젝션 시간 경고(`Relay_Data`, type `timing`)와 같은 방식
- 그림 값 1(관찰) = 실린더가 빨라짐 또는 장기 기준보다 서서히 느려짐. 경고 카드는 생기지 않음

**대차 묶음 — 경고 목록에만 온다 (`parts` 에는 없음)**

| 키 | relay | type | reason |
| -- | ----- | ---- | ------ |
| `P00140:wagons` · `P00141:wagons` · `P00154:wagons` · `P00164:wagons` · `P00451:wagons` · `P00459:wagons` | 그 대차를 본 센서 주소 | wagon | `wagon_rearm` · `wagon_debris_front` · `wagon_debris_back` · `wagon_debris` |

**topology 에는 있지만 값이 오지 않는 주소 (24개)**

| 유닛 | 주소 | 그림에서 |
| ---- | ---- | -------- |
| 전진 | 정위치 `P00452` · 후진 `P00453` · 비상 `P00454` | 이동대 센서(`slide_sensor` 키) 색을 따름 |
| | 스톱퍼CY 전진 `P00455` · 후진 `P00456` | 스톱퍼 실린더 색을 따름 |
| | Air주입CY 후진 `P00457` | 노즐 실린더 색을 따름 |
| | 1번감지 `P00450` | 판정 안 함 |
| | 주입 SOL `P00473` (후진 `P00477`, 에어 주입 `P00243`) | 에어 주입 시간은 이동대 묶음으로 감 |
| 후진 | `P0045A` · `P0045B` · `P0045C` / `P0045D` · `P0045E` / `P0045F` / `P00458` | 전진과 같음 |
| 에어 주입 | `P00131` · `P00132` · `P00133` / `P00134` · `P00135` / `P00136` · `P00137` | 전진과 같음 (1번감지 없음) |

센서와 실린더를 가르는 귀속 규칙이 붙으면 스톱퍼·노즐 감지와 정위치감지가 키로 올라올 수 있다. 같은 유닛 영역 안에서 센서만 짚으면 되게 자리를 남겨 둔다.

**유닛 그림 영역 ↔ 키 (전진 유닛 기준)**

| 영역 (현장 사진 `kta 자료_센서위치.pdf` 2·3쪽) | 영역 색을 정하는 키 | 영역 안 센서 (값 없음) |
| ---------------------------------------------- | ------------------- | ---------------------- |
| 진입감지 | `P00451` | — |
| 스톱퍼 CY | `P00470` | `P00455` · `P00456` |
| Air 주입 CY | `P00471` | `P00457` |
| 이동대 레일 | `CONVEYOR_MD_FWD:slide_sensor`, `P00472` | `P00452` · `P00453` · `P00454` |

- 유닛 점 하나의 색 = 그 유닛 키 중 가장 나쁜 값
- 라인 점: 존 진입 = `P00140`·`P00141` 중 나쁜 값, 클로징 = `P00154`, 오프닝 = `P00164`
