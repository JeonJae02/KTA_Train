# 컨베이어 모델 → HDI_Relay 이식 가이드

```bash
python Conv/relay_port/make_port.py      # 이식 묶음 생성 → Conv/relay_port/out/backend/  (+ out/required_tags.txt)
python Conv/relay_port/verify_port.py    # 묶음 코드로 과거 데이터 재생 → 배치(conv_02 · conv_03) 와 비교
# → out/backend/ 를 HDI_Relay/backend/ 위에 그대로 복사 + 아래 "백엔드 수정" 1곳
```

범위: **코어**(대차 통과 근접 센서 6개 + 구동부) + **유닛**(에어 주입 유닛 3곳의 실린더). 리프트는 다음에.

---

## 1. 구조 — 코어와 유닛은 따로 돈다

```
 Conv/ (DB_Log)                               역할                         배포 위치 (tools_yeji/conveyor_models/)
 ├ core_spec.py      [코어 · 데이터]   센서 6개 태그·센서 간 거리·장기 기준          core_spec.py
 ├ core_rules.py     [코어 · 규칙]     칸 판정·시간 규칙·센서 단위 판정              core_rules.py
 ├ core_realtime.py  [코어 · 실시간]   CoreGrid: 체인 위치·센서 통과·대차 번호       core_realtime.py
 ├ unit_spec.py      [유닛 · 데이터]   유닛 3곳 태그·지표 정의·유닛별 기준값         unit_spec.py
 ├ unit_rules.py     [유닛 · 규칙]     일별 평균 판정 (느려짐·빨라짐·거리 변화)       unit_rules.py
 ├ unit_realtime.py  [유닛 · 실시간]   UnitMonitor: 사이클·지표 (유닛 3곳 같은 클래스) unit_realtime.py
 ├ conveyor_monitor.py [묶기·출력]     코어 → 유닛 순으로 돌리고 스테이션으로 묶음    conveyor_monitor.py
 │
 ├ conv_grid.py · conv_cyl.py          배치 전처리 (노트북용, 같은 spec·rules 를 씀)
 └ conv_02 · conv_03 노트북           분석·검증
```

```
 PLC → live_data (매 tick) ─→ PredictiveHealth.generate_idle_json
                               └─ ConveyorMonitor.read(live_data) ──→ "conveyor"
                                    ├ CoreGrid.update           카운터 6개 → 체인 위치, 카운터 리셋 → 센서 통과·재감지,
                                    │                           원본 비트 → 켜짐 길이, 통과마다 대차 번호
                                    ├ UnitMonitor.update × 3    P 비트 변화 → 사이클 → 지표 (코어의 위치·태그 값을 빌림)
                                    └ 60초마다 evaluate         코어: 최근 20번 통과 칸 + 기록 → core_rules.judge
                                                                유닛: 오늘(2,000 사이클 이상) 또는 전날 평균 + 기록 → unit_rules.cyl_judge
                                    오전 6시: 각자 하루치를 기록에 더하고 저장
```

- **코어는 유닛을 모른다.** `ConveyorMonitor(units=())` 로 만들면 코어만 돈다. 유닛에서 문제가 생겨도 코어 판정은 그대로다.
- **유닛 3곳은 같은 클래스**다. PLC 로직이 같고 다른 건 태그 주소·트리거·노즐 센서 수·에어 주입 최대 시간(10초/3초)·에어백의 5초 타임아웃뿐이다 (`unit_spec.py` 머리말 표). 정상값(원점복귀 0.34 · 0.73 · 0.61초 등)은 유닛별 기준(`UNIT_REF`)으로 따로 둔다.
- 규칙을 고치면 `make_port.py` 를 다시 돌린다. 배치 노트북도 같은 파일을 쓰므로 둘이 갈라지지 않는다.

## 2. 백엔드 수정

**① 묶음 복사** — `out/backend/` 를 `HDI_Relay/backend/` 위에 복사한다.

| 파일 | 내용 |
|---|---|
| `tools_yeji/Conveyor_Health.py` | `ConveyorMonitor` — MOCK 교체. 이름·생성자·`read(live_data)` 는 그대로라 `Predictive_Health.py` 는 안 고친다 |
| `tools_yeji/conveyor_models/*.py` | 위 표의 7개 |
| `data/conveyor_core_history.json` | 코어 처음 기록 (09-10 – 10-19, 31 가동일 × 252칸) |
| `data/conveyor_unit_{md_fwd,md_rev,airbag}_history.json` | 유닛 처음 기록 (31 가동일 일별 평균) |

기록 파일은 운영 중 매일 오전 6시에 갱신된다 — **배포 후 덮어쓰지 말 것.**

**② `config/tag_config.py` 의 `MONIORING_TAGS` 에 태그 46개 추가** (목록: `out/required_tags.txt`). 전부 PREDICTIVE 블록 안이라 이미 수신 중이다.

```python
    # 컨베이어 모델(tools_yeji/conveyor_models) 입력. 전부 PREDICTIVE 블록 안이라 이미 수신 중이다.
    # [코어] 존 카운터 · 대차 통과 센서 비트 · 대차 번호 · 운전 · 속도 지령
    "R00030", "R00040", "R00055", "R00060", "R00070", "R00080",
    "P00140", "P00141", "P00154", "P00164", "P00451", "P00459",
    "Now_Actual_Wagon_Num", "Conv_Run_Out", "Hz_In_Conv",
    # [유닛 전진] SOL 4 · 정위치 · 비상 · 스톱퍼 전진/후진 · 노즐 후진 · 끝단 후진 래치
    "P00470", "P00471", "P00472", "P00473", "P00452", "P00454", "P00455", "P00456", "P00457", "Air_Supply2_Rev_PX_CHK",
    # [유닛 후진]
    "P00474", "P00475", "P00476", "P00477", "P0045A", "P0045C", "P0045D", "P0045E", "P0045F", "Air_Supply3_Rev_PX_CHK",
    # [유닛 에어백] 노즐 후진감지가 2개 (P00136 · P00137)
    "P0028A", "P0028B", "P0028C", "P00243", "P00131", "P00133", "P00134", "P00135", "P00136", "P00137", "Air_Supply_Rev_PX_CHK",
```

빠진 태그가 있으면 처음 tick 에 `⚠️ [ConveyorMonitor] 감시 태그 없음` 이 찍힌다.

## 3. 출력 — `conveyor` (프론트 계약)

**점수 숫자는 내지 않는다.** 상태 4단계와 근거 문구만. `sev` 는 단계 번호(0 정상 · 1 관찰 · 2 점검 추천 · 3 경고)로 색을 정할 때 쓴다.

```jsonc
{
  "carrier": "대차번호-15",          // 지금 주입부(P00140)에 있는 대차 — 예전 MOCK 과 같은 키
  "updated": "2026-10-19T23:59:59",
  "ready": true,                      // false: 코어가 아직 판정할 만큼 통과가 안 모임 (기동 직후 약 1시간)
  "line": { "sev": 2, "status": "점검 추천",
            "drive": { "sev": 0, "status": "정상", "reason": "정상" } },   // 구동부: 속도 비율
  "stations": {
    "pour":    { "name": "주입부", "sensors": ["P00140","P00141"], "sev": 0, "status": "정상",
                 "sensor_flags": [], "wagons": [], "unit": null },
    "closing": { ... }, "opening": { ... },
    "fwd": { "name": "전진 유닛", "sensors": ["P00451"], "sev": 2, "status": "점검 추천",
             "sensor_flags": [ { "sensor": "P00451", "name": "전진 진입", "flag": "센서 재감지",
                                 "action": "대차 여러 대에서 표식 앞까지 감지 → 센서 감지 거리·위치 확인" } ],
             "wagons": [],                    // 센서 재감지가 켜진 센서의 대차 경고는 센서 경고 하나로 묶는다
             // 예) 오프닝: "wagons": [ { "wagon": 20, "sensor": "P00164", "sev": 2, "status": "점검 추천", "state": "이물(분리)",
             //                         "reason": "한 대차에서 다시 켜짐 (통과당 0.7번) → ...", "since": "09-10" } ]
             "unit": { "name": "전진 유닛", "ready": true, "sev": 0, "status": "정상", "basis": "오늘", "day": "10-19",
                       "cycles_today": 4473, "items": [] } },
    "rev": { ..., "unit": { "sev": 1, "status": "관찰", "basis": "오늘",
                            "items": [ { "metric": "stop_fwd_time", "sev": 1, "status": "관찰", "state": "정상",
                                         "trend": "장기 기준보다 느림",
                                         "reason": "스톱퍼 전진 시간 0.292초 (기준 0.280초) → 추세 관찰 (서서히 느려지는 중)",
                                         "since": "10-19" }, ... ] } },
    "airbag": { "name": "에어백 유닛", "sensors": [], ..., "unit": { ... } }
  },
  "wagons": { "20": { "sev": 2, "status": "점검 추천",
                      "stations": [ { "station": "opening", "sensor": "P00164", "state": "이물(분리)", "reason": "...", "since": "09-10" } ] },
              "21": { "sev": 0, "status": "정상", "stations": [] }, ... }
}
```

| 필드 | 값 |
|---|---|
| 스테이션 `sev` | 코어 센서 결과(`wagons`, `sensor_flags`)와 유닛 결과(`unit`) 중 가장 나쁜 것 |
| `wagons[].state` (코어) | `이물(분리)` 한 대차에서 다시 켜짐 · `이물(앞)` · `이물(뒤)` · `이물(기타)` · `위치 변화` (3 가동일 유지되면 승인) · `흔들림` |
| `sensor_flags[].flag` (코어) | `센서 재감지` · `센서 이동` · `센서 감도·면 오염` · `센서 흔들림` — 대차가 아니라 센서 쪽 |
| 센서 재감지 | 그 센서에서 다시 켜지는 대차가 10대 이상. 켜져 있는 동안 그 센서의 대차 경고는 `wagons` 에 내지 않는다 (센서를 고치면 같이 고쳐질 가능성이 커서). 꺼지면 다시 대차 위에 뜬다 |
| `unit.items[]` (유닛) | 지표별: `state` 느려짐(점검 추천) · 빨라짐(관찰) · 거리 변화(점검 추천), `trend` 장기 기준보다 느림(관찰) |
| `unit.basis` | `오늘` (오늘 사이클 2,000개 이상) / `전날까지` (아직 덜 모임) — 실린더는 하루 평균으로 판정하기 때문 |
| `since` | 그 상태가 시작된 가동일 (MM-DD) |

## 4. 검증 (verify_port.py, 10-12 – 10-17 재생)

| 항목 | 결과 |
|---|---|
| 재생 | tick 669,393, 66 µs/tick (코어 + 유닛 3) |
| 코어: 칸 상태 vs 배치 | **1,260칸 100% 일치** |
| 유닛: 하루 평균 지표 vs 배치 | 최대 차이 0.0011초 (전진·후진), 0 (에어백). 사이클 수 차이 ≤ 1 (자정 경계) |
| 백엔드 래퍼 | 키 `carrier · updated · ready · line · stations · wagons`, 스테이션 6곳, 유닛은 fwd · rev · airbag |

## 5. 운영 메모

- **기동 직후:** 코어는 (센서, 대차)마다 8번 이상 지나가야 판정 (약 1시간). 유닛은 오늘 2,000 사이클(약 7–8시간)이 모이기 전까지 전날까지의 기록으로 판정한다.
- **범위:** 정상 가동 중 이상 탐지만 한다. PLC 에러·알람은 다루지 않는다 (운전 중이 아니거나 30초 넘게 걸린 사이클은 버린다).
- **재시작:** 지난 가동일 기록은 `data/conveyor_*_history.json` 에서 읽는다. 오늘 모은 것은 메모리라 처음부터 다시 모은다.
- **DB 공백·정지:** 샘플 간격 5초 넘으면 코어 위치를 이어 붙이지 않는다. 유닛은 30초 넘게 걸린 사이클(중간에 멈춤)을 버린다.
- **설정값:** `core_spec.py` (센서 간 거리 D, 장기 기준), `unit_spec.py` (`UNIT_REF` 유닛별 기준값·σ). 센서·실린더를 바꾸거나 정비로 기준이 바뀌면 conv_02 · conv_03 으로 다시 뽑는다.
