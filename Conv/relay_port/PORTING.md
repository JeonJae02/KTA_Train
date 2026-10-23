# 컨베이어 대차 센서 모델 → HDI_Relay 이식 가이드 (1차 배포)

```bash
python Conv/relay_port/make_port.py      # 이식 묶음 생성 → Conv/relay_port/out/backend/
python Conv/relay_port/verify_port.py    # 묶음 코드로 과거 데이터 재생 → 배치(conv_02) 판정과 비교
# → out/backend/ 를 HDI_Relay/backend/ 위에 그대로 복사 + 아래 "백엔드 수정" 1곳
```

1차 배포 범위는 **대차 통과 근접 센서 6개**(주입부 대차 진입·쉬프트, 클로징·오프닝 존 진입, 전진·후진 유닛 진입)와 구동부(속도 비율)다.
실린더(conv_03)와 리프트는 다음 배포에 넣는다.

---

## 1. 구조

```
 [오프라인 — DB_Log/Conv]
 InfluxDB ─ extract_conv_cache ─→ cache/conv_raw ─ conv_grid.day_events/daily_table ─→ cache/conv_feat/wagon_sensor_daily.pkl
                                                         │ conv_rules.judge (규칙)              │
                                                         ▼                                     ▼
                                               conv_02 노트북 (분석)            make_port ─→ data/conveyor_history.json (처음 기록)
 [실시간 — HDI_Relay backend]
 PLC → live_data (매 tick) ─→ PredictiveHealth.generate_idle_json
                               └─ ConveyorMonitor.read(live_data) ──→ "conveyor"
                                    └ ConveyorRealtime.update        (tick 마다 ~26 µs)
                                         카운터 6개 → 체인 위치, 카운터 리셋 → 센서 통과, 원본 비트 → 켜짐 길이,
                                         통과마다 대차 번호 매칭 → (센서, 대차) 최근 20번 통과
                                    └ 60초마다 evaluate              (~0.15 초)
                                         최근 20번 통과 칸 + 지난 가동일 기록 → conv_rules.judge → 출력
                                    └ 오전 6시 close_day             하루치 칸을 기록에 더하고 data/conveyor_history.json 저장
```

판정 규칙은 배치와 실시간이 **같은 파일**(`conv_rules.py`)을 쓴다. 규칙을 고치면 `make_port.py` 를 다시 돌린다.

## 2. 백엔드 수정

**① 묶음 복사** — `out/backend/` 를 `HDI_Relay/backend/` 위에 복사한다.

| 파일 | 내용 |
|---|---|
| `tools_yeji/Conveyor_Health.py` | `ConveyorMonitor` — MOCK 교체. 이름·생성자·`read(live_data)` 는 그대로라 `Predictive_Health.py` 는 안 고친다 |
| `tools_yeji/conveyor_models/conv_rules.py` | 판정 규칙 |
| `tools_yeji/conveyor_models/conv_realtime.py` | 실시간 모델 |
| `data/conveyor_history.json` | 처음 기록 (09-10 – 10-19, 31 가동일 × 252칸). 운영 중에는 매일 오전 6시에 갱신된다 — **배포 후 덮어쓰지 말 것** |

**② `config/tag_config.py` 의 `MONIORING_TAGS` 에 태그 15개 추가.** 전부 PREDICTIVE 블록 안이라 이미 수신 중이다 (P 0–50, R 0–88, D 500–999, D 1000–1499).

```python
    # 컨베이어 대차 센서 모델(tools_yeji/conveyor_models) 입력. 전부 PREDICTIVE 블록 안이라 이미 수신 중이다.
    # 존 카운터 (엔코더 펄스마다 +1, 존 진입 센서에서 1로 리셋) — 체인 위치와 센서 통과
    "R00030", "R00040", "R00055", "R00060", "R00070", "R00080",
    # 대차 통과 근접 센서 원본 비트 — 켜짐 길이
    "P00140", "P00141", "P00154", "P00164", "P00451", "P00459",
    # 대차 번호 · 운전 · 속도 지령
    "Now_Actual_Wagon_Num", "Conv_Run_Out", "Hz_In_Conv",
```

빠진 태그가 있으면 처음 tick 에 `⚠️ [ConveyorMonitor] 감시 태그 없음` 이 찍힌다.

## 3. 출력 — `conveyor` (프론트 계약)

**점수 숫자는 내지 않는다.** 상태 4단계와 근거 문구만 낸다. `sev` 는 단계 번호(0 정상 · 1 관찰 · 2 점검 추천 · 3 경고)이고, 색을 정할 때 쓴다.

```jsonc
{
  "carrier": "대차번호-15",        // 지금 주입부(P00140)에 있는 대차 — 예전 MOCK 과 같은 키
  "updated": "2026-10-19T23:59:59", // 마지막 판정 시각 (KST)
  "ready": true,                    // false: 아직 판정할 만큼 통과가 안 모임 (기동 직후 약 1시간)
  "line": {
    "sev": 2, "status": "점검 추천",  // 라인 전체 = 가장 나쁜 스테이션
    "drive": { "sev": 0, "status": "정상", "reason": "정상" }   // 구동부: 속도 비율 (초당 카운트 ÷ Hz/10)
  },
  "stations": {                     // 화면의 스테이션 마커 5곳
    "pour":    { "name": "주입부",   "sensors": ["P00140","P00141"], "sev": 0, "status": "정상", "sensor_flags": [], "wagons": [] },
    "closing": { "name": "클로징 존", "sensors": ["P00154"], ... },
    "opening": { "name": "오프닝 존", "sensors": ["P00164"], "sev": 2, "status": "점검 추천", "sensor_flags": [],
                 "wagons": [ { "wagon": 20, "sensor": "P00164", "sev": 2, "status": "점검 추천", "state": "이물(분리)",
                               "reason": "한 대차에서 다시 켜짐 (통과당 1.0번) → 표식 앞 떨어진 이물 또는 채터링",
                               "since": "09-10" } ] },
    "fwd":     { "name": "전진 유닛", "sensors": ["P00451"], "sev": 2, "status": "점검 추천",
                 "sensor_flags": [ { "sensor": "P00451", "name": "전진 유닛 진입", "flag": "센서 재감지",
                                     "action": "센서 감지 거리·채터링 확인" } ],
                 "wagons": [ ... ] },
    "rev":     { "name": "후진 유닛", "sensors": ["P00459"], ... }
  },
  "wagons": {                       // 대차 42대 ("01" – "42") — 화면의 대차 타일 색
    "20": { "sev": 2, "status": "점검 추천",
            "stations": [ { "station": "opening", "name": "오프닝 존", "sensor": "P00164", "sev": 2, "status": "점검 추천",
                            "state": "이물(분리)", "reason": "...", "since": "09-10" },
                          { "station": "fwd", ..., "state": "이물(앞)", "reason": "앞 끝 −6 · 켜짐 길이 +6 → 표식 앞쪽 이물", "since": "10-19" } ] },
    "21": { "sev": 0, "status": "정상", "stations": [] },
    ...
  }
}
```

| 필드 | 값 |
|---|---|
| `status` | `정상` · `관찰` · `점검 추천` · `경고` · `null`(판정 없음 — 그 칸을 아직 충분히 못 봄) |
| `state` (대차 × 센서) | `이물(분리)` 한 대차에서 다시 켜짐 · `이물(앞)` 표식 앞쪽 이물 · `이물(뒤)` · `이물(기타)` · `위치 변화` 표식이 옮겨짐(3 가동일 유지되면 승인 → 정상) · `흔들림` |
| `sensor_flags[].flag` | `센서 재감지` · `센서 이동` · `센서 감도·면 오염` · `센서 흔들림` — 대차가 아니라 **센서 쪽** 문제. 여러 개면 여러 줄 |
| `since` | 그 상태가 시작된 가동일 (MM-DD). 기록 보관 60일 안에서 |
| `drive` | 속도 비율이 0.2744 에서 0.3% 넘게 벗어나면 `경고` |

화면 정리 (10-23 결정): 대차는 움직이지 않고 번호 고정, 스테이션 마커 5곳, 문제 있는 대차만 색. 숫자 점수 없음.

## 4. 판정 규칙 요약 (자세한 근거는 conv_02 노트북)

| 보이는 것 | 상태 | 수준 | 자동 승인 |
|---|---|---|---|
| 한 대차에서 다시 켜짐 (통과당 2차 리셋 ≥ 0.5) | 이물(분리) | 점검 추천 | 안 함. 재감지 < 0.25 · 이물 지수 < 1.5 로 내려오면 복귀 (경고 없음) |
| 켜짐 길이 +3 이상, 앞 끝이 그만큼 당겨짐 | 이물(앞) | 점검 추천 | 안 함 |
| 길이 정상, 앞뒤 끝이 같이 3 이상 이동 | 위치 변화 | 관찰 | 3 가동일 유지 시 승인 |
| 그 대차만 흔들림 IQR ≥ 3 | 흔들림 | 관찰 | — |
| 센서 위치가 장기 기준에서 3 이상 / 정상 켜짐 길이 +3 / 센서 전체 흔들림 / 통과당 재감지 ≥ 0.1 | 센서 단위 | 점검 추천 | — |
| 속도 비율 ±0.3% 밖 | 구동부 | 경고 | — |

물리 기준(0점)은 그날 그 센서에서 **깨끗한 대차들**(재감지 적고 켜짐 길이 정상)의 중앙값이다. 청소하면 그 대차가 기준으로 돌아오므로 경고가 뜨지 않는다.

## 5. 검증 (verify_port.py)

| 항목 | 결과 |
|---|---|
| 재생 | 10-12 – 10-17 (6일), tick 669,393, 26 µs/tick |
| 하루 마감 칸 vs 배치 일별 표 | dist 99.9% 같음(최대 0.5), width 98.6%(최대 1.5), rearm 95.7%(최대 0.02), jitter 99.3%(최대 1.5). 남는 차이는 자정 경계 통과 1–2개 |
| 칸 상태 vs 배치 | **1,260칸 100% 일치** |
| `ConveyorMonitor.read()` | 키 `carrier · updated · ready · line · stations · wagons`, 스테이션 5곳 |

## 6. 운영 메모

- **기동 직후:** (센서, 대차)마다 8번 이상 지나가야 판정한다(약 1시간). 그 전에는 `ready: false`, 상태 `null`.
- **재시작:** 지난 가동일 기록은 `data/conveyor_history.json` 에서 읽는다. 오늘 통과는 메모리라 재시작하면 처음부터 모은다.
- **DB 공백·정지:** 샘플 간격이 5초를 넘으면 위치를 이어 붙이지 않는다. 그때 진행 중이던 통과는 버린다.
- **역회전·수동 조그:** 카운터가 줄어드는 쪽이 다수면 위치도 줄어든다. 통과 판정은 카운터 리셋으로만 한다.
- **설정값** (`conv_realtime.py` 위쪽): 센서 간 거리 `D`, 장기 기준 `LONG_REF` 는 09-10 – 09-19 데이터에서 나왔다. 센서를 옮기거나 대차를 바꾸면 다시 뽑는다 (conv_02 §2, §7).
