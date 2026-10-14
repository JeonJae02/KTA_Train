# 펌프 진단 규칙표 · 알림 문구표

`diagnose.py --table` 로 생성 — 고칠 땐 `diagnose.py` 의 `RULES` 를 고치고 다시 생성.

- 입력: `WindowTracker` 의 펌프 등급(알림일 때만), 창 10사이클 피처별 부호 있는 정규화 편차 중앙값 `zn_med` (±1 = 정상 한계)
- PR 펌프 잔차 · RP 압력/기대 · P0 시작 전 압력 · CV 유량 흔들림. 증거가 가장 큰 규칙 하나, 0.5 미만이면 `pattern`
- 같은 헤드 슬롯 POL·ISO 가 같은 방향 저항 판정이면 헤드 경보 하나로 묶는다
- 30분 안에 3대 이상 같은 판정이면 문구에 공통 원인 표시

| 코드 (`reason` = `pump_` + 코드) | 부품 → 화면 | 아이콘 `type` | 우선순위 | 조건 | 문구 |
|---|---|---|---|---|---|
| `flowmeter_fail` | 유량계 → 그 펌프 마커 | `flow` | 0 | PR ≤ −5 이고 RP ≥ −0.25 (압력은 안 내려감) | 유량계 고장 의심 · 유량값 급감, 토출 압력은 정상 |
| `head_resistance_high` | 헤드 → POL·ISO 펌프 마커 둘 다 (나중에 헤드 인젝션 SOL 마커도) | `pressure` | 1 | 같은 헤드 슬롯 POL·ISO 둘 다 RP 또는 P0 ≥ 0.5 | 헤드 토출 저항 증가 · POL·ISO 동시 압력 상승 (노즐·믹싱헤드 점검) |
| `head_resistance_low` | 헤드 → POL·ISO 펌프 마커 둘 다 (나중에 헤드 인젝션 SOL 마커도) | `pressure` | 1 | 같은 헤드 슬롯 POL·ISO 둘 다 RP 또는 P0 ≤ −0.5 | 헤드 토출 저항 감소 · POL·ISO 동시 압력 하강 (노즐 마모·열림 점검) |
| `flowmeter_low` | 유량계 → 그 펌프 마커 | `flow` | 2 | 증거 −PR (RP ≥ −0.25 일 때) | 유량계 측정 이상 의심 · 같은 회전수 대비 유량값 감소, 토출 압력은 정상 |
| `pump_output_low` | 펌프 본체·모터 → 그 펌프 마커 | `pump` | 2 | 증거 min(−PR, −RP) (둘 다 ≤ −0.25) | 펌프 토출 저하 · 같은 회전수 대비 유량·압력 함께 감소 (펌프 마모·흡입 점검) |
| `line_resistance_high` | 토출 배관·노즐 → 그 펌프 마커 | `pressure` | 3 | 증거 max(RP, P0) | 토출 배관·노즐 저항 증가 · 같은 유량 대비 압력 상승 |
| `line_resistance_low` | 토출 배관·노즐 → 그 펌프 마커 | `pressure` | 3 | 증거 max(−RP, −P0) | 토출 배관 저항 감소 · 같은 유량 대비 압력 하강 (노즐 열림·누설 의심) |
| `flow_reading_high` | 유량계 → 그 펌프 마커 | `flow` | 4 | 증거 PR (\|RP\| < 0.5 일 때) | 유량값 증가 · 같은 회전수 대비 유량 상승 (유량계·슬립 확인) |
| `response_change` | 펌프 본체·모터 → 그 펌프 마커 | `pump` | 4 | 증거 max \|응답 시간, 감속 반감, 압력 해제 반감\| | 펌프 응답 변화 · 증압·감속·압력해제 시간이 평소와 다름 (모터·인버터·밸브 점검) |
| `flow_fluctuation` | 유량계 → 그 펌프 마커 | `flow` | 5 | 증거 \|CV\| | 유량 흔들림 변화 · 유량 신호가 평소와 다르게 흔들림 (유량계·기포 확인) |
| `pattern` | 펌프 본체·모터 → 그 펌프 마커 | `pump` | 6 | 위 규칙 증거가 모두 0.5 미만 (AE 패턴 점수로만 알림) | 펌프 운전 패턴 변화 · 여러 지표의 관계가 평소와 다름 |

## 검증 1 — 알려진 이벤트 (학습 기간·테스트)

| 이벤트 | 알림 상태 판정 (상위 2) | 첫 알림 경보 |
|---|---|---|
| P7 유량계 전조 (전날 밤) | flowmeter_low 63%, pattern 27% | 09-16 22:27 `pump_flowmeter_low` → `Scale_Out___PT_P7` |
| P7 유량계 고장 | flowmeter_fail 88%, response_change 7% | 09-17 06:56 `pump_flowmeter_fail` → `Scale_Out___PT_P7` |
| I1 노즐 저항 감소 (FT/SV 1.09) | line_resistance_low 100% | 09-29 06:54 `pump_line_resistance_low` → `Scale_Out___PT_I1` |
| I1 노즐 저항 증가 (K 7.8) | line_resistance_high 100%, flow_fluctuation 0% |  |
| P2 배관 저항 +7% | line_resistance_high 98%, flow_reading_high 2% | 10-06 06:56 `pump_line_resistance_high` → `Scale_Out___PT_P2` |
| I7 저항 드리프트 (톱니, 9/23 전) | line_resistance_low 99%, pattern 1% | 09-10 06:56 `pump_line_resistance_low` → `Scale_Out___PT_I7` |
| I7 저항 드리프트 재발 (테스트) | line_resistance_low 79%, line_resistance_high 20% | 10-05 21:26 `pump_line_resistance_low` → `Scale_Out___PT_I7` |
| I3 9/18 계단 전 (+5%, 작업 전으로 추정) |  |  |

## 검증 2 — 새 날 10/8~10/11 에 사람에게 간 경보 (`show=True`)

| 시각 | label | reason | relay | relay_fallback | message |
|---|---|---|---|---|---|
| 10-08 06:55 | 1-2펌프 CUSH | pump_line_resistance_high | Scale_Out___PT_P2 | False | 토출 배관·노즐 저항 증가 · 같은 유량 대비 압력 상승 |
| 10-08 07:25 | 1펌프 ISO | pump_line_resistance_high | Scale_Out___PT_I1 | False | 토출 배관·노즐 저항 증가 · 같은 유량 대비 압력 상승 |
| 10-08 07:25 | 3-2펌프 ISO | pump_line_resistance_low | Scale_Out___PT_I6 | False | 토출 배관 저항 감소 · 같은 유량 대비 압력 하강 (노즐 열림·누설 의심) |
| 10-08 08:47 | HD1-2 P2·I2 | pump_head_resistance_high | P0028D | False | 헤드 토출 저항 증가 · POL·ISO 동시 압력 상승 (노즐·믹싱헤드 점검) |
| 10-08 13:46 | 4펌프 CUSH | pump_line_resistance_high | Scale_Out___PT_P7 | False | 토출 배관·노즐 저항 증가 · 같은 유량 대비 압력 상승 |
| 10-08 16:26 | 4펌프 ISO | pump_pattern | Scale_Out___PT_I7 | True | 펌프 운전 패턴 변화 · 여러 지표의 관계가 평소와 다름 |
| 10-09 06:53 | 1펌프 ISO | pump_line_resistance_high | Scale_Out___PT_I1 | False | 토출 배관·노즐 저항 증가 · 같은 유량 대비 압력 상승 |
| 10-09 06:56 | 1-2펌프 CUSH | pump_line_resistance_high | Scale_Out___PT_P2 | False | 토출 배관·노즐 저항 증가 · 같은 유량 대비 압력 상승 |
| 10-09 07:05 | 4펌프 ISO | pump_line_resistance_low | Scale_Out___PT_I7 | False | 토출 배관 저항 감소 · 같은 유량 대비 압력 하강 (노즐 열림·누설 의심) |
| 10-09 07:12 | 3-2펌프 ISO | pump_line_resistance_low | Scale_Out___PT_I6 | False | 토출 배관 저항 감소 · 같은 유량 대비 압력 하강 (노즐 열림·누설 의심) |
| 10-09 11:58 | 2-2펌프 MDI | pump_line_resistance_low | Scale_Out___PT_I4 | False | 토출 배관 저항 감소 · 같은 유량 대비 압력 하강 (노즐 열림·누설 의심) |
| 10-10 07:00 | 1-2펌프 CUSH | pump_line_resistance_high | Scale_Out___PT_P2 | False | 토출 배관·노즐 저항 증가 · 같은 유량 대비 압력 상승 |
| 10-10 07:00 | 4펌프 ISO | pump_line_resistance_low | Scale_Out___PT_I7 | False | 토출 배관 저항 감소 · 같은 유량 대비 압력 하강 (노즐 열림·누설 의심) |
| 10-10 07:05 | 3-2펌프 ISO | pump_line_resistance_low | Scale_Out___PT_I6 | False | 토출 배관 저항 감소 · 같은 유량 대비 압력 하강 (노즐 열림·누설 의심) |
| 10-10 07:32 | 3-2펌프 BACK | pump_pattern | Scale_Out___PT_P6 | True | 펌프 운전 패턴 변화 · 여러 지표의 관계가 평소와 다름 |
| 10-10 07:40 | 1-2펌프 ISO | pump_line_resistance_high | Scale_Out___PT_I2 | False | 토출 배관·노즐 저항 증가 · 같은 유량 대비 압력 상승 |
| 10-10 12:21 | 3펌프 고탄성 | pump_pattern | Scale_Out___PT_P5 | True | 펌프 운전 패턴 변화 · 여러 지표의 관계가 평소와 다름 |
| 10-10 14:51 | 2-2펌프 MDI | pump_line_resistance_low | Scale_Out___PT_I4 | False | 토출 배관 저항 감소 · 같은 유량 대비 압력 하강 (노즐 열림·누설 의심) |
| 10-10 21:52 | 4펌프 CUSH | pump_pattern | Scale_Out___PT_P7 | True | 펌프 운전 패턴 변화 · 여러 지표의 관계가 평소와 다름 |
| 10-10 21:59 | 1펌프 BACK | pump_flow_reading_high | Scale_Out___PT_P1 | True | 유량값 증가 · 같은 회전수 대비 유량 상승 (유량계·슬립 확인) |
| 10-10 22:21 | 2-2펌프 HARD | pump_flow_reading_high | Scale_Out___PT_P4 | True | 유량값 증가 · 같은 회전수 대비 유량 상승 (유량계·슬립 확인) |
| 10-10 22:25 | 2-1펌프 MDI | pump_flow_reading_high | Scale_Out___PT_I3 | True | 유량값 증가 · 같은 회전수 대비 유량 상승 (유량계·슬립 확인) — 같은 시각 다른 펌프 2대도 같은 판정 (재료 온도·공정 조건 등 공통 원인 확인) |
