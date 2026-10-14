# 펌프 사이클 모델 → HDI_Relay 이식 가이드

```bash
python HDI_MODEL/relay_port/make_port.py      # 이식 묶음 생성 → HDI_MODEL/relay_port/out/backend/
python HDI_MODEL/relay_port/verify_port.py    # 실제 백엔드 config·time_utils·Fault_Registry 와 함께 원본과 같은지 확인
# → out/backend/ 를 HDI_Relay/backend/ 위에 그대로 복사 + 아래 "백엔드 수정" 2곳
```

펌프는 **Pump_Monitor → `Pump_Score` → `Pump_Anomaly`(3D 펌프 마커)** 길로 간다. `Relay_Monitor`(탱크·헤드)는 건드리지 않는다
— Relay_Monitor 주석 그대로 "펌프는 Pump_Monitor 가 따로 보낸다".

---

## 1. 전체 구조

```
                          [오프라인 — DB_Log, 학습·검증]
 InfluxDB ─ extract_pump_cache ─→ cache/pump_raw ─ build_pump_features ─→ cache/pump_feat
                                                                │
          baseline/make_baselines ──→ baseline/{펌프}.json ─────┤
          cycle_ae/dataset → train → calibrate ──→ cycle_ae/artifacts/{model.pt, meta.json}
          cycle_ae/diagnose (규칙표·문구표·검증)
          relay_port/make_port ──→ relay_port/out/backend/  ──(복사)──┐
                                                                     ▼
                          [실시간 — HDI_Relay backend]
 PLC → live_data (매 tick) ─→ PredictiveHealth.generate_idle_json
                               ├─ RelayMonitor.check ─── 탱크·헤드 (그대로) ──────────→ Relay_Data
                               ├─ ConveyorMonitor.read (그대로) ──────────────────────→ conveyor
                               └─ PumpMonitor.check ─┬ PumpRealtimeGroup.update  (판정)
                                                     ├ 근무 단위 유지 (_apply_hold)
                                                     └ pump_scores() + 유지 중 경보 ──→ Pump_Score (14대 전량)
                                                                                         │ PredictiveEngine: over 만
                                                                                         ▼
                                                                                    Pump_Anomaly → 3D 펌프 마커
                                                                                    (Health_Digest 는 Pump_Score 를 읽음)
```

### PumpMonitor.check 한 번 (= 1 tick, 펌프 14대 직렬)

```
live_data ─ 태그 별칭 (TT→TK_Temp_PV, Auto_Running→Auto_Running_PL)
   └ 펌프마다: 이 펌프만의 태그(BuildUp·SV·Hz·FT·PT)가 있으면
        PumpProcessor.update ── 상태기계 (pump_preprocess)            ← 매 tick, 대당 ~20µs
          └ 사이클 끝 + 복귀창 5초:
               add_baseline_features (baseline/{펌프}.json)            ← 펌프 잔차, 압력/기대
               CycleScorer.score_one (model.pt + meta.json)            ← 점수·사이클 등급·이유·피처 편차 (~3ms)
               자동운전 사이클만 ↓ (수동·점검·알람·범위 밖은 기록만)
               WindowTracker.update                                    ← 최근 10사이클 → 펌프 등급·알림(운영일 1회)
               PumpDiagnoser.update                                    ← 유량계/토출/배관/헤드 판정
   └ PumpDiagnoser.alerts ─→ [{reason, priority, type, message, show, pumps, part, …}]
   └ 근무 단위 유지: show=True 면 띄우고, 이어지는 동안(show=False) 연장, 근무 끝나면 내림  (Relay_Monitor 와 같은 규칙)
   └ Pump_Score[pid] = {mse, threshold, over, top, …}  + over 일 때 {time, pump, device, type, message, reason,
                                                               active, last_detected, duration_sec, hold_until}
```

| `Pump_Score[pid]` | 뜻 |
|---|---|
| `None` | 판정 없음 — 정지·대기 순환·점검·수동·학습 범위 밖, 아직 사이클 없음 ('정상' 아님) |
| `mse` / `threshold` | 마지막 자동운전 사이클 AE 점수 / 알림 임계 |
| `top` | 마지막 사이클에서 많이 벗어난 피처 3개 `{feature, share}` (Health_Digest '기여 %') |
| `over` | 경보를 띄우는 중 — 운영일 첫 알림부터 그 근무 끝까지 |
| `device` | 프론트 마커 키 — 지금은 전부 `Pump`(metering_pump). 유량계·배관·헤드는 `message` 가 말한다 |
| `message` | 규칙표 문구. 헤드 판정은 끝에 `— 헤드 1-2 P2·I2`, 여러 대 동시면 공통 원인 문구 |

헤드 판정(같은 헤드 POL·ISO 가 같은 방향 저항)은 **두 펌프 모두**에 띄운다.

---

## 2. 옮길 파일 (out/backend → HDI_Relay/backend, 같은 경로)

| 이식 파일 | 내용 | 원본 (DB_Log/HDI_MODEL) |
|---|---|---|
| `models/pump_cycle/model.pt` | 신경망 가중치 (12 KB) | `cycle_ae/artifacts/model.pt` |
| `models/pump_cycle/meta.json` | 묶음별 표준화 통계·두 단계 임계값 | `cycle_ae/artifacts/meta.json` |
| `models/pump_cycle/pump_settings.json` | 펌프·탱크·헤드·원료·전처리 파라미터 | `pump_settings.json` (← `make_pump_settings.py`) |
| `models/pump_cycle/baseline/*.json` | 펌프별 물리 기준선 14개 | `baseline/*.json` (← `make_baselines.py`) |
| `models_structure/Pump_Cycle_AE.py` | 신경망 구조 `CycleAE` | `cycle_ae/model.py` |
| `tools_yeji/pump_models/pump_params.py` | 자산 경로, 설정, 태그, 점도표, `PumpSetup`, `PumpBaseline`(읽기만) | `new_pump_preprocessor.py`, `Pump_Realtime_Model.py` |
| `tools_yeji/pump_models/pump_preprocess.py` | `PumpProcessor`(상태기계), 사이클 피처, 기준선 피처 | `new_pump_preprocessor.py` |
| `tools_yeji/pump_models/pump_judge.py` | `CycleScorer`(채점), `WindowTracker`(창·알림) | `cycle_ae/model.py` |
| `tools_yeji/pump_models/pump_diagnose.py` | `RULES`(규칙·문구표), `PumpDiagnoser` | `cycle_ae/diagnose.py` |
| `tools_yeji/pump_models/pump_realtime.py` | `PumpRealtimeGroup` (14대 직렬 묶음) | `Pump_Realtime_Model.py` |
| **`tools_yeji/Pump_Monitor.py`** | **[MOCK] 교체본** — 묶음 + 근무 단위 유지 → `Pump_Score` | 손으로 쓴 파일 `relay_port/backend/tools_yeji/Pump_Monitor.py` |

`tools_yeji/pump_models/` 는 탱크·헤드의 `relay_models/` 와 나란한 폴더 (`__init__.py` 포함). 나눔은 탱크 모델과 같다
(`tank_params / tank_preprocess / tank_judge / tank_realtime`).
`models/` 는 백엔드 `.gitignore` 대상 — `models/pump_cycle/` 는 git 으로 안 간다. 서버에 따로 복사한다 (옛 `anomaly_models` 와 같음).
백엔드 것을 그대로 쓰는 부분: `config.topology_config.RELAY_CONFIG`, `config.operation_config.RUN_FLAG_TAG`,
`tools_yeji.common.time_utils` (`to_kst`, `hold_until`, `to_utc_iso`), `tools_yeji.Fault_Registry`. 새 라이브러리 없음.

빠진 것 (DB_Log 에만): 학습(`fit`, `train`), 배치(`run`, `window_levels`), 재생(`replay`), 검증(`validate`, `calibrate`).

---

## 3. 백엔드 수정 (2곳)

**① `config/tag_config.py` — 감시 태그 21개 추가**

```python
    # 펌프 사이클 모델 (tools_yeji/pump_models/pump_*) — 인버터 지령 Hz, 헤드 사출 신호
    *[f"Hz_Out_{p}{i}" for p in "PI" for i in range(1, 8)],
    "Shot_Injection_HD1_P1", "Shot_Injection_HD1_P2", "Shot_Injection_HD2_P1", "Shot_Injection_HD2_P2",
    "Shot_Injection_HD3_P1", "Shot_Injection_HD3_P2", "Shot_Injection_HD4_P1",
    "AL_Line_Bit", "AL_MA_Bit", "AL_Tank_Bit", "AL_ETC_Bit",
```
21개 + 알람 비트 4개(`AL_Line_Bit`, `AL_MA_Bit`, `AL_Tank_Bit`, `AL_ETC_Bit`) 모두 PREDICTIVE·DIAGNOSTIC 블록 안
(`data/block_list*.json` — Hz_Out D00808~, 사출 D01321.8~, 알람 D01350.0~3). 블록은 그대로, 감시 목록에만 올린다.
알람 비트도 **넣는다** — 알람 중 사이클을 판정에서 뺀다 (tick 간격이 바뀌는 구간은 따로 수집 간격 가드가 뺀다, §4).
빠진 태그는 첫 tick 에 PumpMonitor 가 찍는다.

**② `tools_yeji/Predictive_Health.py` — 펌프 모델 로딩을 PumpMonitor 한 줄로**

```python
-from models_structure.Pump_AE import PumpAutoencoder
-from tools_yeji.Pump_preprocess import RealTimePreprocessor
 ...
-        self._load_pump_systems()
-        self.pump_monitor = PumpMonitor(self.pump_systems, self.pump_list,
-                                        threshold=self.THRESHOLD, lookback=self.LOOKBACK)
+        self.pump_monitor = PumpMonitor(self.pump_list)     # 모델·기준선은 models/pump_cycle/ 에서 스스로 읽는다
```
`models_structure/Pump_AE.py` 를 지웠으면 이 import 도 **반드시 같이** 지운다 — 남아 있으면 부팅 때 ImportError.
`_load_pump_systems`, `self.pump_systems`, `THRESHOLD`, `LOOKBACK`, `joblib`·`torch`·`PUMP_TO_TANK` import 는 더 안 쓴다.
`__init__(self, model_dir=...)` 인자는 PredictiveEngine 이 넘기므로 남겨 둔다 (안 씀).
`generate_idle_json` 은 그대로 (`"Pump_Score": self.pump_monitor.check(live_data)`).

부팅 확인 (2026-10-11): 백엔드 사본 + 이식 묶음 + 위 두 수정 → `PredictiveHealth()` 생성, `generate_idle_json` 5 tick,
빠진 태그 없음, `Pump_Score` 14개 키, JSON 직렬화 통과.

---

## 4. 이식 전에 정할 것 / 정리할 것

| 항목 | 지금 | 제안 |
|---|---|---|
| **아이콘** | `type` 이 `flow`·`pump` 인 경보는 아이콘 미등록 → 삼각 경고 | `warning-icons.ts` 에 두 줄 |
| **공장 알람 때 tick 간격** — **가드 넣음** | 알람 → `DIAGNOSTIC`(2초 tick)·DRILL_DOWN(3 tick) → `ANALYZING`(예지 호출 **멈춤**, 진단 끝날 때까지) → PREDICTIVE. 펌프 상태기계는 tick **개수**로 센다 (탱크는 초 단위 + 90초 공백 리셋이 이미 있고, 헤드는 PLC 타이머 값이라 상관없다) | `pump_preprocess` 수집 간격 가드: ① tick 사이 > 3초면 부팅처럼 리셋 (진행 중 사이클 무효, 직전 Hz·사출 기록 잊음, 꺼짐을 본 뒤 켜지는 사이클부터) ② 사이클 안 tick 간격 중앙값 > 1초면 무효 (2초 tick). 무효 사이클은 모드 `수집 간격` — 계산은 하되 창·진단·경보에서 뺀다 (결과만 숨기면 창 10사이클 중앙값이 오염된다). 창은 공백 30분 미만이면 이어 쓴다 |
| **live_data 간격** | 상태기계가 "틱 개수" 로 안정 구간을 판단 (로거 약 0.5초). 백엔드는 `PredictiveEngine.period_sec = 0.5` — 같다 | 0.5초는 **최소** 간격이라 MQTT 가 늦으면 길어진다. 연결 후 실제 간격 한 번 확인 |
| **옛 펌프 모델 정리** | `models/anomaly_models/`(펌프별 AE·scaler 28개), `Pump_preprocess.py`, `Pump_Anomaly.py`, `models_structure/Pump_AE.py`, `utils/Anomaly_Logger.save_deep_dive_report` | 교체 후 import 하는 곳 없음 (2026-10-11 grep) → 연결 확인 뒤 제거. `PredictiveEngine` 의 `model_dir='./models/anomaly_models'` 인자도 같이 |
| `Pump_Anomaly` 의 `wagon`·`tick` | 옛 틱 AE 필드 — 항상 None | 쓰는 곳 없으면 그대로 둠 |

## 5. 나중에 할 것

- **헤드 판정을 헤드 SOL 마커에도 띄우기.** 지금은 POL·ISO 펌프 마커 두 곳에만 뜬다. 진단 경보에 헤드 인젝션 SOL 릴레이
  주소(`relay`, 예 HD1/P2 → `Head_1-2 Injection SOL`)가 이미 채워져 있으니, `Relay_Monitor.check` 에서
  ```python
  alerts = (self._tank.update(now, live_data) + self._head.update(now, live_data)
            + [a for a in pump_alerts if a["part"] == "head"])      # PumpMonitor 가 이번 tick 진단 경보를 내놓게 해서 받기
  ```
  처럼 헤드 판정만 더하면 근무 단위 유지·relay 합치기는 Relay_Monitor 가 그대로 한다.
  (PumpMonitor 가 이번 tick 경보 목록을 속성으로 들고 있게 하고, PredictiveHealth 가 RelayMonitor 에 넘겨 주면 된다)
- **유량계 부품 마커.** topology·3D 에 유량계가 생기면 `Pump_Monitor.PART_DEVICE["flowmeter"]` 만 바꾼다.
- **틱 단위 모델·규칙** (`ft_timeout`, `r_early`) — 사이클이 끝나기 전에 잡아야 할 게 생기면.
- **알림 기억 저장.** 운영일 1회 알림·근무 유지 기억이 메모리라 백엔드를 재시작하면 같은 날 다시 알릴 수 있다
  (탱크·헤드 Relay_Monitor 도 같다). 문제 되면 파일로 남긴다.

---

## 6. 다시 학습하거나 고쳤을 때

```bash
python HDI_MODEL/extract_pump_cache.py                 # 새 날 받기 (서버 부담 작게, 펌프·하루 단위)
python HDI_MODEL/build_pump_features.py --force
python HDI_MODEL/baseline/make_baselines.py            # 기준선
python HDI_MODEL/cycle_ae/dataset.py && python HDI_MODEL/cycle_ae/train.py
python HDI_MODEL/cycle_ae/calibrate.py                 # 임계값만 바꿀 땐 이것만
python HDI_MODEL/cycle_ae/diagnose.py --validate       # 진단 규칙 검증
python HDI_MODEL/relay_port/make_port.py && python HDI_MODEL/relay_port/verify_port.py && python HDI_MODEL/relay_port/verify_gap.py
# → 바뀐 파일만 백엔드로 (임계값만 바꿨으면 models/pump_cycle/meta.json 하나)
```

## 7. 검증 기록

`verify_port.py` — 같은 tick 을 원본 `PumpRealtimeGroup` 과 이식본 `PumpMonitor.check` 에 흘려 비교:
진단 경보 tick 마다 같음 / 사이클 판정 같음 / 사람에게 알린 펌프가 그 tick 에 `over=True` / `Pump_Score` JSON 직렬화 /
`Pump_Anomaly` 프론트 계약(`isPumpAnomaly`).

| 구간 | 행 | 사이클 | 진단 경보 다른 tick | 사람에게 알림 → Pump_Anomaly | 형식 |
|---|---|---|---|---|---|
| 10/10 06:40~10:00 | 303,677 | 4,356 같음 | 0 | 5건 = 5대 켜짐 (P2·I7 07:00, I6 07:05, P6 07:32, I2 07:40) | 문제 없음 |
| 9/16 21:30~9/17 08:00 (운영일 경계·야간 리셋) | 882,926 | 5,733 같음 | 0 | 3건 — I7 21:37 배관 저항 감소, P7 22:28 유량계 측정 이상 → 06:56 유량계 고장으로 문구만 바뀜 | 문제 없음 |

HDI_Relay 에 넣은 뒤 (`verify_port.py --backend ../HDI_Relay/backend`): 위 두 구간 같은 결과. 백엔드 자기 venv(pandas 3.0.0 ·
numpy 2.4.2)로 10/10 구간 재생 — 사이클·알림 같음. TagMapper: 원시 PLC 배열 → live_data 에 새 태그 25개가 두 모드 모두 맞는 값.
부팅: `PredictiveEngine` → `maybe_publish` → 프론트 JSON `Pump_Anomaly`, 링버퍼 `Pump_Score` 14개.

**수집 간격 가드** (`verify_gap.py`, 10/10 06:40~10:00):

| | 결과 |
|---|---|
| 평소 데이터 | 사이클 4,356 그대로, 버림 0, 모드 바뀜 0, 알림 5건 같음. 점수만 7사이클 미세 변화 — 로거의 3초 넘는 공백 바로 뒤 사이클(직전 Hz 를 잊어 `dhz_rel`=0), 등급 같음 |
| 공장 알람 1회 (2초 tick 6초 + 진단 4분 호출 없음) | 구간 사이클 113개 판정 제외, 거짓 알림 없음. 다시 판정까지 = 기준 데이터의 다음 사이클 간격과 같음 (잃는 건 걸친 사이클 하나). P6 `pattern` 알림은 그 4분 동안 P6 사이클 7개가 빠져 창이 경계를 안 넘어 안 뜸 |
| 현장이 10분 진단 모드 (2초 tick) | 구간 사이클 238개 판정 제외, 거짓 알림 없음, 알림 5건 그대로 |
| 진단 40분 (창 30분 리셋) | 구간 1,016개 판정 제외, 거짓 알림 없음, 알림 5건 그대로 |

(2026-10-11) 이상이 이어지는 동안은 근무가 끝나도 꺼지지 않는다 — Relay_Monitor 와 같은 규칙. P7 처럼 같은 펌프에 더 급한
판정이 오면 마커는 그대로(최초 시각 유지) 문구·사유만 우선순위 높은 쪽으로 바뀐다.
