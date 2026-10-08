# 펌프 기준선 (14대)

```bash
python HDI_MODEL/baseline/make_baselines.py              # 전 펌프 다시 생성 (약 3초, DB 안 침)
python HDI_MODEL/baseline/make_baselines.py --pumps I7   # 일부만
```

```python
from new_pump_preprocessor import PumpBaseline, PumpProcessor, PumpSetup, add_baseline_features
base = PumpBaseline.load("HDI_MODEL/baseline/P1.json")
cyc = add_baseline_features(cyc, base)                    # pump_resid, r_phys, FT_e
proc = PumpProcessor(PumpSetup("P1"), base)               # 실시간
```

입력은 `cache/pump_feat` (먼저 `extract_pump_cache.py` → `build_pump_features.py`). 기준선의 종류·재학습 시점은 `../new_pump_preprocessor.md` 5절.

## 무엇으로 맞췄나 (2026-10-08 생성)

| 기준선 | 데이터 |
|---|---|
| ① 펌프 (`pump_coef`, `r`) | 09-10 ~ 10-04 전 생산일 (테스트 10-05~ 제외) |
| ② 압력 (`pt_coef`) | 그중 **배관이 평소 상태였던 날·시간** — 자동 선택, `_check.png` |
| ③ 순환 (`circ_coef`) | 09-10 ~ 10-04 순환창 |

공통 제외: 09-27(연휴 Auto-Cal), P7 09-17 06:45~11:30(유량계 고장), 알람·정지 사이클, 펌프의 주된 `steady_src` 가 아닌 사이클.

**Hz 는 레시피 전체 범위로 맞춘다** — 기준선은 Hz 의 함수다(예: P1 레시피 28개·Hz 69가지, 3138~4526 Hz).
사이클 안에서 Hz 가 바뀌는 다단 레시피 사이클도 넣는다(대표 Hz 구간의 안정값 사용). 빼면 다단 레시피만 쓰는 Hz 대역을
외삽한다 — P3 다단 사이클 펌프 잔차 +3.9% → 넣은 뒤 0.0%, r_phys 0.64 → 0.99.

**평소 상태 고르는 규칙** — 일별 K(같은 SV 클래스 대비 %)가 서로 4%p 안에 머문 가장 긴 연속 구간의 중앙값을 기준 수준으로,
그 ±3% 인 날 중 시간별 K 가 ±5% 안인 시간만. 바꾸려면 `make_baselines.py` 의 `PT_OVERRIDE` 에 날짜를 적는다.

## 자동 선택에서 판단한 것 (2026-10-08)

| 펌프 | 결정 | 근거 (시간 단위 K) |
|---|---|---|
| **I7** | **수동: 9/23 ~ 10/3** (`PT_OVERRIDE`) | 9/23 전에는 **톱니** — K 가 1~2일에 걸쳐 15~20% 서서히 빠졌다가 한 번에 되돌아오기를 4번 반복(9/15·17·18·21). 온도 20.3~20.6℃ 일정, 유량 일정, 압력만 93→75. 조절 밸브·노즐 설정이 풀리고 다시 맞춘 것으로 추정. 9/23 06시부터 평평. 자동 선택은 톱니가 우연히 같은 높이를 지난 날도 섞여서 직접 지정. **10/5 밤~10/7 에 같은 드리프트가 다시 나타났다** |
| I3 | 자동(9/18 이후) 유지 | 9/18 12~13시 한 시간 만에 K +4% → −1%, 전후 모두 평평 → 조정·청소 작업 뒤를 정상으로 |
| I6 | 자동 유지 (8일) | 사출 중 안정값이라 잡음이 크고, I1 처럼 압력이 몇 단계(r_phys 0.95/1.06/1.25/1.44) 사이를 오간다. 압력 기준선 사이클 2,858개(34%) |

현장 확인이 되면 바꿀 것: I7 라인 밸브·노즐 재조정 이력(9/15~23, 10/5~7), I3 9/18 정오 작업 여부.

## 파일

| 파일 | 내용 |
|---|---|
| `{펌프}.json` | `PumpBaseline` — 계수 + `fitted_on`(구간, 건강일, 규칙, 학습/검증/테스트 잔차) |
| `_summary.csv` | 펌프별 한 줄 요약 |
| `_healthy_days.csv` | 펌프 × 날짜 일별 K 편차와 건강일 여부 |
| `_check.png` | 위 표를 그림으로 — 파랑 사용 / 주황 제외 |
| `make_baselines.py` | 생성 스크립트 (JSON 은 손으로 고치지 말고 이걸 고쳐 다시 생성) |
