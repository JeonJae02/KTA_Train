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

공통 제외: 09-27(연휴 Auto-Cal), P7 09-17 06:45~11:30(유량계 고장), 알람·정지 사이클, 다단 Hz 사이클, 펌프의 주된 `steady_src` 가 아닌 사이클.

**평소 상태 고르는 규칙** — 일별 K(같은 SV 클래스 대비 %)가 서로 4%p 안에 머문 가장 긴 연속 구간의 중앙값을 기준 수준으로,
그 ±3% 인 날 중 시간별 K 가 ±5% 안인 시간만. 바꾸려면 `make_baselines.py` 의 `PT_OVERRIDE` 에 날짜를 적는다.

## 사람이 확인할 것

| 펌프 | 자동 선택 | 확인할 점 |
|---|---|---|
| **I7** | 09-23 ~ 10-03 (K 가 높아진 뒤) | 9/21 무렵 K 가 +14% 계단으로 올라 그대로다. 그 전(저항 낮음)이 건강한 상태였다면 `PT_OVERRIDE` 로 9/15~17 을 지정 — 그러면 지금 상태는 `r_phys` ≈ 1.10 으로 계속 이상 표시된다 |
| **I3** | 09-18 ~ 10-03 (K 가 낮아진 뒤) | 9/18 에 −6% 계단. 노즐 청소·교체 같은 정비였다면 지금 선택이 맞다 |
| I6 | 9/15·16·18·21~23 등 7일 | 사출 중 안정값이라 일별 K 가 ±20% 씩 흔들린다. 압력 기준선 사이클 2,421개(30%) |

## 파일

| 파일 | 내용 |
|---|---|
| `{펌프}.json` | `PumpBaseline` — 계수 + `fitted_on`(구간, 건강일, 규칙, 학습/검증/테스트 잔차) |
| `_summary.csv` | 펌프별 한 줄 요약 |
| `_healthy_days.csv` | 펌프 × 날짜 일별 K 편차와 건강일 여부 |
| `_check.png` | 위 표를 그림으로 — 파랑 사용 / 주황 제외 |
| `make_baselines.py` | 생성 스크립트 (JSON 은 손으로 고치지 말고 이걸 고쳐 다시 생성) |
