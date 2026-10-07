"""펌프 전처리기 설정 파일(`pump_settings.json`) 생성 스크립트.

설정은 **이 파일을 고치고 다시 실행**해서 바꾼다. JSON 을 손으로 고치지 말 것 —
값마다 근거를 여기 주석으로 남겨 두기 때문이다.

    python HDI_MODEL/make_pump_settings.py            # HDI_MODEL/pump_settings.json 생성

들어가는 것
- params    : 전처리 파라미터 (안정 구간, 밴드, 복귀창, 실시간 규칙 등)
- materials : 원료별 점도 상수 B (Andrade 지수식)
- pumps     : 펌프 → 원료·탱크·헤드·슬롯. 태그 이름은 전처리기가 여기서 만든다
              (온도 = Scale_Out___TT_{tank}, 사출 = Shot_Injection_{head}_P{slot})

들어가지 않는 것
- 점도 조회표 : B 로 결정되는 파생값이라 전처리기가 시작할 때 만든다 (251칸, 비용 ≈ 0).
               따로 저장하면 B 를 바꿨을 때 표만 옛 값으로 남는다
- 기준선      : 정상 구간에서 학습한 값 → `PumpBaseline.save()` 로 펌프별 JSON
"""

import json
import os
from datetime import date

OUT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pump_settings.json")

# --------------------------------------------------------------------------- #
# 전처리 파라미터 — 근거는 new_pump_preprocessor.md 4절
# --------------------------------------------------------------------------- #
PARAMS = {
    "T_REF": 22.0,               # 점도 기준온도 ℃ (이 라인의 평소 재료온도)
    "TEMP_SCALE": 10.0,          # 온도 원값 0.1℃ → ℃ (Scale_Max___TT = 1000)
    "VISC_TABLE_RAW": [150, 400],  # 점도 조회표 범위 (원값, 15.0~40.0℃)

    "STEADY_MIN": 5,             # 대표 Hz 시작 후 이 틱 이후를 안정 구간 (전류_HzOut 노트북)
    "INJ_TICKS": 4,              # 사출 시작 후 이만큼 안정 통계 제외 (new_vis 12)
    "MIN_CYCLE_TICKS": 8,
    "MIN_CLEAN_TICKS": 3,

    "TCROSS_BAND": 0.03,         # t_cross / FT 타임아웃 목표권 SV ±3% (new_vis 11)
    "TSELF_LEVEL": 0.95,         # t_self: 자기 안정값 95% (now_vis §7)
    "KNEE_FRAC": 0.2,            # 증압→안정 꺾임: 최대 기울기의 20% 미만 (설계정리 4절)

    "DECAY_WINDOW_SEC": 5.0,     # 복귀 구간 = 마지막 켜짐 틱부터 5초 (new_vis 15)
    "DECAY_PT_FLOOR": [2.0, 3.0],  # 압력 바닥값 = 꺼진 뒤 2~3초 PT 중앙값
    "DECAY_FT_FLOOR_FROM": 3.0,  # 유량 바닥값 = 꺼진 뒤 3초 이후 FT 중앙값 (= 20 Hz 순환 유량)
    "DECAY_MIN_TICKS": 4,

    "CIRC_HZ": 2000,             # 사이클 사이 순환 속도 지령 (20.00 Hz)
    "CIRC_MIN_FT": 300,          # 순환 중 FT 가 이보다 작으면 펌프 정지
    "K_SCALE": 1e5,              # 유로 저항 K = PT/FT² × 1e5

    # 실시간 규칙 (설계정리 7절). r_early 분모는 E(SV) → 물리식 기대값으로 바꿨다 — 0.7 은 재검증 필요
    "R_EARLY_SEC": 1.6,          # 켜진 뒤 이 시각에 압력 상승률 판정
    "R_EARLY_TH": 0.7,
    "FT_TIMEOUT_SEC": 3.0,       # 켜진 뒤 이 시각까지 목표권 미진입이면 경보
}

# --------------------------------------------------------------------------- #
# 원료별 점도 — OJT/공정도.xls '온도별 점도 (3)' (2013.5.12, 북경공장) 에 Andrade 식을 맞춘 B
# μ_rel(T) = exp[B · (1/T_K − 1/T_ref,K)]
# 현재 원료 TDS 로 갱신 필요. TM-20 은 I1 데이터 추정(−2.36 %/℃)과 원료표(−2.21 %/℃)가 맞았다
# --------------------------------------------------------------------------- #
MATERIALS = {
    "TM-337":  {"type": "POL", "B": 5756.0, "desc": "폴리올 BACK"},
    "AM-315":  {"type": "POL", "B": 5418.0, "desc": "폴리올 CUSH"},
    "MBR-360": {"type": "POL", "B": 5360.0, "desc": "폴리올 고탄성"},
    "TM-20":   {"type": "ISO", "B": 1921.0, "desc": "ISO"},
    "MC-70":   {"type": "ISO", "B": 2781.0, "desc": "ISO"},
}

# --------------------------------------------------------------------------- #
# 펌프 — tank: 루트 pump_config.PUMP_TO_TANK, head/slot: parkkt/Cycle_Feature_Extractor.PUMP_MAP
# ISO 는 같은 번호 폴리올과 헤드·슬롯을 공유한다 (I1 ↔ P1, 사출 신호 공유)
# material=None / verified=False 인 펌프는 전처리기가 거부한다 — 원료·온도 태그 확인 후 채울 것
# --------------------------------------------------------------------------- #
PUMPS = {
    #       원료        탱크(온도 태그)  헤드    슬롯   검증
    "P1": ("TM-337",  "P1", "HD1", 1, True),    # 1-1펌프 BACK — now_vis / 전처리기 md 5절
    "I1": ("TM-20",   "I1", "HD1", 1, True),    # 1-1펌프 ISO  — I1 노트북 §11
    "P2": (None,      "P5", "HD1", 2, False),   # 1-2펌프 CUSH1 (AM-315 추정)
    "P3": (None,      "P2", "HD2", 1, False),   # 2-1펌프 SOFT
    "P4": (None,      "P4", "HD2", 2, False),   # 2-2펌프 HARD
    "P5": (None,      "P3", "HD3", 1, False),   # 3-1펌프 고탄성 (MBR-360 추정)
    "P6": (None,      "P1", "HD3", 2, False),   # 3-2펌프 BACK2 (TM-337 추정)
    "P7": (None,      "P5", "HD4", 1, False),   # 4-1펌프 CUSH2 (AM-315 추정)
    "I2": (None,      "I3", "HD1", 2, False),
    "I3": (None,      "I2", "HD2", 1, False),
    "I4": (None,      "I2", "HD2", 2, False),
    "I5": (None,      "I3", "HD3", 1, False),
    "I6": (None,      "I1", "HD3", 2, False),
    "I7": (None,      "I3", "HD4", 1, False),
}

ALARM_TAGS = ["AL_Line_Bit", "AL_MA_Bit", "AL_Tank_Bit", "AL_ETC_Bit"]
RUN_TAG = "Auto_Running"


def build():
    pumps = {}
    for pid, (mat, tank, head, slot, ok) in PUMPS.items():
        if mat is not None and mat not in MATERIALS:
            raise ValueError(f"{pid}: 원료 {mat} 가 MATERIALS 에 없음")
        pumps[pid] = {"material": mat, "tank": tank, "head": head, "slot": slot, "verified": ok}
    return {"version": date.today().isoformat(), "generated_by": os.path.basename(__file__),
            "params": PARAMS, "materials": MATERIALS, "pumps": pumps,
            "alarm_tags": ALARM_TAGS, "run_tag": RUN_TAG}


if __name__ == "__main__":
    s = build()
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(s, f, ensure_ascii=False, indent=1)
    ok = [p for p, v in s["pumps"].items() if v["verified"]]
    print(f"생성: {OUT_PATH}  (파라미터 {len(s['params'])}개, 원료 {len(s['materials'])}개, "
          f"펌프 {len(s['pumps'])}개 중 사용 가능 {ok})")
