"""펌프 전처리기 설정 파일(`pump_settings.json`) 생성 스크립트.

설정은 **이 파일을 고치고 다시 실행**해서 바꾼다. JSON 을 손으로 고치지 말 것 —
값마다 근거를 여기 주석으로 남겨 두기 때문이다.

    python HDI_MODEL/make_pump_settings.py            # HDI_MODEL/pump_settings.json 생성

들어가는 것
- params    : 전처리 파라미터 (안정 구간, 밴드, 복귀창, 실시간 규칙 등)
- materials : 원료별 점도 상수 B (Andrade 지수식)
- pumps     : 펌프 → 원료·탱크·헤드·슬롯·이름. **탱크는 HDI_Relay/shared/topology.json 에서 읽는다**
              태그 이름은 전처리기가 만든다 (온도 = Scale_Out___TT_{tank}, 사출 = Shot_Injection_{head}_P{slot})

들어가지 않는 것
- 점도 조회표 : B 로 결정되는 파생값이라 전처리기가 시작할 때 만든다
- 기준선      : 정상 구간에서 학습한 값 → `PumpBaseline.save()` 로 펌프별 JSON
"""

import json
import os
from datetime import date

HERE = os.path.dirname(os.path.abspath(__file__))
OUT_PATH = os.path.join(HERE, "pump_settings.json")
TOPOLOGY_PATH = os.path.normpath(os.path.join(HERE, "..", "..", "HDI_Relay", "shared", "topology.json"))

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

    "CIRC_HZ": 2000,             # 순환 속도 지령 기본값 — 펌프별 값은 CIRC_HZ_OF 가 우선
    "CIRC_MIN_FT": 300,          # 순환 중 FT 가 이보다 작으면 펌프 정지
    "K_SCALE": 1e5,              # 유로 저항 K = PT/FT² × 1e5

    # 실시간 규칙 (설계정리 7절). r_early 분모는 E(SV) → 물리식 기대값으로 바꿨다 — 0.7 은 재검증 필요
    "R_EARLY_SEC": 1.6,
    "R_EARLY_TH": 0.7,
    "FT_TIMEOUT_SEC": 3.0,
}

# --------------------------------------------------------------------------- #
# 원료별 점도 — OJT/공정도.xls '온도별 점도 (3)' (2013.5.12, 북경공장) 에 Andrade 식을 맞춘 B
# μ_rel(T) = exp[B · (1/T_K − 1/T_ref,K)]
# basis: "원료표" = 측정값에 맞춘 것 / "임시" = 원료표에 없어 같은 계열 평균을 넣은 것 (데이터로 추정해 교체할 것)
# --------------------------------------------------------------------------- #
MATERIALS = {
    "TM-337":  {"type": "POL", "B": 5756.0, "desc": "폴리올 BACK", "basis": "원료표"},
    "AM-315":  {"type": "POL", "B": 5418.0, "desc": "폴리올 CUSH", "basis": "원료표"},
    "MBR-360": {"type": "POL", "B": 5360.0, "desc": "폴리올 고탄성", "basis": "원료표"},
    "TM-20":   {"type": "ISO", "B": 1921.0, "desc": "ISO", "basis": "원료표 (I1 데이터 추정 −2.36 %/℃ 와 일치)"},
    "MC-70":   {"type": "ISO", "B": 2781.0, "desc": "ISO", "basis": "원료표"},
    # 원료표에 없는 것 — 같은 계열 평균. **가설 검증 단계에서 데이터로 β 를 추정해 교체한다**
    "POL-GENERIC": {"type": "POL", "B": 5511.0, "desc": "폴리올 (SOFT/HARD 등 원료표 없음)", "basis": "임시 — 폴리올 3종 평균"},
    "ISO-GENERIC": {"type": "ISO", "B": 2351.0, "desc": "ISO/MDI (원료 등급 미확인)", "basis": "임시 — ISO 2종 평균"},
}

# --------------------------------------------------------------------------- #
# 펌프 — 탱크·이름은 topology.json, head/slot 은 parkkt/Cycle_Feature_Extractor.PUMP_MAP.
# ISO 는 같은 번호 폴리올과 헤드·슬롯·사출 신호를 공유한다 (알람 이름 "1-2펌프 CUSH1 / ISO" 등, BuildUp 전환 횟수도 쌍끼리 같음)
#
# 원료 판정 근거 (material_basis)
#   검증     : 데이터로 점도 거동을 확인함 (P1 now_vis, I1 노트북 §11)
#   이름     : 펌프 이름이 원료표 원료와 일치 (BACK → TM-337, CUSH → AM-315, 고탄성 → MBR-360)
#   같은탱크 : 검증된 펌프와 같은 탱크 (같은 원료)
#   임시     : 원료표에 없음 → GENERIC
# verified=True 인 펌프만 전처리기가 기본으로 받는다. 나머지는 PumpSetup(..., allow_unverified=True)
# --------------------------------------------------------------------------- #
HEAD_SLOT = {"P1": ("HD1", 1), "P2": ("HD1", 2), "P3": ("HD2", 1), "P4": ("HD2", 2),
             "P5": ("HD3", 1), "P6": ("HD3", 2), "P7": ("HD4", 1)}
MATERIAL_OF = {   # 펌프 → (원료, 근거, 검증)
    "P1": ("TM-337", "검증", True),
    "P6": ("TM-337", "같은탱크(P1)·이름 BACK", False),
    "P2": ("AM-315", "이름 CUSH", False),
    "P7": ("AM-315", "이름 CUSH", False),
    "P5": ("MBR-360", "이름 고탄성", False),
    "P3": ("POL-GENERIC", "임시 (SOFT)", False),
    "P4": ("POL-GENERIC", "임시 (HARD)", False),
    "I1": ("TM-20", "검증", True),
    "I6": ("TM-20", "같은탱크(I1)", False),
    "I2": ("ISO-GENERIC", "임시 (Tank 3 ISO)", False),
    "I5": ("ISO-GENERIC", "임시 (Tank 3 ISO)", False),
    "I7": ("ISO-GENERIC", "임시 (Tank 3 ISO)", False),
    "I3": ("ISO-GENERIC", "임시 (Tank 2-2 MDI)", False),
    "I4": ("ISO-GENERIC", "임시 (Tank 2-2 MDI)", False),
}

#: 사이클 사이 순환운전 Hz 지령 — 캐시 09-10~10-07 의 대기(BuildUp=0, FT>300) 틱 최빈값.
#: 날마다 100% 같은 값이었다 (순환이 없던 09-20·09-24~26·10-04 제외)
CIRC_HZ_OF = {"P1": 2000, "I1": 2000, "I7": 3000,
              **{p: 2500 for p in ["P2", "I2", "P3", "I3", "P4", "I4", "P5", "I5", "P6", "I6", "P7"]}}

#: 펌프별 파라미터 덮어쓰기. P6/I6(HD3-2) 는 켜짐이 7~9틱으로 짧고 FT 가 4틱째 이미 99% 에 닿는다
#: (09-15 평균 파형) — 기본값(안정 5틱 뒤, 최소 8틱)이면 사이클 절반 가까이가 버려진다
PARAM_OVERRIDES = {"P6": {"STEADY_MIN": 4, "MIN_CYCLE_TICKS": 7},
                   "I6": {"STEADY_MIN": 4, "MIN_CYCLE_TICKS": 7}}

ALARM_TAGS = ["AL_Line_Bit", "AL_MA_Bit", "AL_Tank_Bit", "AL_ETC_Bit"]
RUN_TAG = "Auto_Running"


def read_topology(path=TOPOLOGY_PATH):
    """topology.json → {펌프: (탱크, 이름)}"""
    with open(path, encoding="utf-8") as f:
        topo = json.load(f)
    out = {}
    for d in topo["devices"]:
        if d["type"] != "pump":
            continue
        pid = d["id"].replace("PUMP_", "")
        tanks = [r.replace("TANK_", "") for r in d["relatedDeviceIds"] if r.startswith("TANK_")]
        if len(tanks) != 1:
            raise ValueError(f"{pid}: 탱크가 {len(tanks)}개 — topology 확인 필요")
        out[pid] = (tanks[0], d["name"])
    return out, topo.get("version")


def build():
    topo, topo_ver = read_topology()
    pumps = {}
    for pid, (tank, name) in sorted(topo.items()):
        if pid not in MATERIAL_OF:
            raise ValueError(f"{pid}: MATERIAL_OF 에 없음 — 원료를 정할 것")
        mat, basis, ok = MATERIAL_OF[pid]
        if mat not in MATERIALS:
            raise ValueError(f"{pid}: 원료 {mat} 가 MATERIALS 에 없음")
        head, slot = HEAD_SLOT["P" + pid[1:]]           # ISO 는 같은 번호 폴리올과 공유
        pumps[pid] = {"name": name, "material": mat, "material_basis": basis, "tank": tank,
                      "head": head, "slot": slot, "verified": ok,
                      "circ_hz": CIRC_HZ_OF.get(pid, PARAMS["CIRC_HZ"])}
        if pid in PARAM_OVERRIDES:
            unknown = set(PARAM_OVERRIDES[pid]) - set(PARAMS)
            if unknown:
                raise ValueError(f"{pid}: 모르는 파라미터 {unknown}")
            pumps[pid]["params"] = PARAM_OVERRIDES[pid]
    return {"version": date.today().isoformat(), "generated_by": os.path.basename(__file__),
            "topology_version": topo_ver, "params": PARAMS, "materials": MATERIALS, "pumps": pumps,
            "alarm_tags": ALARM_TAGS, "run_tag": RUN_TAG}


if __name__ == "__main__":
    s = build()
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(s, f, ensure_ascii=False, indent=1)
    print(f"생성: {OUT_PATH}")
    print(f"  topology {s['topology_version']}, 파라미터 {len(s['params'])}개, 원료 {len(s['materials'])}개")
    for pid, v in s["pumps"].items():
        print(f"  {pid:3s} {v['name']:12s} 탱크 {v['tank']:3s} {v['head']} 슬롯{v['slot']} 순환{v['circ_hz'] / 100:.0f}Hz  "
              f"{v['material']:12s} ({v['material_basis']}){'  [검증]' if v['verified'] else ''}")
