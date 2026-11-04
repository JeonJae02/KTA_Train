# tools_yeji/relay_models/tank_params.py
"""탱크 모델 상수.

원본 세 파일에서 실시간 모델(tank_realtime.py)이 쓰는 것만 가져왔다.
  - Tank_Preprocess.py      : TANKS, TEMP_SCALE, MERGE_GAP_SEC, PV_VALID_RANGE
  - Tank_Diagnosis_Model.py : 판정 파라미터, RELAY_IDS, MESSAGES, params()
  - Tank_Realtime_Model.py  : HEAT_CONFIRM_SEC, GATED_REASONS, sv 태그
    (가동 플래그 태그 RUN_FLAG_TAG 는 헤드도 같이 쓰므로 config/operation_config 로 옮겼다)
배치용 로더(load_signals 등)는 InfluxDB/CSV 를 읽는 거라 가져오지 않았다.

## 원본과 다른 점
1. RELAY_IDS 를 손으로 적지 않고 shared/topology.json 에서 뽑는다 (TANK_PARTS).
2. TANKS 의 exch / cool 태그를 한글 태그명이 아니라 **P 주소**로 쓴다.
   live_data 는 감시 태그(tag_config) 로 걸러 들어오는데, 거기엔 주소만 있다.
   한글 태그명은 공백 개수까지 장비마다 달라 맞추기도 어렵다.
"""
from config.topology_config import RELAY_CONFIG

# --------------------------------------------------------------------------- #
# 전처리 파라미터 (Tank_Preprocess)
# --------------------------------------------------------------------------- #
TEMP_SCALE = 10.0               #: 0.1℃ 단위 -> ℃
MERGE_GAP_SEC = 90              #: 이보다 짧은 OFF 는 같은 사이클로 이어붙인다
PV_VALID_RANGE = (5.0, 60.0)    #: ℃. 이 범위 밖 PV 는 센서 글리치로 보고 버린다
#: 샘플이 이보다 오래 안 들어오면(통신 공백 · 진단(ANALYZING) 중 예지 미구동) 그 사이 exchanger 가
#: 어땠는지 모른다 -> 사이클 상태를 버리고 재시작처럼 새로 센다. 90초를 못 봤으면 그 사이 꺼짐이
#: 진짜 꺼짐인지 채터링인지도 가를 수 없으므로 MERGE_GAP_SEC 과 같게 둔다.
GAP_RESET_SEC = MERGE_GAP_SEC

# --------------------------------------------------------------------------- #
# 판정 파라미터 (Tank_Diagnosis_Model)
# --------------------------------------------------------------------------- #
MAX_ON_SEC = 600        #: 사이클이 켜진 뒤 이 시간 안에 안 꺼지면 이상 (규칙 ②)
CONSEC_COOL_N = 10      #: cool 동반이 이만큼 연속되면 성능저하 (규칙 ③)

#: 탱크마다 냉각 능력·사이클 리듬이 다르다. 데이터가 쌓이면 탱크별로 조정한다.
#: (비어 있으면 위 공통값을 그대로 쓴다)
TANK_OVERRIDES = {
    # "I2": {"MAX_ON_SEC": 900},
}

HEAT_CONFIRM_SEC = 0    #: PV < L_Set 이 이만큼 이어져야 heat 이상. 0 이면 한 샘플로 확정(배치와 동일)

# --------------------------------------------------------------------------- #
# 가동 플래그로 거르는 사유 (Tank_Realtime_Model). 플래그 태그는 config/operation_config
# --------------------------------------------------------------------------- #
GATED_REASONS = {"exchanger", "cooler", "exchanger_degrade"}   #: heater 는 항상 띄운다

#: 모델 설정이 안 맞아 경보를 내보내지 않는 탱크. 판정은 계속 돈다(status·추세 확인용). 파라미터 재조정 후 비운다.
MUTED_TANKS = {"I2", "P5"}

# --------------------------------------------------------------------------- #
# 릴레이 ID 매핑 — shared/topology.json 에서 뽑는다
# --------------------------------------------------------------------------- #
#: 부위 -> topology partName 끝 단어. 모델이 보는 부위가 늘면 여기 한 줄 추가.
#: groupId 로는 못 가른다 — heater 와 cooler 가 둘 다 "temp" 그룹이다.
TANK_PARTS = {
    "heater": "Heater",
    "cooler": "Cooling SOL",
    "exchanger": "Exchanger SOL",
}


def _build_relay_ids():
    """{부위: {탱크키: 릴레이 주소}}. 탱크키는 deviceId 의 TANK_ 뒤 (TANK_P1 -> P1).

    탱크마다 부위당 정확히 하나여야 한다. 아니면 topology 가 바뀐 것이니 부팅 때 바로 터뜨린다.
    """
    found = {part: {} for part in TANK_PARTS}
    for addr, r in RELAY_CONFIG.items():
        if not r["deviceId"].startswith("TANK_"):
            continue
        tank = r["deviceId"][len("TANK_"):]
        for part, suffix in TANK_PARTS.items():
            if r["partName"].endswith(suffix):
                if tank in found[part]:
                    raise ValueError(f"[tank_params] {tank} {part} 가 둘 이상: "
                                     f"{found[part][tank]}, {addr}")
                found[part][tank] = addr
    tanks = set().union(*(m.keys() for m in found.values()))
    missing = [f"{t}.{p}" for p in found for t in tanks if t not in found[p]]
    if missing:
        raise ValueError(f"[tank_params] topology 에 없는 부위: {missing}")
    return found


#: 고장 부위별로 보고할 릴레이 주소. 백엔드에서는 릴레이 주소 = 그 SOL 신호 주소다.
#: 원본(Tank_Diagnosis_Model)은 손으로 적은 표였고 cooler.P1 / cooler.I3 / exchanger.I3 가
#: topology 와 달랐다.
RELAY_IDS = _build_relay_ids()

WARNING_TYPE = "thermal"

#: 부위별 메시지. heater 문구가 기준이고 나머지는 같은 구조로 맞췄다
#: ([장치][동작] [증상] · [지표] [추세]).
MESSAGES = {
    "heater":
        "히터 가열 응답 지연 · 설정 온도 도달 시간 증가 추세",
    "exchanger":
        "열교환기 냉각 응답 지연 · 설정 온도 복귀 시간 증가 추세",
    "cooler":
        "쿨러 냉각 응답 지연 · 열교환기 동시 가동에도 설정 온도 미복귀",
    "exchanger_degrade":
        "열교환기 냉각 성능 저하 · 쿨러 동시 가동 연속 발생",
}

#: 같은 ID 에 여러 사유가 겹치면 이 순서로 앞선 것을 쓴다(구조적 문제 우선).
_MESSAGE_PRIORITY = ["exchanger_degrade", "exchanger", "cooler", "heater"]

# --------------------------------------------------------------------------- #
# 탱크별 입력 태그
# --------------------------------------------------------------------------- #
#: exch / cool 은 릴레이 주소와 같으므로 RELAY_IDS 에서 만든다.
#: TK_Temp_* 는 탱크 키와 1:1 이라 자동 생성한다. 0.1℃ 단위 (P1 실측 SV 230 = 23.0℃).
TANKS = {
    key: {"exch": RELAY_IDS["exchanger"][key], "cool": RELAY_IDS["cooler"][key],
          "pv": f"TK_Temp_PV_{key}", "lset": f"TK_Temp_L_Set_{key}", "sv": f"TK_Temp_SV_{key}"}
    for key in RELAY_IDS["exchanger"]
}


def params(tank):
    """탱크별 판정 파라미터 — 공통값에 TANK_OVERRIDES 를 덮어쓴 것. TankJudge 가 생성될 때 읽는다."""
    p = {"MAX_ON_SEC": MAX_ON_SEC, "CONSEC_COOL_N": CONSEC_COOL_N}
    p.update(TANK_OVERRIDES.get(tank, {}))
    return p
