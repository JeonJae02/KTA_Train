# config/topology_config.py
"""공유 topology 로더.

단일 기준은 저장소 루트의 `shared/topology.json`이고 프론트엔드도 같은 파일을 읽음.
시작할 때 한 번 읽고 메모리에 보관.
"""
import json
import os

_CANDIDATE_PATHS = (
    os.environ.get("TOPOLOGY_PATH"),
    "./shared/topology.json",
    os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "shared", "topology.json"),
)


def _resolve_path():
    for candidate in _CANDIDATE_PATHS:
        if candidate and os.path.exists(candidate):
            return candidate
    return None


def load_topology():
    """공유 topology를 읽어 dict로 반환. 찾지 못하면 빈 구조를 반환"""
    path = _resolve_path()
    if not path:
        print(f"⚠️ [Topology] 공유 topology를 찾지 못했습니다. 탐색 경로: {[p for p in _CANDIDATE_PATHS if p]}")
        return {"version": "", "devices": [], "groups": {}, "relays": {}}

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        print(f"⚠️ [Topology] 공유 topology 로드 실패: {path} — {e}")
        return {"version": "", "devices": [], "groups": {}, "relays": {}}

    print(
        f"🗺️ [Topology] 공유 topology 로드 완료: {path} "
        f"(설비 {len(data.get('devices', []))}개, 릴레이 {len(data.get('relays', {}))}개)"
    )
    return data


TOPOLOGY = load_topology()

#: 릴레이주소 -> {deviceId, groupId, partName}
RELAY_CONFIG = TOPOLOGY.get("relays", {})

#: 디바이스ID -> {id, type, name, relatedDeviceIds}
DEVICE_BY_ID = {device["id"]: device for device in TOPOLOGY.get("devices", [])}


def get_device_by_relay(p_address):
    """P 주소가 속한 설비 정보를 반환. 없으면 None"""
    relay = RELAY_CONFIG.get(p_address)
    return DEVICE_BY_ID.get(relay["deviceId"]) if relay else None


# =========================================================================
# 🏷️ 경고 유형(summary.type) 매핑
# =========================================================================
# 프론트는 `analysis_report.summary.type` 을 **아이콘 키**로만 쓴다
# (frontend/src/components/prediction/warning-icons.ts). 받는 어휘가 6종뿐이고
# 그 밖의 값은 전부 TriangleAlert 폴백이라, 여기서 그 6종으로 좁혀 보낸다.
#
#   level / pressure / thermal / temperature / vibration / trip
#
# `final_verdict.type` 은 이것과 **다른 값**이다. 그쪽은 워크플로우가
# verdict/confidence/coverage 로 정하는 진단 분류("부품 이상", "파급 알람" …)라
# 아이콘 키로 쓸 수 없다. 이름만 같고 의미가 달라서 섞으면 안 된다.
#
# 매핑 못 하는 그룹은 **키를 만들지 않는다.** 프론트가 `?? 'unknown'` 으로
# 폴백하므로 빈 값을 억지로 채울 이유가 없고, `null` 을 보내면 프론트
# 검증기(isOptionalString)가 페이로드를 통째로 버린다.
WARNING_TYPE_BY_GROUP = {
    # 수위
    "level_h": "level",
    "level_l": "level",
    # 온도 계통
    "temp": "thermal",
    "jacket": "thermal",
    "exchanger": "thermal",
    # 압력 계통
    "pressure": "pressure",
    "relief_valve": "pressure",
    # 전기 보호
    "trip": "trip",
    "pump_power": "trip",
    # 회전체
    "motor": "vibration",
    "coupling": "vibration",
    "agitator": "vibration",
}

#: groupId 로 못 정할 때의 2차 근거. 후보의 `kind` 는 인덱스가 붙여준 값이라
#: topology 에 없는 물리점(후보 269종 중 103종)에서도 살아 있다.
WARNING_TYPE_BY_KIND = {
    "protection": "trip",
}


def warning_type_for_relay(p_address, kind=None):
    """`summary.type` 에 실을 아이콘 키. 정할 수 없으면 None (키를 빼라는 뜻)."""
    relay = RELAY_CONFIG.get(p_address)
    if relay:
        mapped = WARNING_TYPE_BY_GROUP.get(relay.get("groupId"))
        if mapped:
            return mapped
    return WARNING_TYPE_BY_KIND.get(kind)
