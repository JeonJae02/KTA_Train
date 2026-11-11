# config/alarm_mapping_config.py
"""공유 알람 문구 로더.

단일 기준은 저장소 루트의 `shared/alarm_mapping.json`이고 프론트엔드도 같은 파일을 읽음.
시작할 때 한 번 읽고 메모리에 보관.
"""
import json
import os

_CANDIDATE_PATHS = (
    os.environ.get("ALARM_MAPPING_PATH"),
    "./shared/alarm_mapping.json",
    os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "shared", "alarm_mapping.json"),
)


def _resolve_path():
    for candidate in _CANDIDATE_PATHS:
        if candidate and os.path.exists(candidate):
            return candidate
    return None


def load_alarm_mapping():
    """공유 알람 문구를 읽어 dict로 반환. 찾지 못하면 빈 dict를 반환"""
    path = _resolve_path()
    if not path:
        print(f"⚠️ [AlarmMapping] 공유 알람 문구를 찾지 못했습니다. 탐색 경로: {[p for p in _CANDIDATE_PATHS if p]}")
        return {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        print(f"⚠️ [AlarmMapping] 공유 알람 문구 로드 실패: {path} — {e}")
        return {}

    # 문구가 비어 있는 항목은 조회 대상에서 제외
    alarms = data.get("alarms", {})
    mapping = {k: v.strip() for k, v in alarms.items() if isinstance(v, str) and v.strip()}
    print(f"🔔 [AlarmMapping] 공유 알람 문구 로드 완료: {path} (문구 {len(mapping)}개)")
    return mapping


#: 알람 이름 -> 표시 문구
ALARM_MESSAGES = load_alarm_mapping()


def get_alarm_message(alarm_name):
    """알람 이름에 대응하는 표시 문구를 반환. 없으면 None"""
    return ALARM_MESSAGES.get(alarm_name)
