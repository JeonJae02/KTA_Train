# config/operation_config.py
"""공유 공장 운영 기준(근무표) 로더.

단일 기준은 저장소 루트의 `shared/operation.json`이고 프론트엔드도 같은 파일을 읽음.
시작할 때 한 번 읽고 메모리에 보관.

근무자는 휴게 시간에 교대한다고 본다. 근무 = 휴게와 휴게 사이.
경보는 "경보 시각 이후 처음 시작하는 근무가 끝날 때까지" 유지한다
(tools_yeji.common.time_utils.hold_until). 마지막 근무에서 난 경보는 다음 날 첫 근무 끝까지 간다.
"""
import json
import os

_CANDIDATE_PATHS = (
    os.environ.get("OPERATION_PATH"),
    "./shared/operation.json",
    os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "shared", "operation.json"),
)


def _resolve_path():
    for candidate in _CANDIDATE_PATHS:
        if candidate and os.path.exists(candidate):
            return candidate
    return None


def load_operation():
    """공유 운영 기준을 읽어 dict로 반환. 찾지 못하면 빈 구조를 반환"""
    path = _resolve_path()
    if not path:
        print(f"⚠️ [Operation] 공유 근무표를 찾지 못했습니다. 탐색 경로: {[p for p in _CANDIDATE_PATHS if p]}")
        return {"version": "", "shifts": []}

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        print(f"⚠️ [Operation] 공유 근무표 로드 실패: {path} — {e}")
        return {"version": "", "shifts": []}

    shifts = data.get("shifts", [])
    n_tmp = sum(1 for s in shifts if s.get("provisional"))
    print(f"🕘 [Operation] 공유 근무표 로드 완료: {path} (근무 {len(shifts)}개, 임시 {n_tmp}개)")
    return data


OPERATION = load_operation()

#: [(시작, 끝)] "HH:MM", 한국시간, 시각 순. 비어 있으면 hold_until 이 다음 06:00 으로 대신한다
SHIFTS = [(s["start"], s["end"]) for s in OPERATION.get("shifts", [])]

#: 가동 플래그(M00004, PLC 자동운전 표시등). 백엔드만 쓰는 PLC 태그라 shared 에 두지 않는다.
#: 탱크·헤드 모두 이 값으로 경보를 거른다 — 비가동(0) 중 새로 난 이상은 띄우지 않는다.
#: 실측상 06:50, 09:00, 11:30 등 생산 시작 시각에 정확히 켜진다(Tank_Realtime_정리.md 5절).
RUN_FLAG_TAG = "Auto_Running_PL"
