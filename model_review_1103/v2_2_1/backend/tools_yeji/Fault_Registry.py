# tools_yeji/Fault_Registry.py
import time

from tools_yeji.common.time_utils import to_utc_iso


class ActiveFaultRegistry:
    """
    '지금 활성 상태인 이상/경고' 목록을 유지하는 범용 레지스트리.

    - 같은 key로 계속 touch() 되는 동안(=조건이 계속 유지되는 동안)에는
      최초 발생 시각(time)을 그대로 유지한 채 나머지 필드만 최신화한다.
      → 프론트는 이 time으로 "얼마나 지속됐는지"를 계산/표시할 수 있다.
    - 이번 틱에 touch() 되지 않은 key는 조건이 해소된 것으로 보고 sync()에서 제거한다.
    """

    def __init__(self):
        self._active = {}  # key -> event dict (time 포함)

    def touch(self, key, **fields):
        """이번 틱에 조건이 (계속 혹은 새로) 감지됐음을 기록하고 최신 이벤트를 반환한다."""
        if key not in self._active:
            self._active[key] = {"time": to_utc_iso(time.time()), **fields}
        else:
            self._active[key].update(fields)  # 최초 발생 시각(time)은 건드리지 않음
        return self._active[key]

    def sync(self, seen_keys):
        """이번 틱에 touch되지 않은 key는 해소된 것으로 보고 제거한다."""
        for key in list(self._active.keys()):
            if key not in seen_keys:
                del self._active[key]

    def values(self):
        return list(self._active.values())
