# tools_yeji/Conveyor_Health.py
"""컨베이어 실시간 판정 — 코어(대차 통과 센서 6개) + 유닛(전진·후진·에어백 실린더).

예전 MOCK 과 같은 자리·같은 이름이다: PredictiveHealth 가 tick 마다 ConveyorMonitor().read(live_data) 를 부른다.
판정은 tools_yeji/conveyor_models (DB_Log/Conv 의 core_* · unit_* · conveyor_monitor 를 그대로 옮긴 것).
반환 형태는 relay_port/PORTING.md "출력" 절. 예전 키 carrier 는 그대로, sensors(센서별 숫자)는 없앤다.
"""
from datetime import datetime
from zoneinfo import ZoneInfo

from tools_yeji.conveyor_models.conveyor_monitor import ConveyorMonitor as _Monitor
from tools_yeji.conveyor_models.conveyor_monitor import all_required_tags

KST = ZoneInfo("Asia/Seoul")
#: 가동일 기록 (오전 6시에 하루치를 더한다). 재시작해도 승인·복귀 판정과 "언제부터" 가 이어진다
DATA_DIR = "./data"


class ConveyorMonitor:
    def __init__(self, data_dir=DATA_DIR):
        self._m = _Monitor(data_dir=data_dir)
        self._tags_checked = False

    def read(self, live_data=None, now=None):
        """live_data: 감시 태그(tag_config)로 걸러진 {주소/태그명: 원시 PLC 값}. now 는 재생 확인용(epoch 초)."""
        live_data = live_data or {}
        if not self._tags_checked and live_data:
            missing = [t for t in all_required_tags() if t not in live_data]
            if missing:
                print(f"⚠️ [ConveyorMonitor] 감시 태그 없음 (tag_config 확인): {missing}")
            self._tags_checked = True
        t = datetime.fromtimestamp(now, KST) if now is not None else datetime.now(KST)
        return self._m.read(live_data, t)
