# tools_yeji/Conveyor_Health.py
"""컨베이어 대차 통과 센서 실시간 판정 (1차 배포).

예전 MOCK 과 같은 자리·같은 이름이다: PredictiveHealth 가 tick 마다 ConveyorMonitor().read(live_data) 를 부른다.
판정은 tools_yeji/conveyor_models (DB_Log/Conv 의 conv_rules · conv_realtime 을 그대로 옮긴 것).

반환 형태는 바뀌었다 — 프론트 계약은 relay_port/PORTING.md "출력" 절.
예전 키 중 carrier("대차번호-05") 는 그대로 둔다. sensors(센서별 숫자)는 없앤다 (숫자 대신 상태만 낸다).
"""
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from tools_yeji.conveyor_models.conv_realtime import ConveyorRealtime, REQUIRED_TAGS

KST = ZoneInfo("Asia/Seoul")
#: 가동일 기록 (오전 6시에 하루치를 더한다). 재시작해도 승인·복귀 판정과 "언제부터" 가 이어진다
HISTORY_PATH = os.path.join(".", "data", "conveyor_history.json")


class ConveyorMonitor:
    def __init__(self, history_path=HISTORY_PATH):
        self._rt = ConveyorRealtime(history_path=history_path)
        self._tags_checked = False

    def read(self, live_data=None, now=None):
        """live_data: 감시 태그(tag_config)로 걸러진 {주소/태그명: 원시 PLC 값}. now 는 재생 확인용(epoch 초)."""
        live_data = live_data or {}
        if not self._tags_checked and live_data:
            missing = [t for t in REQUIRED_TAGS if t not in live_data]
            if missing:
                print(f"⚠️ [ConveyorMonitor] 감시 태그 없음 (tag_config 확인): {missing}")
            self._tags_checked = True
        t = datetime.fromtimestamp(now, KST) if now is not None else datetime.now(KST)
        return self._rt.read(live_data, t)
