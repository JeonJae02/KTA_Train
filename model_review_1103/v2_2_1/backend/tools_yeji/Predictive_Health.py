# tools/Predictive_Health.py
from tools_yeji.Pump_Monitor import PumpMonitor
from tools_yeji.Relay_Monitor import RelayMonitor
from tools_yeji.Conveyor_Monitor import ConveyorMonitor


class PredictiveHealth:
    """예지(IDLE) JSON 오케스트레이터. 판정은 펌프/릴레이/컨베이어 각각 별도 모듈
    (PumpMonitor/RelayMonitor/ConveyorMonitor)에 위임하고, 여기서는 모으기만 한다."""

    def __init__(self, model_dir=None):
        # model_dir: 옛 펌프별 AE 폴더(models/anomaly_models). PredictiveEngine 이 아직 넘겨서 받기만 한다.
        # 펌프 사이클 모델은 PumpMonitor 가 models/pump_cycle/ 에서 스스로 읽는다.
        print("🧠 [HealthMonitor] 예지 오케스트레이터 부팅 중...")

        # 14개 펌프 리스트 생성 (P1~P7, I1~I7)
        self.pump_list = [f"P{i}" for i in range(1, 8)] + [f"I{i}" for i in range(1, 8)]

        self.relay_monitor = RelayMonitor()
        self.conveyor_monitor = ConveyorMonitor()
        self.pump_monitor = PumpMonitor(self.pump_list)

    # ---------------------------------------------------------
    # 🏭 3. 메인 JSON 생성부
    # ---------------------------------------------------------
    def generate_idle_json(self, live_data, current_state):
        """이번 틱의 예지 판정. 계산은 각 모니터가 하고 여기서는 모으기만 한다.

        `Pump_Score` 는 14개 펌프 전량이다. 프론트로 나갈 때 PredictiveEngine 이
        `over` 인 것만 골라 `Pump_Anomaly` 로 보내고, 전량은 링버퍼로 간다.
        """
        conveyor, conveyor_warnings = self.conveyor_monitor.check(live_data)
        return {
            "system_mode": current_state,
            # 활성 경고 전체 목록. 한 겹 더 감싸지 않고 배열을 그대로 보냄
            "Relay_Data": self.relay_monitor.check(live_data),
            # 키오스크 그림용 스냅샷 (판정 그대로)
            "conveyor": conveyor,
            # 컨베이어 활성 경고 (근무 단위 유지). uplink·경고 이력용
            "Conveyor_Anomaly": conveyor_warnings,
            # 14개 전량 + over 태그. 거르는 건 PredictiveEngine 이 한다.
            "Pump_Score": self.pump_monitor.check(live_data),
        }
