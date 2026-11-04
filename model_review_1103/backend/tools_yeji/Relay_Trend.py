# tools_yeji/Relay_Trend.py
"""릴레이 추세 수집기 — 판정은 하지 않고 **기록만** 한다.

추세 판정(비율 / 기울기 / CUSUM)은 고장·정비 이력이 쌓여야 문턱을 정할 수 있어서
지금은 재료만 모아둔다. 근거: DB_Log/HDI_MODEL/추세_변수_정리.md

- 사이클 단위 : 탱크 exchanger 사이클이 확정될 때 / 헤드 SOL 켜짐·꺼짐 때 1행
- 일별 요약   : 오전 6시 경계를 넘을 때 탱크마다 1행 (TankPreprocessor.last_day)

일별 요약은 재시작해도 남아야 하므로 파일에 append 한다(jsonl).
가동 플래그(Auto_Running_PL)는 여기서 거르지 않고 **값을 같이 기록**한다 —
거르는 기준은 추세 로직을 붙일 때 정한다.

Relay_Monitor 가 전처리의 이번 tick 플래그(TankPreprocessor.closed / day_rolled)를 보고
여기로 넘긴다.
"""
import os

DEFAULT_DIR = "./logs/relay_trend"


class RelayTrend:
    def __init__(self, out_dir=DEFAULT_DIR):
        self.out_dir = out_dir
        # TODO: 함께 구현 — 디렉토리 생성, 파일 경로 규칙

    # ------------------------------------------------------------------ #
    # 탱크
    # ------------------------------------------------------------------ #
    def tank_cycle(self, tank, cycle, running):
        """exchanger 사이클 확정 1건.

        cycle  : TankPreprocessor.last_cycle
                 {cycle_start, cycle_end, on_duration_sec, cool_overlap, truncated}
        running: 그 순간의 Auto_Running_PL (None = 아직 못 받음)
        """
        # TODO: 함께 구현
        raise NotImplementedError

    def tank_day(self, tank, last_day):
        """운영일 마감 1건. last_day : TankPreprocessor.last_day
        {day, min_pv, min_lset, min_t, n_glitch, heat_abnormal}
        """
        # TODO: 함께 구현
        raise NotImplementedError

    # ------------------------------------------------------------------ #
    # 헤드
    # ------------------------------------------------------------------ #
    def head_cycle(self, pump, t, on_t=None, off_t=None):
        """SOL 엣지 1건. 켜짐이면 on_t, 꺼짐이면 off_t 를 채운다."""
        # TODO: 함께 구현
        raise NotImplementedError

    # ------------------------------------------------------------------ #
    def _append(self, name, row):
        """jsonl 한 줄 append."""
        # TODO: 함께 구현
        raise NotImplementedError
