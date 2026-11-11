# tools_yeji/relay_models/tank_realtime.py
"""탱크 heat / exchanger / cool 실시간 진단 — 여러 탱크 + 가동 플래그. 이번 tick 의 경보 목록을 낸다.

원본: DB_Log/HDI_MODEL/Tank_Realtime_Model.py. 원본의 TankRealtime 을 둘로 나눴다.
  - tank_preprocess.TankPreprocessor : 입력 정리(가동 플래그 포함), exchanger 사이클, 운영일 최저 PV
  - tank_judge.TankJudge             : 규칙 ①②③, 같은 이상은 한 번만
이 파일은 탱크마다 그 둘을 한 쌍으로 돌리고, 가동 플래그로 띄울지 정한다.
**얼마나 오래 띄울지**(근무 단위 유지)는 Relay_Monitor 가 정한다 — 원본의 "다음 6시까지 유지"는 거기로 옮겼다.

배치용(`Tank_Diagnosis_Model.py`)과 같은 룰을 **샘플이 들어올 때마다** 판정한다. 하루치
데이터를 들고 있지 않고, 탱크마다 숫자 몇 개짜리 상태만 갖고 간다.

사용법 — 반복문 밖에서 객체 생성, 안에서 update 만:

    mon = TankRealtimeGroup()                       # 반복문 밖에서 한 번
    while True:
        row = {...}                                 # 태그명 -> 원시 PLC 값 (이번 tick 에 들어온 것만)
        alerts = mon.update(current_time, row)      # [tank_judge.build_alert(...), ...]

값은 **원시 PLC 값**(TK_Temp_* 는 0.1℃ 단위)을 그대로 넣는다. 이번 tick 에 안 들어온
태그는 빼거나 None 으로 두면 마지막 값을 이어서 쓴다.

## 가동 플래그 — 띄울지 말지는 여기서

판정은 플래그와 상관없이 돌고, 새로 난 이상을 **그 순간 `Auto_Running_PL`(PLC 자동운전
표시등)이 1 일 때만** 띄운다(show=True).

  - exchanger / cooler / 성능저하 -> 가동 중일 때만 띄운다. 실측(9/9~10/6, 8탱크)에서
    10분 초과 126건 중 103건이 비가동 중이었고, 대부분 생산 시작(06:50) 전 05~06시
    예열 구간이었다.
  - heater -> 항상 띄운다. 탱크 온도가 L_Set 밑으로 가는 건 가동과 무관하게 이상이다.

새로 난 게 아니라 계속 성립 중인 사유는 show=False 로 낸다 — 이미 떠 있으면 유지만 하고,
안 떠 있으면(비가동이라 막혔던 것) 새로 띄우지 않는다.

플래그가 아직 한 번도 안 들어왔으면 가동 중으로 보고 띄운다 — 태그가 빠졌을 때 경보가
조용히 사라지는 것보다 낫다. 플래그는 원래 값 그대로 쓴다. 자동운전 시작 순간 1~2초
깜빡임이 있지만, 120초 유지 처리를 넣어도 결과가 거의 같았다(이상 24건 vs 23건).
"""
from collections import deque

from config.operation_config import RUN_FLAG_TAG
from tools_yeji.common.time_utils import to_kst
from tools_yeji.relay_models.tank_judge import TankJudge, build_alert
from tools_yeji.relay_models.tank_params import GATED_REASONS, MUTED_TANKS, TANKS
from tools_yeji.relay_models.tank_preprocess import TankPreprocessor


class TankRealtimeGroup:
    """탱크 여러 개 + 가동 플래그.

    row 는 {태그명: 원시 PLC 값} — tank_params.TANKS 의 태그명 (sv 포함) + 가동 플래그(`RUN_FLAG_TAG`).
    탱크마다 pre.update -> judge.update(t, pre) 순서로 돈다. 추세 수집은 self.pre[탱크] 의
    closed / day_rolled 와 last_cycle / last_day 를 보면 된다.
    """

    def __init__(self, tanks=None, run_flag_tag=RUN_FLAG_TAG):
        tanks = list(tanks or TANKS)
        self.pre = {t: TankPreprocessor(t) for t in tanks}
        self.judge = {t: TankJudge(t) for t in tanks}
        self.run_flag_tag = run_flag_tag
        self.suppressed = deque(maxlen=200)  # 비가동이라 안 띄운 이벤트 (확인용)

    def update(self, current_time, row):
        """반환: 이번 tick 의 경보 목록 [build_alert(...)]. 조건이 성립한 사유마다 1건."""
        t = to_kst(current_time)
        running = row.get(self.run_flag_tag)
        alerts = []
        for tank, pre in self.pre.items():
            tags = TANKS[tank]
            pre.update(t, exch=row.get(tags["exch"]), cool=row.get(tags["cool"]),
                       pv=row.get(tags["pv"]), lset=row.get(tags["lset"]), sv=row.get(tags["sv"]),
                       running=running)
            judge = self.judge[tank]
            judge.update(t, pre)
            if tank in MUTED_TANKS:
                continue

            shown = set()   # 이번 tick 에 새로 났고 플래그도 통과한 (part, 사유)
            for et, part, reason in judge.new_events:
                if reason in GATED_REASONS and pre.running == 0:
                    self.suppressed.append((et, tank, reason))
                else:
                    shown.add((part, reason))
            for part, reason in dict.fromkeys(judge.asserted):    # 순서 유지 중복 제거
                alerts.append(build_alert(tank, part, reason, show=(part, reason) in shown))
        return alerts

    def status(self, now=None):
        now = to_kst(now) if now is not None else None
        return {t: {**self.pre[t].status(now), **self.judge[t].status()} for t in self.pre}
