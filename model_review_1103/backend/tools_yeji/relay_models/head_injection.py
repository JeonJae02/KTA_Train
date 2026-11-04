# tools_yeji/relay_models/head_injection.py
"""헤드 인젝션 ON_T / OFF_T 문턱 진단.

원본: DB_Log/HDI_MODEL/Head_health.py
원본은 (4, 2) 배열(헤드 × 펌프)에 HD4_P2 자리를 0 으로 채워 넣는 함수 형태이고,
주소(D0470x, relay_id)를 손으로 적었다. 여기서는 topology 의 partName 으로 펌프별
주소를 뽑아(HEAD_PUMPS) 빈 자리 처리와 손표를 없앤다.

## 판정 (원본 그대로)
- ON_T  >= ON_MAX_SEC  (17초) 이면 이상
- OFF_T >= OFF_MAX_SEC (10초) 이면 이상
- 둘 중 하나라도 이상이면 그 펌프의 인젝션 SOL 릴레이로 보고
- 얼마나 오래 띄울지(근무 단위 유지)는 Relay_Monitor 가 정한다

## 가동 플래그 (탱크와 같은 Auto_Running_PL)
원본 주석의 "공장 작동 후 수 분 뒤부터 수집(추후 추가)" 대신 가동 플래그로 통일한다.
이상이 성립하는 동안 매 tick 경보를 내되, 가동 중(플래그 1, 또는 아직 못 받음)이면 show=True,
비가동이면 show=False — 이미 떠 있으면 유지만 하고 새로 띄우지 않는다.
재가동 직후 첫 사이클의 튀는 값은 플래그가 이미 1 이라 걸러지지 않는다. 오탐이 보이면
SOL 엣지로 "재가동 후 첫 사이클 제외"를 붙인다(추세 작업과 같이).

## 주의 (추세_변수_정리.md 2-1)
ON_T / OFF_T 는 사이클이 끝날 때만 갱신되고 그 사이엔 이전 값을 유지한다.
문턱 판정은 이대로도 되지만, 추세(사이클 단위 수열)를 모으려면 SOL 켜짐/꺼짐 시점에
값을 읽어야 한다 — SOL 주소가 relay 와 같다.
"""

from config.operation_config import RUN_FLAG_TAG
from config.topology_config import RELAY_CONFIG
from tools_yeji.common.time_utils import is_missing

ON_MAX_SEC = 17     #: ON_T 상한 [초]
OFF_MAX_SEC = 10    #: OFF_T 상한 [초]

WARNING_TYPE = "timing"
REASON = "injection_time"   #: 경보 사유 키 (헤드는 사유가 하나뿐)

# --------------------------------------------------------------------------- #
# 펌프별 주소 — shared/topology.json 에서 뽑는다
# --------------------------------------------------------------------------- #
#: 타이머 -> partName 끝 단어. 펌프 키(HD1_P1)는 partName 앞부분에서 뽑는다.
HEAD_PARTS = {
    "on_t":  "인젝션 ON 타이머",
    "off_t": "인젝션 OFF 타이머",
}

#: 인젝션 SOL = 경보를 보낼 릴레이 (원본 relay_id). partName 에 펌프 번호가 없고
#: 표기도 제각각이라 이름으로 직접 대응한다. HD2 대응은 미확인(추세_변수_정리.md 2-1).
HEAD_SOL = {
    "HD1_P1": "Head_1 Injection SOL",
    "HD1_P2": "Head_1-2 Injection SOL",
    "HD2_P1": "Head_2 Injection 1 SOL",
    "HD2_P2": "Head_2 Injection_2 SOL",
    "HD3_P1": "Head_3 Injection SOL",
    "HD3_P2": "Head_3-2 Injection SOL",
    "HD4_P1": "Head_4 Injection SOL",
}


def _build_head_pumps():
    """{펌프: {"on_t": 주소, "off_t": 주소, "sol": 주소}}. tank_params._build_relay_ids 와 같은 방식.

    펌프마다 셋 다 정확히 하나여야 한다. 아니면 topology 가 바뀐 것이니 부팅 때 바로 터뜨린다.
    """
    sol_to_pump = {name: pump for pump, name in HEAD_SOL.items()}
    found = {}

    def put(pump, key, addr):
        slot = found.setdefault(pump, {})
        if key in slot:
            raise ValueError(f"[head_injection] {pump} {key} 가 둘 이상: {slot[key]}, {addr}")
        slot[key] = addr

    for addr, r in RELAY_CONFIG.items():
        if not r["deviceId"].startswith("HEAD_"):
            continue
        name = r["partName"]
        if name in sol_to_pump:
            put(sol_to_pump[name], "sol", addr)
            continue
        for key, suffix in HEAD_PARTS.items():
            if name.endswith(suffix):
                put(name.split()[0], key, addr)     # "HD1_P1 인젝션 ON 타이머" -> HD1_P1

    missing = [f"{p}.{k}" for p in found for k in ("sol", *HEAD_PARTS) if k not in found[p]]
    if missing:
        raise ValueError(f"[head_injection] topology 에 없는 부위: {missing}")
    return found


HEAD_PUMPS = _build_head_pumps()


def message(pump):
    """'HD1_P2' -> 원본 문구 '헤드1 P2 인젝션 (로드) 작동 시간이 길어지고 있습니다.'"""
    head, p = pump.split("_")
    return f"헤드{head[2:]} {p} 인젝션 (로드) 작동 시간이 길어지고 있습니다."


class HeadInjection:
    """헤드 펌프 전체의 인젝션 시간 진단.

    사용법은 탱크 모델과 맞춘다 — 반복문 밖에서 생성, 안에서 update 만.

        mon = HeadInjection()
        alerts = mon.update(current_time, row)   # [{relay, reason, priority, type, message, show}, ...]
    """

    def __init__(self, pumps=None, run_flag_tag=RUN_FLAG_TAG):
        self.pumps = pumps or HEAD_PUMPS
        self.run_flag_tag = run_flag_tag
        # 마지막 값 — 이번 tick 에 안 들어온 태그는 이걸 이어서 쓴다 (탱크 모델과 같은 규칙)
        self.on_t = {p: None for p in self.pumps}
        self.off_t = {p: None for p in self.pumps}
        self.running = None     # 가동 플래그. None = 한 번도 못 받음 (가동 중으로 본다)

    def update(self, current_time, row):
        """샘플 1개 반영. 반환: 이번 tick 의 경보 목록 (tank_judge.build_alert 와 같은 형식).

        current_time 은 지금은 안 쓴다 — 탱크 모델과 호출 형태를 맞춘 것이고,
        추세용 SOL 엣지 검출을 붙일 때 쓴다.
        """
        if not is_missing(row.get(self.run_flag_tag)):
            self.running = int(row[self.run_flag_tag])
        for p, a in self.pumps.items():
            if row.get(a["on_t"]) is not None:
                self.on_t[p] = row[a["on_t"]]
            if row.get(a["off_t"]) is not None:
                self.off_t[p] = row[a["off_t"]]

        show = self.running != 0
        return [{"relay": a["sol"], "reason": REASON, "priority": 0,
                 "type": WARNING_TYPE, "message": message(p), "show": show}
                for p, a in self.pumps.items() if self._abnormal(p)]

    def warning(self):
        """플래그를 거치지 않은 모델 판정 -> relay_warning dict (확인·비교용)."""
        return {a["sol"]: {"type": WARNING_TYPE, "message": message(p)}
                for p, a in self.pumps.items() if self._abnormal(p)}

    def _abnormal(self, p):
        """ON_T / OFF_T 둘 중 하나라도 상한 이상이면 이상."""
        on, off = self.on_t[p], self.off_t[p]
        return (on is not None and on >= ON_MAX_SEC) or (off is not None and off >= OFF_MAX_SEC)
