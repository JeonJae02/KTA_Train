"""[유닛 · 데이터] 에어 주입 유닛 3곳 — 무엇을 받고, 사이클에서 무엇을 재는지.

세 유닛은 PLC 로직이 같다 (Air_Supply_Unit.il). 순서·에러 타이머(스톱퍼 2초, 노즐 전진 1초, 노즐 후진 2초,
복귀 2초)·0.5초 지연 두 개가 모두 같고, 다른 것은 아래 설정값뿐이다. 그래서 클래스 하나에 설정 셋으로 간다.

| 다른 점          | 전진                  | 후진                  | 에어백                        |
| 트리거           | R00055 30 + 금형 CY   | R00060 27 + 금형 CY   | R00050 96 (클로징 센서 카운터) |
| 노즐 후진감지    | 1·2 입력 공유         | 공유                  | 따로 2개                      |
| 에어 주입 최대   | 10초                  | 10초                  | 3초                           |
| 정위치 이탈 타임아웃 | 없음              | 없음                  | 5초 (T0109)                   |

사이클 하나 (스톱퍼 SOL ON → 다음 스톱퍼 SOL ON)
  1 스톱퍼 SOL ON → 스톱퍼 후진감지 OFF → 전진감지 ON
  2 대차가 스톱퍼에 걸려 이동대가 끌려감 → 정위치 OFF
  3 노즐 SOL ON → 노즐 후진감지 OFF → 0.5초 뒤 노즐 오픈(에어 주입)
  4 끝단 후진 → 오픈 닫힘 → 0.5초 뒤 노즐 SOL OFF → 노즐 후진감지 ON
  5 스톱퍼 SOL OFF → 전진감지 OFF → 후진감지 ON = 원점복귀 SOL ON → 정위치 ON

지표마다 판정이 뜨는 부위가 정해져 있다 (METRIC_PART). 주소는 여기 UNITS 가, device·group 은 shared/topology.json 이
기준이다. 부팅 때 둘이 맞는지 확인한다.
"""

from config.topology_config import RELAY_CONFIG

MAX_GAP = 1.2           #: 초. 이보다 긴 샘플 간격 사이의 변화는 시각을 모른다고 본다.
#                          0.75초로 줄이면(스로틀로 빠진 약 1초 간격을 버리면) 짧은 동작 시간(0.1 ~ 0.4초)이 엣지 입력에서
#                          −2 ~ −4.6σ 낮게 치우친다 — 길게 잰 사이클이 더 자주 버려지기 때문 (2026-11-20 확인, con_check/stepwise/unit_bias.py).
#                          엣지 측정의 치우침(주입 시간 등)은 UNIT_REF 를 엣지와 같은 방식으로 잡아 상쇄한다
MAX_CYCLE = 30.0        #: 초. 사이클이 이보다 길면 중간에 라인이 멈춘 것 — 지표에서 뺀다

#: 유닛 정의. device 는 topology deviceId, 나머지 값은 신호 키 → 비트(주소). nozzle_rev 가 두 개면 노즐 1·2 를 따로 잰다
#: rev_latch: 끝단 후진은 0.5초 펄스라 원본은 67–79% 만 잡힌다. 끝단 도착 순간 SET 되어 원점복귀까지
#:            유지되는 PLC 래치를 대신 쓴다 (99% 이상).
UNITS = {
    "md_fwd": dict(device="CONVEYOR_MD_FWD",
                   stop_sol="P00470", nozzle_sol="P00471", return_sol="P00472", open_sol="P00473",
                   org="P00452", rev="P00453", flt="P00454", stop_fwd="P00455", stop_rev="P00456",
                   nozzle_rev=["P00457"], rev_latch="Air_Supply2_Rev_PX_CHK"),
    "md_rev": dict(device="CONVEYOR_MD_REV",
                   stop_sol="P00474", nozzle_sol="P00475", return_sol="P00476", open_sol="P00477",
                   org="P0045A", rev="P0045B", flt="P0045C", stop_fwd="P0045D", stop_rev="P0045E",
                   nozzle_rev=["P0045F"], rev_latch="Air_Supply3_Rev_PX_CHK"),
    "airbag": dict(device="CONVEYOR_AIRBAG",
                   stop_sol="P0028A", nozzle_sol="P0028B", return_sol="P0028C", open_sol="P00243",
                   org="P00131", rev="P00132", flt="P00133", stop_fwd="P00134", stop_rev="P00135",
                   nozzle_rev=["P00136", "P00137"], rev_latch="Air_Supply_Rev_PX_CHK"),
}

#: 사이클 지표: (이름, 시작 신호, 시작 변화, 끝 신호, 끝 변화, 단위, 설명). 변화 +1 = 켜짐, −1 = 꺼짐
#: 시간(s)은 P 비트끼리 잰다 (DB 수집기가 P 블록을 한 순간에 읽음). 거리(cnt)는 코어의 체인 위치로 잰다.
METRICS = [
    ("stop_fwd_start", "stop_sol", +1, "stop_rev", -1, "s", "스톱퍼 전진 출발: SOL ON → 후진감지 OFF"),
    ("stop_fwd_time", "stop_sol", +1, "stop_fwd", +1, "s", "스톱퍼 전진 시간: SOL ON → 전진감지 ON"),
    ("stop_rev_start", "stop_sol", -1, "stop_fwd", -1, "s", "스톱퍼 후진 출발: SOL OFF → 전진감지 OFF"),
    ("stop_rev_time", "stop_sol", -1, "stop_rev", +1, "s", "스톱퍼 후진 시간: SOL OFF → 후진감지 ON"),
    ("nozzle_fwd_start", "nozzle_sol", +1, "nozzle_rev", -1, "s", "노즐 전진 출발: SOL ON → 후진감지 OFF"),
    ("nozzle_rev_time", "nozzle_sol", -1, "nozzle_rev", +1, "s", "노즐 후진 시간: SOL OFF → 후진감지 ON"),
    ("return_time", "stop_rev", +1, "org", +1, "s", "원점 복귀 시간: 스톱퍼 후진감지 ON(= 복귀 SOL ON) → 정위치 ON"),
    ("catch_cnt", "stop_fwd", +1, "org", -1, "cnt", "대차가 걸리기까지: 스톱퍼 전진감지 ON → 정위치 OFF"),
    ("drag_cnt", "org", -1, "rev_latch", +1, "cnt", "끌림 거리: 정위치 OFF → 끝단 후진 ON (이동대가 끌려간 거리)"),
    ("air_time", "open_sol", +1, "open_sol", -1, "s", "에어 주입 시간: 노즐 오픈 SOL ON → OFF"),
]
TIME_METRICS = [m[0] for m in METRICS if m[5] == "s"]
COUNT_METRICS = [m[0] for m in METRICS if m[5] == "cnt"]
LABEL = {m[0]: m[6].split(":")[0] for m in METRICS}

#: 지표 → 그 판정이 뜨는 부위의 대표 신호. 그 주소가 경고의 relay 이고 device·group 은 topology 에서 읽는다.
#: 실린더 동작 시간은 SOL 로 낸다 (헤드 인젝션 시간 경고와 같은 방식). None 은 주소로 특정 못 하는 부위 —
#: 거리 변화는 정위치·후진 센서 중 어느 쪽인지 모르므로 device 의 SLIDE_GROUP 묶음으로 낸다.
#: 에어 주입 시간도 묶음으로 — 주입 SOL 은 PLC 가 켜고(노즐 후진감지 OFF + 0.5초) 끄므로(끝단 후진 도착) 밸브 응답이
#: 아니라 이동대가 끌려간 시간이다 (Air_Supply_Unit.il: Nozzle_Open_Dly → Nozzle_SOL_Open, Rev_PX_CHK 에서 RST)
METRIC_PART = {
    "stop_fwd_start": "stop_sol", "stop_fwd_time": "stop_sol",
    "stop_rev_start": "stop_sol", "stop_rev_time": "stop_sol",
    "nozzle_fwd_start": "nozzle_sol", "nozzle_rev_time": "nozzle_sol",
    "return_time": "return_sol",
    "air_time": None,
    "catch_cnt": None, "drag_cnt": None,
}
SLIDE_GROUP = "slide_sensor"

#: 사이클에서 쓰는 신호 키 (실시간은 rev 원본 대신 rev_latch 를 쓴다)
SIGNAL_KEYS = ["stop_sol", "nozzle_sol", "return_sol", "open_sol", "org", "flt", "stop_fwd", "stop_rev",
               "nozzle_rev", "rev_latch"]

#: 지표별 기준 (장기 기준값, 날짜 간 흔들림 σ) — **엣지가 재는 방식**으로 잰 값 (2026-11-20, model_review_1103/con_check/stepwise).
#:   DB 원시에 엣지 예지 엔진의 0.5초 스로틀을 걸어 이 코드에 흘린 가동일 평균, 10-14 ~ 11-14 (27 가동일).
#:   (재생 검증은 10-14 ~ 10-24 10일 기준으로 했다. 배포용은 σ 를 안정적으로 잡으려고 기간을 늘렸다 — 기준값 이동은 대부분 1σ 안,
#:    σ 는 4개 지표가 1.3배 넘게 커짐. 에어백 복귀 시간의 10-19 ~ 20 계단(0.61 → 0.57초)은 새 정상으로 들어간다)
#:   엣지 측정에는 스로틀로 빠진 메시지 때문에 치우침이 있다(주입 시간 +0.045초 등). 기준도 같은 방식으로 잡아 상쇄한다.
#:   이 재생은 엣지 자신의 가동일 기록과 지표 평균 0.11σ 안으로 맞는다.
#:   10-14 부터인 이유: 현장 PC 메시지 간격이 10-09 ~ 14 무렵 0.522 → 0.515초로 바뀌어 스로틀이 건너뛰는 비율이
#:   4.5 → 9% 가 됐고, 치우침 크기도 바뀌었다 (md_fwd 주입 0.64 → 0.68초). **메시지 간격이나 스로틀이 바뀌면 다시 잡는다.**
#: 장기 기준 = 그 기간 중앙값, σ = 연속한 날 차이의 중앙값 기반 (최소 3 ms · 0.1 카운트)
#: 예전 값(09-10 ~ 12 처음 3일, DB)은 md_fwd 9-12 정비 전이라 주입 0.899초·끌림 15.28카운트·복귀 0.395초였다.
#: md_rev 스톱퍼 후진은 9월 중순 +0.01초 계단 뒤 값을 새 정상으로 둔다 (2026-11-20 결정).
UNIT_REF = {
    "md_fwd": {"stop_fwd_start": (0.1320, 0.0030), "stop_fwd_time": (0.2737, 0.0043), "stop_rev_start": (0.1825, 0.0044),
               "stop_rev_time": (0.3684, 0.0030), "nozzle_fwd_start": (0.1145, 0.0030), "nozzle_rev_time": (0.3204, 0.0046),
               "return_time": (0.3518, 0.0048), "catch_cnt": (6.4802, 0.1), "drag_cnt": (12.3132, 0.1), "air_time": (0.6738, 0.0067)},
    "md_rev": {"stop_fwd_start": (0.1340, 0.0030), "stop_fwd_time": (0.2961, 0.0063), "stop_rev_start": (0.1850, 0.0031),
               "stop_rev_time": (0.3651, 0.0030), "nozzle_fwd_start": (0.1281, 0.0030), "nozzle_rev_time": (0.3138, 0.0030),
               "return_time": (0.7423, 0.0051), "catch_cnt": (12.7065, 0.1), "drag_cnt": (15.2308, 0.1), "air_time": (0.9055, 0.0064)},
    "airbag": {"stop_fwd_start": (0.1236, 0.0051), "stop_fwd_time": (0.2658, 0.0061), "stop_rev_start": (0.1684, 0.0039),
               "stop_rev_time": (0.3282, 0.0057), "nozzle_fwd_start_1": (0.1172, 0.0046), "nozzle_fwd_start_2": (0.1040, 0.0048),
               "nozzle_rev_time_1": (0.3026, 0.0044), "nozzle_rev_time_2": (0.3211, 0.0057), "return_time": (0.5735, 0.0068),
               "catch_cnt": (6.7084, 0.1), "drag_cnt": (32.3808, 0.1), "air_time": (2.5031, 0.0146)},
}



def bits(unit, key):
    """신호 키 → 비트 목록 (노즐 후진감지만 여러 개일 수 있다)."""
    v = UNITS[unit][key]
    return [v] if isinstance(v, str) else list(v)


def metric_columns(unit):
    """이 유닛의 지표 컬럼 이름 (노즐이 둘이면 _1 · _2)."""
    cols = []
    for name, _, _, e_sig, _, _, _ in METRICS:
        n = len(bits(unit, e_sig))
        cols += [name] if n == 1 else [f"{name}_{j + 1}" for j in range(n)]
    return cols


def required_tags(unit):
    """실시간 모델이 live_data 에서 읽는 태그 (tag_config 에 있어야 한다)."""
    return [b for k in SIGNAL_KEYS for b in bits(unit, k)]


def metric_part(unit, col):
    """지표 컬럼 → (device, group, relay). 거리·에어 주입 시간 지표는 relay 없이 (device, SLIDE_GROUP, None)."""
    key = METRIC_PART[col[:-2] if col[-2:] in ("_1", "_2") else col]
    if key is None:
        return UNITS[unit]["device"], SLIDE_GROUP, None
    addr = UNITS[unit][key]
    r = RELAY_CONFIG[addr]
    return r["deviceId"], r["groupId"], addr


def _check_topology():
    """UNITS 의 주소가 전부 topology 에 그 유닛 device 로 있어야 한다 (rev_latch 는 PLC 내부 심볼이라 뺀다).
    아니면 topology 가 바뀐 것이니 부팅 때 바로 터뜨린다 (tank_params._build_relay_ids 와 같은 방식)."""
    if set(METRIC_PART) != {m[0] for m in METRICS}:
        raise ValueError(f"[unit_params] METRIC_PART 와 METRICS 의 지표가 다름: "
                         f"{sorted(set(METRIC_PART) ^ {m[0] for m in METRICS})}")
    keys = [k for k in SIGNAL_KEYS if k != "rev_latch"] + ["rev"]
    bad = [f"{u}.{k}={b}" for u, cfg in UNITS.items() for k in keys for b in bits(u, k)
           if RELAY_CONFIG.get(b, {}).get("deviceId") != cfg["device"]]
    if bad:
        raise ValueError(f"[unit_params] topology 에 없거나 device 가 다른 주소: {bad}")


_check_topology()
