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
"""

MAX_GAP = 1.2           #: 초. 이보다 긴 샘플 간격 사이의 변화는 시각을 모른다고 본다
MAX_CYCLE = 30.0        #: 초. 사이클이 이보다 길면 중간에 라인이 멈춘 것 — 지표에서 뺀다

#: 유닛 정의. 값은 신호 키 → 비트(주소). nozzle_rev 가 두 개면 노즐 1·2 를 따로 잰다
#: rev_latch: 끝단 후진은 0.5초 펄스라 원본은 67–79% 만 잡힌다. 끝단 도착 순간 SET 되어 원점복귀까지
#:            유지되는 PLC 래치를 대신 쓴다 (99% 이상).
UNITS = {
    "md_fwd": dict(name="전진 유닛", station="fwd", group="md_fwd", prefix="Air_Supply2",
                   stop_sol="P00470", nozzle_sol="P00471", return_sol="P00472", open_sol="P00473",
                   org="P00452", rev="P00453", flt="P00454", stop_fwd="P00455", stop_rev="P00456",
                   nozzle_rev=["P00457"], rev_latch="Air_Supply2_Rev_PX_CHK"),
    "md_rev": dict(name="후진 유닛", station="rev", group="md_rev", prefix="Air_Supply3",
                   stop_sol="P00474", nozzle_sol="P00475", return_sol="P00476", open_sol="P00477",
                   org="P0045A", rev="P0045B", flt="P0045C", stop_fwd="P0045D", stop_rev="P0045E",
                   nozzle_rev=["P0045F"], rev_latch="Air_Supply3_Rev_PX_CHK"),
    "airbag": dict(name="에어백 유닛", station="airbag", group="airbag", prefix="Air_Supply",
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

#: 사이클에서 쓰는 신호 키 (실시간은 rev 원본 대신 rev_latch 를 쓴다)
SIGNAL_KEYS = ["stop_sol", "nozzle_sol", "return_sol", "open_sol", "org", "flt", "stop_fwd", "stop_rev",
               "nozzle_rev", "rev_latch"]

#: PLC 에러 비트 (`{prefix}_{이름}`) — 배치 분석(conv_03)에서 참고로만 센다. 실시간 판정에는 쓰지 않는다
ERRORS = {
    "Stopper_CY_FWD_Err": "스톱퍼 전진 시간 초과",
    "Stopper_CY_REV_Err": "스톱퍼 후진 시간 초과",
    "Nozzle_CY_FWD_Err": "노즐 전진 시간 초과",
    "Nozzle_CY_REV_Err": "노즐 후진 시간 초과",
    "Return_Err": "원점복귀 시간 초과",
    "Home_CHK_Err": "원점 확인 실패",
    "Rev_FLT_PX_CHK_Err": "비상(오버트래블) 감지",
}

#: 지표별 기준 (장기 기준값, 날짜 간 흔들림 σ) — conv_03 (09-10 – 10-19) 에서 나온 값.
#: 장기 기준 = 처음 3 가동일 중앙값, σ = 연속한 날 차이의 중앙값 기반 (최소 3 ms · 0.05 카운트)
UNIT_REF = {
    "md_fwd": {"stop_fwd_start": (0.1281, 0.0041), "stop_fwd_time": (0.2851, 0.0047), "stop_rev_start": (0.1861, 0.0047),
               "stop_rev_time": (0.3919, 0.0040), "nozzle_fwd_start": (0.1155, 0.0041), "nozzle_rev_time": (0.3296, 0.0044),
               "return_time": (0.3945, 0.0083), "catch_cnt": (6.3933, 0.05), "drag_cnt": (15.28, 0.05), "air_time": (0.8989, 0.0072)},
    "md_rev": {"stop_fwd_start": (0.1279, 0.0030), "stop_fwd_time": (0.2795, 0.0034), "stop_rev_start": (0.1806, 0.0032),
               "stop_rev_time": (0.3445, 0.0037), "nozzle_fwd_start": (0.1286, 0.0035), "nozzle_rev_time": (0.3098, 0.0033),
               "return_time": (0.7313, 0.0063), "catch_cnt": (12.8658, 0.05), "drag_cnt": (15.2155, 0.05), "air_time": (0.8790, 0.0095)},
    "airbag": {"stop_fwd_start": (0.1224, 0.0046), "stop_fwd_time": (0.2597, 0.0046), "stop_rev_start": (0.1619, 0.0052),
               "stop_rev_time": (0.3178, 0.0043), "nozzle_fwd_start_1": (0.1165, 0.0039), "nozzle_fwd_start_2": (0.1030, 0.0035),
               "nozzle_rev_time_1": (0.2895, 0.0042), "nozzle_rev_time_2": (0.3083, 0.0054), "return_time": (0.6258, 0.0030),
               "catch_cnt": (6.6555, 0.05), "drag_cnt": (32.4116, 0.05), "air_time": (2.4748, 0.0227)},
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
