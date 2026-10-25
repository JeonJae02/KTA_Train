"""[코어 · 데이터] 대차 통과 센서 6개 — 무엇을 받고, 각 센서가 어디 있는지.

코어 = 고정된 자리에서 지나가는 대차의 금속 표식을 보는 근접 센서 6개.
  - 주입부: 대차 진입 P00140 · 대차 쉬프트 P00141
  - 클로징 존 진입 P00154 · 오프닝 존 진입 P00164
  - 전진 유닛 진입 P00451 · 후진 유닛 진입 P00459
`ruler=True` 는 "자" 역할(대차에 무관하게 같은 자리에서 켜짐)이 확인된 센서다. 전진 진입은 이물 대차가 많아
자로는 쓰지 않지만, 판정은 다른 코어 센서와 똑같이 한다.

값의 출처: conv_02 (09-10 – 10-19, 31 가동일). 센서를 옮기거나 대차를 바꾸면 다시 뽑는다.
"""

import pandas as pd

#: 존 카운터 6개 — 엔코더 펄스마다 +1, 자기 센서가 켜지는 순간 1 로 리셋. 체인 위치와 센서 통과를 여기서 읽는다
COUNTERS = ["R00030", "R00040", "R00055", "R00060", "R00070", "R00080"]

WAGON_TAG = "Now_Actual_Wagon_Num"     #: 메인 대차번호 1–42 (P00140 에서 +1, P00142 에서 1)
RUN_TAG = "Conv_Run_Out"               #: 컨베이어 운전 출력
HZ_TAG = "Hz_In_Conv"                  #: 속도 지령 (0.1 Hz)

#: 센서 정의
#:   counter : 이 센서가 리셋하는 카운터
#:   D       : 같은 대차가 P00140 을 지나서 이 센서까지 가는 카운트 (센서 간 거리)
#:   first   : 그 위치의 1번 대차 감지 센서 (배치에서 몇 대 뒤인지 확인용)
#:   n_fix   : P00140 에서 대차 몇 대 뒤인지 (1번 대차 센서가 없는 곳의 고정값)
#:   group   : DB 캐시 그룹 (배치 분석용)
SENSOR_ROWS = [
    dict(bit="P00140", name="대차 진입", station="pour", counter="R00040", D=0.0, ruler=True, first="P00142", n_fix=0, group="main"),
    dict(bit="P00141", name="대차 쉬프트", station="pour", counter="R00080", D=19.0, ruler=True, first=None, n_fix=0, group="main"),
    dict(bit="P00154", name="클로징 진입", station="closing", counter="R00030", D=295.0, ruler=True, first=None, n_fix=2, group="closing"),
    dict(bit="P00164", name="오프닝 진입", station="opening", counter="R00070", D=3709.0, ruler=True, first=None, n_fix=26, group="opening"),
    dict(bit="P00451", name="전진 진입", station="fwd", counter="R00055", D=3758.0, ruler=False, first="P00450", n_fix=26, group="md_fwd"),
    dict(bit="P00459", name="후진 진입", station="rev", counter="R00060", D=5728.0, ruler=True, first="P00458", n_fix=41, group="md_rev"),
]
SENSORS = {r["bit"]: r for r in SENSOR_ROWS}
#: 배치 분석용 표 (conv_grid · 노트북)
SENSOR_TABLE = pd.DataFrame(SENSOR_ROWS).set_index("bit")

#: 장기 기준 (09-10 – 09-19 중앙값). 센서 단위 판정(센서 이동·감도·흔들림)의 0점
LONG_REF = {
    "P00140": dict(D=0.0, W0=8.0, J0=0.0), "P00141": dict(D=19.0, W0=8.0, J0=0.0),
    "P00154": dict(D=295.0, W0=8.5, J0=0.0), "P00164": dict(D=3709.0, W0=9.0, J0=0.0),
    "P00451": dict(D=3758.0, W0=9.0, J0=0.0), "P00459": dict(D=5728.0, W0=8.0, J0=0.0),
}

#: 초당 카운트 ÷ (Hz_In_Conv/10). 구동부 판정의 기준 (31 가동일 0.2744 ± 0.0001)
K_SPEED = 0.2744

#: 실시간 모델이 live_data 에서 읽는 태그 (tag_config 에 있어야 한다)
REQUIRED_TAGS = COUNTERS + list(SENSORS) + [WAGON_TAG, RUN_TAG, HZ_TAG]
