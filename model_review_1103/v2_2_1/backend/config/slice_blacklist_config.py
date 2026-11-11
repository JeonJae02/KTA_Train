# config/slice_blacklist_config.py
"""역슬라이스에서 '전역 허가/노이즈'로 보고 경계 처리할 심볼 (CUT-C).

여기 등재된 심볼을 만나면 슬라이서가 더 타고 내려가지 않는다.
고장 후보가 아니라 '기계가 돌고 있었다'는 배경 사실이기 때문이다.

이 목록을 늘리면 후보가 좁아지고, 줄이면 넓어진다.
수정하면 config_fingerprint 가 바뀌어 다음 기동에 인덱스가 전체 재생성된다.
"""

#: 정확히 일치하는 심볼 이름 (대소문자 무시)
SLICE_BLACKLIST = [
    # --- 시스템 플래그 ---
    "_ON", "_OFF", "_RUN",

    # --- 전역 운전 모드 / 허가 ---
    "PREPARE",              # fan-in 68. 준비 완료 = 배경
    "AUTO_RUNNING",         # fan-in 28
    "AUTO_START_WARNING",
    "CONV_RUN_WARNING",
    "CONV_BRAKE_TIME",      # fan-in 32
    "LAMP_TEST",

    # --- 리셋 / 비상 ---
    "RESET_SW",             # fan-in 232 (1위). 리셋 경로는 CUT-A 로도 걸리지만 이중 방어
    "EMERGENCY_SW",
    "CONV_EMERGENCY",       # 라인 전체 비상 OR 집합체

    # --- 스케일 보정 상수 ---
    "GAIN",
    "OFFSET",
]

#: 접두사로 시작하면 전부 제외 (비워 두면 접두사 매칭 안 함)
#: 예: "SCALE_OUT" 을 넣으면 Scale_Out___PT_P1 등이 전부 경계 처리된다.
SLICE_BLACKLIST_PREFIXES = [
]
