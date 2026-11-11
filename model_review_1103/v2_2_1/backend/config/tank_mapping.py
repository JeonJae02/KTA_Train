# config/tank_mapping.py

# 펌프 ID를 Key로, 연결된 탱크 ID를 Value로 가지는 딕셔너리
PUMP_TO_TANK = {
    "P1": {"Tank" : "P1", "Robot" : "RB1"},   # (예시) P1 펌프는 P1 탱크를 쓴다
    "P2": {"Tank" : "P5", "Robot" : "RB1"},   # (예시) P2 펌프도 P5 탱크를 공유한다
    "P3": {"Tank" : "P2", "Robot" : "RB1"},   # (예시) P3 펌프는 T2 탱크를 쓴다
    "P4": {"Tank" : "P4", "Robot" : "RB1"},
    "P5": {"Tank" : "P3", "Robot" : "RB2"},
    "P6": {"Tank" : "P1", "Robot" : "RB2"},
    "P7": {"Tank" : "P5", "Robot" : "RB2"},

    "I1": {"Tank" : "I1", "Robot" : "RB1"}, # ISO 펌프용 탱크 매핑 등
    "I2": {"Tank" : "I3", "Robot" : "RB1"},
    "I3": {"Tank" : "I2", "Robot" : "RB1"},
    "I4": {"Tank" : "I2", "Robot" : "RB1"},
    "I5": {"Tank" : "I3", "Robot" : "RB2"},
    "I6": {"Tank" : "I1", "Robot" : "RB2"},
    "I7": {"Tank" : "I3", "Robot" : "RB2"}
}

