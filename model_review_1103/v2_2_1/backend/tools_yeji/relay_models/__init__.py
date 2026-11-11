# tools_yeji/relay_models
"""릴레이 진단 모델 (룰 기반 로직).

원본: DB_Log/HDI_MODEL (+ parkkt/Tank_Diagnosis_Model.py 의 상수).
원본의 **판정 원리는 그대로** 두고, 코드 구조는 백엔드에 맞춰 바꿨다(주소·topology 기반,
전처리/판정 분리). 레지스트리·추세 기록은 Relay_Monitor / Relay_Trend 가 맡는다.

- tank_params.py     : 탱크 태그·판정 파라미터·릴레이 ID(topology)·메시지
- tank_preprocess.py : 탱크 1개 전처리 — 입력 정리, exchanger 사이클, 운영일 최저 PV
- tank_judge.py      : 탱크 1개 판정 — 규칙 ①②③, 같은 이상은 한 번만
- tank_realtime.py   : 탱크 여러 개 + 가동 플래그 -> relay_warning
- head_injection.py  : 헤드 인젝션 ON_T / OFF_T 문턱 진단
"""
