"""경로와 검토 기간 상수. 다른 모듈은 경로를 여기서만 가져온다."""
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]          # model_review_1103/
DB_LOG = ROOT.parent                                # DB_Log/ (.env, venv, Log_Extractor.py)
BACKEND = ROOT / "backend"                          # 운영 코드 복사본 v2.1.x — 읽기 전용

#: 운영 모델 버전. 엣지 경고 로그의 형식이 바뀐 시각으로 구간을 나눈다 (11-06 17:25 KST 부터 v2.2).
#: 바뀐 것은 컨베이어(부위 단위 경고 키·topology 세분화)뿐이고 펌프·탱크·헤드 코드와 펌프 가중치는 같다.
#: 각 backend 는 자기 옆 shared/ 의 topology·근무표를 읽는다.
MODEL_VERSIONS = {
    "v2.1": dict(backend=ROOT / "backend", shared=ROOT / "shared",
                 commit="HDI_Relay cd73ba2 (v2.1.2, 10-26)",
                 start=pd.Timestamp("2026-10-15"), end=pd.Timestamp("2026-11-06 17:25")),
    "v2.2": dict(backend=ROOT / "v2_2_1" / "backend", shared=ROOT / "v2_2_1" / "shared",
                 commit="HDI_Relay bcaad9a (v2.2.1, 11-06)",
                 start=pd.Timestamp("2026-11-06 17:25"), end=None),
}


def version_at(t):
    """그 시각에 엣지에서 돌던 모델 버전."""
    return "v2.2" if pd.Timestamp(t) >= MODEL_VERSIONS["v2.2"]["start"] else "v2.1"

WARNING_DIR = ROOT / "warning"
FILTERING_XLSX = WARNING_DIR / "filtering.xlsx"     # 실무자 일지 원본 — 읽기 전용
WARNINGS_JSONL_DIR = WARNING_DIR / "predictive_warnings"

ACTIONS_DIR = WARNING_DIR / "actions"               # build_inputs.py 가 만든다
EPISODES_DIR = WARNING_DIR / "episodes"             # build_inputs.py 가 만든다

CACHE_DIR = ROOT / "cache"                          # DB 추출 캐시 (git 제외)

CHECK_DIRS = {
    "conveyor": ROOT / "con_check",
    "pump": ROOT / "pump_check",
    "etc": ROOT / "etc_check",
}

# 검토 기간 (KST, naive). 모델이 예측을 낸 첫날부터.
REVIEW_START = pd.Timestamp("2026-10-16 00:00:00")
REVIEW_END = pd.Timestamp("2026-11-11 09:00:00")    # 경고 로그 마지막 줄(11-11 08:50 KST) 직후

#: 경고 로그에 첫 줄이 찍힌 날 (KST). 이보다 이른 조치는 경고 이력으로 판정할 수 없다
#: → 라벨 PRE, 재생으로만 본다. 운영 반영 버전과 대략 맞는다 (펌프·릴레이 v2.1.0, 컨베이어 v2.1.1).
MODEL_LIVE = {
    "pump": pd.Timestamp("2026-10-15 18:58"),       # 펌프 오토인코더
    "relay": pd.Timestamp("2026-10-19 06:50"),      # 탱크·헤드 (첫 경고 시각 — 실제 반영은 더 이를 수 있음)
    "conveyor": pd.Timestamp("2026-10-26 06:19"),   # 컨베이어 코어·유닛
}
