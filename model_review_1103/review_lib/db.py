"""InfluxDB 에서 검토 구간의 원시 PLC 데이터를 뽑는다. 하루 단위로 캐시한다.

- 접속 정보는 DB_Log/.env, 추출은 DB_Log/Log_Extractor.py 의 LogExtractor 를 그대로 쓴다
  (태그 정규식 pushdown, 앞쪽 스냅샷 패딩, ffill 처리 포함).
- **현장 DB 다.** 태그 목록 없이 부르지 않는다. 하루씩 뽑고 캐시를 먼저 본다.
- 캐시: model_review_1103/cache/raw/<group>/<YYYY-MM-DD>.pkl.xz (git 제외).
  같은 group 이름에 다른 태그 집합을 넣으면 섞이므로 group 이름은 태그 집합마다 따로 쓴다.
- 인덱스는 KST naive (Time), 값은 원시값 그대로. 단위 변환은 모델 코드가 한다.
"""
import sys
from contextlib import redirect_stdout
from io import StringIO

import pandas as pd

from . import paths

_extractor = None


def _get_extractor():
    global _extractor
    if _extractor is None:
        sys.path.insert(0, str(paths.DB_LOG))
        from Log_Extractor import LogExtractor
        _extractor = LogExtractor(env_path=str(paths.DB_LOG / ".env"))
    return _extractor


def model_tags(domain):
    """운영 모델이 실제로 읽는 태그 목록 (backend 코드에서 뽑는다).

    domain: 'pump' (펌프 90 + 헤드 21), 'conveyor' (46), 'tank' (40). 가동 플래그는 항상 포함.
    """
    sys.path.insert(0, str(paths.BACKEND))
    with redirect_stdout(StringIO()):           # backend 로더의 로드 메시지를 숨긴다
        from config.operation_config import RUN_FLAG_TAG
        if domain == "pump":
            from tools_yeji.pump_models.pump_realtime import PumpRealtimeGroup
            from tools_yeji.relay_models.head_injection import HEAD_PUMPS
            tags = {t for v in PumpRealtimeGroup().missing_tags([]).values() for t in v}
            tags |= {t for d in HEAD_PUMPS.values() for t in d.values()}
        elif domain == "conveyor":
            from tools_yeji.conveyor_models.conveyor_realtime import all_required_tags
            tags = set(all_required_tags())
        elif domain == "tank":
            from tools_yeji.relay_models.tank_params import TANKS
            tags = {t for d in TANKS.values() for t in d.values()}
        else:
            raise ValueError(domain)
    return sorted(tags | {RUN_FLAG_TAG})


def fetch_day(day, tags, group, refresh=False, verbose=False):
    """하루치(KST 00:00~24:00) 데이터. 캐시가 있으면 DB 에 가지 않는다."""
    day = pd.Timestamp(day).normalize()
    f = paths.CACHE_DIR / "raw" / group / f"{day:%Y-%m-%d}.pkl.xz"
    if f.exists() and not refresh:
        return pd.read_pickle(f)
    if day + pd.Timedelta(days=1) > pd.Timestamp.now():
        raise ValueError(f"{day:%m-%d} 은 아직 끝나지 않은 날이라 캐시하지 않는다")
    ex = _get_extractor()
    buf = StringIO()
    with redirect_stdout(buf if not verbose else sys.stdout):
        df = ex.get_data(f"{day:%Y-%m-%d %H:%M:%S}", f"{day + pd.Timedelta(days=1):%Y-%m-%d %H:%M:%S}", tags)
    if df.empty:
        print(f"⚠️ {day:%m-%d} {group}: 데이터 없음 (캐시 안 함)\n{buf.getvalue()[-500:]}")
        return df
    f.parent.mkdir(parents=True, exist_ok=True)
    df.to_pickle(f)
    return df


def fetch_range(start, end, tags, group, **kw):
    """[start, end) 를 하루씩 뽑아 이어 붙이고 구간으로 자른다."""
    start, end = pd.Timestamp(start), pd.Timestamp(end)
    days = pd.date_range(start.normalize(), (end - pd.Timedelta(seconds=1)).normalize(), freq="D")
    parts = [fetch_day(d, tags, group, **kw) for d in days]
    parts = [p for p in parts if not p.empty]
    if not parts:
        return pd.DataFrame()
    df = pd.concat(parts).sort_index()
    return df[(df.index >= start) & (df.index < end)]
