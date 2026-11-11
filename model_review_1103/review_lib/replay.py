"""운영 모델에 원시 데이터를 다시 흘려 '모델이 본 피처'를 얻는다 (backend 코드를 그대로 호출).

경고 로그에는 상태 변화만 남는다. 조치 전후로 어떤 피처가 움직였는지,
놓친 경고(FN)에서 점수가 문턱 아래에서 움직였는지는 재생으로만 보인다.

- 입력: db.fetch_range() 결과 (KST naive 인덱스, 태그별 열, ffill 된 원시값).
  DB 행 간격이 실측 약 0.5초라 행 하나를 틱 하나로 넣는다.
- 재생 결과는 엣지와 코드는 같지만, 엣지의 재시작·수집 공백까지 똑같지는 않다.
- 펌프 창 판정(최근 10사이클)은 재생 시작 직후 비어 있다. 보고 싶은 시각보다 최소 반나절 앞에서 시작한다.
"""
import sys
from contextlib import redirect_stdout
from io import StringIO

import pandas as pd

from . import paths


def _backend():
    if str(paths.BACKEND) not in sys.path:
        sys.path.insert(0, str(paths.BACKEND))


def pump_cycles(raw, pumps=None, progress=True):
    """펌프 사이클 한 행 = 한 판정. zn_<피처> 열이 오토인코더 입력 피처의 정규화 값.

    mode: 자동 운전 / 판정 범위 밖 / 수집 간격 / 점검·수동 / 알람 / 유량 없음
    (자동 운전만 창 판정·경보로 이어진다)
    """
    _backend()
    with redirect_stdout(StringIO()):
        from tools_yeji.pump_models.pump_realtime import PumpRealtimeGroup
        grp = PumpRealtimeGroup(pumps=pumps, keep_cycles=None)
    cols = [c for c in raw.columns if c != "source"]
    vals = raw[cols].to_numpy(dtype=object)
    n = len(raw)
    step = max(n // 10, 1)
    for i, (t, v) in enumerate(zip(raw.index, vals)):
        row = {c: x for c, x in zip(cols, v) if x is not None and x == x}
        grp.update(t, row)
        if progress and i % step == 0:
            print(f"\r재생 {i / n:4.0%}  {t}", end="", flush=True)
    if progress:
        print(f"\r재생 완료 — 사이클 {len(grp.cycles)}개")
    df = pd.DataFrame(list(grp.cycles))
    if df.empty:
        return df
    if "zn" in df:
        zn = pd.json_normalize(df.pop("zn").apply(lambda d: d if isinstance(d, dict) else {}))
        df = pd.concat([df, zn.add_prefix("zn_")], axis=1)
    return df


def pump_daily(cyc, feats=None):
    """사이클 → 펌프·날짜별 피처 중앙값과 판정 모드 비율. 조치 전후 비교 표에 쓴다."""
    cyc = cyc.copy()
    cyc["date"] = pd.to_datetime(cyc["t_start"]).dt.date
    auto = cyc[cyc["mode"] == "자동 운전"]
    feats = feats or [c for c in cyc.columns if c.startswith("zn_")]
    med = auto.groupby(["pump", "date"])[feats + ["score"]].median()
    mode = cyc.groupby(["pump", "date"])["mode"].value_counts(normalize=True).unstack(fill_value=0).add_prefix("mode_")
    return med.join(mode, how="outer")
