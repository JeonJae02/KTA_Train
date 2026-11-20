"""컨베이어 가동일 기록(history JSON)을 수정본(HDI_MODEL/conveyor_models)으로 다시 만든다 — 배포 전에 한 번.

왜: 엣지가 10-26부터 쌓은 기록에는 예지 엔진 0.5초 스로틀로 빠진 샘플의 치우침이 들어 있다
    (켜짐 길이 "엣지가 김" 25%, md_fwd 주입 시간 0.67 ~ 0.69초). 수정본의 새 유닛 기준(md_fwd 주입 0.6277 ± 0.0066초)으로
    이 기록을 판정하면 배포 첫날 "느려짐" 오경보가 난다 (z +6 ~ +10).
무엇을: FROM 이전 날은 입력 기록 그대로 두고, FROM ~ TO 는 DB 원시 전부(스로틀 없음)를 수정본에 흘려
        하루 마감 값으로 바꾼다. 입력 기록에서 TO 뒤의 날은 버린다 (다시 만들 수 없는 치우친 값이라).

사용 (DB_Log/ 에서):
  venv/bin/python HDI_MODEL/conveyor_rebuild_history.py <입력 data 폴더> <출력 폴더> <FROM> <TO>
  예) venv/bin/python HDI_MODEL/conveyor_rebuild_history.py model_review_1103/v2_2_1/backend/data \
         HDI_MODEL/conveyor_models/data 2026-10-26 2026-11-14
  - 날짜는 가동일(06시 시작). TO 가동일은 TO+1 날 06시까지라 TO+1 원시도 필요하다.
  - 원시는 model_review_1103 의 review_lib.db 캐시(cache/raw/conveyor)를 쓰고, 없으면 현장 DB 에서 하루씩 뽑는다.
"""
import importlib.util, json, os, sys
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
REVIEW = HERE.parent / "model_review_1103"
sys.path.insert(0, str(REVIEW))
from review_lib import backend, db          # noqa: E402

backend.use("v2.2")                          # config(topology) 경로만 빌린다. 모델 코드는 아래에서 수정본으로 바꾼다
with redirect_stdout(StringIO()):
    import tools_yeji                         # noqa: F401
    spec = importlib.util.spec_from_file_location("tools_yeji.conveyor_models", HERE / "conveyor_models" / "__init__.py",
                                                  submodule_search_locations=[str(HERE / "conveyor_models")])
    mod = importlib.util.module_from_spec(spec)
    sys.modules["tools_yeji.conveyor_models"] = mod
    spec.loader.exec_module(mod)
    from tools_yeji.conveyor_models.conveyor_realtime import ConveyorRealtimeGroup
    import tools_yeji.conveyor_models.core_realtime as cr

CORE_FILE = "conveyor_core_history.json"
UNIT_FILE = "conveyor_unit_{}_history.json"


def load(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def main(src, dst, d_from, d_to):
    d_from, d_to = pd.Timestamp(d_from), pd.Timestamp(d_to)
    assert Path(cr.__file__).parent == HERE / "conveyor_models", cr.__file__
    g = ConveyorRealtimeGroup(data_dir=None)
    g.evaluate = lambda t: None               # 판정은 필요 없다 — 하루 마감 값만
    tags = db.model_tags("conveyor", "v2.2")
    for day in pd.date_range(d_from, d_to):
        d0 = day + pd.Timedelta(hours=6)
        raw = pd.concat([db.fetch_day(day, tags, "conveyor"), db.fetch_day(day + pd.Timedelta(days=1), tags, "conveyor")])
        raw = raw[(raw.index >= d0) & (raw.index < d0 + pd.Timedelta(days=1))]
        cols = [c for c in raw.columns if c != "source"]
        for t, v in zip(raw.index, raw[cols].to_numpy(dtype=object)):
            g.update(t, {k: x for k, x in zip(cols, v) if x is not None and x == x})
        print("재생", day.date(), flush=True)
    g.core.close_day()
    for m in g.units.values():
        m.close_day()

    keep = lambda rows: [r for r in rows if r["day"] < f"{d_from:%Y-%m-%d}"]
    fmt = lambda rows: [{**r, "day": pd.Timestamp(r["day"]).strftime("%Y-%m-%d")} for r in rows]
    new_core = [r for r in fmt(g.core.history) if f"{d_from:%Y-%m-%d}" <= r["day"] <= f"{d_to:%Y-%m-%d}"]
    out = {CORE_FILE: keep(load(Path(src) / CORE_FILE)) + new_core}
    for u, m in g.units.items():
        new_u = [r for r in fmt(m.history) if f"{d_from:%Y-%m-%d}" <= r["day"] <= f"{d_to:%Y-%m-%d}"]
        out[UNIT_FILE.format(u)] = keep(load(Path(src) / UNIT_FILE.format(u))) + new_u
    os.makedirs(dst, exist_ok=True)
    for name, rows in out.items():
        rows = sorted(rows, key=lambda r: (r["day"], r.get("sensor", ""), r.get("wagon", 0)))
        with open(Path(dst) / name, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False)
        days = sorted({r["day"] for r in rows})
        print(f"{name}: {len(rows)}행 · 가동일 {len(days)} ({days[0]} ~ {days[-1]})")


if __name__ == "__main__":
    main(*sys.argv[1:5])
