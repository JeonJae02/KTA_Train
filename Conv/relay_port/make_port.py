"""컨베이어 모델(코어 + 유닛) → HDI_Relay 이식 묶음을 만든다.

    python Conv/relay_port/make_port.py          # → Conv/relay_port/out/backend/
    python Conv/relay_port/verify_port.py        # 묶음 코드로 과거 데이터 재생 → 배치(conv_02 · conv_03)와 비교
    # → out/backend/ 를 HDI_Relay/backend/ 위에 복사 + PORTING.md 의 "백엔드 수정" (tag_config 태그 추가)

묶음
  tools_yeji/Conveyor_Health.py                         ConveyorMonitor (MOCK 교체, 백엔드 래퍼)
  tools_yeji/conveyor_models/{core_spec,core_rules,core_realtime}.py      코어: 데이터 · 규칙 · 실시간
  tools_yeji/conveyor_models/{unit_spec,unit_rules,unit_realtime}.py      유닛: 데이터 · 규칙 · 실시간
  tools_yeji/conveyor_models/conveyor_monitor.py        코어 → 유닛 순으로 돌리고 스테이션으로 묶음
  data/conveyor_core_history.json                       처음 기록: conv_02 일별 칸
  data/conveyor_unit_{md_fwd,md_rev,airbag}_history.json 처음 기록: conv_03 일별 평균
"""
import os
import re
import shutil
import sys

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
CONV = os.path.dirname(HERE)
ROOT = os.path.dirname(CONV)
OUT = os.path.join(HERE, "out", "backend")
MODULES = ["core_spec", "core_rules", "core_realtime", "unit_spec", "unit_rules", "unit_realtime", "conveyor_monitor"]
sys.path.insert(0, CONV)


def main():
    if os.path.exists(OUT):
        shutil.rmtree(OUT)
    pkg = os.path.join(OUT, "tools_yeji", "conveyor_models")
    os.makedirs(pkg)
    os.makedirs(os.path.join(OUT, "data"))
    open(os.path.join(pkg, "__init__.py"), "w").close()
    pat = re.compile(r"^from (%s) import" % "|".join(MODULES), flags=re.M)
    for m in MODULES:
        src = open(os.path.join(CONV, f"{m}.py"), encoding="utf-8").read()
        open(os.path.join(pkg, f"{m}.py"), "w", encoding="utf-8").write(pat.sub(r"from .\1 import", src))
    shutil.copy(os.path.join(HERE, "Conveyor_Health.py"), os.path.join(OUT, "tools_yeji", "Conveyor_Health.py"))

    from conveyor_monitor import ConveyorMonitor, all_required_tags
    from conv_cyl import cyl_daily
    feat = os.path.join(ROOT, "cache", "conv_feat")
    mon = ConveyorMonitor(data_dir=None)
    mon.core.seed_history(pd.read_pickle(os.path.join(feat, "wagon_sensor_daily.pkl")))
    mon.core.save_history(os.path.join(OUT, "data", "conveyor_core_history.json"))
    D = cyl_daily(pd.read_pickle(os.path.join(feat, "cyl_cycles.pkl")))
    for u, m in mon.units.items():
        m.seed_history(D)
        m.save_history(os.path.join(OUT, "data", f"conveyor_unit_{u}_history.json"))
    days = sorted({r["day"] for r in mon.core.history})
    print(f"묶음 → {OUT}")
    print(f"  코어 기록: {len(mon.core.history)} 칸, {days[0]:%Y-%m-%d} – {days[-1]:%Y-%m-%d} ({len(days)} 가동일)")
    print(f"  유닛 기록: " + ", ".join(f"{u} {len(m.history)}일" for u, m in mon.units.items()))
    tags = all_required_tags()
    print(f"  tag_config MONIORING_TAGS 에 넣을 태그 {len(tags)}개")
    open(os.path.join(HERE, "out", "required_tags.txt"), "w").write("\n".join(tags) + "\n")


if __name__ == "__main__":
    main()
