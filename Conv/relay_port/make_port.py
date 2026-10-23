"""컨베이어 대차 센서 모델 → HDI_Relay 이식 묶음을 만든다.

    python Conv/relay_port/make_port.py          # → Conv/relay_port/out/backend/
    python Conv/relay_port/verify_port.py        # 묶음 코드로 과거 데이터 재생 → 배치(conv_02) 판정과 비교
    # → out/backend/ 를 HDI_Relay/backend/ 위에 복사 + PORTING.md 의 "백엔드 수정" (tag_config 태그 추가)

묶음
  tools_yeji/Conveyor_Health.py                   ConveyorMonitor (MOCK 교체)
  tools_yeji/conveyor_models/__init__.py
  tools_yeji/conveyor_models/conv_rules.py        판정 규칙 (Conv/conv_rules.py 그대로)
  tools_yeji/conveyor_models/conv_realtime.py     실시간 모델 (import 만 패키지 상대 경로로)
  data/conveyor_history.json                      처음 기록: conv_02 일별 칸 (cache/conv_feat/wagon_sensor_daily.pkl)
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
sys.path.insert(0, CONV)


def main():
    if os.path.exists(OUT):
        shutil.rmtree(OUT)
    pkg = os.path.join(OUT, "tools_yeji", "conveyor_models")
    os.makedirs(pkg)
    os.makedirs(os.path.join(OUT, "data"))
    open(os.path.join(pkg, "__init__.py"), "w").close()
    shutil.copy(os.path.join(CONV, "conv_rules.py"), os.path.join(pkg, "conv_rules.py"))
    src = open(os.path.join(CONV, "conv_realtime.py"), encoding="utf-8").read()
    src = re.sub(r"^from conv_rules import", "from .conv_rules import", src, flags=re.M)
    open(os.path.join(pkg, "conv_realtime.py"), "w", encoding="utf-8").write(src)
    shutil.copy(os.path.join(HERE, "Conveyor_Health.py"), os.path.join(OUT, "tools_yeji", "Conveyor_Health.py"))

    from conv_realtime import ConveyorRealtime, REQUIRED_TAGS
    daily = pd.read_pickle(os.path.join(ROOT, "cache", "conv_feat", "wagon_sensor_daily.pkl"))
    rt = ConveyorRealtime()
    rt.seed_history(daily)
    rt.save_history(os.path.join(OUT, "data", "conveyor_history.json"))
    days = sorted({r["day"] for r in rt.history})
    print(f"묶음 → {OUT}")
    print(f"  처음 기록: {len(rt.history)} 칸, {days[0]:%Y-%m-%d} – {days[-1]:%Y-%m-%d} ({len(days)} 가동일)")
    print(f"  tag_config MONIORING_TAGS 에 넣을 태그 {len(REQUIRED_TAGS)}개: {REQUIRED_TAGS}")


if __name__ == "__main__":
    main()
