"""캐시 원본(cache/pump_raw) → 펌프별 사이클 피처·순환 창 (cache/pump_feat).

    python HDI_MODEL/build_pump_features.py                  # 전 펌프, 캐시에 있는 전 기간
    python HDI_MODEL/build_pump_features.py --pumps P3 I3    # 일부만
    python HDI_MODEL/build_pump_features.py --force          # 이미 있어도 다시
    python HDI_MODEL/build_pump_features.py --circ-only      # 순환 창만 다시 (사이클은 그대로)

- DB 를 치지 않는다 (extract_pump_cache.py 로 받아 둔 캐시만 읽는다)
- 기준선 없이 만든다 → 기준선이 필요한 피처(pump_resid, r_phys, err_fte …)는 분석·학습 단계에서
  PumpBaseline.fit 후 add_baseline_features 로 붙인다
- 원료 점도가 미검증인 펌프도 만든다 (allow_unverified). mu_rel 은 설정의 임시 B 로 계산된 값이라,
  원료를 확정한 뒤 다시 만들거나 T_C 로부터 다시 계산해서 쓴다
- 틱 표는 저장하지 않는다 (펌프당 수백만 행). 틱 AE 학습 때 필요한 구간만 run() 으로 다시 만든다

저장: cache/pump_feat/{펌프}_cycles.pkl.xz, {펌프}_circ.pkl.xz, {펌프}_alerts.pkl.xz, _build_log.csv
"""

import argparse
import csv
import os
import sys
import time
from datetime import datetime

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)

from extract_pump_cache import CACHE_DIR as RAW_DIR, ORDER, load_range  # noqa: E402
from new_pump_preprocessor import PumpSetup, circulation_windows, load_settings, run  # noqa: E402

FEAT_DIR = os.path.join(ROOT, "cache", "pump_feat")


def feat_path(pump, kind):
    return os.path.join(FEAT_DIR, f"{pump}_{kind}.pkl.xz")


def load_feat(pump, kind="cycles"):
    return pd.read_pickle(feat_path(pump, kind), compression="xz")


def cached_days(pump):
    d = os.path.join(RAW_DIR, pump)
    return sorted(f[:10] for f in os.listdir(d) if f.endswith(".pkl.xz")) if os.path.isdir(d) else []


def build_circ(pump, settings):
    """순환 창만 다시 만든다 (펌프당 수 초)."""
    days = cached_days(pump)
    setup = PumpSetup(pump, settings, allow_unverified=True)
    circ = circulation_windows(load_range(pump, days[0], days[-1]), setup)
    circ.to_pickle(feat_path(pump, "circ"), compression="xz")
    return len(circ)


def build(pump, settings):
    days = cached_days(pump)
    if not days:
        raise FileNotFoundError(f"{pump}: 원본 캐시 없음 — extract_pump_cache.py 먼저")
    setup = PumpSetup(pump, settings, allow_unverified=True)
    t0 = time.time()
    raw = load_range(pump, days[0], days[-1])
    t_load = time.time() - t0
    _, cyc, alerts = run(raw, setup, collect_ticks=False)
    circ = circulation_windows(raw, setup)
    cyc.to_pickle(feat_path(pump, "cycles"), compression="xz")
    circ.to_pickle(feat_path(pump, "circ"), compression="xz")
    alerts.to_pickle(feat_path(pump, "alerts"), compression="xz")
    return {"pump": pump, "days": f"{days[0]}~{days[-1]}", "ticks": len(raw), "cycles": len(cyc),
            "circ_windows": len(circ), "alerts": len(alerts), "load_sec": round(t_load, 1),
            "total_sec": round(time.time() - t0, 1), "material": setup.material_name, "verified": setup.verified}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--pumps", nargs="*", default=None)
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--circ-only", action="store_true")
    a = ap.parse_args()
    settings = load_settings()
    os.makedirs(FEAT_DIR, exist_ok=True)
    pumps = a.pumps or [p for p in ORDER if p in settings["pumps"]]
    if a.circ_only:
        for p in pumps:
            print(f"{p}: 순환창 {build_circ(p, settings)}", flush=True)
        sys.exit(0)
    log = os.path.join(FEAT_DIR, "_build_log.csv")
    for p in pumps:
        if not a.force and os.path.exists(feat_path(p, "cycles")):
            print(f"{p}: 있음 — 건너뜀 (--force 로 다시)", flush=True)
            continue
        r = build(p, settings)
        r["time"] = datetime.now().isoformat(timespec="seconds")
        new = not os.path.exists(log)
        with open(log, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(r))
            if new:
                w.writeheader()
            w.writerow(r)
        print(f"{p}: 틱 {r['ticks']:,} → 사이클 {r['cycles']:,}, 순환창 {r['circ_windows']}, 경보 {r['alerts']}  "
              f"({r['total_sec']}s, 읽기 {r['load_sec']}s)", flush=True)
