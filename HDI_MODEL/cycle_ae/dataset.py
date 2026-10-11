r"""사이클 AE 학습용 데이터셋 — 캐시 사이클 + 펌프별 기준선 JSON → 한 표.

    python HDI_MODEL/cycle_ae/dataset.py           # → cache/pump_dataset/cycles.pkl.xz

먼저: extract_pump_cache.py → build_pump_features.py → baseline/make_baselines.py

## 무엇을 담나

펌프 14대의 사이클을 한 표로. 기준선 피처(`pump_resid`, `r_phys`)를 붙이고, 사이클마다 쓰임새를 표시한다.

| 컬럼 | 뜻 |
|---|---|
| `group` | `{펌프}\|{안정 출처 clean/shot}\|{single/multi}` — 모델의 표준화·임계값 단위 |
| `split` | train (< 09-28) / val (09-28 ~ 10-04) / test (10-05 ~) — make_baselines 와 같은 경계 |
| `excluded` | 09-27, 알려진 이벤트(P7 9/17 유량계) — 학습·임계값 모두 제외 |
| `healthy` | 압력 기준선을 맞춘 "평소 배관 상태" 시간대의 사이클 (테스트 전만 정의, 테스트는 NaN). 판정은 주된 출처 사이클로 하고, 같은 시간대의 다른 출처 사이클에 넓힌다 |
| `role` | `fit` AE 학습 / `calib` 임계값 보정 / `eval` 평가만 — healthy 사이클을 **날짜 단위로** fit·calib 로 나눈다 |
| `event` | 평가용 이벤트 이름 (EVAL_EVENTS) |

포함 범위: 알람·정지 사이클만 빼고 **전부** — 모든 안정 출처, 단일·다단 Hz.
기준선은 펌프의 주된 출처로 맞춘 하나라서, 다른 출처 사이클의 잔차에는 일정한 치우침이 있다(사출 중 압력 등).
모델이 `group` 별로 중앙값을 빼서 표준화하므로 그 치우침은 지워진다.
"""

import json
import os
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
MODEL_DIR = os.path.dirname(HERE)
ROOT = os.path.dirname(MODEL_DIR)
sys.path.insert(0, MODEL_DIR)
sys.path.insert(0, os.path.join(MODEL_DIR, "baseline"))

import make_baselines as mb  # noqa: E402
from extract_pump_cache import ORDER  # noqa: E402
from new_pump_preprocessor import PumpBaseline, add_baseline_features  # noqa: E402

OUT_DIR = os.path.join(ROOT, "cache", "pump_dataset")
OUT_PATH = os.path.join(OUT_DIR, "cycles.pkl.xz")
BASELINE_DIR = os.path.join(MODEL_DIR, "baseline")
CALIB_EVERY = 5               # healthy 날짜를 정렬해 5일마다 1일을 calib 로 (나머지 fit)

#: 평가용 이벤트 — now_verif.ipynb 에서 확인한 것. (펌프, 시작, 끝, 이름)
EVAL_EVENTS = [
    ("P7", "2026-09-16 21:30", "2026-09-17 00:00", "P7 유량계 전조 (전날 밤)"),
    ("P7", "2026-09-17 06:45", "2026-09-17 11:30", "P7 유량계 고장"),
    ("I1", "2026-09-29 06:13", "2026-09-29 11:23", "I1 노즐 저항 감소 (FT/SV 1.09)"),
    ("I1", "2026-09-28 12:00", "2026-09-28 18:00", "I1 노즐 저항 증가 (K 7.8)"),
    ("P2", "2026-10-06 00:00", "2026-10-08 00:00", "P2 배관 저항 +7%"),
    ("I7", "2026-09-10 00:00", "2026-09-23 06:00", "I7 저항 드리프트 (톱니, 9/23 전)"),
    ("I7", "2026-10-05 21:00", "2026-10-08 00:00", "I7 저항 드리프트 재발 (테스트)"),
    ("I3", "2026-09-10 00:00", "2026-09-18 13:00", "I3 9/18 계단 전 (+5%, 작업 전으로 추정)"),
]


def build_pump(pump):
    base = PumpBaseline.load(os.path.join(BASELINE_DIR, f"{pump}.json"))
    main, src = mb.load_cycles(pump)                       # 건강 판정용 (기준선과 같은 후보 집합)
    main_cand = main[(main.split != "test") & ~main.excluded]
    hours = mb.healthy_hours(main_cand, base.fitted_on["pt_fit"])

    c = mb.load_all_cycles(pump)
    cand = (c.split != "test") & ~c.excluded
    c["healthy"] = np.nan
    c.loc[cand, "healthy"] = c.loc[cand, "t_start"].dt.floor("h").isin(hours).astype(float)
    c = add_baseline_features(c, base)
    c["group"] = pump + "|" + c.steady_src + "|" + np.where(c.n_hz_levels > 1, "multi", "single")

    days = sorted(c.loc[c.healthy == 1, "day"].unique())
    calib_days = set(days[CALIB_EVERY - 1::CALIB_EVERY]) or set(days[-1:])
    c["role"] = "eval"
    c.loc[(c.healthy == 1) & ~c.day.isin(calib_days), "role"] = "fit"
    c.loc[(c.healthy == 1) & c.day.isin(calib_days), "role"] = "calib"

    c["event"] = ""
    for p, a, b, name in EVAL_EVENTS:
        if p == pump:
            c.loc[(c.t_start >= a) & (c.t_start < b), "event"] = name
    c["material"] = base.material
    c["main_src"] = src
    return c


def build():
    parts = [build_pump(p) for p in ORDER]
    df = pd.concat(parts, ignore_index=True)
    keep = ["pump", "group", "material", "steady_src", "main_src", "n_hz_levels", "t_start", "day", "split", "excluded",
            "healthy", "role", "event",
            "SV_class", "Hz", "T_C", "mu_rel", "dHz_prev", "Gap_sec",
            "pump_resid", "r_phys", "FT_e", "t_self", "decel_half_sec", "release_half_sec", "ft_cv", "pt_std", "P0",
            "FT", "PT", "K", "FT_SV", "Hz_SV", "t_cross"]
    return df[keep]


if __name__ == "__main__":
    os.makedirs(OUT_DIR, exist_ok=True)
    df = build()
    df.to_pickle(OUT_PATH, compression="xz")
    with open(os.path.join(OUT_DIR, "cycles_meta.json"), "w", encoding="utf-8") as f:
        json.dump({"created": pd.Timestamp.now().isoformat(timespec="seconds"), "n": len(df),
                   "val_start": mb.VAL_START, "test_start": mb.TEST_START, "calib_every": CALIB_EVERY,
                   "events": EVAL_EVENTS,
                   "baselines": {p: json.load(open(os.path.join(BASELINE_DIR, f"{p}.json")))["fitted_on"]["created"]
                                 for p in ORDER}}, f, ensure_ascii=False, indent=1)
    t = df[df.split != "test"].pivot_table(index="pump", columns="role", values="t_start", aggfunc="size").reindex(ORDER).fillna(0).astype(int)
    t["test"] = df[df.split == "test"].groupby("pump").size().reindex(ORDER)
    t["이벤트"] = df[df.event != ""].groupby("pump").size().reindex(ORDER).fillna(0).astype(int)
    print(t.to_string())
    print(f"저장: {OUT_PATH}  ({len(df):,} 사이클)")
