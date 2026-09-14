"""여러 워킹 탱크에 대해 온도(Scale_Out___TT_*)와 heat/cool/exchanger/feeding
4개 디지털 제어 요소를 같은 그림에 위아래로 겹쳐서 본다.

`Temp_Vs_Factors_Plot.py` (P1 BACK 한 탱크만 보던 것)를 일반화한 것 —
탱크가 늘어도 `TANKS` 에 한 줄 추가하면 되고, 실행할 때는 **시간 범위만**
`--start`/`--end` 로 바꿔주면 된다.

사용법:
    python Tank_Temp_Factor_Analysis.py --start "2026-09-09 19:00:00"
    python Tank_Temp_Factor_Analysis.py --start "2026-09-09 19:00:00" --end "2026-09-10 06:00:00"
    python Tank_Temp_Factor_Analysis.py --start "-1d" --tanks P1 P2   (일부 탱크만)

**태그명 공백 주의.** PLC 원본 태그 이름의 공백 개수가 탱크마다 다르다
(예: "워킹 Tank 2-1 SOFT  Heater" 는 SOFT 뒤에 스페이스 2개인데
"워킹 Tank 2-1 SOFT Cooling SOL" 은 1개다. I2 는 heat/feeding 은
"Tank 2 MDI", cool/exchanger 는 "Tank 2-2 MDI" 로 이름 자체가 다르다).
전부 InfluxDB 에 실제로 찍혀 있는 tag_name 을 그대로 복사한 것이니 —
"정리"한답시고 공백을 맞추면 태그를 못 찾게 된다.
"""

import os
import sys
from datetime import datetime, timedelta

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# 한글 라벨이 깨지지 않게 (Windows 기본 탑재 폰트).
matplotlib.rcParams["font.family"] = "Malgun Gothic"
matplotlib.rcParams["axes.unicode_minus"] = False

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from Log_Extractor import LogExtractor
from parkkt.code_ongoing.PLC_Signed_Convert import to_signed16

ENV_PATH = os.path.join(ROOT_DIR, ".env")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SAVE_DIR = os.path.join(BASE_DIR, "extracted_csv")
OUT_DIR = os.path.join(BASE_DIR, "analysis_out")

FACTOR_NAMES = ("heat", "cool", "exchanger", "feeding")

#: 사이클 카운터. 1 부터 42 까지 1씩 오르다가 42 에서 1 로 리셋된다 (0 은 안 나온다).
#: 탱크별 신호가 아니라 라인 공통이라, 모든 탱크 CSV 에 같은 값이 들어간다 —
#: 그래야 CSV 하나만 열어도 사이클을 끊을 수 있다.
WAGON_TAG = "Now_Actual_Wagon_Num"

TANKS = {
    "P1": {
        "temp": "Scale_Out___TT_P1",
        "heat": "워킹 Tank 1 BACK Heater",
        "cool": "워킹 Tank 1 BACK Cooling SOL",
        "exchanger": "워킹 Tank 1 BACK Exchanger SOL",
        "feeding": "워킹 Tank 1 BACK Feeding SOL",
    },
    "P2": {
        "temp": "Scale_Out___TT_P2",
        "heat": "워킹 Tank 2-1 SOFT  Heater",
        "cool": "워킹 Tank 2-1 SOFT Cooling SOL",
        "exchanger": "워킹 Tank 2-1 SOFT Exchanger SOL",
        "feeding": "워킹 Tank 2-1 SOFT  Feeding SOL",
    },
    "P3": {
        "temp": "Scale_Out___TT_P3",
        "heat": "워킹 Tank 3-1 고탄성  Heater",
        "cool": "워킹 Tank 3-1 고탄성 Cooling SOL",
        "exchanger": "워킹 Tank 3-1 고탄성 Exchanger SOL",
        "feeding": "워킹 Tank 3-1 고탄성  Feeding SOL",
    },
    "P4": {
        "temp": "Scale_Out___TT_P4",
        "heat": "워킹 Tank 2-2 HARD  Heater",
        "cool": "워킹 Tank 2-2 HARD Cooling SOL",
        "exchanger": "워킹 Tank 2-2 HARD Exchanger SOL",
        "feeding": "워킹 Tank 2-2 HARD  Feeding SOL",
    },
    "P5": {
        "temp": "Scale_Out___TT_P5",
        "heat": "워킹 Tank 3-2 CUSH  Heater",
        "cool": "워킹 Tank 3-2 CUSH Cooling SOL",
        "exchanger": "워킹 Tank 3-2 CUSH Exchanger SOL",
        "feeding": "워킹 Tank 3-2 CUSH  Feeding SOL",
    },
    "I1": {
        "temp": "Scale_Out___TT_I1",
        "heat": "워킹 Tank 1 ISO Heater",
        "cool": "워킹 Tank 1 ISO Cooling SOL",
        "exchanger": "워킹 Tank 1 ISO Exchanger SOL",
        "feeding": "워킹 Tank 1 ISO Feeding SOL",
    },
    "I2": {
        "temp": "Scale_Out___TT_I2",
        "heat": "워킹 Tank 2 MDI  Heater",
        "cool": "워킹 Tank 2-2 MDI Cooling SOL",
        "exchanger": "워킹 Tank 2-2 MDI Exchanger SOL",
        "feeding": "워킹 Tank 2  MDI Feeding SOL",
    },
    "I3": {
        "temp": "Scale_Out___TT_I3",
        "heat": "워킹 Tank 3  ISO  Heater",
        "cool": "워킹 Tank 3  ISO Cooling SOL",
        "exchanger": "워킹 Tank 3  ISO Exchanger SOL",
        "feeding": "워킹 Tank 3  ISO  Feeding SOL",
    },
}

#: 수위(level)는 위의 한글 태그들과 달리 탱크 키와 정확히 1:1 로 규칙적이라
#: 일일이 적지 않고 붙인다. heat/cool 처럼 이진값이 아니라 연속값이므로
#: FACTOR_NAMES 에는 넣지 않는다 (그래프도 계단이 아니라 선으로 그려야 한다).
#:
#: **`TK_Level_PV_*` 는 정수로 반올림된 값이다.** 실측하면 센서 자체의 잡음은
#: 0.04~0.08 인데 정수화가 ±0.5 오차를 집어넣고 있다 — 버려지는 게 노이즈가
#: 아니라 실제 신호다. 그래서 원시 아날로그값에서 실수 수위(`level_f`)를 같이
#: 계산한다. 해상도가 38~67배 올라간다 (고유값 10~15개 -> 377~940개).
#: `Scale_Out___LT_*` 는 쓰면 안 된다 — PLC 가 이미 정수로 만든 값이라 이득이 없다.
LEVEL_COMPONENTS = ("Ana_In", "Ana_Max", "Scale_Max", "Gain", "OffSet")

for _key, _cfg in TANKS.items():
    _cfg["level"] = f"TK_Level_PV_{_key}"
    for _c in LEVEL_COMPONENTS:
        _cfg[f"lt_{_c}"] = f"{_c}___LT_{_key}"

DEFAULT_START = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")
DEFAULT_END = "now()"


def add_level_float(df, cfg):
    """정수 수위 대신 원시 아날로그값에서 실수 수위를 계산해 `level_f` 로 붙인다.

    Scale_Out = (Ana_In / Ana_Max) * Scale_Max * (Gain/1000) + OffSet

    현재 설정은 8개 탱크 전부 Ana_Max=10000, Scale_Max=100, Gain=1000, OffSet=0
    이라 결과적으로 `Ana_In/100` 이 되지만, **그렇게 하드코딩하면 안 된다** —
    온도 쪽 Scale_Max 가 4월 100 에서 지금 1000 으로 바뀐 전례가 있다.
    """
    cols = {c: to_signed16(df[cfg[f"lt_{c}"]]) for c in LEVEL_COMPONENTS}
    ana_max = cols["Ana_Max"].replace(0, np.nan)   # 0 이면 미설정 채널
    df["level_f"] = ((cols["Ana_In"] / ana_max) * cols["Scale_Max"]
                     * (cols["Gain"] / 1000) + cols["OffSet"])
    # 원시 부품은 계산에 다 들어갔으므로 버린다 (4개는 상수, Ana_In 은 level_f 에 담겼다).
    # 남겨두면 CSV 가 40% 커지는데 얻는 정보가 없다.
    return df.drop(columns=[cfg[f"lt_{c}"] for c in LEVEL_COMPONENTS])


def fetch_tank(extractor, tank_key, cfg, start_time, end_time):
    tags = ([cfg["temp"], cfg["level"]] + [cfg[f] for f in FACTOR_NAMES]
            + [cfg[f"lt_{c}"] for c in LEVEL_COMPONENTS] + [WAGON_TAG])
    df = extractor.get_data(start_time=start_time, end_time=end_time, target_tags=tags)
    if df.empty:
        print(f"⚠️ [{tank_key}] 데이터가 없습니다.")
        return None

    df[cfg["temp"]] = to_signed16(df[cfg["temp"]])
    df = add_level_float(df, cfg)

    os.makedirs(SAVE_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    csv_path = os.path.join(SAVE_DIR, f"{tank_key}_Temp_vs_Factors_{timestamp}.csv")
    df.to_csv(csv_path, encoding="utf-8-sig")
    print(f"💾 [{tank_key}] CSV: {csv_path}")
    return df


def plot_tank(tank_key, cfg, df):
    n_factors = len(FACTOR_NAMES)
    fig, axes = plt.subplots(
        1 + n_factors, 1, sharex=True, figsize=(16, 3 + 1.3 * n_factors),
        gridspec_kw={"height_ratios": [3] + [1] * n_factors},
    )

    temp_tag = cfg["temp"]
    ax_temp = axes[0]
    ax_temp.plot(df.index, df[temp_tag], color="#d62728", linewidth=0.9)
    ax_temp.set_ylabel(temp_tag, fontsize=9)
    ax_temp.set_title(f"{tank_key} — 온도 vs 제어 요소 (heat/cool/exchanger/feeding)")
    ax_temp.grid(axis="y", alpha=0.3)

    heat = df[cfg["heat"]]
    cool = df[cfg["cool"]]
    both_off = (heat == 0) & (cool == 0)
    both_on = (heat == 1) & (cool == 1)
    ylim = ax_temp.get_ylim()
    if both_off.any():
        ax_temp.fill_between(df.index, ylim[0], ylim[1], where=both_off.values,
                              color="orange", alpha=0.15, label="heat/cool 둘 다 OFF", step="post")
    if both_on.any():
        ax_temp.fill_between(df.index, ylim[0], ylim[1], where=both_on.values,
                              color="purple", alpha=0.15, label="heat/cool 둘 다 ON", step="post")
    if both_off.any() or both_on.any():
        ax_temp.legend(loc="upper right", fontsize=8)

    colors = {"heat": "#d62728", "cool": "#1f77b4", "exchanger": "#2ca02c", "feeding": "#9467bd"}
    for ax, name in zip(axes[1:], FACTOR_NAMES):
        tag = cfg[name]
        ax.step(df.index, df[tag], where="post", color=colors[name], linewidth=1.0)
        ax.set_ylim(-0.2, 1.2)
        ax.set_yticks([0, 1])
        ax.set_ylabel(name, fontsize=9)
        ax.set_title(tag, loc="right", fontsize=8, color="#666666", pad=2)
        ax.grid(axis="y", alpha=0.3)

    fig.autofmt_xdate()
    fig.tight_layout()

    os.makedirs(OUT_DIR, exist_ok=True)
    out_path = os.path.join(OUT_DIR, f"{tank_key}_Temp_vs_Factors.png")
    fig.savefig(out_path, dpi=130)
    plt.close(fig)
    print(f"🖼  [{tank_key}] 그래프: {out_path}")

    return len(df), int(both_off.sum()), int(both_on.sum())


def main(start_time=DEFAULT_START, end_time=DEFAULT_END, tanks=None):
    extractor = LogExtractor(env_path=ENV_PATH)
    tank_keys = tanks or list(TANKS.keys())

    summary = []
    for tank_key in tank_keys:
        cfg = TANKS[tank_key]
        print(f"\n===== {tank_key} =====")
        df = fetch_tank(extractor, tank_key, cfg, start_time, end_time)
        if df is None:
            continue
        n_rows, n_both_off, n_both_on = plot_tank(tank_key, cfg, df)
        summary.append((tank_key, n_rows, n_both_off, n_both_on))

    print("\n=== heat/cool 상호배타성 체크 (둘 다 OFF / 둘 다 ON 이면 0이어야 정상) ===")
    for tank_key, n_rows, n_both_off, n_both_on in summary:
        print(f"{tank_key:4s} rows={n_rows:6d}  둘다OFF={n_both_off:5d}  둘다ON={n_both_on:5d}")

    return summary


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", default=DEFAULT_START,
                         help="시작 시각. 예: '2026-09-09 19:00:00' (KST 로 해석). 기본: 어제 00:00")
    parser.add_argument("--end", default=DEFAULT_END, help="끝 시각. 기본 now()")
    parser.add_argument("--tanks", nargs="+", default=None, choices=list(TANKS.keys()),
                         help="분석할 탱크만 고르기 (예: --tanks P1 P2). 기본: 전체 8개")
    args = parser.parse_args()

    main(start_time=args.start, end_time=args.end, tanks=args.tanks)
