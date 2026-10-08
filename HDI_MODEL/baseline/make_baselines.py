"""펌프 14대 기준선 생성 — 캐시 사이클(cache/pump_feat) → 펌프별 기준선 JSON.

    python HDI_MODEL/baseline/make_baselines.py               # 전 펌프
    python HDI_MODEL/baseline/make_baselines.py --pumps I7    # 일부만
    python HDI_MODEL/baseline/make_baselines.py --no-plot

기준선이 무엇이고 언제 다시 맞추는지는 `new_pump_preprocessor.md` 5절. 여기서는 그 규칙대로 **자동으로** 맞춘다.

| 기준선 | 맞추는 데이터 |
|---|---|
| ① 펌프 (FT/Hz)  | 테스트 구간 전 모든 생산일 — 넓을수록 좋다 (이상치는 fit 안에서 제거) |
| ② 압력 (PT)     | 그중 **배관이 평소 상태였던 날·시간**만 (아래 `select_healthy`) |
| ③ 순환 (FT/Hz)  | 테스트 구간 전 순환창 |

공통 제외: 테스트 구간(TEST_START 이후), EXCLUDE_DAYS, EVENTS(알려진 이상), 알람·정지 사이클(flag_any),
펌프의 주된 안정 출처(steady_src)가 아닌 사이클.

**다단 Hz 사이클(사이클 안에서 Hz 가 바뀌는 레시피)도 넣는다.** 안정값은 대표 Hz 구간에서 그 Hz 로 바뀐 뒤 5틱 이후라
물리적으로 유효하고, 빼면 다단 레시피만 쓰는 Hz 대역(P3 본사출 5973 Hz — 단일 사이클은 3803~4372)을 기준선이 외삽한다.
넣은 뒤 다단 사이클 잔차: P3 +3.9% → 0.0%, r_phys 0.64 → 0.99 / P2 −2.3% → −0.1%. 단일 사이클 쪽은 거의 그대로
(P7·I7 퍼짐만 0.24 → 0.46%).

## 평소(건강) 배관 상태를 고르는 규칙 — `select_healthy`

1. 사이클 K 를 같은 SV 클래스 중앙값으로 나눠 % 로 (레시피 차이 제거) → 일별 중앙값
2. 일별 값이 서로 RUN_RANGE(4%p) 안에 머문 **가장 긴 연속 구간** = 그 펌프의 "평소 상태", 그 중앙값 = 기준 수준
3. 기준 수준 ± DAY_BAND(3%) 안인 날 = 건강일
4. 건강일 안에서도 시간별 K 가 기준 수준 ± HOUR_BAND(5%) 를 벗어난 시간은 뺀다 (I1 처럼 하루 안에 노즐 상태가 바뀌는 펌프)

"가장 긴" 을 고르는 이유: 가장 이른 구간을 고르면 P1·P2 는 9/10~14 의 높은 저항 상태(4일)를 기준으로 삼는다.
자동 결과가 맞지 않으면 `PT_OVERRIDE` 에 날짜를 직접 적는다 (`_check.png` 로 눈으로 확인).

출력 (이 디렉터리)
  {펌프}.json         PumpBaseline.load() 로 읽는다. fitted_on 에 구간·규칙·검증값 기록
  _summary.csv        펌프별 한 줄 — 구간, 사이클 수, 학습/검증/테스트 잔차
  _healthy_days.csv   펌프 × 날짜 — 일별 K %, 건강일 여부
  _check.png          펌프별 일별 K 와 고른 건강일 — 눈으로 확인용
"""

import argparse
import json
import os
import sys
from datetime import datetime

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from build_pump_features import load_feat  # noqa: E402
from extract_pump_cache import ORDER  # noqa: E402
from new_pump_preprocessor import PumpBaseline, PumpSetup, add_baseline_features, load_settings  # noqa: E402

# --------------------------------------------------------------------------- #
# 설정
# --------------------------------------------------------------------------- #
VAL_START = "2026-09-28"     # 검증 시작 (연휴 뒤 첫 생산일) — 기준선 평가 표에서만 나눔
TEST_START = "2026-10-05"    # 테스트 시작 — 이후는 기준선 학습에 쓰지 않는다
EXCLUDE_DAYS = ["2026-09-27"]  # 연휴 중 Auto-Cal 날: 사이클 적고 재료 26~33℃
EVENTS = [                   # 알려진 이상 — 모든 기준선 학습에서 뺀다 (now_verif.ipynb)
    ("P7", "2026-09-17 06:45", "2026-09-17 11:30", "유량계 고장 (FT≈0, 압력 정상)"),
]
RUN_RANGE = 4.0              # 평소 상태 연속 구간: 일별 K 최대−최소 (%p)
DAY_BAND = 3.0               # 건강일: 기준 수준 ± (%)
HOUR_BAND = 5.0              # 건강 시간: 기준 수준 ± (%)
MIN_PT_CYCLES = 500          # 압력 기준선 최소 사이클 수 — 모자라면 경고

#: 자동 선택 대신 쓸 압력 기준선 건강일. {펌프: ["YYYY-MM-DD", ...]}
PT_OVERRIDE = {
    # 9/23 전에는 K 가 톱니 모양 — 1~2일에 걸쳐 15~20% 서서히 빠졌다가 한 번에 되돌아오기를 반복(온도 20.3~20.6℃ 일정,
    # 유량 일정, 압력만 93→75). 조절 밸브·노즐 설정이 풀리고 다시 맞춘 것으로 추정. 9/23 06시부터 +1~3% 로 평평.
    # 자동 선택은 기준 수준은 맞게 잡았지만 톱니가 우연히 같은 높이를 지난 날(9/11·16·17·21)도 섞여 있어 직접 지정 (2026-10-08)
    "I7": ["2026-09-23", "2026-09-28", "2026-09-29", "2026-09-30", "2026-10-01", "2026-10-02", "2026-10-03"],
}
#: 자동 선택 결과에 대한 메모 (출력·JSON 에 남김)
REVIEW = {
    "I7": "수동 지정 — 9/23 이후 안정 상태. 그 전 톱니(드리프트) 구간은 이상으로 잡힌다",
    "I3": "자동(9/18 이후) 유지 — 9/18 12~13시 한 시간 만에 K +4% → −1% 계단, 전후 모두 평평. 조정·청소 작업 뒤로 판단",
    "I6": "사출 중 안정값이라 K 잡음이 커서 건강일이 적다 (사이클 약 34%)",
}

SURFACE, INK, INK2, GRID, AXIS = "#fcfcfb", "#0b0b0b", "#52514e", "#e1e0d9", "#c3c2b7"
BLUE, ORANGE, MUTED = "#2a78d6", "#eb6834", "#898781"


# --------------------------------------------------------------------------- #
# 데이터
# --------------------------------------------------------------------------- #
def main_src(c):
    return "clean" if (c.steady_src == "clean").mean() > 0.5 else "shot"


def event_mask(c, pump):
    m = pd.Series(False, index=c.index)
    for p, a, b, _ in EVENTS:
        if p == pump:
            m |= (c.t_start >= a) & (c.t_start < b)
    return m


def mark(c, pump):
    """day · excluded(제외일·이벤트) · split 컬럼."""
    c = c.copy()
    c["day"] = c.t_start.dt.normalize()
    c["excluded"] = c.day.isin(pd.to_datetime(EXCLUDE_DAYS)) | event_mask(c, pump)
    c["split"] = np.where(c.t_start >= TEST_START, "test", np.where(c.t_start >= VAL_START, "val", "train"))
    return c


def load_all_cycles(pump):
    """알람·정지 제외, 값 있는 사이클 전부 (모든 안정 출처, 단일·다단 Hz)."""
    c = load_feat(pump)
    c = c[~c.flag_any].dropna(subset=["FT", "PT", "Hz", "mu_rel"])
    return mark(c[(c.FT > 0) & (c.Hz > 0)], pump)


def load_cycles(pump):
    """기준선 학습·평가에 쓰는 사이클 — 펌프의 주된 안정 출처(단일·다단 Hz 모두)."""
    c = load_all_cycles(pump)
    src = main_src(c)
    return c[c.steady_src == src].copy(), src


def load_circ(pump):
    g = load_feat(pump, "circ").dropna(subset=["FT_Hz", "PT", "mu_rel"])
    day = g.index.normalize()
    ok = (g.index < TEST_START) & ~day.isin(pd.to_datetime(EXCLUDE_DAYS))
    for p, a, b, _ in EVENTS:
        if p == pump:
            ok &= ~((g.index >= a) & (g.index < b))
    return g[ok], g


# --------------------------------------------------------------------------- #
# 평소(건강) 배관 상태
# --------------------------------------------------------------------------- #
def pipe_dev(c):
    """사이클 K 를 같은 SV 클래스 중앙값 대비 % 로 (c 안의 중앙값 기준)."""
    med = c.groupby("SV_class").K.median()
    return (c.K / c.SV_class.map(med) - 1) * 100


def healthy_from_fitted(c, pt_fit):
    """저장된 기준선 JSON 의 pt_fit(건강일·기준 수준)으로 같은 건강 사이클 마스크를 다시 만든다.
    c 는 기준선을 맞출 때와 같은 후보 집합(테스트 전·제외 뺀 것)이어야 같은 결과가 나온다."""
    k = pipe_dev(c)
    hourly = k.groupby(c.t_start.dt.floor("h")).transform("median")
    return c.day.isin(pd.to_datetime(pt_fit["healthy_days"])) & ((hourly - pt_fit["level_pct"]).abs() <= HOUR_BAND)


def healthy_hours(c, pt_fit):
    """건강 사이클이 있는 시간(floor h) 집합 — 주된 출처 사이클로 정한 건강 판정을 다른 출처 사이클에 넓힐 때."""
    m = healthy_from_fitted(c, pt_fit)
    return set(c.t_start[m].dt.floor("h"))


def select_healthy(c, pump):
    """c: 학습 후보 사이클(테스트·제외 이미 뺀 것). 반환 (사이클 마스크, 일별 K %, 정보 dict)."""
    k = pipe_dev(c)
    daily = k.groupby(c.day).median()

    if pump in PT_OVERRIDE:
        days = pd.to_datetime(PT_OVERRIDE[pump])
        level = float(daily[daily.index.isin(days)].median())
        info = {"rule": "manual", "run": None}
    else:
        best = (0, 0, 0)
        for i in range(len(daily)):
            j = i
            while j + 1 < len(daily) and np.ptp(daily.iloc[i:j + 2]) <= RUN_RANGE:
                j += 1
            if j - i + 1 > best[0]:
                best = (j - i + 1, i, j)
        _, i, j = best
        level = float(daily.iloc[i:j + 1].median())
        days = daily.index[(daily - level).abs() <= DAY_BAND]
        info = {"rule": "auto", "run": [str(daily.index[i].date()), str(daily.index[j].date())]}

    hourly = k.groupby(c.t_start.dt.floor("h")).transform("median")
    mask = c.day.isin(days) & ((hourly - level).abs() <= HOUR_BAND)
    info.update({"level_pct": round(level, 2), "healthy_days": [str(d.date()) for d in sorted(days)],
                 "excluded_days": [str(d.date()) for d in daily.index if d not in set(days)],
                 "n_hours_dropped": int((c.day.isin(days) & ~mask).groupby(c.t_start.dt.floor("h")).any().sum())})
    return mask, daily - level, info


# --------------------------------------------------------------------------- #
# 맞추기 · 평가
# --------------------------------------------------------------------------- #
def spread(x):
    x = pd.Series(x).dropna()
    return float(1.4826 * np.median(np.abs(x - x.median()))) if len(x) else np.nan


def evaluate(base, c, circ_all):
    """split 별 펌프 잔차 · r_phys · 순환 잔차의 중앙값과 퍼짐 (제외일·이벤트 뺌)."""
    x = add_baseline_features(c[~c.excluded], base)
    out = {}
    for s in ("train", "val", "test"):
        v = x[x.split == s]
        out[s] = {"n": int(len(v)),
                  "pump_resid_med": round(float(v.pump_resid.median()), 3), "pump_resid_spread": round(spread(v.pump_resid), 3),
                  "r_phys_med": round(float(v.r_phys.median()), 4), "r_phys_spread": round(spread(v.r_phys), 4)}
    if base.circ_coef is not None:
        g = circ_all
        cr = (g.FT_Hz / base.expected_circ_fthz(g.PT, g.mu_rel) - 1) * 100
        sp = np.where(g.index >= TEST_START, "test", np.where(g.index >= VAL_START, "val", "train"))
        for s in ("train", "val", "test"):
            out[s]["circ_resid_med"] = round(float(np.nanmedian(cr[sp == s])), 3) if (sp == s).any() else None
    return out


def fit_pump(pump, settings):
    setup = PumpSetup(pump, settings, allow_unverified=True)
    c, src = load_cycles(pump)
    cand = c[(c.split != "test") & ~c.excluded]
    pt_mask, daily_dev, h_info = select_healthy(cand, pump)
    circ_fit, circ_all = load_circ(pump)
    base = PumpBaseline.fit(cand, setup, circ=circ_fit, pt_cycles=cand[pt_mask])
    ev = evaluate(base, c, circ_all)
    base.fitted_on.update({
        "created": datetime.now().isoformat(timespec="seconds"), "generated_by": "baseline/make_baselines.py",
        "steady_src": src, "hz_levels": "all", "split": {"val_start": VAL_START, "test_start": TEST_START},
        "exclude_days": EXCLUDE_DAYS, "events": [e for e in EVENTS if e[0] == pump],
        "pump_fit": {"start": str(cand.t_start.min()), "end": str(cand.t_start.max()), "n": int(len(cand))},
        "pt_fit": {**h_info, "n": int(pt_mask.sum()), "params": {"RUN_RANGE": RUN_RANGE, "DAY_BAND": DAY_BAND,
                                                                  "HOUR_BAND": HOUR_BAND}},
        "circ_fit": {"n_windows": int(len(circ_fit))},
        "material_verified": setup.verified, "review": REVIEW.get(pump), "eval": ev})
    if pt_mask.sum() < MIN_PT_CYCLES:
        base.fitted_on["warning"] = f"압력 기준선 사이클 {int(pt_mask.sum())}개 < {MIN_PT_CYCLES}"
    return base, daily_dev, ev


# --------------------------------------------------------------------------- #
# 확인 그림
# --------------------------------------------------------------------------- #
def plot_check(devs, infos, path):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    matplotlib.rcParams["font.family"] = "AppleGothic"
    matplotlib.rcParams["axes.unicode_minus"] = False

    pumps = list(devs)
    ncol = 2
    nrow = (len(pumps) + 1) // ncol
    fig, axes = plt.subplots(nrow, ncol, figsize=(13, 1.7 * nrow + 0.8), facecolor=SURFACE, sharex=True)
    axes = np.atleast_2d(axes)
    for ax, p in zip(axes.ravel(), pumps):
        d = devs[p]
        ok = d.index.isin(pd.to_datetime(infos[p]["pt_fit"]["healthy_days"]))
        ax.axhspan(-DAY_BAND, DAY_BAND, color=GRID, zorder=0)
        ax.plot(d.index, d.values, color=MUTED, lw=1, zorder=2)
        ax.scatter(d.index[ok], d.values[ok], s=16, color=BLUE, zorder=3)
        ax.scatter(d.index[~ok], d.values[~ok], s=16, color=ORANGE, zorder=3)
        lim = max(8, min(40, np.nanmax(np.abs(d.values)) * 1.1))
        ax.set_ylim(-lim, lim)
        ax.set_facecolor(SURFACE)
        for s in ("top", "right"):
            ax.spines[s].set_visible(False)
        for s in ("left", "bottom"):
            ax.spines[s].set_color(AXIS)
        ax.tick_params(colors=INK2, labelsize=7)
        pf = infos[p]["pt_fit"]
        tag = "수동" if pf["rule"] == "manual" else f"평소 {pf['run'][0][5:]}~{pf['run'][1][5:]}"
        flag = "  ※확인" if infos[p].get("review") else ""
        ax.set_title(f"{p}  {tag}  건강일 {len(pf['healthy_days'])}  사이클 {pf['n']}{flag}",
                     loc="left", fontsize=8, color=ORANGE if flag else INK)
    for ax in axes.ravel()[len(pumps):]:
        ax.set_visible(False)
    import matplotlib.dates as mdates
    for ax in axes[-1]:
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d"))
    fig.suptitle("압력 기준선 건강일 — 일별 K (기준 수준 대비 %), 파랑 = 사용, 주황 = 제외, 회색 띠 = ±3%",
                 x=0.01, ha="left", fontsize=10, color=INK)
    fig.tight_layout()
    fig.savefig(path, dpi=110, facecolor=SURFACE)
    plt.close(fig)


# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pumps", nargs="*", default=None)
    ap.add_argument("--no-plot", action="store_true")
    a = ap.parse_args()
    settings = load_settings()
    pumps = a.pumps or [p for p in ORDER if p in settings["pumps"]]

    rows, devs, infos, hd = [], {}, {}, []
    for p in pumps:
        base, dev, ev = fit_pump(p, settings)
        base.save(os.path.join(HERE, f"{p}.json"))
        devs[p], infos[p] = dev, base.fitted_on
        pf = base.fitted_on["pt_fit"]
        rows.append({"pump": p, "material": base.fitted_on["material_name"], "verified": base.fitted_on["material_verified"],
                     "steady_src": base.fitted_on["steady_src"], "n_pump_fit": base.fitted_on["pump_fit"]["n"],
                     "pt_rule": pf["rule"], "pt_run": "~".join(pf["run"]) if pf["run"] else "",
                     "pt_healthy_days": len(pf["healthy_days"]), "n_pt_fit": pf["n"],
                     **{f"{s}_{k}": ev[s][k] for s in ("train", "val", "test")
                        for k in ("pump_resid_med", "pump_resid_spread", "r_phys_med")},
                     "review": REVIEW.get(p, ""), "warning": base.fitted_on.get("warning", "")})
        for d, v in dev.items():
            hd.append({"pump": p, "day": d.date(), "K_dev_pct": round(float(v), 2),
                       "healthy": str(d.date()) in pf["healthy_days"]})
        print(f"{p}: 펌프기준선 {base.fitted_on['pump_fit']['n']:>6} 사이클 | 압력기준선 {pf['n']:>6} 사이클 "
              f"(건강일 {len(pf['healthy_days'])}, {pf['rule']}) | 테스트 잔차 {ev['test']['pump_resid_med']:+.2f}% "
              f"r_phys {ev['test']['r_phys_med']:.3f}{'  ※ ' + REVIEW[p] if p in REVIEW else ''}", flush=True)

    summ = pd.DataFrame(rows)
    if a.pumps and os.path.exists(os.path.join(HERE, "_summary.csv")):   # 일부만 다시 만들면 나머지 줄은 유지
        old = pd.read_csv(os.path.join(HERE, "_summary.csv"))
        summ = pd.concat([old[~old.pump.isin(pumps)], summ]).set_index("pump").loc[
            [p for p in ORDER if p in set(old.pump) | set(pumps)]].reset_index()
    summ.to_csv(os.path.join(HERE, "_summary.csv"), index=False, encoding="utf-8-sig")
    hdf = pd.DataFrame(hd)
    if a.pumps and os.path.exists(os.path.join(HERE, "_healthy_days.csv")):
        old = pd.read_csv(os.path.join(HERE, "_healthy_days.csv"))
        hdf = pd.concat([old[~old.pump.isin(pumps)], hdf])
    hdf.to_csv(os.path.join(HERE, "_healthy_days.csv"), index=False, encoding="utf-8-sig")
    if not a.no_plot and not a.pumps:
        plot_check(devs, infos, os.path.join(HERE, "_check.png"))
    print("저장:", HERE)
