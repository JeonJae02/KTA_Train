"""P1_Exchanger_Heat_Diagnosis.ipynb 를 생성하는 스크립트.

노트북을 손으로 JSON 쓰다 깨지는 걸 막으려고 nbformat 으로 만든다.
노트북 내용을 고칠 일이 있으면 이 파일을 고치고 다시 실행하면 된다.
"""

import os

import nbformat as nbf

OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                   "P1_Exchanger_Heat_Diagnosis.ipynb")

nb = nbf.v4.new_notebook()
cells = []
md = lambda s: cells.append(nbf.v4.new_markdown_cell(s))
code = lambda s: cells.append(nbf.v4.new_code_cell(s))

# --------------------------------------------------------------------------- #
md("""# 탱크 Exchanger / Heat 진단 모델

워킹 탱크의 **exchanger / cool / heat** 이 정상 동작하는지 룰 기반으로 판정한다.
기본 대상은 P1 이고, `TANKS` 설정에 태그만 추가하면 다른 탱크도 그대로 돌아간다.

## 입력 (탱크당 4개 신호)

| 역할 | 예시(P1) | 쓰이는 곳 |
|---|---|---|
| exchanger 신호 | `워킹 Tank 1 BACK Exchanger SOL` | 규칙 ②③ |
| cool 신호 | `워킹 Tank 1 BACK Cooling SOL` | 규칙 ②③ |
| 현재 온도 | `TK_Temp_PV_P1` | 규칙 ① |
| 온도 하한 설정 | `TK_Temp_L_Set_P1` | 규칙 ① |

> 온도 계열은 **0.1℃ 단위**라 10 으로 나눠서 ℃ 로 쓴다 (`Scale_Max___TT_*=1000`).

## 판정 규칙 3가지

**① heat 이상** — 전날 `TK_Temp_PV` 최솟값이 `TK_Temp_L_Set` 보다 낮으면 이상.

**② exchanger / cool 이상** — 병합된 사이클이 켜진 뒤 **10분 안에 꺼지지 않으면** 이상.
(exchanger 는 온도가 SV 밑으로 내려가면 꺼지므로, "안 꺼졌다 = 온도를 못 잡았다" 이다.)
- 그 사이클에 cool 이 같이 켜졌으면 → `exchanger와 cool 이상`
- exchanger 만 켜졌으면 → `exchanger 이상`

**③ exchanger 성능 저하** — exchanger+cool 이 같이 들어간 사이클이 **10회 연속** 나오면,
exchanger 혼자서 온도를 못 잡는 걸로 보고 이상.

## 전처리

exchanger 의 짧은 채터링은 **90초** 미만 OFF 를 이어붙여 하나의 사이클로 합친다.

데이터가 **exchanger 가 이미 켜져 있는 도중부터** 시작할 수 있는데, 그 사이클은 언제
시작했는지 알 수 없어 길이를 못 믿는다. 그래서 **데이터 안에서 0→1 전환이 실제로 관측된
사이클만** 추적한다(첫 사이클이 잘려 있으면 자동으로 버려진다).
""")

# --------------------------------------------------------------------------- #
md("## 1. 설정")

code('''import os
import sys
import glob

import numpy as np
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt

matplotlib.rcParams["font.family"] = "Malgun Gothic"
matplotlib.rcParams["axes.unicode_minus"] = False
pd.set_option("display.width", 160)

ROOT_DIR = os.path.abspath(os.path.join(os.getcwd(), ".."))
if not os.path.exists(os.path.join(ROOT_DIR, "Log_Extractor.py")):
    ROOT_DIR = os.path.abspath(os.getcwd())
sys.path.insert(0, ROOT_DIR)

RAW_DIR = os.path.join(ROOT_DIR, "parkkt", "extracted_csv")
OUT_DIR = os.path.join(ROOT_DIR, "parkkt", "analysis_out", "p1_diagnosis")
os.makedirs(OUT_DIR, exist_ok=True)

print("ROOT_DIR =", ROOT_DIR)''')

md("""### 탱크별 태그 설정

**태그 이름은 모델 로직이 아니라 이 표에만 있다.** 탱크를 추가하려면 여기 한 줄만 넣으면 된다.

> ⚠️ 한글 태그는 탱크마다 **공백 개수가 다르다** (예: `워킹 Tank 2-1 SOFT  Heater` 는 스페이스 2개,
> `워킹 Tank 2-1 SOFT Cooling SOL` 은 1개). InfluxDB 에 찍힌 그대로 복사해야 하고,
> "정리"한답시고 공백을 맞추면 태그를 못 찾는다.
> I2 는 cool/exchanger 가 `Tank 2-2 MDI`, heat/feeding 은 `Tank 2 MDI` 로 이름 자체가 다르다.""")

code('''# 한글 태그는 규칙성이 없어서 그대로 적고, TK_Temp_* 는 탱크키와 1:1 이라 자동 생성한다.
_EXCH_COOL = {
    "P1": ("워킹 Tank 1 BACK Exchanger SOL",    "워킹 Tank 1 BACK Cooling SOL"),
    "P2": ("워킹 Tank 2-1 SOFT Exchanger SOL",  "워킹 Tank 2-1 SOFT Cooling SOL"),
    "P3": ("워킹 Tank 3-1 고탄성 Exchanger SOL", "워킹 Tank 3-1 고탄성 Cooling SOL"),
    "P4": ("워킹 Tank 2-2 HARD Exchanger SOL",  "워킹 Tank 2-2 HARD Cooling SOL"),
    "P5": ("워킹 Tank 3-2 CUSH Exchanger SOL",  "워킹 Tank 3-2 CUSH Cooling SOL"),
    "I1": ("워킹 Tank 1 ISO Exchanger SOL",     "워킹 Tank 1 ISO Cooling SOL"),
    "I2": ("워킹 Tank 2-2 MDI Exchanger SOL",   "워킹 Tank 2-2 MDI Cooling SOL"),
    "I3": ("워킹 Tank 3  ISO Exchanger SOL",    "워킹 Tank 3  ISO Cooling SOL"),
}

TANKS = {
    key: {"exch": exch, "cool": cool,
          "pv": f"TK_Temp_PV_{key}", "lset": f"TK_Temp_L_Set_{key}"}
    for key, (exch, cool) in _EXCH_COOL.items()
}

TANK = "P1"          # <- 진단할 탱크
print(TANK, TANKS[TANK])''')

code('''TEMP_SCALE = 10.0          # 0.1℃ 단위 -> ℃

# ---- 공통 파라미터 ----
MERGE_GAP_SEC   = 90       # 이보다 짧은 exchanger OFF 는 같은 사이클로 이어붙임(채터링 제거)
MAX_ON_SEC      = 600      # 사이클이 켜진 뒤 10분 안에 안 꺼지면 이상 (규칙 ②)
CONSEC_COOL_N   = 10       # cool 동반 사이클이 이만큼 연속되면 성능저하 (규칙 ③)
PV_VALID_RANGE  = (5.0, 60.0)   # ℃, 이 범위 밖 PV 는 센서 글리치로 보고 버린다

# ---- 탱크별로 값을 다르게 쓰고 싶을 때만 여기 덮어쓴다 ----
#   탱크마다 냉각 능력·사이클 리듬이 달라서, 데이터가 쌓이면 탱크별로 조정하는 게 맞다.
#   (비어 있으면 위 공통값을 그대로 쓴다)
TANK_OVERRIDES = {
    # "I2": {"MAX_ON_SEC": 900},
}


def params(tank):
    """해당 탱크에 적용할 파라미터 묶음."""
    p = {"MERGE_GAP_SEC": MERGE_GAP_SEC, "MAX_ON_SEC": MAX_ON_SEC,
         "CONSEC_COOL_N": CONSEC_COOL_N, "PV_VALID_RANGE": PV_VALID_RANGE}
    p.update(TANK_OVERRIDES.get(tank, {}))
    return p


params(TANK)''')

# --------------------------------------------------------------------------- #
md("""## 2. 데이터 로드

이미 뽑아둔 CSV 가 있으면 재사용하고, 없으면 InfluxDB 에서 받는다.
**실제 운영에서는 전처리된 데이터를 그대로 넣으면 되고, 이 셀은 건너뛰어도 된다** —
아래 전처리/진단 함수들은 전부 `pandas.Series` 4개만 받는다.""")

code('''def _latest_csv_with(*tags):
    """RAW_DIR 안에서 해당 태그들을 모두 가진 가장 최근 CSV 경로."""
    for p in sorted(glob.glob(os.path.join(RAW_DIR, "*_analysis.csv")), reverse=True):
        try:
            cols = pd.read_csv(p, nrows=0).columns
        except Exception:
            continue
        if all(t in cols for t in tags):
            return p
    return None


def _read(path, tag):
    s = pd.read_csv(path, index_col="Time", parse_dates=True, usecols=["Time", tag])
    s = s[~s.index.duplicated(keep="last")].sort_index()
    return s[tag].dropna()


def load_signals(tank, start, end, use_cache=True):
    """탱크 1개의 4개 신호를 읽어 dict(exch, cool, pv, lset) 으로 돌려준다."""
    want = TANKS[tank]
    out, missing = {}, []

    if use_cache:
        for key, tag in want.items():
            p = _latest_csv_with(tag)
            (out.__setitem__(key, _read(p, tag)) if p else missing.append(tag))
    else:
        missing = list(want.values())

    if missing:
        from Log_Extractor import LogExtractor
        print("InfluxDB 에서 추출:", missing)
        ex = LogExtractor(env_path=os.path.join(ROOT_DIR, ".env"))
        df = ex.get_data(start_time=start, end_time=end, target_tags=missing)
        ex.save_to_csv(df, save_dir=RAW_DIR)
        for key, tag in want.items():
            if tag in missing:
                out[key] = df[tag].dropna()

    # 온도 ℃ 변환 + 글리치 제거
    lo, hi = params(tank)["PV_VALID_RANGE"]
    for key in ("pv", "lset"):
        out[key] = out[key] / TEMP_SCALE
    n_before = len(out["pv"])
    out["pv"] = out["pv"][(out["pv"] >= lo) & (out["pv"] <= hi)]
    print(f"PV 글리치 제거: {n_before - len(out['pv'])}개 (허용범위 {lo}~{hi}℃)")

    # 요청 구간으로 자르기
    s = pd.Timestamp(start)
    e = pd.Timestamp.now() if end == "now()" else pd.Timestamp(end)
    for key in out:
        out[key] = out[key].loc[s:e]
    return out''')

code('''START = "2026-09-09 10:00:00"
END   = "now()"

sig = load_signals(TANK, START, END)
for k, v in sig.items():
    print(f"{k:5s} {len(v):>9,}행   {v.index.min()} ~ {v.index.max()}")''')

# --------------------------------------------------------------------------- #
md("""## 3. 전처리 — exchanger 사이클 병합

원시 신호는 SV 문턱 근처에서 0.5~3초 간격으로 계속 깜빡인다(채터링).
`MERGE_GAP_SEC` 미만의 OFF 는 같은 사이클로 보고 이어붙인다.

> 실측으로 OFF 길이 분포에 **55.7초 ~ 402.1초 사이가 완전히 비어 있는** 구간이 있어서,
> 그 안의 값이면 90초든 180초든 결과가 동일하다.

**잘린 첫 사이클 처리** — `on_starts` 를 "데이터 안에서 0→1 전환이 실제로 보인 것" 으로만
잡는다. 데이터가 exchanger 켜진 도중부터 시작하면 그 사이클은 시작 시각을 모르니
자동으로 빠진다.""")

code('''def _segments(sig01):
    """0/1 신호에서 (ON 시작, OFF 시작) 구간 목록.

    ON 시작은 **0 -> 1 전환이 데이터 안에서 실제로 보인 것만** 인정한다.
    (데이터 첫 샘플이 이미 1 이면 그 사이클은 시작 시각을 알 수 없으므로 버린다)
    """
    s = sig01[sig01.diff() != 0]
    ch = pd.DataFrame({"val": s})
    ch["prev"] = ch["val"].shift(1)
    on_starts = ch.index[(ch["prev"] == 0) & (ch["val"] == 1)]
    off_starts = ch.index[(ch["prev"] == 1) & (ch["val"] == 0)]
    segs, oi = [], 0
    for on_t in on_starts:
        while oi < len(off_starts) and off_starts[oi] <= on_t:
            oi += 1
        if oi >= len(off_starts):
            break
        segs.append((on_t, off_starts[oi]))
    return segs


def merge_cycles(exch, gap_sec=None, tank=None):
    """채터링을 합쳐 '논리적 사이클' 표를 만든다."""
    gap_sec = gap_sec or params(tank or TANK)["MERGE_GAP_SEC"]
    segs = _segments(exch)
    if not segs:
        return pd.DataFrame(columns=["cycle_start", "cycle_end", "next_on",
                                      "off_duration_sec", "n_bridged", "on_duration_sec"])
    rows, cycle_start, n_bridged = [], segs[0][0], 0
    for i in range(len(segs) - 1):
        gap = (segs[i + 1][0] - segs[i][1]).total_seconds()
        if gap < gap_sec:
            n_bridged += 1
            continue
        rows.append({"cycle_start": cycle_start, "cycle_end": segs[i][1],
                     "next_on": segs[i + 1][0], "off_duration_sec": gap,
                     "n_bridged": n_bridged})
        cycle_start, n_bridged = segs[i + 1][0], 0
    rows.append({"cycle_start": cycle_start, "cycle_end": segs[-1][1],
                 "next_on": pd.NaT, "off_duration_sec": np.nan, "n_bridged": n_bridged})
    df = pd.DataFrame(rows)
    df["on_duration_sec"] = (df["cycle_end"] - df["cycle_start"]).dt.total_seconds()
    return df


cycles = merge_cycles(sig["exch"], tank=TANK)
first_val = sig["exch"].iloc[0]
print(f"데이터 첫 샘플의 exchanger 값 = {first_val:.0f}"
      f"{'  -> 켜진 도중부터 시작, 첫 사이클 버림' if first_val == 1 else ''}")
print(f"raw ON 구간 {len(_segments(sig['exch'])):,}개 -> 병합 사이클 {len(cycles):,}개")
cycles.head()''')

md("""### cool 동반 여부""")

code('''def flag_cool(cycles, cool):
    """각 사이클 구간 동안 cool 이 한 번이라도 1 이었는가."""
    cool = cool.sort_index()
    flags = []
    for _, r in cycles.iterrows():
        seg = cool.loc[r["cycle_start"]:r["cycle_end"]]
        before = cool.loc[:r["cycle_start"]]
        start_val = before.iloc[-1] if len(before) else 0
        flags.append(bool((seg == 1).any() or start_val == 1))
    cycles = cycles.copy()
    cycles["cool_overlap"] = flags
    return cycles


cycles = flag_cool(cycles, sig["cool"])
print(f"cool 동반 사이클: {cycles['cool_overlap'].sum()}개 / {len(cycles)}개 "
      f"({100*cycles['cool_overlap'].mean():.1f}%)")''')

# --------------------------------------------------------------------------- #
md("""## 4. 진단 규칙

### 규칙 ① heat 이상 — 전날 최저 PV < L_Set""")

code('''def check_heater(pv, lset):
    """일별 최저 PV 와 그날의 L_Set 을 비교. 전날 것을 다음날 아침에 판정한다고 보면 된다."""
    daily = pv.groupby(pv.index.date).min().rename("pv_min").to_frame()
    daily["l_set"] = lset.groupby(lset.index.date).median().reindex(daily.index).ffill()
    daily["margin"] = daily["pv_min"] - daily["l_set"]
    daily["heat_abnormal"] = daily["pv_min"] < daily["l_set"]
    daily.index.name = "day"
    return daily


heat_daily = check_heater(sig["pv"], sig["lset"])
print(f"heat 이상 판정일: {int(heat_daily['heat_abnormal'].sum())}일 / {len(heat_daily)}일")
heat_daily.round(2)''')

md("""### 규칙 ② exchanger / cool 이상 — 10분 안에 사이클이 안 꺼짐

exchanger 는 온도가 SV 밑으로 내려가면 꺼지게 되어 있다. 그래서 **사이클이 계속 켜져 있다
= 아직 온도를 못 잡았다** 와 같은 말이다.""")

code('''def check_cycle_duration(cycles, max_on=None, tank=None):
    """ON 지속시간이 max_on 을 넘으면 이상. cool 동반 여부로 원인을 나눈다."""
    max_on = max_on or params(tank or TANK)["MAX_ON_SEC"]
    out = cycles.copy()
    out["abnormal"] = out["on_duration_sec"] > max_on
    out["cause"] = None
    out.loc[out["abnormal"] & out["cool_overlap"], "cause"] = "exchanger와 cool 이상"
    out.loc[out["abnormal"] & ~out["cool_overlap"], "cause"] = "exchanger 이상"
    return out


cycles = check_cycle_duration(cycles, tank=TANK)
print(f"{len(cycles)}개 중 이상 {int(cycles['abnormal'].sum())}개 "
      f"(기준: ON > {params(TANK)['MAX_ON_SEC']}초)")
print(cycles[cycles["abnormal"]][
    ["cycle_start", "cycle_end", "on_duration_sec", "cool_overlap", "cause"]].to_string(index=False))''')

md("""### 규칙 ③ exchanger 성능 저하 — cool 동반 사이클 10회 연속""")

code('''def check_consecutive_cool(cycles, n=None, tank=None):
    """cool 동반이 n회 연속인 지점을 표시한다."""
    n = n or params(tank or TANK)["CONSEC_COOL_N"]
    out = cycles.sort_values("cycle_start").reset_index(drop=True).copy()
    flags, runs, run = [], [], 0
    vals = out["cool_overlap"].astype(bool).values
    for i, v in enumerate(vals):
        run = run + 1 if v else 0
        flags.append(run >= n)
        if run > 0 and (i + 1 == len(vals) or not vals[i + 1]):
            runs.append({"run_len": run,
                         "start_time": out["cycle_start"].iloc[i - run + 1],
                         "end_time": out["cycle_start"].iloc[i]})
    out["degrade_flag"] = flags
    return out, pd.DataFrame(runs)


cycles, cool_runs = check_consecutive_cool(cycles, tank=TANK)
print(f"cool 연속 런 {len(cool_runs)}개 — 최대 {int(cool_runs['run_len'].max())}회 연속")
print(f"{params(TANK)['CONSEC_COOL_N']}회 연속 도달: {int(cycles['degrade_flag'].sum())}건")
cool_runs["run_len"].value_counts().sort_index()''')

# --------------------------------------------------------------------------- #
md("""## 5. 통합 진단 함수

탱크 하나를 통째로 돌리는 진입점.""")

code('''def diagnose(tank, start, end, use_cache=True, verbose=True):
    """탱크 1개 진단 전체 파이프라인. (일별 heat 결과, 사이클별 결과, 요약) 반환."""
    s = load_signals(tank, start, end, use_cache=use_cache)

    cyc = merge_cycles(s["exch"], tank=tank)
    cyc = flag_cool(cyc, s["cool"])
    cyc = check_cycle_duration(cyc, tank=tank)
    cyc, runs = check_consecutive_cool(cyc, tank=tank)
    heat = check_heater(s["pv"], s["lset"])

    summary = {
        "탱크": tank,
        "기간": f"{start} ~ {end}",
        "사이클 수": len(cyc),
        "① heat 이상(일)": int(heat["heat_abnormal"].sum()),
        "② exchanger 이상": int((cyc["cause"] == "exchanger 이상").sum()),
        "② exchanger+cool 이상": int((cyc["cause"] == "exchanger와 cool 이상").sum()),
        "③ 성능저하(연속)": int(cyc["degrade_flag"].sum()),
        "cool 최대 연속": int(runs["run_len"].max()) if len(runs) else 0,
    }
    if verbose:
        for k, v in summary.items():
            print(f"  {k:22s} {v}")
    return heat, cyc, summary


heat_daily, cycles, summary = diagnose(TANK, START, END)''')

# --------------------------------------------------------------------------- #
md("""## 6. 결과 리포트""")

code('''print("=== ① heat 진단 (일별 최저 PV vs L_Set) ===")
display(heat_daily.round(2))

print("\\n=== ② exchanger / cool 이상 사이클 ===")
hits = cycles[cycles["abnormal"]]
display(hits[["cycle_start", "cycle_end", "on_duration_sec", "cool_overlap", "cause"]]
        if len(hits) else "없음")

print("\\n=== ③ exchanger 성능저하 ===")
deg = cycles[cycles["degrade_flag"]]
print(f"{len(deg)}건" if len(deg) else "없음")''')

code('''# 정상 사이클 ON 지속시간 분포 — 판정 기준이 적절한지 확인
ok = cycles[~cycles["abnormal"]]["on_duration_sec"]
thr = params(TANK)["MAX_ON_SEC"]

fig, ax = plt.subplots(figsize=(11, 4.5))
ax.hist(ok, bins=60, color="#0072B2", alpha=0.85)
ax.axvline(thr, color="#D55E00", linestyle="--", linewidth=1.6,
           label=f"판정 기준 {thr}초 ({thr/60:.0f}분)")
ax.axvline(ok.max(), color="#009E73", linestyle=":", linewidth=1.6,
           label=f"정상 최댓값 {ok.max():.0f}초")
ax.set_xlabel("사이클 ON 지속시간 (초)")
ax.set_ylabel("사이클 수")
ax.set_title(f"[{TANK}] 정상 사이클 ON 지속시간 분포 vs 판정 기준", loc="left")
ax.legend(frameon=False)
ax.spines[["top", "right"]].set_visible(False)
fig.tight_layout()
plt.show()

print(f"정상 최댓값 {ok.max():.0f}초 / 기준 {thr}초 → 여유 {thr/ok.max():.1f}배")
print(f"99% 지점 {ok.quantile(0.99):.0f}초, 99.9% 지점 {ok.quantile(0.999):.0f}초")''')

code('''heat_daily.to_csv(os.path.join(OUT_DIR, f"{TANK}_heat_daily.csv"), encoding="utf-8-sig")
cycles.to_csv(os.path.join(OUT_DIR, f"{TANK}_cycles_diagnosis.csv"), index=False, encoding="utf-8-sig")
print("저장:", OUT_DIR)''')

# --------------------------------------------------------------------------- #
md("""## 7. 여러 탱크 한 번에

`TANKS` 에 태그가 들어 있는 탱크는 아래처럼 반복만 돌리면 된다.
(태그가 없는 탱크는 추출 단계에서 "데이터 없는 태그" 경고가 뜨므로 건너뛴다)""")

code('''def diagnose_many(tank_list, start, end):
    rows = []
    for t in tank_list:
        try:
            _, _, s = diagnose(t, start, end, verbose=False)
            rows.append(s)
        except Exception as e:
            rows.append({"탱크": t, "기간": f"{start} ~ {end}", "오류": str(e)[:80]})
    return pd.DataFrame(rows)


# 예시 (P1 만). 다른 탱크를 돌리려면 리스트에 키를 추가한다.
diagnose_many(["P1"], START, END)''')

# --------------------------------------------------------------------------- #
md("""## 8. 실시간 판정 함수

사이클이 **끝나기를 기다릴 필요가 없다** — 켜진 지 10분이 지나는 순간 바로 이상으로 띄운다.
`cycle_end=None` 으로 넘기면 "아직 켜져 있는 중" 으로 보고 현재 시각 기준으로 판정한다.""")

code('''def judge_cycle(cycle_start, cycle_end, cool, now=None, recent_cool_run=0, tank=None):
    """사이클 하나 판정.

    cycle_start     : 사이클 ON 시작 시각 (0->1 전환이 실제로 관측된 것만 넣을 것)
    cycle_end       : 꺼진 시각. 아직 켜져 있으면 None
    now             : 현재 시각 (cycle_end 가 None 일 때 경과시간 계산용)
    recent_cool_run : 이 사이클 직전까지 이어진 cool 동반 연속 횟수

    반환 : dict(상태, 원인, ON경과(초), cool동반, cool연속)
    """
    p = params(tank or TANK)
    now = now or pd.Timestamp.now()
    end = cycle_end if cycle_end is not None else now
    on_sec = (end - cycle_start).total_seconds()

    seg_cool = cool.loc[cycle_start:end]
    before = cool.loc[:cycle_start]
    start_val = before.iloc[-1] if len(before) else 0
    has_cool = bool((seg_cool == 1).any() or start_val == 1)

    run = recent_cool_run + 1 if has_cool else 0

    if on_sec > p["MAX_ON_SEC"]:
        state = "이상"
        cause = "exchanger와 cool 이상" if has_cool else "exchanger 이상"
    else:
        state = "정상" if cycle_end is not None else "감시중"
        cause = None

    if run >= p["CONSEC_COOL_N"]:
        state = "이상"
        cause = (cause + " / exchanger 성능저하") if cause else "exchanger 성능저하"

    return {"상태": state, "원인": cause, "ON경과(초)": round(on_sec, 1),
            "cool동반": has_cool, "cool연속": run}


r = cycles.iloc[-1]
print("정상 예시 :", judge_cycle(r["cycle_start"], r["cycle_end"], sig["cool"], tank=TANK))

bad = cycles[cycles["abnormal"]]
if len(bad):
    b = bad.iloc[-1]
    print("이상 예시 :", judge_cycle(b["cycle_start"], b["cycle_end"], sig["cool"], tank=TANK))
    t_now = b["cycle_start"] + pd.Timedelta(seconds=params(TANK)["MAX_ON_SEC"] + 1)
    print("실시간 예시:", judge_cycle(b["cycle_start"], None, sig["cool"], now=t_now, tank=TANK))''')

nb["cells"] = cells
nb.metadata = {
    "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
    "language_info": {"name": "python", "version": "3.13"},
}

with open(OUT, "w", encoding="utf-8") as f:
    nbf.write(nb, f)
print("생성:", OUT)
