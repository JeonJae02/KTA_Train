"""전처리 설명 노트북 + 최종 정리 노트북을 생성한다.

노트북을 손으로 JSON 쓰다 깨지는 걸 막으려고 nbformat 으로 만든다.
내용을 고칠 일이 있으면 이 파일을 고치고 다시 실행하면 된다.
"""

import os

import nbformat as nbf

PARKKT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def build(path, cells_spec, title):
    nb = nbf.v4.new_notebook()
    cells = []
    for kind, src in cells_spec:
        cells.append(nbf.v4.new_markdown_cell(src) if kind == "md"
                     else nbf.v4.new_code_cell(src))
    nb["cells"] = cells
    nb.metadata = {
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {"name": "python", "version": "3.13"},
    }
    with open(path, "w", encoding="utf-8") as f:
        nbf.write(nb, f)
    print("생성:", path)


# =========================================================================== #
# 1) 전처리 설명 노트북
# =========================================================================== #
PRE = []
m = lambda s: PRE.append(("md", s))
c = lambda s: PRE.append(("code", s))

m("""# 전처리 설명 — 탱크 신호 정리

`Tank_Preprocess.py` 가 하는 일을 단계별로 눈으로 확인하는 노트북.

원시 PLC 신호를 진단 모델에 바로 넣으면 안 되는 이유가 있다. **exchanger 신호가
SV 문턱에서 초 단위로 들썩여서, 한 번의 냉각 동작이 수십 개 조각으로 쪼개져 보이기
때문**이다. 이걸 정리하지 않으면 "사이클 길이" 같은 지표가 전부 무의미해진다.

## 전처리 4단계

| 단계 | 하는 일 | 왜 |
|---|---|---|
| 1 | 온도 스케일 변환 | `TK_Temp_*` 는 0.1℃ 단위 |
| 2 | 센서 글리치 제거 | PV 가 0℃ / 327℃ 로 튀는 기록이 있음 |
| 3 | **exchanger 채터링 병합** | 핵심. 쪼개진 조각을 하나의 사이클로 |
| 4 | cool 동반 여부 표시 | 사이클마다 cool 이 같이 켜졌는지 |
""")

m("## 0. 준비")
c('''import os
import sys

import numpy as np
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt

matplotlib.rcParams["font.family"] = "Malgun Gothic"
matplotlib.rcParams["axes.unicode_minus"] = False
pd.set_option("display.width", 160)

PARKKT = os.getcwd() if os.path.basename(os.getcwd()) == "parkkt" else os.path.join(os.getcwd(), "parkkt")
sys.path.insert(0, PARKKT)

import Tank_Preprocess as prep

TANK = "P1"
START, END = "2026-09-09 10:00:00", "now()"
print("대상 탱크:", TANK, "/", prep.TANKS[TANK])''')

c('''sig = prep.load_signals(TANK, START, END)
for k, v in sig.items():
    print(f"{k:5s} {len(v):>9,}행   {v.index.min()} ~ {v.index.max()}")''')

m("""## 1~2. 온도 스케일 변환 + 센서 글리치 제거

`TK_Temp_*` 는 0.1℃ 단위라 10 으로 나눈다 (`Scale_Max___TT_*=1000` 이라서).

그리고 PV 가 **물리적으로 불가능한 값으로 튀는 기록**이 있다. 2026-09-09 에 0℃ 와
327℃ 가 찍혔는데, 이걸 안 거르면 "일별 최저 PV" 가 0℃ 로 잡혀서 heat 진단이
오판정된다. 허용범위 밖은 버린다.""")

c('''pv_raw, lset_raw = sig["pv"], sig["lset"]
pv, lset, n_glitch = prep.clean_temperature(pv_raw, lset_raw)

print(f"변환 전 PV 범위: {pv_raw.min():.0f} ~ {pv_raw.max():.0f} (0.1℃ 단위)")
print(f"변환 후 PV 범위: {(pv_raw/10).min():.1f} ~ {(pv_raw/10).max():.1f} ℃")
print(f"글리치 제거: {n_glitch}개  ->  {pv.min():.1f} ~ {pv.max():.1f} ℃")
print(f"L_Set: {lset.median():.1f} ℃")''')

c('''# 글리치가 일별 최저 PV 를 어떻게 망치는지
before = (pv_raw / 10).groupby((pv_raw.index.date)).min()
after = pv.groupby(pv.index.date).min()
cmp = pd.DataFrame({"제거 전": before, "제거 후": after})
cmp["차이"] = cmp["제거 후"] - cmp["제거 전"]
display(cmp[cmp["차이"].abs() > 0.01].round(1))''')

m("""## 3. exchanger 채터링 병합 — 핵심 단계

### 왜 필요한가

exchanger 는 온도가 SV 에 닿으면 켜지는데, 그 문턱에서 센서값이 229-230-231-230 식으로
들썩인다. 그래서 원시 신호는 0.5~3초짜리 ON/OFF 가 수십 번 반복되는 것처럼 보인다.""")

c('''# 원시 신호를 30분만 확대해서 보기
t0 = pd.Timestamp("2026-09-10 09:00:00")
seg = sig["exch"].loc[t0:t0 + pd.Timedelta(minutes=30)]

fig, ax = plt.subplots(figsize=(13, 3))
ax.step(seg.index, seg.values, where="post", color="#0072B2", linewidth=1.2)
ax.set_ylim(-0.15, 1.15); ax.set_yticks([0, 1])
ax.set_title("원시 exchanger 신호 30분 — 하나의 냉각 동작이 잘게 쪼개져 보인다", loc="left")
ax.spines[["top", "right"]].set_visible(False)
fig.tight_layout(); plt.show()

print(f"이 30분 안의 raw ON 구간 수: {len(prep.raw_segments(seg))}개")''')

m("""### 어디서 끊을 것인가 — OFF 길이 분포

OFF 길이를 전부 모아보면 **두 덩어리로 완전히 갈린다.** 그 사이에 아무것도 없는
빈 구간이 있어서, 그 안의 값이면 임계를 어디로 잡든 결과가 같다.""")

c('''segs = prep.raw_segments(sig["exch"])
gaps = pd.Series([(segs[i+1][0] - segs[i][1]).total_seconds() for i in range(len(segs)-1)])

short_max = gaps[gaps < 100].max()
long_min = gaps[gaps > 100].min()
print(f"짧은 쪽 최댓값 {short_max:.1f}초  /  긴 쪽 최솟값 {long_min:.1f}초")
print(f"-> 그 사이 {short_max:.1f} ~ {long_min:.1f}초 구간에는 실측 데이터가 하나도 없다")
print(f"   기본 임계 {prep.MERGE_GAP_SEC}초는 이 빈 구간 안에 있다")

fig, ax = plt.subplots(figsize=(11, 4))
ax.hist(np.log10(gaps[gaps > 0]), bins=70, color="#0072B2", alpha=0.85)
ax.axvline(np.log10(prep.MERGE_GAP_SEC), color="#D55E00", linestyle="--", linewidth=1.6,
           label=f"병합 임계 {prep.MERGE_GAP_SEC}초")
ticks = [1, 3, 10, 30, 60, 300, 600, 3600]
ax.set_xticks(np.log10(ticks))
ax.set_xticklabels([f"{t}s" if t < 60 else f"{t//60}m" for t in ticks])
ax.set_xlabel("OFF 길이 (로그축)"); ax.set_ylabel("건수")
ax.set_title("OFF 길이 분포 — 두 덩어리 사이가 비어 있다", loc="left")
ax.legend(frameon=False); ax.spines[["top", "right"]].set_visible(False)
fig.tight_layout(); plt.show()''')

m("""### 잘린 첫 사이클 버리기

데이터가 **exchanger 가 이미 켜져 있는 도중부터** 시작할 수 있다. 그 사이클은 언제
시작했는지 알 수 없어 길이를 믿을 수 없다 — 실측에서 5.8시간짜리 가짜 사이클이
만들어진 적이 있다.

그래서 ON 시작은 **데이터 안에서 0→1 전환이 실제로 보인 것만** 인정한다.
첫 샘플이 이미 1 이면 그 사이클은 자동으로 빠진다.""")

c('''first_val = sig["exch"].iloc[0]
print(f"데이터 첫 샘플의 exchanger 값 = {first_val:.0f}")
print("-> 켜진 도중부터 시작. 첫 사이클은 버려진다" if first_val == 1
      else "-> 꺼진 상태에서 시작. 버릴 것 없음")

cycles = prep.merge_cycles(sig["exch"])
print(f"\\nraw ON 구간 {len(segs):,}개  ->  병합 사이클 {len(cycles):,}개 "
      f"({len(segs)/len(cycles):.1f}:1 로 압축)")
display(cycles.head())''')

c('''# 병합 전/후 같은 구간 비교
t0 = pd.Timestamp("2026-09-10 09:00:00")
t1 = t0 + pd.Timedelta(minutes=30)
seg = sig["exch"].loc[t0:t1]
cyc_in = cycles[(cycles["cycle_start"] >= t0) & (cycles["cycle_start"] <= t1)]

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(13, 5), sharex=True)
ax1.step(seg.index, seg.values, where="post", color="#0072B2", linewidth=1.2)
ax1.set_ylim(-0.15, 1.15); ax1.set_yticks([0, 1]); ax1.set_ylabel("병합 전")
ax1.set_title(f"병합 전/후 비교 — 같은 30분 (사이클 {len(cyc_in)}개로 정리)", loc="left")
for _, r in cyc_in.iterrows():
    ax2.fill_between([r["cycle_start"], r["cycle_end"]], 0, 1,
                     color="#D55E00", alpha=0.85, linewidth=0)
ax2.set_ylim(-0.15, 1.15); ax2.set_yticks([0, 1]); ax2.set_ylabel("병합 후")
for a in (ax1, ax2):
    a.spines[["top", "right"]].set_visible(False)
fig.tight_layout(); plt.show()

print("사이클별로 몇 개의 짧은 OFF 를 이어붙였는지(n_bridged):")
print(cyc_in[["cycle_start", "on_duration_sec", "n_bridged"]].to_string(index=False))''')

m("""## 4. cool 동반 여부

사이클 구간 동안 cool 이 한 번이라도 켜졌는지 표시한다. 진단 규칙 ②③ 에서
"exchanger 단독 문제인지 cool 까지 걸린 문제인지" 를 가르는 데 쓴다.""")

c('''cycles = prep.flag_cool(cycles, sig["cool"])
print(f"cool 동반 사이클: {int(cycles['cool_overlap'].sum())}개 / {len(cycles)}개 "
      f"({100*cycles['cool_overlap'].mean():.1f}%)")
display(cycles.head())''')

m("""## 전체를 한 번에

위 1~4 단계는 `preprocess()` 한 줄로 끝난다.
**이미 전처리된 Series 를 갖고 있으면** `preprocess_signals(exch, cool, pv, lset)` 를 쓰면 된다.""")

c('''data = prep.preprocess(TANK, START, END, verbose=True)
print()
print("반환 키:", list(data.keys()))
display(data["cycles"].head(3))''')

m("""## 전처리 결과 요약

| 항목 | 값 |
|---|---|
| 압축비 | raw ON 구간 → 사이클 |
| 버린 글리치 | PV 허용범위 밖 |
| 사이클당 평균 이어붙인 OFF | `n_bridged` 평균 |""")

c('''cyc = data["cycles"]
print(f"사이클 수            {len(cyc):,}개")
print(f"사이클당 평균 n_bridged  {cyc['n_bridged'].mean():.1f}개")
print(f"ON 지속시간 중앙값      {cyc['on_duration_sec'].median():.0f}초")
print(f"ON 지속시간 범위        {cyc['on_duration_sec'].min():.0f} ~ {cyc['on_duration_sec'].max():.0f}초")
print(f"cool 동반 비율         {100*cyc['cool_overlap'].mean():.1f}%")''')

build(os.path.join(PARKKT, "전처리_설명.ipynb"), PRE, "전처리 설명")


# =========================================================================== #
# 2) 최종 정리 노트북
# =========================================================================== #
FIN = []
m = lambda s: FIN.append(("md", s))
c = lambda s: FIN.append(("code", s))

m("""# 탱크 Exchanger / Heat 진단 모델 — 최종 정리

## 구성

| 파일 | 역할 |
|---|---|
| `Tank_Preprocess.py` | 신호 전처리 (스케일 변환, 글리치 제거, 채터링 병합, cool 표시) |
| `Tank_Diagnosis_Model.py` | 진단 규칙 3가지 + `relay_warning` JSON 출력 |
| `전처리_설명.ipynb` | 전처리가 왜/어떻게 돌아가는지 |
| 이 노트북 | 전체 사용법과 실측 검증 결과 |

## 판정 규칙

| 규칙 | 조건 | 보고 대상 |
|---|---|---|
| **① heat 이상** | 전날 PV 최솟값 < `TK_Temp_L_Set` | heater ID |
| **② exchanger/cool 이상** | 사이클이 켜진 뒤 **10분 안에 안 꺼짐** | cool 없으면 exchanger ID / cool 있으면 exchanger + cooler ID |
| **③ 성능 저하** | cool 동반 사이클 **10회 연속** | exchanger ID |

규칙 ② 의 근거: exchanger 는 온도가 SV 밑으로 내려가면 꺼진다. 그래서
**안 꺼졌다 = 온도를 못 잡았다** 와 같은 말이다.

## 출력

```json
{
  "relay_warning": {
    "P00221": {"type": "thermal", "message": "열교환기 냉각 응답 지연 · 설정 온도 복귀 시간 증가 추세"},
    "P00220": {"type": "thermal", "message": "쿨러 냉각 응답 지연 · 열교환기 동시 가동에도 설정 온도 미복귀"}
  }
}
```
""")

m("## 1. 준비")
c('''import os
import sys

import numpy as np
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt

matplotlib.rcParams["font.family"] = "Malgun Gothic"
matplotlib.rcParams["axes.unicode_minus"] = False
pd.set_option("display.width", 170)

PARKKT = os.getcwd() if os.path.basename(os.getcwd()) == "parkkt" else os.path.join(os.getcwd(), "parkkt")
sys.path.insert(0, PARKKT)

import Tank_Preprocess as prep
import Tank_Diagnosis_Model as model

TANK = "P1"
START, END = "2026-09-09 10:00:00", "now()"''')

m("""### 릴레이 ID 표

고장 부위별로 보고할 ID. 모델 로직은 이 표만 보고, 태그 이름/ID 를 직접 들고 있지 않다.""")

c('''display(pd.DataFrame(model.RELAY_IDS))
dups = model.validate_relay_ids()
if dups:
    print("\\n⚠️ 위 경고는 받은 ID 표에 중복이 있다는 뜻이다. 그대로 두고 쓰는 중.")''')

m("""### 메시지 문구

`heater` 문구가 기준이고, 나머지는 같은 구조([장치][동작] [증상] · [지표] [추세])로 맞췄다.""")

c('''for k, v in model.MESSAGES.items():
    print(f"{k:20s} {v}")''')

m("""## 2. 한 줄 사용법""")

c('''result = model.diagnose(TANK, START, END)
print(model.to_json(result))''')

m("""## 3. 자세히 보기

`detail=True` 를 주면 판정 근거가 같이 온다.""")

c('''result = model.diagnose(TANK, START, END, detail=True)
d = result["_detail"]

print(f"[{d['tank']}] 파라미터 {d['params']}")
print(f"  사이클 수               {d['n_cycles']:,}")
print(f"  ① heat 이상일           {d['heat_abnormal_days']}")
print(f"  ② exchanger 단독        {d['exchanger_only']}")
print(f"  ② exchanger+cool        {d['exchanger_with_cool']}")
print(f"  ③ 성능저하 사이클        {d['degrade_cycles']}  (cool 최대 연속 {d['max_cool_run']})")''')

c('''print("=== ① heat 진단 (일별 최저 PV vs L_Set) ===")
display(d["heat_daily"].round(2))''')

c('''print("=== ② 이상으로 잡힌 사이클 ===")
display(d["abnormal_cycles"])''')

m("""## 4. 판정 기준이 적절한지 확인

정상 사이클의 ON 지속시간 분포와 판정 기준(10분)을 겹쳐 본다.""")

c('''data = prep.preprocess(TANK, START, END)
cyc = model.check_cycle_duration(data["cycles"], model.params(TANK)["MAX_ON_SEC"])
ok = cyc[~cyc["abnormal"]]["on_duration_sec"]
thr = model.params(TANK)["MAX_ON_SEC"]

fig, ax = plt.subplots(figsize=(11, 4.5))
ax.hist(ok, bins=60, color="#0072B2", alpha=0.85)
ax.axvline(thr, color="#D55E00", linestyle="--", linewidth=1.6, label=f"판정 기준 {thr}초")
ax.axvline(ok.max(), color="#009E73", linestyle=":", linewidth=1.6,
           label=f"정상 최댓값 {ok.max():.0f}초")
ax.set_xlabel("사이클 ON 지속시간 (초)"); ax.set_ylabel("사이클 수")
ax.set_title(f"[{TANK}] 정상 사이클 ON 지속시간 vs 판정 기준", loc="left")
ax.legend(frameon=False); ax.spines[["top", "right"]].set_visible(False)
fig.tight_layout(); plt.show()

print(f"정상 최댓값 {ok.max():.0f}초 / 기준 {thr}초 → 여유 {thr/ok.max():.1f}배")
print(f"99% 지점 {ok.quantile(0.99):.0f}초")''')

m("""## 5. 여러 탱크 한 번에

`relay_warning` 이 하나로 합쳐져서 나온다.""")

c('''multi = model.diagnose_many(["P1"], START, END)
print(model.to_json(multi))''')

m("""## 6. 실시간 판정

사이클이 끝나기를 기다리지 않는다. 켜진 지 10분이 지나는 순간 바로 이상으로 띄운다.
`cycle_end=None` 이면 "아직 켜져 있는 중" 으로 보고 `now` 기준으로 판정한다.""")

c('''cyc, _ = model.check_consecutive_cool(cyc)

# 정상 사이클
r = cyc[~cyc["abnormal"]].iloc[-1]
print("정상   :", model.judge_cycle(TANK, r["cycle_start"], r["cycle_end"], data["cool"]))

# 이상 사이클 — 끝난 뒤 판정
bad = cyc[cyc["abnormal"]]
if len(bad):
    b = bad.iloc[-1]
    print("이상   :", model.judge_cycle(TANK, b["cycle_start"], b["cycle_end"], data["cool"]))

    # 같은 사이클을 "아직 켜져 있는 중" 으로, 10분 1초 시점에 판정
    t_now = b["cycle_start"] + pd.Timedelta(seconds=model.params(TANK)["MAX_ON_SEC"] + 1)
    live = model.judge_cycle(TANK, b["cycle_start"], None, data["cool"], now=t_now)
    print("실시간 :", live["state"], live["on_sec"], "초")
    print(model.to_json(live))''')

m("""## 7. 다른 탱크로 확장할 때

**태그 이름과 릴레이 ID 는 설정 표에만 있다.** 진단 함수들은 `pandas.Series` 만 받고
태그 이름을 모른다. 그래서 탱크 추가는 표에 한 줄 넣는 걸로 끝난다.

```python
# Tank_Preprocess.py
_EXCH_COOL["P6"] = ("워킹 Tank ... Exchanger SOL", "워킹 Tank ... Cooling SOL")

# Tank_Diagnosis_Model.py
RELAY_IDS["heater"]["P6"] = "P002xx"
RELAY_IDS["cooler"]["P6"] = "P002xx"
RELAY_IDS["exchanger"]["P6"] = "P002xx"
```

**주의**
- 한글 태그는 탱크마다 공백 개수가 다르다(`워킹 Tank 2-1 SOFT  Heater` 는 스페이스 2개).
  InfluxDB 에 찍힌 그대로 복사해야 한다.
- I2 는 cool/exchanger 가 `Tank 2-2 MDI`, heat/feeding 은 `Tank 2 MDI` 로 이름이 다르다.
- `TK_Temp_PV_I*` 는 I1~I3 만 있다.

**임계값도 탱크별로 다를 수 있다.** 탱크마다 냉각 능력과 사이클 리듬이 달라서,
P1 에서 맞춘 10분 기준이 다른 탱크에도 맞는지는 각 탱크 데이터로 확인해야 한다.
확인 후에는 `TANK_OVERRIDES` 에 넣으면 된다.

```python
TANK_OVERRIDES = {"I2": {"MAX_ON_SEC": 900}}
```
""")

c('''# 탱크별로 기준이 맞는지 확인할 때 쓰는 코드
def check_threshold(tank, start, end):
    data = prep.preprocess(tank, start, end)
    on = data["cycles"]["on_duration_sec"]
    thr = model.params(tank)["MAX_ON_SEC"]
    return {"탱크": tank, "사이클": len(on),
            "ON 중앙값": round(on.median()),
            "ON 99%": round(on.quantile(0.99)),
            "ON 최댓값": round(on.max()),
            "현재 기준": thr,
            "여유(99% 대비)": round(thr / on.quantile(0.99), 1)}


pd.DataFrame([check_threshold("P1", START, END)])''')

m("""## 알아둘 점

실측(P1, 2026-09-09~10-02, 1,254 사이클) 기준으로 지금 설정은 이렇게 동작한다.

**① heat 기준은 사실상 안 울린다.** `TK_Temp_L_Set_P1` 이 10.0℃ 인데 실제 일별 최저
PV 는 16.9~17.5℃ 라 여유가 7℃ 다. 완전 고장 전용 알람에 가깝다. 조기 경보를 원하면
L_Set 대신 과거 일별 최저값 분포 기준으로 바꾸는 게 실효성이 있다.

**③ 10회 연속 기준도 안 울린다.** 실측 최대 연속이 3회(1회 110건, 2회 8건, 3회 2건)라
10회는 역대 최대의 3.3배다. 조기 경보용으로는 5회 정도가 현실적이다.

**② 10분 기준은 적절하다.** 정상 사이클 ON 지속시간 99% 지점이 255초라 여유가 충분하다.

**아침 기동 사이클 주의.** 밤새 멈춰 있다 아침에 처음 켜질 때는 온도가 많이 올라가 있어서
사이클이 14~37분까지 길어진다. 이건 고장이 아니라 정상적인 캐치업이다. 전처리된 데이터에
이 구간이 포함되면 매일 오탐이 뜨므로, 직전 OFF 가 1시간 이상인 사이클은 건너뛰는 로직을
추가하는 게 좋다.
""")

build(os.path.join(PARKKT, "Tank_Diagnosis_최종정리.ipynb"), FIN, "최종 정리")
