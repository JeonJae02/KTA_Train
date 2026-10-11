"""build_cae_1009.py 에 '예전 임계값으로 진단 → §8 적용 후' 를 넣는 일회성 패치."""
import os

f = os.path.join(os.path.dirname(os.path.abspath(__file__)), "build_cae_1009.py")
s = open(f, encoding="utf-8").read()
Q = "'''"


def rep(old, new):
    global s
    assert s.count(old) == 1, old[:70]
    s = s.replace(old, new)


rep("""**볼 것**: ① 그날 펌프들이 어떤 상태였나 ② 펌프별 등급·알림 ③ 피처별로 평소와 얼마나 달랐나 ④ 10/11 점검을 모델이 어떻게 봤나 ⑤ 화면에 어떻게 표시할까""",
    """**볼 것**: ① 그날 펌프들이 어떤 상태였나 ② 펌프별 등급·알림 ③ 피처별로 평소와 얼마나 달랐나 ④ 10/11 점검을 모델이 어떻게 봤나 ⑤ 화면에 어떻게 표시할까

**임계값** — §2~§5 는 처음 돌렸을 때의 **예전 임계값**(calib 건강일 분위)으로 본다. 여기서 "정상 펌프도 알림이 많다" 를 찾았고,
그 뒤 임계값을 **날짜 단위**로 다시 잡고(`calibrate.py`, 신경망은 그대로) 같은 펌프·같은 이유 알림을 **운영일에 한 번**으로 바꿨다.
바뀐 결과는 **§8**.""")
rep("""meta = sc.meta
""", """NEWMETA = sc.meta                                                         # 지금 임계값 (날짜 단위, calibrate.py)
meta = {**NEWMETA, "thresholds": NEWMETA.get("thresholds_prev", NEWMETA["thresholds"])}   # §2~§5 는 예전 임계값
sc_old = cae.CycleScorer(sc.model, meta)
""")
rep("""    parts.append(sc.score(cy.assign(pump=p, material=bs.material)))""",
    """    parts.append(sc_old.score(cy.assign(pump=p, material=bs.material)))""")
rep("""SC = pd.read_pickle(os.path.join(ROOT_DIR, "cache", "pump_dataset", "scores.pkl.xz"), compression="xz")
""", """SC = pd.read_pickle(os.path.join(ROOT_DIR, "cache", "pump_dataset", "scores.pkl.xz"), compression="xz")
SC = cae.add_levels(SC.drop(columns=["alarm_ae", "alarm_z", "alarm", "alert", "level"]), meta["thresholds"])   # 예전 임계값으로
""")
rep("""- **제안: 창 알림 임계 × 1.25** — `train.py` 에 `WINDOW_ALERT_MARGIN = 1.25` 로 넣고 다시 돌린다 (아직 반영 안 함)
- 근본적으로는 **데이터가 쌓이면 임계값을 다시 잡는 것** — 4주로 정한 임계값은 날마다 조금씩 달라지는 정상 변동을 다 담지 못한다""",
    """- 배수를 곱하는 건 임시방편이라, 대신 **임계값을 날짜 단위로 다시 잡았다** — 정상 날 하루하루의 분위값 중앙값 + 4·MAD
  (`calibrate.py`). 날마다 흔들리는 폭이 임계값에 들어가고, 배수 없이 비슷한 효과를 낸다 → §8""")

rep("""nb = {"cells": [],""", '''md(r''' + Q + '''
---
## §8. 적용 후 — 날짜 단위 임계값 + 운영일 알림 기억

바꾼 것 두 가지 (신경망 `model.pt` 는 그대로):

1. **임계값** (`calibrate.py`): 묶음마다 정상 날(학습 기간 건강일) **하루하루의** 분위값을 구해 `중앙값 + 4 × MAD`.
   예전엔 calib 건강일(묶음당 1~3일) 분위값 하나라, 학습 뒤 조금 달라진 날에 경계선 알림이 많았다
2. **알림 기억** (`WindowTracker`): 같은 펌프·같은 이유는 **운영일(06시 경계)에 한 번**. 재가동 리셋은 창만 비운다

같은 사이클을 지금 임계값으로 다시 채점한다.
''' + Q + ''')
code(r''' + Q + '''
ALLn = sc.score(ALL.drop(columns=[c for c in ALL.columns if c.startswith("err_")] +
                         ["score", "zmax", "alarm_ae", "alarm_z", "alarm", "alert", "level", "top_feature", "top_dir", "in_scope"]))
ALLn["day"] = ALLn.t_start.dt.strftime("%m-%d")
OPSn = cae.window_levels(ALLn[~ALLn.flag_any & ALLn.in_scope].copy(), NEWMETA)
KNOWN = ["P2", "I2", "I6", "I7"]
rows = []
for name, o in [("예전 임계값", OPS), ("지금 (날짜 단위 + 운영일 기억)", OPSn)]:
    o = o[o.day.isin(["10-08", "10-09", "10-10"])]
    for grp, m in [("정상 10대", ~o.pump.isin(KNOWN)), ("지속 상태 4대 (P2·I2·I6·I7)", o.pump.isin(KNOWN))]:
        g = o[m]
        rows.append({"임계값": name, "펌프": grp, "사이클 주의%": round((g.level != "정상").mean() * 100, 2),
                     "사이클 알림%": round((g.level == "알림").mean() * 100, 2),
                     "사람에게 알림/펌프·일": round(g.notify.sum() / g.groupby(["pump", "day"]).ngroups, 2)})
pd.DataFrame(rows).set_index(["임계값", "펌프"])
''' + Q + ''')
code(r''' + Q + '''
DTn = day_table(OPSn)
print("지금 임계값으로 사람에게 간 알림 — 10/8~10/11")
DTn[DTn["알림 횟수"] > 0].sort_values(["날", "펌프"])[["날", "펌프", "사이클", "주의%", "알림%", "알림 횟수", "알림 이유"]].reset_index(drop=True)
''' + Q + ''')
code(r''' + Q + '''
# 알려진 이벤트 — calibrate.py 가 저장한 평가 (학습 기간 하루 빼고 검증 포함)
pd.Series(NEWMETA["threshold_method"]["eval"], name="지금 임계값").to_frame()
''' + Q + ''')
md(r''' + Q + '''
**읽기** — 아래 표를 보고 쓴다 (`__fill__`)
''' + Q + ''')

nb = {"cells": [],''')
open(f, "w", encoding="utf-8").write(s)
print("patched")
