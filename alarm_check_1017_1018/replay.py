"""실제 함수(branch.causal_tick, RungEvaluatorV2) 로 에피소드 재현. 시스템과 같은 버퍼 길이."""
import math, sys, pandas as pd
BK = "/Users/jeonjae/Desktop/KTA/HDI_Relay/backend"
SP = "/private/tmp/claude-501/-Users-jeonjae-Desktop-KTA-HDI-Relay/b2451888-49fe-4aab-9adc-088eb7174247/scratchpad/oldmod"
sys.path.insert(0, BK); sys.path.insert(0, SP)
import os; os.chdir(BK)
from utils.Logic_Engine import LogicEngineV2
from utils.Rung_Evaluator import RungEvaluatorV2 as New, eval_expr
from Rung_Evaluator_old import RungEvaluatorV2 as Old
from alarm_index import branch
D = "/Users/jeonjae/Desktop/KTA/DB_Log/alarm_check_1017_1018/"
df = pd.read_csv(D + "raw_1017_1018.csv", index_col=0, parse_dates=True, encoding="utf-8-sig")
E = LogicEngineV2.from_dir("IL_file")
TICK = 0.5
for al, preset in (("Air_Supply2_Home_CHK_Err", 0.5), ("Air_Supply3_Home_CHK_Err", 0.5), ("RB2_Home_Position_CHK_Err", 0.0)):
    a = df[al].fillna(0) > 0.5
    rises = df.index[a & ~a.shift(fill_value=False)]
    pre = max(8, math.ceil((preset + 10.0) / TICK) + 2)
    print(f"\n===== {al}  pre={pre} ticks")
    for t in rises:
        grid = pd.date_range(t - pd.Timedelta(seconds=pre * TICK), t + pd.Timedelta(seconds=6 * TICK), freq="500ms")
        snap = df.reindex(df.index.union(grid)).ffill().loc[grid]
        buf = [{k: (float(v) if isinstance(v, (int, float)) else v) for k, v in r.items() if pd.notna(v)} for _, r in snap.iterrows()]
        bnd = pre
        tick, side = branch.causal_tick(E, al, buf, eval_expr, New(E).resolver, bnd)
        ov = [r for r in Old(E, tick_sec=TICK).evaluate_target_rungs_timeline(al, buf) if "latch" in r["action_type"]][0]
        nv = [r for r in New(E, tick_sec=TICK).evaluate_target_rungs_timeline(al, buf) if "latch" in r["action_type"]][0]
        off = None if tick is None else (tick - bnd) * TICK
        print(f"  {t}  causal_tick={off if off is None else f'{off:+.1f}s'} ({side})  F-재현 old={ov['verdict']:9} new={nv['verdict']}")

# --- CUT-1 실제 결과 (branch.resolve) : 에피소드별 고른 가지 ---
from alarm_index.builder import make_builder
from alarm_index.catalog import AlarmCatalog
from alarm_index.slicer import AlarmSlicer
b_ = make_builder("IL_file", AlarmCatalog.from_config())
print("\n===== branch.resolve (CUT-1)")
from collections import Counter
for al, preset in (("Air_Supply2_Home_CHK_Err", 0.5), ("Air_Supply3_Home_CHK_Err", 0.5), ("RB2_Home_Position_CHK_Err", 0.0)):
    a = df[al].fillna(0) > 0.5
    rises = df.index[a & ~a.shift(fill_value=False)]
    pre = max(8, math.ceil((preset + 10.0) / TICK) + 2)
    base = b_.slicer.slice(al)
    out = Counter()
    for t in rises:
        grid = pd.date_range(t - pd.Timedelta(seconds=pre * TICK), t + pd.Timedelta(seconds=6 * TICK), freq="500ms")
        snap = df.reindex(df.index.union(grid)).ffill().loc[grid]
        buf = [{k: (float(v) if isinstance(v, (int, float)) else v) for k, v in r.items() if pd.notna(v)} for _, r in snap.iterrows()]
        sl = branch.resolve(b_.slicer, E, al, buf, eval_expr, New(E).resolver, boundary=pre)
        if sl is None:
            out["None(인덱스 그대로)"] += 1; continue
        dropped = sorted(set(base["leaves"]) - set(sl["leaves"]))
        out[(sl["causal_side"], tuple(dropped))] += 1
    print(f"## {al}  leaves(index)={len(base['leaves'])}")
    for k, n in out.items(): print(f"   {n:2}x  {k}")
