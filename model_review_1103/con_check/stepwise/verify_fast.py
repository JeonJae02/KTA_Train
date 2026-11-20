"""FastCore.evaluate 가 원래 CoreRealtime.evaluate 와 같은 결과를 내는지 — 같은 입력을 두 객체에 흘려 판정마다 비교."""
import sys, json
import pandas as pd
sys.path.insert(0, "/Users/jeonjae/Desktop/KTA/DB_Log/model_review_1103/con_check/stepwise")
import harness as H
cr = H.cr
hist = json.load(open(H.ROOT + "/v2_2_1/backend/data/conveyor_core_history.json"))
day = pd.Timestamp(sys.argv[1]); hours = int(sys.argv[2])
a, b = cr.CoreRealtime(history_path=None), H.FastCore(history_path=None)
for c in (a, b):
    c.history = [{**r, "day": pd.Timestamp(r["day"])} for r in hist if r["day"] < str(day.date())]
tags = H.db.model_tags("conveyor", "v2.2")
n = same = 0; last = None; diffs = []
for t, row in H.rows_of(day, tags):
    if t >= day + pd.Timedelta(hours=6 + hours):
        break
    a.update(t, row); b.update(t, row)
    if last is None or (t - last).total_seconds() >= 60:
        last = t
        ra, rb = a.evaluate(t), b.evaluate(t)
        n += 1
        ja, jb = json.dumps(ra, sort_keys=True, default=str), json.dumps(rb, sort_keys=True, default=str)
        if ja == jb:
            same += 1
        elif len(diffs) < 3:
            diffs.append((t, ja[:300], jb[:300]))
print(day.date(), "판정", n, "같음", same)
for d in diffs: print(d)
