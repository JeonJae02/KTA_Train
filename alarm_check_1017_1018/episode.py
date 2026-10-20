import sys, pandas as pd
D = "/Users/jeonjae/Desktop/KTA/DB_Log/alarm_check_1017_1018/"
raw = pd.read_csv(D + "raw_1017_1018.csv", index_col=0, parse_dates=True, encoding="utf-8-sig")
t0 = pd.Timestamp(sys.argv[1]); pre = float(sys.argv[2]); post = float(sys.argv[3]); cols = sys.argv[4].split(",")
w = raw.loc[t0 - pd.Timedelta(seconds=pre): t0 + pd.Timedelta(seconds=post), cols].fillna(-1).astype(int)
w = w[(w != w.shift()).any(axis=1)]          # 값이 바뀐 행만
w.columns = [c.replace("Air_Supply", "AS").replace("_CHK", "").replace("Nozzle", "Nz").replace("Stopper", "St") for c in cols]
pd.set_option("display.width", 250); pd.set_option("display.max_columns", 30)
w.index = [f"{(t - t0).total_seconds():+8.3f}" for t in w.index]
print(w.to_string())
