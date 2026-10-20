import pandas as pd
D = "/Users/jeonjae/Desktop/KTA/DB_Log/alarm_check_1017_1018/"
df = pd.read_csv(D + "reset_1017_1018.csv", index_col=0, parse_dates=True, encoding="utf-8-sig")
ins = ["Reset_Sw", "P00012", "P00175", "M00801", "P00182", "P00185", "P0015C", "P0016C",
       "Reset PBL", "Local Reset PBL", "RESET PB_HMI", "Reset_PL"]
ins = [c for c in ins if c in df.columns]
b = lambda s: df[s].fillna(0).astype(float) > 0.5
print("컬럼:", list(df.columns))
print("\n신호별 1 이 된 횟수(0→1):")
for c in ins:
    s = b(c); print(f"  {c:18} {int((s & ~s.shift(fill_value=False)).sum()):4}  (1 인 행 {int(s.sum())})")
for al in ("Air_Supply2_Home_CHK_Err", "Air_Supply3_Home_CHK_Err", "RB2_Home_Position_CHK_Err"):
    a = b(al); falls = df.index[~a & a.shift(fill_value=False)]
    print(f"\n== {al} 해제 {len(falls)}회 — 해제 ±3초 안의 리셋 신호 변화")
    for t in falls:
        w = df.loc[t - pd.Timedelta(seconds=3): t + pd.Timedelta(seconds=1), ins].fillna(0).astype(int)
        ch = w[(w != w.shift()).any(axis=1)].iloc[1:]
        hits = sorted({c for c in ins if (w[c] == 1).any()})
        print(f"   {t}  1 이었던 신호: {hits or '-'}")
