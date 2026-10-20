import pandas as pd
D = "/Users/jeonjae/Desktop/KTA/DB_Log/alarm_check_1017_1018/"
df = pd.read_csv(D + "raw_1017_1018.csv", index_col=0, parse_dates=True, encoding="utf-8-sig")
b = lambda s: df[s].fillna(0).astype(float) > 0.5
print("Reset_Sw 1 인 행:", int(b("Reset_Sw").sum()), "| 0→1 횟수:", int((b("Reset_Sw") & ~b("Reset_Sw").shift(fill_value=False)).sum()))
dt = df.index.to_series().diff().dt.total_seconds()
print("행 간격(초) 분위수:", dt.quantile([.5, .9, .99]).round(3).to_dict())
for n, k in ((2, "K00031"), (3, "K00032")):
    al = b(f"Air_Supply{n}_Home_CHK_Err")
    inp = b(f"Air_Supply{n}_In_CHK_P") | (b("Conv_Run_Warning") & ~b(f"Air_Supply{n}_Stopper_CY") & ~b(f"Air_Supply{n}_Nozzle_CY"))
    setc = inp & ~b(f"Air_Supply{n}_Home_CHK") & b(k)
    blk = setc & ~al
    # 연속 구간으로 묶기
    grp = (blk != blk.shift()).cumsum()[blk]
    spans = [(g.index[0], g.index[-1]) for _, g in df[blk].groupby(grp)]
    rises = df.index[al & ~al.shift(fill_value=False)]
    print(f"\n== AS{n}: 'SET 참 & 알람 0' 구간 {len(spans)}개, 알람 상승 {len(rises)}개")
    for s, e in spans[:12]:
        nxt = rises[rises >= s]
        gap = (nxt[0] - e).total_seconds() if len(nxt) else None
        why = ("In_CHK_P" if b(f"Air_Supply{n}_In_CHK_P")[s] else "ConvRun&!St&!Nz")
        print(f"   {s} ~ {e}  ({(e-s).total_seconds():.2f}s, via {why})  → 다음 알람 상승까지 {gap}s")
