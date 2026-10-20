import pandas as pd
D = "/Users/jeonjae/Desktop/KTA/DB_Log/alarm_check_1017_1018/"
df = pd.read_csv(D + "raw_1017_1018.csv", index_col=0, parse_dates=True, encoding="utf-8-sig")
b = lambda s: df[s].fillna(0).astype(float) > 0.5
crw = b("Conv_Run_Warning")
up = crw & ~crw.shift(fill_value=False); dn = ~crw & crw.shift(fill_value=False)
dur = []
ups = df.index[up]; dns = df.index[dn]
for u in ups:
    d = dns[dns > u]
    if len(d): dur.append((d[0] - u).total_seconds())
print("CRW 1 유지시간(초) 분위수:", pd.Series(dur).quantile([.1, .5, .9]).round(2).to_dict(), "n=", len(dur))
for n, k in ((2, "K00031"), (3, "K00032")):
    al = b(f"Air_Supply{n}_Home_CHK_Err"); rise = al & ~al.shift(fill_value=False)
    St, Nz, Hm, K = (b(f"Air_Supply{n}_Stopper_CY"), b(f"Air_Supply{n}_Nozzle_CY"),
                     b(f"Air_Supply{n}_Home_CHK"), b(k))
    cond = ~St & ~Nz & ~Hm & K
    # 하강 에지 행: 그 행 값(변화 후) 과 직전 행 값(변화 전) 둘 다
    prev = cond.shift(fill_value=False)
    tbl = pd.crosstab(pd.Series(cond[dn].values, name="하강행 cond(후)"),
                      pd.Series(rise[dn].values, name="그 행에서 알람↑"))
    print(f"\n== AS{n}  CRW 하강 {int(dn.sum())}회 / 알람↑ {int(rise.sum())}회")
    print(tbl)
    print("  상승행 cond(후) True 횟수:", int(cond[up].sum()), "/", int(up.sum()))
    print("  알람↑ 중 CRW 하강행과 같은 행:", int((rise & dn).sum()))
    # 하강 직전 행(CRW=1 마지막) 에서 St/Hm 상태
    last1 = dn.shift(-1, fill_value=False)
    sub = df.index[last1 & K]
    print("  하강 직전행(K=1): St=1 비율", round(St[sub].mean(), 3), "Home=1 비율", round(Hm[sub].mean(), 3))
