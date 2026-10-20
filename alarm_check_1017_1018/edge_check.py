import pandas as pd
D = "/Users/jeonjae/Desktop/KTA/DB_Log/alarm_check_1017_1018/"
df = pd.read_csv(D + "raw_1017_1018.csv", index_col=0, parse_dates=True, encoding="utf-8-sig")
b = lambda s: df[s].fillna(0).astype(float) > 0.5
crw = b("Conv_Run_Warning"); crw_up = df.index[crw & ~crw.shift(fill_value=False)]
print("Conv_Run_Warning 상승 에지:", len(crw_up))
for n, k in ((2, "K00031"), (3, "K00032")):
    al = b(f"Air_Supply{n}_Home_CHK_Err")
    rises = df.index[al & ~al.shift(fill_value=False)]
    # 에지 순간(그 행)에 나머지 조건
    St, Nz, Hm, K = (b(f"Air_Supply{n}_Stopper_CY"), b(f"Air_Supply{n}_Nozzle_CY"),
                     b(f"Air_Supply{n}_Home_CHK"), b(k))
    edge_set = [t for t in crw_up if not St[t] and not Nz[t] and not Hm[t] and K[t]]
    lvl = (crw & ~St & ~Nz & ~Hm & K)
    print(f"\n== AS{n}: 알람 상승 {len(rises)} | 에지 기준 SET 성립 {len(edge_set)} | 레벨 기준 SET 참 행 {int(lvl.sum())}")
    for t in rises:
        prev_up = crw_up[crw_up <= t]
        pu = prev_up[-1] if len(prev_up) else None
        lag = (t - pu).total_seconds() if pu is not None else None
        st = (f"St={int(St[pu])} Nz={int(Nz[pu])} Home={int(Hm[pu])} K={int(K[pu])}" if pu is not None else "")
        near = [e for e in edge_set if abs((e - t).total_seconds()) < 5]
        print(f"   알람↑ {t}  직전 CRW↑ {pu} (lag {lag:.2f}s)  그 에지에서 {st}  | ±5s 내 에지-SET {len(near)}")
