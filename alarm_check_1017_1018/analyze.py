import sys, pandas as pd
D = "/Users/jeonjae/Desktop/KTA/DB_Log/alarm_check_1017_1018/"
df = pd.read_csv(D + "raw_1017_1018.csv", index_col=0, parse_dates=True, encoding="utf-8-sig")
print(df.shape, "extra cols:", [c for c in df.columns if not c[0].isalnum()][:5])
b = lambda s: df[s].fillna(0).astype(float) > 0.5

def as_cond(n):
    k = "K00031" if n == 2 else "K00032"
    inp = b(f"Air_Supply{n}_In_CHK_P") | (b("Conv_Run_Warning") & ~b(f"Air_Supply{n}_Stopper_CY") & ~b(f"Air_Supply{n}_Nozzle_CY"))
    return inp & ~b(f"Air_Supply{n}_Home_CHK") & b(k), inp
rb2_set = (b("Auto_Start_Warning") & b("Robot2_Use") & (~b("RB2_HOME") | ~b("RB2_PGM_OUT_CHK"))
           & b("RB2_AUTO_READY") & b("Conv_Ps_Robot_Start_2"))
rb2_rst = b("Reset_Sw") | b("Conv_Ps_Robot_Start_2")

cases = {"71 Air_Supply2_Home_CHK_Err": ("Air_Supply2_Home_CHK_Err", *as_cond(2), b("Reset_Sw"), "RST-last"),
         "81 Air_Supply3_Home_CHK_Err": ("Air_Supply3_Home_CHK_Err", *as_cond(3), b("Reset_Sw"), "RST-last"),
         "15 RB2_Home_Position_CHK_Err": ("RB2_Home_Position_CHK_Err", rb2_set, None, rb2_rst, "SET-last")}
for name, (al, setc, inp, rstc, order) in cases.items():
    a = b(al)
    rise = df.index[(a & ~a.shift(fill_value=False))]
    fall = df.index[(~a & a.shift(fill_value=False))]
    print(f"\n===== {name}  ({order})  rises={len(rise)} falls={len(fall)}  on-time={a.mean()*100:.2f}%")
    for t in rise[:40]:
        i = df.index.get_loc(t)
        if isinstance(i, slice): i = i.start
        w = df.iloc[max(0, i - 3): i + 1]
        set_now = bool(setc.iloc[i]); rst_now = bool(rstc.iloc[i])
        set_prev = bool(setc.iloc[max(0, i-1)])
        nf = fall[fall > t]; dur = (nf[0] - t).total_seconds() if len(nf) else None
        # 직전 '알람 0 인데 SET 참' 구간이 있었나 (= SET 이 RST 에 막혔던 시점)
        lo = max(0, i - 200)
        blocked = df.index[lo:i][(setc.iloc[lo:i] & ~a.iloc[lo:i]).values]
        extra = ""
        if name.startswith("15"):
            extra = (f" Ps_Start={int(b('Conv_Ps_Robot_Start_2').iloc[i])} HOME={int(b('RB2_HOME').iloc[i])}"
                     f" PGM_OUT={int(b('RB2_PGM_OUT_CHK').iloc[i])} READY={int(b('RB2_AUTO_READY').iloc[i])}")
        else:
            n = name[0:2] == "71" and 2 or 3
            extra = (f" In_CHK_P={int(b(f'Air_Supply{n}_In_CHK_P').iloc[i])} Home_CHK={int(b(f'Air_Supply{n}_Home_CHK').iloc[i])}"
                     f" ORG={int(b(f'Air_Supply{n}_ORG_PX').iloc[i])} StopREV={int(b(f'Air_Supply{n}_Stopper_REV_CS').iloc[i])}"
                     f" N1REV={int(b(f'Air_Supply{n}_Nozzle1_REV_CS').iloc[i])} N2REV={int(b(f'Air_Supply{n}_Nozzle2_REV_CS').iloc[i])}")
        print(f"  {t}  dur={dur}s  SET@rise={int(set_now)} prev={int(set_prev)} RST@rise={int(rst_now)}"
              f"  blocked_before={len(blocked)}{extra}")
