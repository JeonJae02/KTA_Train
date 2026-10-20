"""Alarm.il Line 71/81/15 (Air_Supply2/3_Home_CHK_Err, RB2_Home_Position_CHK_Err)
10/17~10/18 데이터. 세 알람의 SET/RST 식 심볼 + 중간 비트 1단계만 뽑는다."""
import os, sys
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)
os.chdir(ROOT)                      # .env 상대경로
from Log_Extractor import LogExtractor

AS = lambda n: [f"Air_Supply{n}_Home_CHK_Err", f"Air_Supply{n}_In_CHK_P", f"Air_Supply{n}_In_CHK_Dly",
                f"Air_Supply{n}_In_CHK_PX", f"Air_Supply{n}_Home_CHK", f"Air_Supply{n}_ORG_PX",
                f"Air_Supply{n}_Stopper_REV_CS", f"Air_Supply{n}_Nozzle1_REV_CS",
                f"Air_Supply{n}_Nozzle2_REV_CS", f"Air_Supply{n}_Stopper_CY", f"Air_Supply{n}_Nozzle_CY"]
TAGS = (AS(2) + ["K00031", "Mould_CY_FWD_Use"] + AS(3) + ["K00032", "Mould_CY_REV_Use"]
        + ["Conv_Run_Warning", "Conv_Brake_Time", "Reset_Sw"]
        + ["RB2_Home_Position_CHK_Err", "Auto_Start_Warning", "Robot2_Use", "RB2_HOME",
           "RB2_PGM_OUT_CHK", "RB2_AUTO_READY", "Conv_Ps_Robot_Start_2", "K0000B",
           "P00113", "P00114", "P0011B", "R00040", "R00045", "R00048"])

if __name__ == "__main__":
    ex = LogExtractor(env_path=".env")
    df = ex.get_data("2026-10-17 00:00:00", "2026-10-19 00:00:00", TAGS)
    out = os.path.join(HERE, "raw_1017_1018.csv")
    df.to_csv(out, encoding="utf-8-sig")
    print("saved", out, df.shape)
