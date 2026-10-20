"""리셋 입력 원신호. Reset_Sw = P00012|P00175|M00801|P00182|P00185|P0015C|P0016C"""
import os, sys
HERE = os.path.dirname(os.path.abspath(__file__)); ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT); os.chdir(ROOT)
from Log_Extractor import LogExtractor
TAGS = ["Reset_Sw", "P00012", "P00175", "M00801", "P00182", "P00185", "P0015C", "P0016C",
        "Reset PBL", "Local Reset PBL", "RESET PB_HMI", "Reset_PL",
        "Air_Supply2_Home_CHK_Err", "Air_Supply3_Home_CHK_Err", "RB2_Home_Position_CHK_Err"]
ex = LogExtractor(env_path=".env")
df = ex.get_data("2026-10-17 00:00:00", "2026-10-19 00:00:00", TAGS)
df.to_csv(os.path.join(HERE, "reset_1017_1018.csv"), encoding="utf-8-sig")
print(df.shape)
