import os
import time
import pandas as pd
from influxdb_client import InfluxDBClient
from dotenv import load_dotenv

class LogExtractor:
    def __init__(self, env_path=".env"):
        """
        팀원들과 깃허브로 협업하기 위해, 민감한 정보는 .env에서 불러옵니다.
        :param env_path: .env 파일이 있는 상대 경로 (구조에 맞게 수정하세요)
        """
        # .env 파일 로드
        load_dotenv(dotenv_path=env_path)

        db_port = os.getenv("INFLUXDB_PORT", "8086")
        self.db_url = os.getenv("INFLUXDB_URL", f"http://localhost:{db_port}")
        
        self.token = os.getenv("INFLUXDB_ADMIN_TOKEN")
        self.org = os.getenv("INFLUXDB_ORG")
        self.bucket = os.getenv("INFLUXDB_BUCKET")

        if not self.token:
            print("❌ [.env 오류] 토큰을 찾을 수 없습니다. .env 파일 경로와 내용을 확인해주세요.")

        self.client = InfluxDBClient(url=self.db_url, token=self.token, org=self.org, timeout=3000000)
        self.query_api = self.client.query_api()
        
        print("🔌 [Extractor] InfluxDB 분석용 추출기 연결 완료!")

    def get_data(self, start_time, end_time, target_tags=None):
        print(f"🔍 데이터 추출 시작... ({start_time} ~ {end_time})")
        
        # 1. target_tags가 있을 경우, Flux 쿼리용 배열 문자열로 변환
        # 예: '["g_s_SV_P1", "Ana_Out_P1", ...]'
        if target_tags:
            # 큰따옴표로 묶고 쉼표로 연결
            flux_array_str = "[" + ", ".join([f'"{tag}"' for tag in target_tags]) + "]"
            
            # [핵심] contains 함수를 사용해 pivot 이전에 데이터를 확 줄여버립니다!
            # (or 연산자 수십 개를 쓰는 것보다 수십 배 빠릅니다)
            filter_query = f'|> filter(fn: (r) => contains(value: r.tag_name, set: {flux_array_str}))'
        else:
            filter_query = "" # 태그 지정 안 하면 다 가져옴

        # 2. 최적화된 Flux 쿼리 생성
        flux_query = f"""
        from(bucket: "{self.bucket}")
            |> range(start: {start_time}, stop: {end_time})
            |> filter(fn: (r) => r._measurement == "plc_line2")
            {filter_query}
            |> pivot(rowKey:["_time"], columnKey: ["tag_name"], valueColumn: "_value")
            |> drop(columns: ["_start", "_stop", "_measurement"])
        """

        # DB에 쿼리 요청
        df = self.query_api.query_data_frame(query=flux_query)
        
        if isinstance(df, list):
            if len(df) == 0:
                print("⚠️ 해당 조건의 데이터가 없습니다.")
                return pd.DataFrame()
            df = pd.concat(df)

        if not df.empty and '_time' in df.columns:
            # 시간 처리 및 정렬
            df['_time'] = pd.to_datetime(df['_time']).dt.tz_convert('Asia/Seoul')
            df['_time'] = df['_time'].dt.tz_localize(None) 
            df.set_index('_time', inplace=True)
            df.index.name = 'Time'
            df = df.sort_index()
            
            # (이제 파이썬 메모리에서 굳이 또 필터링할 필요가 없습니다. 이미 DB에서 걸러서 왔으니까요!)
            
            # 3. 비어있는 값 복원
            df = df.ffill()

        print(f"✅ 추출 완료! 총 {len(df)}행, {len(df.columns)}개 컬럼 확보.")
        return df

    def save_to_csv(self, df, save_dir="./extracted_csv"):
        """
        [핵심] 뽑아낸 데이터프레임을 날짜시간이 박힌 CSV 파일로 깔끔하게 저장합니다.
        """
        if df.empty:
            print("⚠️ 저장할 데이터가 없습니다.")
            return

        if not os.path.exists(save_dir):
            os.makedirs(save_dir)

        # 파일명 자동 생성 (예: 2026-04-02_143000_analysis.csv)
        current_time = time.strftime("%Y-%m-%d_%H%M%S")
        file_name = f"{current_time}_analysis.csv"
        file_path = os.path.join(save_dir, file_name)

        # utf-8-sig 옵션: 한글 태그명이 엑셀에서 깨지는 것을 방지
        df.to_csv(file_path, encoding='utf-8-sig')
        print(f"💾 [저장 완료] 분석용 CSV 파일이 생성되었습니다: {file_path}")

# ==========================================
# 🧪 실행부
# ==========================================
if __name__ == "__main__":
    # 1. 추출기 가동 (.env 경로만 잘 맞춰주십쇼)
    extractor = LogExtractor(env_path=".env")

    # 2. 원하는 데이터 검색
    my_df = extractor.get_data(
        start_time="-7h", 
        end_time="now()", 
        target_tags=["Actual_P1_g_s1_HD1", "Actual_P1_g_s2_HD1", "Actual_P1_g_s3_HD1", "Actual_P1_g_s4_HD1", "Actual_P1_g_s5_HD1", "Actual_P1_g_s6_HD1", "Actual_P1_g_s1_HD2", "Actual_P1_g_s2_HD2", "Actual_P1_g_s3_HD2", "Actual_P1_g_s4_HD2", "Actual_P1_g_s5_HD2", "Actual_P1_g_s6_HD2", "Actual_P1_g_s1_HD3", "Actual_P1_g_s2_HD3", "Actual_P1_g_s3_HD3", "Actual_P1_g_s4_HD3", "Actual_P1_g_s5_HD3", "Actual_P1_g_s6_HD3", "Actual_P1_g_s1_HD4", "Actual_P1_g_s2_HD4", "Actual_P1_g_s3_HD4", "Actual_P1_g_s4_HD4", "Actual_P1_g_s5_HD4", "Actual_P1_g_s6_HD4", "MTK_FEED_ERR_T_P1", "Para_DM_Press_H_Set_P1", "Para_DM_Press_L_Set_P1", "Pump_Cir_SPD_Set_P1", "Para_DM_H_ExSet_P1", "TK_Temp_PV_P1", "TK_Temp_H_Set_P1", "TK_Temp_SV_P1", "TK_Temp_L_Set_P1", "TK_Level_PV_P1", "TK_Level_Max_Set_P1", "TK_Level_Stop_Set_P1", "TK_Level_Start_Set_P1", "TK_Level_Min_Set_P1", "TK_FEED_ERR_T_P1", "STK_Temp_PV_P1", "STK_Temp_H_Set_P1", "STK_Temp_SV_P1", "STK_Temp_L_Set_P1", "STK_Level_PV_P1", "STK_Level_Max_Set_P1", "STK_Level_Stop_Set_P1", "STK_Level_Start_Set_P1", "STK_Level_Min_Set_P1", "STK_FEED_ERR_T_P1", "ANA_IN___PT_P1", "Ana_Max___PT_P1", "Gain___PT_P1", "OffSet___PT_P1", "Scale_Max___PT_P1", "Scale_Out___PT_P1", "FT_P1_Imp/L HMI_Real", "FT_P1_비중 HMI_Real", "FT_P1_Offset HMI", "FT_P1_Scale_Out", "Ana_In___TT_P1", "Ana_Max___TT_P1", "Gain___TT_P1", "OffSet___TT_P1", "Scale_Max___TT_P1", "Scale_Out___TT_P1", "Ana_In___TT_STK_P1", "Ana_Max___TT_STK_P1", "Gain___TT_STK_P1", "OffSet___TT_STK_P1", "Scale_Max___TT_STK_P1", "Scale_Out___TT_STK_P1", "Ana_In___LT_P1", "Ana_Max___LT_P1", "Gain___LT_P1", "OffSet___LT_P1", "Scale_Max___LT_P1", "Scale_Out___LT_P1", "g_s_SV_P1", "Pump_rpm_P1", "Pump_Capa_P1", "Ana_Out_Max_P1", "Gain_P1", "Pump_Hz_Max_P1", "Max_Capa_P1", "Ana_Out_P1", "Hz_Out_P1", "CAL_Base_Qty_P1", "CAL_Range_Qty_H_P1", "A_CAL_Result_H_QTY_HMI_P1", "A_CAL_Result_L_QTY_HMI_P1", "Ana_In___LT_STK_P1", "Ana_Max___LT_STK_P1", "Gain___LT_STK_P1", "OffSet___LT_STK_P1", "Scale_Max___LT_STK_P1", "Scale_Out___LT_STK_P1", "이전 CNT_P1", "현재 CNT_P1", "초당 CNT_P1", "입력 주파수_P1", "TK_JK_Temp_PV_P1", "TK_JK_Temp_H_Set_P1", "STK_JK_Temp_PV_P1", "STK_JK_Temp_H_Set_P1", "TK_JK_Temp_H_Set_Hys_P1", "STK_JK_Temp_H_Set_Hys_P1", "Ana_In___PT_HD_P1", "Ana_Max___PT_HD_P1", "Gain___PT_HD_P1", "OffSet___PT_HD_P1", "Scale_Max___PT_HD_P1", "Scale_Out___PT_HD_P1", "Ana_In___TT_JK_P1", "Ana_Max___TT_JK_P1", "Gain___TT_JK_P1", "OffSet___TT_JK_P1", "Scale_Max___TT_JK_P1", "Scale_Out___TT_JK_P1", "Ana_In___TT_JK_STK_P1", "Ana_Max___TT_JK_STK_P1", "Gain___TT_JK_STK_P1", "OffSet___TT_JK_STK_P1", "Scale_Max___TT_JK_STK_P1", "Scale_Out___TT_JK_STK_P1", "Ana_In___TT_EX_P1", "Ana_Max___TT_EX_P1", "Gain___TT_EX_P1", "OffSet___TT_EX_P1", "Scale_Max___TT_EX_P1", "Scale_Out___TT_EX_P1", "HD1_P1_인젝션_ON_T", "HD1_P1_인젝션_OFF_T", "HD2_P1_인젝션_ON_T", "HD2_P1_인젝션_OFF_T", "HD3_P1_인젝션_ON_T", "HD3_P1_인젝션_OFF_T", "HD4_P1_인젝션_ON_T", "HD4_P1_인젝션_OFF_T", "HD1_P1_인젝션_ON_T_FLT_SV", "HD1_P1_인젝션_OFF_T_FLT_SV", "HD2_P1_인젝션_ON_T_FLT_SV", "HD2_P1_인젝션_OFF_T_FLT_SV", "HD3_P1_인젝션_ON_T_FLT_SV", "HD3_P1_인젝션_OFF_T_FLT_SV", "HD4_P1_인젝션_ON_T_FLT_SV", "HD4_P1_인젝션_OFF_T_FLT_SV", "Data기록 CNT_P1"]
    )

    # 3. 콘솔에서 살짝 확인
    print(my_df.head())

    # 4. 분석용 CSV로 내려받기 (딱! 저장됩니다)
    extractor.save_to_csv(my_df)