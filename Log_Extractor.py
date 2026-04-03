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

        self.client = InfluxDBClient(url=self.db_url, token=self.token, org=self.org, timeout=300000)
        self.query_api = self.client.query_api()
        
        print("🔌 [Extractor] InfluxDB 분석용 추출기 연결 완료!")

    def get_data(self, start_time, end_time, target_tags=None):
        print(f"🔍 데이터 추출 시작... ({start_time} ~ {end_time})")
        
        # 1. 태그 필터를 DB에 맡기지 않습니다. (리스트가 너무 길면 DB가 뻗음)
        # 쿼리에서는 태그 필터를 제거하여 속도를 확보합니다.
        flux_query = f"""
        from(bucket: "{self.bucket}")
            |> range(start: {start_time}, stop: {end_time})
            |> filter(fn: (r) => r._measurement == "plc_line2")
            |> pivot(rowKey:["_time"], columnKey: ["tag_name"], valueColumn: "_value")
            |> drop(columns: ["_start", "_stop", "_measurement"])
        """

        # DB에서 통째로 긁어오기 (이게 훨씬 빠릅니다)
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
            
            # 2. [핵심] 파이썬 메모리에서 태그 필터링을 수행합니다.
            if target_tags:
                # 존재하는 컬럼만 골라내기 (오타나 없는 태그로 인한 에러 방지)
                existing_tags = [tag for tag in target_tags if tag in df.columns]
                df = df[existing_tags]
            
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
        start_time="-6h", 
        end_time="now()", 
        target_tags=["Actual_P1_SEL1_HD1", "Actual_P1_SEL2_HD1", "Actual_P1_SEL3_HD1", "Actual_P1_SEL4_HD1", "Actual_P1_SEL5_HD1", "Actual_P1_SEL6_HD1", "Actual_P1_g_s1_HD1", "Actual_P1_g_s2_HD1", "Actual_P1_g_s3_HD1", "Actual_P1_g_s4_HD1", "Actual_P1_g_s5_HD1", "Actual_P1_g_s6_HD1", "Actual_P1_SEL1_HD2", "Actual_P1_SEL2_HD2", "Actual_P1_SEL3_HD2", "Actual_P1_SEL4_HD2", "Actual_P1_SEL5_HD2", "Actual_P1_SEL6_HD2", "Actual_P1_g_s1_HD2", "Actual_P1_g_s2_HD2", "Actual_P1_g_s3_HD2", "Actual_P1_g_s4_HD2", "Actual_P1_g_s5_HD2", "Actual_P1_g_s6_HD2", "Actual_P1_SEL1_HD3", "Actual_P1_SEL2_HD3", "Actual_P1_SEL3_HD3", "Actual_P1_SEL4_HD3", "Actual_P1_SEL5_HD3", "Actual_P1_SEL6_HD3", "Actual_P1_g_s1_HD3", "Actual_P1_g_s2_HD3", "Actual_P1_g_s3_HD3", "Actual_P1_g_s4_HD3", "Actual_P1_g_s5_HD3", "Actual_P1_g_s6_HD3", "Actual_P1_SEL1_HD4", "Actual_P1_SEL2_HD4", "Actual_P1_SEL3_HD4", "Actual_P1_SEL4_HD4", "Actual_P1_SEL5_HD4", "Actual_P1_SEL6_HD4", "Actual_P1_g_s1_HD4", "Actual_P1_g_s2_HD4", "Actual_P1_g_s3_HD4", "Actual_P1_g_s4_HD4", "Actual_P1_g_s5_HD4", "Actual_P1_g_s6_HD4", "MTK_FEED_ERR_T_P1", "Para_DM_Press_H_Set_P1", "Para_DM_Press_L_Set_P1", "Pump_Cir_SPD_Set_P1", "Para_DM_H_ExSet_P1", "TK_Temp_PV_P1", "TK_Temp_H_Set_P1", "TK_Temp_SV_P1", "TK_Temp_L_Set_P1", "TK_Level_PV_P1", "TK_Level_Max_Set_P1", "TK_Level_Stop_Set_P1", "TK_Level_Start_Set_P1", "TK_Level_Min_Set_P1", "TK_FEED_ERR_T_P1", "STK_Temp_PV_P1", "STK_Temp_H_Set_P1", "STK_Temp_SV_P1", "STK_Temp_L_Set_P1", "STK_Level_PV_P1", "STK_Level_Max_Set_P1", "STK_Level_Stop_Set_P1", "STK_Level_Start_Set_P1", "STK_Level_Min_Set_P1", "STK_FEED_ERR_T_P1", "ANA_IN___PT_P1", "Ana_Max___PT_P1", "Gain___PT_P1", "OffSet___PT_P1", "Scale_Max___PT_P1", "Scale_Out___PT_P1", "FT_P1_Imp/L HMI_Real", "FT_P1_비중 HMI_Real", "FT_P1_Offset HMI", "FT_P1_Scale_Out", "Err__PT_P1", "Err__PT_HD_P1", "Ana_In___TT_P1", "Ana_Max___TT_P1", "Gain___TT_P1", "OffSet___TT_P1", "Scale_Max___TT_P1", "Scale_Out___TT_P1", "Ana_In___TT_STK_P1", "Ana_Max___TT_STK_P1", "Gain___TT_STK_P1", "OffSet___TT_STK_P1", "Scale_Max___TT_STK_P1", "Scale_Out___TT_STK_P1", "Ana_In___LT_P1", "Ana_Max___LT_P1", "Gain___LT_P1", "OffSet___LT_P1", "Scale_Max___LT_P1", "Scale_Out___LT_P1", "Err__TT_P1", "Err__TT_JK_P1", "Err__TT_STK_P1", "Err__TT_JK_STK_P1", "Err__LT_P1", "Err__TT_EX_P1", "Err__LT_STK_P1", "g_s_SV_P1", "Pump_rpm_P1", "Pump_Capa_P1", "Ana_Out_Max_P1", "Gain_P1", "Pump_Hz_Max_P1", "Max_Capa_P1", "Ana_Out_P1", "Hz_Out_P1", "Err_P1", "Pump_On_Sw_P1", "Pump_Trip_P1", "Pump_BuildUp_P1", "Pump_Run_P1_C", "CAL_Base_Qty_P1", "CAL_Range_Qty_H_P1", "A_CAL_Result_H_QTY_HMI_P1", "A_CAL_Result_L_QTY_HMI_P1", "TK_Heat_Limit_P1", "TK_Heat_Out_P1", "TK_Cool_Out_P1", "TK_Temp_H_Alarm_P1", "TK_Temp_L_Alarm_P1", "TK_AG_On_Sw_P1", "TK_AG_Trip_P1", "TK_AG_Run_P1", "TK_Temp_Control_P1_Bit8", "TK_Temp_Control_P1_Bit9", "TK_Temp_Control_P1_Bit10", "TK_Temp_Control_P1_Bit11", "TK_Temp_Control_P1_Bit12", "TK_Temp_Control_P1_Bit13", "TK_Temp_Control_P1_Bit14", "TK_Temp_Control_P1_Bit15", "TK_Feed_Auto_Sel_P1", "TK_Feed_On_Sw_P1", "TK_Feed_VV_Out_P1", "TK_Feed_VV_Open_P1", "TK_Feed_VV_Close_P1", "TK_Feed_VV_Open_Err_P1", "TK_Feed_VV_Close_Err_P1", "TK_Feed_Pump_Out_P1", "TK_Feed_Pump_Trip_P1", "TK_Feed_Run_P1", "TK_Feeding_Pump_Err_P1", "TK_Level_Max_Alarm_P1", "TK_Level_LL_Alarm_P1", "TK_Level_HH_CHK_P1", "TK_Level_LL_CHK_P1", "TK_Level_HHH_CHK_P1", "STK_Heat_Limit_P1", "STK_Heat_Out_P1", "STK_Cool_Out_P1", "STK_Temp_H_Alarm_P1", "STK_Temp_L_Alarm_P1", "STK_AG_On_Sw_P1", "STK_AG_Trip_P1", "STK_AG_Run_P1", "STK_Temp_Control_P1_Bit8", "STK_Temp_Control_P1_Bit9", "STK_Temp_Control_P1_Bit10", "STK_Temp_Control_P1_Bit11", "STK_Temp_Control_P1_Bit12", "STK_Temp_Control_P1_Bit13", "STK_Level_Max_CHK_P1", "STK_Level_Min_CHK_P1", "STK_Feed_Auto_Sel_P1", "STK_Feed_On_Sw_P1", "STK_Feed_VV_Out_P1", "STK_Feed_VV_Open_P1", "STK_Feed_VV_Close_P1", "STK_Feed_VV_Open_Err_P1", "STK_Feed_VV_Close_Err_P1", "STK_Feed_Pump_Out_P1(BACK)", "STK_Feed_Pump_Trip_P1", "STK_Feed_Run_P1", "STK_Feeding_Pump_Err_P1", "STK_Level_Max_Alarm_P1", "STK_Level_Min_Alarm_P1", "STK_Level_HH_CHK_P1", "STK_Level_LL_CHK_P1", "STK_Level_HHH_CHK_P1", "Cal_Nozzle_P1", "Press_High_Err_P1", "Press_Low_Err_P1", "Pump_In_Press_Low_Err_P1", "MTK_AG_Trip_P1", "MTK_Level_HH_CHK_P1", "MTK_Level_LL_CHK_P1", "MTK_Feed_VV_Open_Err_P1", "MTK_Feed_VV_Close_Err_P1", "MTK_Feeding_Pump_Err_P1", "MTK_Level_HHH_CHK_P1", "Shot_P1_SEL_HD1", "Shot_Next_On_HD1_P1", "Shot_Next_Off_HD1_P1", "Injection_OP_Err_HD1_P1", "Injection_CL_Err_HD1_P1", "Shot_Injection_HD1_P1", "Shot_Injection_PS_HD1_P1", "Shot_P1_SEL_HD2", "Shot_Next_On_HD2_P1", "Shot_Next_Off_HD2_P1", "Injection_OP_Err_HD2_P1", "Injection_CL_Err_HD2_P1", "Shot_Injection_HD2_P1", "Shot_Injection_PS_HD2_P1", "Test_Shot_Injection_HD1_P1", "Test_Shot_Injection_HD2_P1", "Manual_Build_Up_Sel_HD1_P1", "Manual_Build_Up_Sel_HD2_P1", "Head_SW_Build_Up_HD1_P1", "Head_SW_Build_Up_HD2_P1", "Head_SW_Build_Up_HD3_P1", "Shot_P1_SEL_HD3", "Shot_Next_On_HD3_P1", "Shot_Next_Off_HD3_P1", "Injection_OP_Err_HD3_P1", "Injection_CL_Err_HD3_P1", "Shot_Injection_HD3_P1", "Shot_Injection_PS_HD3_P1", "Test_Shot_Injection_HD3_P1", "Manual_Build_Up_Sel_HD3_P1", "Head_SW_Build_Up_HD3_P1", "Shot_Clean_Up_HD1_P1", "Shot_Clean_DN_HD1_P1", "Shot_Clean_Up_HD2_P1", "Shot_Clean_DN_HD2_P1", "Shot_Clean_Up_HD3_P1", "Shot_Clean_DN_HD3_P1", "Shot_TMR_Run_P1", "A_Cal_2nd 가감속_P1", "Shot_P1_SEL_HD4", "Shot_Next_On_HD4_P1", "Shot_Next_Off_HD4_P1", "Injection_OP_Err_HD4_P1", "Injection_CL_Err_HD4_P1", "Shot_Injection_HD4_P1", "Shot_Injection_PS_HD4_P1", "Test_Shot_Injection_HD4_P1", "Manual_Build_Up_Sel_HD4_P1", "Head_SW_Build_Up_HD4_P1", "Shot_Clean_Up_HD4_P1", "Shot_Clean_DN_HD4_P1", "Ana_In___LT_STK_P1", "Ana_Max___LT_STK_P1", "Gain___LT_STK_P1", "OffSet___LT_STK_P1", "Scale_Max___LT_STK_P1", "Scale_Out___LT_STK_P1", "이전 CNT_P1", "현재 CNT_P1", "초당 CNT_P1", "입력 주파수_P1", "TK_JK_Temp_PV_P1", "TK_JK_Temp_H_Set_P1", "STK_JK_Temp_PV_P1", "STK_JK_Temp_H_Set_P1", "TK_JK_Temp_H_Set_Hys_P1", "STK_JK_Temp_H_Set_Hys_P1", "Ana_In___PT_HD_P1", "Ana_Max___PT_HD_P1", "Gain___PT_HD_P1", "OffSet___PT_HD_P1", "Scale_Max___PT_HD_P1", "Scale_Out___PT_HD_P1", "Ana_In___TT_JK_P1", "Ana_Max___TT_JK_P1", "Gain___TT_JK_P1", "OffSet___TT_JK_P1", "Scale_Max___TT_JK_P1", "Scale_Out___TT_JK_P1", "Ana_In___TT_JK_STK_P1", "Ana_Max___TT_JK_STK_P1", "Gain___TT_JK_STK_P1", "OffSet___TT_JK_STK_P1", "Scale_Max___TT_JK_STK_P1", "Scale_Out___TT_JK_STK_P1", "Ana_In___TT_EX_P1", "Ana_Max___TT_EX_P1", "Gain___TT_EX_P1", "OffSet___TT_EX_P1", "Scale_Max___TT_EX_P1", "Scale_Out___TT_EX_P1", "HD1_P1_인젝션_ON_T", "HD1_P1_인젝션_OFF_T", "HD2_P1_인젝션_ON_T", "HD2_P1_인젝션_OFF_T", "HD3_P1_인젝션_ON_T", "HD3_P1_인젝션_OFF_T", "HD4_P1_인젝션_ON_T", "HD4_P1_인젝션_OFF_T", "HD1_P1_인젝션_ON_T_FLT_SV", "HD1_P1_인젝션_OFF_T_FLT_SV", "HD2_P1_인젝션_ON_T_FLT_SV", "HD2_P1_인젝션_OFF_T_FLT_SV", "HD3_P1_인젝션_ON_T_FLT_SV", "HD3_P1_인젝션_OFF_T_FLT_SV", "HD4_P1_인젝션_ON_T_FLT_SV", "HD4_P1_인젝션_OFF_T_FLT_SV", "A_CAL_START_SEL_HMI_P1", "Auto_Cal Done!_P1", "P1_BACK_SEL TM337/ST241", "HD1_P1_인젝션_ON_T_FLT", "HD1_P1_인젝션_OFF_T_FLT", "HD2_P1_인젝션_ON_T_FLT", "HD2_P1_인젝션_OFF_T_FLT", "HD3_P1_인젝션_ON_T_FLT", "HD3_P1_인젝션_OFF_T_FLT", "HD4_P1_인젝션_ON_T_FLT", "HD4_P1_인젝션_OFF_T_FLT", "펄스값_도달 P1", "펄스계수_시작 Bit_P1", "인젝션_HD1_P1_Sig", "인젝션_HD2_P1_Sig", "인젝션_HD3_P1_Sig", "인젝션_HD4_P1_Sig", "CF2_WT_P1_Feeding_Req", "유압1 INV P1", "유압2 INV P1", "유압3 INV P1", "유압4 INV P1", "Data기록 CNT_P1"]
    )

    # 3. 콘솔에서 살짝 확인
    print(my_df.head())

    # 4. 분석용 CSV로 내려받기 (딱! 저장됩니다)
    extractor.save_to_csv(my_df)