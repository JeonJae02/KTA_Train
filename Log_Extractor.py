import os
import time
import pandas as pd
from influxdb_client import InfluxDBClient
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

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

    def _parse_time(self, t_str):
        """InfluxDB 시간 문자열(-5d, now() 등)을 datetime 객체(UTC)로 변환"""
        now = datetime.now(timezone.utc)
        if t_str == "now()":
            return now
        if isinstance(t_str, str) and t_str.startswith("-"):
            val = int(''.join(filter(str.isdigit, t_str)))
            unit = t_str[-1]
            if unit == 'd': return now - timedelta(days=val)
            if unit == 'h': return now - timedelta(hours=val)
            if unit == 'm': return now - timedelta(minutes=val)
            return now - timedelta(days=val)
        # 절대 시간 문자열 처리 (KST -> UTC)
        return pd.to_datetime(t_str).tz_localize('Asia/Seoul').tz_convert('UTC')

    def get_data(self, start_time, end_time, target_tags=None):
        # 1. 시간 범위를 datetime 객체로 변환
        start_dt = self._parse_time(start_time)
        end_dt = self._parse_time(end_time)
        
        print(f"🚀 전체 구간 데이터 추출 시작: {start_dt.isoformat()} ~ {end_dt.isoformat()}")

        # 2. 태그 필터 생성 (루프 밖에서 한 번만 생성)
        filter_query = ""
        if target_tags:
            flux_array_str = "[" + ", ".join([f'"{tag}"' for tag in target_tags]) + "]"
            filter_query = f'|> filter(fn: (r) => contains(value: r.tag_name, set: {flux_array_str}))'

        all_chunks = []
        current_start = start_dt

        # 3. 하루(24시간) 단위로 끊어서 루프 실행
        while current_start < end_dt:
            current_end = min(current_start + timedelta(hours=6), end_dt)
            
            # Flux 쿼리용 RFC3339 포맷
            str_start = current_start.strftime('%Y-%m-%dT%H:%M:%SZ')
            str_end = current_end.strftime('%Y-%m-%dT%H:%M:%SZ')

            print(f"📦 [Chunk] {str_start} ~ {str_end} 요청 중...")

            flux_query = f"""
            from(bucket: "{self.bucket}")
                |> range(start: {str_start}, stop: {str_end})
                |> filter(fn: (r) => r._measurement == "plc_line2")
                {filter_query}
                |> pivot(rowKey:["_time"], columnKey: ["tag_name"], valueColumn: "_value")
                |> drop(columns: ["_start", "_stop", "_measurement"])
            """

            try:
                chunk_df = self.query_api.query_data_frame(query=flux_query)
                
                if isinstance(chunk_df, list):
                    if len(chunk_df) > 0:
                        all_chunks.append(pd.concat(chunk_df))
                elif not chunk_df.empty:
                    all_chunks.append(chunk_df)
                    
            except Exception as e:
                print(f"❌ [Chunk 에러] {str_start} 구간 실패: {e}")

            current_start = current_end # 다음 구간으로 이동

        # 4. 모든 청크 합치기 및 전처리
        if not all_chunks:
            print("⚠️ 수집된 데이터가 전혀 없습니다.")
            return pd.DataFrame()

        full_df = pd.concat(all_chunks)

        if '_time' in full_df.columns:
            # 한국 시간으로 변환 및 정렬
            full_df['_time'] = pd.to_datetime(full_df['_time']).dt.tz_convert('Asia/Seoul')
            full_df['_time'] = full_df['_time'].dt.tz_localize(None)
            full_df.set_index('_time', inplace=True)
            full_df.index.name = 'Time'
            full_df = full_df.sort_index()
            
            # 결측치 복원 (전체 구간에 대해 수행)
            full_df = full_df.ffill()

        print(f"✅ 전체 데이터 통합 완료! 총 {len(full_df)}행 확보.")
        return full_df

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
        start_time="-1d", 
        end_time="now()", 
        target_tags=["AirBag_Zone_In_PS_CHK_Err", "AirBag_Zone_In_PS_CHK_OK", "Air_Back_Up_LS_CHK_Err", "Air_Supply2_Home_CHK_Err", "Air_Supply2_Nozzle_CY_FWD_Err", "Air_Supply2_Nozzle_CY_REV_Err", "Air_Supply2_Return_Err", "Air_Supply2_Rev_FLT_PX_CHK_Err", "Air_Supply2_Stopper_CY_FWD_Err", "Air_Supply2_Stopper_CY_REV_Err", "Air_Supply3_Home_CHK_Err", "Air_Supply3_Nozzle_CY_FWD_Err", "Air_Supply3_Nozzle_CY_REV_Err", "Air_Supply3_Return_Err", "Air_Supply3_Rev_FLT_PX_CHK_Err", "Air_Supply3_Stopper_CY_FWD_Err", "Air_Supply3_Stopper_CY_REV_Err", "Air_Supply_Home_CHK_Err", "Air_Supply_Nozzle_CY_FWD_Err", "Air_Supply_Nozzle_CY_REV_Err", "Air_Supply_Return_Err", "Air_Supply_Rev_FLT_PX_CHK_Err", "Air_Supply_Stopper_CY_FWD_Err", "Air_Supply_Stopper_CY_REV_Err", "Alarm_All", "Alarm_Bit_P", "Alarm_Buzzer", "Alram_Hone_Time", "AL_ETC_Bit", "AL_ETC_Word", "AL_Heavy_Alarm", "AL_Heavy_Alarm_ETC", "AL_Heavy_Alarm_Line", "AL_Heavy_Alarm_MA", "AL_Heavy_Alarm_TK", "AL_Line_Bit", "AL_Line_Word", "AL_MA_Bit", "AL_MA_Word", "AL_Soft_Alarm", "AL_Soft_Alarm_ETC", "AL_Soft_Alarm_Line", "AL_Soft_Alarm_MA", "AL_Soft_Alarm_TK", "AL_Tank_Bit", "AL_Tank_Word", "AL_Warning", "AL_Warning_Buzzer", "AL_Warning_BZ_Time", "AL_Warning_ETC", "AL_Warning_Line", "AL_Warning_MA", "AL_Warning_PL", "AL_Warning_PL_Time", "AL_Warning_TK", "AT_Start_Pump_Run_CHK_Err", "AT_Start_RB_Ready_CHK_Err", "AT_Start_Select_CHK_Err", "AT_Start_Spare1_Err", "AT_Start_Spare_Err", "Auto_Running_PL", "Auto_Start_Warning", "Auto_Warning_Time", "Build_UP_Fault_HD2", "Build_UP_On_HD1", "Build_UP_On_HD2", "Build_UP_On_HD3", "Cange_Emergency", "Change_Unit_Hmoe_FLT", "Clean_DN_Err_HD1", "Clean_DN_Err_HD2", "Clean_DN_Err_HD3", "Clean_DN_Err_HD4", "Clean_UP_Err_HD1", "Clean_UP_Err_HD2", "Clean_UP_Err_HD3", "Clean_UP_Err_HD4", "Closing_Auto_Sel", "Closing_DN_Over_Err", "Closing_DN_Start_Err", "Closing_DN_Stop_Err", "Closing_Down_RB2_Home_CHK", "Closing_LS_CHK_Err", "Closing_Trip", "Closing_UP_Over_Err", "Closing_UP_Start_Err", "Closing_UP_Stop_Err", "Closing_Zone_In_PS_CHK_Err", "Closing_Zone_In_PS_CHK_OK", "Conv_ON_PB", "Conv_Robot_Home_Fault", "Conv_Run_Warning", "Conv_Start_Closing_CHK_Err", "Conv_Start_Openning_CHK_Err", "Conv_Trip", "Conv_Warning_Time", "Emergency_Sw", "HYD1_Build_Up_Err", "HYD1_Temp_H_Alarm", "HYD1_Temp_L_Alarm", "HYD1_Trip", "HYD2_Build_Up_Err", "HYD2_Temp_H_Alarm", "HYD2_Temp_L_Alarm", "HYD2_Trip", "HYD3_Build_Up_Err", "HYD3_Temp_H_Alarm", "HYD3_Temp_L_Alarm", "HYD3_Trip", "HYD4_Build_Up_Err", "HYD4_Trip", "ID_Fault_Check", "ID_Write_Button_ON_CHK", "Injection_CL_Err_HD1_P1", "Injection_CL_Err_HD1_P2", "Injection_CL_Err_HD2_P1", "Injection_CL_Err_HD2_P2", "Injection_CL_Err_HD3_P1", "Injection_CL_Err_HD3_P2", "Injection_CL_Err_HD4_P1", "Injection_CL_Err_HD4_P2", "Injection_OP_Err_HD1_P1", "Injection_OP_Err_HD1_P2", "Injection_OP_Err_HD2_P1", "Injection_OP_Err_HD2_P2", "Injection_OP_Err_HD3_P1", "Injection_OP_Err_HD3_P2", "Injection_OP_Err_HD4_P1", "Injection_OP_Err_HD4_P2", "Lamp_Test", "MD_FWD_Zone_In_PS_CHK_Err", "MD_FWD_Zone_In_PS_CHK_OK", "MD_REV_Zone_In_PS_CHK_Err", "MD_REV_Zone_In_PS_CHK_OK", "Mould_Clamp_Close_Fault_LS_CHK_Err", "Mould_Cylinder_Back_Fault_LS_CHK_Err", "MTK_AG_Trip_P1", "MTK_AG_Trip_P5", "MTK_Feeding_Pump_Err_P1", "MTK_Feeding_Pump_Err_P5", "MTK_Feed_VV_Close_Err_P1", "MTK_Feed_VV_Close_Err_P5", "MTK_Feed_VV_Open_Err_P1", "MTK_Feed_VV_Open_Err_P5", "MTK_Level_HHH_CHK_P1", "MTK_Level_HHH_CHK_P5", "MTK_Level_HH_CHK_P1", "MTK_Level_HH_CHK_P5", "MTK_Level_LL_CHK_P1", "MTK_Level_LL_CHK_P5", "Opening_DN_Over_Err", "Opening_DN_Start_Err", "Opening_DN_Stop_Err", "Opening_LS_CHK_Err", "Opening_Trip", "Opening_Unit_Err", "Opening_UP_Over_Err", "Opening_UP_Start_Err", "Opening_UP_Stop_Err", "Opening_Zone_In_PS_CHK_Err", "Opening_Zone_In_PS_CHK_OK", "Para_L_C_Err_Hone_Time", "Pour_Zone_In_PS_CHK_OK", "Pour_Zone_Stop_PS_CHK_Err", "Pour_Zone_Stop_PS_CHK_OK", "Prepare", "Press_Err_Total_HD1", "Press_Err_Total_HD2", "Press_Err_Total_HD3", "Press_High_Err_I1", "Press_High_Err_I2", "Press_High_Err_I3", "Press_High_Err_I4", "Press_High_Err_I5", "Press_High_Err_I6", "Press_High_Err_I7", "Press_High_Err_P1", "Press_High_Err_P2", "Press_High_Err_P3", "Press_High_Err_P4", "Press_High_Err_P5", "Press_High_Err_P6", "Press_High_Err_P7", "Press_Low_Err_I1", "Press_Low_Err_I2", "Press_Low_Err_I3", "Press_Low_Err_I4", "Press_Low_Err_I5", "Press_Low_Err_I6", "Press_Low_Err_I7", "Press_Low_Err_P1", "Press_Low_Err_P2", "Press_Low_Err_P3", "Press_Low_Err_P4", "Press_Low_Err_P5", "Press_Low_Err_P6", "Press_Low_Err_P7", "Pump_In_Press_Low_Err_I1", "Pump_In_Press_Low_Err_I2", "Pump_In_Press_Low_Err_I3", "Pump_In_Press_Low_Err_I4", "Pump_In_Press_Low_Err_I5", "Pump_In_Press_Low_Err_I6", "Pump_In_Press_Low_Err_I7", "Pump_In_Press_Low_Err_P1", "Pump_In_Press_Low_Err_P2", "Pump_In_Press_Low_Err_P3", "Pump_In_Press_Low_Err_P4", "Pump_In_Press_Low_Err_P5", "Pump_In_Press_Low_Err_P6", "Pump_In_Press_Low_Err_P7", "Pump_Trip_I1", "Pump_Trip_I2", "Pump_Trip_I3", "Pump_Trip_I4", "Pump_Trip_I5", "Pump_Trip_I6", "Pump_Trip_I7", "Pump_Trip_P1", "Pump_Trip_P2", "Pump_Trip_P3", "Pump_Trip_P4", "Pump_Trip_P5", "Pump_Trip_P6", "Pump_Trip_P7", "RB1_Auto_Mode_CHK_Err", "RB1_BUSY", "RB1_EMERGENCY_CHK", "RB1_Home_Position_CHK_Err", "RB1_Soft_Alarm_CHK", "RB1_START_Err", "RB1_START_LS_CHK", "RB1_Total_Alarm_CHK", "RB2_Auto_Mode_CHK_Err", "RB2_BUSY", "RB2_EMERGENCY_CHK", "RB2_Home_Position_CHK_Err", "RB2_Soft_Alarm_CHK", "RB2_START_Err", "RB2_START_LS_CHK", "RB2_Total_Alarm_CHK", "RB3_ALARM", "RB3_Auto_Mode_CHK_Err", "RB3_Home_Position_CHK_Err", "RB3_START_Err", "Reset_PL", "Reset_Sw", "Shoting_HD1_Up_PX_Off_CHK_Err", "Shoting_HD2_Up_PX_Off_CHK_Err", "Shoting_HD3_Up_PX_Off_CHK_Err", "Shoting_HD4_Up_PX_Off_CHK_Err", "STK_AG_Trip_I1", "STK_AG_Trip_P1", "STK_AG_Trip_P2", "STK_AG_Trip_P3", "STK_AG_Trip_P4", "STK_AG_Trip_P5", "STK_Feeding_Pump_Err_I1", "STK_Feeding_Pump_Err_P1", "STK_Feeding_Pump_Err_P2", "STK_Feeding_Pump_Err_P3", "STK_Feeding_Pump_Err_P4", "STK_Feeding_Pump_Err_P5", "STK_Feed_Pump_Trip_I1", "STK_Feed_Pump_Trip_P1", "STK_Feed_Pump_Trip_P2", "STK_Feed_Pump_Trip_P3", "STK_Feed_Pump_Trip_P4", "STK_Feed_Pump_Trip_P5", "STK_Feed_VV_Close_Err_I1", "STK_Feed_VV_Close_Err_P1", "STK_Feed_VV_Close_Err_P2", "STK_Feed_VV_Close_Err_P3", "STK_Feed_VV_Close_Err_P4", "STK_Feed_VV_Close_Err_P5", "STK_Feed_VV_Open_Err_I1", "STK_Feed_VV_Open_Err_P1", "STK_Feed_VV_Open_Err_P2", "STK_Feed_VV_Open_Err_P3", "STK_Feed_VV_Open_Err_P4", "STK_Feed_VV_Open_Err_P5", "STK_Heat_Limit_I1", "STK_Heat_Limit_P1", "STK_Heat_Limit_P2", "STK_Heat_Limit_P3", "STK_Heat_Limit_P4", "STK_Heat_Limit_P5", "STK_Level_HHH_CHK_I1", "STK_Level_HHH_CHK_P1", "STK_Level_HHH_CHK_P2", "STK_Level_HHH_CHK_P3", "STK_Level_HHH_CHK_P4", "STK_Level_HHH_CHK_P5", "STK_Level_HH_CHK_I1", "STK_Level_HH_CHK_P1", "STK_Level_HH_CHK_P2", "STK_Level_HH_CHK_P3", "STK_Level_HH_CHK_P4", "STK_Level_HH_CHK_P5", "STK_Level_LL_CHK_I1", "STK_Level_LL_CHK_P1", "STK_Level_LL_CHK_P2", "STK_Level_LL_CHK_P3", "STK_Level_LL_CHK_P4", "STK_Level_LL_CHK_P5", "STK_Level_Max_Alarm_I1", "STK_Level_Max_Alarm_P1", "STK_Level_Max_Alarm_P2", "STK_Level_Max_Alarm_P3", "STK_Level_Max_Alarm_P4", "STK_Level_Max_Alarm_P5", "STK_Level_Min_Alarm_I1", "STK_Level_Min_Alarm_P1", "STK_Level_Min_Alarm_P2", "STK_Level_Min_Alarm_P3", "STK_Level_Min_Alarm_P4", "STK_Level_Min_Alarm_P5", "STK_Temp_H_Alarm_I1", "STK_Temp_H_Alarm_P1", "STK_Temp_H_Alarm_P2", "STK_Temp_H_Alarm_P3", "STK_Temp_H_Alarm_P4", "STK_Temp_H_Alarm_P5", "STK_Temp_L_Alarm_I1", "STK_Temp_L_Alarm_P1", "STK_Temp_L_Alarm_P2", "STK_Temp_L_Alarm_P3", "STK_Temp_L_Alarm_P4", "STK_Temp_L_Alarm_P5", "Tension_HYD_BuildUP_Err", "Tension_HYD_Off_CHK_Err", "Tension_LS_CHK_Err", "Tension_Trip", "TK_AG_Trip_I1", "TK_AG_Trip_I2", "TK_AG_Trip_I3", "TK_AG_Trip_P1", "TK_AG_Trip_P2", "TK_AG_Trip_P3", "TK_AG_Trip_P4", "TK_AG_Trip_P5", "TK_Feeding_Pump_Err_I1", "TK_Feeding_Pump_Err_I2", "TK_Feeding_Pump_Err_I3", "TK_Feeding_Pump_Err_P1", "TK_Feeding_Pump_Err_P2", "TK_Feeding_Pump_Err_P3", "TK_Feeding_Pump_Err_P4", "TK_Feeding_Pump_Err_P5", "TK_Feed_Pump_Trip_I1", "TK_Feed_Pump_Trip_I2", "TK_Feed_Pump_Trip_I3", "TK_Feed_Pump_Trip_P1", "TK_Feed_Pump_Trip_P2", "TK_Feed_Pump_Trip_P3", "TK_Feed_Pump_Trip_P4", "TK_Feed_Pump_Trip_P5", "TK_Feed_VV_Close_Err_I1", "TK_Feed_VV_Close_Err_I2", "TK_Feed_VV_Close_Err_I3", "TK_Feed_VV_Close_Err_P1", "TK_Feed_VV_Close_Err_P2", "TK_Feed_VV_Close_Err_P3", "TK_Feed_VV_Close_Err_P4", "TK_Feed_VV_Close_Err_P5", "TK_Feed_VV_Open_Err_I1", "TK_Feed_VV_Open_Err_I2", "TK_Feed_VV_Open_Err_I3", "TK_Feed_VV_Open_Err_P1", "TK_Feed_VV_Open_Err_P2", "TK_Feed_VV_Open_Err_P3", "TK_Feed_VV_Open_Err_P4", "TK_Feed_VV_Open_Err_P5", "TK_Heat_Limit_I1", "TK_Heat_Limit_I2", "TK_Heat_Limit_I3", "TK_Heat_Limit_P1", "TK_Heat_Limit_P2", "TK_Heat_Limit_P3", "TK_Heat_Limit_P4", "TK_Heat_Limit_P5", "TK_Level_HHH_CHK_I1", "TK_Level_HHH_CHK_I2", "TK_Level_HHH_CHK_I3", "TK_Level_HHH_CHK_P1", "TK_Level_HHH_CHK_P2", "TK_Level_HHH_CHK_P3", "TK_Level_HHH_CHK_P4", "TK_Level_HHH_CHK_P5", "TK_Level_HH_CHK_I1", "TK_Level_HH_CHK_I2", "TK_Level_HH_CHK_I3", "TK_Level_HH_CHK_P1", "TK_Level_HH_CHK_P2", "TK_Level_HH_CHK_P3", "TK_Level_HH_CHK_P4", "TK_Level_HH_CHK_P5", "TK_Level_LL_CHK_I1", "TK_Level_LL_CHK_I2", "TK_Level_LL_CHK_I3", "TK_Level_LL_CHK_P1", "TK_Level_LL_CHK_P2", "TK_Level_LL_CHK_P3", "TK_Level_LL_CHK_P4", "TK_Level_LL_CHK_P5", "TK_Level_Max_Alarm_I1", "TK_Level_Max_Alarm_I2", "TK_Level_Max_Alarm_I3", "TK_Level_Max_Alarm_P1", "TK_Level_Max_Alarm_P2", "TK_Level_Max_Alarm_P3", "TK_Level_Max_Alarm_P4", "TK_Level_Max_Alarm_P5", "TK_Level_Min_Alarm_I1", "TK_Level_Min_Alarm_I2", "TK_Level_Min_Alarm_I3", "TK_Level_Min_Alarm_P1", "TK_Level_Min_Alarm_P2", "TK_Level_Min_Alarm_P3", "TK_Level_Min_Alarm_P4", "TK_Level_Min_Alarm_P5", "TK_Temp_H_Alarm_I1", "TK_Temp_H_Alarm_I2", "TK_Temp_H_Alarm_I3", "TK_Temp_H_Alarm_P1", "TK_Temp_H_Alarm_P2", "TK_Temp_H_Alarm_P3", "TK_Temp_H_Alarm_P4", "TK_Temp_H_Alarm_P5", "TK_Temp_L_Alarm_I1", "TK_Temp_L_Alarm_I2", "TK_Temp_L_Alarm_I3", "TK_Temp_L_Alarm_P1", "TK_Temp_L_Alarm_P2", "TK_Temp_L_Alarm_P3", "TK_Temp_L_Alarm_P4", "TK_Temp_L_Alarm_P5", "_0A_CH0_IDD", "_0A_CH10_IDD", "_0A_CH11_IDD", "_0A_CH12_IDD", "_0A_CH13_IDD", "_0A_CH14_IDD", "_0A_CH15_IDD", "_0A_CH1_IDD", "_0A_CH2_IDD", "_0A_CH3_IDD", "_0A_CH4_IDD", "_0A_CH5_IDD", "_0A_CH6_IDD", "_0A_CH7_IDD", "_0A_CH8_IDD", "_0A_CH9_IDD", "_10_CH0_IDD", "_10_CH1_IDD", "_10_CH2_IDD", "_10_CH3_IDD", "_10_CH4_IDD", "_10_CH5_IDD", "_10_CH6_IDD", "_10_CH7_IDD", "_11_CH0_IDD", "_11_CH1_IDD", "_11_CH2_IDD", "_11_CH3_IDD", "_11_CH4_IDD", "_11_CH5_IDD", "_11_CH6_IDD", "_11_CH7_IDD", "금형교체존_인터록SW_CHK_Err", "대차_Dog_CHK1_Err", "대차_Dog_CHK2_Err", "대차_Dog_CHK3_Err"]
    )

    # 3. 콘솔에서 살짝 확인
    print(my_df.head())

    # 4. 분석용 CSV로 내려받기 (딱! 저장됩니다)
    extractor.save_to_csv(my_df)