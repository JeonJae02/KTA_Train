TOP_LEVEL_ALARMS = [
    "AL_Warning_Line", "AL_Soft_Alarm_Line", "AL_Heavy_Alarm_Line",
    "AL_Warning_MA", "AL_Soft_Alarm_MA", "AL_Heavy_Alarm_MA",
    "AL_Warning_TK", "AL_Soft_Alarm_TK", "AL_Heavy_Alarm_TK",
    "AL_Warning_ETC", "AL_Soft_Alarm_ETC", "AL_Heavy_Alarm_ETC"
]

MONIORING_TAGS = [
    "TK_Temp_PV_P1", "TK_Temp_PV_I1", 
    "Scale_Out___PT_P1", "Scale_Out___PT_P2", "Scale_Out___PT_P3", "Scale_Out___PT_P4", "Scale_Out___PT_P5", "Scale_Out___PT_P6", "Scale_Out___PT_P7",
    "Scale_Out___PT_I1", "Scale_Out___PT_I2", "Scale_Out___PT_I3", "Scale_Out___PT_I4", "Scale_Out___PT_I5", "Scale_Out___PT_I6", "Scale_Out___PT_I7", 
    "g_s_SV_I1", "g_s_SV_I2", "g_s_SV_I3", "g_s_SV_I4", "g_s_SV_I5", "g_s_SV_I6", "g_s_SV_I7",
    "g_s_SV_P1", "g_s_SV_P2", "g_s_SV_P3", "g_s_SV_P4", "g_s_SV_P5", "g_s_SV_P6", "g_s_SV_P7",
    "Scale_Out___FT_I1", "Scale_Out___FT_I2", "Scale_Out___FT_I3", "Scale_Out___FT_I4", "Scale_Out___FT_I5", "Scale_Out___FT_I6", "Scale_Out___FT_I7",
    "Scale_Out___FT_P1", "Scale_Out___FT_P2", "Scale_Out___FT_P3", "Scale_Out___FT_P4", "Scale_Out___FT_P5", "Scale_Out___FT_P6", "Scale_Out___FT_P7",
    "Ana_Out_P1", "Ana_Out_P2", "Ana_Out_P3", "Ana_Out_P4", "Ana_Out_P5", "Ana_Out_P6", "Ana_Out_P7",
    "Ana_Out_I1", "Ana_Out_I2", "Ana_Out_I3", "Ana_Out_I4", "Ana_Out_I5", "Ana_Out_I6", "Ana_Out_I7", 
    "Pump_BuildUp_P1", "Pump_BuildUp_P2", "Pump_BuildUp_P3", "Pump_BuildUp_P4", "Pump_BuildUp_P5", "Pump_BuildUp_P6", "Pump_BuildUp_P7",
    "Pump_BuildUp_I1", "Pump_BuildUp_I2", "Pump_BuildUp_I3", "Pump_BuildUp_I4", "Pump_BuildUp_I5", "Pump_BuildUp_I6", "Pump_BuildUp_I7",
    "RB1_Robot_Num", "RB2_Robot_Num", 
    "TK_Temp_PV_P1", "TK_Temp_PV_P2", "TK_Temp_PV_P3", "TK_Temp_PV_P4", "TK_Temp_PV_P5", "TK_Temp_PV_I1", "TK_Temp_PV_I2", "TK_Temp_PV_I3", 
    "TK_Level_PV_P1", "TK_Level_PV_P2", "TK_Level_PV_P3", "TK_Level_PV_P4", "TK_Level_PV_P5", "TK_Level_PV_I1", "TK_Level_PV_I2", "TK_Level_PV_I3",

    # 인젝션 실측 동작시간 (Forming_PGM.il: MOVP Shot_TMR_xx → D0470x).
    # 상한값은 D0472x/D0473x 이고 알람은 `D04720 < D04700` 형태로 뜬다.
    # 센서는 아니지만 "실제로 몇 초 걸렸는가"라는 실측량이므로 고장 후보가 된다.
    # 블록 {"type":"D","offset":4700,"size":40} 으로 이미 수신 중.
    # IL 원문이 raw 주소를 쓰므로 심볼명이 아니라 주소로 적는다
    # (TagMapper 가 name / il_addr 두 키를 모두 넣어주므로 조회된다).
    "D04700", "D04701", "D04702", "D04703", "D04704", "D04705", "D04706", "D04707",
    "D04710", "D04711", "D04712", "D04713", "D04714", "D04715",

    # 릴레이 모델(tools_yeji/relay_models) 입력. 전부 PREDICTIVE 블록 안이라 이미 수신 중이다.
    # 탱크 온도 SV / 하한(L_Set) — 0.1℃ 단위. heat 규칙과 최저 PV 시작값에 쓴다.
    "TK_Temp_SV_P1", "TK_Temp_SV_P2", "TK_Temp_SV_P3", "TK_Temp_SV_P4", "TK_Temp_SV_P5", "TK_Temp_SV_I1", "TK_Temp_SV_I2", "TK_Temp_SV_I3",
    "TK_Temp_L_Set_P1", "TK_Temp_L_Set_P2", "TK_Temp_L_Set_P3", "TK_Temp_L_Set_P4", "TK_Temp_L_Set_P5", "TK_Temp_L_Set_I1", "TK_Temp_L_Set_I2", "TK_Temp_L_Set_I3",
    # 가동 플래그(M00004). 비가동 중 exchanger/cooler 경보를 막는다.
    "Auto_Running_PL",
    # 헤드 인젝션 SOL — 켜짐/꺼짐이 ON_T/OFF_T 사이클 경계다(추세용).
    "P00279", "P0028D", "P0027B", "P0027C", "P0027E", "P0028E", "P00280",

    # 펌프 사이클 모델(tools_yeji/pump_models) 입력. 전부 PREDICTIVE·DIAGNOSTIC 블록 안이라 이미 수신 중이다.
    # 인버터 지령 Hz (D00808~) — 펌프 잔차·묶음(Hz 단계)
    "Hz_Out_P1", "Hz_Out_P2", "Hz_Out_P3", "Hz_Out_P4", "Hz_Out_P5", "Hz_Out_P6", "Hz_Out_P7",
    "Hz_Out_I1", "Hz_Out_I2", "Hz_Out_I3", "Hz_Out_I4", "Hz_Out_I5", "Hz_Out_I6", "Hz_Out_I7",
    # 헤드 사출 신호 (D01321.8~) — 사이클 시작
    "Shot_Injection_HD1_P1", "Shot_Injection_HD1_P2", "Shot_Injection_HD2_P1", "Shot_Injection_HD2_P2",
    "Shot_Injection_HD3_P1", "Shot_Injection_HD3_P2", "Shot_Injection_HD4_P1",
    # 알람 비트 (D01350.0~3) — 알람 중 사이클은 판정에서 뺀다
    "AL_Line_Bit", "AL_MA_Bit", "AL_Tank_Bit", "AL_ETC_Bit"
]

# 컨베이어 모델(tools_yeji/conveyor_models) 입력. 전부 PREDICTIVE 블록 안이라 이미 수신 중이다.
# MONIORING_TAGS 에 넣지 않는다 — alarm_index 가 그 목록을 측정값(고장 후보)으로 보는데,
# R00040·Now_Actual_Wagon_Num 같은 스텝·대차 번호가 비교식 26·23곳에서 후보로 올라간다.
CONVEYOR_TAGS = [
    # [코어] 존 카운터 · 대차 통과 센서 비트 · 대차 번호 · 운전 · 속도 지령
    "R00030", "R00040", "R00055", "R00060", "R00070", "R00080",
    "P00140", "P00141", "P00154", "P00164", "P00451", "P00459",
    "Now_Actual_Wagon_Num", "Conv_Run_Out", "Hz_In_Conv",
    # [유닛 전진] SOL 4 · 정위치 · 비상 · 스톱퍼 전진/후진 · 노즐 후진 · 끝단 후진 래치
    "P00470", "P00471", "P00472", "P00473", "P00452", "P00454", "P00455", "P00456", "P00457", "Air_Supply2_Rev_PX_CHK",
    # [유닛 후진]
    "P00474", "P00475", "P00476", "P00477", "P0045A", "P0045C", "P0045D", "P0045E", "P0045F", "Air_Supply3_Rev_PX_CHK",
    # [유닛 에어백] 노즐 후진감지가 2개 (P00136 · P00137)
    "P0028A", "P0028B", "P0028C", "P00243", "P00131", "P00133", "P00134", "P00135", "P00136", "P00137", "Air_Supply_Rev_PX_CHK",
]


DEVICE_TAGS = [
    "P00030","P00031","P00032","P00033","P00034","P00035","P00036","P00037","P00038","P00039","P0003A","P0003B","P0003C","P0003E","P0003F",
    "P00040","P00041","P00042","P00043","P00044","P00045","P00046","P00047","P00048","P00049","P0004A","P0004B","P0004C","P0004D",
    "P00066","P00067","P00068","P00069","P0006A","P0006B","P0006C","P0006D",
    "P00070","P00071","P00072","P00073","P00074","P00075","P00076","P00077","P00078","P00079","P0007A","P0007B","P0007C","P0007D","P0007E","P0007F",
    "P00080","P00081","P00082","P00083","P00084","P00085","P00086","P00087","P00088","P00089","P0008A","P0008B","P0008C","P0008D","P0008E","P0008F",
    "P00190","P00191","P00192","P00193","P00194","P00195","P00196","P00197","P00198","P00199","P0019A","P0019B","P0019C","P0019D","P0019E",
    "P00200","P00201","P00202","P00203","P00204","P00205","P00206","P00207","P00208","P00209","P0020A","P0020B","P0020C","P0020D","P0020E",
    "P00210","P00211","P00212","P00213","P00214","P00215","P00216","P00217","P00218","P00219","P0021A","P0021B","P0021C","P0021D","P0021E","P0021F",
    "P00220","P00221","P00222","P00223","P00224","P00225","P00226","P00227","P00228","P00229","P0022A","P0022B","P0022C","P0022D","P0022E","P0022F",
    "P00230","P00231","P00232","P00233","P00234","P00235","P00236","P00237","P00238","P00239","P0023A","P0023B","P0023C","P0023D","P0023E","P0023F",
    "P00240","P00241","P00242","P00243","P00244","P00245","P00246","P00247","P00248","P00249","P0024E","P0024F",
    "P00250","P00251","P00252","P00253","P00254","P00255","P00256","P00257","P00258","P00259","P0025A","P0025B","P0025C","P0025D","P0025E","P0025F",
    "P00270","P00271","P00272","P00273","P00274","P00275","P00276","P00277",
    "P00282","P00283","P00284","P00285","P00286","P00287","P00288","P00289",
    "P00306","P00307",
    "P00460","P00461","P00462","P00463","P00464","P00465","P00466","P00467","P00468","P00469","P0046A","P0046B","P0046C","P0046D","P0046E","P0046F",
    "P00478","P00479","P0047A","P0047B"
]

SPOOFDATA = {
    "AL_Warning_Line": 0,           # 라인 경고 알람
    "AL_Soft_Alarm_Line": 0,        # 라인 소프트 알람
    "AL_Heavy_Alarm_Line": 1,       # 라인 중대 알람
    "AL_Warning_MA": 0,             # MA 경고 알람
    "AL_Soft_Alarm_MA": 0,          # MA 소프트 알람
    "AL_Heavy_Alarm_MA": 1,         # MA 중대 알람
    "AL_Warning_TK": 0,             # 탱크 경고 알람
    "AL_Soft_Alarm_TK": 0,          # 탱크 소프트 알람
    "AL_Heavy_Alarm_TK": 0,         # 탱크 중대 알람
    "AL_Warning_ETC": 0,            # 기타 경고 알람
    "AL_Soft_Alarm_ETC": 0,         # 기타 소프트 알람
    "AL_Heavy_Alarm_ETC": 0,        # 기타 중대 알람
    "K00030" : 0,
    "K00031" : 1,
    "Pump_Trip_P1": 1,          # 1번 펌프 트립
    "Pump_Trip_I1": 1,          # 2번 펌프 트립
    "Pump_Trip_I3": 1,          # 4번 펌프 트립
    "Pump_Trip_P3": 1,          # 5번 펌프 트립
    "Scale_Out___PT_P1":68,     # 압력 센서 P1
    "Scale_Out___PT_I1":33,     # 압력 센서 I1
    "Scale_Out___PT_P3": 120.5, # 유량 센서 P3
    "Scale_Out___PT_I3": 105.5, # 유량 센서 I3
    "Scale_Out___FT_P1":1496,     # 압력 센서 P1
    "Scale_Out___FT_I1":776,     # 압력 센서 I1
    "Scale_Out___FT_P3": 2856, # 유량 센서 P3
    "Scale_Out___FT_I3": 2424, # 유량 센서 I3
    "Pump_BuildUp_P1": 1,        # 1번 펌프 빌드업
    "Pump_BuildUp_I1": 1,        # 2번 펌프 빌드업
    "Pump_BuildUp_I3": 1
}

ALL_WATCH_TAGS = TOP_LEVEL_ALARMS + MONIORING_TAGS + DEVICE_TAGS + CONVEYOR_TAGS

# IDLE 감시용: 알람 트리거 감지 + 연속 센서 트렌드
# 하위 알람과 게이트 K 비트는 Gate_Keeper 가 기동 시 IL 에서 계산해 덧붙인다
# (Data_collect.add_mode_watch_tags). TOP_LEVEL_ALARMS 비트는 트리거에 더는 쓰지
# 않지만, 룽은 하위 알람 정의(tools/Alarm_Defs.py)의 원천이다.
PREDICTIVE_WATCH_TAGS = TOP_LEVEL_ALARMS + MONIORING_TAGS + DEVICE_TAGS + CONVEYOR_TAGS

# 고장 분석용: PREDICTIVE의 모든 태그 + 물리 I/O (펌프트립, 인버터, 밸브 등)
DIAGNOSTIC_WATCH_TAGS = ALL_WATCH_TAGS