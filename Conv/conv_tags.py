"""컨베이어 센서 분석에 쓰는 태그 그룹 정의 — 추출기·노트북이 같이 쓴다.

물리 I/O 는 IL 과 같은 P 주소 이름으로 받는다(DB 에 사람 이름 시리즈도 같이 있지만 같은 점이다).
PLC 내부 비트·카운터는 매핑 CSV 의 변수명/R 주소로 받는다.

모든 그룹에 COMMON 을 넣는다 — 그룹 파일 하나만 열어도 운전 구간·속도·대차번호로 거를 수 있게.
"""

#: 모든 그룹 공통: 운전 여부·속도 지령·메인 대차번호·메인 카운터 2개(위치 눈금 복원용)
COMMON = [
    "Conv_Run_Out",           # 컨베이어 운전 출력
    "Hz_In_Conv",             # 속도 지령 (0.1 Hz). 초당 카운트 = 0.2743 × Hz/10 (10/19 실측)
    "Now_Actual_Wagon_Num",   # 메인 대차번호 1–42 (P00140 에서 +1, P00142 에서 1)
    "R00030",                 # Closing 존 카운터  — P00154 에서 1로 리셋
    "R00040",                 # 주입부 카운터      — 대차 진입(P00140)에서 1로 리셋
]

GROUPS = {
    # ① 구동부 + 주입부 대차 감지 (메인 라인)
    "main": [
        "P00140", "P00141", "P00142",              # 대차 진입 / 쉬프트 / 1번 대차 감지
        "P00143", "P00144",                        # 엔코더 A / B (원본은 0.5초 샘플링이라 참고용)
        "R00080",                                  # 쉬프트 카운터 — P00141 에서 리셋
        "R00041", "R00042", "R00045", "R00046",    # 로봇1·2 시작/동기 위치 설정값
        "R00081", "R00082", "R00083", "R00085",    # 정지 위치 설정값
        "R00065", "R00066", "R00067",              # 한대차 펄스 최소/최대, 존 미감지 에러 기준(160)
        "Carrier_IN_CHK", "Actual_WAGGON_NUM", "대차번호 변경 Run",
        "Conv_Brake_Time", "Conv_Run_Slow", "Conv_Stop", "Conv_Stop_Ready", "Conv_Run_Warning",
        "Conv_Emergency", "Conv_Trip", "Conv_Auto_Sel", "Conv_Ps_Waggon_Slow", "Conv_Ps_Waggon_Stop",
        "P0021C", "P0021D", "P0021E", "P0021F",    # 인버터 정회전 / 역회전 / X4 / 리셋
        "P0025D", "P0003A",                        # 브레이크 MC / 컨베이어 트립
        "대차_Dog_CHK1_Err", "대차_Dog_CHK2_Err", "대차_Dog_CHK3_Err", "Wagon_Dog_Err_Num",
        "Conv_Zone_In_PS_CHK_Err", "Pour_Zone_In_PS_CHK_OK",
    ],
    # ② 오프닝 존 (왼쪽 회전부, 리프트)
    "opening": [
        "P00164",                                  # 오프닝 존 진입 감지 → R00070 리셋
        "P00160", "P00161", "P00162", "P00163",    # 상승정지 / 상승감속 / 하강감속 / 하강정지
        "P00165", "P00166", "P0016E",              # 오버트래블 상승 / 하강, 비상 감지
        "P00218", "P00219", "P0025F", "P0003C",    # 모터 정 / 역, 브레이크, 트립
        "R00070", "R00071", "R00072", "R00073", "R00074",
        "Hz_In_Opening", "Opening_Auto_Sel", "Opening_DN_Start", "Opening_UP_Start",
        "Opening_Down_Fwd_Out", "Opening_Up_Rev_Out", "Opening_Slow_Out",
        "Opening_Zone_In_PS_CHK_Err", "Opening_Unit_Err", "Opening_DN_Start_Err", "Opening_DN_Stop_Err",
        "Opening_UP_Start_Err", "Opening_UP_Stop_Err", "Opening_UP_Over_Err", "Opening_DN_Over_Err",
        "Opening_LS_CHK_Err",
    ],
    # ③ 클로징 존 (오른쪽 회전부, 리프트)
    "closing": [
        "P00154",                                  # 클로징 존 진입 감지 → R00030 리셋
        "P00150", "P00151", "P00152", "P00153",
        "P00155", "P00156", "P0015E",
        "P00214", "P00215", "P0025E", "P0003B",
        "R00031", "R00032", "R00033", "R00034",
        "Hz_In_Closing", "Closing_Auto_Sel", "Closing_DN_Start", "Closing_UP_Start",
        "Closing_Down_Fwd_Out", "Closing_Up_Rev_Out", "Closing_Slow_Out",
        "Closing_Zone_In_PS_CHK_Err", "Closing_Unit_Err", "Closing_DN_Start_Err", "Closing_DN_Stop_Err",
        "Closing_UP_Start_Err", "Closing_UP_Stop_Err", "Closing_UP_Over_Err", "Closing_DN_Over_Err",
        "Closing_LS_CHK_Err",
    ],
    # ④ 전진 유닛 (중형 CY 전진용 에어 주입, Air_Supply2)
    "md_fwd": [
        "P00450", "P00451",                        # 1번 대차 감지 / 진입 감지 → R00055 리셋
        "P00452", "P00453", "P00454",              # 정위치 / 끝단 후진 / 비상 감지
        "P00455", "P00456", "P00457",              # 스톱퍼 전진 / 후진 감지, 노즐 후진 감지
        "P00470", "P00471", "P00472", "P00473",    # 스톱퍼 / 노즐 전후진 / 원점복귀 / 노즐 오픈 SOL
        "R00055", "R00056", "R00057",
        "Mould_CY_FWD_Use", "MD_FWD_Zone_In_PS_CHK_Err", "MD_FWD_Zone_In_PS_CHK_OK",
        "Air_Supply2_Home_CHK", "Air_Supply2_In_CHK_PX", "Air_Supply2_In_CHK_P",
        "Air_Supply2_In_CHK_Stopper_FWD", "Air_Supply2_Home_Off_CHK", "Air_Supply2_Nozzle_FWD",
        "Air_Supply2_Nozzle_SOL_Open", "Air_Supply2_Rev_PX_CHK", "Air_Supply2_Nozzle_REV_CHK",
        "Air_Supply2_Return_On", "Air_Supply2_Return_OK_CHK",
        "Air_Supply2_Stopper_CY_FWD_Err", "Air_Supply2_Stopper_CY_REV_Err", "Air_Supply2_Nozzle_CY_FWD_Err",
        "Air_Supply2_Nozzle_CY_REV_Err", "Air_Supply2_Return_Err", "Air_Supply2_Home_CHK_Err",
        "Air_Supply2_Rev_FLT_PX_CHK_Err", "Air_Supply2_Total_Err",
    ],
    # ⑤ 후진 유닛 (Air_Supply3) — 전진 유닛과 같은 구조
    "md_rev": [
        "P00458", "P00459",
        "P0045A", "P0045B", "P0045C",
        "P0045D", "P0045E", "P0045F",
        "P00474", "P00475", "P00476", "P00477",
        "R00060", "R00061", "R00062",
        "Mould_CY_REV_Use", "MD_REV_Zone_In_PS_CHK_Err", "MD_REV_Zone_In_PS_CHK_OK",
        "Air_Supply3_Home_CHK", "Air_Supply3_In_CHK_PX", "Air_Supply3_In_CHK_P",
        "Air_Supply3_In_CHK_Stopper_FWD", "Air_Supply3_Home_Off_CHK", "Air_Supply3_Nozzle_FWD",
        "Air_Supply3_Nozzle_SOL_Open", "Air_Supply3_Rev_PX_CHK", "Air_Supply3_Nozzle_REV_CHK",
        "Air_Supply3_Return_On", "Air_Supply3_Return_OK_CHK",
        "Air_Supply3_Stopper_CY_FWD_Err", "Air_Supply3_Stopper_CY_REV_Err", "Air_Supply3_Nozzle_CY_FWD_Err",
        "Air_Supply3_Nozzle_CY_REV_Err", "Air_Supply3_Return_Err", "Air_Supply3_Home_CHK_Err",
        "Air_Supply3_Rev_FLT_PX_CHK_Err", "Air_Supply3_Total_Err",
    ],
    # ⑥ 에어백 유닛 #1 (Air_Supply) — 진입 센서 P00130 은 PLC 에서 꺼져 있음, 사용 여부 확인용
    "airbag": [
        "P00130", "P00131", "P00132", "P00133", "P00134", "P00135", "P00136", "P00137",
        "P0028A", "P0028B", "P0028C", "P00243",
        "R00050", "R00051", "R00052",
        "Air_Supply_Use_Sel", "AirBag_Zone_In_PS_CHK_Err", "AirBag_Zone_In_PS_CHK_OK",
        "Air_Supply_In_CHK_PX", "Air_Supply_In_CHK_Stopper_FWD", "Air_Supply_Nozzle_FWD",
        "Air_Supply_Nozzle_SOL_Open", "Air_Supply_Return_On", "Air_Supply_Total_Err",
    ],
    # ⑦ 체인 텐션·안전 (장기 추세·마스킹용)
    "tension": [
        "P0006F", "P0006E", "P0003D", "P00250", "P00238", "P0015F",
        "Tension_CHK_LS1", "Tension_CHK_LS2", "Tension_HYD_Run", "Tension_HYD_BuildUP", "Tension_Press_L",
        "Tension_LS_CHK_Err", "Tension_Emergency", "Tension_HYD_Off_CHK_Err",
    ],
}

ORDER = list(GROUPS)


def group_tags(group):
    """그룹 태그 + 공통 태그 (중복 제거, 순서 유지)."""
    return list(dict.fromkeys(COMMON + GROUPS[group]))
