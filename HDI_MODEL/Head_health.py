"""
각 헤드는 D04700: HD1_P1_인젝션_ON_T과 D04701: HD1_P1_인젝션_OFF_T의 값을 받을 것이다. 
on은 17초, off는 10초를 상한선으로 설정할 것이다. 
이때, 정상상태를 기준으로 할 것이기 때문에 공장이 작동 한 지 수 분 후부터 데이터를 수집할 예정이다. (추후 추가)
"""
#HDn_Pn_Inj_On_T와 off_T는 위 변수의 모든 펌프에대한 (4,2) shape의 np.array로 들어올 것이다.
import numpy as np

def Injection_Time_Preprocessing(
    D04700, D04701,
    D04702, D04703,
    D04704, D04705,
    D04706, D04707,
    D04710, D04711,
    D04712, D04713,
    D04714, D04715
):

    HDn_Pn_Inj_On_T = np.array([
        [D04700, D04702],
        [D04704, D04706],
        [D04710, D04712],
        [D04714, 0]
    ])

    HDn_Pn_Inj_Off_T = np.array([
        [D04701, D04703],
        [D04705, D04707],
        [D04711, D04713],
        [D04715, 0]
    ])

    return HDn_Pn_Inj_On_T, HDn_Pn_Inj_Off_T

def Head_Injection_ON_Err(HDn_Pn_Inj_On_T):
    result = np.zeros((4, 2))
    for i in range(4):
        for j in range(2):
            if HDn_Pn_Inj_On_T[i,j] >= 17:
                result[i,j] = 1

    return result

def Head_Injection_OFF_Err(HDn_Pn_Inj_Off_T):
    result = np.zeros((4, 2))
    for i in range(4):
        for j in range(2):
            if HDn_Pn_Inj_Off_T[i,j] >= 10:
                result[i,j] = 1

    return result




import numpy as np
import json


def Injection_Time_Preprocessing(
    D04700, D04701,
    D04702, D04703,
    D04704, D04705,
    D04706, D04707,
    D04710, D04711,
    D04712, D04713,
    D04714, D04715
):

    HDn_Pn_Inj_On_T = np.array([
        [D04700, D04702],
        [D04704, D04706],
        [D04710, D04712],
        [D04714, 0]
    ])

    HDn_Pn_Inj_Off_T = np.array([
        [D04701, D04703],
        [D04705, D04707],
        [D04711, D04713],
        [D04715, 0]
    ])

    return HDn_Pn_Inj_On_T, HDn_Pn_Inj_Off_T


def Head_Injection_ON_Err(HDn_Pn_Inj_On_T):

    result = np.zeros((4, 2), dtype=int)

    for i in range(4):
        for j in range(2):

            if HDn_Pn_Inj_On_T[i, j] >= 17:
                result[i, j] = 1

    return result


def Head_Injection_OFF_Err(HDn_Pn_Inj_Off_T):

    result = np.zeros((4, 2), dtype=int)

    for i in range(4):
        for j in range(2):

            if HDn_Pn_Inj_Off_T[i, j] >= 10:
                result[i, j] = 1

    return result


def Injection_Relay_Warning(
    D04700, D04701,
    D04702, D04703,
    D04704, D04705,
    D04706, D04707,
    D04710, D04711,
    D04712, D04713,
    D04714, D04715
):

    # --------------------------------
    # 1. 데이터 전처리
    # --------------------------------

    HDn_Pn_Inj_On_T, HDn_Pn_Inj_Off_T = \
        Injection_Time_Preprocessing(
            D04700, D04701,
            D04702, D04703,
            D04704, D04705,
            D04706, D04707,
            D04710, D04711,
            D04712, D04713,
            D04714, D04715
        )


    # --------------------------------
    # 2. ON / OFF 이상 여부 판단
    # --------------------------------

    ON_Err = Head_Injection_ON_Err(
        HDn_Pn_Inj_On_T
    )

    OFF_Err = Head_Injection_OFF_Err(
        HDn_Pn_Inj_Off_T
    )


    # --------------------------------
    # 3. 각 위치에 해당하는 Relay ID
    # --------------------------------

    relay_id = np.array([
        ["P00279", "P0028D"],
        ["P0027B", "P0027C"],
        ["P0027E", "P0028E"],
        ["P00280", None]
    ], dtype=object)


    # --------------------------------
    # 4. Warning 생성
    # --------------------------------

    relay_warning = {}

    for i in range(4):
        for j in range(2):

        # 실제 펌프가 없는 (3,1)은 제외
            if relay_id[i, j] is None:
                continue

        # ON 또는 OFF 시간 이상
            if ON_Err[i, j] == 1 or OFF_Err[i, j] == 1:

                head_num = i + 1
                pump_num = j + 1

                relay_warning[relay_id[i, j]] = {
                    "type": "timing",
                    "message": f"헤드{head_num} P{pump_num} 인젝션 (로드) 작동 시간이 길어지고 있습니다."
                }


    # --------------------------------
    # 5. 최종 출력
    # --------------------------------

    result = {
        "relay_warning": relay_warning
    }

    return result


result = Injection_Relay_Warning(
    10, 5,     # Head1 P1 정상
    18, 6,     # Head1 P2 ON 이상
    12, 11,    # Head2 P1 OFF 이상
    20, 12,    # Head2 P2 ON/OFF 이상
    8, 4,      # Head3 P1 정상
    17, 9,     # Head3 P2 ON 이상
    16, 10     # Head4 P1 OFF 이상
)

print(result)