"""
    공장이 실제로 작동중인지를 체크해서 결과비트를 리턴하는 클래스.

    필요한 변수: current_time, D01330_4, D01331_4, D01332_4, D01383_4, P0025D
    사용법: 반복문 이전에 객체를 생성하고, 반복문 가장 위에서 update()만 호출하면 됨.

    D01330.4, D01331.4, D01332.4, D01383.4 중 하나라도 1이면
    Injection_On = 1,
    모두 0이면 Injection_On = 0.

    결과비트가 0일 때:
        Injection_On == 1 AND P0025D == 1 상태가
        2분(120초) 이상 지속되면 결과비트를 1로 변경.

    결과비트가 1일 때:
        Injection_On == 0 상태가
        2분(120초) 이상 지속되면 결과비트를 0으로 변경.
        (이때는 CV 고려 x. 인젝션 꺼져도 몇분간은 CV가 돌아가는 경우가 많음)



"""

class Is_Factory_Running:
    def __init__(self):
        self.result_bit = 0

        # 작동 조건이 처음 만족된 시간
        self.start_condition_time = None

        # Injection이 처음 꺼진 시간
        self.injection_off_time = None

        # 기준시간
        self.start_delay_time = 120.0   # 작동 기준시간 [초]
        self.stop_delay_time = 120.0    # 정지 기준시간 [초]


    def update(
        self,
        current_time,
        D01330_4,
        D01331_4,
        D01332_4,
        D01383_4,
        P0025D
    ):

        # 1. Injection_On 계산
        if (
            D01330_4 == 1
            or D01331_4 == 1
            or D01332_4 == 1
            or D01383_4 == 1
        ):
            Injection_On = 1
        else:
            Injection_On = 0


        # 2. 현재 공장이 정지 상태라고 판단 중
        if self.result_bit == 0:

            if Injection_On == 1 and P0025D == 1:

                # 작동 조건을 처음 만족한 시점 저장
                if self.start_condition_time is None:
                    self.start_condition_time = current_time

                # 작동 기준시간 이상 유지되었는지 확인
                elif (
                    current_time - self.start_condition_time
                    >= self.start_delay_time
                ):
                    self.result_bit = 1
                    self.start_condition_time = None

            else:
                # 조건이 중간에 깨지면 작동 타이머 초기화
                self.start_condition_time = None


        # 3. 현재 공장이 작동 상태라고 판단 중
        else:

            if Injection_On == 0:

                # Injection이 처음 꺼진 시점 저장
                if self.injection_off_time is None:
                    self.injection_off_time = current_time

                # 정지 기준시간 이상 유지되었는지 확인
                elif (
                    current_time - self.injection_off_time
                    >= self.stop_delay_time
                ):
                    self.result_bit = 0
                    self.injection_off_time = None

            else:
                # Injection이 다시 켜지면 정지 타이머 초기화
                self.injection_off_time = None


        return self.result_bit