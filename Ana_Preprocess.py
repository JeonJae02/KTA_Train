import pandas as pd
import numpy as np

class VirtualSensorPreprocessor:
    def __init__(self, feature_cols, pump_id='P1', tank_id='P1', robot_id='RB1'):
        """
        초기화 단계: 가상 센서가 사용하는 컬럼 순서를 저장합니다.
        """
        self.feature_cols = feature_cols
        self.pump_id = pump_id
        self.tank_id = tank_id
        self.robot_id = robot_id
        
        # 🧠 상태 기억용 변수들 (Memory)
        self.current_robot_num = None  # 전진_Actual_Wagon_Num 대신 RB1_Robot_Num 사용
        self.is_running = False        # Pump_BuildUp 상태 추적
        
        self.tick_index = -1
        self.prev_sv = 0.0
        self.current_max_sv = 0.0
        self.cum_ft_error = 0.0
        self.pt_history = []
        self.last_tick_sv = None

    def process_raw_tick(self, raw_tick):
        """
        1줄짜리 Raw 데이터가 들어오면 피처를 추출합니다.
        Pump_BuildUp이 0이면 가동 중이 아니므로 None을 반환합니다.
        """
        buildup = raw_tick.get(f'Pump_BuildUp_{self.pump_id}', 0)
        
        # 🚨 가동 중이 아닐 때는 연산 패스
        if buildup == 0:
            self.is_running = False
            return None, None

        raw_ana_out = raw_tick.get(f'Ana_Out_{self.pump_id}')
        if pd.isna(raw_ana_out):
            return None, None
        
        # 1. Raw 값 추출
        robot_num = raw_tick.get(f'{self.robot_id}_Robot_Num', 0)
        sv = float(raw_tick.get(f'g_s_SV_{self.pump_id}', 0.0))
        pt = float(raw_tick.get(f'Scale_Out___PT_{self.pump_id}', 0.0))
        ft = float(raw_tick.get(f'Scale_Out___FT_{self.pump_id}', 0.0))
        
        # 2. 새로운 샷의 시작점 캐치 (BuildUp이 방금 1이 되었거나, 로봇 번호가 바뀜)
        if not self.is_running or self.current_robot_num != robot_num:
            self.prev_sv = self.current_max_sv
            self.current_robot_num = robot_num
            self.tick_index = 0
            self.current_max_sv = sv
            self.cum_ft_error = 0.0
            self.pt_history = []
            self.is_running = True
        else:
            self.tick_index += 1
            if sv > self.current_max_sv:
                self.current_max_sv = sv
                
        # 3. 실시간 파생 피처 계산
        instant_ft_error = sv - ft
        instant_ft_error_rate = (instant_ft_error / sv * 100) if sv > 0 else 0.0
        self.cum_ft_error += instant_ft_error
        
        self.pt_history.append(pt)
        if len(self.pt_history) > 3:
            self.pt_history.pop(0)
            
        rolling_pt_max_3 = max(self.pt_history)
        rolling_pt_diff_3 = pt - self.pt_history[0] if len(self.pt_history) > 1 else 0.0

        phase_start = 1.0 if self.tick_index <= 2 else 0.0
        phase_steady = 1.0 if self.tick_index > 2 else 0.0

        if self.last_tick_sv is None:
            instant_sv_diff = 0.0
        else:
            instant_sv_diff = sv - self.last_tick_sv
            
        phase_transition = 1.0 if instant_sv_diff != 0 else 0.0
        self.last_tick_sv = sv

        # 4. 모델 입력용 데이터 조립
        processed_data = {
            f'g_s_SV_{self.pump_id}': sv,
            'Prev_SV': self.prev_sv,
            'Prev_SV_Diff': sv - self.prev_sv,
            f'Ana_Out_{self.pump_id}': raw_tick.get(f'Ana_Out_{self.pump_id}', 0.0),
            f'TK_Temp_PV_{self.tank_id}': raw_tick.get(f'TK_Temp_PV_{self.tank_id}', 0.0), # 탱크 온도는 임시 하드코딩
            f'Scale_Out___PT_{self.pump_id}': pt,
            'Rolling_PT_Max_3': rolling_pt_max_3,
            'Rolling_PT_Diff_3': rolling_pt_diff_3,
            f'Scale_Out___FT_{self.pump_id}': ft,
            'Instant_FT_Error_Rate': instant_ft_error_rate,
            'Cum_FT_Error': self.cum_ft_error,
            'Phase_Start': phase_start,
            'Phase_Steady': phase_steady,
            'Phase_Transition': phase_transition
        }
        
        df_processed = pd.DataFrame([processed_data])[self.feature_cols]
        meta_info = {'Robot_Num': robot_num, 'Tick_Index': self.tick_index}
        
        return df_processed, meta_info