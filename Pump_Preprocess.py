# tools_yeji/Pump_preprocess.py
import pandas as pd
import time
import torch

class RealTimePreprocessor:
    def __init__(self, scaler, feature_cols, pump_id, tank_id, robot_id):
        self.scaler = scaler
        self.feature_cols = feature_cols
        self.pump_id = pump_id
        self.tank_id = tank_id
        self.robot_id = robot_id

        self.current_wagon_num = None
        self.tick_index = -1
        self.prev_sv = 0.0
        self.current_max_sv = 0.0
        self.cum_ft_error = 0.0
        self.pt_history = []
        self.last_tick_sv = None
        self.last_buildup_state = 0       

    def process_raw_tick(self, raw_tick):
        # 태그 명칭 정의
        tag_SV = f'g_s_SV_{self.pump_id}'
        tag_PT = f'Scale_Out___PT_{self.pump_id}'
        tag_FT = f'Scale_Out___FT_{self.pump_id}'
        tag_Ana = f'Ana_Out_{self.pump_id}'
        tag_Temp = f'TK_Temp_PV_{self.tank_id}'
        tag_Wagon = f'{self.robot_id}_Robot_Num'
        tag_BuildUp = f'Pump_BuildUp_{self.pump_id}'

        # 값 추출
        wagon_num = raw_tick.get(tag_Wagon, 0)
        sv = float(raw_tick.get(tag_SV, 0.0))
        pt = float(raw_tick.get(tag_PT, 0.0))
        ft = float(raw_tick.get(tag_FT, 0.0))
        ana_out = float(raw_tick.get(tag_Ana, 0.0))
        tk_temp = float(raw_tick.get(tag_Temp, 0.0))
        buildup = int(raw_tick.get(tag_BuildUp, 0))

        # [핵심] 상태 갱신 로직: 대차가 바뀌거나, 빌드업이 새로 시작될 때 리셋
        is_new_shot = (self.current_wagon_num != wagon_num) or (self.last_buildup_state == 0 and buildup == 1)
        
        if is_new_shot:
            self.prev_sv = self.current_max_sv # 이전 샷의 최종 SV 저장
            self.current_wagon_num = wagon_num
            self.tick_index = 0
            self.current_max_sv = sv
            self.cum_ft_error = 0.0
            self.pt_history = []
        else:
            self.tick_index += 1
            if sv > self.current_max_sv:
                self.current_max_sv = sv

        self.last_buildup_state = buildup
                
        # 파생 변수 계산
        instant_ft_error = sv - ft
        instant_ft_error_rate = (instant_ft_error / sv * 100) if sv > 0 else 0.0
        self.cum_ft_error += instant_ft_error
        
        self.pt_history.append(pt)
        if len(self.pt_history) > 3:
            self.pt_history.pop(0)
            
        rolling_pt_max_3 = max(self.pt_history) if self.pt_history else 0.0
        rolling_pt_diff_3 = pt - self.pt_history[0] if len(self.pt_history) > 1 else 0.0

        phase_start = 1.0 if self.tick_index < 2 else 0.0
        phase_steady = 1.0 if self.tick_index >= 2 else 0.0

        instant_sv_diff = (sv - self.last_tick_sv) if self.last_tick_sv is not None else 0.0
        phase_transition = 1.0 if instant_sv_diff != 0 else 0.0
        self.last_tick_sv = sv

        # 데이터 조립
        processed_data = {
            tag_SV: sv,
            'Prev_SV': self.prev_sv,
            'Prev_SV_Diff': sv - self.prev_sv,
            tag_Ana: ana_out,
            tag_Temp: tk_temp,
            tag_PT: pt,
            'Rolling_PT_Max_3': rolling_pt_max_3,
            'Rolling_PT_Diff_3': rolling_pt_diff_3,
            tag_FT: ft,
            'Instant_FT_Error_Rate': instant_ft_error_rate,
            'Cum_FT_Error': self.cum_ft_error,
            'Phase_Start': phase_start,
            'Phase_Steady': phase_steady,
            'Phase_Transition': phase_transition
        }
        
        df_processed = pd.DataFrame([processed_data])[self.feature_cols]
        scaled_data = self.scaler.transform(df_processed)
        tensor_data = torch.FloatTensor(scaled_data)
        
        meta_info = {'Wagon_Num': wagon_num, 'Tick_Index': self.tick_index, 'Time': time.time()}
        return tensor_data, df_processed, meta_info