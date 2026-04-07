import pandas as pd
import numpy as np
import torch

class RealTimePreprocessor:
    def __init__(self, scaler, feature_cols):
        """
        초기화 단계: 학습할 때 썼던 스케일러와 컬럼 순서를 넘겨받아 저장합니다.
        """
        self.scaler = scaler
        self.feature_cols = feature_cols
        
        # 🧠 상태 기억용 변수들 (Memory)
        self.current_wagon_num = None  # 지금 지나가고 있는 대차 번호
        self.tick_index = -1           # 현재 샷의 틱 카운트
        self.prev_sv = 0.0             # 직전 샷의 SV 값
        self.current_max_sv = 0.0      # 이번 샷에서 관측된 최대 SV
        self.cum_ft_error = 0.0        # 누적 유량 오차
        self.pt_history = []           # 최근 3틱의 PT 값을 담아둘 리스트

        self.last_tick_sv = None

    def process_raw_tick(self, raw_tick):
        """
        1줄짜리 Raw 데이터(Series 또는 Dict)가 들어오면 피처를 추출하고 스케일링합니다.
        """
        # 1. Raw 값 추출
        wagon_num = raw_tick['전진_Actual_Wagon_Num']
        sv = raw_tick['g_s_SV_P1']
        pt = raw_tick['Scale_Out___PT_P1']
        ft = raw_tick['Scale_Out___FT_P1']
        
        # 2. 대차 번호가 바뀌었는지 체크 (새로운 샷의 시작)
        if self.current_wagon_num != wagon_num:
            # 상태 리셋 및 갱신
            self.prev_sv = self.current_max_sv # 이전 샷의 최대 SV를 백업
            self.current_wagon_num = wagon_num
            self.tick_index = 0
            self.current_max_sv = sv
            self.cum_ft_error = 0.0
            self.pt_history = []
        else:
            self.tick_index += 1
            if sv > self.current_max_sv:
                self.current_max_sv = sv
                
        # 3. 실시간 파생 피처(Feature) 계산
        instant_ft_error = sv - ft
        instant_ft_error_rate = (instant_ft_error / sv * 100) if sv > 0 else 0.0
        self.cum_ft_error += instant_ft_error
        
        self.pt_history.append(pt)
        if len(self.pt_history) > 3:
            self.pt_history.pop(0) # 가장 오래된 값 쳐내기 (항상 3개 유지)
            
        rolling_pt_max_3 = max(self.pt_history)
        # 3틱 전 데이터가 없으면 현재 pt와의 차이는 0, 있으면 (현재 - 3틱 전)
        rolling_pt_diff_3 = pt - self.pt_history[0] if len(self.pt_history) > 1 else 0.0

        # ------------------------------------------------
        # [추가] 3.5. 틱(Tick) 기반 Phase(구간) 피처 생성
        # ------------------------------------------------
        # 틱 0~2까지는 과도기(Start), 3부터는 안정기(Steady)로 정의
        phase_start = 1.0 if self.tick_index <= 2 else 0.0
        phase_steady = 1.0 if self.tick_index > 2 else 0.0

        # [신규 추가] 직전 틱 대비 SV 변화량 계산 (Transition 캐치)
        if self.last_tick_sv is None:
            instant_sv_diff = 0.0
        else:
            instant_sv_diff = sv - self.last_tick_sv
            
        phase_transition = 1.0 if instant_sv_diff != 0 else 0.0
        
        # 다음 틱을 위해 현재 SV 값을 기억 장치에 저장
        self.last_tick_sv = sv

        # 4. 모델 입력용 데이터 딕셔너리 조립
        processed_data = {
            'g_s_SV_P1': sv,
            'Prev_SV': self.prev_sv,
            'Prev_SV_Diff': sv - self.prev_sv,
            'Ana_Out_P1': raw_tick['Ana_Out_P1'],
            'TK_Temp_PV_P1': raw_tick['TK_Temp_PV_P1'],
            'Scale_Out___PT_P1': pt,
            'Rolling_PT_Max_3': rolling_pt_max_3,
            'Rolling_PT_Diff_3': rolling_pt_diff_3,
            'Scale_Out___FT_P1': ft,
            'Instant_FT_Error_Rate': instant_ft_error_rate,
            'Cum_FT_Error': self.cum_ft_error,
            'Phase_Start': phase_start,
            'Phase_Steady': phase_steady,
            'Phase_Transition': phase_transition
        }
        
        # 5. 스케일링 (학습 데이터와 동일한 순서 유지)
        df_processed = pd.DataFrame([processed_data])[self.feature_cols]
        scaled_data = self.scaler.transform(df_processed)
        
        # 6. PyTorch 텐서로 변환
        tensor_data = torch.FloatTensor(scaled_data)
        
        # 추론용 텐서, 시각화용 원본 피처 데이터, 메타 정보를 같이 리턴합니다.
        meta_info = {'Wagon_Num': wagon_num, 'Tick_Index': self.tick_index}
        return tensor_data, df_processed, meta_info