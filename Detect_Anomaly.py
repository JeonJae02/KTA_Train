# Detect_Anomaly.py
import torch
import numpy as np
import pandas as pd

# ==========================================
# 🚨 함수 1: 실시간 이상 탐지 (점수 계산)
# ==========================================
def detect_anomaly(model, scaler, tick_df, feature_cols):
    """
    들어온 1틱의 데이터에 대해 건강도(오차 점수)와 변수별 오차량을 계산합니다.
    """
    model.eval() # 추론 모드 (Dropout 등 비활성화)
    with torch.no_grad(): # 실시간 추론 시에는 기울기 계산을 꺼서 메모리와 속도를 아낍니다.
        # 1. 스케일링
        scaled_data = scaler.transform(tick_df[feature_cols])
        tensor_data = torch.FloatTensor(scaled_data)
        
        # 2. 모델 복원
        reconstructed = model(tensor_data)
        
        # 3. 전체 오차(MSE) 계산 -> 이것이 '이상 점수(Anomaly Score)'가 됩니다.
        mse_loss = torch.mean((tensor_data - reconstructed) ** 2).item()
        
        # 4. 변수별 절대 오차 계산 (나중에 범인 잡을 때 씁니다)
        # 차원(1, 11)을 1차원(11,)으로 펴줍니다.
        feature_errors = torch.abs(tensor_data - reconstructed).numpy()[0] 
        
    return mse_loss, feature_errors


# ==========================================
# 📦 함수 2: 문맥 데이터 추출 (Context Window)
# ==========================================
def extract_context_data(buffer_df, current_idx, lookback=5):
    """
    이상이 발생했을 때, 룰 기반 판단을 위해 '해당 틱 포함 이전 N개의 틱'을 묶어서 가져옵니다.
    실시간 환경이므로 미래 데이터는 볼 수 없고, 직전 궤적(Lookback)을 봅니다.
    """
    # 버퍼(최근 데이터가 쌓이는 곳)에서 현재 인덱스 기준 lookback만큼 가져옵니다.
    start_idx = max(0, current_idx - lookback + 1)
    context_df = buffer_df.iloc[start_idx : current_idx + 1].copy()
    
    return context_df


# ==========================================
# 🕵️‍♂️ 함수 3: 범인 색출 (Feature Attribution)
# ==========================================
def identify_culprit(feature_errors, feature_cols, top_k=3):
    """
    변수별 오차량을 바탕으로 이번 이상 수치에 가장 크게 기여한 Top K 범인을 뽑아냅니다.
    """
    total_error = np.sum(feature_errors)
    if total_error == 0:
        return pd.DataFrame() # 에러가 0이면 범인도 없음
    
    # 각 변수가 전체 에러에서 차지하는 지분(%) 계산
    contributions = (feature_errors / total_error) * 100
    
    # 보기 좋게 데이터프레임으로 묶고 기여도 순으로 정렬
    culprit_df = pd.DataFrame({
        'Feature': feature_cols,
        'Error_Contribution(%)': contributions
    }).sort_values(by='Error_Contribution(%)', ascending=False)
    
    # 가장 심각한 놈들(top_k)만 리턴
    return culprit_df.head(top_k)