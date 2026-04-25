import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from Log_Extractor import LogExtractor
from pump_config import PUMP_TO_TANK
import joblib
from sklearn.ensemble import RandomForestRegressor
from Pump_Preprocess import RealTimePreprocessor
from sklearn.preprocessing import StandardScaler
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset
from Pump_AE import PumpAutoencoder
import os


def prepare_pump_features(raw_df, pump_id, tank_id, robot_id):
    """
    [최종본] 모든 시계열 상태(Prev_SV 포함)를 연속된 시간선 위에서 먼저 계산하고,
    마지막에 가동(BuildUp==1) 구간만 추출합니다.
    """
    df = raw_df.ffill().fillna(0).copy()
    df_reset = df.reset_index() 
    
    tag_SV = f'g_s_SV_{pump_id}'
    tag_Ana = f'Ana_Out_{pump_id}'
    tag_PT = f'Scale_Out___PT_{pump_id}'
    tag_FT = f'Scale_Out___FT_{pump_id}'
    tag_Temp = f'TK_Temp_PV_{tank_id}'
    tag_BuildUp = f'Pump_BuildUp_{pump_id}' 
    tag_Wagon = f'{robot_id}_Robot_Num'

    # ==========================================
    # ✂️ 1. 블록(Block) ID 부여 (전체 연속 시간 기준)
    # ==========================================
    # 쉬는 시간(BuildUp=0)과 가동 시간(BuildUp=1)이 모두 고유한 블록 ID를 가집니다.
    df_reset['Wagon_Changed'] = df_reset[tag_Wagon].diff() != 0
    df_reset['BuildUp_Changed'] = df_reset[tag_BuildUp].diff() != 0
    df_reset['Wagon_Block_ID'] = (df_reset['Wagon_Changed'] | df_reset['BuildUp_Changed']).cumsum()

    # ==========================================
    # 🌍 2. 연속 시계열 파생 변수 생성 (자르기 전!)
    # ==========================================
    # [Prev_SV 연속 계산 로직]
    # 가동 구간(BuildUp==1)인 블록들의 최대 SV만 모아서 구함
    shot_sv_max = df_reset[df_reset[tag_BuildUp] == 1].groupby('Wagon_Block_ID')[tag_SV].max()
    
    # 1칸 당겨서 '이전 가동 샷의 SV' 생성
    prev_sv_map = shot_sv_max.shift(1).fillna(0)
    
    # 전체 데이터에 매핑 (실시간 전처리기에서 self.prev_sv를 들고 있는 것과 동일한 효과)
    df_reset['Prev_SV'] = df_reset['Wagon_Block_ID'].map(prev_sv_map)
    # 비가동 구간은 NaN이 되므로, 바로 이전 가동 샷의 값을 그대로 끌고 옴 (Forward Fill)
    df_reset['Prev_SV'] = df_reset['Prev_SV'].ffill().fillna(0)
    
    # 차이 계산
    df_reset['Prev_SV_Diff'] = df_reset[tag_SV] - df_reset['Prev_SV']

    # 나머지 연속 차분 변수들
    df_reset['Instant_SV_Diff'] = df_reset[tag_SV].diff().fillna(0)
    df_reset['Phase_Transition'] = np.where(df_reset['Instant_SV_Diff'] != 0, 1.0, 0.0)
    df_reset['Rolling_PT_Max_3'] = df_reset[tag_PT].rolling(window=3, min_periods=1).max()
    df_reset['Rolling_PT_Diff_3'] = df_reset[tag_PT].diff(periods=2).fillna(0)

    # ==========================================
    # 🎯 3. 진짜 가동 샷 추출 (BuildUp == 1)
    # ==========================================
    # 이미 과거의 상태값(Rolling, Prev_SV 등)은 전부 파생변수에 안전하게 담겨있습니다!
    df_shots = df_reset[df_reset[tag_BuildUp] == 1].copy()

    # ==========================================
    # 🔄 4. 샷(Shot) 내부 전용 피처 (인덱스 & 누적합)
    # ==========================================
    model_df = df_shots.copy()
    
    # 샷 안에서 얼마나 진행되었는지는 잘라낸 이후에 계산해야 0부터 시작함
    model_df['Tick_Index'] = model_df.groupby('Wagon_Block_ID').cumcount()
    model_df['Phase_Start'] = np.where(model_df['Tick_Index'] < 2, 1.0, 0.0)
    model_df['Phase_Steady'] = np.where(model_df['Tick_Index'] >= 2, 1.0, 0.0)

    # 누적 오차도 샷 내부에서만 쌓아야 함
    model_df['Instant_FT_Error'] = model_df[tag_SV] - model_df[tag_FT]
    model_df['Instant_FT_Error_Rate'] = np.where(
        model_df[tag_SV] > 0, 
        (model_df['Instant_FT_Error'] / model_df[tag_SV]) * 100, 
        0
    )
    model_df['Cum_FT_Error'] = model_df.groupby('Wagon_Block_ID')['Instant_FT_Error'].cumsum()

    # ==========================================
    # 🧹 5. 최종 컬럼 정리
    # ==========================================
    feature_cols = [
        tag_SV, 'Prev_SV', 'Prev_SV_Diff', 
        tag_Ana, tag_Temp,                
        tag_PT, 'Rolling_PT_Max_3', 'Rolling_PT_Diff_3', 
        tag_FT, 'Instant_FT_Error_Rate', 'Cum_FT_Error',  
        'Phase_Start', 'Phase_Steady', 'Phase_Transition'             
    ]
    
    model_df = model_df.fillna(0)

    print("✅ 동적 피처 생성 함수 완료! (Prev_SV 연속 상태 반영)")
    
    return model_df[feature_cols], feature_cols


if __name__ == "__main__":
    extract = LogExtractor()
    start_time="2026-04-13T01:00:00Z"
    end_time="2026-04-17T15:00:00Z"

    for pid in ["P1", "P2", "P3", "P4", "P5", "P6", "P7", "I1", "I2", "I3", "I4", "I5", "I6", "I7"]:
        pump_info = PUMP_TO_TANK.get(pid, {})
        tank_id = pump_info.get("Tank", pid)
        robot_id = pump_info.get("Robot", None)

        target_tags = [
            f'Pump_BuildUp_{pid}' , f'{robot_id}_Robot_Num', 
            f'g_s_SV_{pid}', f'Ana_Out_{pid}', 
            f'Scale_Out___PT_{pid}', f'Scale_Out___FT_{pid}', 
            f'TK_Temp_PV_{tank_id}', f'TK_Level_PV_{tank_id}'
        ]

        raw_df = extract.get_data(start_time=start_time, end_time=end_time, target_tags=target_tags)
        train_df, feature_cols = prepare_pump_features(raw_df, pid, tank_id, robot_id)
        print(f"✅ {pid} 데이터 준비 완료! 총 {len(train_df)}틱, 피처 수: {len(feature_cols)}")

        rrr_df = extract.get_data(start_time="2026-04-18T01:00:00Z", end_time="2026-04-18T09:00:00Z", target_tags=target_tags)
        valid_df, _ = prepare_pump_features(rrr_df, pid, tank_id, robot_id)

        # ==========================================
        # ⚖️ 4. 스케일링 및 스케일러 저장 (매우 중요!)
        # ==========================================
        scaler = StandardScaler()

        # *중요: 스케일러는 무조건 Train 데이터로만 fit 해야 합니다! (데이터 누수 방지)
        X_train_scaled = scaler.fit_transform(train_df)
        X_valid_scaled = scaler.transform(valid_df) # Valid는 transform만!

        # 💾 배포용 스케일러 저장 (나중에 실시간 추론 스크립트에서 불러옵니다)
        scaler_path = f"anomaly_models/scaler_{pid}.pkl"
        joblib.dump(scaler, scaler_path)
        print(f"💾 스케일러 저장 완료: {scaler_path}")

        # 텐서 및 데이터로더 변환
        train_tensor = torch.FloatTensor(X_train_scaled)
        valid_tensor = torch.FloatTensor(X_valid_scaled)

        train_loader = DataLoader(TensorDataset(train_tensor, train_tensor), batch_size=64, shuffle=True)
        valid_loader = DataLoader(TensorDataset(valid_tensor, valid_tensor), batch_size=64, shuffle=False)

        # 피처 개수(11개)만큼 input_dim 설정
        input_dim = len(feature_cols)
        model = PumpAutoencoder(input_dim)

        # 손실 함수(오차 계산)와 최적화 도구(학습 알고리즘) 설정
        criterion = nn.MSELoss() # 평균 제곱 오차
        optimizer = optim.Adam(model.parameters(), lr=0.005)

        epochs = 200 # epoch을 넉넉하게 줍니다.
        best_val_loss = float('inf')
        model_path = f"anomaly_models/autoencoder_{pid}.pth"

        print(f"\n🏃‍♂️ {pid} 펌프 모델 학습 시작...")
        for epoch in range(epochs):
            # --- Train Phase ---
            model.train()
            total_train_loss = 0
            for batch_x, batch_y in train_loader:
                optimizer.zero_grad()
                outputs = model(batch_x)
                loss = criterion(outputs, batch_y)
                loss.backward()
                optimizer.step()
                total_train_loss += loss.item()
            
            avg_train_loss = total_train_loss / len(train_loader)
            
            # --- Validation Phase ---
            model.eval()
            total_val_loss = 0
            with torch.no_grad():
                for batch_x, batch_y in valid_loader:
                    outputs = model(batch_x)
                    loss = criterion(outputs, batch_y)
                    total_val_loss += loss.item()
                    
            avg_val_loss = total_val_loss / len(valid_loader)
            
            # 최고 성능 갱신 시 모델 저장!
            if avg_val_loss < best_val_loss:
                best_val_loss = avg_val_loss
                # 💾 모델 가중치 저장
                torch.save(model.state_dict(), model_path)
                saved_flag = "⭐ (Best Model Saved!)"
            else:
                saved_flag = ""
                
            if (epoch + 1) % 10 == 0 or epoch == 0:
                print(f"Epoch [{epoch+1:03d}/{epochs}] | Train Loss: {avg_train_loss:.4f} | Val Loss: {avg_val_loss:.4f} {saved_flag}")

        print(f"\n✅ 학습 완료! 최적의 모델이 저장되었습니다: {model_path}")
        print(f"✅ 순수 물리 가상 센서 저장 완료! 이제 아까 그 테스트 코드를 다시 돌려보세요!")