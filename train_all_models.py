import os
import pandas as pd
import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor
import joblib

# 사용자 정의 모듈 (기존 환경에 맞게 유지)
from Log_Extractor import LogExtractor
from pump_config import PUMP_TO_TANK
from Pump_AE import PumpAutoencoder

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

    # 1. 블록(Block) ID 부여
    df_reset['Wagon_Changed'] = df_reset[tag_Wagon].diff() != 0
    df_reset['BuildUp_Changed'] = df_reset[tag_BuildUp].diff() != 0
    df_reset['Wagon_Block_ID'] = (df_reset['Wagon_Changed'] | df_reset['BuildUp_Changed']).cumsum()

    # 2. 연속 시계열 파생 변수 생성 (자르기 전!)
    shot_sv_max = df_reset[df_reset[tag_BuildUp] == 1].groupby('Wagon_Block_ID')[tag_SV].max()
    prev_sv_map = shot_sv_max.shift(1).fillna(0)
    
    df_reset['Prev_SV'] = df_reset['Wagon_Block_ID'].map(prev_sv_map)
    df_reset['Prev_SV'] = df_reset['Prev_SV'].ffill().fillna(0)
    df_reset['Prev_SV_Diff'] = df_reset[tag_SV] - df_reset['Prev_SV']

    df_reset['Instant_SV_Diff'] = df_reset[tag_SV].diff().fillna(0)
    df_reset['Phase_Transition'] = np.where(df_reset['Instant_SV_Diff'] != 0, 1.0, 0.0)
    df_reset['Rolling_PT_Max_3'] = df_reset[tag_PT].rolling(window=3, min_periods=1).max()
    df_reset['Rolling_PT_Diff_3'] = df_reset[tag_PT].diff(periods=2).fillna(0)

    # 3. 진짜 가동 샷 추출 (BuildUp == 1)
    df_shots = df_reset[df_reset[tag_BuildUp] == 1].copy()

    # 4. 샷(Shot) 내부 전용 피처 (인덱스 & 누적합)
    model_df = df_shots.copy()
    model_df['Tick_Index'] = model_df.groupby('Wagon_Block_ID').cumcount()
    model_df['Phase_Start'] = np.where(model_df['Tick_Index'] < 2, 1.0, 0.0)
    model_df['Phase_Steady'] = np.where(model_df['Tick_Index'] >= 2, 1.0, 0.0)

    model_df['Instant_FT_Error'] = model_df[tag_SV] - model_df[tag_FT]
    model_df['Instant_FT_Error_Rate'] = np.where(
        model_df[tag_SV] > 0, 
        (model_df['Instant_FT_Error'] / model_df[tag_SV]) * 100, 
        0
    )
    model_df['Cum_FT_Error'] = model_df.groupby('Wagon_Block_ID')['Instant_FT_Error'].cumsum()

    # 5. 최종 컬럼 정리
    feature_cols = [
        tag_SV, 'Prev_SV', 'Prev_SV_Diff', 
        tag_Ana, tag_Temp,                
        tag_PT, 'Rolling_PT_Max_3', 'Rolling_PT_Diff_3', 
        tag_FT, 'Instant_FT_Error_Rate', 'Cum_FT_Error',  
        'Phase_Start', 'Phase_Steady', 'Phase_Transition'             
    ]
    
    model_df = model_df.fillna(0)
    return model_df[feature_cols], feature_cols

if __name__ == "__main__":
    # ---------------------------------------------------------
    # 📁 0. 저장할 폴더 미리 생성 (밥 먹는 동안 에러 방지)
    # ---------------------------------------------------------
    for folder in ["anomaly_models", "Ana_models", "PT_models"]:
        os.makedirs(folder, exist_ok=True)
        
    extract = LogExtractor()
    
    # 학습/검증 시간대 설정
    train_start = "2026-04-13T01:00:00Z"
    train_end = "2026-04-17T15:00:00Z"
    valid_start = "2026-04-20T01:00:00Z"
    valid_end = "2026-04-20T09:00:00Z"
    
    pump_list = ["P3", "P4", "P5", "P6", "P7", "I1", "I2", "I3", "I4", "I5", "I6", "I7"]

    print("🚀 [전체 통합 파이프라인] 총 14개 펌프 x 3개 모델 = 42개 모델 동시 학습 시작!")
    print("=" * 80)

    for pid in pump_list:
        print(f"\n==================== [ {pid} 펌프 작업 시작 ] ====================")
        pump_info = PUMP_TO_TANK.get(pid, {})
        tank_id = pump_info.get("Tank", pid)
        robot_id = pump_info.get("Robot", None)

        target_tags = [
            f'Pump_BuildUp_{pid}' , f'{robot_id}_Robot_Num', 
            f'g_s_SV_{pid}', f'Ana_Out_{pid}', 
            f'Scale_Out___PT_{pid}', f'Scale_Out___FT_{pid}', 
            f'TK_Temp_PV_{tank_id}', f'TK_Level_PV_{tank_id}'
        ]

        # ---------------------------------------------------------
        # 💾 1. 데이터 추출 및 전처리 (단 한 번만 실행!)
        # ---------------------------------------------------------
        print(f"📥 [{pid}] 데이터 다운로드 중 (가장 오래 걸리는 작업)...")
        raw_train_df = extract.get_data(start_time=train_start, end_time=train_end, target_tags=target_tags)
        raw_valid_df = extract.get_data(start_time=valid_start, end_time=valid_end, target_tags=target_tags)
        
        train_df, feature_cols = prepare_pump_features(raw_train_df, pid, tank_id, robot_id)
        valid_df, _ = prepare_pump_features(raw_valid_df, pid, tank_id, robot_id)
        
        print(f"✅ [{pid}] 데이터 전처리 완료! Train: {len(train_df)}틱, Valid: {len(valid_df)}틱")

        # ---------------------------------------------------------
        # 🧠 2. 이상 탐지 모델 (AutoEncoder) 학습
        # ---------------------------------------------------------
        print(f"\n▶️ [{pid} - Model 1/3] AutoEncoder 이상 탐지 모델 학습")
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(train_df)
        X_valid_scaled = scaler.transform(valid_df)

        scaler_path = f"anomaly_models/scaler_{pid}.pkl"
        joblib.dump(scaler, scaler_path)

        train_tensor = torch.FloatTensor(X_train_scaled)
        valid_tensor = torch.FloatTensor(X_valid_scaled)

        train_loader = DataLoader(TensorDataset(train_tensor, train_tensor), batch_size=64, shuffle=True)
        valid_loader = DataLoader(TensorDataset(valid_tensor, valid_tensor), batch_size=64, shuffle=False)

        model_ae = PumpAutoencoder(input_dim=len(feature_cols))
        criterion = nn.MSELoss()
        optimizer = optim.Adam(model_ae.parameters(), lr=0.005)

        epochs = 200
        best_val_loss = float('inf')
        model_ae_path = f"anomaly_models/autoencoder_{pid}.pth"

        for epoch in range(epochs):
            model_ae.train()
            total_train_loss = 0
            for batch_x, batch_y in train_loader:
                optimizer.zero_grad()
                outputs = model_ae(batch_x)
                loss = criterion(outputs, batch_y)
                loss.backward()
                optimizer.step()
                total_train_loss += loss.item()
            
            avg_train_loss = total_train_loss / len(train_loader)
            
            model_ae.eval()
            total_val_loss = 0
            with torch.no_grad():
                for batch_x, batch_y in valid_loader:
                    outputs = model_ae(batch_x)
                    loss = criterion(outputs, batch_y)
                    total_val_loss += loss.item()
            avg_val_loss = total_val_loss / len(valid_loader)
            
            if avg_val_loss < best_val_loss:
                best_val_loss = avg_val_loss
                torch.save(model_ae.state_dict(), model_ae_path)
                
        print(f"✅ [{pid}] AE 저장 완료 (Best Val Loss: {best_val_loss:.4f})")

        # ---------------------------------------------------------
        # ⚡ 3. 가상 센서 (Ana_Out 예측) 학습
        # ---------------------------------------------------------
        print(f"\n▶️ [{pid} - Model 2/3] Ana_Out 가상 센서 (RandomForest) 학습")
        ana_features = [
            f'Scale_Out___PT_{pid}', 'Rolling_PT_Max_3', 'Rolling_PT_Diff_3', 
            f'Scale_Out___FT_{pid}', f'TK_Temp_PV_{tank_id}', 'Phase_Start', 'Phase_Steady', 'Phase_Transition'
        ]
        X_train_ana = train_df[ana_features]
        y_train_ana = train_df[f'Ana_Out_{pid}']

        model_ana = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
        model_ana.fit(X_train_ana, y_train_ana)
        
        joblib.dump(model_ana, f'Ana_models/virtual_sensor_{pid}.pkl')
        print(f"✅ [{pid}] Ana_Out 모델 저장 완료! (사용 피처 수: {len(ana_features)}개)")


        # ---------------------------------------------------------
        # 🗜️ 4. 압력 예측 (PT 예측) 학습
        # ---------------------------------------------------------
        print(f"\n▶️ [{pid} - Model 3/3] Scale_Out_PT 가상 센서 (RandomForest) 학습")
        pt_features = [
            f'g_s_SV_{pid}', 'Prev_SV', 'Prev_SV_Diff', f'Ana_Out_{pid}', f'TK_Temp_PV_{tank_id}',
            f'Scale_Out___FT_{pid}', 'Instant_FT_Error_Rate', 'Cum_FT_Error',  
            'Phase_Start', 'Phase_Steady', 'Phase_Transition'
        ]
        X_train_pt = train_df[pt_features]
        y_train_pt = train_df[f'Scale_Out___PT_{pid}']

        model_pt = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
        model_pt.fit(X_train_pt, y_train_pt)
        
        joblib.dump(model_pt, f'PT_models/virtual_sensor_{pid}.pkl')
        print(f"✅ [{pid}] PT 예측 모델 저장 완료! (사용 피처 수: {len(pt_features)}개)")

        print(f"🎉 [{pid}] 펌프에 대한 3개 모델 구축 완료!")
        print("-" * 80)

    print("\n🏆 모든 펌프(총 14대)의 42개 모델 학습 및 저장이 완벽하게 끝났습니다! 식사 맛있게 하셨나요?")

