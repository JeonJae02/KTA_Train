import torch
import numpy as np
import pandas as pd

# ==========================================
# 🧠 [모듈 1] Core ML Engine (범용)
# ==========================================
def detect_anomaly(model, scaler, tick_df, feature_cols):
    """들어온 1틱의 데이터에 대해 건강도(오차 점수)와 변수별 오차량을 계산합니다."""
    model.eval() 
    with torch.no_grad():
        scaled_data = scaler.transform(tick_df[feature_cols])
        tensor_data = torch.FloatTensor(scaled_data)
        
        reconstructed = model(tensor_data)
        mse_loss = torch.mean((tensor_data - reconstructed) ** 2).item()
        feature_errors = torch.abs(tensor_data - reconstructed).numpy()[0] 
        
    return mse_loss, feature_errors

def extract_context_data(buffer_df, current_idx, lookback=5):
    """이상이 발생했을 때, 룰 기반 판단을 위해 '해당 틱 포함 이전 N개의 틱'을 확보합니다."""
    start_idx = max(0, current_idx - lookback + 1)
    return buffer_df.iloc[start_idx : current_idx + 1].copy()

def identify_culprit(feature_errors, feature_cols, top_k=3):
    """변수별 오차량을 바탕으로 이번 이상 수치에 가장 크게 기여한 범인을 뽑아냅니다."""
    total_error = np.sum(feature_errors)
    if total_error == 0:
        return pd.DataFrame()
    
    contributions = (feature_errors / total_error) * 100
    
    culprit_df = pd.DataFrame({
        'Feature': feature_cols,
        'Error_Contribution(%)': contributions
    }).sort_values(by='Error_Contribution(%)', ascending=False)
    
    return culprit_df.head(top_k)


# ==========================================
# 📢 [모듈 2] Business Logic Engine (도메인 맞춤형)
# ==========================================
def generate_fact_bomb(culprits_df, context_df, meta_info, pump_id):
    """
    AI가 색출한 범인(Feature)과 현재 상황을 바탕으로, 
    해당 펌프(pump_id)에 맞는 현장 맞춤형 조치 사항을 뱉어냅니다.
    """
    wagon_num = meta_info['Wagon_Num']
    tick_idx = meta_info['Tick_Index']
    
    # 1. 1, 2등 범인 리스트 가져오기
    top_features = culprits_df['Feature'].values[:2]
    top_1_name = culprits_df.iloc[0]['Feature']
    top_1_score = culprits_df.iloc[0]['Error_Contribution(%)']
    
    # 2. 펌프 ID에 따른 동적 태그명 생성 (핵심!)
    tag_SV = f'g_s_SV_{pump_id}'
    tag_Ana = f'Ana_Out_{pump_id}'
    tag_PT = f'Scale_Out___PT_{pump_id}'
    
    # 리포트 헤더
    report = f"📢 [펌프: {pump_id} | 대차: {wagon_num}번 / {tick_idx}틱] 긴급 진단 리포트\n"
    report += "-" * 60 + "\n"
    
    # --------------------------------------------------
    # 🚨 Rule 1. 펌프 슬립 (유량 부족 / 에어 포켓)
    # (파생 변수인 Error_Rate와 Cum_Error는 펌프 이름에 상관없이 공통 명칭 사용)
    # --------------------------------------------------
    if 'Cum_FT_Error' in top_features or 'Instant_FT_Error_Rate' in top_features:
        report += f"💥 [팩트 폭격] {pump_id} 펌프가 심각하게 헛돌고 있습니다!\n"
        report += "👉 [현상] 모터는 지령(SV)대로 쌩쌩 돌고 있는데, 뿜어내는 유량(FT)이 턱없이 부족합니다.\n"
        report += "🛠️ [즉각 조치] \n"
        report += "   1) 원액 탱크 수위가 너무 낮지 않은지 확인하세요. (Starvation)\n"
        report += "   2) 펌프 흡입구 라인에 에어가 찼는지 확인 후 에어 빼기 작업을 진행하세요.\n"
        report += "   3) 빈번하게 발생 시, 내부 기어 마모로 인한 '슬립'이므로 펌프 교체를 준비하세요."
        
    # --------------------------------------------------
    # 🚨 Rule 2. 제어 시퀀스 꼬임 (선행 지령 / 잔압 충돌)
    # --------------------------------------------------
    elif tick_idx <= 1 and ('Prev_SV' in top_features or tag_SV in top_features or tag_Ana in top_features):
        report += f"💥 [팩트 폭격] {pump_id} 펌프의 앞/뒤 대차 간 제어 타이밍이 꼬였습니다!\n"
        report += f"👉 [현상] 대차가 진입하는 극초반({tick_idx}틱)에 이전 샷의 지령과 현재 샷의 지령이 충돌하며 맥동이 발생했습니다.\n"
        report += "🛠️ [즉각 조치] \n"
        report += f"   1) PLC 제어 담당자 호출! {wagon_num}번 대차 진입 전후의 인버터 감속/가속 로직을 확인하세요.\n"
        report += "   2) 이전 샷의 잔압이 충분히 빠지기 전에 밸브가 너무 일찍 열리는지 타이밍을 체크하세요."

    # --------------------------------------------------
    # 🚨 Rule 3. 압력 붕괴 (조루 현상 / 배관 이상)
    # --------------------------------------------------
    elif tag_PT in top_features or 'Rolling_PT_Max_3' in top_features:
        report += f"💥 [팩트 폭격] {pump_id} 펌프의 토출 압력(PT)이 비정상적으로 무너지거나 솟구쳤습니다!\n"
        report += "👉 [현상] 지령(SV)은 그대로인데 물리적인 압력이 갑자기 떨어지거나 치솟아 일관성이 깨졌습니다.\n"
        report += "🛠️ [즉각 조치] \n"
        report += "   1) 토출 밸브가 레시피가 끝나기도 전에 먼저 닫혀버리는지 밸브 타이밍을 점검하세요.\n"
        report += "   2) 압력이 급증했다면 믹싱 헤드나 말단 필터 쪽에 이물질 막힘이 없는지 확인하세요."

    # --------------------------------------------------
    # 🚨 Rule 4. 기타 미분류 이상
    # --------------------------------------------------
    else:
        report += f"💥 [팩트 폭격] {pump_id} 펌프에서 평소와 다른 복합적인 패턴 붕괴가 감지되었습니다.\n"
        report += f"👉 [현상] 현재 물리 법칙 붕괴에 가장 큰 책임이 있는 변수는 '{top_1_name}' (기여도 {top_1_score:.1f}%) 입니다.\n"
        report += "🛠️ [즉각 조치] 해당 시간대의 그래프(Time Machine View)를 띄워 작업자와 엔지니어가 함께 확인하세요."
        
    return report