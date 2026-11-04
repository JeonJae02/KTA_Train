# tools_yeji/pump_models
"""펌프 사이클 모델 — 판정만. 얼마나 오래 띄울지(근무 단위 유지)는 tools_yeji/Pump_Monitor.

    pump_params      설정·자산 경로(models/pump_cycle)·태그·점도·기준선 읽기
    pump_preprocess  틱 → 사이클 상태기계(PumpProcessor), 사이클 피처, 기준선 피처
    pump_judge       채점(CycleScorer) · 창 판정·알림(WindowTracker)
    pump_diagnose    규칙표·문구표 · 진단(PumpDiagnoser) → 경보 목록
    pump_realtime    14대 직렬 묶음(PumpRealtimeGroup) — Pump_Monitor 가 갖고 매 tick 부른다
"""
# 자동 생성 — DB_Log/HDI_MODEL/relay_port/make_port.py. 원본: HDI_MODEL/relay_port/make_port.py
