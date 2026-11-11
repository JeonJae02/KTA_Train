# config/Device_config.py

# 🚨 속도 최적화(O(1))를 위한 1:1 정적 매핑 테이블
DEVICE_ADDRESS_MAP = {
    # ================= [ Tank 1 BACK ] =================
    "P00042": {"device_type": "Tank", "device_name": "Tank 1 BACK"},
    "P00255": {"device_type": "Tank", "device_name": "Tank 1 BACK"},
    "P00270": {"device_type": "Tank", "device_name": "Tank 1 BACK"},
    "P00282": {"device_type": "Tank", "device_name": "Tank 1 BACK"},
    "P00220": {"device_type": "Tank", "device_name": "Tank 1 BACK"},
    "P00221": {"device_type": "Tank", "device_name": "Tank 1 BACK"},
    "P00070": {"device_type": "Tank", "device_name": "Tank 1 BACK"},
    "P00071": {"device_type": "Tank", "device_name": "Tank 1 BACK"},
    "P00072": {"device_type": "Tank", "device_name": "Tank 1 BACK"},
    "P00073": {"device_type": "Tank", "device_name": "Tank 1 BACK"},

    # ================= [ 1펌프 BACK ] =================
    "P00030": {"device_type": "Pump", "device_name": "1펌프 BACK"},
    "P00239": {"device_type": "Pump", "device_name": "1펌프 BACK"},
    "P00190": {"device_type": "Pump", "device_name": "1펌프 BACK"},
    "P00191": {"device_type": "Pump", "device_name": "1펌프 BACK"},
    "P00192": {"device_type": "Pump", "device_name": "1펌프 BACK"},

    # ================= [ 3-2 펌프 BACK ] =================
    "P00246": {"device_type": "Pump", "device_name": "3-2 펌프 BACK"},
    "P0004C": {"device_type": "Pump", "device_name": "3-2 펌프 BACK"},
    "P00466": {"device_type": "Pump", "device_name": "3-2 펌프 BACK"},
    "P00467": {"device_type": "Pump", "device_name": "3-2 펌프 BACK"},
    "P00468": {"device_type": "Pump", "device_name": "3-2 펌프 BACK"},

    # ================= [ Tank 2-1 SOFT ] =================
    "P00044": {"device_type": "Tank", "device_name": "Tank 2-1 SOFT"},
    "P00078": {"device_type": "Tank", "device_name": "Tank 2-1 SOFT"},
    "P00079": {"device_type": "Tank", "device_name": "Tank 2-1 SOFT"},
    "P0007A": {"device_type": "Tank", "device_name": "Tank 2-1 SOFT"},
    "P0007B": {"device_type": "Tank", "device_name": "Tank 2-1 SOFT"},
    "P00224": {"device_type": "Tank", "device_name": "Tank 2-1 SOFT"},
    "P00225": {"device_type": "Tank", "device_name": "Tank 2-1 SOFT"},
    "P00257": {"device_type": "Tank", "device_name": "Tank 2-1 SOFT"},
    "P00272": {"device_type": "Tank", "device_name": "Tank 2-1 SOFT"},
    "P00284": {"device_type": "Tank", "device_name": "Tank 2-1 SOFT"},

    # ================= [ 2-1 펌프 SOFT ] =================
    "P00032": {"device_type": "Pump", "device_name": "2-1 펌프 SOFT"},
    "P00196": {"device_type": "Pump", "device_name": "2-1 펌프 SOFT"},
    "P00197": {"device_type": "Pump", "device_name": "2-1 펌프 SOFT"},
    "P00198": {"device_type": "Pump", "device_name": "2-1 펌프 SOFT"},
    "P0023B": {"device_type": "Pump", "device_name": "2-1 펌프 SOFT"},

    # ================= [ Tank 3-1 고탄성 ] =================
    "P00047": {"device_type": "Tank", "device_name": "Tank 3-1 고탄성"},
    "P00084": {"device_type": "Tank", "device_name": "Tank 3-1 고탄성"},
    "P00085": {"device_type": "Tank", "device_name": "Tank 3-1 고탄성"},
    "P00086": {"device_type": "Tank", "device_name": "Tank 3-1 고탄성"},
    "P00087": {"device_type": "Tank", "device_name": "Tank 3-1 고탄성"},
    "P0022A": {"device_type": "Tank", "device_name": "Tank 3-1 고탄성"},
    "P0022B": {"device_type": "Tank", "device_name": "Tank 3-1 고탄성"},
    "P0025A": {"device_type": "Tank", "device_name": "Tank 3-1 고탄성"},
    "P00275": {"device_type": "Tank", "device_name": "Tank 3-1 고탄성"},
    "P00287": {"device_type": "Tank", "device_name": "Tank 3-1 고탄성"},

    # ================= [ 3 펌프 고탄성 ] =================
    "P00036": {"device_type": "Pump", "device_name": "3 펌프 고탄성"},
    "P00203": {"device_type": "Pump", "device_name": "3 펌프 고탄성"},
    "P00204": {"device_type": "Pump", "device_name": "3 펌프 고탄성"},
    "P00205": {"device_type": "Pump", "device_name": "3 펌프 고탄성"},
    "P0023F": {"device_type": "Pump", "device_name": "3 펌프 고탄성"},

    # ================= [ Tank 2-2 HARD ] =================
    "P00046": {"device_type": "Tank", "device_name": "Tank 2-2 HARD"},
    "P00080": {"device_type": "Tank", "device_name": "Tank 2-2 HARD"},
    "P00081": {"device_type": "Tank", "device_name": "Tank 2-2 HARD"},
    "P00082": {"device_type": "Tank", "device_name": "Tank 2-2 HARD"},
    "P00228": {"device_type": "Tank", "device_name": "Tank 2-2 HARD"},
    "P00083": {"device_type": "Tank", "device_name": "Tank 2-2 HARD"},
    "P00229": {"device_type": "Tank", "device_name": "Tank 2-2 HARD"},
    "P00259": {"device_type": "Tank", "device_name": "Tank 2-2 HARD"},
    "P00274": {"device_type": "Tank", "device_name": "Tank 2-2 HARD"},
    "P00286": {"device_type": "Tank", "device_name": "Tank 2-2 HARD"},

    # ================= [ 2-2 펌프 HARD ] =================
    "P00034": {"device_type": "Pump", "device_name": "2-2 펌프 HARD"},
    "P0019C": {"device_type": "Pump", "device_name": "2-2 펌프 HARD"},
    "P0019D": {"device_type": "Pump", "device_name": "2-2 펌프 HARD"},
    "P0019E": {"device_type": "Pump", "device_name": "2-2 펌프 HARD"},
    "P0023D": {"device_type": "Pump", "device_name": "2-2 펌프 HARD"},

    # ================= [ Tank 3-2 CUSH ] =================
    "P00049": {"device_type": "Tank", "device_name": "Tank 3-2 CUSH"},
    "P0008C": {"device_type": "Tank", "device_name": "Tank 3-2 CUSH"},
    "P0008D": {"device_type": "Tank", "device_name": "Tank 3-2 CUSH"},
    "P0008E": {"device_type": "Tank", "device_name": "Tank 3-2 CUSH"},
    "P0008F": {"device_type": "Tank", "device_name": "Tank 3-2 CUSH"},
    "P0022E": {"device_type": "Tank", "device_name": "Tank 3-2 CUSH"},
    "P0022F": {"device_type": "Tank", "device_name": "Tank 3-2 CUSH"},
    "P0025C": {"device_type": "Tank", "device_name": "Tank 3-2 CUSH"},
    "P00277": {"device_type": "Tank", "device_name": "Tank 3-2 CUSH"},
    "P00289": {"device_type": "Tank", "device_name": "Tank 3-2 CUSH"},

    # ================= [ 1-2 펌프 CUSH ] =================
    "P0004A": {"device_type": "Pump", "device_name": "1-2 펌프 CUSH"},
    "P00244": {"device_type": "Pump", "device_name": "1-2 펌프 CUSH"},
    "P00460": {"device_type": "Pump", "device_name": "1-2 펌프 CUSH"},
    "P00461": {"device_type": "Pump", "device_name": "1-2 펌프 CUSH"},
    "P00462": {"device_type": "Pump", "device_name": "1-2 펌프 CUSH"},

    # ================= [ 4펌프 CUSH ] =================
    "P00038": {"device_type": "Pump", "device_name": "4펌프 CUSH"},
    "P00209": {"device_type": "Pump", "device_name": "4펌프 CUSH"},
    "P0020A": {"device_type": "Pump", "device_name": "4펌프 CUSH"},
    "P0020B": {"device_type": "Pump", "device_name": "4펌프 CUSH"},
    "P00241": {"device_type": "Pump", "device_name": "4펌프 CUSH"},

    # ================= [ Tank 1 ISO ] =================
    "P00043": {"device_type": "Tank", "device_name": "Tank 1 ISO"},
    "P00074": {"device_type": "Tank", "device_name": "Tank 1 ISO"},
    "P00075": {"device_type": "Tank", "device_name": "Tank 1 ISO"},
    "P00076": {"device_type": "Tank", "device_name": "Tank 1 ISO"},
    "P00077": {"device_type": "Tank", "device_name": "Tank 1 ISO"},
    "P00222": {"device_type": "Tank", "device_name": "Tank 1 ISO"},
    "P00223": {"device_type": "Tank", "device_name": "Tank 1 ISO"},
    "P00256": {"device_type": "Tank", "device_name": "Tank 1 ISO"},
    "P00283": {"device_type": "Tank", "device_name": "Tank 1 ISO"},
    "P00230": {"device_type": "Tank", "device_name": "Tank 1 ISO"},

    # ================= [ 1펌프 ISO ] =================
    "P00031": {"device_type": "Pump", "device_name": "1펌프 ISO"},
    "P00193": {"device_type": "Pump", "device_name": "1펌프 ISO"},
    "P00194": {"device_type": "Pump", "device_name": "1펌프 ISO"},
    "P00195": {"device_type": "Pump", "device_name": "1펌프 ISO"},
    "P0023A": {"device_type": "Pump", "device_name": "1펌프 ISO"},

    # ================= [ 3-2펌프 ISO ] =================
    "P0004D": {"device_type": "Pump", "device_name": "3-2펌프 ISO"},
    "P00247": {"device_type": "Pump", "device_name": "3-2펌프 ISO"},
    "P00469": {"device_type": "Pump", "device_name": "3-2펌프 ISO"},
    "P0046A": {"device_type": "Pump", "device_name": "3-2펌프 ISO"},
    "P0046B": {"device_type": "Pump", "device_name": "3-2펌프 ISO"},

    # ================= [ Tank 2-2 MDI ] =================
    "P00226": {"device_type": "Tank", "device_name": "Tank 2-2 MDI"},
    "P00227": {"device_type": "Tank", "device_name": "Tank 2-2 MDI"},
    "P0007C": {"device_type": "Tank", "device_name": "Tank 2-2 MDI"},
    "P0007D": {"device_type": "Tank", "device_name": "Tank 2-2 MDI"},
    "P0007E": {"device_type": "Tank", "device_name": "Tank 2-2 MDI"},
    "P0007F": {"device_type": "Tank", "device_name": "Tank 2-2 MDI"},
    "P00258": {"device_type": "Tank", "device_name": "Tank 2-2 MDI"},
    "P00273": {"device_type": "Tank", "device_name": "Tank 2-2 MDI"},
    "P00285": {"device_type": "Tank", "device_name": "Tank 2-2 MDI"},

    # ================= [ 2-1 펌프 MDI ] =================
    "P00033": {"device_type": "Pump", "device_name": "2-1 펌프 MDI"},
    "P00199": {"device_type": "Pump", "device_name": "2-1 펌프 MDI"},
    "P0019A": {"device_type": "Pump", "device_name": "2-1 펌프 MDI"},
    "P0019B": {"device_type": "Pump", "device_name": "2-1 펌프 MDI"},
    "P0023C": {"device_type": "Pump", "device_name": "2-1 펌프 MDI"},

    # ================= [ 2-2 펌프 MDI ] =================
    "P00035": {"device_type": "Pump", "device_name": "2-2 펌프 MDI"},
    "P00200": {"device_type": "Pump", "device_name": "2-2 펌프 MDI"},
    "P00201": {"device_type": "Pump", "device_name": "2-2 펌프 MDI"},
    "P00202": {"device_type": "Pump", "device_name": "2-2 펌프 MDI"},
    "P0023E": {"device_type": "Pump", "device_name": "2-2 펌프 MDI"},

    # ================= [ Tank 3 ISO ] =================
    "P00231": {"device_type": "Tank", "device_name": "Tank 3 ISO"},
    "P00048": {"device_type": "Tank", "device_name": "Tank 3 ISO"},
    "P00088": {"device_type": "Tank", "device_name": "Tank 3 ISO"},
    "P00089": {"device_type": "Tank", "device_name": "Tank 3 ISO"},
    "P0008A": {"device_type": "Tank", "device_name": "Tank 3 ISO"},
    "P0008B": {"device_type": "Tank", "device_name": "Tank 3 ISO"},
    "P0022C": {"device_type": "Tank", "device_name": "Tank 3 ISO"},
    "P0025B": {"device_type": "Tank", "device_name": "Tank 3 ISO"},
    "P00276": {"device_type": "Tank", "device_name": "Tank 3 ISO"},
    "P00288": {"device_type": "Tank", "device_name": "Tank 3 ISO"},

    # ================= [ 1-2 펌프 ISO ] =================
    "P0004B": {"device_type": "Pump", "device_name": "1-2 펌프 ISO"},
    "P00245": {"device_type": "Pump", "device_name": "1-2 펌프 ISO"},
    "P00463": {"device_type": "Pump", "device_name": "1-2 펌프 ISO"},
    "P00464": {"device_type": "Pump", "device_name": "1-2 펌프 ISO"},
    "P00465": {"device_type": "Pump", "device_name": "1-2 펌프 ISO"},

    # ================= [ 3 펌프 ISO ] =================
    "P00037": {"device_type": "Pump", "device_name": "3 펌프 ISO"},
    "P00206": {"device_type": "Pump", "device_name": "3 펌프 ISO"}, # 오타 수정 (P002063 -> P00206)
    "P00207": {"device_type": "Pump", "device_name": "3 펌프 ISO"},
    "P00208": {"device_type": "Pump", "device_name": "3 펌프 ISO"},
    "P00240": {"device_type": "Pump", "device_name": "3 펌프 ISO"},

    # ================= [ 4펌프 ISO ] =================
    "P00039": {"device_type": "Pump", "device_name": "4펌프 ISO"},
    "P0020C": {"device_type": "Pump", "device_name": "4펌프 ISO"},
    "P0020D": {"device_type": "Pump", "device_name": "4펌프 ISO"},
    "P0020E": {"device_type": "Pump", "device_name": "4펌프 ISO"},
    "P00242": {"device_type": "Pump", "device_name": "4펌프 ISO"},

    # ================= [ HYD 1 (유압펌프 1) ] =================
    "P00478": {"device_type": "Head", "device_name": "HYD 1"},
    "P00278": {"device_type": "Head", "device_name": "HYD 1"},
    "P00279": {"device_type": "Head", "device_name": "HYD 1"},
    "P00251": {"device_type": "Head", "device_name": "HYD 1"},
    "P00234": {"device_type": "Head", "device_name": "HYD 1"},
    "P0028D": {"device_type": "Head", "device_name": "HYD 1"},

    # ================= [ HYD 2 (유압펌프 2) ] =================
    "P00235": {"device_type": "Head", "device_name": "HYD 2"},
    "P00252": {"device_type": "Head", "device_name": "HYD 2"},
    "P0027A": {"device_type": "Head", "device_name": "HYD 2"},
    "P0027B": {"device_type": "Head", "device_name": "HYD 2"},
    "P00479": {"device_type": "Head", "device_name": "HYD 2"},

    # ================= [ HYD 3 (유압펌프 3) ] =================
    "P00236": {"device_type": "Head", "device_name": "HYD 3"},
    "P00253": {"device_type": "Head", "device_name": "HYD 3"},
    "P0027D": {"device_type": "Head", "device_name": "HYD 3"},
    "P0027E": {"device_type": "Head", "device_name": "HYD 3"},
    "P0047A": {"device_type": "Head", "device_name": "HYD 3"},
    "P0028E": {"device_type": "Head", "device_name": "HYD 3"},

    # ================= [ HYD 4 (유압펌프 4) ] =================
    "P00237": {"device_type": "Head", "device_name": "HYD 4"},
    "P00254": {"device_type": "Head", "device_name": "HYD 4"},
    "P0027F": {"device_type": "Head", "device_name": "HYD 4"},
    "P00280": {"device_type": "Head", "device_name": "HYD 4"},
    "P0047B": {"device_type": "Head", "device_name": "HYD 4"},

    # ================= [ Line / 컨베이어 ] =================
    "P00470": {"device_type": "Conveyor", "device_name": "Line / 컨베이어"},
    "P00471": {"device_type": "Conveyor", "device_name": "Line / 컨베이어"},
    "P00472": {"device_type": "Conveyor", "device_name": "Line / 컨베이어"},
    "P00473": {"device_type": "Conveyor", "device_name": "Line / 컨베이어"},
    "P00474": {"device_type": "Conveyor", "device_name": "Line / 컨베이어"},
    "P00475": {"device_type": "Conveyor", "device_name": "Line / 컨베이어"},
    "P00476": {"device_type": "Conveyor", "device_name": "Line / 컨베이어"},
    "P00477": {"device_type": "Conveyor", "device_name": "Line / 컨베이어"},
    "P0028A": {"device_type": "Conveyor", "device_name": "Line / 컨베이어"},
    "P0028B": {"device_type": "Conveyor", "device_name": "Line / 컨베이어"},
    "P0028C": {"device_type": "Conveyor", "device_name": "Line / 컨베이어"},
    "P0025D": {"device_type": "Conveyor", "device_name": "Line / 컨베이어"},
    "P0025E": {"device_type": "Conveyor", "device_name": "Line / 컨베이어"},
    "P00250": {"device_type": "Conveyor", "device_name": "Line / 컨베이어"},
    "P00238": {"device_type": "Conveyor", "device_name": "Line / 컨베이어"},
}