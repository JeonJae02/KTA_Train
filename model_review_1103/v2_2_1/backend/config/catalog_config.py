SENSOR_CATALOG = {
  "Macro_Process_Sensors": {
    "Function_Based": {
      "Temperture":[
        {"TK_Temp_PV_P1":"D00500"},
        {"TK_Temp_PV_I1":"D00540"}
      ],
      "Pressure_ISO": [
        {"Scale_Out___PT_I1":"D00635"},
        {"Scale_Out___PT_I2":"D00641"},
        {"Scale_Out___PT_I3":"D00647"},
        {"Scale_Out___PT_I4":"D02665"},
        {"Scale_Out___PT_I5":"D02671"},
        {"Scale_Out___PT_I6":"D02689"},
        {"Scale_Out___PT_I7":"D02711"}
      ],
      "pressure_POL": [
        {"Scale_Out___PT_P1":"D00605"},
        {"Scale_Out___PT_P2":"D00611"},
        {"Scale_Out___PT_P3":"D00617"},
        {"Scale_Out___PT_P4":"D00623"},
        {"Scale_Out___PT_P5":"D02653"},
        {"Scale_Out___PT_P6":"D02659"},
        {"Scale_Out___PT_P7":"D02677"}
      ],
      "Flowrate_ISO": [
        {"Scale_Out___FT_I1":"D00677"},
        {"Scale_Out___FT_I2":"D00683"},
        {"Scale_Out___FT_I3":"D00689"},
        {"Scale_Out___FT_I4":"D00695"},
        {"Scale_Out___FT_I5":"D01093"},
        {"Scale_Out___FT_I6":"D01099"},
        {"Scale_Out___FT_I7":"D01613"}
      ],
      "Flowrate_POL": [
        {"Scale_Out___FT_P1":"D00653"},
        {"Scale_Out___FT_P2":"D00659"},
        {"Scale_Out___FT_P3":"D00665"},
        {"Scale_Out___FT_P4":"D00671"},
        {"Scale_Out___FT_P5":"D01081"},
        {"Scale_Out___FT_P6":"D01087"},
        {"Scale_Out___FT_P7":"D01601"}
      ],
      "Set_Flowrate_ISO": [
        {"g_s_SV_I1":"D00840"},
        {"g_s_SV_I2":"D00850"},
        {"g_s_SV_I3":"D00860"},
        {"g_s_SV_I4":"D00870"},
        {"g_s_SV_I5":"D01820"},
        {"g_s_SV_I6":"D01830"}, 
        {"g_s_SV_I7":"D01860"}
      ],
      "Set_Flowrate_POL": [
        {"g_s_SV_P1":"D00800"},
        {"g_s_SV_P2":"D00810"},
        {"g_s_SV_P3":"D00820"},
        {"g_s_SV_P4":"D00830"},
        {"g_s_SV_P5":"D01800"},
        {"g_s_SV_P6":"D01810"},
        {"g_s_SV_P7":"D01840"}
      ]
    },
    "Location_Based": {
      "워킹 Tank 1": [
        {"TK_Temp_PV_P1":"D00500"}
      ],
      "워킹 Tank 1 ISO": [
        {"TK_Temp_PV_I1":"D00540"}
      ],
      "1-1펌프 BACK": [
        {"Scale_Out___PT_P1": "D00605"},
        {"Scale_Out___FT_P1": "D00653"},
        {"g_s_SV_P1": "D00800"}
      ],
      "1-1펌프 ISO": [
        {"Scale_Out___PT_I1": "D00635"},
        {"Scale_Out___FT_I1": "D00677"},
        {"g_s_SV_I1": "D00840"}
        
      ],
      "1-2펌프 CUSH": [
        {"Scale_Out___PT_P2": "D00611"},
        {"Scale_Out___FT_P2": "D00659"},
        {"g_s_SV_P2": "D00810"}
      ],
      "1-2펌프 ISO": [
        {"Scale_Out___PT_I2": "D00641"},
        {"Scale_Out___FT_I2": "D00683"},
        {"g_s_SV_I2": "D00850"}
      ],
      "2-1펌프 SOFT": [
        {"Scale_Out___PT_P3": "D00617"},
        {"Scale_Out___FT_P3": "D00665"},
        {"g_s_SV_P3": "D00820"}
      ],
      "2-1펌프 MDI": [
        {"Scale_Out___PT_I3": "D00647"},
        {"Scale_Out___FT_I3": "D00689"},
        {"g_s_SV_I3": "D00860"}
      ],
      "2-2펌프 HARD": [
        {"Scale_Out___PT_P4": "D00623"},
        {"Scale_Out___FT_P4": "D00671"},
        {"g_s_SV_P4": "D00830"}
      ],
      "2-2펌프 MDI": [
        {"Scale_Out___PT_I4": "D02665"},
        {"Scale_Out___FT_I4": "D00695"},
        {"g_s_SV_I4": "D00870"}
      ],
      "3펌프 고탄성": [
        {"Scale_Out___PT_P5": "D02653"},
        {"Scale_Out___FT_P5": "D01081"},
        {"g_s_SV_P5": "D01800"}
      ],
      "3펌프 ISO": [
        {"Scale_Out___PT_I5": "D02671"},
        {"Scale_Out___FT_I5": "D01093"},
        {"g_s_SV_I5": "D01820"}
      ],
      "3-2펌프 BACK": [
        {"Scale_Out___PT_P6": "D02659"},
        {"Scale_Out___FT_P6": "D01087"},
        {"g_s_SV_P6": "D01810"}
      ],
      "3-2펌프 ISO": [
        {"Scale_Out___PT_I6": "D02689"},
        {"Scale_Out___FT_I6": "D01099"},
        {"g_s_SV_I6": "D01830"}
      ],
      "4펌프 CUSH": [
        {"Scale_Out___PT_P7": "D02677"},
        {"Scale_Out___FT_P7": "D01601"},
        {"g_s_SV_P7": "D01840"}
      ],
      "4펌프 ISO": [
        {"Scale_Out___PT_I7": "D02711"},
        {"Scale_Out___FT_I7": "D01613"},
        {"g_s_SV_I7": "D01860"}
      ]
    }
  },

  "Micro_Device_Sensors": {
    "Function_Based": {
      "교반기_Trip": [
        {"P00042": "워킹 Tank 1 BACK 교반기 Trip"},
        {"P00043": "워킹 Tank 1 ISO 교반기 Trip"},
        {"P00044": "워킹 Tank 2-1 SOFT 교반기 Trip"},
        {"P00045": "워킹 Tank 2 MDI 교반기 Trip"},
        {"P00046": "워킹 Tank 2-2 HARD 교반기 Trip"},
        {"P00047": "워킹 Tank 3-1 고탄성 교반기 Trip"},
        {"P00048": "워킹 Tank 3 ISO 교반기 Trip"},
        {"P00049": "워킹 Tank 3-2 CUSH 교반기 Trip"}
      ],
      "Trip": [
        {"P00030": "1-1펌프 BACK Trip"},
        {"P00031": "1-1펌프 ISO Trip"},
        {"P00032": "2-1펌프 SOFT Trip"},
        {"P00033": "2-1펌프 MDI Trip"},
        {"P00034": "2-2펌프 HARD Trip"},
        {"P00035": "2-2펌프 MDI Trip"},
        {"P00036": "3펌프 고탄성 Trip"},
        {"P00037": "3펌프 ISO Trip"},
        {"P00038": "4펌프 CUSH Trip"},
        {"P00039": "4펌프 ISO Trip"},
        {"P0003A": "Oval Conveyor Trip"},
        {"P0003B": "Closing Unit Trip"},
        {"P0003C": "Opening Unit Trip"},
        {"P0003E": "유압 1 Trip"},
        {"P0003F": "유압 2 Trip"},
        {"P00040": "유압 3 Trip"},
        {"P00041": "유압 4 Trip"},
        {"P0004A": "1-2펌프 CUSH Trip"},
        {"P0004B": "1-2펌프 ISO Trip"},
        {"P0004C": "3-2펌프 BACK Trip"},
        {"P0004D": "3-2펌프 ISO Trip"}
      ],
      "Cooling_SOL": [
        {"P00220": "워킹 Tank 1 BACK Cooling SOL"},
        {"P00222": "워킹 Tank 1 ISO Cooling SOL"},
        {"P00224": "워킹 Tank 2-1 SOFT Cooling SOL"},
        {"P00226": "워킹 Tank 2-2 MDI Cooling SOL"},
        {"P00228": "워킹 Tank 2-2 HARD Cooling SOL"},
        {"P0022A": "워킹 Tank 3-1 고탄성 Cooling SOL"},
        {"P0022C": "워킹 Tank 3 ISO Cooling SOL"},
        {"P0022E": "워킹 Tank 3-2 CUSH Cooling SOL"}
      ],
      "Exchanger_SOL": [
        {"P00221": "워킹 Tank 1 BACK Exchanger SOL"},
        {"P00223": "워킹 Tank 1 ISO Exchanger SOL"},
        {"P00225": "워킹 Tank 2-1 SOFT Exchanger SOL"},
        {"P00227": "워킹 Tank 2-2 MDI Exchanger SOL"},
        {"P00229": "워킹 Tank 2-2 HARD Exchanger SOL"},
        {"P0022B": "워킹 Tank 3-1 고탄성 Exchanger SOL"},
        {"P0022D": "워킹 Tank 3 ISO Exchanger SOL"},
        {"P0022F": "워킹 Tank 3-2 CUSH Exchanger SOL"},
        {"P00230": "KPX ISO1 원료탱크 Exchanger SOL"},
        {"P00231": "금호 ISO3 원료탱크 Exchanger SOL"},
        {"P00232": "Pump 1-2 CUSH Exchanger SOL"},
        {"P00233": "Pump 1-2 ISO Exchanger SOL"},
        {"P00248": "Pump 3-2 BACK Exchanger SOL"},
        {"P00249": "Pump 3-2 ISO Exchanger SOL"}
      ],
      "Relife_SOL": [
        {"P00234": "유압 1 Pump Relife SOL"},
        {"P00235": "유압 2 Pump Relife SOL"},
        {"P00236": "유압 3 Pump Relife SOL"},
        {"P00237": "유압 4 Pump Relife SOL"},
        {"P00238": "Tension HYD' Unit Relife SOL"}
      ],
      "Nozzle_SOL": [
        {"P00239": "1펌프 BACK Calibratin Nozzle SOL"},
        {"P0023A": "1펌프 ISO Calibratin Nozzle SOL"},
        {"P0023B": "2-1펌프 SOFT Calibratin Nozzle SOL"},
        {"P0023C": "2-1펌프 MDI Calibratin Nozzle SOL"},
        {"P0023D": "2-2펌프 HARD Calibratin Nozzle SOL"},
        {"P0023E": "2-2펌프 MDI Calibratin Nozzle SOL"},
        {"P0023F": "3펌프 고탄성 Calibratin Nozzle SOL"},
        {"P00240": "3펌프 ISO Calibratin Nozzle SOL"},
        {"P00241": "4펌프 CUSH Calibratin Nozzle SOL"},
        {"P00242": "4펌프 ISO Calibratin Nozzle SOL"},
        {"P00244": "1-2펌프 CUSH Calibratin Nozzle SOL"},
        {"P00245": "1-2펌프 ISO Calibratin Nozzle SOL"},
        {"P00246": "3-2펌프 BACK Calibratin Nozzle SOL"},
        {"P00247": "3-2펌프 ISO Calibratin Nozzle SOL"}
      ],
      "Feeding_SOL": [
        {"P00282": "워킹 Tank 1 BACK Feeding SOL"},
        {"P00283": "워킹 Tank 1 ISO Feeding SOL"},
        {"P00284": "워킹 Tank 2-1 SOFT Feeding SOL"},
        {"P00285": "워킹 Tank 2 MDI Feeding SOL"},
        {"P00286": "워킹 Tank 2-2 HARD Feeding SOL"},
        {"P00287": "워킹 Tank 3-1 고탄성 Feeding SOL"},
        {"P00288": "워킹 Tank 3 ISO Feeding SOL"},
        {"P00289": "워킹 Tank 3-2 CUSH Feeding SOL"}
      ],
      "MC": [
        {"P00250": "Tension HYD' MC"},
        {"P00251": "유압 1 Pump MC"},
        {"P00252": "유압 2 Pump MC"},
        {"P00253": "유압 3 Pump MC"},
        {"P00254": "유압 4 Pump MC"}
      ],
      "Agitator_MC": [
        {"P00255": "워킹 Tank 1 BACK Agitator MC"},
        {"P00256": "워킹 Tank 1 ISO Agitator MC"},
        {"P00257": "워킹 Tank 2-1 SOFT Agitator MC"},
        {"P00258": "워킹 Tank 2 MDI Agitator MC"},
        {"P00259": "워킹 Tank 2-2 HARD Agitator MC"},
        {"P0025A": "워킹 Tank 3-1 고탄성 Agitator MC"},
        {"P0025B": "워킹 Tank 3 ISO Agitator MC"},
        {"P0025C": "워킹 Tank 3-2 CUSH Agitator MC"}
      ],
      "Break_MC": [
        {"P0025D": "Oval Conveyor Break MC"},
        {"P0025E": "Closing Unit Break MC"},
        {"P0025F": "Opening Unit Break MC"}
      ],
      "Heater": [
        {"P00270": "워킹 Tank 1 BACK Heater"},
        {"P00271": "워킹 Tank 1 ISO Heater"},
        {"P00272": "워킹 Tank 2-1 SOFT Heater"},
        {"P00273": "워킹 Tank 2 MDI Heater"},
        {"P00274": "워킹 Tank 2-2 HARD Heater"},
        {"P00275": "워킹 Tank 3-1 고탄성 Heater"},
        {"P00276": "워킹 Tank 3 ISO Heater"},
        {"P00277": "워킹 Tank 3-2 CUSH Heater"}
      ],
      "INV_P1": [
        {"P0024E": "유압1 INV P1"},
        {"P0024F": "유압2 INV P1"},
        {"P00306": "유압3 INV P1"},
        {"P00307": "유압4 INV P1"}
      ],
      "Inverter_Signals": [
        {"P00190": "1펌프 BACK Inverter_FWD"},
        {"P00191": "1펌프 BACK Inverter_X4 SPEED"},
        {"P00192": "1펌프 BACK Inverter_X5 RESET"},
        {"P00193": "1펌프 ISO Inverter_FWD"},
        {"P00194": "1펌프 ISO Inverter_X4 SPEED"},
        {"P00195": "1펌프 ISO Inverter_X5 RESET"},
        {"P00196": "2-1펌프 SOFT Inverter_FWD"},
        {"P00197": "2-1펌프 SOFT Inverter_X4 SPEED"},
        {"P00198": "2-1펌프 SOFT Inverter_X5 RESET"},
        {"P00199": "2-1펌프 MDI Inverter_FWD"},
        {"P0019A": "2-1펌프 MDI Inverter_X4 SPEED"},
        {"P0019B": "2-1펌프 MDI Inverter_X5 RESET"},
        {"P0019C": "2-2펌프 HARD Inverter_FWD"},
        {"P0019D": "2-2펌프 HARD Inverter_X4 SPEED"},
        {"P0019E": "2-2펌프 HARD Inverter_X5 RESET"},
        {"P00200": "2-2펌프 MDI Inverter_FWD"},
        {"P00201": "2-2펌프 MDI Inverter_X4 SPEED"},
        {"P00202": "2-2펌프 MDI Inverter_X5 RESET"},
        {"P00203": "3펌프 고탄성 Inverter_FWD"},
        {"P00204": "3펌프 고탄성 Inverter_X4 SPEED"},
        {"P00205": "3펌프 고탄성 Inverter_X5 RESET"},
        {"P00206": "3펌프 ISO Inverter_FWD"},
        {"P00207": "3펌프 ISO Inverter_X4 SPEED"},
        {"P00208": "3펌프 ISO Inverter_X5 RESET"},
        {"P00209": "4펌프 CUSH Inverter_FWD"},
        {"P0020A": "4펌프 CUSH Inverter_X4 SPEED"},
        {"P0020B": "4펌프 CUSH Inverter_X5 RESET"},
        {"P0020C": "4펌프 ISO Inverter_FWD"},
        {"P0020D": "4펌프 ISO Inverter_X4 SPEED"},
        {"P0020E": "4펌프 ISO Inverter_X5 RESET"},
        {"P00210": "Oval Conveyor Inverter_FWD [카드손상]"},
        {"P00211": "Oval Conveyor Inverter_REV [카드손상]"},
        {"P00212": "Oval Conveyor Inverter_X4 SPEED [카드손상]"},
        {"P00213": "Oval Conveyor Inverter_X5 RESET [카드손상]"},
        {"P00214": "Closing Unit Inverter_FWD"},
        {"P00215": "Closing Unit Inverter_REV"},
        {"P00216": "Closing Unit Inverter_X4 LOW SPEED"},
        {"P00217": "Closing Unit Inverter_X5 RESET"},
        {"P00218": "Opening Unit Inverter_FWD"},
        {"P00219": "Opening Unit Inverter_REV"},
        {"P0021A": "Opening Unit Inverter_X4 LOW SPEED"},
        {"P0021B": "Opening Unit Inverter_X5 RESET"},
        {"P0021C": "Oval Conveyor Inverter_FWD"},
        {"P0021D": "Oval Conveyor Inverter_REV"},
        {"P0021E": "Oval Conveyor Inverter_X4 SPEED"},
        {"P0021F": "Oval Conveyor Inverter_X5 RESET"},
        {"P00460": "1-2펌프 CUSH Inverter_FWD"},
        {"P00461": "1-2펌프 CUSH Inverter_X4 SPEED"},
        {"P00462": "1-2펌프 CUSH Inverter_X5 RESET"},
        {"P00463": "1-2펌프 ISO Inverter_FWD"},
        {"P00464": "1-2펌프 ISO Inverter_X4 SPEED"},
        {"P00465": "1-2펌프 ISO Inverter_X5 RESET"},
        {"P00466": "3-2펌프 BACK Inverter_FWD"},
        {"P00467": "3-2펌프 BACK Inverter_X4 SPEED"},
        {"P00468": "3-2펌프 BACK Inverter_X5 RESET"},
        {"P00469": "3-2펌프 ISO Inverter_FWD"},
        {"P0046A": "3-2펌프 ISO Inverter_X4 SPEED"},
        {"P0046B": "3-2펌프 ISO Inverter_X5 RESET"},
        {"P0046C": "Changing Unit Inverter_FWD"},
        {"P0046D": "Changing Unit Inverter_REV"},
        {"P0046E": "Changing Unit Inverter_SPEED"},
        {"P0046F": "Changing Unit Inverter_RESET"}
      ],
      "Pressure": [
        {"P00066": "유압1 Pressure Low"},
        {"P00067": "유압1 Pressure High"},
        {"P00068": "유압2 Pressure Low"},
        {"P00069": "유압2 Pressure High"},
        {"P0006A": "유압3 Pressure Low"},
        {"P0006B": "유압3 Pressure High"},
        {"P0006C": "유압4 Pressure Low"},
        {"P0006D": "유압4 Pressure High"}
      ],
      "Air_Control": [
        {"P00478": "HD1 주입구 Air분사"},
        {"P00479": "HD2 주입구 Air분사"},
        {"P0047A": "HD3 주입구 Air분사"},
        {"P0047B": "HD4주입구 Air_Sol"},
        {"P00243": "Air주입 Unit 에어주입1,2원통 SOL"}
      ]
    },
    "Location_Based": {
      "워킹 Tank 1": [
        {"P00042": " BACK 교반기 Trip"},
        {"P00220": "워킹 Tank 1 BACK Cooling SOL"},
        {"P00221": "워킹 Tank 1 BACK Exchanger SOL"},
        {"P00255": "워킹 Tank 1 BACK Agitator MC"},
        {"P00270": "워킹 Tank 1 BACK Heater"},
        {"P00282": "워킹 Tank 1 BACK Feeding SOL"}
      ],
      "워킹 Tank 1 ISO": [
        {"P00043": "워킹 Tank 1 ISO 교반기 Trip"},
        {"P00222": "워킹 Tank 1 ISO Cooling SOL"},
        {"P00223": "워킹 Tank 1 ISO Exchanger SOL"},
        {"P00256": "워킹 Tank 1 ISO Agitator MC"},
        {"P00271": "워킹 Tank 1 ISO Heater"},
        {"P00283": "워킹 Tank 1 ISO Feeding SOL"},
        {"P00230": "KPX ISO1 원료탱크 Exchanger SOL"}
      ],
      "워킹 Tank 2-1 SOFT": [
        {"P00044": "워킹 Tank 2-1 SOFT 교반기 Trip"},
        {"P00224": "워킹 Tank 2-1 SOFT Cooling SOL"},
        {"P00225": "워킹 Tank 2-1 SOFT Exchanger SOL"},
        {"P00257": "워킹 Tank 2-1 SOFT Agitator MC"},
        {"P00272": "워킹 Tank 2-1 SOFT Heater"},
        {"P00284": "워킹 Tank 2-1 SOFT Feeding SOL"}
      ],
      "워킹 Tank 2 MDI": [
        {"P00045": "워킹 Tank 2 MDI 교반기 Trip"},
        {"P00258": "워킹 Tank 2 MDI Agitator MC"},
        {"P00273": "워킹 Tank 2 MDI Heater"},
        {"P00285": "워킹 Tank 2 MDI Feeding SOL"},
        {"P00226": "워킹 Tank 2-2 MDI Cooling SOL"},
        {"P00227": "워킹 Tank 2-2 MDI Exchanger SOL"}
      ],
      "워킹 Tank 2-2 HARD": [
        {"P00046": "워킹 Tank 2-2 HARD 교반기 Trip"},
        {"P00228": "워킹 Tank 2-2 HARD Cooling SOL"},
        {"P00229": "워킹 Tank 2-2 HARD Exchanger SOL"},
        {"P00259": "워킹 Tank 2-2 HARD Agitator MC"},
        {"P00274": "워킹 Tank 2-2 HARD Heater"},
        {"P00286": "워킹 Tank 2-2 HARD Feeding SOL"}
      ],
      "워킹 Tank 3 ISO": [
        {"P00048": "워킹 Tank 3 ISO 교반기 Trip"},
        {"P0022C": "워킹 Tank 3 ISO Cooling SOL"},
        {"P0022D": "워킹 Tank 3 ISO Exchanger SOL"},
        {"P0025B": "워킹 Tank 3 ISO Agitator MC"},
        {"P00276": "워킹 Tank 3 ISO Heater"},
        {"P00288": "워킹 Tank 3 ISO Feeding SOL"},
        {"P00231": "금호 ISO3 원료탱크 Exchanger SOL"}
      ],
      "워킹 Tank 3-1 고탄성": [
        {"P00047": "워킹 Tank 3-1 고탄성 교반기 Trip"},
        {"P0022A": "워킹 Tank 3-1 고탄성 Cooling SOL"},
        {"P0022B": "워킹 Tank 3-1 고탄성 Exchanger SOL"},
        {"P0025A": "워킹 Tank 3-1 고탄성 Agitator MC"},
        {"P00275": "워킹 Tank 3-1 고탄성 Heater"},
        {"P00287": "워킹 Tank 3-1 고탄성 Feeding SOL"}
      ],
      "워킹 Tank 3-2 CUSH": [
        {"P00049": "워킹 Tank 3-2 CUSH 교반기 Trip"},
        {"P0022E": "워킹 Tank 3-2 CUSH Cooling SOL"},
        {"P0022F": "워킹 Tank 3-2 CUSH Exchanger SOL"},
        {"P0025C": "워킹 Tank 3-2 CUSH Agitator MC"},
        {"P00277": "워킹 Tank 3-2 CUSH Heater"},
        {"P00289": "워킹 Tank 3-2 CUSH Feeding SOL"}
      ],
      "1-1펌프 BACK": [
        {"P00030": "1-1펌프 BACK Trip"},
        {"P00190": "1펌프 BACK Inverter_FWD"},
        {"P00191": "1펌프 BACK Inverter_X4 SPEED"},
        {"P00192": "1펌프 BACK Inverter_X5 RESET"},
        {"P00239": "1펌프 BACK Calibratin Nozzle SOL"}
      ],
      "1-1펌프 ISO": [
        {"P00031": "1-1펌프 ISO Trip"},
        {"P00193": "1펌프 ISO Inverter_FWD"},
        {"P00194": "1펌프 ISO Inverter_X4 SPEED"},
        {"P00195": "1펌프 ISO Inverter_X5 RESET"},
        {"P0023A": "1펌프 ISO Calibratin Nozzle SOL"}
      ],
      "1-2펌프 CUSH": [
        {"P0004A": "1-2펌프 CUSH Trip"},
        {"P00244": "1-2펌프 CUSH Calibratin Nozzle SOL"},
        {"P00460": "1-2펌프 CUSH Inverter_FWD"},
        {"P00461": "1-2펌프 CUSH Inverter_X4 SPEED"},
        {"P00462": "1-2펌프 CUSH Inverter_X5 RESET"},
        {"P00232": "Pump 1-2 CUSH Exchanger SOL"}
      ],
      "1-2펌프 ISO": [
        {"P0004B": "1-2펌프 ISO Trip"},
        {"P00245": "1-2펌프 ISO Calibratin Nozzle SOL"},
        {"P00463": "1-2펌프 ISO Inverter_FWD"},
        {"P00464": "1-2펌프 ISO Inverter_X4 SPEED"},
        {"P00465": "1-2펌프 ISO Inverter_X5 RESET"},
        {"P00233": "Pump 1-2 ISO Exchanger SOL"}
      ],
      "2-1펌프 SOFT": [
        {"P00032": "2-1펌프 SOFT Trip"},
        {"P00196": "2-1펌프 SOFT Inverter_FWD"},
        {"P00197": "2-1펌프 SOFT Inverter_X4 SPEED"},
        {"P00198": "2-1펌프 SOFT Inverter_X5 RESET"},
        {"P0023B": "2-1펌프 SOFT Calibratin Nozzle SOL"}
      ],
      "2-1펌프 MDI": [
        {"P00033": "2-1펌프 MDI Trip"},
        {"P00199": "2-1펌프 MDI Inverter_FWD"},
        {"P0019A": "2-1펌프 MDI Inverter_X4 SPEED"},
        {"P0019B": "2-1펌프 MDI Inverter_X5 RESET"},
        {"P0023C": "2-1펌프 MDI Calibratin Nozzle SOL"}
      ],
      "2-2펌프 HARD": [
        {"P00034": "2-2펌프 HARD Trip"},
        {"P0019C": "2-2펌프 HARD Inverter_FWD"},
        {"P0019D": "2-2펌프 HARD Inverter_X4 SPEED"},
        {"P0019E": "2-2펌프 HARD Inverter_X5 RESET"},
        {"P0023D": "2-2펌프 HARD Calibratin Nozzle SOL"}
      ],
      "2-2펌프 MDI": [
        {"P00035": "2-2펌프 MDI Trip"},
        {"P00200": "2-2펌프 MDI Inverter_FWD"},
        {"P00201": "2-2펌프 MDI Inverter_X4 SPEED"},
        {"P00202": "2-2펌프 MDI Inverter_X5 RESET"},
        {"P0023E": "2-2펌프 MDI Calibratin Nozzle SOL"}
      ],
      "3펌프 고탄성": [
        {"P00036": "3펌프 고탄성 Trip"},
        {"P00203": "3펌프 고탄성 Inverter_FWD"},
        {"P00204": "3펌프 고탄성 Inverter_X4 SPEED"},
        {"P00205": "3펌프 고탄성 Inverter_X5 RESET"},
        {"P0023F": "3펌프 고탄성 Calibratin Nozzle SOL"}
      ],
      "3펌프 ISO": [
        {"P00037": "3펌프 ISO Trip"},
        {"P00206": "3펌프 ISO Inverter_FWD"},
        {"P00207": "3펌프 ISO Inverter_X4 SPEED"},
        {"P00208": "3펌프 ISO Inverter_X5 RESET"},
        {"P00240": "3펌프 ISO Calibratin Nozzle SOL"}
      ],
      "3-2펌프 BACK": [
        {"P0004C": "3-2펌프 BACK Trip"},
        {"P00246": "3-2펌프 BACK Calibratin Nozzle SOL"},
        {"P00466": "3-2펌프 BACK Inverter_FWD"},
        {"P00467": "3-2펌프 BACK Inverter_X4 SPEED"},
        {"P00468": "3-2펌프 BACK Inverter_X5 RESET"},
        {"P00248": "Pump 3-2 BACK Exchanger SOL"}
      ],
      "3-2펌프 ISO": [
        {"P0004D": "3-2펌프 ISO Trip"},
        {"P00247": "3-2펌프 ISO Calibratin Nozzle SOL"},
        {"P00469": "3-2펌프 ISO Inverter_FWD"},
        {"P0046A": "3-2펌프 ISO Inverter_X4 SPEED"},
        {"P0046B": "3-2펌프 ISO Inverter_X5 RESET"},
        {"P00249": "Pump 3-2 ISO Exchanger SOL"}
      ],
      "4펌프 CUSH": [
        {"P00038": "4펌프 CUSH Trip"},
        {"P00209": "4펌프 CUSH Inverter_FWD"},
        {"P0020A": "4펌프 CUSH Inverter_X4 SPEED"},
        {"P0020B": "4펌프 CUSH Inverter_X5 RESET"},
        {"P00241": "4펌프 CUSH Calibratin Nozzle SOL"}
      ],
      "4펌프 ISO": [
        {"P00039": "4펌프 ISO Trip"},
        {"P0020C": "4펌프 ISO Inverter_FWD"},
        {"P0020D": "4펌프 ISO Inverter_X4 SPEED"},
        {"P0020E": "4펌프 ISO Inverter_X5 RESET"},
        {"P00242": "4펌프 ISO Calibratin Nozzle SOL"}
      ],
      "유압 1": [
        {"P0003E": "유압 1 Trip"},
        {"P00234": "유압 1 Pump Relife SOL"},
        {"P0024E": "유압1 INV P1"},
        {"P00251": "유압 1 Pump MC"},
        {"P00066": "유압1 Pressure Low"},
        {"P00067": "유압1 Pressure High"},
        {"P00478": "HD1 주입구 Air분사"}
      ],
      "유압 2": [
        {"P0024F": "유압2 INV P1"},
        {"P00068": "유압2 Pressure Low"},
        {"P00069": "유압2 Pressure High"},
        {"P0003F": "유압 2 Trip"},
        {"P00235": "유압 2 Pump Relife SOL"},
        {"P00252": "유압 2 Pump MC"},
        {"P00479": "HD2 주입구 Air분사"}
      ],
      "유압 3": [
        {"P0006A": "유압3 Pressure Low"},
        {"P0006B": "유압3 Pressure High"},
        {"P00040": "유압 3 Trip"},
        {"P00236": "유압 3 Pump Relife SOL"},
        {"P00253": "유압 3 Pump MC"},
        {"P0047A": "HD3 주입구 Air분사"},
        {"P00306": "유압3 INV P1"}
      ],
      "유압 4": [
        {"P0006C": "유압4 Pressure Low"},
        {"P0006D": "유압4 Pressure High"},
        {"P00041": "유압 4 Trip"},
        {"P00237": "유압 4 Pump Relife SOL"},
        {"P00254": "유압 4 Pump MC"},
        {"P0047B": "HD4주입구 Air_Sol"},
        {"P00307": "유압4 INV P1"}
      ],
      "Opening_Unit": [
        {"P0025F": "Opening Unit Break MC"},
        {"P0003C": "Opening Unit Trip"},
        {"P00218": "Opening Unit Inverter_FWD"},
        {"P00219": "Opening Unit Inverter_REV"},
        {"P0021A": "Opening Unit Inverter_X4 LOW SPEED"},
        {"P0021B": "Opening Unit Inverter_X5 RESET"}
      ],
      "Closing_Unit": [
        {"P0025E": "Closing Unit Break MC"},
        {"P0003B": "Closing Unit Trip"},
        {"P00214": "Closing Unit Inverter_FWD"},
        {"P00215": "Closing Unit Inverter_REV"},
        {"P00216": "Closing Unit Inverter_X4 LOW SPEED"},
        {"P00217": "Closing Unit Inverter_X5 RESET"}
      ],
      "Changing_Unit": [
        {"P0046C": "Changing Unit Inverter_FWD"},
        {"P0046D": "Changing Unit Inverter_REV"},
        {"P0046E": "Changing Unit Inverter_SPEED"},
        {"P0046F": "Changing Unit Inverter_RESET"}
      ],
      "Tension_HYD": [
        {"P00238": "Tension HYD' Unit Relife SOL"},
        {"P00250": "Tension HYD' MC"}
      ],
      "Air_Injection_Unit": [
        {"P00243": "Air주입 Unit 에어주입1,2원통 SOL"}
      ],
      "Oval_Conveyor": [
        {"P0025D": "Oval Conveyor Break MC"},
        {"P0003A": "Oval Conveyor Trip"},
        {"P00210": "Oval Conveyor Inverter_FWD [카드손상]"},
        {"P00211": "Oval Conveyor Inverter_REV [카드손상]"},
        {"P00212": "Oval Conveyor Inverter_X4 SPEED [카드손상]"},
        {"P00213": "Oval Conveyor Inverter_X5 RESET [카드손상]"},
        {"P0021C": "Oval Conveyor Inverter_FWD"},
        {"P0021D": "Oval Conveyor Inverter_REV"},
        {"P0021E": "Oval Conveyor Inverter_X4 SPEED"},
        {"P0021F": "Oval Conveyor Inverter_X5 RESET"}
      ]
    }
  }
}