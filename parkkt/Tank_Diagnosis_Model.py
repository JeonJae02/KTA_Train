"""탱크 heat / exchanger / cool 진단 모델.

`Tank_Preprocess.py` 로 정리한 데이터를 받아 룰 3가지를 적용하고, 문제가 있으면
`relay_warning` JSON 으로 돌려준다.

## 판정 규칙

**① heat 이상** — 전날 `TK_Temp_PV` 최솟값이 `TK_Temp_L_Set` 보다 낮으면 이상.
  -> heater ID 로 보고

**② exchanger / cool 이상** — 병합된 사이클이 켜진 뒤 `MAX_ON_SEC`(기본 10분) 안에
  꺼지지 않으면 이상. exchanger 는 온도가 SV 밑으로 내려가면 꺼지므로
  "안 꺼졌다 = 온도를 못 잡았다" 와 같은 말이다.
  - cool 이 같이 안 켜졌으면 -> exchanger ID 로만 보고
  - cool 이 같이 켜졌으면    -> exchanger ID + cooler ID 둘 다 보고
                               (둘이 같이 돌았는데도 실패한 것이므로)

**③ exchanger 성능 저하** — cool 동반 사이클이 `CONSEC_COOL_N`(기본 10회) 연속 나오면
  exchanger 혼자서 온도를 못 잡는 걸로 보고 이상. -> exchanger ID 로 보고

## 출력 형식

    {
      "relay_warning": {
        "P00270": {"type": "thermal", "message": "히터 가열 응답 지연 · ..."},
        "P00221": {"type": "thermal", "message": "열교환기 냉각 응답 지연 · ..."}
      }
    }

문제가 없으면 `relay_warning` 은 빈 dict 이다.
"""

import json

import numpy as np
import pandas as pd

from Tank_Preprocess import preprocess, preprocess_signals

# --------------------------------------------------------------------------- #
# 판정 파라미터
# --------------------------------------------------------------------------- #
MAX_ON_SEC = 600        #: 사이클이 켜진 뒤 이 시간 안에 안 꺼지면 이상 (규칙 ②)
CONSEC_COOL_N = 10      #: cool 동반이 이만큼 연속되면 성능저하 (규칙 ③)

#: 탱크마다 냉각 능력·사이클 리듬이 다르다. 데이터가 쌓이면 탱크별로 조정한다.
#: (비어 있으면 위 공통값을 그대로 쓴다)
TANK_OVERRIDES = {
    # "I2": {"MAX_ON_SEC": 900},
}

# --------------------------------------------------------------------------- #
# 릴레이 ID 매핑
# --------------------------------------------------------------------------- #
#: 고장 부위별로 보고할 exchanger id.
#:
#: 중복 여부는 `validate_relay_ids()` 로 확인할 수 있다(import 시 자동 호출 아님).
#: cooler.P1 은 처음에 P00220 으로 받았다가 exchanger.I3 와 겹쳐서 P0022D 로 정정했다.
RELAY_IDS = {
    "heater": {
        "P1": "P00270", "P2": "P00272", "P3": "P00275", "P4": "P00274",
        "P5": "P00277", "I1": "P00271", "I2": "P00273", "I3": "P00276",
    },
    "cooler": {
        "P1": "P0022D", "P2": "P00224", "P3": "P0022A", "P4": "P00228",
        "P5": "P0022E", "I1": "P00222", "I2": "P00226", "I3": "P00231",
    },
    "exchanger": {
        "P1": "P00221", "P2": "P00225", "P3": "P0022B", "P4": "P00229",
        "P5": "P0022F", "I1": "P00223", "I2": "P00227", "I3": "P00220",
    },
}

WARNING_TYPE = "thermal"

#: 부위별 메시지. heater 문구가 기준이고 나머지는 같은 구조로 맞췄다
#: ([장치][동작] [증상] · [지표] [추세]).
MESSAGES = {
    "heater":
        "히터 가열 응답 지연 · 설정 온도 도달 시간 증가 추세",
    "exchanger":
        "열교환기 냉각 응답 지연 · 설정 온도 복귀 시간 증가 추세",
    "cooler":
        "쿨러 냉각 응답 지연 · 열교환기 동시 가동에도 설정 온도 미복귀",
    "exchanger_degrade":
        "열교환기 냉각 성능 저하 · 쿨러 동시 가동 연속 발생",
}

#: 같은 ID 에 여러 사유가 겹치면 이 순서로 앞선 것을 쓴다(구조적 문제 우선).
_MESSAGE_PRIORITY = ["exchanger_degrade", "exchanger", "cooler", "heater"]


def validate_relay_ids(verbose=True):
    """RELAY_IDS 안에 중복된 값이 있는지 확인한다."""
    seen, dups = {}, []
    for part, mapping in RELAY_IDS.items():
        for tank, rid in mapping.items():
            if rid in seen:
                dups.append((rid, seen[rid], f"{part}.{tank}"))
            else:
                seen[rid] = f"{part}.{tank}"
    if dups and verbose:
        for rid, a, b in dups:
            print(f"⚠️ 릴레이 ID 중복: {rid} <- {a} / {b}")
    return dups


def params(tank):
    """해당 탱크에 적용할 판정 파라미터."""
    p = {"MAX_ON_SEC": MAX_ON_SEC, "CONSEC_COOL_N": CONSEC_COOL_N}
    p.update(TANK_OVERRIDES.get(tank, {}))
    return p


# --------------------------------------------------------------------------- #
# 규칙 ① heat
# --------------------------------------------------------------------------- #

def check_heater(pv, lset):
    """일별 최저 PV vs 그날의 L_Set. 전날 것을 다음날 아침에 판정한다고 보면 된다."""
    daily = pv.groupby(pv.index.date).min().rename("pv_min").to_frame()
    daily["l_set"] = lset.groupby(lset.index.date).median().reindex(daily.index).ffill()
    daily["margin"] = daily["pv_min"] - daily["l_set"]
    daily["heat_abnormal"] = daily["pv_min"] < daily["l_set"]
    daily.index.name = "day"
    return daily


# --------------------------------------------------------------------------- #
# 규칙 ② exchanger / cool
# --------------------------------------------------------------------------- #

def check_cycle_duration(cycles, max_on=MAX_ON_SEC):
    """ON 지속시간이 max_on 을 넘으면 이상. cool 동반 여부로 원인을 나눈다."""
    out = cycles.copy()
    out["abnormal"] = out["on_duration_sec"] > max_on
    out["cause"] = None
    out.loc[out["abnormal"] & out["cool_overlap"], "cause"] = "exchanger와 cool 이상"
    out.loc[out["abnormal"] & ~out["cool_overlap"], "cause"] = "exchanger 이상"
    return out


# --------------------------------------------------------------------------- #
# 규칙 ③ exchanger 성능 저하
# --------------------------------------------------------------------------- #

def check_consecutive_cool(cycles, n=CONSEC_COOL_N):
    """cool 동반이 n회 연속인 지점을 표시한다. 반환: (표시된 cycles, 런 목록)"""
    out = cycles.sort_values("cycle_start").reset_index(drop=True).copy()
    vals = out["cool_overlap"].astype(bool).values
    flags, runs, run = [], [], 0
    for i, v in enumerate(vals):
        run = run + 1 if v else 0
        flags.append(run >= n)
        if run > 0 and (i + 1 == len(vals) or not vals[i + 1]):
            runs.append({"run_len": run,
                         "start_time": out["cycle_start"].iloc[i - run + 1],
                         "end_time": out["cycle_start"].iloc[i]})
    out["degrade_flag"] = flags
    return out, pd.DataFrame(runs)


# --------------------------------------------------------------------------- #
# relay_warning 조립
# --------------------------------------------------------------------------- #

def build_relay_warning(tank, heat_daily, cycles):
    """진단 결과 -> relay_warning dict.

    같은 ID 에 사유가 여러 개 겹치면 `_MESSAGE_PRIORITY` 순서로 하나만 남긴다.
    """
    parts = {}   # part_key -> 사유 집합

    if bool(heat_daily["heat_abnormal"].any()):
        parts.setdefault("heater", set()).add("heater")

    hits = cycles[cycles["abnormal"]]
    if (hits["cause"] == "exchanger 이상").any():
        parts.setdefault("exchanger", set()).add("exchanger")
    if (hits["cause"] == "exchanger와 cool 이상").any():
        # 둘이 같이 돌았는데도 실패 -> 양쪽 다 보고
        parts.setdefault("exchanger", set()).add("exchanger")
        parts.setdefault("cooler", set()).add("cooler")

    if bool(cycles["degrade_flag"].any()):
        parts.setdefault("exchanger", set()).add("exchanger_degrade")

    warning = {}
    for part, reasons in parts.items():
        rid = RELAY_IDS[part][tank]
        reason = next(r for r in _MESSAGE_PRIORITY if r in reasons)
        warning[rid] = {"type": WARNING_TYPE, "message": MESSAGES[reason]}
    return warning


# --------------------------------------------------------------------------- #
# 진단 진입점
# --------------------------------------------------------------------------- #

def diagnose_prepared(tank, cycles, pv, lset, detail=False):
    """이미 전처리된 데이터로 진단한다.

    cycles : Tank_Preprocess.merge_cycles + flag_cool 을 거친 표
    pv, lset : ℃ 로 변환되고 글리치가 제거된 Series

    반환 : {"relay_warning": {...}}  (detail=True 면 "_detail" 키가 붙는다)
    """
    p = params(tank)
    cyc = check_cycle_duration(cycles, p["MAX_ON_SEC"])
    cyc, runs = check_consecutive_cool(cyc, p["CONSEC_COOL_N"])
    heat = check_heater(pv, lset)

    result = {"relay_warning": build_relay_warning(tank, heat, cyc)}

    if detail:
        result["_detail"] = {
            "tank": tank,
            "params": p,
            "n_cycles": int(len(cyc)),
            "heat_abnormal_days": int(heat["heat_abnormal"].sum()),
            "exchanger_only": int((cyc["cause"] == "exchanger 이상").sum()),
            "exchanger_with_cool": int((cyc["cause"] == "exchanger와 cool 이상").sum()),
            "degrade_cycles": int(cyc["degrade_flag"].sum()),
            "max_cool_run": int(runs["run_len"].max()) if len(runs) else 0,
            "abnormal_cycles": cyc.loc[cyc["abnormal"],
                                        ["cycle_start", "cycle_end",
                                         "on_duration_sec", "cool_overlap", "cause"]],
            "heat_daily": heat,
        }
    return result


def diagnose(tank, start, end, use_cache=True, detail=False, **kw):
    """태그 로드 -> 전처리 -> 진단까지 한 번에."""
    data = preprocess(tank, start, end, use_cache=use_cache, **kw)
    return diagnose_prepared(tank, data["cycles"], data["pv"], data["lset"], detail=detail)


def diagnose_many(tanks, start, end, **kw):
    """여러 탱크를 돌려 relay_warning 을 하나로 합친다."""
    merged = {}
    for t in tanks:
        try:
            merged.update(diagnose(t, start, end, **kw)["relay_warning"])
        except Exception as e:
            print(f"[{t}] 진단 실패: {e}")
    return {"relay_warning": merged}


def to_json(result, path=None, ensure_ascii=False, indent=2):
    """relay_warning 만 JSON 문자열로. path 를 주면 파일로도 쓴다."""
    payload = {"relay_warning": result["relay_warning"]}
    text = json.dumps(payload, ensure_ascii=ensure_ascii, indent=indent)
    if path:
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)
    return text


# --------------------------------------------------------------------------- #
# 실시간 단일 사이클 판정
# --------------------------------------------------------------------------- #

def judge_cycle(tank, cycle_start, cycle_end, cool, now=None, recent_cool_run=0):
    """사이클 하나를 즉시 판정. 끝나기를 기다리지 않고, 켜진 지 MAX_ON_SEC 이
    지나는 순간 이상으로 띄운다.

    cycle_end : 꺼진 시각. 아직 켜져 있으면 None
    반환 : dict(state, relay_warning, on_sec, cool_overlap, cool_run)
    """
    p = params(tank)
    now = now or pd.Timestamp.now()
    end = cycle_end if cycle_end is not None else now
    on_sec = (end - cycle_start).total_seconds()

    seg = cool.loc[cycle_start:end]
    before = cool.loc[:cycle_start]
    start_val = before.iloc[-1] if len(before) else 0
    has_cool = bool((seg == 1).any() or start_val == 1)
    run = recent_cool_run + 1 if has_cool else 0

    warning, state = {}, "정상" if cycle_end is not None else "감시중"

    if on_sec > p["MAX_ON_SEC"]:
        state = "이상"
        warning[RELAY_IDS["exchanger"][tank]] = {
            "type": WARNING_TYPE, "message": MESSAGES["exchanger"]}
        if has_cool:
            warning[RELAY_IDS["cooler"][tank]] = {
                "type": WARNING_TYPE, "message": MESSAGES["cooler"]}

    if run >= p["CONSEC_COOL_N"]:
        state = "이상"
        warning[RELAY_IDS["exchanger"][tank]] = {
            "type": WARNING_TYPE, "message": MESSAGES["exchanger_degrade"]}

    return {"state": state, "relay_warning": warning, "on_sec": round(on_sec, 1),
            "cool_overlap": has_cool, "cool_run": run}


if __name__ == "__main__":
    validate_relay_ids()
    res = diagnose("P1", "2026-09-09 10:00:00", "now()", detail=True)
    d = res["_detail"]
    print(f"\n[{d['tank']}] 사이클 {d['n_cycles']}개")
    print(f"  heat 이상일 {d['heat_abnormal_days']}, exchanger 단독 {d['exchanger_only']}, "
          f"exchanger+cool {d['exchanger_with_cool']}, 성능저하 {d['degrade_cycles']}")
    print("\n" + to_json(res))
