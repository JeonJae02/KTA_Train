"""조치(사건) ↔ 경고(에피소드) 짝짓기 후보와 라벨 파일 관리.

후보는 기계적으로 뽑고, TP/FN/FP 판정은 사람이 노트북에서 데이터를 보고
`*_check/labels_*.csv` 에 적는다. 자동 판정을 라벨로 바로 쓰지 않는다.
"""
import pandas as pd

from . import paths

#: 짝짓기 시간창 (초안 — 회의에서 확정). 조치 시작 기준.
BEFORE = pd.Timedelta(days=7)      # 조치 전 이만큼 안에 떠 있던 경고 → 사전 탐지 후보
AFTER = pd.Timedelta(hours=24)     # 조치 후 이만큼 안에 처음 뜬 경고 → '사후' 로 따로 표시

INCIDENT_LABEL_COLS = [
    "cm",                # TP / FN / OUT(모델범위 밖) / PRE(그 모델 경고 로그 시작 전) / HOLD(판단 보류)
    "matched_episodes",  # 짝지은 episode_id ; 구분
    "match_level",       # 부위 / 설비 / 불일치  (경고 문구 부위 vs 실제 조치 부위)
    "lead_h",            # 경고 onset → 조치 시작 (시간)
    "feature_shift",     # 조치 시점 전후로 움직인 우리 피처 (이름·방향·크기)
    "shift_time_ok",     # 피처 변화 시점과 조치 시점이 맞는가 Y/N/애매
    "missed_feature",    # FN 일 때 — 어떤 피처를 봤어야 했나 / 새로 필요한 신호
    "meaning_gap",       # 경고가 말한 것 vs 현장이 받아들인 것·실제 원인의 차이
    "reviewer", "note",
]

EPISODE_LABEL_COLS = [
    "cm",                # TP / FP / FIELD_CHECK(현장 확인 요청) / HOLD
    "matched_incident",  # 짝지은 incident_id
    "fp_cause",          # 현장놓침의심 / 모델과민 / 재가동과도 / 데이터결측 / 진단오류 / 기타
    "evidence",          # 판단 근거 (피처 값, 그림 파일명)
    "field_check_req",   # 현장에 확인 요청한 내용
    "field_check_result",  # 현장 회신 — 이상 있었음이면 cm 을 TP 로 바꾼다
    "reviewer", "note",
]


def incidents(actions):
    """작업ID 행 → 사건(incident_id) 한 행. 공무반·기계실 중복 기록을 하나로 본다."""
    a = actions.copy()
    a["start_dt"] = pd.to_datetime(a["start_dt"])
    a["end_dt"] = pd.to_datetime(a["end_dt"])
    a["작업일"] = pd.to_datetime(a["작업일"])

    def agg(x):
        start = x["start_dt"].min()
        return pd.Series({
            "domain": x["domain"].iloc[0],
            "device_key": "|".join(dict.fromkeys("|".join(x["device_key"].dropna()).split("|"))),
            "sub_unit": x["sub_unit"].dropna().iloc[0] if x["sub_unit"].notna().any() else None,
            "wagon_no": x["wagon_no"].dropna().iloc[0] if x["wagon_no"].notna().any() else None,
            "model_scope": x["model_scope"].iloc[0],
            "expected_reasons": x["expected_reasons"].iloc[0],
            "작업일": x["작업일"].min().date(),
            # 시각이 없으면 작업일 0시 — 시간창 비교 때 '시각 미기재' 로 구분한다
            "start_dt": start if pd.notna(start) else x["작업일"].min(),
            "start_known": pd.notna(start),
            "end_dt": x["end_dt"].max(),
            "설비상세": " / ".join(dict.fromkeys(x["설비상세"].astype(str))),
            "조치": " / ".join(dict.fromkeys(x["조치"].astype(str))),
            "생산영향": " / ".join(dict.fromkeys(x["생산영향"].astype(str))),
            "기록구간_분": x["기록구간_분"].max(),
            "작업IDs": ";".join(x["작업ID"]),
            "journals": ";".join(sorted(set(x["journal"]))),
        })

    return a.groupby("incident_id").apply(agg, include_groups=False).reset_index()


def _keys(s):
    return set(str(s).split("|")) if pd.notna(s) else set()


def _same_num(a, b):
    try:
        return pd.notna(a) and pd.notna(b) and int(float(a)) == int(float(b))
    except (TypeError, ValueError):
        return False


def candidates(inc, ep, before=BEFORE, after=AFTER):
    """사건마다 시간창 안에 걸친 경고 에피소드 후보.

    걸침 = onset ≤ 조치+after 이고 last_detected ≥ 조치−before.
    match_on: 'pump' (같은 펌프/설비 키) / 'head' (같은 헤드만 공유) / 'conveyor'.
    phase: 'before' (조치 전에 떴음) / 'after' (조치 뒤에 처음 뜸 — 사후).
    """
    ep = ep.copy()
    for c in ("onset", "last_detected"):
        ep[c] = pd.to_datetime(ep[c])
    rows = []
    for _, r in inc.iterrows():
        t0 = pd.Timestamp(r["start_dt"])
        ak = _keys(r["device_key"])
        win = ep[(ep["onset"] <= t0 + after) & (ep["last_detected"] >= t0 - before)]
        for _, e in win.iterrows():
            ek = _keys(e["device_key"])
            common = ak & ek
            if not common:
                continue
            if any(k.startswith("PUMP_") for k in common) or any(k.startswith(("TANK_",)) for k in common):
                on = "pump" if any(k.startswith("PUMP_") for k in common) else "tank"
            elif any(k.startswith("HEAD_") for k in common):
                on = "head"
            else:
                on = "conveyor"
            rows.append({
                "incident_id": r["incident_id"], "episode_id": e["episode_id"], "match_on": on,
                "action_start": t0, "onset": e["onset"], "last_detected": e["last_detected"],
                "lead_h": round((t0 - e["onset"]).total_seconds() / 3600, 1),
                "phase": "before" if e["onset"] <= t0 else "after",
                "ep_key": e["key"], "ep_sub_unit": e.get("sub_unit"), "ep_wagon": e.get("wagon_no"),
                "act_sub_unit": r["sub_unit"], "act_wagon": r["wagon_no"],
                # 컨베이어는 키가 전부 CONVEYOR 라 이 두 칸으로 좁혀 본다
                "same_unit": pd.notna(r["sub_unit"]) and str(e.get("sub_unit")) == str(r["sub_unit"]),
                "same_wagon": _same_num(e.get("wagon_no"), r["wagon_no"]),
                "reasons": e["reasons"], "max_level": e["max_level"], "max_sev": e.get("max_sev"),
                "message_first": e["message_first"],
                "expected_reasons": r["expected_reasons"],
            })
    cols = ["incident_id", "episode_id", "match_on", "action_start", "onset", "last_detected", "lead_h", "phase",
            "ep_key", "ep_sub_unit", "ep_wagon", "act_sub_unit", "act_wagon", "same_unit", "same_wagon",
            "reasons", "max_level", "max_sev", "message_first", "expected_reasons"]
    return pd.DataFrame(rows, columns=cols)


def sync_labels(df, path, key, label_cols, info_cols):
    """라벨 파일을 df 의 행 목록에 맞춘다. 기존 라벨은 절대 지우지 않는다.

    - 새 key → 라벨 칸을 빈칸으로 추가
    - 기존 key → info_cols 만 최신 값으로 갱신, 라벨 칸은 그대로
    - df 에서 사라진 key → 남겨 두고 `orphan=Y` 로 표시 (사람이 지운다)
    """
    new = df[[key] + info_cols].copy()
    if path.exists():
        old = pd.read_csv(path, dtype=str, keep_default_na=False)
        labels = old[[key] + [c for c in label_cols if c in old.columns]]
        out = new.astype(str).merge(labels, on=key, how="left")
        orphans = old[~old[key].isin(new[key].astype(str))].copy()
        if len(orphans):
            orphans["orphan"] = "Y"
            out = pd.concat([out, orphans], ignore_index=True)
    else:
        out = new.astype(str)
    for c in label_cols:
        if c not in out.columns:
            out[c] = ""
    out = out.fillna("")
    out.to_csv(path, index=False, encoding="utf-8-sig")
    return out


def confusion_table(inc_labels, ep_labels):
    """라벨 파일 두 개 → 혼동행렬 숫자.

    TP = 사건 기준 TP 수, FN = 사건 기준 FN 수, FP = 에피소드 기준 FP 수,
    FIELD_CHECK = 현장 확인 대기 에피소드 수. TN 은 설비×운영일 단위로 따로 센다.
    """
    ic = inc_labels["cm"].value_counts()
    ec = ep_labels["cm"].value_counts()
    return pd.Series({
        "TP(사건)": ic.get("TP", 0), "FN(사건)": ic.get("FN", 0), "OUT(범위밖 사건)": ic.get("OUT", 0),
        "PRE(가동 전 사건)": ic.get("PRE", 0),
        "TP(경고)": ec.get("TP", 0), "FP(경고)": ec.get("FP", 0),
        "FIELD_CHECK(경고)": ec.get("FIELD_CHECK", 0),
        "미판정 사건": (inc_labels["cm"] == "").sum(), "미판정 경고": (ep_labels["cm"] == "").sum(),
    })
