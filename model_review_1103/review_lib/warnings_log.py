"""엣지 경고 이력(warnings.jsonl*) → 이벤트 표 / 에피소드 표.

경고 로그의 구조 (2026-10-15 ~ 11-10 실측):

- 한 줄 = 상태 변화 하나. event 는 raised · updated · cleared.
- `time` 은 그 경고가 처음 뜬 시각이고 updated 내내 그대로다. 그래서
  **(kind, key, time) 하나가 경고 한 건(에피소드)** 이다.
- 에피소드 안에서 reason 이 바뀔 수 있다 (펌프 진단이 바뀜, 331건 중 31건).
  그래서 reason 은 에피소드의 속성이 아니라 목록으로 남긴다.
- `warnings.jsonl` (확장자 없음)은 그날 쓰는 중인 파일이라 날짜 파일과 줄이 겹친다.
  완전히 같은 줄은 지운다.
- 시각은 UTC(…Z). 여기서 KST naive 로 바꾼다. 일지가 KST 다.
- cleared 시각은 근무 구간 끝(hold_until)에 몰린다. 짝짓기에는 onset 과
  last_detected 를 쓴다.
"""
import json
import re

import pandas as pd

from . import paths

_TS_COLS = ("ts", "time", "last_detected", "hold_until")


def _to_kst(s):
    return pd.to_datetime(s, utc=True, errors="coerce").dt.tz_convert("Asia/Seoul").dt.tz_localize(None)


def load_events(jsonl_dir=None):
    """경고 로그 전체를 한 줄 = 한 이벤트 DataFrame 으로."""
    jsonl_dir = jsonl_dir or paths.WARNINGS_JSONL_DIR
    rows = []
    for f in sorted(jsonl_dir.glob("warnings.jsonl*")):
        with open(f, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    d = json.loads(line)
                    d["_file"] = f.name
                    rows.append(d)
    df = pd.DataFrame(rows)
    df["top"] = df["top"].apply(lambda v: json.dumps(v, ensure_ascii=False) if isinstance(v, list) else None)
    dedup_cols = [c for c in df.columns if c != "_file"]
    df = df.drop_duplicates(subset=dedup_cols).reset_index(drop=True)
    for c in _TS_COLS:
        df[c] = _to_kst(df[c])
    df["domain"] = df.apply(domain_of, axis=1)
    df["device_key"] = df.apply(device_key_of, axis=1)
    df["sub_unit"] = df.apply(sub_unit_of, axis=1)
    df["wagon_no"] = df["message"].map(wagon_no_of)
    return df.sort_values("ts").reset_index(drop=True)


# ---------------------------------------------------------------- 설비 매핑
def _topology():
    with open(paths.ROOT / "shared" / "topology.json", encoding="utf-8") as fh:
        return json.load(fh)


def _pump_head():
    with open(paths.BACKEND / "models" / "pump_cycle" / "pump_settings.json", encoding="utf-8") as fh:
        return {k: v["head"] for k, v in json.load(fh)["pumps"].items()}


_RELAYS = _topology()["relays"]
_PUMP_HEAD = _pump_head()


def domain_of(r):
    """경고를 어느 *_check 에서 볼지. 펌프와 헤드는 같이, 탱크는 etc."""
    if r["kind"] == "conveyor":
        return "conveyor"
    if r["kind"] == "pump":
        return "pump"
    dev = _RELAYS.get(r["key"], {}).get("deviceId", "")
    return "pump" if dev.startswith("HEAD_") else "etc"


def device_key_of(r):
    """topology 설비 ID. 펌프는 'PUMP_P7|HEAD_HD4' 처럼 헤드도 붙인다."""
    if r["kind"] == "pump":
        return f"PUMP_{r['key']}|HEAD_{_PUMP_HEAD.get(r['key'], '?')}"
    if r["kind"] == "relay":
        return _RELAYS.get(r["key"], {}).get("deviceId", "?")
    return "CONVEYOR"


_CONV_UNIT_BY_REASON = {"drive_speed": "drive"}
_CONV_UNIT_BY_KEY = {
    "P00451": "md_fwd", "P00451:wagons": "md_fwd", "md_fwd": "md_fwd", "CONVEYOR_MD_FWD:slide_sensor": "md_fwd",
    "P00459:wagons": "md_rev", "md_rev": "md_rev", "CONVEYOR_MD_REV:slide_sensor": "md_rev",
    "P00474": "md_rev", "P00470": "md_fwd",
    "airbag": "airbag", "P0028A": "airbag", "P0028B": "airbag",
    "P00140:wagons": "zone", "P00141:wagons": "zone",
    "P00154:wagons": "closing", "P00164:wagons": "opening",
    "drive": "drive", "CONVEYOR:drive": "drive",
}


def sub_unit_of(r):
    """컨베이어 세부 위치 (zone/closing/opening/md_fwd/md_rev/airbag/drive). 그 외는 빈칸.

    릴레이 주소 → 유닛은 conveyor_models/unit_params.py UNITS 의 *_sol 정의,
    센서 비트 → 위치는 core_params.py 센서 정의를 따랐다.
    """
    if r["kind"] != "conveyor":
        return None
    return _CONV_UNIT_BY_KEY.get(r["key"]) or _CONV_UNIT_BY_REASON.get(r["reason"])


_WAGON_RE = re.compile(r"대차\s*(\d+)")


def wagon_no_of(msg):
    m = _WAGON_RE.search(msg or "")
    return int(m.group(1)) if m else None


# ---------------------------------------------------------------- 에피소드
def build_episodes(ev):
    """(kind, key, time) 별 한 행. onset·last_detected·cleared, 사유 목록, 최고 등급."""
    ev = ev[ev["time"].notna()].copy()
    g = ev.groupby(["kind", "key", "time"], sort=False)

    def agg(x):
        x = x.sort_values("ts")
        reasons = list(dict.fromkeys(x["reason"].dropna()))
        cleared = x.loc[x["event"] == "cleared", "ts"]
        levels = set(x["level"].dropna())
        top_feats = []
        for t in x["top"].dropna():
            top_feats += [d["feature"] for d in json.loads(t)]
        return pd.Series({
            "domain": x["domain"].iloc[0],
            "device_key": x["device_key"].iloc[0],
            "sub_unit": x["sub_unit"].dropna().iloc[0] if x["sub_unit"].notna().any() else None,
            "wagon_no": x["wagon_no"].dropna().iloc[0] if x["wagon_no"].notna().any() else None,
            "reason_first": reasons[0] if reasons else None,
            "reasons": ";".join(reasons),
            "message_first": x["message"].iloc[0],
            "type": x["type"].iloc[0],
            "max_level": "알림" if "알림" in levels else ("주의" if "주의" in levels else None),
            "max_sev": x["sev"].max() if "sev" in x else None,   # 컨베이어 등급 (2·3 만 로그에 남음)
            "top_features": ";".join(pd.Series(top_feats).value_counts().index[:5]) if top_feats else None,
            "first_logged": x["ts"].iloc[0],
            "last_detected": x["last_detected"].max(),
            "cleared": cleared.iloc[-1] if len(cleared) else pd.NaT,
            "has_raised": (x["event"] == "raised").any(),
            "n_events": len(x),
        })

    ep = g.apply(agg, include_groups=False).reset_index().rename(columns={"time": "onset"})
    # 로그 시작(10-15) 전에 이미 떠 있던 경고는 raised 가 없다 → onset 이 진짜 시작이 아닐 수 있음
    ep["onset_before_log"] = ~ep["has_raised"]
    ep["duration_h"] = (ep["last_detected"] - ep["onset"]).dt.total_seconds() / 3600
    ep = ep.sort_values("onset").reset_index(drop=True)
    ep.insert(0, "episode_id", [f"W{i:04d}" for i in range(1, len(ep) + 1)])
    return ep
