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
    df["model_ver"] = df["ts"].map(paths.version_at)
    df["domain"] = df.apply(domain_of, axis=1)
    df["device_key"] = df.apply(device_key_of, axis=1)
    df["sub_unit"] = df.apply(sub_unit_of, axis=1)
    part = df.apply(lambda r: conv_part(r) if r["kind"] == "conveyor" else (None, None), axis=1)
    df["part_device"], df["part_group"] = part.str[0], part.str[1]
    df["wagon_nos"] = df["message"].map(wagon_nos_of)
    df["wagon_no"] = df["wagon_nos"].map(lambda s: int(s.split(";")[0]) if isinstance(s, str) else None)
    return df.sort_values("ts").reset_index(drop=True)


# ---------------------------------------------------------------- 설비 매핑
def _topology():
    # v2.2 topology 는 v2.1 의 상위집합이다 (컨베이어 부위 주소 39개를 device·group 으로 세분화).
    # 두 버전 경고를 같은 부위 체계로 읽으려고 새 것을 쓴다.
    with open(paths.MODEL_VERSIONS["v2.2"]["shared"] / "topology.json", encoding="utf-8") as fh:
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


#: 컨베이어 device → 세부 위치 이름 (일지 매핑용 짧은 이름)
_DEVICE_UNIT = {"CONVEYOR_MD_FWD": "md_fwd", "CONVEYOR_MD_REV": "md_rev", "CONVEYOR_AIRBAG": "airbag"}
#: device=CONVEYOR 인 코어 센서 → 위치
_CORE_SENSOR_UNIT = {"P00140": "zone", "P00141": "zone", "P00154": "closing", "P00164": "opening"}
#: v2.1 유닛 경고 키 (유닛 하나에 지표 전부) → device
_OLD_UNIT_DEVICE = {"md_fwd": "CONVEYOR_MD_FWD", "md_rev": "CONVEYOR_MD_REV", "airbag": "CONVEYOR_AIRBAG"}
#: 유닛 지표 → 부위 group. v2.2 unit_params.METRIC_PART 와 같은 규칙 (실린더 시간은 그 SOL, 거리·에어 시간은 이동대 센서 묶음).
#: v2.1 경고(키 md_fwd 등)도 사유의 지표 이름으로 같은 부위를 매겨서 두 버전을 나란히 비교한다.
_METRIC_GROUP = {
    "stop_fwd_start": "stopper_rod", "stop_fwd_time": "stopper_rod",
    "stop_rev_start": "stopper_rod", "stop_rev_time": "stopper_rod",
    "nozzle_fwd_start": "nozzle_rod", "nozzle_rev_time": "nozzle_rod", "return_time": "slide_rod",
    "air_time": "slide_sensor", "catch_cnt": "slide_sensor", "drag_cnt": "slide_sensor",
}
_TAILS = ("_slow", "_fast", "_distance", "_trend")


def _metric_of(reason):
    m = reason or ""
    for t in _TAILS:
        if m.endswith(t):
            m = m[: -len(t)]
    return re.sub(r"_\d$", "", m)         # nozzle_rev_time_1 → nozzle_rev_time


def conv_part(r):
    """컨베이어 경고 → (device, group). v2.1·v2.2 키 형식을 같은 부위 체계로."""
    key, reason = r["key"], r["reason"]
    base = key.split(":")[0]
    if key.endswith(":wagons"):                      # 대차 묶음 — 그 대차를 본 센서
        return _RELAYS.get(base, {}).get("deviceId", "CONVEYOR"), "wagon"
    if reason == "drive_speed":
        return "CONVEYOR", "drive"
    if base in _RELAYS:                              # v2.2 부위 주소 (센서 또는 SOL)
        return _RELAYS[base]["deviceId"], _RELAYS[base]["groupId"]
    if ":" in key:                                   # v2.2 주소 없는 부위 {device}:{group}
        return tuple(key.split(":", 1))
    if base in _OLD_UNIT_DEVICE:                     # v2.1 유닛 키
        return _OLD_UNIT_DEVICE[base], _METRIC_GROUP.get(_metric_of(reason))
    return "CONVEYOR", None


def sub_unit_of(r):
    """컨베이어 세부 위치 (zone/closing/opening/md_fwd/md_rev/airbag/drive). 그 외는 빈칸."""
    if r["kind"] != "conveyor":
        return None
    dev, grp = conv_part(r)
    if grp == "drive":
        return "drive"
    if dev in _DEVICE_UNIT:
        return _DEVICE_UNIT[dev]
    return _CORE_SENSOR_UNIT.get(r["key"].split(":")[0])


_WAGON_RE = re.compile(r"대차\s*([\d·]+)")


def wagon_nos_of(msg):
    """'대차 02·03: …' / '… 대차 11: …' → '2;3' (대차 묶음 경고는 여러 대). 없으면 None."""
    m = _WAGON_RE.search(msg or "")
    if not m:
        return None
    return ";".join(str(int(x)) for x in m.group(1).split("·") if x)


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
            "wagon_nos": ";".join(sorted({w for s in x["wagon_nos"].dropna() for w in s.split(";")}, key=int)) or None,
            "model_ver": x["model_ver"].iloc[0],
            "part_device": x["part_device"].iloc[0],
            "part_group": ";".join(dict.fromkeys(x["part_group"].dropna())) or None,
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
