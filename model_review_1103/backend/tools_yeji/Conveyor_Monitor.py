# tools_yeji/Conveyor_Monitor.py
import json
import os
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

from tools_yeji.common.time_utils import hold_until, to_kst, to_utc_iso
from tools_yeji.conveyor_models.conveyor_realtime import EVAL_SEC, ConveyorRealtimeGroup

KST = ZoneInfo("Asia/Seoul")
#: 가동일 기록 (오전 6시에 하루치를 더한다). 재시작해도 승인·복귀 판정과 "언제부터" 가 이어진다
DATA_DIR = "./data"
#: 근무 단위 유지 상태. 코어는 재시작 뒤 1–2시간 판정을 못 하므로, 그동안 떠 있던 경고를 이어 붙이려고 남긴다
HOLD_FILE = "conveyor_warn_hold.json"
#: 마지막 감지 뒤 이만큼 지나야 active=False — 판정(EVAL_SEC) 2번 연속 빠져야 해소로 본다.
#: 유닛 하루 평균이 기준선 근처를 오가면 판정 1번마다 켜졌다 꺼졌다 한다 (10-21 실데이터)
CLEAR_SEC = 1.5 * EVAL_SEC
#: hold 항목에서 파일에 그대로 쓰는 필드 (시각 필드는 따로 변환)
HOLD_FIELDS = ("type", "unit", "unit_name", "name", "message", "priority", "since", "active")


class ConveyorMonitor:
    """컨베이어 경고. 판정은 conveyor_models, 얼마나 오래 띄울지(근무 단위 유지)는 여기서.

    check() 반환은 (스냅샷, 경고 목록).
    - 스냅샷: 키오스크 그림용 전체 상태 — PredictiveHealth 가 `conveyor` 로 낸다 (유지 없음, 판정 그대로)
    - 경고: [{time, conv, type, message, reason, active, last_detected, duration_sec, hold_until,
      unit, unit_name, name, sev, since}] — `Conveyor_Anomaly` 로 낸다. conv 앞뒤는 Relay_Data(relay)·
      Pump_Anomaly(pump) 와 같은 이름·같은 뜻이고, unit 부터는 컨베이어만 갖는다.
    필드 설명은 shared/응답 예시/idle_data.md.

    Relay_Monitor·Pump_Monitor 와 다른 점 둘:
    - 판정이 없는 conv(재시작 직후 코어 센서)는 해소됐는지 모르므로 유지를 붙잡아 둔다
    - 유지 상태를 파일로 남긴다 — 그 판정 공백 동안 떠 있던 경고가 해소로 기록되지 않게
    """

    def __init__(self, data_dir=DATA_DIR):
        self._rt = ConveyorRealtimeGroup(data_dir=data_dir)
        self._tags_checked = False
        #: (conv, 사유) -> {type, unit, unit_name, name, message, priority, since, expires, last, active}
        self._hold = {}
        #: conv -> 처음 띄운 시각(KST). 경고의 time 이 된다
        self._since = {}
        self._hold_path = os.path.join(data_dir, HOLD_FILE) if data_dir else None
        self._saved_for = None
        self._load_hold()

    def check(self, live_data, now=None):
        """live_data 는 감시 태그(tag_config) 로 걸러진 {주소/태그명: 원시 PLC 값}. now 는 재생 확인용(epoch 초)."""
        now = time.time() if now is None else now
        if not self._tags_checked and live_data:
            missing = self._rt.missing_tags(live_data.keys())
            if missing:
                print(f"⚠️ [ConveyorMonitor] 감시 태그 없음 (tag_config 확인): {missing}")
            self._tags_checked = True

        alerts = self._rt.update(datetime.fromtimestamp(now, KST), live_data)
        snapshot = self._rt.snapshot()
        self._apply_hold(to_kst(now), alerts, self._rt.judged())
        warnings = self._held_warnings()
        # 판정은 EVAL_SEC 마다 바뀐다 — 그때만 저장한다 (그 사이 last 몇십 초는 재시작 때 잃어도 된다)
        if snapshot["updated"] != self._saved_for:
            self._save_hold()
            self._saved_for = snapshot["updated"]
        return snapshot, warnings

    def _apply_hold(self, t, alerts, judged):
        """경보를 "t 이후 처음 시작하는 근무가 끝날 때까지" 유지한다 (Relay_Monitor._apply_hold 와 같은 규칙).

        판정이 있는 conv 는 마지막 감지 뒤 CLEAR_SEC 가 지나면 active=False (해소, 근무 끝까지 표시).
        판정이 없는 conv(재시작 직후)는 active 를 그대로 두고 만료도 미룬다 — 판정이 돌아오면 그때 정한다.
        """
        until = hold_until(t)
        for (conv, _), h in self._hold.items():
            if conv in judged:
                h["active"] = (t - h["last"]).total_seconds() < CLEAR_SEC
            else:
                h["expires"] = max(h["expires"], until)
        for a in alerts:
            self._hold[(a["conv"], a["reason"])] = {
                **{f: a[f] for f in ("type", "unit", "unit_name", "name", "message", "since")},
                "priority": -a["sev"], "expires": until, "last": t, "active": True}
        for key in [k for k, h in self._hold.items() if h["expires"] <= t]:
            del self._hold[key]

    def _held_warnings(self):
        """conv 마다 유지 중인 사유를 하나로 합친다. Relay_Monitor._held_warnings 와 같은 규칙.

        ActiveFaultRegistry 는 쓰지 않는다 — 처음 띄운 시각(time)을 재시작 뒤에도 이어 가려고 self._since 에서 낸다.
        """
        by_conv = {}
        for (conv, reason), h in self._hold.items():
            by_conv.setdefault(conv, []).append((reason, h))
        self._since = {k: self._since.get(k, min(h["last"] for _, h in held)) for k, held in by_conv.items()}

        out = []
        for conv, held in by_conv.items():
            reason, best = min(held, key=lambda rh: rh[1]["priority"])
            last = max(h["last"] for _, h in held)
            # 앞 9개는 Relay_Monitor 와 같은 순서·이름. 뒤는 컨베이어만
            out.append({"time": to_utc_iso(self._since[conv]), "conv": conv,
                        "type": best["type"], "message": best["message"], "reason": reason,
                        "active": any(h["active"] for _, h in held),
                        "last_detected": to_utc_iso(last),
                        "duration_sec": round((last - self._since[conv]).total_seconds(), 1),
                        "hold_until": to_utc_iso(max(h["expires"] for _, h in held)),
                        **{f: best[f] for f in ("unit", "unit_name", "name")},
                        "sev": -best["priority"], "since": best["since"]})
        return out

    # ------------------------------------------------------------------ #
    # 유지 상태 저장 — 재시작해도 떠 있던 경고가 해소로 기록되지 않게
    # ------------------------------------------------------------------ #
    def _save_hold(self):
        if not self._hold_path:
            return
        data = {"hold": [{"conv": conv, "reason": reason, **{f: h[f] for f in HOLD_FIELDS},
                          "expires": h["expires"].isoformat(), "last": h["last"].isoformat()}
                         for (conv, reason), h in self._hold.items()],
                "since": {k: v.isoformat() for k, v in self._since.items()}}
        try:
            tmp = self._hold_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
            os.replace(tmp, self._hold_path)
        except Exception as e:
            print(f"⚠️ [ConveyorMonitor] 경고 유지 상태 저장 실패: {e}")

    def _load_hold(self):
        if not self._hold_path or not os.path.exists(self._hold_path):
            return
        try:
            with open(self._hold_path, encoding="utf-8") as f:
                data = json.load(f)
            self._hold = {(r["conv"], r["reason"]): {**{f: r[f] for f in HOLD_FIELDS},
                                                       "expires": pd.Timestamp(r["expires"]),
                                                       "last": pd.Timestamp(r["last"])}
                          for r in data.get("hold", [])}
            self._since = {k: pd.Timestamp(v) for k, v in data.get("since", {}).items()}
        except Exception as e:
            print(f"⚠️ [ConveyorMonitor] 경고 유지 상태 읽기 실패 (처음부터 모음): {e}")
            self._hold, self._since = {}, {}
