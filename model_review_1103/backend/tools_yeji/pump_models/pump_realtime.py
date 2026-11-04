# tools_yeji/pump_models/pump_realtime.py
"""펌프 14대 사이클 실시간 진단 — 전처리 → 채점 → 창 판정 → 진단 → 경보. 14대 직렬.
tools_yeji/Pump_Monitor 가 하나 갖고 매 tick 부른다.

    mon = PumpRealtimeGroup()                        # 한 번 (모델·기준선 읽기)
    print(mon.missing_tags(live_data.keys()))        # 처음 한 번 — 빠진 감시 태그
    alerts = mon.update(now, live_data)              # 매 tick — 진단 경보 목록 (pump_diagnose 형식)
    scores = mon.pump_scores()                       # 14대 전량 {펌프: dict 또는 None} — Pump_Score 형식
    state  = mon.status()                            # 펌프별 화면 상태 (정지/대기 순환/점검·수동/자동 사출 + 등급)

자동운전 사이클만 판정·경보한다. 수동·점검 사이클은 참고 채점만, 알람 사이클·학습 범위 밖은 제외.
수집 간격(백엔드 진단 모드 2초 틱 · 3초 넘는 공백)에 걸린 사이클도 참고 채점만 — pump_preprocess 의 가드.
이 펌프만의 태그(BuildUp·SV·Hz·FT·PT)가 row 에 있을 때만 그 펌프 tick 을 진행한다.
"""
# 자동 생성 — DB_Log/HDI_MODEL/relay_port/make_port.py. 원본: HDI_MODEL/Pump_Realtime_Model.py
import os
from collections import deque

import numpy as np
import pandas as pd

from tools_yeji.common.time_utils import to_kst
from tools_yeji.pump_models.pump_diagnose import PumpDiagnoser
from tools_yeji.pump_models.pump_judge import CycleScorer, WindowTracker
from tools_yeji.pump_models.pump_params import (ASSET_DIR, BASELINE_DIR, PUMPS, PumpBaseline, PumpSetup,
                                                 load_settings, tag_aliases)
from tools_yeji.pump_models.pump_preprocess import PumpProcessor, add_baseline_features


def _top_features(zn, k=3):
    """피처별 편차 zn → [{feature, share}] |zn| 큰 순 k 개. share = |zn| / Σ|zn|."""
    a = {f: abs(float(v)) for f, v in (zn or {}).items() if v is not None and np.isfinite(v)}
    tot = sum(a.values())
    if not tot:
        return []
    return [{"feature": f, "share": round(v / tot, 4)} for f, v in sorted(a.items(), key=lambda kv: -kv[1])[:k]]


class PumpRealtimeGroup:
    """펌프 14대. update() 는 이번 tick 의 진단 경보 목록, status()/pump_scores() 는 펌프별 현재 상태."""

    def __init__(self, pumps=None, settings=None, artifact_dir=None, baseline_dir=BASELINE_DIR, keep_cycles=2000):
        s = settings or load_settings()
        self.pumps = [p for p in (pumps or PUMPS) if p in s["pumps"]]
        self.setup = {p: PumpSetup(p, s, allow_unverified=True) for p in self.pumps}
        self.base = {p: PumpBaseline.load(os.path.join(baseline_dir, f"{p}.json")) for p in self.pumps}
        self.proc = {p: PumpProcessor(self.setup[p], self.base[p]) for p in self.pumps}
        self.scorer = CycleScorer.load(artifact_dir or ASSET_DIR)
        self.win = WindowTracker(self.scorer.meta)
        self.diag = PumpDiagnoser(s)
        self.aliases = tag_aliases(s)
        self.tags = {p: set(self.setup[p].required_tags()) for p in self.pumps}
        # 이 펌프만의 태그(BuildUp·SV·Hz·FT·PT)가 row 에 있어야 tick 을 진행한다. 나머지(탱크 온도 — 같은 탱크 펌프끼리,
        # 사출 신호 — 같은 헤드 POL·ISO 끼리, 알람·가동 플래그 — 전부)는 여러 펌프가 같이 쓰는 태그라 값만 기억한다
        self.own = {p: {self.setup[p].tags[k] for k in ("bu", "sv", "hz", "ft", "pt")} for p in self.pumps}
        self.context = {p: self.tags[p] - self.own[p] for p in self.pumps}
        self.run_tag = s["run_tag"]
        self.circ_min_ft = s["params"]["CIRC_MIN_FT"]
        self.tick_state = {p: "정지" for p in self.pumps}     # 마지막 tick 의 가동 상태
        self.last = {p: None for p in self.pumps}           # 마지막 사이클 판정
        self.cycles = deque(maxlen=keep_cycles)             # 최근 사이클 판정 기록 (확인용, None = 전부)
        self.notified = deque(maxlen=None if keep_cycles is None else 200)   # 사람에게 간 경보 (show=True) 기록
        self._alerts = []                                   # 마지막으로 계산한 경보 (show=False 로 이어 낸다)

    # ------------------------------------------------------------------ #
    def missing_tags(self, keys):
        """백엔드 row 키 목록을 받아, 대신할 이름도 없는 필요 태그를 펌프별로 돌려준다."""
        keys = set(keys)
        out = {}
        for p in self.pumps:
            miss = [t for t in sorted(self.tags[p]) if t not in keys and not any(a in keys for a in self.aliases.get(t, []))]
            if miss:
                out[p] = miss
        return out

    def _alias(self, row):
        row = dict(row)
        for tag, alts in self.aliases.items():
            if row.get(tag) is None:
                for a in alts:
                    if row.get(a) is not None:
                        row[tag] = row[a]
                        break
        return row

    # ------------------------------------------------------------------ #
    def update(self, current_time, row):
        """row: {태그/주소: 원시 PLC 값}. 반환: 이번 tick 의 진단 경보 [diagnose 형식]."""
        t = to_kst(current_time)
        row = self._alias(row)
        keys = set(row)
        changed = False
        for p in self.pumps:
            if not (self.own[p] & keys):
                # 이 펌프 신호가 없는 row — 같이 쓰는 태그 값만 기억하고 tick 은 진행하지 않는다.
                # (진행하면 다른 펌프 row 마다 묵은 값으로 tick 이 늘어 증압·안정 구간 길이가 틀어진다)
                for k in self.context[p] & keys:
                    if row[k] is not None:
                        self.proc[p].last[k] = row[k]
                continue
            o = self.proc[p].update(t, row)
            self._tick_state(p)
            if o["cycle"] is not None:
                self._on_cycle(p, o["cycle"], t)
                changed = True
        if not changed:
            return self._alerts                             # 사이클이 안 끝난 tick — 같은 경보를 show=False 로 유지
        alerts = self.diag.alerts(t)
        for a in alerts:
            if a["show"]:
                self.notified.append({"time": t, **a})
        self._alerts = [dict(a, show=False) for a in alerts]
        return alerts

    def _tick_state(self, p):
        pr, tg = self.proc[p], self.setup[p].tags
        bu = int(pr._get("bu", 0) or 0)
        ft = float(pr._get("ft", 0) or 0)
        run = pr.last.get(self.run_tag, 1)
        if bu:
            self.tick_state[p] = "자동 사출" if run else "점검·수동"
        else:
            self.tick_state[p] = "대기 순환" if ft > self.circ_min_ft else "정지"

    def _on_cycle(self, p, cy, t):
        rec = {"pump": p, "t_start": cy["t_start"], "t_judge": t, "mode": None, "level": None, "score": np.nan,
               "zmax": np.nan, "reason": None, "pump_level": None, "notify": False}
        if not (cy.get("FT", 0) > 0):
            rec["mode"] = "유량 없음"
        else:
            r = self.scorer.score_one(add_baseline_features(cy, self.base[p]), p)
            rec.update({"score": r["score"], "zmax": r["zmax"], "level": r["level"],
                        "reason": f"{r['top_feature']} {r['top_dir']}", "group": r["group"]})
            if not r["in_scope"]:
                rec["mode"] = "판정 범위 밖"
            elif cy.get("flag_cadence"):
                rec["mode"] = "수집 간격"                     # 0.5초 틱이 아닌 구간(진단 모드·공백) — 참고값만
            elif cy.get("flag_stopped"):
                rec["mode"] = "점검·수동"                     # 참고값만 — 창 판정·경보 없음
            elif cy.get("flag_alarm"):
                rec["mode"] = "알람"
            else:
                rec["mode"] = "자동 운전"
                zn = {f: r[f"zn_{f}"] for f in self.scorer.meta["features"]}
                w = self.win.update(p, r["group"], r["score"], r["zmax"], t=cy["t_start"],
                                    reason=rec["reason"], level=r["level"], zn=zn)
                self.diag.update(p, w, cy["t_start"])
                rec.update({"pump_level": w["pump_level"], "notify": w["notify"], "window_reason": w["reason"], "zn": zn})
        self.last[p] = rec
        self.cycles.append(rec)

    # ------------------------------------------------------------------ #
    def status(self):
        """펌프별 화면 상태. state: 정지 / 대기 순환 / 점검·수동 / 자동 사출, level: 마지막 판정 등급."""
        out = {}
        for p in self.pumps:
            r = self.last[p] or {}
            d = self.diag.state.get(p)
            out[p] = {"state": self.tick_state[p], "last_mode": r.get("mode"), "cycle_level": r.get("level"),
                      "pump_level": r.get("pump_level"), "score": r.get("score"), "reason": r.get("window_reason"),
                      "diagnosis": d["code"] if d else None, "last_cycle": r.get("t_start")}
        return out

    def pump_scores(self):
        """백엔드 PumpMonitor.check() 반환 형식 — 14개 전량 {펌프: dict 또는 None}.
        None = 판정 없음(정지·순환·점검·범위 밖, 아직 사이클 없음). over = 펌프 등급이 알림
        (백엔드 Pump_Monitor 는 over 를 '경보를 띄우는 중'(근무 끝까지 유지)으로 덮어쓴다).
        top: 마지막 사이클 피처 편차 |zn| 상위 3개와 비중 — Health_Digest 가 '기여 피처 %' 로 읽는다."""
        out = {}
        for p in self.pumps:
            r = self.last[p]
            if not r or r["mode"] != "자동 운전":
                out[p] = None
                continue
            g = r.get("group")
            thr = self.scorer.thr.get(g, {}).get("alert", {}).get("cycle", np.nan)
            out[p] = {"mse": None if pd.isna(r["score"]) else round(float(r["score"]), 6),
                      "threshold": None if pd.isna(thr) else round(float(thr), 6),
                      "over": r["pump_level"] == "알림", "level": r["pump_level"], "cycle_level": r["level"],
                      "top": _top_features(r.get("zn")),
                      "wagon": None, "tick": None}
        return out
