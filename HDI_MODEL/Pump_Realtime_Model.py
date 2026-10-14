"""펌프 14대 사이클 실시간 진단 — 전처리 → 사이클 AE → 창 판정 → 진단 → 경보. 14대 직렬.

`Tank_Realtime_Model.py` 와 같은 사용법 (반복문 밖에서 객체 생성, 안에서 update 만):

    mon = PumpRealtimeGroup()                       # 반복문 밖에서 한 번 (모델·기준선·topology 읽기)
    print(mon.missing_tags(live_data.keys()))        # 처음 한 번 — 빠진 태그 확인
    while True:
        alerts = mon.update(current_time, live_data) # [{reason, priority, type, message, show, pumps, ...}] — 진단 경보
        scores = mon.pump_scores()                   # {펌프: 판정 dict 또는 None} — 백엔드 Pump_Score 형식 (14개 전량)

값은 **원시 PLC 값** 그대로. 이번 tick 에 안 들어온 태그는 빼거나 None 으로 두면 마지막 값을 이어서 쓴다.

## 한 tick 에 하는 일 (펌프마다 순서대로)

    PumpProcessor.update  (전처리기, 상태기계)        → 사이클이 끝나면 cycle 피처 한 줄
      └ add_baseline_features (baseline/{펌프}.json) → 펌프 잔차, 압력/기대
      └ CycleScorer.score_one (cycle_ae/artifacts)   → 점수, 사이클 등급, 이유, 피처별 편차 zn
      └ WindowTracker.update                         → 펌프 등급, 알림(운영일 1회), 창 편차 중앙값
      └ PumpDiagnoser.update                         → 유량계·토출·배관·헤드 판정
    PumpDiagnoser.alerts                              → 경보 목록 (이 tick 의 반환값)

사이클은 펌프마다 10~30초에 하나라 채점은 드물다(14대 합쳐 초당 0.35회, 1회 약 3ms). 나머지 tick 은 상태기계만 돈다.

## 어떤 사이클을 판정에 넣나

| 사이클 | 채점 | 창 판정·진단·경보 | 화면 상태 (`status`) |
|---|---|---|---|
| 자동운전 중 (`Auto_Running=1`), 알람 없음, 모델 범위 안 | O | O | 정상 / 주의 / 알림 |
| 수동·점검 (`Auto_Running=0` 인데 사출) | O (참고값) | **X** — 재가동 첫 사출 등은 평소와 다른 게 당연 (10/11 P1 사례) | 점검·수동 |
| 알람 사이클 | O (참고값) | X | 알람 |
| 수집 간격 — 0.5초 틱이 아닌 구간(백엔드 진단 모드 2초 틱, 3초 넘는 공백) | O (참고값) | X | 수집 간격 |
| 학습 범위 밖 묶음 | X | X | 판정 범위 밖 |

사이클 사이 상태: 순환 Hz 로만 돌면 **대기 순환**, FT 도 없으면 **정지** — "정상" 이 아니라 "판정 없음" 이다.

## 태그 — 백엔드 감시 태그(tag_config)와 맞추기

전처리기가 쓰는 태그 이름 중 두 가지는 백엔드에 다른 이름으로 있다 — `TAG_ALIASES` 로 자동 대응:

- `Scale_Out___TT_{탱크}` → `TK_Temp_PV_{탱크}` (같은 센서, 2026-10-10 실측 100% 일치)
- `Auto_Running` → `Auto_Running_PL` (99.94% 일치, 자동운전 시작 때 1~2초 깜빡임만 다름)

**백엔드 감시 태그에 추가해야 하는 것**: `Hz_Out_{펌프}` 14개, `Shot_Injection_HD{n}_P{m}` 7개.
알람 비트(`AL_*_Bit`) 4개는 없어도 돈다(알람 사이클 표시만 빠진다). `missing_tags()` 로 확인.

재생(replay)처럼 펌프마다 따로 기록된 row 가 섞여 들어와도 맞게 돈다: 이 펌프만의 태그(BuildUp·SV·Hz·FT·PT)가 있는
row 에서만 tick 을 진행하고, 같이 쓰는 태그(탱크 온도·사출 신호·알람·가동 플래그)는 값만 기억한다.

## 검증 (재생 = 캐시 원시 틱을 시간순으로 update 에 넣기)

- 10/10 하루 (14대, 210만 행, 2.7분): 사람에게 간 경보 11건이 배치 검증(`diagnose.py --validate`)과 **펌프·시각·판정 모두 같다**
  (시각은 사이클 끝 + 복귀창만큼 몇 초 늦다)
- 10/11 점검 날: P1·I1 수동 사출 4사이클 → 점검·수동(참고 채점만), 경보 0, 14대 모두 정지
- 실시간 부하: tick 마다 14대 상태기계(대당 약 20µs) + 사이클이 끝날 때만 채점·진단(약 3ms)

## 백엔드 연결 (HDI_Relay/backend/tools_yeji)

`relay_port/make_port.py` 가 백엔드 구조로 뽑아 준다 (`relay_port/PORTING.md`). 백엔드에서는 `Pump_Monitor` 가
이 묶음을 갖고, 매 tick `update` → 경보를 근무 끝까지 유지 → `pump_scores()` 에 붙여 `Pump_Score` 로 낸다.
PredictiveEngine 이 `over` 인 것만 `Pump_Anomaly` 로 프론트(3D 펌프 마커)에 보낸다. `Relay_Monitor` 는 그대로(탱크·헤드).

확인할 것: 백엔드 live_data 가 들어오는 간격이 로거처럼 약 0.5초인지 — 상태기계의 안정 구간 판정(`STEADY_MIN` 등)이
틱 개수 기준이라 간격이 많이 다르면 `pump_settings.json` 파라미터를 다시 맞춰야 한다.
"""

import os
import sys
from collections import deque

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "cycle_ae"))

from new_pump_preprocessor import (PumpBaseline, PumpProcessor, PumpSetup, add_baseline_features,  # noqa: E402
                                   load_settings)
import model as cae  # noqa: E402
from diagnose import PumpDiagnoser  # noqa: E402

BASELINE_DIR = os.path.join(HERE, "baseline")
KST = "Asia/Seoul"
PUMPS = ["P1", "I1", "P2", "I2", "P3", "I3", "P4", "I4", "P5", "I5", "P6", "I6", "P7", "I7"]


def tag_aliases(settings):
    """전처리기 태그 이름 → 백엔드에서 대신 올 수 있는 이름들."""
    al = {settings["run_tag"]: ["Auto_Running_PL"]}
    for p, v in settings["pumps"].items():
        al[f"Scale_Out___TT_{v['tank']}"] = [f"TK_Temp_PV_{v['tank']}"]
    return al


def to_kst_naive(t):
    """epoch 초 / tz 있는 시각 / tz 없는 시각(KST 로 본다) → tz 없는 KST Timestamp (캐시·학습과 같은 형식)."""
    if isinstance(t, (int, float, np.integer, np.floating)):
        return pd.Timestamp(t, unit="s", tz="UTC").tz_convert(KST).tz_localize(None)
    t = pd.Timestamp(t)
    return t.tz_convert(KST).tz_localize(None) if t.tzinfo is not None else t


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
        self.scorer = cae.CycleScorer.load(artifact_dir or cae.ARTIFACT_DIR)
        self.win = cae.WindowTracker(self.scorer.meta)
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
        t = to_kst_naive(current_time)
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


# --------------------------------------------------------------------------- #
# 재생 — 캐시 원시 데이터를 시간순으로 흘려 실시간 판정을 재현
# --------------------------------------------------------------------------- #
def replay(start, end, pumps=None, verbose=True):
    """cache/pump_raw 의 원시 틱을 펌프 섞어 시간순으로 update 에 넣는다.
    반환: (PumpRealtimeGroup, 사람에게 간 경보 DataFrame, 사이클 판정 DataFrame)."""
    from extract_pump_cache import load_range
    import time
    mon = PumpRealtimeGroup(pumps, keep_cycles=None)
    frames = []
    for p in mon.pumps:
        x = load_range(p, start[:10], end[:10])
        x = x[(x.index >= start) & (x.index < end)]
        x = x.select_dtypes("number")
        cols = [c for c in x.columns if c in mon.tags[p]]
        frames.append((p, x.index.to_numpy(), x[cols].to_numpy(float), cols))
    # 시간순 병합: (시각, 펌프 번호, 행 번호)
    order = np.concatenate([np.c_[np.arange(len(ix)), np.full(len(ix), k)] for k, (_, ix, _, _) in enumerate(frames)])
    times = np.concatenate([ix for _, ix, _, _ in frames])
    srt = np.argsort(times, kind="stable")
    t0 = time.time()
    for n, i in enumerate(srt):
        k, j = order[i, 1], order[i, 0]
        p, ix, vals, cols = frames[k]
        v = vals[j]
        row = {c: (None if np.isnan(x) else x) for c, x in zip(cols, v)}
        mon.update(times[i], row)
        if verbose and n and n % 1_000_000 == 0:
            print(f"  {n:,}/{len(srt):,} 행 ({time.time() - t0:.0f}s)", flush=True)
    return mon, pd.DataFrame(list(mon.notified)), pd.DataFrame(list(mon.cycles))


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-10-10 00:00:00")
    ap.add_argument("--end", default="2026-10-11 00:00:00")
    a = ap.parse_args()
    mon, alerts, cyc = replay(a.start, a.end)
    print(f"사이클 {len(cyc):,}개 — 모드별", cyc["mode"].value_counts().to_dict() if len(cyc) else {})
    if len(alerts):
        print(alerts[["time", "label", "reason", "relay", "relay_fallback", "message"]].to_string(index=False))
    print(pd.DataFrame(mon.status()).T.to_string())
