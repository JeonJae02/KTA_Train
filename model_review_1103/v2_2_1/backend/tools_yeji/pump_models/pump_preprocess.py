# tools_yeji/pump_models/pump_preprocess.py
"""펌프 전처리 — 틱을 하나씩 받는 상태기계(PumpProcessor), 사이클 피처, 기준선 피처.

사이클이 끝나면(복귀창 5초 뒤) 사이클 피처 한 줄을 낸다. 배치 함수(run, circulation_windows)는 넣지 않았다.
"""
# 자동 생성 — DB_Log/HDI_MODEL/relay_port/make_port.py. 원본: HDI_MODEL/new_pump_preprocessor.py
from collections import deque

import numpy as np
import pandas as pd

from tools_yeji.pump_models.pump_params import PumpSetup  # noqa: F401 (형식 참고)


# --------------------------------------------------------------------------- #
# 작은 계산 도구
# --------------------------------------------------------------------------- #
def crossing_time(t, v, lo, hi=None):
    """v 가 처음으로 [lo, hi] (hi 없으면 lo 이상) 에 들어간 시각. 진입 직전의 **실제 기록점**
    (같은 값이 이어지면 ffill 된 묵은 값이라 거슬러 감) 과 진입 틱 사이 직선 보간 (설계정리 6절)."""
    inside = v >= lo if hi is None else (v >= lo) & (v <= hi)
    if not inside.any():
        return np.nan
    j = int(np.argmax(inside))
    if j == 0:
        return 0.0
    k = j - 1
    while k > 0 and v[k] == v[k - 1]:
        k -= 1
    line = lo if v[k] < lo else hi
    if v[j] == v[k]:
        return float(t[j])
    return float(t[k] + (line - v[k]) / (v[j] - v[k]) * (t[j] - t[k]))


def half_time(t, v, start, floor):
    """꺼진 뒤 v 가 start 에서 floor 쪽으로 절반을 처음 지난 시각(초). t 는 마지막 켜짐 틱 기준,
    start 를 t=0 에 두고 같은 값(묵은 틱)을 뺀 기록점 사이를 보간한다 (new_vis 15)."""
    if not (np.isfinite(floor) and np.isfinite(start)) or start == floor:
        return np.nan
    tr, vr = np.r_[0.0, t], np.r_[start, v]
    keep = np.r_[True, np.diff(vr) != 0]
    tr, vr = tr[keep], vr[keep]
    level = 0.5 * (start + floor)
    hit = np.where(vr <= level if start > floor else vr >= level)[0]
    if len(hit) == 0 or hit[0] == 0:
        return np.nan
    i = hit[0]
    return float(tr[i - 1] + (level - vr[i - 1]) / (vr[i] - vr[i - 1]) * (tr[i] - tr[i - 1]))


def _mode(a):
    vals, cnt = np.unique(a, return_counts=True)
    return vals[np.argmax(cnt)]


def cycle_features(t, sv, hz, ft, pt, tt_raw, shot, decay_t, decay_ft, decay_pt,
                   prev_hz, gap_sec, p0, setup):
    """t·sv·hz·ft·pt·tt_raw·shot : 켜진 동안 틱 (t = 켜진 순간 기준 초, shot = 사출 영향 틱)
    decay_*                       : 꺼진 뒤 복귀창 틱 (t = 마지막 켜짐 틱 기준 초)
    반환: dict (기준선 무관 피처). 안정 구간이 부족하면 None."""
    p, n = setup.p, len(t)
    if n < p.MIN_CYCLE_TICKS:
        return None
    hz_c, sv_c = _mode(hz), _mode(sv)          # 대표 Hz · SV 클래스 (사이클 대부분을 차지한 값)
    idx = np.where(hz == hz_c)[0]
    stable = np.zeros(n, bool)
    if len(idx) > p.STEADY_MIN:
        stable[idx[p.STEADY_MIN:]] = True
    clean, src = stable & ~shot, "clean"
    if clean.sum() < p.MIN_CLEAN_TICKS:
        # 사출이 안정 구간을 거의 덮는 헤드(HD2 다단 사출 등) — 사출 중 안정 틱으로 대신하고 출처를 남긴다.
        # PT·K 는 노즐이 열린 상태라 clean 사이클과 섞어 비교하지 말 것 (steady_src 로 나눠 볼 것)
        clean, src = stable.copy(), "shot"
    if clean.sum() < p.MIN_CLEAN_TICKS or hz_c <= 0 or sv_c <= 0:
        return None
    ft_s, pt_s = float(np.median(ft[clean])), float(np.median(pt[clean]))
    tt_med = float(np.median(tt_raw))
    err = (sv - ft) / np.where(sv > 0, sv, np.nan) * 100.0
    f = {
        # A. 조건
        "SV_class": float(sv_c), "Hz": float(hz_c), "T_C": tt_med / p.TEMP_SCALE, "mu_rel": float(setup.visc(tt_med)),
        "dHz_prev": float(hz[0] - prev_hz) if np.isfinite(prev_hz) else np.nan,
        "P0": float(p0), "Gap_sec": float(gap_sec),
        # B. 품질 (SV)
        "FT_SV": ft_s / sv_c,
        "t_cross": crossing_time(t, ft, (1 - p.TCROSS_BAND) * sv_c, (1 + p.TCROSS_BAND) * sv_c),
        "Err_mean_pct": float(np.nanmean(err)), "Err_max_pct": float(np.nanmax(np.abs(err))),
        "Cum_Err": float(np.nansum(sv - ft)),
        # C. 펌프계 (기준선 무관 부분)
        "FT": ft_s, "FT_Hz": ft_s / hz_c,
        "t_self": crossing_time(t, ft, p.TSELF_LEVEL * ft_s),
        "ft_cv": float(np.std(ft[clean]) / ft_s * 100.0) if ft_s > 0 else np.nan,
        # D. 배관 (기준선 무관 부분)
        "PT": pt_s, "K": pt_s / ft_s ** 2 * p.K_SCALE if ft_s > 0 else np.nan,   # FT 0 = 지령은 있는데 안 나옴
        "pt_std": float(np.std(pt[clean])),
        # E. 기록
        "Hz_SV": hz_c / sv_c, "n_ticks": int(n), "duration_sec": float(t[-1]),
        "n_shot_ticks": int((stable & shot).sum()), "n_clean_ticks": int(clean.sum()), "steady_src": src,
        "n_hz_levels": int(len(np.unique(hz))),   # 사이클 안 Hz 단계 수 (HD2 는 다단: 3)
        "decel_half_sec": np.nan, "release_half_sec": np.nan,
    }
    if decay_t is not None and len(decay_t) >= p.DECAY_MIN_TICKS and decay_t[-1] >= p.DECAY_FT_FLOOR_FROM:
        pfl = (decay_t >= p.DECAY_PT_FLOOR[0]) & (decay_t <= p.DECAY_PT_FLOOR[1])
        ffl = decay_t >= p.DECAY_FT_FLOOR_FROM
        if pfl.any():
            f["release_half_sec"] = half_time(decay_t, decay_pt, float(pt[-1]), float(np.median(decay_pt[pfl])))
        if ffl.any():
            f["decel_half_sec"] = half_time(decay_t, decay_ft, float(ft[-1]), float(np.median(decay_ft[ffl])))
    return f


def add_baseline_features(df, base):
    """기준선이 있어야 계산되는 사이클 피처를 붙인다 (DataFrame 또는 dict)."""
    single = isinstance(df, dict)
    d = pd.DataFrame([df]) if single else df.copy()
    d["FT_e"] = d.Hz * base.r
    d["pump_resid"] = (d.FT_Hz / base.expected_fthz(d.Hz, d.PT, d.mu_rel) - 1.0) * 100.0
    d["r_phys"] = d.PT / base.expected_pt(d.FT_e, d.mu_rel)
    return d.iloc[0].to_dict() if single else d


# --------------------------------------------------------------------------- #
# 증압 → 안정 꺾임 (실시간 판정)
# --------------------------------------------------------------------------- #
class KneeTracker:
    """FT 기록점(값이 바뀐 틱)이 들어올 때마다 직전 기록점과의 기울기를 갱신한다.
    최대 기울기가 나온 **뒤** 처음으로 최대의 KNEE_FRAC 미만인 구간이 나오면, 그 구간의 시작점을 꺾임으로 확정.
    확정은 그 구간의 끝 기록점이 들어온 순간에 일어난다 (실시간에서 피할 수 없는 지연)."""

    def __init__(self, frac):
        self.frac = frac
        self.reset()

    def reset(self):
        self.prev = None            # (t, ft) 직전 기록점
        self.max_slope, self.max_seg, self.seg = 0.0, -1, -1
        self.knee_t = None

    def update(self, t, ft, fresh):
        """반환: 이번 틱에 꺾임이 확정됐으면 True."""
        if self.knee_t is not None or not fresh:
            return False
        if self.prev is None:
            self.prev = (t, ft)
            return False
        t0, f0 = self.prev
        self.seg += 1
        slope = (ft - f0) / max(t - t0, 1e-6)
        self.prev = (t, ft)
        if slope > self.max_slope:
            self.max_slope, self.max_seg = slope, self.seg
            return False
        if self.max_slope > 0 and self.seg > self.max_seg and slope < self.frac * self.max_slope:
            self.knee_t = t0
            return True
        return False


# --------------------------------------------------------------------------- #
# 상태기계
# --------------------------------------------------------------------------- #
PHASES = ("buildup", "steady", "decay")


class PumpProcessor:
    """틱을 하나씩 받아 틱 출력·사이클 피처·실시간 경보를 낸다.

        proc = PumpProcessor(PumpSetup("P1"), PumpBaseline.load("baseline_P1.json"))
        out = proc.update(now, row)     # row = {태그: 값}. 이번 틱에 바뀐 것만 넣어도 된다 (내부 ffill)
        out["tick"]    # 틱 AE 입력 (idle 이면 phase="idle", one-hot 전부 0)
        out["cycle"]   # 사이클 끝 + 복귀창 뒤 한 번. 그 외 None
        out["alerts"]  # [{"type": "ft_timeout" | "r_early", ...}]

    데이터가 사이클 도중부터 시작하면 그 사이클은 버린다 (켜짐 전환을 실제로 본 사이클만).

    수집 간격 가드 (상태기계가 틱 개수로 세므로):
    - 틱 사이 공백 > GAP_RESET_SEC(3초): 부팅과 같게 — 진행 중 사이클은 flag_cadence 로 마감, 직전 Hz·사출 기록 잊음,
      다시 꺼짐을 본 뒤 켜지는 사이클부터
    - 사이클 안 틱 간격 중앙값 > CADENCE_MED_SEC(1초)면 flag_cadence (진단 모드 2초 틱)
    flag_cadence 사이클은 피처는 내되 판정(창·진단·경보)에서 뺀다 — flag_any 에 포함.
    """

    def __init__(self, setup, base=None):
        self.s, self.base, self.p, self.tg = setup, base, setup.p, setup.tags
        self.last = {}
        self.inj = deque([False] * (self.p.INJ_TICKS + 1), maxlen=self.p.INJ_TICKS + 1)
        self.knee = KneeTracker(self.p.KNEE_FRAC)
        self.prev_bu = None                # None: 아직 모름, -1: 잘린 사이클 진행 중
        self.prev_pt = np.nan
        self.prev_ft = None
        self.prev_hz = np.nan
        self.prev_end = None
        self.last_t = None                 # 직전 틱 시각 (수집 간격)
        self._new_cycle_state()

    def _new_cycle_state(self):
        self.on, self.off = [], []
        self.dts = []                      # 이 사이클 틱 간격 [초]
        self.gap_cut = False
        self.t0 = self.last_on = None
        self.p0 = np.nan
        self.flag_alarm = self.flag_stopped = False
        self.entered = False
        self.fired = set()
        self.max_pt_early = -np.inf
        self.r_early = np.nan
        self.knee.reset()

    def _get(self, key, default=np.nan):
        v = self.last.get(self.tg[key], default)
        return default if v is None else v

    # ---------------------------------------------------------------- #
    def update(self, now, row):
        p = self.p
        for k, v in row.items():
            if v is not None and v == v:          # NaN 은 ffill 대상이라 덮어쓰지 않는다
                self.last[k] = v
        now = pd.Timestamp(now)
        dt = (now - self.last_t).total_seconds() if self.last_t is not None else np.nan
        self.last_t = now
        bu = int(self._get("bu", 0) or 0)
        sv, hz, ft = float(self._get("sv")), float(self._get("hz")), float(self._get("ft"))
        pt = float(self._get("pt"))
        pt = pt - 65536 if pt > 32767 else pt
        tt = float(self._get("tt"))
        if dt > p.GAP_RESET_SEC:                  # 공백 전 사출 기록은 잊는다 (아래 공백 리셋과 같이)
            self.inj.extend([False] * self.inj.maxlen)
        self.inj.append(bool(self._get("shot", 0)))
        shot = any(self.inj)
        alarm = any(bool(self.last.get(a, 0)) for a in self.s.alarm_tags)
        stopped = (self.s.run_tag in self.last) and not bool(self.last[self.s.run_tag])
        fresh = self.prev_ft is None or ft != self.prev_ft
        self.prev_ft = ft
        out = {"tick": None, "cycle": None, "alerts": []}

        # 공백(통신 끊김·백엔드 진단 중 예지 미구동) — 그 사이를 모르므로 부팅과 같게 다시 시작한다
        if dt > p.GAP_RESET_SEC:
            if self.on:
                self.gap_cut = True
                out["cycle"] = self._flush()
            self._new_cycle_state()
            self.prev_bu, self.prev_hz = None, np.nan

        # 시작 시점의 상태를 모르면 잘린 사이클은 버린다
        if self.prev_bu is None:
            self.prev_bu, self.prev_pt = (0 if bu == 0 else -1), pt
            out["tick"] = self._tick(now, "idle", np.nan, sv, hz, ft, pt, tt, shot, fresh)
            return out
        if self.prev_bu == -1:
            if bu == 0:
                self.prev_bu = 0
            self.prev_pt = pt
            out["tick"] = self._tick(now, "idle", np.nan, sv, hz, ft, pt, tt, shot, fresh)
            return out

        # 켜짐: 이전 사이클 복귀창이 덜 찼으면 여기서 마감
        if bu == 1 and self.prev_bu == 0:
            out["cycle"] = self._flush()
            self._new_cycle_state()
            self.t0, self.p0 = now, self.prev_pt

        if bu == 1:
            e = (now - self.t0).total_seconds()
            if now > self.t0:
                self.dts.append(dt)
            self.on.append((now, sv, hz, ft, pt, tt, shot))
            self.last_on = now
            self.flag_alarm |= alarm
            self.flag_stopped |= stopped
            self.knee.update(e, ft, fresh)
            phase = "steady" if self.knee.knee_t is not None else "buildup"
            elapsed = e - self.knee.knee_t if phase == "steady" else e
            out["alerts"] = self._rules(now, e, sv, hz, ft, pt, tt)
            out["tick"] = self._tick(now, phase, elapsed, sv, hz, ft, pt, tt, shot, fresh)
        elif self.on:
            d = (now - self.last_on).total_seconds()
            if d <= p.DECAY_WINDOW_SEC:
                self.dts.append(dt)
                self.off.append((now, sv, hz, ft, pt, tt, shot))
                self.flag_alarm |= alarm
                self.flag_stopped |= stopped
                out["tick"] = self._tick(now, "decay", d, sv, hz, ft, pt, tt, shot, fresh)
            else:
                out["cycle"] = self._flush()
                self._new_cycle_state()
                out["tick"] = self._tick(now, "idle", np.nan, sv, hz, ft, pt, tt, shot, fresh)
        else:
            out["tick"] = self._tick(now, "idle", np.nan, sv, hz, ft, pt, tt, shot, fresh)

        self.prev_bu, self.prev_pt = bu, pt
        return out

    # ---------------------------------------------------------------- #
    def _rules(self, now, e, sv, hz, ft, pt, tt):
        """실시간 규칙 (설계정리 7절). 사이클당 규칙마다 한 번만."""
        p, alerts = self.p, []
        if sv > 0 and abs(ft - sv) <= p.TCROSS_BAND * sv:
            self.entered = True
        if e >= p.FT_TIMEOUT_SEC and not self.entered and "ft_timeout" not in self.fired:
            self.fired.add("ft_timeout")
            alerts.append({"type": "ft_timeout", "time": now, "cycle_start": self.t0, "elapsed": e,
                           "FT": ft, "SV": sv, "pump": self.s.pump})
        if e <= p.R_EARLY_SEC:
            self.max_pt_early = max(self.max_pt_early, pt)
        elif "r_early" not in self.fired:
            self.fired.add("r_early")
            if self.base is not None and np.isfinite(self.p0):
                exp_pt = float(self.base.expected_pt(hz * self.base.r, self.s.visc(tt)))
                rise = exp_pt - self.p0
                if rise > 0:
                    self.r_early = (self.max_pt_early - self.p0) / rise
                    if self.r_early < p.R_EARLY_TH:
                        alerts.append({"type": "r_early", "time": now, "cycle_start": self.t0, "elapsed": e,
                                       "r_early": self.r_early, "PT_max": self.max_pt_early, "P0": self.p0,
                                       "pump": self.s.pump})
        return alerts

    def _tick(self, now, phase, elapsed, sv, hz, ft, pt, tt, shot, fresh):
        mu = float(self.s.visc(tt))
        d = {"time": now, "pump": self.s.pump, "cycle_start": self.t0 if phase != "idle" else None,
             "phase": phase, **{f"ph_{x}": int(phase == x) for x in PHASES}, "elapsed": elapsed,
             "SV": sv, "Hz": hz, "FT": ft, "PT": pt, "T_C": tt / self.p.TEMP_SCALE, "mu_rel": mu,
             "shot": shot, "ft_fresh": fresh,
             "err_sv": (sv - ft) / sv * 100.0 if sv > 0 else np.nan, "err_fte": np.nan, "pt_ratio": np.nan}
        if self.base is not None and hz > 0:
            fte = hz * self.base.r
            d["err_fte"] = (fte - ft) / fte * 100.0
            d["pt_ratio"] = pt / float(self.base.expected_pt(fte, mu))
        return d

    def _flush(self):
        if not self.on:
            return None
        on = list(zip(*self.on))
        t = np.array([(x - self.t0).total_seconds() for x in on[0]])
        if self.off:
            off = list(zip(*self.off))
            dt = np.array([(x - self.last_on).total_seconds() for x in off[0]])
            dft, dpt = np.array(off[3], float), np.array(off[4], float)
        else:
            dt = dft = dpt = None
        gap = (self.t0 - self.prev_end).total_seconds() if self.prev_end is not None else np.nan
        f = cycle_features(t, np.array(on[1], float), np.array(on[2], float), np.array(on[3], float),
                           np.array(on[4], float), np.array(on[5], float), np.array(on[6], bool),
                           dt, dft, dpt, self.prev_hz, gap, self.p0, self.s)
        self.prev_end = self.last_on
        if f is None:
            self.prev_hz = np.nan
            return None
        self.prev_hz = f["Hz"]
        dts = np.array(self.dts, float)
        cadence = self.gap_cut or (len(dts) > 0 and (np.median(dts) > self.p.CADENCE_MED_SEC
                                                      or dts.max() > self.p.GAP_RESET_SEC))
        f.update({"pump": self.s.pump, "t_start": self.t0, "t_end": self.last_on,
                  "t_knee": self.knee.knee_t if self.knee.knee_t is not None else np.nan,
                  "r_early": self.r_early,
                  "dt_med": float(np.median(dts)) if len(dts) else np.nan,
                  "flag_alarm": self.flag_alarm, "flag_stopped": self.flag_stopped, "flag_cadence": bool(cadence),
                  "flag_any": self.flag_alarm or self.flag_stopped or bool(cadence)})
        return add_baseline_features(f, self.base) if self.base is not None else f
