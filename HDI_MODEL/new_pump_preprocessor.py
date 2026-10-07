"""펌프 피처 전처리기 — Hz + 온도(점도) 기준, 실시간·배치 한 코드.

피처 정의·역할은 `new_pump_preprocessor.md`, 근거는 `펌프_피처_정리.md` 와 노트북
(`now_vis.ipynb`, `I1_FT_SV_괴리_원인규명_9월29일.ipynb`) 에 있다. 여기는 **계산만** 한다.
설정(펌프·원료·파라미터)은 `pump_settings.json` — `make_pump_settings.py` 로 만든다.

## 구조 — 하나의 상태기계

    PumpProcessor.update(now, row) → {"tick":   틱 출력 (매 틱),
                                      "cycle":  사이클 피처 (사이클 종료 + 복귀창 뒤 한 번, 아니면 None),
                                      "alerts": 실시간 규칙 경보 (r_early, FT 타임아웃)}
    run(raw, setup, base)          → 과거 데이터를 **같은 PumpProcessor 에 틱 순서대로 흘린 것**

학습 데이터와 실시간 입력이 같아지도록 **틱이 들어온 시점까지의 정보만** 쓴다.
특히 증압→안정 꺾임은 FT 다음 기록점이 와야 확정되므로, 확정 전 틱은 "증압" 으로 남는다
(0.5~1초 지연, 설계정리 4절).

## 왜 SV 가 아니라 Hz 인가

개루프다(Hz = SV × Auto-Cal 계수). Auto-Cal 마다 SV→Hz 표가 바뀌어 SV 기준 기대값은 0.7~1.1% 틀어진다.

    품질   ("지시한 만큼 나왔나")     → SV        FT_SV, t_cross, err_sv, FT 타임아웃
    건강   ("설비가 평소처럼 반응했나") → Hz + 점도  pump_resid, r_phys, K, err_fte, pt_ratio, r_early
    모양   ("곡선 모양이 평소와 같나")  → 자기 곡선  t_self, 반감 시각, 흔들림

## 사용

    setup = PumpSetup("P1")                                   # pump_settings.json 읽기, 점도표 생성
    ticks, cyc, alerts = run(raw_train, setup)                # 1) 기준선 없이
    base = PumpBaseline.fit(cyc[~cyc.flag_any], setup, circ=circulation_windows(raw_train, setup))
    base.save("baseline_P1.json")                             # 2) 기준선 저장
    proc = PumpProcessor(setup, base)                         # 3) 실시간
    out = proc.update(now, row)
"""

import json
import os
from collections import deque
from dataclasses import asdict, dataclass, field
from types import SimpleNamespace

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_SETTINGS = os.path.join(HERE, "pump_settings.json")


# --------------------------------------------------------------------------- #
# 설정 · 점도
# --------------------------------------------------------------------------- #
def load_settings(path=DEFAULT_SETTINGS):
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} 없음 — `python HDI_MODEL/make_pump_settings.py` 로 먼저 만든다")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def mu_rel(T_c, B, T_ref):
    """μ(T)/μ(T_ref). Andrade 지수식. T_c 는 ℃."""
    return np.exp(B * (1.0 / (np.asarray(T_c, float) + 273.15) - 1.0 / (T_ref + 273.15)))


class ViscosityTable:
    """온도 원값(0.1℃ 정수) → μ_rel 조회표. 초기화 때 한 번 만들고, 범위 밖은 직접 계산."""

    def __init__(self, B, T_ref, raw_range, temp_scale):
        self.B, self.T_ref, self.scale = B, T_ref, temp_scale
        self.lo = int(raw_range[0])
        self.table = mu_rel(np.arange(raw_range[0], raw_range[1] + 1) / temp_scale, B, T_ref)

    def __call__(self, tt_raw):
        x = np.rint(np.asarray(tt_raw, float))
        idx = x - self.lo
        ok = np.isfinite(x) & (idx >= 0) & (idx < len(self.table))
        out = np.where(ok, self.table[np.clip(np.nan_to_num(idx), 0, len(self.table) - 1).astype(int)], np.nan)
        far = np.isfinite(x) & ~ok
        if np.any(far):
            out = np.where(far, mu_rel(x / self.scale, self.B, self.T_ref), out)
        return out


class PumpSetup:
    """펌프 한 대의 실행 설정 — 태그, 원료, 파라미터, 점도표. 프로세스 시작 때 한 번 만든다."""

    def __init__(self, pump, settings=None, path=DEFAULT_SETTINGS, allow_unverified=False):
        s = settings or load_settings(path)
        if pump not in s["pumps"]:
            raise KeyError(f"{pump} 가 설정에 없음")
        pc = s["pumps"][pump]
        if pc["material"] is None or (not pc["verified"] and not allow_unverified):
            raise ValueError(f"{pump}: 원료·온도 태그 미확인 (make_pump_settings.py 의 PUMPS 를 채울 것)")
        mat = s["materials"][pc["material"]]
        self.pump, self.material_name, self.material, self.B = pump, pc["material"], mat["type"], mat["B"]
        self.p = SimpleNamespace(**s["params"])
        self.tags = {"bu": f"Pump_BuildUp_{pump}", "sv": f"g_s_SV_{pump}", "hz": f"Hz_Out_{pump}",
                     "ft": f"Scale_Out___FT_{pump}", "pt": f"Scale_Out___PT_{pump}",
                     "tt": f"Scale_Out___TT_{pc['tank']}",                 # 원료 탱크 기준 (P2 → TT_P5)
                     "shot": f"Shot_Injection_{pc['head']}_P{pc['slot']}"}  # ISO 는 같은 번호 폴리올과 공유
        self.alarm_tags, self.run_tag = s["alarm_tags"], s["run_tag"]
        self.visc = ViscosityTable(self.B, self.p.T_REF, self.p.VISC_TABLE_RAW, self.p.TEMP_SCALE)
        self.settings_version = s.get("version")

    def required_tags(self):
        """LogExtractor.get_data 에 그대로 넣을 태그 목록."""
        return list(self.tags.values()) + list(self.alarm_tags) + [self.run_tag]


# --------------------------------------------------------------------------- #
# 기준선 (정상 구간에서 학습 → 펌프별 JSON)
# --------------------------------------------------------------------------- #
@dataclass
class PumpBaseline:
    """정상 구간에서 맞춘 물리 기대값.

    r         : 1 Hz 당 유량. 기대 유량 FT_e = Hz × r
    pump_coef : POL → FT/Hz = a + b·h + c·h² + d·(μ−1),  h = Hz/1000
                ISO → FT/Hz = a + b·(PT/Hz×1000)/μ        (누설 ∝ 압력/점도)
    pt_coef   : PT = c + R·μ·FT_e + K·FT_e²               (관 점성 마찰 + 노즐)
    circ_coef : 순환 FT/Hz — POL: a + b·(μ−1), ISO: a + b·(PT/circ_hz×1000)/μ
    """
    pump: str
    material: str
    r: float
    pump_coef: list
    pt_coef: list
    circ_coef: list = None
    circ_hz: float = 2000.0
    fitted_on: dict = field(default_factory=dict)

    def expected_fthz(self, hz, pt, mu):
        hz, pt, mu = (np.asarray(v, float) for v in (hz, pt, mu))
        c = self.pump_coef
        if self.material == "POL":
            h = hz / 1000.0
            return c[0] + c[1] * h + c[2] * h ** 2 + c[3] * (mu - 1.0)
        return c[0] + c[1] * (pt / hz * 1000.0) / mu

    def expected_pt(self, fte, mu):
        c = self.pt_coef
        fte, mu = np.asarray(fte, float), np.asarray(mu, float)
        return c[0] + c[1] * mu * fte + c[2] * fte ** 2

    def expected_circ_fthz(self, pt, mu):
        mu = np.asarray(mu, float)
        if self.circ_coef is None:
            return np.full_like(mu, np.nan)
        c = self.circ_coef
        if self.material == "POL":
            return c[0] + c[1] * (mu - 1.0)
        return c[0] + c[1] * (np.asarray(pt, float) / self.circ_hz * 1000.0) / mu

    @classmethod
    def fit(cls, cycles, setup, circ=None):
        """cycles: run() 의 사이클 표 중 **정상 사이클만**. circ: circulation_windows() 결과(선택)."""
        c = cycles.dropna(subset=["Hz", "FT", "PT", "mu_rel"])
        r = float(np.median(c.FT / c.Hz))
        if setup.material == "POL":
            h = c.Hz / 1000.0
            X = np.c_[np.ones(len(c)), h, h ** 2, c.mu_rel - 1.0]
        else:
            X = np.c_[np.ones(len(c)), (c.PT / c.Hz * 1000.0) / c.mu_rel]
        pump_coef = np.linalg.lstsq(X, c.FT / c.Hz, rcond=None)[0].tolist()
        fte = c.Hz * r
        pt_coef = np.linalg.lstsq(np.c_[np.ones(len(c)), c.mu_rel * fte, fte ** 2], c.PT, rcond=None)[0].tolist()
        circ_coef = None
        if circ is not None and len(circ) >= 10:
            cc = circ.dropna(subset=["FT_Hz", "PT", "mu_rel"])
            xc = cc.mu_rel - 1.0 if setup.material == "POL" else (cc.PT / setup.p.CIRC_HZ * 1000.0) / cc.mu_rel
            circ_coef = np.linalg.lstsq(np.c_[np.ones(len(cc)), xc], cc.FT_Hz, rcond=None)[0].tolist()
        info = {"n_cycles": int(len(c)), "start": str(c.t_start.min()), "end": str(c.t_start.max()),
                "T_range": [float(c.T_C.quantile(0.01)), float(c.T_C.quantile(0.99))],
                "settings_version": setup.settings_version, "material_name": setup.material_name}
        return cls(setup.pump, setup.material, r, pump_coef, pt_coef, circ_coef, float(setup.p.CIRC_HZ), info)

    def save(self, path):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(asdict(self), f, ensure_ascii=False, indent=1)

    @classmethod
    def load(cls, path):
        with open(path, encoding="utf-8") as f:
            return cls(**json.load(f))


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


# --------------------------------------------------------------------------- #
# 사이클 피처 — 사이클이 끝난 뒤 그 사이클의 틱만으로 계산
# --------------------------------------------------------------------------- #
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
    clean = stable & ~shot
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
        "ft_cv": float(np.std(ft[clean]) / ft_s * 100.0),
        # D. 배관 (기준선 무관 부분)
        "PT": pt_s, "K": pt_s / ft_s ** 2 * p.K_SCALE, "pt_std": float(np.std(pt[clean])),
        # E. 기록
        "Hz_SV": hz_c / sv_c, "n_ticks": int(n), "duration_sec": float(t[-1]),
        "n_shot_ticks": int((stable & shot).sum()), "n_clean_ticks": int(clean.sum()),
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
# 상태기계 — 실시간·배치 공통
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
        self._new_cycle_state()

    def _new_cycle_state(self):
        self.on, self.off = [], []
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
        bu = int(self._get("bu", 0) or 0)
        sv, hz, ft = float(self._get("sv")), float(self._get("hz")), float(self._get("ft"))
        pt = float(self._get("pt"))
        pt = pt - 65536 if pt > 32767 else pt
        tt = float(self._get("tt"))
        self.inj.append(bool(self._get("shot", 0)))
        shot = any(self.inj)
        alarm = any(bool(self.last.get(a, 0)) for a in self.s.alarm_tags)
        stopped = (self.s.run_tag in self.last) and not bool(self.last[self.s.run_tag])
        fresh = self.prev_ft is None or ft != self.prev_ft
        self.prev_ft = ft
        out = {"tick": None, "cycle": None, "alerts": []}

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
        f.update({"pump": self.s.pump, "t_start": self.t0, "t_end": self.last_on,
                  "t_knee": self.knee.knee_t if self.knee.knee_t is not None else np.nan,
                  "r_early": self.r_early,
                  "flag_alarm": self.flag_alarm, "flag_stopped": self.flag_stopped,
                  "flag_any": self.flag_alarm or self.flag_stopped})
        return add_baseline_features(f, self.base) if self.base is not None else f


# --------------------------------------------------------------------------- #
# 배치 = 과거 데이터를 같은 상태기계에 흘린 것
# --------------------------------------------------------------------------- #
def run(raw, setup, base=None, keep_idle=False):
    """raw: LogExtractor.get_data 결과 (인덱스 = 시각, 펌프 한 대 분량).
    반환: (ticks, cycles, alerts) DataFrame 3개. keep_idle=False 면 idle 틱은 뺀다."""
    need = [setup.tags[k] for k in ("bu", "sv", "hz", "ft", "pt", "tt")]
    missing = [t for t in need if t not in raw.columns]
    if missing:
        raise KeyError(f"필수 태그 없음: {missing} — 0 으로 채우면 피처가 조용히 틀린다")
    proc = PumpProcessor(setup, base)
    cols = list(raw.columns)
    ticks, cycles, alerts = [], [], []
    raw = raw.sort_index()
    for ts, vals in zip(raw.index, raw.to_numpy()):
        o = proc.update(ts, dict(zip(cols, vals)))
        if o["tick"] is not None and (keep_idle or o["tick"]["phase"] != "idle"):
            ticks.append(o["tick"])
        if o["cycle"] is not None:
            cycles.append(o["cycle"])
        alerts.extend(o["alerts"])
    cyc = pd.DataFrame(cycles)
    if not cyc.empty:
        lead = ["pump", "t_start", "t_end"]
        cyc = cyc[lead + [c for c in cyc.columns if c not in lead]]
    return pd.DataFrame(ticks), cyc, pd.DataFrame(alerts)


# --------------------------------------------------------------------------- #
# 순환운전 창 — 사이클과 무관한 별도 집계 (생산 전 표준 시험)
# --------------------------------------------------------------------------- #
def circulation_windows(raw, setup, base=None, freq="5min", min_ticks=30):
    """사이클 사이 20 Hz 순환운전을 창마다 요약 — 순환 펌프 잔차, 순환 K.
    직전 창과 30분 넘게 떨어진 첫 창 = 재가동 직후 (`morning=True`) = 생산 전 표준 시험."""
    p, tg = setup.p, setup.tags
    q = raw.sort_index().ffill()
    bu = q[tg["bu"]].fillna(0).astype(int).to_numpy()
    hz, ft = q[tg["hz"]].to_numpy(float), q[tg["ft"]].to_numpy(float)
    pt = q[tg["pt"]].to_numpy(float)
    pt = np.where(pt > 32767, pt - 65536, pt)
    alarm = np.zeros(len(q), bool)
    for a in setup.alarm_tags:
        if a in q.columns:
            alarm |= (q[a].fillna(0) != 0).to_numpy()
    m = (bu == 0) & (hz == p.CIRC_HZ) & (ft > p.CIRC_MIN_FT) & ~alarm
    d = pd.DataFrame({"time": q.index.to_numpy()[m], "ft": ft[m], "pt": pt[m], "tt": q[tg["tt"]].to_numpy(float)[m]})
    if d.empty:
        return d
    g = d.groupby(d.time.dt.floor(freq)).agg(n=("ft", "size"), FT=("ft", "median"), PT=("pt", "median"),
                                             tt_raw=("tt", "median"))
    g = g[g.n >= min_ticks].copy()
    g["T_C"] = g.tt_raw / p.TEMP_SCALE
    g["mu_rel"] = setup.visc(g.tt_raw)
    g["FT_Hz"] = g.FT / p.CIRC_HZ
    g["K_circ"] = g.PT / g.FT ** 2 * p.K_SCALE
    gaps = g.index.to_series().diff() > pd.Timedelta(minutes=30)
    g["morning"] = gaps | (g.index == g.index.min())
    if base is not None and base.circ_coef is not None:
        g["circ_pump_resid"] = (g.FT_Hz / base.expected_circ_fthz(g.PT, g.mu_rel) - 1.0) * 100.0
    g.index.name = "window"
    return g.drop(columns="tt_raw")


# --------------------------------------------------------------------------- #
# 샷 혼합비 — 헤드 단위 (HD1 만 확인)
# --------------------------------------------------------------------------- #
#: HD1 리포트 슬롯 (샷 번호 → (POL g, ISO g)), Foam_Record.il. 같은 슬롯에 P1/I1 과 P2/I2 샷이 번갈아 기록된다
REPORT_SLOTS_HD1 = {1: ("D00011", "D00013"), 2: ("D00015", "D00017"), 3: ("D00019", "D00021"),
                    4: ("D00023", "D00025"), 5: ("D00027", "D00029"), 6: ("D00031", "D00033")}


def shot_mix_ratio(report_events, injection_events, slots=REPORT_SLOTS_HD1,
                   pairs=(("Shot_Injection_HD1_P1", "P1/I1"), ("Shot_Injection_HD1_P2", "P2/I2")),
                   max_lag_sec=5.0):
    """샷별 ISO g / POL g. 입력은 피벗 안 한 이벤트 표 (_time, tag_name, _value).
    편차(%)는 학습 구간의 `ratio.groupby(recipe).median()` 으로 나눠 만든다."""
    recs = []
    for s, (pol, iso) in slots.items():
        w = (report_events[report_events.tag_name.isin([pol, iso])]
             .pivot_table(index="_time", columns="tag_name", values="_value", aggfunc="last").sort_index().ffill().dropna())
        if w.empty:
            continue
        w = w[(w[pol].diff() != 0) | (w[iso].diff() != 0)].rename(columns={pol: "POL_g", iso: "ISO_g"})
        recs.append(w.assign(slot=s).reset_index())
    if not recs:
        return pd.DataFrame()
    shots = pd.concat(recs).sort_values("_time")
    shots = shots[(shots.POL_g > 50) & (shots.ISO_g > 50)]
    ends = []
    for tag, pair in pairs:
        v = injection_events[injection_events.tag_name == tag].sort_values("_time")
        v = v[v["_value"].ne(v["_value"].shift())]
        e = v[(v["_value"] == 0) & (v["_value"].shift() == 1)]
        ends.append(pd.DataFrame({"inj_end": e["_time"].to_numpy(), "pair": pair}))
    ends = pd.concat(ends).sort_values("inj_end")
    shots = pd.merge_asof(shots, ends, left_on="_time", right_on="inj_end", direction="backward")
    shots = shots[(shots["_time"] - shots.inj_end).dt.total_seconds() <= max_lag_sec]
    shots["ratio"] = shots.ISO_g / shots.POL_g
    shots["recipe"] = shots.pair + "|" + shots.slot.astype(str) + "|" + (shots.POL_g / 100).round().astype(int).astype(str)
    return shots.rename(columns={"_time": "time"})[["time", "pair", "slot", "POL_g", "ISO_g", "ratio", "recipe"]]


if __name__ == "__main__":
    s = load_settings()
    print(f"new_pump_preprocessor — 설정 {s['version']} ({s['generated_by']})")
    for pid, pc in s["pumps"].items():
        if pc["verified"]:
            st = PumpSetup(pid, s)
            print(f"  {pid}: {st.material_name}({st.material}) B={st.B:.0f} K  태그 {st.required_tags()}")
