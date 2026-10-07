"""펌프 피처 전처리기 — Hz + 온도(점도) 기준.

피처 정의·역할은 `new_pump_preprocessor.md`, 근거는 `펌프_피처_정리.md` 와 노트북
(`now_vis.ipynb`, `I1_FT_SV_괴리_원인규명_9월29일.ipynb`) 에 있다. 여기는 **계산만** 한다.

## 왜 SV 가 아니라 Hz 인가

이 펌프는 개루프다. ``Hz = SV × Auto-Cal 구간계수 × Gain`` 이고 FT 는 제어에 안 들어간다.
Auto-Cal 을 누를 때마다 SV → Hz 표가 바뀌므로(P1: 9/27, 9/30), SV 로 만든 기대값은
Auto-Cal 뒤 0.7~1.1% 틀어진다. 그래서 피처를 세 가지 기준으로 나눈다.

    품질   ("지시한 만큼 나왔나")     → SV        FT_SV, t_cross, 오차율
    건강   ("설비가 평소처럼 반응했나") → Hz + 점도  pump_resid, r_phys, K
    모양   ("곡선 모양이 평소와 같나")  → 자기 곡선  t_self, 반감 시각, 흔들림

## 온도

``Scale_Out___TT_*`` (= ``TK_Temp_PV_*``, PLC 가 그대로 복사하는 같은 센서) 를 쓰고,
점도비는 Andrade 지수식 ``μ_rel = exp[B·(1/T_K − 1/T_ref)]`` 이다. 원값이 0.1℃ 정수라
**원값을 키로 하는 조회표**로 계산한다(근사 오차 0, 지수 계산 없음).
폴리올은 압력(관 마찰)에, ISO 는 토출량(펌프 누설)에 들어간다.

## 사용 순서

    cfg  = PUMPS["P1"]
    cyc  = extract_cycles(raw_train, cfg)                    # 1) 기준선 없이 추출
    base = PumpBaseline.fit(cyc[~cyc.flag_any], cfg)         # 2) 정상 구간으로 기준선 학습
    base.save("baseline_P1.json")
    cyc  = add_baseline_features(extract_cycles(raw_new, cfg), base)   # 3) 기준선 피처 추가

실시간은 ``PumpCycleRealtime`` 이 틱을 받아 같은 ``cycle_features`` 를 부른다.
"""

import json
import os
from dataclasses import asdict, dataclass, field

import numpy as np
import pandas as pd

# --------------------------------------------------------------------------- #
# 펌프 설정
# --------------------------------------------------------------------------- #
#: 원료별 Andrade 상수 B (K). OJT/공정도.xls '온도별 점도 (3)' (2013.5.12) 에 맞춘 값.
#: **현재 원료 TDS 로 갱신 필요.** I1 은 데이터 추정(−2.36 %/℃)이 TM-20(−2.21 %/℃)과 맞았다.
VISC_B = {
    "TM-337": 5756.0,    # 폴리올 BACK
    "AM-315": 5418.0,    # 폴리올 CUSH
    "MBR-360": 5360.0,   # 폴리올 고탄성
    "TM-20": 1921.0,     # ISO
    "MC-70": 2781.0,     # ISO
}


@dataclass
class PumpConfig:
    """펌프 한 대의 태그·원료 설정.

    material : "POL" 이면 펌프 기준선을 Hz 2차식, "ISO" 면 슬립 직선으로 맞춘다.
               (폴리올은 누설이 작아 슬립 직선이 안 맞는다 — now_vis §5, r = −0.44)
    shot_tag : 이 펌프가 실제로 사출하는 순간. ISO 는 같은 번호 폴리올과 헤드·슬롯을 공유한다.
    """
    pump: str
    material: str
    visc_B: float
    shot_tag: str
    temp_tag: str = None
    verified: bool = False

    def __post_init__(self):
        if self.temp_tag is None:
            self.temp_tag = f"Scale_Out___TT_{self.pump}"

    @property
    def tags(self):
        p = self.pump
        return {"bu": f"Pump_BuildUp_{p}", "sv": f"g_s_SV_{p}", "hz": f"Hz_Out_{p}",
                "ft": f"Scale_Out___FT_{p}", "pt": f"Scale_Out___PT_{p}", "tt": self.temp_tag,
                "shot": self.shot_tag}


#: 검증한 펌프만 넣는다. 다른 펌프는 원료·온도 태그를 확인하고 같은 형식으로 추가할 것.
#: (parkkt/Cycle_Feature_Extractor.PUMP_MAP 의 head/slot 을 따른다)
PUMPS = {
    "P1": PumpConfig("P1", "POL", VISC_B["TM-337"], "Shot_Injection_HD1_P1", verified=True),
    "I1": PumpConfig("I1", "ISO", VISC_B["TM-20"], "Shot_Injection_HD1_P1", verified=True),
}

#: 수집해야 하는 공통 태그
ALARM_TAGS = ["AL_Line_Bit", "AL_MA_Bit", "AL_Tank_Bit", "AL_ETC_Bit"]
RUN_TAG = "Auto_Running"


def required_tags(cfg):
    """이 전처리기가 읽는 태그 목록 (LogExtractor.get_data 에 그대로 넣는다)."""
    return list(cfg.tags.values()) + ALARM_TAGS + [RUN_TAG]


# --------------------------------------------------------------------------- #
# 파라미터 (근거는 new_pump_preprocessor.md 4절)
# --------------------------------------------------------------------------- #
T_REF = 22.0              #: 점도 기준온도 ℃ (이 라인의 평소 재료온도)
TEMP_SCALE = 10.0         #: 온도 원값 0.1℃ → ℃
STEADY_MIN = 5            #: 대표 Hz 가 시작된 뒤 이 틱 수 이후를 안정 구간으로
INJ_TICKS = 4             #: 사출 시작 후 이만큼(틱)은 안정 통계에서 뺀다
TCROSS_BAND = 0.03        #: t_cross: SV ±3% (설계정리 6절)
TSELF_LEVEL = 0.95        #: t_self: 자기 안정값의 95%
DECAY_WINDOW_SEC = 5.0    #: 꺼진 뒤 이 시간까지를 복귀 구간으로 본다 (t=0 = 마지막 켜짐 틱)
DECAY_PT_FLOOR = (2.0, 3.0)  #: 압력 바닥값 = 이 구간 PT 중앙값 (new_vis 15)
DECAY_FT_FLOOR_FROM = 3.0    #: 유량 바닥값 = 이 시각 이후 FT 중앙값 (= 20 Hz 순환 유량)
DECAY_MIN_TICKS = 4       #: 복귀 구간 틱이 이보다 적거나 3초를 못 채우면 반감 시각을 안 구한다
CIRC_HZ = 2000            #: 사이클 사이 순환 속도 지령 (20.00 Hz)
CIRC_MIN_FT = 300         #: 순환 중 FT 가 이보다 작으면 펌프 정지로 본다
MIN_CYCLE_TICKS = 8
MIN_CLEAN_TICKS = 3
K_SCALE = 1e5             #: 유로 저항 K = PT/FT² × 1e5


# --------------------------------------------------------------------------- #
# 점도
# --------------------------------------------------------------------------- #
def mu_rel(T_c, B, T_ref=T_REF):
    """μ(T)/μ(T_ref). Andrade 지수식. T_c 는 ℃."""
    return np.exp(B * (1.0 / (np.asarray(T_c, float) + 273.15) - 1.0 / (T_ref + 273.15)))


class ViscosityTable:
    """온도 원값(0.1℃ 정수) → μ_rel 조회표. 15.0~40.0℃ 를 미리 계산하고 범위 밖은 직접 계산."""

    def __init__(self, B, T_ref=T_REF, raw_range=(150, 400)):
        self.B, self.T_ref = B, T_ref
        keys = np.arange(raw_range[0], raw_range[1] + 1)
        self.lo = raw_range[0]
        self.table = mu_rel(keys / TEMP_SCALE, B, T_ref)

    def __call__(self, tt_raw):
        x = np.rint(np.asarray(tt_raw, float))
        idx = x - self.lo
        ok = (idx >= 0) & (idx < len(self.table)) & np.isfinite(x)
        out = np.where(ok, self.table[np.clip(idx, 0, len(self.table) - 1).astype(int)], np.nan)
        if (~ok & np.isfinite(x)).any():
            out = np.where(~ok & np.isfinite(x), mu_rel(x / TEMP_SCALE, self.B, self.T_ref), out)
        return out


# --------------------------------------------------------------------------- #
# 기준선 (학습 구간에서 맞추고 JSON 으로 저장)
# --------------------------------------------------------------------------- #
@dataclass
class PumpBaseline:
    """정상 구간에서 맞춘 물리 기대값.

    r          : 1 Hz 당 유량 (FT/Hz 중앙값). 기대 유량 FT_e = Hz × r
    pump_coef  : POL → FT/Hz = a + b·h + c·h² + d·(μ−1),  h = Hz/1000
                 ISO → FT/Hz = a + b·(PT/Hz×1000)/μ        (슬립 직선, 누설 ∝ 압력/점도)
    pt_coef    : PT = c + R·μ·FT_e + K·FT_e²              (관 점성 마찰 + 노즐)
    circ_coef  : 순환(20 Hz) 기준선 — 순환 펌프 잔차용
                 POL → FT/Hz = a + b·(μ−1)
                 ISO → FT/Hz = a + b·(PT/Hz×1000)/μ   (ISO 는 누설이 압력을 따라가 노즐 상태에 끌려가므로 압력항 필요)
    """
    pump: str
    material: str
    r: float
    pump_coef: list
    pt_coef: list
    circ_coef: list = None
    fitted_on: dict = field(default_factory=dict)

    # ---- 기대값 ----
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
        return c[0] + c[1] * (np.asarray(pt, float) / CIRC_HZ * 1000.0) / mu

    # ---- 학습 ----
    @classmethod
    def fit(cls, cycles, cfg, circ=None):
        """cycles: extract_cycles() 결과 중 **정상 사이클만**. circ: circulation_windows() 결과(선택)."""
        c = cycles.dropna(subset=["Hz", "FT", "PT", "mu_rel"])
        r = float(np.median(c.FT / c.Hz))
        fthz = c.FT / c.Hz
        if cfg.material == "POL":
            h = c.Hz / 1000.0
            X = np.c_[np.ones(len(c)), h, h ** 2, c.mu_rel - 1.0]
        else:
            X = np.c_[np.ones(len(c)), (c.PT / c.Hz * 1000.0) / c.mu_rel]
        pump_coef = np.linalg.lstsq(X, fthz, rcond=None)[0].tolist()
        fte = c.Hz * r
        Xp = np.c_[np.ones(len(c)), c.mu_rel * fte, fte ** 2]
        pt_coef = np.linalg.lstsq(Xp, c.PT, rcond=None)[0].tolist()
        circ_coef = None
        if circ is not None and len(circ) >= 10:
            cc = circ.dropna(subset=["FT_Hz", "PT", "mu_rel"])
            xc = cc.mu_rel - 1.0 if cfg.material == "POL" else (cc.PT / CIRC_HZ * 1000.0) / cc.mu_rel
            circ_coef = np.linalg.lstsq(np.c_[np.ones(len(cc)), xc], cc.FT_Hz, rcond=None)[0].tolist()
        info = {"n_cycles": int(len(c)), "start": str(c.t_start.min()), "end": str(c.t_start.max()),
                "T_range": [float(c.T_C.quantile(0.01)), float(c.T_C.quantile(0.99))]}
        return cls(cfg.pump, cfg.material, r, pump_coef, pt_coef, circ_coef, info)

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
    """v 가 처음으로 [lo, hi] 에 (hi 없으면 lo 이상에) 들어간 시각. 두 '기록점' 사이를 직선 보간.

    v 는 ffill 된 틱 배열이라 같은 값이 이어지면 새 기록이 아니다. 진입 직전의 실제 기록점까지
    거슬러 가서 보간한다(설계정리 6절 t_cross).
    """
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
    """꺼진 뒤 v 가 start 에서 floor 쪽으로 절반을 처음 지난 시각(초).

    t, v : 꺼진 뒤 틱 (t 는 마지막 켜짐 틱 기준). start 는 마지막 켜짐 틱 값으로 t=0 에 놓는다.
    값이 같은 틱(ffill 된 묵은 값)은 빼고 **실제 기록점 사이**를 보간한다 (new_vis 15).
    """
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
# 사이클 피처 — 배치·실시간 공통
# --------------------------------------------------------------------------- #
def cycle_features(t, sv, hz, ft, pt, tt_raw, shot, decay_t=None, decay_ft=None, decay_pt=None,
                   prev_hz=np.nan, gap_sec=np.nan, p0=np.nan, visc=None):
    """사이클 하나의 피처 (기준선 없이 계산되는 것만).

    t, sv, hz, ft, pt, tt_raw, shot : 켜진 동안(BuildUp=1) 틱 배열. t 는 켜진 순간 기준 초.
                                      shot 은 '사출 영향 틱'(사출 시작 + INJ_TICKS) bool.
    decay_t, decay_ft, decay_pt      : 꺼진 뒤 DECAY_WINDOW_SEC 까지(다음 사이클 전까지) 틱.
                                      t 는 **마지막 켜짐 틱** 기준 초 (첫 꺼짐 틱 > 0).
    prev_hz, gap_sec, p0             : 직전 사이클 대표 Hz, 직전 사이클 종료 후 공백(초), 켜지기 직전 PT.
    visc                             : ViscosityTable
    반환: dict. 안정 구간이 부족하면 None.
    """
    n = len(t)
    if n < MIN_CYCLE_TICKS:
        return None
    hz_c = _mode(hz)                                   # 대표 Hz (사이클 대부분을 차지한 지령)
    sv_c = _mode(sv)                                   # SV 클래스 (최빈값)
    idx = np.where(hz == hz_c)[0]
    stable = np.zeros(n, bool)
    if len(idx) > STEADY_MIN:
        stable[idx[STEADY_MIN:]] = True
    clean = stable & ~shot
    if clean.sum() < MIN_CLEAN_TICKS or hz_c <= 0 or sv_c <= 0:
        return None

    ft_s, pt_s = float(np.median(ft[clean])), float(np.median(pt[clean]))
    T_c = float(np.median(tt_raw)) / TEMP_SCALE
    mu = float(visc(np.median(tt_raw))) if visc is not None else np.nan
    err = (sv - ft) / np.where(sv > 0, sv, np.nan) * 100.0

    f = {
        # A. 조건
        "SV_class": float(sv_c), "Hz": float(hz_c), "T_C": T_c, "mu_rel": mu,
        "dHz_prev": float(hz[0] - prev_hz) if np.isfinite(prev_hz) else np.nan,
        "P0": float(p0), "Gap_sec": float(gap_sec),
        # B. 품질 (SV 기준)
        "FT_SV": ft_s / sv_c,
        "t_cross": crossing_time(t, ft, (1 - TCROSS_BAND) * sv_c, (1 + TCROSS_BAND) * sv_c),
        "Err_mean_pct": float(np.nanmean(err)), "Err_max_pct": float(np.nanmax(np.abs(err))),
        "Cum_Err": float(np.nansum(sv - ft)),
        # C. 펌프계 (기준선 무관 부분)
        "FT": ft_s, "FT_Hz": ft_s / hz_c,
        "t_self": crossing_time(t, ft, TSELF_LEVEL * ft_s),
        "ft_cv": float(np.std(ft[clean]) / ft_s * 100.0),
        # D. 배관 (기준선 무관 부분)
        "PT": pt_s, "K": pt_s / ft_s ** 2 * K_SCALE, "pt_std": float(np.std(pt[clean])),
        # E. 기록
        "Hz_SV": hz_c / sv_c, "n_ticks": int(n), "duration_sec": float(t[-1]),
        "n_shot_ticks": int((stable & shot).sum()), "n_clean_ticks": int(clean.sum()),
        "decel_half_sec": np.nan, "release_half_sec": np.nan,
    }

    # 복귀 구간: 반감 시각 두 개 (Decay_Slope 대체 — new_vis 15)
    if decay_t is not None and len(decay_t) >= DECAY_MIN_TICKS and decay_t[-1] >= DECAY_FT_FLOOR_FROM:
        pfl = (decay_t >= DECAY_PT_FLOOR[0]) & (decay_t <= DECAY_PT_FLOOR[1])
        ffl = decay_t >= DECAY_FT_FLOOR_FROM
        if pfl.any():
            f["release_half_sec"] = half_time(decay_t, decay_pt, float(pt[-1]), float(np.median(decay_pt[pfl])))
        if ffl.any():
            f["decel_half_sec"] = half_time(decay_t, decay_ft, float(ft[-1]), float(np.median(decay_ft[ffl])))
    return f


def add_baseline_features(df, base):
    """기준선이 있어야 계산되는 피처를 붙인다 (DataFrame 또는 dict)."""
    single = isinstance(df, dict)
    d = pd.DataFrame([df]) if single else df.copy()
    d["FT_e"] = d.Hz * base.r
    d["pump_resid"] = (d.FT_Hz / base.expected_fthz(d.Hz, d.PT, d.mu_rel) - 1.0) * 100.0
    d["r_phys"] = d.PT / base.expected_pt(d.FT_e, d.mu_rel)
    return d.iloc[0].to_dict() if single else d


# --------------------------------------------------------------------------- #
# 원본 정리
# --------------------------------------------------------------------------- #
def _prepare(raw, cfg):
    """원본 → 틱 배열. ffill, PT 16비트 보정, 사출 영향 틱, 제외 플래그."""
    tg = cfg.tags
    missing = [tg[k] for k in ("bu", "sv", "hz", "ft", "pt", "tt") if tg[k] not in raw.columns]
    if missing:
        raise KeyError(f"필수 태그 없음: {missing} — 0 으로 채우면 피처가 조용히 틀린다")
    q = raw.sort_index().ffill()
    pt = q[tg["pt"]].to_numpy(float)
    pt = np.where(pt > 32767, pt - 65536, pt)
    if tg["shot"] in q.columns:
        inj = (q[tg["shot"]].fillna(0) != 0).to_numpy()
    else:
        inj = np.zeros(len(q), bool)
    shot = inj.copy()
    for k in range(1, INJ_TICKS + 1):
        shot[k:] |= inj[:-k]
    alarm = np.zeros(len(q), bool)
    for a in ALARM_TAGS:
        if a in q.columns:
            alarm |= (q[a].fillna(0) != 0).to_numpy()
    stopped = (q[RUN_TAG].fillna(0) == 0).to_numpy() if RUN_TAG in q.columns else np.zeros(len(q), bool)
    return {
        "time": q.index.to_numpy(), "bu": q[tg["bu"]].fillna(0).astype(int).to_numpy(),
        "sv": q[tg["sv"]].to_numpy(float), "hz": q[tg["hz"]].to_numpy(float), "ft": q[tg["ft"]].to_numpy(float),
        "pt": pt, "tt": q[tg["tt"]].to_numpy(float), "shot": shot, "alarm": alarm, "stopped": stopped,
        "has_shot": tg["shot"] in q.columns, "has_run": RUN_TAG in q.columns,
    }


def _sec(a, b):
    return (a - b) / np.timedelta64(1, "s")


# --------------------------------------------------------------------------- #
# 배치 추출
# --------------------------------------------------------------------------- #
def extract_cycles(raw, cfg, base=None):
    """원본(LogExtractor.get_data 결과, 펌프 한 대 분량) → 사이클별 피처 DataFrame.

    제외는 하지 않고 플래그만 단다 (설계정리 2절: 원본 전체로 추출 → 사이클 단위로 제외).
      flag_alarm    : 사이클 구간 [시작, 끝+복귀창] 에 알람 4개 중 하나라도 켜짐
      flag_stopped  : 같은 구간에 Auto_Running == 0
      flag_any      : 둘 중 하나
    """
    a = _prepare(raw, cfg)
    visc = ViscosityTable(cfg.visc_B)
    T, bu = a["time"], a["bu"]
    starts = np.where(np.diff(bu) == 1)[0] + 1
    ends = np.where(np.diff(bu) == -1)[0] + 1
    rows, prev_hz, prev_end = [], np.nan, None
    for i, st in enumerate(starts):
        e = ends[ends > st]
        if len(e) == 0:
            break                                      # 데이터 끝에서 아직 켜진 사이클
        en = int(e[0])
        nxt = starts[i + 1] if i + 1 < len(starts) else len(bu)
        t = _sec(T[st:en], T[st])
        # 복귀 구간: 꺼진 순간(en) 부터 DECAY_WINDOW_SEC 까지, 다음 사이클 전까지
        dj = en
        while dj < nxt and _sec(T[dj], T[en - 1]) <= DECAY_WINDOW_SEC:
            dj += 1
        dt = _sec(T[en:dj], T[en - 1])                # 마지막 켜짐 틱 기준
        f = cycle_features(t, a["sv"][st:en], a["hz"][st:en], a["ft"][st:en], a["pt"][st:en], a["tt"][st:en],
                           a["shot"][st:en], dt, a["ft"][en:dj], a["pt"][en:dj],
                           prev_hz=prev_hz, gap_sec=_sec(T[st], prev_end) if prev_end is not None else np.nan,
                           p0=a["pt"][st - 1] if st > 0 else np.nan, visc=visc)
        prev_end = T[en - 1]
        if f is None:
            prev_hz = np.nan
            continue
        prev_hz = f["Hz"]
        span = slice(st, dj)
        f.update({"t_start": T[st], "t_end": T[en - 1],
                  "flag_alarm": bool(a["alarm"][span].any()), "flag_stopped": bool(a["stopped"][span].any())})
        rows.append(f)
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["flag_any"] = df.flag_alarm | df.flag_stopped
    df.insert(0, "pump", cfg.pump)
    lead = ["pump", "t_start", "t_end"]
    df = df[lead + [c for c in df.columns if c not in lead]]
    return add_baseline_features(df, base) if base is not None else df


def circulation_windows(raw, cfg, base=None, freq="5min", min_ticks=30):
    """사이클 사이 20 Hz 순환운전 구간을 창(기본 5분)마다 요약 — 순환 펌프 잔차, 순환 K.

    매일 재가동 직후 첫 창이 **생산 전 표준 시험**이다 (`morning=True`).
    """
    a = _prepare(raw, cfg)
    visc = ViscosityTable(cfg.visc_B)
    m = (a["bu"] == 0) & (a["hz"] == CIRC_HZ) & (a["ft"] > CIRC_MIN_FT) & ~a["alarm"]
    d = pd.DataFrame({"time": a["time"][m], "ft": a["ft"][m], "pt": a["pt"][m], "tt": a["tt"][m]})
    if d.empty:
        return d
    g = d.groupby(d.time.dt.floor(freq)).agg(n=("ft", "size"), FT=("ft", "median"), PT=("pt", "median"),
                                             tt_raw=("tt", "median"))
    g = g[g.n >= min_ticks].copy()
    g["T_C"] = g.tt_raw / TEMP_SCALE
    g["mu_rel"] = visc(g.tt_raw)
    g["FT_Hz"] = g.FT / CIRC_HZ
    g["K_circ"] = g.PT / g.FT ** 2 * K_SCALE
    # 재가동 직후: 직전 창과 30분 넘게 떨어진 첫 창
    gaps = g.index.to_series().diff() > pd.Timedelta(minutes=30)
    g["morning"] = gaps | (g.index == g.index.min())
    if base is not None and base.circ_coef is not None:
        g["circ_pump_resid"] = (g.FT_Hz / base.expected_circ_fthz(g.PT, g.mu_rel) - 1.0) * 100.0
    g.index.name = "window"
    return g.drop(columns="tt_raw")


# --------------------------------------------------------------------------- #
# 틱 프레임 — 틱 AE 입력용
# --------------------------------------------------------------------------- #
def knee_index(t, ft, frac=0.2):
    """증압 → 안정 경계 (설계정리 4절). 실제 FT 기록점 사이 기울기가, 최대 기울기 이후 처음으로
    최대의 frac 미만이 된 구간의 시작 틱. SV 를 쓰지 않는다."""
    rec = np.r_[0, np.where(np.diff(ft) != 0)[0] + 1]
    if len(rec) < 3:
        return len(ft) - 1
    slope = np.diff(ft[rec]) / np.maximum(np.diff(t[rec]), 1e-6)
    j = int(np.argmax(slope))
    after = np.where(slope[j:] < frac * slope[j])[0]
    return int(rec[j + after[0]]) if len(after) else len(ft) - 1


def tick_frame(raw, cfg, base=None):
    """틱 단위 표 (가동 중 + 복귀 구간). 틱 AE 의 입력을 만든다.

    phase   : "buildup" (켜짐 ~ 꺾임) / "steady" / "decay" (꺼진 뒤 DECAY_WINDOW_SEC)
    elapsed : 그 구간 안 경과초
    err_sv  : (SV − FT)/SV %      — 품질
    err_fte : (FT_e − FT)/FT_e %  — 건강 (기준선 필요)
    pt_ratio: PT / 기대 PT(FT_e, μ) — 건강 (기준선 필요)
    ft_fresh: 이 틱에 FT 가 새로 기록됐나 (FT 새 값은 틱의 약 56%)
    """
    a = _prepare(raw, cfg)
    visc = ViscosityTable(cfg.visc_B)
    T, bu = a["time"], a["bu"]
    starts = np.where(np.diff(bu) == 1)[0] + 1
    out = []
    for st in starts:
        en = st
        while en < len(bu) and bu[en] == 1:
            en += 1
        if en - st < MIN_CYCLE_TICKS:
            continue
        dj = en
        while dj < len(bu) and bu[dj] == 0 and _sec(T[dj], T[en - 1]) <= DECAY_WINDOW_SEC:
            dj += 1
        t_on = _sec(T[st:en], T[st])
        kn = knee_index(t_on, a["ft"][st:en])
        phase = np.array(["buildup"] * (en - st) + ["decay"] * (dj - en), dtype=object)
        phase[kn:en - st] = "steady"
        tt = np.r_[t_on, _sec(T[en:dj], T[en - 1])]
        elapsed = tt.copy()
        elapsed[kn:en - st] = t_on[kn:] - t_on[kn]
        sl = slice(st, dj)
        sv, ft, pt, hz = a["sv"][sl], a["ft"][sl], a["pt"][sl], a["hz"][sl]
        mu = visc(a["tt"][sl])
        f = pd.DataFrame({"time": T[sl], "cycle_start": T[st], "phase": phase, "elapsed": elapsed,
                          "SV": sv, "Hz": hz, "FT": ft, "PT": pt, "mu_rel": mu, "shot": a["shot"][sl],
                          "ft_fresh": np.r_[True, np.diff(ft) != 0],
                          "err_sv": (sv - ft) / np.where(sv > 0, sv, np.nan) * 100.0})
        if base is not None:
            fte = _mode(a["hz"][st:en]) * base.r
            f["err_fte"] = (fte - ft) / fte * 100.0
            f["pt_ratio"] = pt / base.expected_pt(fte, mu)
        out.append(f)
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()


# --------------------------------------------------------------------------- #
# 샷 혼합비 — 헤드 단위 (HD1 만 확인)
# --------------------------------------------------------------------------- #
#: HD1 리포트 슬롯 (샷 번호 → (POL g, ISO g)). Foam_Record.il. 0.1 g 단위.
#: 같은 슬롯에 P1/I1 샷과 P2/I2 샷이 번갈아 기록된다 — 사출 신호로 가른다.
REPORT_SLOTS_HD1 = {1: ("D00011", "D00013"), 2: ("D00015", "D00017"), 3: ("D00019", "D00021"),
                    4: ("D00023", "D00025"), 5: ("D00027", "D00029"), 6: ("D00031", "D00033")}


def shot_mix_ratio(report_events, injection_events, slots=REPORT_SLOTS_HD1,
                   pairs=(("Shot_Injection_HD1_P1", "P1/I1"), ("Shot_Injection_HD1_P2", "P2/I2")),
                   max_lag_sec=5.0):
    """샷별 ISO g / POL g. 입력은 피벗 안 한 이벤트 표 (_time, tag_name, _value).

    반환: 샷마다 (time, pair, slot, POL_g, ISO_g, ratio, recipe). 편차(%)는 레시피별 정상 중앙값으로
    나눠서 만든다 — 학습 구간에서 `ratio.groupby(recipe).median()` 을 기준으로 저장해 쓴다.
    """
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


# --------------------------------------------------------------------------- #
# 실시간
# --------------------------------------------------------------------------- #
class PumpCycleRealtime:
    """틱을 받아 사이클이 끝나고 복귀 구간(DECAY_WINDOW_SEC)이 지나면 사이클 피처를 내놓는다.

    배치(extract_cycles)와 같은 cycle_features 를 쓴다.

        rt = PumpCycleRealtime(PUMPS["P1"], PumpBaseline.load("baseline_P1.json"))
        while True:
            feat = rt.update(now, row)      # row = {태그: 값} — 이번 틱에 들어온 것만 넣어도 된다
            if feat is not None:
                ...                          # 사이클 AE / 기록 테이블로
    """

    def __init__(self, cfg, base=None):
        self.cfg, self.base = cfg, base
        self.visc = ViscosityTable(cfg.visc_B)
        self.tg = cfg.tags
        self.last = {}                     # 태그별 마지막 값 (ffill)
        self.inj_hist = [False] * (INJ_TICKS + 1)
        self.on = []                       # 켜진 동안 틱
        self.off = []                      # 꺼진 뒤 틱
        self.off_since = None
        self.prev_bu = None                # 첫 틱의 이전 상태는 모른다
        self.prev_pt = np.nan
        self.p0 = np.nan
        self.prev_hz = np.nan
        self.prev_end = None
        self.alarm = False

    def _val(self, key, default=np.nan):
        v = self.last.get(self.tg[key], default)
        return np.nan if v is None else v

    def update(self, now, row):
        self.last.update({k: v for k, v in row.items() if v is not None})
        now = pd.Timestamp(now)
        bu = int(self._val("bu", 0) or 0)
        pt = float(self._val("pt"))
        pt = pt - 65536 if pt > 32767 else pt
        inj = bool(self._val("shot", 0))
        self.inj_hist = self.inj_hist[1:] + [inj]
        shot = any(self.inj_hist)
        self.alarm |= any(bool(self.last.get(a, 0)) for a in ALARM_TAGS)
        tick = (now, self._val("sv"), self._val("hz"), self._val("ft"), pt, self._val("tt"), shot)
        out = None
        if self.prev_bu is None:                           # 데이터가 사이클 도중부터 시작하면 그 사이클은 버린다
            self.prev_bu, self.prev_pt = (0 if bu == 0 else -1), pt
            return None
        if self.prev_bu == -1:                             # 잘린 사이클이 끝날 때까지 기다림
            if bu == 0:
                self.prev_bu = 0
            self.prev_pt = pt
            return None
        if bu == 1 and self.prev_bu == 0:                  # 켜짐
            out = self._flush()                            # 이전 사이클 복귀 구간이 덜 찼으면 여기서 마감
            self.p0, self.on, self.off, self.off_since, self.alarm = self.prev_pt, [], [], None, False
        if bu == 1:
            self.on.append(tick)
        elif self.on:
            if self.off_since is None:
                self.off_since = now
            self.off.append(tick)
            if (now - self.on[-1][0]).total_seconds() > DECAY_WINDOW_SEC:
                out = self._flush()
        self.prev_bu, self.prev_pt = bu, pt
        return out

    def _flush(self):
        if not self.on:
            return None
        on = list(zip(*self.on))
        t0 = on[0][0]
        t = np.array([(x - t0).total_seconds() for x in on[0]])
        if self.off:
            off = list(zip(*self.off))
            dt = np.array([(x - on[0][-1]).total_seconds() for x in off[0]])   # 마지막 켜짐 틱 기준
            dft, dpt = np.array(off[3], float), np.array(off[4], float)
            keep = dt <= DECAY_WINDOW_SEC
            dt, dft, dpt = dt[keep], dft[keep], dpt[keep]
        else:
            dt = dft = dpt = None
        gap = (t0 - self.prev_end).total_seconds() if self.prev_end is not None else np.nan
        f = cycle_features(t, np.array(on[1], float), np.array(on[2], float), np.array(on[3], float),
                           np.array(on[4], float), np.array(on[5], float), np.array(on[6], bool),
                           dt, dft, dpt, prev_hz=self.prev_hz, gap_sec=gap, p0=self.p0, visc=self.visc)
        self.prev_end = on[0][-1]
        self.on, self.off = [], []
        if f is None:
            self.prev_hz = np.nan
            return None
        self.prev_hz = f["Hz"]
        f.update({"pump": self.cfg.pump, "t_start": t0, "t_end": on[0][-1], "flag_alarm": self.alarm,
                  "flag_any": self.alarm})
        return add_baseline_features(f, self.base) if self.base is not None else f


if __name__ == "__main__":
    print("new_pump_preprocessor — 사용법은 모듈 docstring 과 new_pump_preprocessor.md 참고")
    for k, c in PUMPS.items():
        print(f"  {k}: {c.material}, B={c.visc_B:.0f} K, 태그 {required_tags(c)}")
