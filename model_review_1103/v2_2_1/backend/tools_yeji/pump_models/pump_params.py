# tools_yeji/pump_models/pump_params.py
"""펌프 사이클 모델 — 설정·자산 경로·태그·점도·기준선 읽기.

자산(학습 결과물)은 `models/pump_cycle/` — 환경변수 `PUMP_CYCLE_DIR` 로 바꿀 수 있다.
가동 플래그는 탱크·헤드와 같은 `config.operation_config.RUN_FLAG_TAG` 로 대신 받는다 (TAG_ALIASES).
"""
# 자동 생성 — DB_Log/HDI_MODEL/relay_port/make_port.py. 원본: HDI_MODEL/new_pump_preprocessor.py, Pump_Realtime_Model.py
import json
import os
from dataclasses import asdict, dataclass, field
from types import SimpleNamespace

import numpy as np

from config.operation_config import RUN_FLAG_TAG

#: 학습 결과물 폴더 (model.pt, meta.json, pump_settings.json, baseline/)
ASSET_DIR = os.environ.get("PUMP_CYCLE_DIR") or os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "models", "pump_cycle")
DEFAULT_SETTINGS = os.path.join(ASSET_DIR, "pump_settings.json")
BASELINE_DIR = os.path.join(ASSET_DIR, "baseline")
PUMPS = ["P1", "I1", "P2", "I2", "P3", "I3", "P4", "I4", "P5", "I5", "P6", "I6", "P7", "I7"]


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


def pump_tags(pump, settings):
    """펌프 태그 이름 (원료 검증 여부와 무관 — 데이터 추출용으로도 쓴다)."""
    pc = settings["pumps"][pump]
    tags = {"bu": f"Pump_BuildUp_{pump}", "sv": f"g_s_SV_{pump}", "hz": f"Hz_Out_{pump}",
            "ft": f"Scale_Out___FT_{pump}", "pt": f"Scale_Out___PT_{pump}",
            "tt": f"Scale_Out___TT_{pc['tank']}",                 # 원료 탱크 기준 (P2 → TT_P5)
            "shot": f"Shot_Injection_{pc['head']}_P{pc['slot']}"}  # ISO 는 같은 번호 폴리올과 공유
    return tags


def required_tags(pump, settings):
    """LogExtractor.get_data 에 넣을 태그 목록 — 펌프 태그 + 알람 4개 + Auto_Running."""
    return list(pump_tags(pump, settings).values()) + list(settings["alarm_tags"]) + [settings["run_tag"]]


class PumpSetup:
    """펌프 한 대의 실행 설정 — 태그, 원료, 파라미터, 점도표. 프로세스 시작 때 한 번 만든다."""

    def __init__(self, pump, settings=None, path=DEFAULT_SETTINGS, allow_unverified=False):
        s = settings or load_settings(path)
        if pump not in s["pumps"]:
            raise KeyError(f"{pump} 가 설정에 없음")
        pc = s["pumps"][pump]
        if pc["material"] is None or (not pc["verified"] and not allow_unverified):
            raise ValueError(f"{pump}: 원료 점도 미검증 ({pc.get('material_basis')}) — "
                             f"검증 전에 쓰려면 allow_unverified=True (make_pump_settings.py 의 MATERIAL_OF 참고)")
        mat = s["materials"][pc["material"]]
        self.pump, self.material_name, self.material, self.B = pump, pc["material"], mat["type"], mat["B"]
        self.p = SimpleNamespace(**{**s["params"], **pc.get("params", {})})   # 펌프별 덮어쓰기 우선
        self.tags = pump_tags(pump, s)
        self.alarm_tags, self.run_tag = s["alarm_tags"], s["run_tag"]
        self.verified = pc["verified"]
        self.circ_hz = float(pc.get("circ_hz", self.p.CIRC_HZ))   # 순환 Hz 지령 (P1/I1 20, I7 30, 나머지 25)
        self.visc = ViscosityTable(self.B, self.p.T_REF, self.p.VISC_TABLE_RAW, self.p.TEMP_SCALE)
        self.settings_version = s.get("version")

    def required_tags(self):
        """LogExtractor.get_data 에 그대로 넣을 태그 목록."""
        return list(self.tags.values()) + list(self.alarm_tags) + [self.run_tag]


def tag_aliases(settings):
    """전처리기 태그 이름 → 백엔드에서 대신 올 수 있는 이름들."""
    al = {settings["run_tag"]: [RUN_FLAG_TAG]}
    for p, v in settings["pumps"].items():
        al[f"Scale_Out___TT_{v['tank']}"] = [f"TK_Temp_PV_{v['tank']}"]
    return al


# --------------------------------------------------------------------------- #
# 기준선 (읽기만 — 맞추기는 DB_Log/HDI_MODEL/baseline/make_baselines.py)
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
    def load(cls, path):
        with open(path, encoding="utf-8") as f:
            return cls(**json.load(f))
