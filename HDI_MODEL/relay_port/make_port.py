"""HDI_Relay 백엔드로 옮길 펌프 사이클 모델 묶음을 만든다 — 원본에서 **실시간에 쓰는 것만** 뽑아 백엔드 폴더 구조로.

    python HDI_MODEL/relay_port/make_port.py          # → HDI_MODEL/relay_port/out/backend/...
    python HDI_MODEL/relay_port/verify_port.py        # 원본과 이식본이 같은 결과를 내는지 재생으로 확인

원본 코드를 손으로 베끼지 않는다 — 함수·클래스 소스를 ast 로 그대로 잘라 붙인다. 원본을 고치거나 다시 학습하면
이 스크립트를 다시 돌리면 된다. 학습·배치·분석 코드(fit, run, replay, validate …)는 넣지 않는다.

    out/backend/
      models/pump_cycle/                    학습 결과물 (복사)
        model.pt, meta.json                 가중치 / 표준화 통계·임계값
        pump_settings.json                  펌프·탱크·헤드·원료·파라미터
        baseline/P1.json ~ I7.json          펌프별 물리 기준선
      models_structure/Pump_Cycle_AE.py     신경망 구조 (CycleAE)
      tools_yeji/pump_models/
        pump_params.py                      설정·자산 경로·태그·점도·기준선 읽기
        pump_preprocess.py                  틱 → 사이클 상태기계, 사이클 피처, 기준선 피처
        pump_judge.py                       채점(CycleScorer) · 창 판정·알림(WindowTracker)
        pump_diagnose.py                    규칙표·문구표 · 진단(PumpDiagnoser) → 경보 목록
        pump_realtime.py                    14대 직렬 묶음(PumpRealtimeGroup)
      tools_yeji/Pump_Monitor.py            [MOCK] 교체본 — 묶음을 갖고 근무 단위 유지 → Pump_Score (백엔드가 부르는 것)
                                            (손으로 쓴 파일: relay_port/backend/tools_yeji/Pump_Monitor.py 를 복사)
"""

import ast
import os
import shutil

HERE = os.path.dirname(os.path.abspath(__file__))
MODEL_DIR = os.path.dirname(HERE)
OUT = os.path.join(HERE, "out", "backend")
GLUE = os.path.join(HERE, "backend")          # 백엔드에 맞춰 손으로 쓴 파일 (그대로 복사)
SRC = {
    "pre": os.path.join(MODEL_DIR, "new_pump_preprocessor.py"),
    "model": os.path.join(MODEL_DIR, "cycle_ae", "model.py"),
    "diag": os.path.join(MODEL_DIR, "cycle_ae", "diagnose.py"),
    "rt": os.path.join(MODEL_DIR, "Pump_Realtime_Model.py"),
}
GEN = "# 자동 생성 — DB_Log/HDI_MODEL/relay_port/make_port.py. 원본: {src}\n"


class Source:
    def __init__(self, path):
        self.path = path
        self.text = open(path, encoding="utf-8").read()
        self.lines = self.text.splitlines(keepends=True)
        self.tree = ast.parse(self.text)
        self.nodes = {}
        for n in self.tree.body:
            if isinstance(n, (ast.FunctionDef, ast.ClassDef)):
                self.nodes[n.name] = n
            elif isinstance(n, ast.Assign):
                for t in n.targets:
                    if isinstance(t, ast.Name):
                        self.nodes[t.id] = n
            elif isinstance(n, ast.AnnAssign) and isinstance(n.target, ast.Name):
                self.nodes[n.target.id] = n

    @staticmethod
    def _start(n):
        return min([n.lineno] + [d.lineno for d in getattr(n, "decorator_list", [])])

    def get(self, name, drop=()):
        """최상위 정의 하나의 소스. drop: 클래스에서 뺄 메서드 이름."""
        n = self.nodes[name]
        a, b = self._start(n) - 1, n.end_lineno
        keep = list(range(a, b))
        if drop:
            for m in n.body:
                if isinstance(m, ast.FunctionDef) and m.name in drop:
                    cut = set(range(self._start(m) - 1, m.end_lineno))
                    keep = [i for i in keep if i not in cut]
        out = "".join(self.lines[i] for i in keep)
        # 메서드를 빼며 남은 연속 빈 줄 정리
        while "\n\n\n\n" in out:
            out = out.replace("\n\n\n\n", "\n\n\n")
        return out.rstrip() + "\n"


def sub(text, old, new):
    assert text.count(old) == 1, f"원본이 바뀌었다 — 치환 대상 못 찾음: {old[:60]!r}"
    return text.replace(old, new)


def write(rel, text):
    path = os.path.join(OUT, rel)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    return path


def section(title):
    return f"\n\n# {'-' * 75} #\n# {title}\n# {'-' * 75} #\n"


def build():
    pre, mdl, dg, rt = (Source(SRC[k]) for k in ("pre", "model", "diag", "rt"))
    if os.path.exists(OUT):
        shutil.rmtree(OUT)

    # ------------------------------------------------------------------ 신경망 구조
    write("models_structure/Pump_Cycle_AE.py",
          '# models_structure/Pump_Cycle_AE.py\n"""펌프 사이클 AE — 14대 공용 조건부 오토인코더 구조. 가중치는 models/pump_cycle/model.pt"""\n'
          + GEN.format(src="HDI_MODEL/cycle_ae/model.py") + "import torch\nimport torch.nn as nn\n\n"
          + mdl.get("FEATURES") + mdl.get("CONDS") + "\n\n" + mdl.get("CycleAE"))

    # ------------------------------------------------------------------ pump_params
    params = ('# tools_yeji/pump_models/pump_params.py\n"""펌프 사이클 모델 — 설정·자산 경로·태그·점도·기준선 읽기.\n\n'
              "자산(학습 결과물)은 `models/pump_cycle/` — 환경변수 `PUMP_CYCLE_DIR` 로 바꿀 수 있다.\n"
              "가동 플래그는 탱크·헤드와 같은 `config.operation_config.RUN_FLAG_TAG` 로 대신 받는다 (TAG_ALIASES).\n"
              '"""\n' + GEN.format(src="HDI_MODEL/new_pump_preprocessor.py, Pump_Realtime_Model.py")
              + "import json\nimport os\nfrom dataclasses import asdict, dataclass, field\nfrom types import SimpleNamespace\n\n"
              + "import numpy as np\n\nfrom config.operation_config import RUN_FLAG_TAG\n\n"
              + "#: 학습 결과물 폴더 (model.pt, meta.json, pump_settings.json, baseline/)\n"
              + 'ASSET_DIR = os.environ.get("PUMP_CYCLE_DIR") or os.path.join(\n'
              + '    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "models", "pump_cycle")\n'
              + 'DEFAULT_SETTINGS = os.path.join(ASSET_DIR, "pump_settings.json")\n'
              + 'BASELINE_DIR = os.path.join(ASSET_DIR, "baseline")\n'
              + rt.get("PUMPS")
              + section("설정 · 점도")
              + pre.get("load_settings") + "\n\n" + pre.get("mu_rel") + "\n\n" + pre.get("ViscosityTable") + "\n\n"
              + pre.get("pump_tags") + "\n\n" + pre.get("required_tags") + "\n\n" + pre.get("PumpSetup")
              + "\n\n" + sub(rt.get("tag_aliases"), 'al = {settings["run_tag"]: ["Auto_Running_PL"]}',
                             'al = {settings["run_tag"]: [RUN_FLAG_TAG]}')
              + section("기준선 (읽기만 — 맞추기는 DB_Log/HDI_MODEL/baseline/make_baselines.py)")
              + pre.get("PumpBaseline", drop=("fit", "save")))
    write("tools_yeji/pump_models/pump_params.py", params)
    write("tools_yeji/pump_models/__init__.py",
          '# tools_yeji/pump_models\n"""펌프 사이클 모델 — 판정만. 얼마나 오래 띄울지(근무 단위 유지)는 tools_yeji/Pump_Monitor.\n\n'
          "    pump_params      설정·자산 경로(models/pump_cycle)·태그·점도·기준선 읽기\n"
          "    pump_preprocess  틱 → 사이클 상태기계(PumpProcessor), 사이클 피처, 기준선 피처\n"
          "    pump_judge       채점(CycleScorer) · 창 판정·알림(WindowTracker)\n"
          "    pump_diagnose    규칙표·문구표 · 진단(PumpDiagnoser) → 경보 목록\n"
          "    pump_realtime    14대 직렬 묶음(PumpRealtimeGroup) — Pump_Monitor 가 갖고 매 tick 부른다\n"
          '"""\n' + GEN.format(src="HDI_MODEL/relay_port/make_port.py"))

    # ------------------------------------------------------------------ pump_preprocess
    prep = ('# tools_yeji/pump_models/pump_preprocess.py\n"""펌프 전처리 — 틱을 하나씩 받는 상태기계(PumpProcessor), 사이클 피처, 기준선 피처.\n\n'
            "사이클이 끝나면(복귀창 5초 뒤) 사이클 피처 한 줄을 낸다. 배치 함수(run, circulation_windows)는 넣지 않았다.\n"
            '"""\n' + GEN.format(src="HDI_MODEL/new_pump_preprocessor.py")
            + "from collections import deque\n\nimport numpy as np\nimport pandas as pd\n\n"
            + "from tools_yeji.pump_models.pump_params import PumpSetup  # noqa: F401 (형식 참고)\n"
            + section("작은 계산 도구")
            + pre.get("crossing_time") + "\n\n" + pre.get("half_time") + "\n\n" + pre.get("_mode") + "\n\n"
            + pre.get("cycle_features") + "\n\n" + pre.get("add_baseline_features")
            + section("증압 → 안정 꺾임 (실시간 판정)") + pre.get("KneeTracker")
            + section("상태기계") + pre.get("PHASES") + "\n\n" + pre.get("PumpProcessor"))
    write("tools_yeji/pump_models/pump_preprocess.py", prep)

    # ------------------------------------------------------------------ pump_judge
    consts = "".join(mdl.get(n) for n in ("FEATURE_KO", "CLIP", "MIN_FIT", "TIERS", "LEVELS", "RESET_GAP_MIN",
                                          "COOLDOWN_MIN", "DAY_START_HOUR", "ZQ"))
    scorer = sub(mdl.get("CycleScorer"), "def load(cls, path=ARTIFACT_DIR):", "def load(cls, path=ASSET_DIR):")
    judge = ('# tools_yeji/pump_models/pump_judge.py\n"""펌프 사이클 판정 — 채점(CycleScorer)과 창 판정·알림(WindowTracker).\n\n'
             "- CycleScorer.score_one: 사이클 1개 → 패턴 점수(AE 복원 오차)·수준 점수·사이클 등급(정상/주의/알림)·이유·피처별 편차\n"
             "- WindowTracker.update: 펌프별 최근 10사이클 → 펌프 등급, 사람에게 알림(운영일 1회), 창 편차 중앙값\n"
             "임계값은 meta.json 의 숫자 — DB_Log/HDI_MODEL/cycle_ae/calibrate.py 가 계산한다.\n"
             '"""\n' + GEN.format(src="HDI_MODEL/cycle_ae/model.py")
             + "import json\nimport os\nimport warnings\nfrom collections import deque\n\n"
             + "import numpy as np\nimport pandas as pd\nimport torch\n\n"
             + "from models_structure.Pump_Cycle_AE import CONDS, FEATURES, CycleAE  # noqa: F401\n"
             + "from tools_yeji.pump_models.pump_params import ASSET_DIR\n\n"
             + consts + section("표준화 · 점수")
             + "\n\n".join(mdl.get(n) for n in ("group_key", "add_group", "prepare", "zsigned", "zmax_of", "errors",
                                                  "score_from_errors", "explain"))
             + section("등급 · 창 판정") + mdl.get("add_levels") + "\n\n" + mdl.get("WindowTracker")
             + section("채점기") + scorer)
    write("tools_yeji/pump_models/pump_judge.py", judge)

    # ------------------------------------------------------------------ pump_diagnose
    diag_cls = dg.get("PumpDiagnoser")
    diag_cls = sub(diag_cls, '''    def __init__(self, settings=None, topology_path=TOPOLOGY_PATH):
        s = settings or load_settings()''', '''    def __init__(self, settings=None):
        s = settings or load_settings()''')
    diag_cls = sub(diag_cls, "        self.resolve = RelayResolver(topology_path)", "        self.resolve = RelayResolver()")
    resolver = sub(dg.get("RelayResolver"), '''    def __init__(self, path=TOPOLOGY_PATH):
        topo = json.load(open(path, encoding="utf-8"))
        self.relays = topo["relays"]''', '''    def __init__(self, relays=None):
        self.relays = RELAY_CONFIG if relays is None else relays''')
    dconsts = "".join(dg.get(n) for n in ("E", "STRONG", "COMMON_WINDOW_MIN", "COMMON_MIN_PUMPS"))
    diagnose = ('# tools_yeji/pump_models/pump_diagnose.py\n"""펌프 진단 — 창 판정 결과를 "어디가 왜" 로 해석해 경보 목록으로 낸다.\n\n'
                "규칙표·문구표는 RULES. 경보 {reason, priority, type, message, show, pumps, part}\n"
                "(+ 참고 필드 pump, head, level, evidence, label, relay, relay_fallback). 근무 단위 유지는 Pump_Monitor 가 한다.\n"
                "relay(헤드 SOL·PT 릴레이 주소)는 채워만 둔다 — 나중에 헤드 판정을 헤드 SOL 마커에도 띄울 때 쓸 자리.\n"
                '"""\n' + GEN.format(src="HDI_MODEL/cycle_ae/diagnose.py")
                + "from collections import deque\n\nimport numpy as np\nimport pandas as pd\n\n"
                + "from config.topology_config import RELAY_CONFIG\n"
                + "from tools_yeji.pump_models.pump_params import load_settings\n\n"
                + dconsts + section("규칙표 · 문구표") + dg.get("RULES") + dg.get("COMMON_SUFFIX") + "\n\n"
                + dg.get("evidence") + "\n\n" + dg.get("classify")
                + section("릴레이 주소 — topology") + dg.get("HEAD_SOL") + "\n\n" + resolver
                + section("진단기") + diag_cls)
    write("tools_yeji/pump_models/pump_diagnose.py", diagnose)

    # ------------------------------------------------------------------ pump_realtime
    grp = rt.get("PumpRealtimeGroup")
    grp = sub(grp, "        self.scorer = cae.CycleScorer.load(artifact_dir or cae.ARTIFACT_DIR)",
              "        self.scorer = CycleScorer.load(artifact_dir or ASSET_DIR)")
    grp = sub(grp, "        self.win = cae.WindowTracker(self.scorer.meta)", "        self.win = WindowTracker(self.scorer.meta)")
    grp = sub(grp, "        t = to_kst_naive(current_time)", "        t = to_kst(current_time)")
    realtime = ('# tools_yeji/pump_models/pump_realtime.py\n"""펌프 14대 사이클 실시간 진단 — 전처리 → 채점 → 창 판정 → 진단 → 경보. 14대 직렬.\n'
                "tools_yeji/Pump_Monitor 가 하나 갖고 매 tick 부른다.\n\n"
                "    mon = PumpRealtimeGroup()                        # 한 번 (모델·기준선 읽기)\n"
                "    print(mon.missing_tags(live_data.keys()))        # 처음 한 번 — 빠진 감시 태그\n"
                "    alerts = mon.update(now, live_data)              # 매 tick — 진단 경보 목록 (pump_diagnose 형식)\n"
                "    scores = mon.pump_scores()                       # 14대 전량 {펌프: dict 또는 None} — Pump_Score 형식\n"
                "    state  = mon.status()                            # 펌프별 화면 상태 (정지/대기 순환/점검·수동/자동 사출 + 등급)\n\n"
                "자동운전 사이클만 판정·경보한다. 수동·점검 사이클은 참고 채점만, 알람 사이클·학습 범위 밖은 제외.\n"
                "수집 간격(백엔드 진단 모드 2초 틱 · 3초 넘는 공백)에 걸린 사이클도 참고 채점만 — pump_preprocess 의 가드.\n"
                "이 펌프만의 태그(BuildUp·SV·Hz·FT·PT)가 row 에 있을 때만 그 펌프 tick 을 진행한다.\n"
                '"""\n' + GEN.format(src="HDI_MODEL/Pump_Realtime_Model.py")
                + "import os\nfrom collections import deque\n\nimport numpy as np\nimport pandas as pd\n\n"
                + "from tools_yeji.common.time_utils import to_kst\n"
                + "from tools_yeji.pump_models.pump_diagnose import PumpDiagnoser\n"
                + "from tools_yeji.pump_models.pump_judge import CycleScorer, WindowTracker\n"
                + "from tools_yeji.pump_models.pump_params import (ASSET_DIR, BASELINE_DIR, PUMPS, PumpBaseline, PumpSetup,\n"
                + "                                                 load_settings, tag_aliases)\n"
                + "from tools_yeji.pump_models.pump_preprocess import PumpProcessor, add_baseline_features\n\n\n"
                + rt.get("_top_features") + "\n\n" + grp)
    write("tools_yeji/pump_models/pump_realtime.py", realtime)

    # ------------------------------------------------------------------ 백엔드 연결 (손으로 쓴 파일)
    for root, _, fs in os.walk(GLUE):
        for f in fs:
            if f.endswith(".py"):
                rel = os.path.relpath(os.path.join(root, f), GLUE)
                os.makedirs(os.path.dirname(os.path.join(OUT, rel)), exist_ok=True)
                shutil.copy2(os.path.join(root, f), os.path.join(OUT, rel))

    # ------------------------------------------------------------------ 자산
    adir = os.path.join(OUT, "models", "pump_cycle")
    os.makedirs(os.path.join(adir, "baseline"), exist_ok=True)
    art = os.path.join(MODEL_DIR, "cycle_ae", "artifacts")
    for f in ("model.pt", "meta.json"):
        shutil.copy2(os.path.join(art, f), adir)
    shutil.copy2(os.path.join(MODEL_DIR, "pump_settings.json"), adir)
    for f in sorted(os.listdir(os.path.join(MODEL_DIR, "baseline"))):
        if f.endswith(".json") and not f.startswith("_"):
            shutil.copy2(os.path.join(MODEL_DIR, "baseline", f), os.path.join(adir, "baseline"))

    files = []
    for root, _, fs in os.walk(OUT):
        for f in fs:
            p = os.path.join(root, f)
            files.append((os.path.relpath(p, OUT), os.path.getsize(p)))
    for rel, size in sorted(files):
        print(f"  {rel:55s} {size / 1024:7.1f} KB")
    print("생성:", OUT)


if __name__ == "__main__":
    build()
