"""운영 backend 복사본 중 하나를 골라 import 할 수 있게 한다.

두 버전의 패키지 이름이 같다 (config, tools_yeji, models_structure). 그래서
- 한 번에 한 버전만 import 된다. use() 가 다른 버전으로 바꿀 때 이미 불러온 모듈을 비운다.
  바꾸기 전에 만든 객체는 옛 버전 코드로 계속 돈다 (섞어 쓰지 않는다).
- topology·근무표 경로는 환경변수로 고정한다. 로더가 먼저 현재 폴더의 ./shared 를 찾으므로,
  고정하지 않으면 작업 폴더에 따라 다른 버전의 topology 를 읽을 수 있다.
"""
import os
import sys
from contextlib import redirect_stdout
from io import StringIO

from . import paths

_PKGS = ("config", "tools_yeji", "models_structure")
_current = None


def use(version="v2.1", quiet=True):
    """version: 'v2.1' (10-15 ~ 11-06 17:25) / 'v2.2' (11-06 17:25 ~)."""
    global _current
    v = paths.MODEL_VERSIONS[version]
    if _current == version:
        return v
    for name in [m for m in sys.modules if m.split(".")[0] in _PKGS]:
        del sys.modules[name]
    for other in paths.MODEL_VERSIONS.values():
        while str(other["backend"]) in sys.path:
            sys.path.remove(str(other["backend"]))
    sys.path.insert(0, str(v["backend"]))
    os.environ["TOPOLOGY_PATH"] = str(v["shared"] / "topology.json")
    os.environ["OPERATION_PATH"] = str(v["shared"] / "operation.json")
    if quiet:
        with redirect_stdout(StringIO()):
            import config.topology_config  # noqa: F401  (로드 메시지 숨김)
            import config.operation_config  # noqa: F401
    _current = version
    return v


def current():
    return _current
