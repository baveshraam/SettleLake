import importlib.util
import shutil

import pytest

# Never call pytest.skip() at conftest import time: it aborts collection of every test directory that
# sorts after this one (that silently dropped tests/unit from the run). Ignore the Spark tests instead.
if importlib.util.find_spec("pyspark") is None or shutil.which("java") is None:
    collect_ignore_glob = ["test_*.py"]

from settlelake.config import get_paths  # noqa: E402
from settlelake.pipeline import run_pipeline  # noqa: E402
from settlelake.simulator.generator import GenConfig, generate  # noqa: E402


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    import os

    os.environ["SETTLELAKE_CATALOG"] = "local"
    os.environ["SETTLELAKE_WAREHOUSE"] = str(tmp_path_factory.mktemp("warehouse"))
    from settlelake.spark.session import build_spark

    s = build_spark("tests", **{"spark.driver.memory": "2g"})
    yield s
    s.stop()


@pytest.fixture(scope="session")
def dataset(tmp_path_factory):
    """One stress dataset processed by BOTH engines."""
    home = tmp_path_factory.mktemp("parity")
    paths = get_paths(home)
    generate(paths, GenConfig(1000, 42, "stress"))
    res = run_pipeline(home)
    return paths, res
