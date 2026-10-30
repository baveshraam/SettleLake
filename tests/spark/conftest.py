import shutil

import pytest

pytest.importorskip("pyspark")
if shutil.which("java") is None:
    pytest.skip("Java runtime not available", allow_module_level=True)

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
