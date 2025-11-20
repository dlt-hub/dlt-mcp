import pathlib
from typing import Generator

import pytest
import dlt


@pytest.fixture(scope="module")
def module_tmp_path(tmp_path_factory) -> pathlib.Path:
    """Temporary directory that persist for the lifetime of test `.py` file."""
    return tmp_path_factory.mktemp("pytest_dlt-mcp")


@pytest.fixture(scope="function")
def tmp_duckdb_destination(
    tmp_path: pathlib.Path,
) -> Generator[dlt.destinations.duckdb, None, None]:
    """Function-scoped temporary duckdb destination."""
    tmp_duckdb_path = tmp_path / "data.duckdb"
    dest = dlt.destinations.duckdb(str(tmp_duckdb_path))
    yield dest


@pytest.fixture(scope="function")
def tmp_pipelines_dir(tmp_path: pathlib.Path) -> pathlib.Path:
    return tmp_path / "pipelines_dir"


@pytest.fixture(scope="function")
def tmp_pipeline(
    request,
    tmp_duckdb_destination: dlt.destinations.duckdb,
    tmp_pipelines_dir: pathlib.Path,
) -> dlt.Pipeline:
    """Temporary pipeline with a name that matches the test name. It has its
    own `pipelines_dir` and duckdb destination instance.
    """
    return dlt.pipeline(
        pipeline_name=request.node.name,
        pipelines_dir=str(tmp_pipelines_dir),
        destination=tmp_duckdb_destination,
    )
