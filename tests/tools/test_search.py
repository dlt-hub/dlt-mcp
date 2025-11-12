import pathlib
from typing import Iterator
from unittest.mock import patch

import pytest
import lancedb

from dlt_mcp._utilities import ingestion
from dlt_mcp._tools import search

# TODO ensure certain extras are installed before running pytest module


@pytest.fixture(scope="module")
def module_tmp_path(tmp_path_factory) -> pathlib.Path:
    return tmp_path_factory.mktemp("dlt-mcp_test_search")


# this fixture must `yield` to ensure the `with patch` context is
# preserved in the test function
@pytest.fixture(scope="module")
def page_chunks_table(module_tmp_path: pathlib.Path) -> Iterator[lancedb.Table]:
    with patch("pathlib.Path.home", return_value=module_tmp_path):
        yield ingestion._ingest_docs(dlt_version="1.18.1")


# this fixture must `yield` to ensure the `with patch` context is
# preserved in the test function
@pytest.fixture(scope="module")
def code_chunks_table(module_tmp_path: pathlib.Path) -> Iterator[lancedb.Table]:
    with patch("pathlib.Path.home", return_value=module_tmp_path):
        yield ingestion._ingest_code(dlt_version="1.18.1")


def test_docs_search(page_chunks_table: lancedb.Table):
    results = search.search_docs("What is incremental loading?", mode="full_text")

    assert len(results) > 0
    assert isinstance(results, list)

    first_result = results[0]
    second_result = results[1]

    # check result type; could be a string
    assert isinstance(first_result, dict)
    # check available fields; this could change
    assert first_result.keys() == set(["text", "file_path", "_score"])
    assert first_result.keys() == second_result.keys()
    # check sorting; most-relevant at the top. For massive context retrieval,
    # some LLM may benefit from more relevant at the bottom
    assert first_result["_score"] >= second_result["_score"]


def test_code_search(code_chunks_table: lancedb.Table):
    results = search.search_code(
        "Where is `TerminalDestinationException` defined?",
        file_path="dlt/core/pipeline.py",
    )

    # exact number of retrieved results
    assert len(results) > 0
    assert isinstance(results, list)

    first_result = results[0]
    second_result = results[1]

    # check result type; could be a string
    assert isinstance(first_result, dict)
    # check available fields; this could change
    assert first_result.keys() == set(["text", "file_path", "_score"])
    assert first_result.keys() == second_result.keys()
    # check sorting; most-relevant at the top. For massive context retrieval,
    # some LLM may benefit from more relevant at the bottom
    assert first_result["_score"] >= second_result["_score"]
