from __future__ import annotations

import pathlib
import random
import uuid
from typing import Iterator, Literal
from unittest.mock import patch

import pytest

# try importing lancedb before importing `dlt_mcp` modules using it
lancedb = pytest.importorskip(
    "lancedb", reason="`lancedb` is required to run search tools tests."
)

from dlt_mcp._utilities import ingestion  # noqa: E402
from dlt_mcp._tools import search  # noqa: E402


@pytest.fixture(scope="module")
def module_tmp_path(tmp_path_factory) -> pathlib.Path:
    return tmp_path_factory.mktemp("dlt-mcp_test_search")


# this fixture must `yield` to ensure the `with patch` context is
# preserved in the test function
@pytest.fixture(scope="module")
def page_chunks_table(module_tmp_path: pathlib.Path) -> Iterator[lancedb.Table]:
    with patch("pathlib.Path.home", return_value=module_tmp_path):
        db_con = ingestion.db_con(ingestion.DLT_VERSION)
        docs_chunks = [
            dict(
                id=str(uuid.uuid4()),
                text=f"This is a test chunk {i}",
                token_count=100,
                file_path="test.md",
                start_index=i * 100,
                end_index=(i + 1) * 100,
                chunk_index=i,
                vector=[
                    random.random()
                    for _ in range(ingestion._lancedb_embedding_model.ndims())
                ],
            )
            for i in range(10)
        ]
        yield ingestion.page_chunks_table(db_con, docs_chunks)


# this fixture must `yield` to ensure the `with patch` context is
# preserved in the test function
@pytest.fixture(scope="module")
def code_chunks_table(module_tmp_path: pathlib.Path) -> Iterator[lancedb.Table]:
    with patch("pathlib.Path.home", return_value=module_tmp_path):
        db_con = ingestion.db_con(ingestion.DLT_VERSION)
        code_chunks = [
            dict(
                id=str(uuid.uuid4()),
                text=f"This is a test chunk {i}",
                token_count=100,
                # `file_path` has to start with `dlt/` to be picked up
                file_path=f"dlt/{i % 2}/test.md",
                start_index=i * 100,
                end_index=(i + 1) * 100,
                chunk_index=i,
                vector=[
                    random.random()
                    for _ in range(ingestion._lancedb_embedding_model.ndims())
                ],
            )
            for i in range(10)
        ]
        yield ingestion.code_chunks_table(db_con, code_chunks)


@pytest.mark.parametrize("mode", ["full_text", "hybrid", "vector"])
def test_docs_search(
    page_chunks_table: lancedb.Table, mode: Literal["full_text", "hybrid", "vector"]
):
    if mode == "full_text":
        scoring_field = "_score"
        lower_is_better = False
    elif mode == "hybrid":
        scoring_field = "_relevance_score"
        lower_is_better = False
    elif mode == "vector":
        scoring_field = "_distance"
        lower_is_better = True

    expected_fields = set(["text", "file_path", scoring_field])

    # the query `chunk` needs to appear in the loaded mock data.
    results = search.search_docs("chunk", mode=mode)

    assert len(results) > 0
    assert isinstance(results, list)

    first_result = results[0]
    second_result = results[1]

    # check result type; could be a string
    assert isinstance(first_result, dict)

    # check available fields; this could change
    assert first_result.keys() == expected_fields
    assert first_result.keys() == second_result.keys()

    # check sorting; most-relevant at the top. For massive context retrieval,
    # some LLM may benefit from more relevant at the bottom
    if lower_is_better:
        assert first_result[scoring_field] <= second_result[scoring_field]
    else:
        assert first_result[scoring_field] >= second_result[scoring_field]


# `file_path` matches the loaded mock data.
@pytest.mark.parametrize("file_path", [None, "dlt/0/test.md"])
def test_code_search(code_chunks_table: lancedb.Table, file_path: str | None):
    results = search.search_code("test", file_path=file_path)

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

    if file_path:
        assert all(result["file_path"] == file_path for result in results)
