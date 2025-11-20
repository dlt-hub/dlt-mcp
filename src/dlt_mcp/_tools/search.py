from typing import Literal

import lancedb

from dlt_mcp._utilities.ingestion import (
    db_con,
    DLT_VERSION,
    DLT_DOCS_CHUNKS_TABLE_NAME,
    DLT_CODE_CHUNKS_TABLE_NAME,
    _maybe_ingest_docs_and_code,
)

LOCAL_DATA_IS_AVAILABLE = False
"""Global flag indicating the local LanceDB database powering search tools
is populated. This is asserted on the first search tool call.

We chose to conduct on first tool call instead of module init or server startup
to avoid blocking the server or slowing down operations unrelated to search. 
"""


def _ensure_docs_and_code_ingestion():
    global LOCAL_DATA_IS_AVAILABLE
    if not LOCAL_DATA_IS_AVAILABLE:
        _maybe_ingest_docs_and_code(DLT_VERSION)
        LOCAL_DATA_IS_AVAILABLE = True


def _retrieve_docs(
    table: lancedb.Table, query: str, query_type: Literal["fts", "vector", "hybrid"]
) -> list[dict]:
    retrieval_query = (
        table.search(query, query_type=query_type)
        .select(["text", "file_path"])
        .limit(3)
    )
    results = retrieval_query.to_list()
    return results


# TODO improve docstring to instruct with `mode` to use
# TODO maybe it doesn't need `vector` and
def search_docs(
    query: str, mode: Literal["hybrid", "full_text", "vector"] = "full_text"
) -> list[dict] | str:
    """Search over the `dlt` documentation. Use it to verify if a feature
    exists, answer general questions, or identify recommended patterns.
    """
    # TODO find a more elegant mechanism
    _ensure_docs_and_code_ingestion()

    query_type: Literal["fts", "vector", "hybrid"] = (
        "fts" if mode == "full_text" else mode
    )

    db = db_con(dlt_version=DLT_VERSION)
    table = db.open_table(DLT_DOCS_CHUNKS_TABLE_NAME)
    results = _retrieve_docs(table=table, query=query, query_type=query_type)
    if not results:
        return (
            f"No docs result found for `{query=}` and `{query_type=}`."
            " Consider using a different `query_type` or using a more appropriate `query`"
            " for this query type."
        )
    else:
        return results


def _retrieve_code(
    table: lancedb.Table,
    query: str,
    file_path: str | None,
) -> list[dict]:
    retrieval_query = (
        table.search(query, query_type="fts").select(["text", "file_path"]).limit(3)
    )
    if file_path:
        retrieval_query = retrieval_query.where(
            f"file_path = '{file_path}'", prefilter=True
        )
    results = retrieval_query.to_list()
    return results


# The source code search could degrade performance given the majority of
# code is internal and not public-facing APIs. It could help debug though.
def search_code(query: str, file_path: str | None = None) -> list[dict] | str:
    # TODO find a more elegant mechanism
    _ensure_docs_and_code_ingestion()

    db = db_con(dlt_version=DLT_VERSION)
    table = db.open_table(DLT_CODE_CHUNKS_TABLE_NAME)
    results = _retrieve_code(table=table, query=query, file_path=file_path)
    if not results:
        return (
            f"No code result found for `{query=}` and `{file_path=}`."
            " Consider using a different `file_path` filter or modifying the `query`."
        )
    else:
        return results
