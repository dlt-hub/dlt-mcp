from typing import Literal

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


# TODO improve docstring to instruct with `mode` to use
# TODO maybe it doesn't need `vector` and
def search_docs(
    query: str, mode: Literal["hybrid", "full_text", "vector"] = "full_text"
) -> list[dict]:
    """Search over the `dlt` documentation. Use it to verify if a feature
    exists, answer general questions, or identify recommended patterns.
    """
    # TODO find a more elegant mechanism
    global LOCAL_DATA_IS_AVAILABLE
    if not LOCAL_DATA_IS_AVAILABLE:
        _maybe_ingest_docs_and_code(DLT_VERSION)
        LOCAL_DATA_IS_AVAILABLE = True

    query_type: Literal["fts", "vector", "hybrid"] = (
        "fts" if mode == "full_text" else mode
    )

    db = db_con(dlt_version=DLT_VERSION)
    table = db.open_table(DLT_DOCS_CHUNKS_TABLE_NAME)

    retrieval_query = (
        table.search(query, query_type=query_type)
        .select(["text", "file_path"])
        .limit(3)
    )
    results = retrieval_query.to_list()
    return results


# The source code search could degrade performance given the majority of
# code is internal and not public-facing APIs. It could help debug though.
def search_code(query: str, file_path: str | None = None) -> list[dict]:
    # TODO find a more elegant mechanism
    global LOCAL_DATA_IS_AVAILABLE
    if not LOCAL_DATA_IS_AVAILABLE:
        _maybe_ingest_docs_and_code(DLT_VERSION)
        LOCAL_DATA_IS_AVAILABLE = True

    db = db_con(dlt_version=DLT_VERSION)

    db = db_con(dlt_version=DLT_VERSION)
    table = db.open_table(DLT_CODE_CHUNKS_TABLE_NAME)

    retrieval_query = (
        table.search(query, query_type="fts").select(["text", "file_path"]).limit(3)
    )
    if file_path:
        retrieval_query = retrieval_query.where(
            f"file_path = '{file_path}'", prefilter=True
        )

    results = retrieval_query.to_list()
    return results
