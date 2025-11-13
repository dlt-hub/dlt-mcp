from typing import Literal

from dlt_mcp._utilities.ingestion import (
    db_con,
    DLT_VERSION,
    DLT_DOCS_CHUNKS_TABLE_NAME,
    DLT_CODE_CHUNKS_TABLE_NAME,
)


# TODO add mechanism to ingest docs on first launch
# TODO optimized docs could be released as a zip on the `dlt-mcp` repo


# TODO improve docstring to instruct with `mode` to use
# TODO maybe it doesn't need `vector` and
def search_docs(
    query: str, mode: Literal["hybrid", "full_text", "vector"] = "full_text"
) -> list[dict]:
    """Search over the `dlt` documentation. Use it to verify if a feature
    exists, answer general questions, or identify recommended patterns.
    """
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
