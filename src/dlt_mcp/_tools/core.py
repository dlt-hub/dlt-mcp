"""This module defines functions for MCP tools associated with the core `dlt` library.

It shouldn't depend on packages that aren't installed by `dlt`
"""

import json
import pprint
from difflib import unified_diff
from typing import Any, Optional

import dlt
from dlt.common.schema.typing import LOADS_TABLE_NAME
from dlt.common.pipeline import TPipelineState
from dlt.common.schema.typing import TTableSchema
from dlt.common.pipeline import get_dlt_pipelines_dir
from dlt.common.storages.file_storage import FileStorage


def list_pipelines() -> list[str]:
    """List all available dlt pipelines. Each pipeline has several tables."""
    pipelines_dir = get_dlt_pipelines_dir()
    storage = FileStorage(pipelines_dir)
    dirs = storage.list_folder_dirs(".", to_root=False)
    return dirs


def list_tables(pipeline_name: str) -> list[str]:
    """List all available tables in the specified pipeline."""
    pipeline = dlt.attach(pipeline_name)
    schema = pipeline.default_schema
    return schema.data_table_names()


def get_table_schema(pipeline_name: str, table_name: str) -> TTableSchema:
    """Get the schema of the specified table."""
    # TODO refactor try/except to specific line or at the tool manager level
    # the inconsistent errors are probably due to database locking
    try:
        pipeline = dlt.attach(pipeline_name)
        table_schema = pipeline.default_schema.get_table(table_name)
        return table_schema
    except Exception:
        raise


def execute_sql_query(pipeline_name: str, sql_select_query: str) -> list[tuple]:
    f"""Executes SELECT SQL statement for simple data analysis.

    Use the `{list_tables.__name__}()` and `{get_table_schema.__name__}()` tools to 
    retrieve the available tables and columns.
    """
    pipeline = dlt.attach(pipeline_name)
    dataset = pipeline.dataset()
    results = dataset(sql_select_query).fetchall()

    return results


def get_load_table(pipeline_name: str) -> list[dict[str, Any]]:
    """Retrieve metadata about data loaded with dlt."""
    pipeline = dlt.attach(pipeline_name)
    dataset = pipeline.dataset()
    load_table = dataset(f"SELECT * FROM {LOADS_TABLE_NAME};").fetchall()
    columns = list(dataset.schema.tables[LOADS_TABLE_NAME].get("columns", []))
    return [dict(zip(columns, row)) for row in load_table]


def get_pipeline_local_state(pipeline_name: str) -> TPipelineState:
    """Retrieve the pipeline state information.
    Includes: incremental dates, resource state, source state
    """
    pipeline = dlt.attach(pipeline_name)
    return pipeline.state


def get_table_schema_diff(
    pipeline_name: str,
    table_name: str,
    another_version_hash: Optional[str] = None,
) -> str:
    """Get the diff between schema versions of a table."""
    NO_CHANGE_MSG = "There has been no change in the schema"

    pipeline = dlt.attach(pipeline_name)
    current_schema = pipeline.default_schema
    if not another_version_hash:
        another_version_hash = current_schema.previous_hashes[0]

    if another_version_hash == current_schema.version_hash:
        return NO_CHANGE_MSG

    dataset = pipeline.dataset()

    results: list | None = (
        dataset.table(current_schema.version_table_name)
        .where("version_hash", "eq", another_version_hash)
        .select("schema")
    ).fetchone()

    if not results:
        return NO_CHANGE_MSG

    schema_dict = json.loads(results[0]).get("tables").get(table_name)

    return _dict_diff(
        current_schema.tables.get(table_name, {}),
        schema_dict,
        "Previous schema",
    )


def _dict_diff(
    schema: dict | TTableSchema,
    other_schema: dict | TTableSchema,
    to_title: str,
    from_title: str = "Current schema",
) -> str:
    """Convert the two dictionaries to strings and compute string diff.

    Assumes the same key ordering in the two dictionaries.
    """
    str1 = pprint.pformat(schema)
    str2 = pprint.pformat(other_schema)

    lines1 = str1.splitlines(keepends=True)
    lines2 = str2.splitlines(keepends=True)

    diff = "".join(unified_diff(lines2, lines1, fromfile=from_title, tofile=to_title))
    return diff


def display_schema(pipeline_name: str, hide_columns: bool = False) -> str:
    """Generate a mermaid diagram to represent the pipeline schema

    pipeline_name: name of the pipeline
    hide_columns: when True, the columns are hidden
    """
    pipeline = dlt.attach(pipeline_name)
    return pipeline.default_schema.to_mermaid(hide_columns=hide_columns)
