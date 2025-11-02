"""This module defines functions for MCP tools associated with the core `dlt` library.

It shouldn't depend on packages that aren't installed by `dlt`
"""

from difflib import unified_diff
import json
import pprint
from typing import Any

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
    columns = list(dataset.schema.tables[LOADS_TABLE_NAME]["columns"])  # type: ignore
    return [dict(zip(columns, row)) for row in load_table]


def get_pipeline_local_state(pipeline_name: str) -> TPipelineState:
    """Retrieve the pipeline state information.
    Includes: incremental dates, resource state, source state
    """
    pipeline = dlt.attach(pipeline_name)
    return pipeline.state


def get_table_schema_changes(pipeline_name: str, table_name: str) -> str:
    """Retrieve the diff between versions of tables compared to it's previous version"""
    pipeline = dlt.attach(pipeline_name)

    dataset = pipeline.dataset()
    schemas = dataset.query(
        "select schema from _dlt_version order by inserted_at desc limit 2"
    ).df()

    current_schema = _load_schema_for_table(table_name, schemas.iloc[0]["schema"])
    previous_schema = _load_schema_for_table(table_name, schemas.iloc[1]["schema"])

    return _dict_diff(current_schema, previous_schema, "Previous Schema")


def _load_schema_for_table(table_name, schema):
    schema_dict = json.loads(schema).get("tables").get(table_name)
    return schema_dict


def _dict_diff(schema_dict, another_schema_dict, compared_to: str) -> str:
    # Convert dictionaries to string representation
    str1 = pprint.pformat(schema_dict)
    str2 = pprint.pformat(another_schema_dict)

    # Split into lines
    lines1 = str1.splitlines(keepends=True)
    lines2 = str2.splitlines(keepends=True)

    # Generate diff
    return "".join(
        unified_diff(lines2, lines1, fromfile="Current Schema", tofile=compared_to)
    )
