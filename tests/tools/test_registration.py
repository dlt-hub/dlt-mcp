import asyncio

from dlt_mcp.server import create_server
from dlt_mcp._tools import TOOLS_REGISTRY


def test_expected_tools_in_all_clause():
    """Ensures expected tool references are in `TOOLS_REGISTRY`.

    This test is expected to be modified as tools are updated.

    Renaming or removing a function used by a tool is technically a breaking change,
    but it can be patched downstream.
    """

    expected_tool_names = [
        "list_pipelines",
        "list_tables",
        "get_table_schema",
        "execute_sql_query",
        "get_load_table",
        "get_pipeline_local_state",
        "search_code",
        "search_docs",
        "get_table_schema_diff",
        "display_schema",
    ]

    assert len(TOOLS_REGISTRY) == len(expected_tool_names)
    assert set(TOOLS_REGISTRY) == set(expected_tool_names)


def test_expected_tools_are_registered():
    """Ensures expected tools exist on the server instance."""
    expected_tool_names = [
        "list_pipelines",
        "list_tables",
        "get_table_schema",
        "execute_sql_query",
        "get_load_table",
        "get_pipeline_local_state",
        "search_code",
        "search_docs",
        "get_table_schema_diff",
        "display_schema",
    ]

    mcp_server = create_server()

    tools = asyncio.run(mcp_server.get_tools())

    assert len(tools) == len(expected_tool_names)
    assert set(tools) == set(expected_tool_names)
