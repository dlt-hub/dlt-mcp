from types import FunctionType


__all__ = [
    "TOOLS_REGISTRY",
]


TOOLS_REGISTRY: dict[str, FunctionType] = {}


def register_tool(fn: FunctionType) -> FunctionType:
    global TOOLS_REGISTRY
    TOOLS_REGISTRY[fn.__name__] = fn
    return fn


# register core tools
from dlt_mcp._tools.core import (  # noqa: E402
    list_pipelines,
    list_tables,
    get_table_schemas,
    execute_sql_query,
    get_load_table,
    get_pipeline_local_state,
    get_table_schema_diff,
)


register_tool(list_pipelines)
register_tool(list_tables)
register_tool(get_table_schemas)
register_tool(execute_sql_query)
register_tool(get_load_table)
register_tool(get_pipeline_local_state)
register_tool(get_table_schema_diff)

try:
    from dlt_mcp._tools.search import search_docs, search_code

    register_tool(search_docs)
    register_tool(search_code)
except ImportError:
    pass
