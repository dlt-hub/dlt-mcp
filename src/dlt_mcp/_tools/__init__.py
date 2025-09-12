from dlt_mcp._tools.core import (
    list_pipelines,
    list_tables,
)


__all__ = [
    "list_pipelines",
    "list_tables",
]


# conditionally add to `__all__`
try:
    from dlt_mcp._tools.knowledge import (
        search_docs,
    )

    __all__ += [
        "search_docs",
    ]
except Exception:
    pass
