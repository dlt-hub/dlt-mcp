from __future__ import annotations

from types import FunctionType

from fastmcp import FastMCP

import dlt_mcp._tools


def get_available_tools() -> list[FunctionType]:
    return [getattr(dlt_mcp._tools, name) for name in dlt_mcp._tools.__all__]


def create_server() -> FastMCP:
    tools = get_available_tools()

    server = FastMCP(
        name="dlt MCP",
        instructions="Helps you build with the dlt Python library.",
        tools=tools,  # type: ignore[invalid-argument-type]
    )

    return server


def start() -> None:
    server = create_server()
    server.run()
