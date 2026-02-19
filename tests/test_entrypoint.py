import subprocess

import fastmcp
import pytest

import dlt_mcp.server


@pytest.mark.parametrize("command", [("python", "-m", "dlt_mcp"), ("dlt-mcp",)])
def test_launch_server_entrypoint(command: tuple[str, ...]) -> None:
    """Test that `python -m dlt_mcp` starts the MCP server"""
    mcp_server_process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    timeout = 5
    expected_message = "INFO     Starting MCP server"

    try:
        # Start the server process and wait for output (with timeout)
        proc = subprocess.Popen(
            ["python", "-m", "dlt_mcp"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        # Wait for server output or timeout
        output, _ = proc.communicate(timeout=timeout)

        # Check for expected startup message
        assert expected_message in output, (
            f"Didn't find `{expected_message}` in startup message. Output:\n{output}"
        )

    except subprocess.TimeoutExpired:
        mcp_server_process.kill()
        output, _ = mcp_server_process.communicate()
        raise AssertionError(
            f"MCP server didn't produce startup output in {timeout=} seconds. Partial output:\n\n{output}"
        )

    finally:
        # Ensure process is cleaned up
        if mcp_server_process.poll() is None:
            mcp_server_process.terminate()


def test_create_server_returns_fastmcp_server() -> None:
    mcp_server = dlt_mcp.server.create_server()

    assert isinstance(mcp_server, fastmcp.FastMCP)
