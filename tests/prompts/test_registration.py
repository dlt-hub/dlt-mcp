import asyncio

from dlt_mcp.server import create_server
from dlt_mcp._prompts import PROMPTS_REGISTRY


def test_expected_prompts_in_all_clause():
    """Ensures expected prompt references are in `PROMPTS_REGISTRY`.

    This test is expected to be modified as prompts are updated.

    Renaming or removing a prompt is technically a breaking change,
    but it can be patched downstream.
    """

    expected_prompt_names = [
        "infer_table_reference",
    ]

    assert len(PROMPTS_REGISTRY) == len(expected_prompt_names)
    assert set(PROMPTS_REGISTRY) == set(expected_prompt_names)


def test_expected_prompts_are_registered():
    """Ensures expected prompts exist on the server instance."""
    expected_prompt_names = [
        "infer_table_reference",
    ]

    mcp_server = create_server()

    prompts = asyncio.run(mcp_server.list_prompts())

    assert len(prompts) == len(expected_prompt_names)
    assert set(p.name for p in prompts) == set(expected_prompt_names)
