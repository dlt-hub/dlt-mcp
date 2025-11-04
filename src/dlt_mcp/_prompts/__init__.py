from types import FunctionType
from fastmcp.prompts import Prompt

from dlt_mcp._prompts.infer_table_reference import infer_table_reference


__all__ = [
    "PROMPTS_REGISTRY",
]


PROMPTS_REGISTRY: dict[str, Prompt] = {}


def register_prompt(fn: FunctionType) -> FunctionType:
    global PROMPTS_REGISTRY
    PROMPTS_REGISTRY[fn.__name__] = Prompt.from_function(fn)
    return fn


register_prompt(infer_table_reference)
