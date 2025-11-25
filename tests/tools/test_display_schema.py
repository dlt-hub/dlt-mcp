from unittest.mock import patch
import dlt
from src.dlt_mcp._tools import core


def test_display_schema_generates_mermaid_diagram(
    tmp_pipeline: dlt.Pipeline,
) -> None:
    data = [
        {"id": 1, "name": "Alice", "email": "alice@example.com", "age": 30},
        {"id": 2, "name": "Bob", "email": "bob@example.com", "age": 35},
    ]
    
    tmp_pipeline.run([data], table_name="users")
    
    with patch("dlt.attach", return_value=tmp_pipeline):
        erdiagram = core.display_schema(
            pipeline_name=tmp_pipeline.pipeline_name
        )
    
    assert "users" in erdiagram
    assert "name" in erdiagram
    assert "email" in erdiagram
    assert "age" in erdiagram


def test_display_schema_generates_mermaid_diagram_without_columns(
    tmp_pipeline: dlt.Pipeline,
) -> None:
    data = [
        {"id": 1, "name": "Alice", "email": "alice@example.com", "age": 30},
        {"id": 2, "name": "Bob", "email": "bob@example.com", "age": 35},
    ]
    
    tmp_pipeline.run([data], table_name="users")
    
    with patch("dlt.attach", return_value=tmp_pipeline):
        erdiagram = core.display_schema(
            pipeline_name=tmp_pipeline.pipeline_name,
            hide_columns=True
        )
    
    assert "users" in erdiagram
    assert "name" not in erdiagram
    assert "email" not in erdiagram
    assert "age" not in erdiagram
