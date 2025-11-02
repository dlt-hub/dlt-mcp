import dlt

from dlt_mcp._tools.core import get_table_schema_changes


@dlt.resource(table_name="users")
def user_data(updated_user: bool):
    if updated_user:
        yield [
            {"id": 1, "name": "Alice", "email": "alice@example.com", "age": 30},
            {"id": 2, "name": "Bob", "email": "bob@example.com", "age": 35},
        ]
    yield [
        {"id": 1, "name": "Alice", "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "email": "bob@example.com"},
    ]


def test_get_table_schema_changes_when_schema_has_changed():
    pipeline_name = "table_schema_change_pipeline"
    pipeline = dlt.pipeline(pipeline_name, destination="duckdb", dev_mode=True)

    pipeline.run(user_data(False))
    pipeline.run(user_data(True))

    expected_diff_message = """--- Current Schema
+++ Previous Schema
@@ -6,6 +6,7 @@
              '_dlt_load_id': {'data_type': 'text',
                               'name': '_dlt_load_id',
                               'nullable': False},
+             'age': {'data_type': 'bigint', 'name': 'age', 'nullable': True},
              'email': {'data_type': 'text', 'name': 'email', 'nullable': True},
              'id': {'data_type': 'bigint', 'name': 'id', 'nullable': True},
              'name': {'data_type': 'text', 'name': 'name', 'nullable': True}},
    """

    diff = get_table_schema_changes(pipeline_name, "users")
    # Print actual and expected diff for readability in case of failure
    assert diff.strip() == expected_diff_message.strip(), f"""
    Expected and actual schema differences do not match.
    
    Expected:
    {expected_diff_message.strip()}
    
    Actual:
    {diff.strip()}
    """


def test_get_table_schema_should_say_no_change():
    pipeline_name = "no_change_pipeline"
    pipeline = dlt.pipeline(pipeline_name, destination="duckdb", dev_mode=True)

    # Run the resource twice with the same schema
    pipeline.run(user_data(False))
    pipeline.run(user_data(False))

    # Get schema changes
    diff = get_table_schema_changes(pipeline_name, "users")

    # Assert that there are no schema changes
    assert diff.strip() == "There has been no change in the schema", f"""
    Expected no schema changes, but got:
    {diff.strip()}
    """
