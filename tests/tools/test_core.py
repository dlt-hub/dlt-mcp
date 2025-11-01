import dlt

@dlt.resource(table_name="users")
def user_data(updated_user: bool):
    if updated_user:
        yield [
            {"id": 1, "name": "Alice", "email": "alice@example.com", "age": 30},
            {"id": 2, "name": "Bob", "email": "bob@example.com", "age": 35} 
        ]
    yield [
            {"id": 1, "name": "Alice", "email": "alice@example.com"},
            {"id": 2, "name": "Bob", "email": "bob@example.com"} 
        ] 


def test_get_table_schema_changes_when_schema_has_changed():
    pipeline_name = "table_schema_change_pipeline"
    pipeline = dlt.pipeline(
        pipeline_name,
        destination="duckdb"
    )

    pipeline.run(user_data(False))
    pipeline.run(user_data(True))

    assert pipeline.schemas

