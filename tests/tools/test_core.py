import unittest.mock

import dlt

from dlt_mcp._tools import core


def test_get_table_schema_changes_when_schema_has_changed(
    tmp_pipeline: dlt.Pipeline,
) -> None:
    data_v1 = [
        {"id": 1, "name": "Alice", "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "email": "bob@example.com"},
    ]
    data_v2 = [
        {"id": 1, "name": "Alice", "email": "alice@example.com", "age": 30},
        {"id": 2, "name": "Bob", "email": "bob@example.com", "age": 35},
    ]

    tmp_pipeline.run([data_v1], table_name="users")
    tmp_pipeline.run([data_v2], table_name="users")

    expected_diff_message = """--- Current schema
+++ Previous schema
@@ -6,6 +6,7 @@
              '_dlt_load_id': {'data_type': 'text',
                               'name': '_dlt_load_id',
                               'nullable': False},
+             'age': {'data_type': 'bigint', 'name': 'age', 'nullable': True},
              'email': {'data_type': 'text', 'name': 'email', 'nullable': True},
              'id': {'data_type': 'bigint', 'name': 'id', 'nullable': True},
              'name': {'data_type': 'text', 'name': 'name', 'nullable': True}},
    """

    with unittest.mock.patch("dlt.attach", return_value=tmp_pipeline):
        diff = core.get_table_schema_diff(
            pipeline_name=tmp_pipeline.pipeline_name, table_name="users"
        )

    assert diff.strip() == expected_diff_message.strip()


def test_get_table_schema_should_say_no_change(tmp_pipeline: dlt.Pipeline) -> None:
    data_v1 = [
        {"id": 1, "name": "Alice", "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "email": "bob@example.com"},
    ]
    data_v2 = data_v1

    tmp_pipeline.run([data_v1], table_name="users")
    tmp_pipeline.run([data_v2], table_name="users")

    with unittest.mock.patch("dlt.attach", return_value=tmp_pipeline):
        diff = core.get_table_schema_diff(
            pipeline_name=tmp_pipeline.pipeline_name, table_name="users"
        )

    # Assert that there are no schema changes
    assert diff.strip() == "There has been no change in the schema"


def test_get_table_schema_with_same_version_hash(tmp_pipeline: dlt.Pipeline) -> None:
    data_v1 = [
        {"id": 1, "name": "Alice", "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "email": "bob@example.com"},
    ]

    load_info = tmp_pipeline.run([data_v1], table_name="users")
    version_hash = load_info.load_packages[0].schema_hash

    with unittest.mock.patch("dlt.attach", return_value=tmp_pipeline):
        diff = core.get_table_schema_diff(
            pipeline_name=tmp_pipeline.pipeline_name,
            table_name="users",
            another_version_hash=version_hash,
        )

    assert diff.strip() == "There has been no change in the schema"


def test_get_table_schema_with_different_version_hash(
    tmp_pipeline: dlt.Pipeline,
) -> None:
    data_v1 = [
        {"id": 1, "name": "Alice", "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "email": "bob@example.com"},
    ]
    data_v2 = [
        {"id": 1, "name": "Alice", "email": "alice@example.com", "age": 30},
        {"id": 2, "name": "Bob", "email": "bob@example.com", "age": 35},
    ]

    load_info_v1 = tmp_pipeline.run([data_v1], table_name="users")
    version_hash_v1 = load_info_v1.load_packages[0].schema_hash
    tmp_pipeline.run([data_v2], table_name="users")

    with unittest.mock.patch("dlt.attach", return_value=tmp_pipeline):
        diff = core.get_table_schema_diff(
            pipeline_name=tmp_pipeline.pipeline_name,
            table_name="users",
            another_version_hash=version_hash_v1,
        )

    expected_diff_message = """--- Current schema
+++ Previous schema
@@ -6,6 +6,7 @@
              '_dlt_load_id': {'data_type': 'text',
                               'name': '_dlt_load_id',
                               'nullable': False},
+             'age': {'data_type': 'bigint', 'name': 'age', 'nullable': True},
              'email': {'data_type': 'text', 'name': 'email', 'nullable': True},
              'id': {'data_type': 'bigint', 'name': 'id', 'nullable': True},
              'name': {'data_type': 'text', 'name': 'name', 'nullable': True}},
    """
    assert diff.strip() == expected_diff_message.strip()


def test_dict_diff_different_inputs() -> None:
    expected_diff_message = (
        "--- Current schema\n+++ different_dict\n@@ -1 +1 @@\n-{'b': 0}+{'a': 0}"
    )
    output = core._dict_diff({"a": 0}, {"b": 0}, "different_dict")

    assert output.strip() == expected_diff_message.strip()


def test_dict_diff_same_inputs() -> None:
    expected_diff_message = ""
    output = core._dict_diff({"a": 0}, {"a": 0}, "same_dict")

    assert output.strip() == expected_diff_message
