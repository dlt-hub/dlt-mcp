from decimal import Decimal
import dlt
import pytest


# Define the customers resource
@dlt.resource(
    primary_key="id",
    write_disposition="merge",
    nested_hints={
        "purchases": dlt.mark.make_nested_hints(
            columns=[{"name": "customer_id", "data_type": "bigint"}],
            primary_key=["customer_id", "id"],
            write_disposition="merge",
            references=[
                {
                    "referenced_table": "customers",
                    "columns": ["customer_id"],
                    "referenced_columns": ["id"],
                },
            ],
        )
    },
)
def customers():
    """Load customer data from a list."""
    yield [
        {
            "id": 1,
            "name": "Simon",
            "city": "Berlin",
            "purchases": [{"id": 1, "name": "Apple", "price": Decimal("1.50")}],
        },
        {
            "id": 2,
            "name": "Violet",
            "city": "London",
            "purchases": [{"id": 2, "name": "Banana", "price": Decimal("1.70")}],
        },
        {
            "id": 3,
            "name": "Tammo",
            "city": "New York",
            "purchases": [{"id": 3, "name": "Pear", "price": Decimal("2.50")}],
        },
    ]


# Define the products resource
@dlt.resource(primary_key="id", write_disposition="merge")
def products():
    """Load product data."""
    yield [
        {"id": 1, "name": "Apple", "price": Decimal("1.50")},
        {"id": 2, "name": "Banana", "price": Decimal("1.70")},
        {"id": 3, "name": "Pear", "price": Decimal("2.50")},
    ]


# Define the orders resource
@dlt.resource(
    primary_key="id",
    write_disposition="merge",
    nested_hints={
        "order_items": dlt.mark.make_nested_hints(
            primary_key=["order_id", "product_id"],
            write_disposition="merge",
            references=[
                {
                    "referenced_table": "products",
                    "columns": ["product_id"],
                    "referenced_columns": ["id"],
                },
                {
                    "referenced_table": "customers",
                    "columns": ["customer_id"],
                    "referenced_columns": ["id"],
                },
            ],
        )
    },
)
def orders():
    """Load order data from a list."""
    yield [
        {
            "id": 1,
            "customer_id": 1,
            "order_date": "2024-10-10",
            "order_items": [
                {"order_id": 1, "product_id": 1, "quantity": 2},
                {"order_id": 1, "product_id": 2, "quantity": 1},
            ],
        },
        {
            "id": 2,
            "customer_id": 2,
            "order_date": "2024-10-11",
            "order_items": [
                {"order_id": 2, "product_id": 2, "quantity": 3},
                {"order_id": 2, "product_id": 3, "quantity": 1},
            ],
        },
        {
            "id": 3,
            "customer_id": 3,
            "order_date": "2024-10-12",
            "order_items": [
                {"order_id": 3, "product_id": 1, "quantity": 1},
                {"order_id": 3, "product_id": 3, "quantity": 2},
            ],
        },
    ]


# Pushdown functions to populate relational data
def _pushdown_customer_id(row):
    id_ = row["id"]
    for purchase in row["purchases"]:
        purchase["customer_id"] = id_
    return row


def _pushdown_product_id(row):
    id_ = row["id"]
    for item in row["order_items"]:
        item["product_id"] = id_
    return row


def _pushdown_order_id(row):
    id_ = row["id"]
    for item in row["order_items"]:
        item["order_id"] = id_
    return row


# Create a dlt pipeline for test purposes
@pytest.fixture
def pipeline():
    p = dlt.pipeline(
        pipeline_name="test_complete_pipeline",
        destination="duckdb",
        dataset_name="local",
    )
    return p


# Alternative test function that demonstrates the merge functionality
def test_pipeline_merge_functionality(pipeline):
    # Load customers with pushdown function
    pipeline.run(customers().add_map(_pushdown_customer_id))

    # Load products
    pipeline.run(products())

    # Load orders with pushdown functions
    pipeline.run(orders().add_map(_pushdown_order_id).add_map(_pushdown_product_id))

    # Load same data again to prove that merge works
    pipeline.run(customers().add_map(_pushdown_customer_id))
    pipeline.run(orders().add_map(_pushdown_order_id).add_map(_pushdown_product_id))

    # Check row counts (should be the same after merge)
    row_count = pipeline.dataset().row_counts().fetchall()

    assert row_count == [
        ("customers", 3),
        ("customers__purchases", 3),
        ("products", 3),
        ("orders", 3),
        ("orders__order_items", 3),
    ]
