import random
from typing import Dict, List, Any
from dlt.common import pendulum
import dlt
import pytest


# Generate mock data for products
def generate_products() -> List[Dict[str, Any]]:
    products = []
    product_names = [
        "Wireless Headphones",
        "Smartphone",
        "Laptop",
        "Smart Watch",
        "Tablet",
        "Bluetooth Speaker",
        "Gaming Console",
        "Camera",
    ]
    categories = ["Electronics", "Computers", "Audio", "Wearables", "Cameras"]

    for i in range(100):
        products.append(
            {
                "id": i + 1,
                "name": random.choice(product_names),
                "category": random.choice(categories),
                "price": round(random.uniform(20.0, 1000.0), 2),
                "qty": random.randint(0, 100),
                "created_at": pendulum.now()
                .subtract(days=random.randint(0, 365))
                .isoformat(),
            }
        )
    return products


# Generate mock data for customers
def generate_customers() -> List[Dict[str, Any]]:
    customers = []
    first_names = ["John", "Jane", "Mike", "Sarah", "David", "Lisa", "Tom", "Emma"]
    last_names = [
        "Smith",
        "Johnson",
        "Williams",
        "Brown",
        "Jones",
        "Garcia",
        "Miller",
        "Davis",
    ]
    cities = [
        "New York",
        "Los Angeles",
        "Chicago",
        "Houston",
        "Phoenix",
        "Philadelphia",
    ]

    for i in range(50):
        customers.append(
            {
                "_id": i + 1,
                "first_name": random.choice(first_names),
                "last_name": random.choice(last_names),
                "email": f"user{i + 1}@example.com",
                "city": random.choice(cities),
                "date": pendulum.now()
                .subtract(days=random.randint(0, 365))
                .isoformat(),
                "moanie": round(random.uniform(0.0, 5000.0), 2),
            }
        )
    return customers


# Generate mock data for orders
def generate_orders(
    customers: List[Dict[str, Any]], products: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    orders = []
    order_status = ["completed", "pending", "shipped", "cancelled"]

    for i in range(200):
        customer = random.choice(customers)
        product = random.choice(products)
        quantity = random.randint(1, 5)
        order_date = pendulum.now().subtract(days=random.randint(0, 30))

        orders.append(
            {
                "_id": i + 1,
                "customer": customer["_id"],
                "product": product["id"],
                "quantity": quantity,
                "prices": product["price"],
                "total": round(product["price"] * quantity, 2),
                "date": order_date.isoformat(),
                "status": random.choice(order_status),
                "address": f"{random.randint(1, 1000)} {random.choice(['Main St', 'Oak Ave', 'Pine Rd'])}, {random.choice(['New York', 'Los Angeles', 'Chicago'])}",
            }
        )
    return orders


# Generate mock data for returns
def generate_returns(orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    returns = []
    return_reasons = ["Defective", "Wrong Item", "Not Satisfied", "Changed Mind"]

    # Only some orders will have returns
    order_ids_with_returns = random.sample(
        [order["_id"] for order in orders], k=min(30, len(orders))
    )

    for order_id in order_ids_with_returns:
        order = next((o for o in orders if o["_id"] == order_id), None)
        if order:
            returns.append(
                {
                    "_id": len(returns) + 1,
                    "some_order_id": order_id,
                    "cust_id": order["customer"],
                    "product": order["product"],
                    "qty": random.randint(1, order["quantity"]),
                    "why": random.choice(return_reasons),
                    "return_date": pendulum.now()
                    .subtract(days=random.randint(1, 30))
                    .isoformat(),
                    "refund_amount": round(random.uniform(10.0, order["prices"]), 2),
                    "status": random.choice(["processed", "pending", "refunded"]),
                }
            )
    return returns


# Create DLT resources
@dlt.resource(table_name="products")
def products():
    """Resource for products data"""
    yield generate_products()


@dlt.resource(table_name="customers")
def customers():
    """Resource for customers data"""
    yield generate_customers()


@dlt.resource(table_name="orders")
def orders():
    """Resource for orders data"""
    yield generate_orders(generate_customers(), generate_products())


@dlt.resource(table_name="returns")
def returns():
    orders = generate_orders(generate_customers(), generate_products())
    yield generate_returns(orders)


# Run the pipeline
def ecommerce_pipeline():
    """Main source that combines all resources"""
    pipeline = dlt.pipeline(
        "table_reference_sandbox", destination="duckdb", dev_mode=True
    )

    pipeline.run(customers)
    pipeline.run(products)
    pipeline.run(orders)
    pipeline.run(returns)


@pytest.mark.sandbox
def test_initiate_sandbox():
    ecommerce_pipeline()
