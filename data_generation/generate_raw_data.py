"""
generate_raw_data.py

Generates synthetic e-commerce data for the End-to-End Business Analytics Platform project.

Simulates two source systems feeding into the business, the way a real company often has
a legacy order system and a newer web platform running in parallel:

  - "legacy_pos"   : an older point-of-sale / order system. Dates stored as MM/DD/YYYY strings.
                     Occasionally double-writes an order due to a retry bug (duplicate order_id
                     with identical or near-identical payload).
  - "web_platform" : the newer e-commerce backend. Dates stored as ISO 8601 (YYYY-MM-DD).
                     A small percentage of orders reference a customer_id or product_id that
                     doesn't exist in the dimension tables (orphaned foreign key), simulating
                     a sync lag between the customer service and the order service.

Also simulates late-arriving data: a batch of orders from the last reporting period that
didn't land until after that period's reports had already gone out, which is a very common
real-world data engineering problem and worth documenting explicitly in the README.

Output: raw CSVs written to ../raw_data/, structured as if pulled directly from each source
system, i.e. NOT pre-cleaned. Cleaning happens downstream in the pipeline, not here.
"""

import csv
import random
import uuid
from datetime import datetime, timedelta

random.seed(42)  # reproducible runs

OUTPUT_DIR = "../raw_data"

N_CUSTOMERS = 2000
N_PRODUCTS = 150
N_ORDERS = 60000
CATEGORIES = ["Electronics", "Home & Kitchen", "Apparel", "Beauty", "Sports & Outdoors",
              "Books", "Toys", "Grocery", "Office Supplies", "Pet Supplies"]
REGIONS = ["Northeast", "Southeast", "Midwest", "Southwest", "West"]
CHANNELS = ["legacy_pos", "web_platform"]

START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2026, 6, 30)


def random_date(start, end):
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))


def gen_customers(n):
    customers = []
    for i in range(1, n + 1):
        customers.append({
            "customer_id": f"CUST-{i:05d}",
            "first_name": f"FirstName{i}",
            "last_name": f"LastName{i}",
            "email": f"customer{i}@example.com",
            "region": random.choice(REGIONS),
            "signup_date": random_date(START_DATE, END_DATE).strftime("%Y-%m-%d"),
        })
    return customers


def gen_products(n):
    products = []
    for i in range(1, n + 1):
        category = random.choice(CATEGORIES)
        products.append({
            "product_id": f"PROD-{i:04d}",
            "product_name": f"{category} Item {i}",
            "category": category,
            "unit_cost": round(random.uniform(3, 150), 2),
            "unit_price": round(random.uniform(10, 400), 2),
        })
    return products


def gen_orders(n, customers, products):
    """
    Generates orders across both source systems with deliberate, documented messiness:

    1. DUPLICATE ORDERS (legacy_pos retry bug, ~2.5% of legacy orders):
       Same order_id written twice, sometimes with a slightly different order_timestamp
       (simulating a retry a few seconds later), which is exactly the kind of duplicate
       that a naive dedup on order_id + timestamp would miss.

    2. INCONSISTENT DATE FORMATS:
       legacy_pos writes order_date as MM/DD/YYYY. web_platform writes ISO YYYY-MM-DD.
       This has to be standardized downstream, not here.

    3. ORPHANED FOREIGN KEYS (~1% of web_platform orders):
       customer_id or product_id references a record that doesn't exist in the dimension
       files, simulating a customer/product service sync lag.

    4. LATE-ARRIVING RECORDS (~3% of all orders):
       order_date falls in a period earlier than the record's load_timestamp, i.e. the record
       shows up in an extract well after the period it belongs to already closed.
    """
    orders = []
    customer_ids = [c["customer_id"] for c in customers]
    product_ids = [p["product_id"] for p in products]

    for i in range(1, n + 1):
        order_id = f"ORD-{i:06d}"
        channel = random.choices(CHANNELS, weights=[0.35, 0.65])[0]
        order_dt = random_date(START_DATE, END_DATE)

        # orphaned FK injection for web_platform
        if channel == "web_platform" and random.random() < 0.01:
            customer_id = f"CUST-{random.randint(N_CUSTOMERS + 1, N_CUSTOMERS + 500):05d}"  # doesn't exist
        else:
            customer_id = random.choice(customer_ids)

        if channel == "web_platform" and random.random() < 0.01:
            product_id = f"PROD-{random.randint(N_PRODUCTS + 1, N_PRODUCTS + 100):04d}"  # doesn't exist
        else:
            product_id = random.choice(product_ids)

        quantity = random.randint(1, 5)

        # late-arriving record: load_timestamp is well after order_dt
        if random.random() < 0.03:
            load_dt = order_dt + timedelta(days=random.randint(35, 70))
        else:
            load_dt = order_dt + timedelta(hours=random.randint(0, 4))

        date_str = order_dt.strftime("%m/%d/%Y") if channel == "legacy_pos" else order_dt.strftime("%Y-%m-%d")

        record = {
            "order_id": order_id,
            "customer_id": customer_id,
            "product_id": product_id,
            "quantity": quantity,
            "order_date": date_str,
            "load_timestamp": load_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "source_system": channel,
        }
        orders.append(record)

        # retry-bug duplicate injection, legacy_pos only
        if channel == "legacy_pos" and random.random() < 0.025:
            dup = record.copy()
            retry_dt = load_dt + timedelta(seconds=random.randint(5, 90))
            dup["load_timestamp"] = retry_dt.strftime("%Y-%m-%d %H:%M:%S")
            orders.append(dup)

    return orders


def write_csv(path, rows, fieldnames):
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main():
    import os
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    customers = gen_customers(N_CUSTOMERS)
    products = gen_products(N_PRODUCTS)
    orders = gen_orders(N_ORDERS, customers, products)

    write_csv(f"{OUTPUT_DIR}/customers.csv", customers,
              ["customer_id", "first_name", "last_name", "email", "region", "signup_date"])
    write_csv(f"{OUTPUT_DIR}/products.csv", products,
              ["product_id", "product_name", "category", "unit_cost", "unit_price"])
    write_csv(f"{OUTPUT_DIR}/orders_raw.csv", orders,
              ["order_id", "customer_id", "product_id", "quantity", "order_date",
               "load_timestamp", "source_system"])

    dup_count = len(orders) - len(set(o["order_id"] for o in orders))
    orphan_cust = sum(1 for o in orders if o["customer_id"] not in {c["customer_id"] for c in customers})
    orphan_prod = sum(1 for o in orders if o["product_id"] not in {p["product_id"] for p in products})
    late = sum(1 for o in orders
               if (datetime.strptime(o["load_timestamp"], "%Y-%m-%d %H:%M:%S") -
                   (datetime.strptime(o["order_date"], "%m/%d/%Y") if o["source_system"] == "legacy_pos"
                    else datetime.strptime(o["order_date"], "%Y-%m-%d"))).days > 30)

    print(f"Customers: {len(customers)}")
    print(f"Products: {len(products)}")
    print(f"Orders (raw, includes duplicates): {len(orders)}")
    print(f"  Duplicate order_ids from retry bug: {dup_count}")
    print(f"  Orphaned customer_id references: {orphan_cust}")
    print(f"  Orphaned product_id references: {orphan_prod}")
    print(f"  Late-arriving records (>30 days after order_date): {late}")
    print(f"\nFiles written to {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
