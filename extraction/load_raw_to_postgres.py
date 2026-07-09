"""
load_raw_to_postgres.py

Loads the raw, uncleaned CSVs (customers.csv, products.csv, orders_raw.csv) into the
`raw` schema in local PostgreSQL, exactly as they are, no transformation applied.

This script represents the "extraction" step of the pipeline: pulling data as-is from
source systems into a landing zone. Cleaning and standardization happen later, in dbt.

Requires: psycopg2-binary
    pip3 install psycopg2-binary

Usage:
    python3 load_raw_to_postgres.py

Connection settings are read from environment variables with sensible local defaults,
so this script can later point at a different host (e.g. a cloud Postgres instance)
without code changes.
"""

import csv
import os
import sys
import logging
import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

DB_CONFIG = {
    "host": os.getenv("PGHOST", "localhost"),
    "port": os.getenv("PGPORT", "5432"),
    "dbname": os.getenv("PGDATABASE", "unified_commerce"),
    "user": os.getenv("PGUSER", os.getenv("USER", "postgres")),
    "password": os.getenv("PGPASSWORD", ""),
}

RAW_DATA_DIR = "../raw_data"


def get_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        log.info(f"Connected to Postgres database '{DB_CONFIG['dbname']}' at {DB_CONFIG['host']}:{DB_CONFIG['port']}")
        return conn
    except psycopg2.OperationalError as e:
        log.error(f"Could not connect to Postgres: {e}")
        log.error("Check that Postgres is running and that the database exists. "
                   "See README section 'Phase 2 setup' for the createdb command.")
        sys.exit(1)


def load_csv(conn, csv_path, table_name, columns):
    """
    Loads a CSV into the given table using COPY, which is the fast, standard way to
    bulk load into Postgres (much faster than row-by-row INSERTs for tens of thousands
    of rows). Row counts are logged before and after so a partial/failed load is visible
    immediately rather than silently producing a short table.

    IDEMPOTENCY: the table is truncated immediately before loading. Without this, running
    the pipeline more than once (which happens constantly in practice, reruns, backfills,
    manual triggers, retries after a failure) silently appends duplicate copies of the
    same data on top of itself. That's a real production bug class, an unprotected load
    step isn't safe to re-run, and it's specifically what caused every downstream
    uniqueness test to fail after this DAG was triggered multiple times during testing:
    raw.customers ended up with 6 copies of each customer row, which fanned out into
    massive duplicate joins by the time it reached fct_orders. Truncating first makes
    every run start from a clean, predictable state, so reruns are safe.
    """
    if not os.path.exists(csv_path):
        log.error(f"File not found: {csv_path}. Did you run generate_raw_data.py first?")
        sys.exit(1)

    with open(csv_path) as f:
        expected_rows = sum(1 for _ in f) - 1  # minus header

    cur = conn.cursor()

    cur.execute(f"TRUNCATE TABLE {table_name}")
    conn.commit()
    log.info(f"Truncated {table_name} before load (keeps repeated runs idempotent).")

    col_list = ", ".join(columns)
    copy_sql = f"COPY {table_name} ({col_list}) FROM STDIN WITH CSV HEADER"

    with open(csv_path) as f:
        try:
            cur.copy_expert(copy_sql, f)
            conn.commit()
        except Exception as e:
            conn.rollback()
            log.error(f"Load failed for {table_name}: {e}")
            sys.exit(1)

    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    actual_rows = cur.fetchone()[0]
    cur.close()

    status = "OK" if actual_rows == expected_rows else "MISMATCH"
    log.info(f"Loaded {table_name}: expected {expected_rows} rows, table has {actual_rows} rows [{status}]")

    if status == "MISMATCH":
        log.warning(f"Row count mismatch on {table_name}. Investigate before proceeding to dbt.")


def main():
    conn = get_connection()

    load_csv(conn, f"{RAW_DATA_DIR}/customers.csv", "raw.customers",
              ["customer_id", "first_name", "last_name", "email", "region", "signup_date"])
    load_csv(conn, f"{RAW_DATA_DIR}/products.csv", "raw.products",
              ["product_id", "product_name", "category", "unit_cost", "unit_price"])
    load_csv(conn, f"{RAW_DATA_DIR}/orders_raw.csv", "raw.orders_raw",
              ["order_id", "customer_id", "product_id", "quantity", "order_date",
               "load_timestamp", "source_system"])

    conn.close()
    log.info("Raw load complete. Data is intentionally uncleaned at this stage.")


if __name__ == "__main__":
    main()
