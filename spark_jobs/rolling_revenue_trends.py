"""
rolling_revenue_trends.py

The PySpark step of the pipeline. This is where a heavier, genuinely
Spark-appropriate computation happens: rolling 7-day and 30-day revenue trends
across every order, computed with window functions over date-ordered partitions.

WHY SPARK HERE (and not plain SQL, which could technically do this too):
This project's dataset is modest (60K rows) on purpose, so it's demonstrable on
a laptop, but the pattern here, daily aggregation followed by sliding-window
trend calculations across a full order history, is exactly the kind of
computation that stops being practical in plain SQL once you're dealing with
millions of orders across many years: window functions over huge partitions in
a transactional database compete with production query load and don't scale
horizontally. Spark's DataFrame API expresses the same rolling-window logic but
can distribute the computation across a cluster, which is the actual reason
data teams reach for Spark for this kind of trend analysis at real scale.

Reads fct_orders directly from Postgres via JDBC, aggregates to daily revenue,
computes rolling metrics, and writes the result back to a new table,
marts.spark_revenue_trends, so it's queryable by BI tools alongside the dbt marts.
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

PGHOST = os.getenv("PGHOST", "host.docker.internal")
PGPORT = os.getenv("PGPORT", "5432")
PGDATABASE = os.getenv("PGDATABASE", "unified_commerce")
PGUSER = os.getenv("PGUSER")
PGPASSWORD = os.getenv("PGPASSWORD", "")

JDBC_URL = f"jdbc:postgresql://{PGHOST}:{PGPORT}/{PGDATABASE}"


def main():
    spark = SparkSession.builder.appName("unified_commerce_rolling_revenue_trends").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    log.info(f"Reading dbt_dev_marts.fct_orders from {JDBC_URL}")

    fct_orders = (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "dbt_dev_marts.fct_orders")
        .option("user", PGUSER)
        .option("password", PGPASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    row_count = fct_orders.count()
    log.info(f"Loaded {row_count} rows from fct_orders")

    # Aggregate to one row per day: total revenue and order count that day.
    daily_revenue = (
        fct_orders.groupBy("order_date")
        .agg(
            F.sum("revenue").alias("daily_revenue"),
            F.count("order_id").alias("daily_order_count"),
        )
        .orderBy("order_date")
    )

    # Rolling windows: 7-day and 30-day trailing sums, computed by row count within
    # a date-ordered window rather than a literal day-range, since order_date has no
    # gaps to worry about here after the dbt cleaning layer already deduped and
    # standardized dates upstream.
    window_7d = Window.orderBy("order_date").rowsBetween(-6, 0)
    window_30d = Window.orderBy("order_date").rowsBetween(-29, 0)

    trends = (
        daily_revenue.withColumn("revenue_rolling_7d", F.sum("daily_revenue").over(window_7d))
        .withColumn("revenue_rolling_30d", F.sum("daily_revenue").over(window_30d))
        .withColumn("avg_daily_revenue_7d", F.round(F.avg("daily_revenue").over(window_7d), 2))
        .withColumn("avg_daily_revenue_30d", F.round(F.avg("daily_revenue").over(window_30d), 2))
    )

    trend_row_count = trends.count()
    log.info(f"Computed rolling trends for {trend_row_count} distinct order dates")

    # Write result back to Postgres so it's queryable alongside the dbt marts.
    (
        trends.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "dbt_dev_marts.spark_revenue_trends")
        .option("user", PGUSER)
        .option("password", PGPASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )

    log.info("Wrote dbt_dev_marts.spark_revenue_trends. PySpark step complete.")
    spark.stop()


if __name__ == "__main__":
    main()
