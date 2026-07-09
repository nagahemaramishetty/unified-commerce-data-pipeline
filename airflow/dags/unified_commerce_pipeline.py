"""
unified_commerce_pipeline.py

Airflow DAG that orchestrates the core pipeline:

    load_raw_data  ->  dbt_run  ->  dbt_test

Each task only runs if the one before it succeeded. If dbt_run fails (a broken
model), dbt_test never runs, and the DAG shows a clear red failure in the UI
instead of silently producing stale or partial marts. This is the orchestration
layer's whole job: make failures visible and stop bad data from propagating
downstream, rather than a cron job that just fails quietly in a log file no one
reads.

A PySpark aggregation step is added as a fourth task in a later revision of
this DAG (see mart_monthly_kpis discussion in the README for why the heavier
aggregation is split out separately).
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator

log = logging.getLogger(__name__)


def alert_on_failure(context):
    """
    Basic failure alerting. In a real production setup this would post to
    Slack or send an email (Airflow supports both natively via
    on_failure_callback). For this project, it logs a clearly visible error
    so a failed run is never silent, which is the actual point of
    orchestration: making failures loud, not just having a schedule.
    """
    task_id = context['task_instance'].task_id
    dag_id = context['task_instance'].dag_id
    execution_date = context['execution_date']
    log.error(
        f"PIPELINE FAILURE: task '{task_id}' in DAG '{dag_id}' failed "
        f"at {execution_date}. Check task logs in the Airflow UI for details."
    )


default_args = {
    "owner": "unified_commerce_pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": alert_on_failure,
}

with DAG(
    dag_id="unified_commerce_pipeline",
    description="Loads raw e-commerce data, cleans it via dbt, and validates it with dbt tests.",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["unified-commerce", "portfolio-project"],
) as dag:

    load_raw_data = BashOperator(
        task_id="load_raw_data",
        bash_command="cd /opt/project/extraction && python3 load_raw_to_postgres.py",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/project/dbt_project && dbt run",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/project/dbt_project && dbt test",
    )

    pyspark_revenue_trends = BashOperator(
        task_id="pyspark_revenue_trends",
        bash_command=(
            "export JAVA_HOME=$(dirname $(dirname $(readlink -f $(command -v java)))) && "
            "cd /opt/project/spark_jobs && python3 rolling_revenue_trends.py"
        ),
    )
    load_raw_data >> dbt_run >> dbt_test >> pyspark_revenue_trends
