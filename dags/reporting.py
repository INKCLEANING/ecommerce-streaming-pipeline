"""
Refreshes Athena partition metadata daily so Looker Studio always
queries the latest data without manual MSCK REPAIR.
"""
import os
from datetime import datetime, timedelta

import awswrangler as wr
from airflow import DAG
from airflow.operators.python import PythonOperator

S3_BUCKET = os.getenv("S3_BUCKET", "ecommerce-streaming-raw")
ATHENA_DATABASE = os.getenv("ATHENA_DATABASE", "ecommerce")
ATHENA_OUTPUT = os.getenv("ATHENA_OUTPUT_LOCATION", f"s3://{S3_BUCKET}/athena-results/")

default_args = {
    "owner": "pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

TABLES = ["orders_placed", "orders_shipped", "page_views", "dlq"]


def repair_table(table: str, **_):
    wr.athena.start_query_execution(
        sql=f"MSCK REPAIR TABLE {ATHENA_DATABASE}.{table}",
        database=ATHENA_DATABASE,
        s3_output=ATHENA_OUTPUT,
        wait=True,
    )
    print(f"[OK] Repaired partitions for {ATHENA_DATABASE}.{table}")


with DAG(
    dag_id="reporting_refresh",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["reporting"],
) as dag:

    for table in TABLES:
        PythonOperator(
            task_id=f"repair_{table}",
            python_callable=repair_table,
            op_kwargs={"table": table},
        )
