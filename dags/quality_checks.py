"""
Runs Great Expectations validation suites against the latest S3 partition
for each event type and publishes HTML data docs back to S3.
Scheduled: every hour.
"""
import os
from datetime import datetime, timedelta

import awswrangler as wr
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

S3_BUCKET = os.getenv("S3_BUCKET", "ecommerce-streaming-raw")

default_args = {
    "owner": "pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

SUITES = [
    ("orders.placed", "orders_placed"),
    ("orders.shipped", "orders_shipped"),
    ("page.views", "page_views"),
]


def run_ge_validation(topic: str, suite_name: str, **context):
    import great_expectations as ge

    execution_date: datetime = context["execution_date"]
    prefix = (
        f"{topic}/year={execution_date.year}"
        f"/month={execution_date.month:02d}"
        f"/day={execution_date.day:02d}/"
    )

    # read all files in today's partition
    paths = wr.s3.list_objects(f"s3://{S3_BUCKET}/{prefix}")
    if not paths:
        print(f"No data found at s3://{S3_BUCKET}/{prefix} — skipping")
        return

    dfs = []
    for path in paths:
        content = wr.s3.read_json(path, lines=True)
        dfs.append(content)
    df = pd.concat(dfs, ignore_index=True)

    ge_df = ge.from_pandas(df)
    suite_path = f"/opt/airflow/great_expectations/expectations/{suite_name}.json"
    suite = ge.core.ExpectationSuite(
        expectation_suite_name=suite_name,
        expectations=ge.core.ExpectationSuite.from_json_dict(
            open(suite_path).read()
        ).expectations,
    )

    results = ge_df.validate(expectation_suite=suite, result_format="SUMMARY")

    success = results["success"]
    stats = results["statistics"]
    print(
        f"[{suite_name}] success={success} | "
        f"evaluated={stats['evaluated_expectations']} | "
        f"passed={stats['successful_expectations']} | "
        f"failed={stats['unsuccessful_expectations']}"
    )

    # write result summary to S3 for downstream use
    summary = {
        "suite": suite_name,
        "run_date": execution_date.isoformat(),
        "success": success,
        "evaluated": stats["evaluated_expectations"],
        "passed": stats["successful_expectations"],
        "failed": stats["unsuccessful_expectations"],
    }
    wr.s3.to_json(
        pd.DataFrame([summary]),
        f"s3://{S3_BUCKET}/ge-results/{suite_name}/{execution_date.strftime('%Y%m%dT%H%M%S')}.json",
    )

    if not success:
        raise ValueError(f"GE validation FAILED for {suite_name} — {stats['unsuccessful_expectations']} expectations failed")


with DAG(
    dag_id="quality_checks",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["quality"],
) as dag:

    for topic, suite in SUITES:
        PythonOperator(
            task_id=f"validate_{suite}",
            python_callable=run_ge_validation,
            op_kwargs={"topic": topic, "suite_name": suite},
        )
