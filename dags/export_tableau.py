"""
Exports Athena query results to CSV in S3 for Tableau Public ingestion.
Run manually or on a schedule before publishing a Tableau workbook.
"""
import os
from datetime import datetime, timedelta

import awswrangler as wr
from airflow import DAG
from airflow.operators.python import PythonOperator

S3_BUCKET = os.getenv("S3_BUCKET", "ecommerce-streaming-raw")
ATHENA_DATABASE = os.getenv("ATHENA_DATABASE", "ecommerce")
ATHENA_OUTPUT = os.getenv("ATHENA_OUTPUT_LOCATION", f"s3://{S3_BUCKET}/athena-results/")
EXPORT_PREFIX = f"s3://{S3_BUCKET}/tableau-exports"

default_args = {
    "owner": "pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

EXPORTS = [
    (
        "order_volume_daily",
        """
        SELECT
            SUBSTR(timestamp, 1, 10) AS order_date,
            category,
            COUNT(*)                 AS order_count,
            SUM(price * quantity)    AS revenue,
            AVG(price)               AS avg_price
        FROM ecommerce.orders_placed
        GROUP BY 1, 2
        ORDER BY 1 DESC
        """,
    ),
    (
        "product_performance",
        """
        SELECT
            product_id,
            category,
            COUNT(*)              AS orders,
            SUM(quantity)         AS units_sold,
            SUM(price * quantity) AS revenue,
            AVG(price)            AS avg_price
        FROM ecommerce.orders_placed
        GROUP BY 1, 2
        ORDER BY revenue DESC
        LIMIT 200
        """,
    ),
    (
        "customer_behavior",
        """
        SELECT
            o.customer_id,
            COUNT(DISTINCT o.order_id)  AS total_orders,
            SUM(o.price * o.quantity)   AS lifetime_value,
            COUNT(DISTINCT p.session_id) AS sessions,
            COUNT(p.session_id)          AS page_views
        FROM ecommerce.orders_placed o
        LEFT JOIN ecommerce.page_views p ON o.customer_id = p.customer_id
        GROUP BY 1
        ORDER BY lifetime_value DESC
        LIMIT 500
        """,
    ),
    (
        "device_breakdown",
        """
        SELECT
            device_type,
            COUNT(*) AS view_count,
            COUNT(DISTINCT session_id) AS sessions,
            COUNT(DISTINCT customer_id) AS unique_customers
        FROM ecommerce.page_views
        GROUP BY device_type
        """,
    ),
    (
        "carrier_performance",
        """
        SELECT
            carrier,
            COUNT(*) AS shipments,
            COUNT(DISTINCT order_id) AS unique_orders
        FROM ecommerce.orders_shipped
        GROUP BY carrier
        ORDER BY shipments DESC
        """,
    ),
]


def export_query(export_name: str, sql: str, **context):
    run_date = context["execution_date"].strftime("%Y%m%d")
    output_path = f"{EXPORT_PREFIX}/{export_name}/{run_date}.csv"

    df = wr.athena.read_sql_query(
        sql=sql,
        database=ATHENA_DATABASE,
        s3_output=ATHENA_OUTPUT,
    )

    wr.s3.to_csv(df, output_path, index=False)
    print(f"[OK] Exported {len(df)} rows → {output_path}")


with DAG(
    dag_id="export_tableau",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["tableau", "export"],
) as dag:

    for name, query in EXPORTS:
        PythonOperator(
            task_id=f"export_{name}",
            python_callable=export_query,
            op_kwargs={"export_name": name, "sql": query},
        )
