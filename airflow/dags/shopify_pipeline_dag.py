from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="shopify_data_pipeline",
    description="Shopify End-to-End AWS Glue Pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["shopify", "aws", "glue", "airflow"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    customer_etl = GlueJobOperator(
        task_id="customer_etl",
        job_name="customer_etl_job",
        aws_conn_id="aws_default",
        wait_for_completion=True,
    )

    product_etl = GlueJobOperator(
        task_id="product_etl",
        job_name="product_etl_job",
        aws_conn_id="aws_default",
        wait_for_completion=True,
    )

    orders_etl = GlueJobOperator(
        task_id="orders_etl",
        job_name="Orders_etl_job",
        aws_conn_id="aws_default",
        wait_for_completion=True,
    )

    payments_etl = GlueJobOperator(
        task_id="payments_etl",
        job_name="payments_etl_job",
        aws_conn_id="aws_default",
        wait_for_completion=True,
    )

    inventory_etl = GlueJobOperator(
        task_id="inventory_etl",
        job_name="Inventory_etl_job",
        aws_conn_id="aws_default",
        wait_for_completion=True,
    )

    dim_customer = GlueJobOperator(
        task_id="gold_dim_customer",
        job_name="gold_dim_customer",
        aws_conn_id="aws_default",
        wait_for_completion=True,
    )

    dim_product = GlueJobOperator(
        task_id="gold_dim_product",
        job_name="gold_dim_product",
        aws_conn_id="aws_default",
        wait_for_completion=True,
    )

    fact_orders = GlueJobOperator(
        task_id="gold_fact_orders",
        job_name="gold_fact_orders",
        aws_conn_id="aws_default",
        wait_for_completion=True,
    )

    fact_payments = GlueJobOperator(
        task_id="gold_fact_payments",
        job_name="gold_fact_payments",
        aws_conn_id="aws_default",
        wait_for_completion=True,
    )

    inventory_summary = GlueJobOperator(
        task_id="gold_inventory_summary",
        job_name="gold_inventory_summary",
        aws_conn_id="aws_default",
        wait_for_completion=True,
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> customer_etl
    customer_etl >> product_etl
    product_etl >> orders_etl
    orders_etl >> payments_etl
    payments_etl >> inventory_etl
    inventory_etl >> dim_customer
    dim_customer >> dim_product
    dim_product >> fact_orders
    fact_orders >> fact_payments
    fact_payments >> inventory_summary
    inventory_summary >> end