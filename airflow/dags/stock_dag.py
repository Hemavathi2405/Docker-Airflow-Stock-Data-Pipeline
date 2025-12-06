from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from fetch_and_store import fetch_and_store_stock_data

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="stock_market_pipeline",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    description="Fetch stock market data and store in PostgreSQL",
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_and_store_stock_data",
        python_callable=fetch_and_store_stock_data,
    )
