from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from DBC_ETL.Purchase_Orders import run_etl

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="Purchase_Orders",
    start_date=datetime(2026, 1, 1),
    schedule="30 0 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["BusinessCentral", "Full Refresh"],
    default_args=default_args,
) as dag:

    run_task = PythonOperator(
        task_id="run_Purchase_Orders_etl",
        python_callable=run_etl,
        retries=3,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=3),
    )
