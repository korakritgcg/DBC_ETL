from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from DBC_ETL.Old.Purchase_Order_Line_Excel import run_etl

# Optimized schedule generated for SQL-load control.
# - catchup=False prevents backlog storms.
# - max_active_runs=1 prevents duplicate overlapping DAG runs.
# - max_active_tasks=1 keeps this DAG single-threaded.
# - pool controls cross-DAG database pressure.

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id='Purchase_Order_Line_Excel',
    start_date=datetime(2026, 1, 1),
    schedule='30 3 * * *',
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=['BusinessCentral', 'FullRefresh', 'Heavy'],
    default_args=default_args,
) as dag:

    run_task = PythonOperator(
        task_id='run_Purchase_Order_Line_Excel_etl',
        python_callable=run_etl,
        retries=2,
        retry_delay=timedelta(minutes=15),
        execution_timeout=timedelta(hours=3),
        pool='sql_heavy_pool',
        priority_weight=55,
    )
