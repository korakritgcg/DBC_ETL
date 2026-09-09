from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from DBC_ETL.Old.Item_Card import run_etl

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
    dag_id='Old_Item_Card',
    start_date=datetime(2026, 1, 1),
    schedule='0 8 * * *',
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=['BusinessCentral', 'FullRefresh', 'Master'],
    default_args=default_args,
) as dag:

    run_task = PythonOperator(
        task_id='run_Item_Card_etl',
        python_callable=run_etl,
        retries=2,
        retry_delay=timedelta(minutes=10),
        execution_timeout=timedelta(hours=1),
        pool='sql_light_pool',
        priority_weight=35,
    )
