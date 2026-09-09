from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from DBC_ETL.Old.DBC_to_Slip import run_etl

# Full-refresh sync: SQL Server (DBC_PRD) -> PostgreSQL (slip), hourly.
# - catchup=False prevents backlog storms.
# - max_active_runs=1 prevents duplicate overlapping DAG runs.
# - max_active_tasks=1 keeps this DAG single-threaded.

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id='Old_DBC_to_Slip',
    start_date=datetime(2026, 1, 1),
    schedule='10 * * * *',          # every hour at minute 10
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=['DBC_PRD', 'Slip', 'FullRefresh', 'Postgres'],
    default_args=default_args,
) as dag:

    run_task = PythonOperator(
        task_id='run_DBC_to_Slip_etl',
        python_callable=run_etl,
        retries=2,
        retry_delay=timedelta(minutes=10),
        execution_timeout=timedelta(hours=1),
        pool='sql_light_pool',
        priority_weight=35,
    )
