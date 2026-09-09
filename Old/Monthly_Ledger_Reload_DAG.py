from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from DBC_ETL.Old.Monthly_Reload_Item_Ledger_Entries import run_etl as run_item_ledger

# Runs TWICE a week: 04:00 on Sunday and Wednesday.
# - Schedule kept frequent so BC's Adjust Cost shifts (Expected -> Actual)
#   on Item_Ledger FlowFields are reflected within a few days.
# - Only Item_Ledger is reconciled here. GL reload has been removed.
# - Shares 'sql_incremental_pool' with the 15-min incremental writers so it
#   never writes to those tables concurrently.

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(hours=4),
}

with DAG(
    dag_id='Old_Monthly_Ledger_Reload',
    start_date=datetime(2026, 1, 1),
    schedule='0 4 * * 0,3',
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=['BusinessCentral', 'Incremental', 'BiweeklyReconcile'],
    default_args=default_args,
) as dag:

    item_ledger_task = PythonOperator(
        task_id='reload_Item_Ledger_Entries',
        python_callable=run_item_ledger,
        pool='sql_incremental_pool',
        priority_weight=120,
    )
