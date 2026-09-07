from argparse import Namespace
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def run_repair_from_dag(**context):
    from Repair_Missing_Entry_No import repair_missing_entries

    conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}

    args = Namespace(
        table=conf.get("table", "Item_Ledger_Entries"),
        odata=conf.get("odata"),
        pk=conf.get("pk"),
        schema=conf.get("schema", "raw"),
        from_entry=conf.get("from_entry"),
        to_entry=conf.get("to_entry"),
        max_ranges=conf.get("max_ranges", 500),
        max_gap_width=conf.get("max_gap_width", 100000),
        dry_run=conf.get("dry_run", True),
    )

    repair_missing_entries(args)


default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


with DAG(
    dag_id="Repair_Missing_Entry_No",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["BusinessCentral", "Repair", "Manual"],
    default_args=default_args,
) as dag:
    repair_task = PythonOperator(
        task_id="repair_missing_entry_no",
        python_callable=run_repair_from_dag,
        pool="sql_incremental_pool",
        priority_weight=100,
    )
