from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from DBC_ETL.bc_query_raw_etl import CONFIG_RAW, run_job


LOCAL_DIR = Path(__file__).resolve().parent
DEFAULT_DAGS_DIR = Path("/opt/airflow/dags")


def _config_path(filename: str) -> Path:
    for base in (LOCAL_DIR, DEFAULT_DAGS_DIR):
        candidate = base / filename
        if candidate.exists():
            return candidate
    return LOCAL_DIR / filename


def _enabled_services() -> list[str]:
    with _config_path(CONFIG_RAW).open(encoding="utf-8") as f:
        raw_cfg = json.load(f)
    return [
        job["service_name"]
        for job in raw_cfg.get("jobs", [])
        if job.get("enabled", True)
    ]


def _dag_id(service_name: str) -> str:
    return f"bc_query_bronze_{service_name}"


def _job_config(service_name: str) -> dict:
    with _config_path(CONFIG_RAW).open(encoding="utf-8") as f:
        raw_cfg = json.load(f)

    for job in raw_cfg.get("jobs", []):
        if job.get("service_name") == service_name:
            return job

    return {}


def _execution_timeout(service_name: str) -> timedelta:
    with _config_path(CONFIG_RAW).open(encoding="utf-8") as f:
        raw_cfg = json.load(f)

    default_hours = float(raw_cfg.get("execution_timeout_hours", 12))
    for job in raw_cfg.get("jobs", []):
        if job.get("service_name") == service_name:
            return timedelta(hours=float(job.get("execution_timeout_hours", default_hours)))

    return timedelta(hours=default_hours)


def _load_mode_tag(service_name: str) -> str:
    job = _job_config(service_name)
    load_mode = str(job.get("load_mode") or "watermark")
    return f"load:{load_mode}"


default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
}


for service_name in _enabled_services():
    with DAG(
        dag_id=_dag_id(service_name),
        start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Bangkok"),
        schedule="0 6 * * *",
        catchup=False,
        max_active_runs=1,
        max_active_tasks=1,
        tags=["business central", "Bronze", service_name, _load_mode_tag(service_name)],
        default_args=default_args,
    ) as service_dag:
        PythonOperator(
            task_id="load_bronze_table",
            python_callable=run_job,
            op_kwargs={"service_name": service_name},
            retries=2,
            retry_delay=timedelta(minutes=15),
            execution_timeout=_execution_timeout(service_name),
        )

    globals()[_dag_id(service_name)] = service_dag
