"""Airflow DAG for loading Supermetrics TikTok Ads data to SQL Server."""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

from airflow.sdk import dag, task


DAG_FILE = Path(__file__).resolve()
DAG_DIR = Path(os.environ.get("SUPERMETRICS_DAG_DIR", DAG_FILE.parent))
if str(DAG_DIR) not in sys.path:
    sys.path.insert(0, str(DAG_DIR))


@dag(
    dag_id="supermetric_tiktok_ad",
    description="Load Supermetrics TikTok Ads data into bronze.supermetrics_tiktok_ad.",
    start_date=datetime(2026, 9, 14),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["supermetrics", "tiktok", "ads", "bronze"],
)
def supermetric_tiktok_ad():
    @task(task_id="load_supermetric_tiktok_ad", retries=2)
    def load_supermetric_tiktok_ad() -> None:
        from supermetric_tiktok_ad import run

        exit_code = run()
        if exit_code != 0:
            raise RuntimeError(
                f"Supermetrics TikTok Ads ingestion failed with exit code {exit_code}"
            )

    load_supermetric_tiktok_ad()


supermetric_tiktok_ad()
