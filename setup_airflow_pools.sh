#!/usr/bin/env bash
set -euo pipefail

# Run this from ~/airflow after copying the optimized DAG files.
# It creates pools used by the generated DAGs.

docker-compose exec airflow-webserver airflow pools set bc_realtime_pool 1 "Near realtime incremental ETL. One SQL writer at a time to avoid deadlocks."
docker-compose exec airflow-webserver airflow pools set bc_incremental_pool 1 "Reserved incremental pool."
docker-compose exec airflow-webserver airflow pools set bc_full_heavy_pool 1 "Heavy full refresh ETL. One job at a time."
docker-compose exec airflow-webserver airflow pools set bc_full_medium_pool 1 "Medium full refresh ETL. One job at a time."
docker-compose exec airflow-webserver airflow pools set bc_full_light_pool 2 "Light/master data refresh ETL. Up to two concurrent jobs."

docker-compose restart airflow-scheduler airflow-worker airflow-webserver
