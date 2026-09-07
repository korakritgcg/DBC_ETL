"""
DBC_to_Slip.py — Full refresh ETL: SQL Server (DBC_PRD) -> PostgreSQL (slip).

Runs a custom SQL query against DBC_PRD and replaces the target table in the
`slips_online` PostgreSQL database (TRUNCATE once + batched APPEND, so the
table definition / indexes survive each run).

>>> EDIT THESE TWO BEFORE FIRST RUN <<<
  - SOURCE_QUERY : the SELECT you want to pull from DBC_PRD
  - TARGET_TABLE : destination table name in PostgreSQL (slip)
"""
import json
import urllib.parse

import pandas as pd
from sqlalchemy import create_engine, text

from _base_etl import get_logger

logger = get_logger("dbc_to_slip_etl")

# ---------------------------------------------------------------------------
# CONFIG — edit these for your data.
# ---------------------------------------------------------------------------
SOURCE_QUERY = """
    SELECT *
    FROM raw.Item_Ledger_Entries
    -- WHERE Posting_Date >= DATEADD(DAY, -7, GETDATE())
"""

TARGET_SCHEMA = "public"          # PostgreSQL schema in slips_online
TARGET_TABLE = "item_ledger_entries"

ROWS_PER_COMMIT = 20000           # batch size for read + write
SQL_CONFIG = "/opt/airflow/dags/_config_sql.json"      # DBC_PRD (SQL Server)
SLIP_CONFIG = "/opt/airflow/dags/_config_slip.json"    # slips_online (PostgreSQL)


def _load(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def build_mssql_engine(cfg: dict):
    """Source engine — DBC_PRD on SQL Server."""
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={cfg['server']},{cfg['port']};"
        f"DATABASE={cfg['database']};"
        f"UID={cfg['username']};"
        f"PWD={cfg['password']};"
        "TrustServerCertificate=yes;"
        "Unicode_Results=Yes;"
    )
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        pool_pre_ping=True,
    )


def build_pg_engine(cfg: dict):
    """Destination engine — slips_online on PostgreSQL (psycopg2)."""
    pwd = urllib.parse.quote_plus(str(cfg["password"]))
    user = urllib.parse.quote_plus(str(cfg["username"]))
    return create_engine(
        f"postgresql+psycopg2://{user}:{pwd}"
        f"@{cfg['server']}:{cfg['port']}/{cfg['database']}",
        pool_pre_ping=True,
    )


def _pg_table_exists(conn, schema: str, table: str) -> bool:
    return conn.execute(
        text(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = :schema
              AND table_name = :table
            """
        ),
        {"schema": schema, "table": table},
    ).scalar() is not None


def run_etl():
    logger.info("===== START ETL: DBC_PRD -> slip (full refresh) =====")

    src_engine = build_mssql_engine(_load(SQL_CONFIG))
    dst_engine = build_pg_engine(_load(SLIP_CONFIG))

    total_rows = 0
    table_prepared = False

    # Stream the source query in chunks so large results don't blow memory.
    with src_engine.connect().execution_options(stream_results=True) as src_conn:
        chunks = pd.read_sql(text(SOURCE_QUERY), src_conn, chunksize=ROWS_PER_COMMIT)

        for chunk in chunks:
            if chunk.empty:
                continue

            # Normalise column names to lowercase for Postgres convenience.
            chunk.columns = [str(c).lower() for c in chunk.columns]

            with dst_engine.begin() as dst_conn:
                if not table_prepared:
                    if _pg_table_exists(dst_conn, TARGET_SCHEMA, TARGET_TABLE):
                        before = dst_conn.execute(
                            text(f'SELECT COUNT(*) FROM "{TARGET_SCHEMA}"."{TARGET_TABLE}"')
                        ).scalar()
                        logger.info(f"Rows before load: {before:,}")
                        dst_conn.execute(
                            text(f'TRUNCATE TABLE "{TARGET_SCHEMA}"."{TARGET_TABLE}"')
                        )
                        logger.info(f"Truncated: {TARGET_SCHEMA}.{TARGET_TABLE}")
                        if_exists = "append"
                    else:
                        logger.info(
                            f"Table {TARGET_SCHEMA}.{TARGET_TABLE} not found "
                            f"— creating on first chunk."
                        )
                        if_exists = "replace"
                    table_prepared = True
                else:
                    if_exists = "append"

                chunk.to_sql(
                    TARGET_TABLE,
                    dst_conn,
                    schema=TARGET_SCHEMA,
                    if_exists=if_exists,
                    index=False,
                    method="multi",
                    chunksize=1000,
                )

            total_rows += len(chunk)
            logger.info(f"Loaded so far: {total_rows:,} rows")

    if not table_prepared:
        logger.info("Source returned 0 rows — target left untouched to protect data.")
        logger.info("===== END ETL: DBC_PRD -> slip =====")
        return

    with dst_engine.connect() as dst_conn:
        after = dst_conn.execute(
            text(f'SELECT COUNT(*) FROM "{TARGET_SCHEMA}"."{TARGET_TABLE}"')
        ).scalar()

    logger.info(f"Rows after load: {after:,}")
    logger.info(f"Total transferred: {total_rows:,} rows")
    logger.info("===== END ETL: DBC_PRD -> slip =====")


if __name__ == "__main__":
    run_etl()
