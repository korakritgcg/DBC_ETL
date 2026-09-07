"""
Purchase_Order_List.py — Full Refresh (staged batches → atomic swap).
OData page / SQL table : raw.Purchase_Order_List
PK                     : Document_Type, No
Strategy:
  1) Stream OData into a uniquely-named staging table in batches of ROWS_PER_COMMIT
  2) In one transaction: TRUNCATE target + INSERT FROM staging
  3) Drop staging in finally (cleanup on success or failure)
"""
import uuid
from urllib.parse import quote

import pandas as pd
from sqlalchemy import text
from sqlalchemy.types import NVARCHAR

from _base_etl import (
    get_logger, load_configs, get_session, get_access_token,
    build_engine, fetch_page_with_token_refresh,
    get_sql_columns, table_exists, ROWS_PER_COMMIT,
)

logger = get_logger("Purchase_Order_List_etl")

ODATA_PAGE   = "Purchase_Order_List"
TABLE_NAME   = "Purchase_Order_List"
SCHEMA       = "raw"
PRIMARY_KEYS = ['Document_Type', 'No']


def _append_batch_to_staging(engine, records, sql_columns, staging):
    df = pd.DataFrame(records)
    df = df.loc[:, ~df.columns.astype(str).str.startswith("@")]

    valid_cols = [c for c in df.columns if c in sql_columns]
    dropped    = [c for c in df.columns if c not in sql_columns]
    if dropped:
        logger.warning(f"Dropping {len(dropped)} cols not in SQL: {dropped}")
    df = df[valid_cols]

    if PRIMARY_KEYS:
        df = df.drop_duplicates(subset=PRIMARY_KEYS, keep="last")

    if df.empty:
        return 0

    dtype_map = {
        c: NVARCHAR(4000)
        for c in df.select_dtypes(include=["object"]).columns
    }

    with engine.begin() as conn:
        df.to_sql(
            staging, conn, schema=SCHEMA,
            if_exists="append", index=False,
            chunksize=1000, dtype=dtype_map,
        )
    return len(df)


def run_etl():
    logger.info("===== START ETL: Purchase_Order_List (Full Refresh / staged swap) =====")
    bc_cfg, sql_cfg = load_configs()
    session = get_session()
    token   = get_access_token(session, bc_cfg)
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    engine  = build_engine(sql_cfg)

    if not table_exists(engine, SCHEMA, TABLE_NAME):
        raise RuntimeError(
            f"Target table [{SCHEMA}].[{TABLE_NAME}] not found — "
            f"generate it via the ETL generator app first."
        )

    sql_columns = get_sql_columns(engine, SCHEMA, TABLE_NAME)
    if not sql_columns:
        raise RuntimeError(f"No columns found for [{SCHEMA}].[{TABLE_NAME}]")

    company   = quote(bc_cfg["company"])
    odata_url = (
        f"{bc_cfg['resource']}/v2.0/{bc_cfg['tenant_id']}/{bc_cfg['environment']}"
        f"/ODataV4/Company('{company}')/Purchase_Order_List"
    )

    staging = f"stg_{TABLE_NAME}_{uuid.uuid4().hex[:8]}"

    # Empty clone of target — same columns/types, no rows
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS [{SCHEMA}].[{staging}]"))
        conn.execute(text(
            f"SELECT TOP 0 * INTO [{SCHEMA}].[{staging}] FROM [{SCHEMA}].[{TABLE_NAME}]"
        ))

    buffer        = []
    total_fetched = 0
    total_staged  = 0

    try:
        while odata_url:
            records, next_link, headers = fetch_page_with_token_refresh(
                session, odata_url, headers, bc_cfg, logger,
            )
            buffer.extend(records)
            total_fetched += len(records)
            logger.info(f"Fetched so far: {total_fetched:,} rows")

            if len(buffer) >= ROWS_PER_COMMIT:
                staged = _append_batch_to_staging(engine, buffer, sql_columns, staging)
                total_staged += staged
                logger.info(f"Staged batch: {staged:,} (total staged: {total_staged:,})")
                buffer = []

            odata_url = next_link

        if buffer:
            staged = _append_batch_to_staging(engine, buffer, sql_columns, staging)
            total_staged += staged
            logger.info(f"Staged final batch: {staged:,} (total staged: {total_staged:,})")
            buffer = []

        if total_fetched == 0:
            logger.info("No data fetched — keeping existing target untouched.")
            logger.info("===== END ETL: Purchase_Order_List =====")
            return

        # Atomic swap inside one transaction
        col_list = ", ".join(f"[{c}]" for c in sql_columns)
        with engine.begin() as conn:
            before = conn.execute(
                text(f"SELECT COUNT(*) FROM [{SCHEMA}].[{TABLE_NAME}]")
            ).scalar()
            conn.execute(text(f"TRUNCATE TABLE [{SCHEMA}].[{TABLE_NAME}]"))
            result = conn.execute(text(
                f"INSERT INTO [{SCHEMA}].[{TABLE_NAME}] ({col_list}) "
                f"SELECT {col_list} FROM [{SCHEMA}].[{staging}]"
            ))
            inserted = result.rowcount

        logger.info(
            f"Swap done — fetched: {total_fetched:,} | staged: {total_staged:,} | "
            f"before: {before:,} | inserted: {inserted:,}"
        )

    finally:
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS [{SCHEMA}].[{staging}]"))

    logger.info("===== END ETL: Purchase_Order_List =====")
