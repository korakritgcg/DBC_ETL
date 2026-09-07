"""
Posted_Sales_Invoice_Excel.py — Full load (TRUNCATE once + batched APPEND).
OData page / SQL table : raw.Posted_Sales_Invoice_Excel
PK                     : No
Note: ข้อมูลเยอะ ใช้ fetch_page_with_token_refresh เพื่อ handle token หมดอายุ
"""
from urllib.parse import quote

from _base_etl import flush_fullload_append

import pandas as pd
from sqlalchemy import text
from sqlalchemy.types import NVARCHAR

from _base_etl import (
    get_logger,
    load_configs,
    get_session,
    get_access_token,
    build_engine,
    fetch_page_with_token_refresh,
    table_exists,
)

logger = get_logger("Posted_Sales_Invoice_Excel_etl")

ODATA_PAGE = "Posted_Sales_Invoice_Excel"
TABLE_NAME = "Posted_Sales_Invoice_Excel"
SCHEMA = "raw"
PRIMARY_KEYS = ["No"]
ROWS_PER_COMMIT = 20000


def _append_batch(engine, records: list[dict]) -> int:
    return flush_fullload_append(
        engine,
        records,
        SCHEMA,
        TABLE_NAME,
        PRIMARY_KEYS,
        ODATA_PAGE,
        logger,
    )


def run_etl():
    logger.info("===== START ETL: Posted_Sales_Invoice_Excel (full load / batched) =====")

    bc_cfg, sql_cfg = load_configs()
    session = get_session()
    token = get_access_token(session, bc_cfg)
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    engine = build_engine(sql_cfg)

    company = quote(bc_cfg["company"])
    odata_url = (
        f"{bc_cfg['resource']}/v2.0/{bc_cfg['tenant_id']}/{bc_cfg['environment']}"
        f"/ODataV4/Company('{company}')/{ODATA_PAGE}"
    )

    total_fetched = 0
    total_inserted = 0
    buffer: list[dict] = []
    table_truncated = False

    while odata_url:
        records, next_link, headers = fetch_page_with_token_refresh(
            session,
            odata_url,
            headers,
            bc_cfg,
            logger,
        )
        odata_url = next_link

        if not records:
            continue

        if not table_truncated:
            with engine.begin() as conn:
                if table_exists(engine, SCHEMA, TABLE_NAME):
                    before_count = conn.execute(
                        text(f"SELECT COUNT(*) FROM [{SCHEMA}].[{TABLE_NAME}]")
                    ).scalar()
                    logger.info(f"Rows before load: {before_count:,}")

                    conn.execute(text(f"TRUNCATE TABLE [{SCHEMA}].[{TABLE_NAME}]"))
                    logger.info(f"Table truncated: [{SCHEMA}].[{TABLE_NAME}]")
                else:
                    logger.info(
                        f"Table [{SCHEMA}].[{TABLE_NAME}] not found — will create on first append."
                    )

            table_truncated = True

        buffer.extend(records)
        total_fetched += len(records)
        logger.info(f"Fetched so far: {total_fetched:,} rows")

        if len(buffer) >= ROWS_PER_COMMIT:
            inserted = _append_batch(engine, buffer)
            total_inserted += inserted
            logger.info(
                f"Inserted batch: {inserted:,} rows "
                f"(total inserted: {total_inserted:,})"
            )
            buffer = []

    if not table_truncated:
        logger.info("No data returned. Skipping truncate to protect existing data.")
        logger.info("===== END ETL: Posted_Sales_Invoice_Excel =====")
        return

    if buffer:
        inserted = _append_batch(engine, buffer)
        total_inserted += inserted
        logger.info(
            f"Inserted final batch: {inserted:,} rows "
            f"(total inserted: {total_inserted:,})"
        )

    with engine.connect() as conn:
        after_count = conn.execute(
            text(f"SELECT COUNT(*) FROM [{SCHEMA}].[{TABLE_NAME}]")
        ).scalar()

    logger.info(f"Rows after load: {after_count:,}")
    logger.info(f"Total fetched from BC: {total_fetched:,} rows")
    logger.info(f"Total inserted into SQL: {total_inserted:,} rows")
    logger.info("===== END ETL: Posted_Sales_Invoice_Excel =====")