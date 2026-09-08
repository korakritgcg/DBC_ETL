"""
Item_Units_of_Measure_Excel.py — Full Refresh (TRUNCATE once + batched APPEND).
OData page / SQL table : raw.Item_Units_of_Measure_Excel
PK                     : Item_No + Code
"""
from urllib.parse import quote

from DBC_ETL._base_etl import flush_fullload_append

import pandas as pd
from sqlalchemy import text
from sqlalchemy.types import NVARCHAR

from DBC_ETL._base_etl import (
    get_logger,
    load_configs,
    get_session,
    get_access_token,
    build_engine,
    fetch_page_with_token_refresh,
    table_exists,
)

logger = get_logger("Item_Units_of_Measure_Excel_etl")

ODATA_PAGE = "Item_Units_of_Measure_Excel"
TABLE_NAME = "Item_Units_of_Measure_Excel"
SCHEMA = "raw"
PRIMARY_KEYS = ["Item_No", "Code"]
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
    logger.info("===== START ETL: Item_Units_of_Measure_Excel (full refresh / batched) =====")

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
        logger.info("===== END ETL: Item_Units_of_Measure_Excel =====")
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
    logger.info("===== END ETL: Item_Units_of_Measure_Excel =====")
