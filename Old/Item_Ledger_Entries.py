"""
Item_Ledger_Entries.py — Incremental load (Entry_No watermark).
OData page / SQL table : raw.Item_Ledger_Entries
PK                     : Entry_No
"""
from urllib.parse import quote
from sqlalchemy import text

from DBC_ETL.Old._base_etl import (
    get_logger,
    load_configs,
    get_session,
    get_access_token,
    build_engine,
    fetch_page_with_token_refresh,
    get_sql_columns,
    flush_via_staging,
    table_exists,
    ROWS_PER_COMMIT,
)

logger = get_logger("Item_Ledger_Entries_etl")

ODATA_PAGE = "Item_Ledger_Entries"
TABLE_NAME = "Item_Ledger_Entries"
SCHEMA = "raw"
PRIMARY_KEYS = ["Entry_No"]
WATERMARK_COL = "Entry_No"


def get_max_entry_no(engine) -> int | None:
    try:
        if not table_exists(engine, SCHEMA, TABLE_NAME):
            logger.warning(f"Table [{SCHEMA}].[{TABLE_NAME}] not found")
            return None

        with engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT MAX([{WATERMARK_COL}]) FROM [{SCHEMA}].[{TABLE_NAME}]")
            ).scalar()

        return int(result) if result is not None else None

    except Exception as e:
        logger.warning(f"Could not read watermark: {e}")
        return None


def run_etl():
    logger.info("===== START ETL: Item_Ledger_Entries (incremental) =====")

    bc_cfg, sql_cfg = load_configs()
    session = get_session()
    token = get_access_token(session, bc_cfg)
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    engine = build_engine(sql_cfg)

    max_entry_no = get_max_entry_no(engine)
    if max_entry_no is None:
        logger.info("Table empty or missing — run full load first")
        logger.info("===== END ETL =====")
        return

    logger.info(f"Watermark Entry_No = {max_entry_no}")

    company = quote(bc_cfg["company"])
    base_url = (
        f"{bc_cfg['resource']}/v2.0/{bc_cfg['tenant_id']}/{bc_cfg['environment']}"
        f"/ODataV4/Company('{company}')/{ODATA_PAGE}"
    )

    odata_url = (
        f"{base_url}"
        f"?$filter={WATERMARK_COL} gt {max_entry_no}"
        f"&$orderby={WATERMARK_COL} asc"
    )

    sql_columns = get_sql_columns(engine, SCHEMA, TABLE_NAME)

    with engine.connect() as conn:
        before = conn.execute(
            text(f"SELECT COUNT(*) FROM [{SCHEMA}].[{TABLE_NAME}]")
        ).scalar()

    logger.info(f"Rows before load: {before:,}")

    buffer = []
    total_fetched = 0
    total_inserted = 0
    last_entry_no = max_entry_no

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

        buffer.extend(records)
        total_fetched += len(records)
        last_entry_no = records[-1].get(WATERMARK_COL, last_entry_no)

        logger.info(
            f"Fetched: {total_fetched:,} rows "
            f"(last Entry_No: {last_entry_no})"
        )

        if len(buffer) >= ROWS_PER_COMMIT:
            inserted = flush_via_staging(
                engine,
                buffer,
                SCHEMA,
                TABLE_NAME,
                sql_columns,
                PRIMARY_KEYS,
                logger,
            )
            total_inserted += inserted
            logger.info(f"Flushed {inserted:,} rows (total {total_inserted:,})")
            buffer = []

    if buffer:
        inserted = flush_via_staging(
            engine,
            buffer,
            SCHEMA,
            TABLE_NAME,
            sql_columns,
            PRIMARY_KEYS,
            logger,
        )
        total_inserted += inserted

    if total_fetched == 0:
        logger.info("No new records")
        logger.info("===== END ETL =====")
        return

    with engine.connect() as conn:
        after = conn.execute(
            text(f"SELECT COUNT(*) FROM [{SCHEMA}].[{TABLE_NAME}]")
        ).scalar()

    logger.info(
        f"Rows before: {before:,} | after: {after:,} | inserted: {total_inserted:,}"
    )
    logger.info(f"Last Entry_No: {last_entry_no}")
    logger.info("===== END ETL =====")
