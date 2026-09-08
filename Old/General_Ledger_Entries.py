"""
General_Ledger_Entries.py — Incremental load (Entry_No watermark).
OData page / SQL table : raw.General_Ledger_Entries
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

logger = get_logger("General_Ledger_Entries_etl")

ODATA_PAGE = "General_Ledger_Entries"
TABLE_NAME = "General_Ledger_Entries"
SCHEMA = "raw"
PRIMARY_KEYS = ["Entry_No"]
WATERMARK_COL = "Entry_No"


def get_max_entry_no(engine) -> int | None:
    try:
        if not table_exists(engine, SCHEMA, TABLE_NAME):
            logger.warning(f"Table [{SCHEMA}].[{TABLE_NAME}] not found — skipping.")
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
    logger.info("===== START ETL: General_Ledger_Entries (incremental) =====")

    bc_cfg, sql_cfg = load_configs()
    session = get_session()
    token = get_access_token(session, bc_cfg)
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    engine = build_engine(sql_cfg)

    max_entry_no = get_max_entry_no(engine)
    if max_entry_no is None:
        logger.info("Table is empty or missing — please do a full load first.")
        logger.info("===== END ETL: General_Ledger_Entries =====")
        return

    logger.info(f"Watermark {WATERMARK_COL} = {max_entry_no} → fetching > {max_entry_no}")

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
    logger.info(f"SQL table has {len(sql_columns)} columns")

    with engine.connect() as conn:
        before_count = conn.execute(
            text(f"SELECT COUNT(*) FROM [{SCHEMA}].[{TABLE_NAME}]")
        ).scalar()
    logger.info(f"Rows before load: {before_count:,}")

    buffer: list[dict] = []
    total_fetched = 0
    total_inserted = 0
    last_entry_no_fetched = max_entry_no

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
        last_entry_no_fetched = records[-1].get(WATERMARK_COL, last_entry_no_fetched)

        logger.info(
            f"Fetched so far: {total_fetched:,} rows "
            f"(last {WATERMARK_COL}: {last_entry_no_fetched})"
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
            logger.info(
                f"Flushed batch: {inserted:,} rows "
                f"(total inserted: {total_inserted:,})"
            )
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
        logger.info(
            f"Flushed final batch: {inserted:,} rows "
            f"(total inserted: {total_inserted:,})"
        )

    if total_fetched == 0:
        logger.info("No new records since last run. Nothing to load.")
        logger.info("===== END ETL: General_Ledger_Entries =====")
        return

    with engine.connect() as conn:
        after_count = conn.execute(
            text(f"SELECT COUNT(*) FROM [{SCHEMA}].[{TABLE_NAME}]")
        ).scalar()

    logger.info(
        f"Rows before: {before_count:,} | after: {after_count:,} | inserted: {total_inserted:,}"
    )
    logger.info(f"Last {WATERMARK_COL} fetched: {last_entry_no_fetched}")
    logger.info("Incremental load completed successfully")
    logger.info("===== END ETL: General_Ledger_Entries =====")
