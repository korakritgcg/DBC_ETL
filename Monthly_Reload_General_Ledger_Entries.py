"""
Monthly_Reload_General_Ledger_Entries.py — Monthly rolling-window reconciliation.

Window = Entry_No >= MIN(Entry_No) of rows whose Posting_Date is on/after the
first day of the previous calendar month. That window is re-fetched from BC
and replaced atomically (stage -> DELETE window + INSERT from stage).
"""
from DBC_ETL._base_etl import (
    get_logger,
    load_configs,
    get_session,
    get_access_token,
    build_engine,
    get_sql_columns,
    reload_recent_window,
)
from DBC_ETL._entry_no_auto_repair import repair_existing_entry_no_gaps

logger = get_logger("Monthly_Reload_General_Ledger_Entries_etl")

SCHEMA = "raw"
TABLE_NAME = "General_Ledger_Entries"
ODATA_PAGE = "General_Ledger_Entries"
WATERMARK_COL = "Entry_No"
PRIMARY_KEYS = ["Entry_No"]
DATE_COL = "Posting_Date"
AUTO_REPAIR_MISSING_ENTRY_NO = True
AUTO_REPAIR_MAX_RANGES = 100
AUTO_REPAIR_MAX_GAP_WIDTH = 50000


def run_etl():
    logger.info(f"===== START ETL: Monthly_Reload [{SCHEMA}].[{TABLE_NAME}] =====")

    bc_cfg, sql_cfg = load_configs()
    session = get_session()
    token = get_access_token(session, bc_cfg)
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    engine = build_engine(sql_cfg)

    reload_recent_window(
        engine,
        session,
        headers,
        bc_cfg,
        SCHEMA,
        TABLE_NAME,
        ODATA_PAGE,
        WATERMARK_COL,
        PRIMARY_KEYS,
        DATE_COL,
        logger,
    )

    if AUTO_REPAIR_MISSING_ENTRY_NO:
        sql_columns = get_sql_columns(engine, SCHEMA, TABLE_NAME)
        repair_fetched, repair_inserted, headers = repair_existing_entry_no_gaps(
            engine,
            session,
            headers,
            bc_cfg,
            SCHEMA,
            TABLE_NAME,
            ODATA_PAGE,
            WATERMARK_COL,
            sql_columns,
            PRIMARY_KEYS,
            logger,
            AUTO_REPAIR_MAX_RANGES,
            AUTO_REPAIR_MAX_GAP_WIDTH,
        )
        logger.info(
            f"Auto repair fetched: {repair_fetched:,} | inserted: {repair_inserted:,}"
        )

    logger.info(f"===== END ETL: Monthly_Reload [{SCHEMA}].[{TABLE_NAME}] =====")
