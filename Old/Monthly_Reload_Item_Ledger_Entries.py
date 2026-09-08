"""
Monthly_Reload_Item_Ledger_Entries.py — Monthly rolling-window reconciliation.

Window = Entry_No >= MIN(Entry_No) of rows whose Posting_Date is on/after the
first day of the previous calendar month. That window is re-fetched from BC
and replaced atomically (stage -> DELETE window + INSERT from stage).
"""
from DBC_ETL.Old._base_etl import (
    get_logger,
    load_configs,
    get_session,
    get_access_token,
    build_engine,
    reload_recent_window,
)

logger = get_logger("Monthly_Reload_Item_Ledger_Entries_etl")

SCHEMA = "raw"
TABLE_NAME = "Item_Ledger_Entries"
ODATA_PAGE = "Item_Ledger_Entries"
WATERMARK_COL = "Entry_No"
PRIMARY_KEYS = ["Entry_No"]
DATE_COL = "Posting_Date"


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

    logger.info(f"===== END ETL: Monthly_Reload [{SCHEMA}].[{TABLE_NAME}] =====")
