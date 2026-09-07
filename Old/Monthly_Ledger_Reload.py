"""
Monthly_Ledger_Reload.py — Monthly rolling-window reconciliation.

Incremental ledger loads only ever append rows with a higher Entry_No, so
retroactive changes BC makes to already-loaded rows are never picked up.
Once a month this re-pulls the previous + current calendar month and
replaces that window atomically.

Window per table = Entry_No >= MIN(Entry_No) of rows whose Posting_Date is
on/after the first day of the previous calendar month.
"""
from _base_etl import (
    get_logger,
    load_configs,
    get_session,
    get_access_token,
    build_engine,
    reload_recent_window,
)

logger = get_logger("Monthly_Ledger_Reload_etl")

SCHEMA = "raw"

TARGETS = [
    {
        "table": "General_Ledger_Entries",
        "odata_page": "General_Ledger_Entries",
        "watermark_col": "Entry_No",
        "pk_cols": ["Entry_No"],
        "date_col": "Posting_Date",
    },
    {
        "table": "Item_Ledger_Entries",
        "odata_page": "Item_Ledger_Entries",
        "watermark_col": "Entry_No",
        "pk_cols": ["Entry_No"],
        "date_col": "Posting_Date",
    },
]


def run_etl():
    logger.info("===== START ETL: Monthly_Ledger_Reload =====")

    bc_cfg, sql_cfg = load_configs()
    session = get_session()
    token = get_access_token(session, bc_cfg)
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    engine = build_engine(sql_cfg)

    for t in TARGETS:
        logger.info(f"--- Reconciling [{SCHEMA}].[{t['table']}] ---")
        reload_recent_window(
            engine,
            session,
            headers,
            bc_cfg,
            SCHEMA,
            t["table"],
            t["odata_page"],
            t["watermark_col"],
            t["pk_cols"],
            t["date_col"],
            logger,
        )

    logger.info("===== END ETL: Monthly_Ledger_Reload =====")
