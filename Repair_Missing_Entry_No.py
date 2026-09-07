"""
Repair_Missing_Entry_No.py - find missing Entry_No values in SQL and fetch
those rows back from Business Central OData.

Examples:
  python Repair_Missing_Entry_No.py --table Item_Ledger_Entries --dry-run
  python Repair_Missing_Entry_No.py --table General_Ledger_Entries --from-entry 1000 --to-entry 2000
  python Repair_Missing_Entry_No.py --table Warehouse_Entries_Excel --pk Entry_No Line_No
"""
import argparse
import json
import re
import urllib.parse
from pathlib import Path
from urllib.parse import quote

from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.exc import InterfaceError, OperationalError

from _base_etl import (
    ROWS_PER_COMMIT,
    build_engine,
    fetch_page_with_token_refresh,
    flush_via_staging,
    get_access_token,
    get_logger,
    get_session,
    get_sql_columns,
    load_configs,
    table_exists,
)


logger = get_logger("Repair_Missing_Entry_No")

SCHEMA = "raw"
ENTRY_COL = "Entry_No"
SQL_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
ODBC_DRIVER_PREFERENCE = [
    "ODBC Driver 18 for SQL Server",
    "ODBC Driver 17 for SQL Server",
    "SQL Server Native Client 11.0",
    "SQL Server",
]

TABLE_DEFAULTS = {
    "Change_Log_Entries": {"odata": "Change_Log_Entries", "pk": ["Entry_No"]},
    "General_Ledger_Entries": {"odata": "General_Ledger_Entries", "pk": ["Entry_No"]},
    "Item_Ledger_Entries": {"odata": "Item_Ledger_Entries", "pk": ["Entry_No"]},
    "Warehouse_Entries_Excel": {
        "odata": "Warehouse_Entries_Excel",
        "pk": ["Entry_No", "Line_No"],
    },
}


def load_runtime_configs() -> tuple[dict, dict]:
    try:
        return load_configs()
    except FileNotFoundError:
        script_dir = Path(__file__).resolve().parent

        with (script_dir / "_config_DBC.json").open(encoding="utf-8") as f:
            bc = json.load(f)

        with (script_dir / "_config_sql.json").open(encoding="utf-8") as f:
            sql = json.load(f)

        return bc, sql


def build_runtime_engine(sql_cfg: dict):
    import pyodbc

    installed_drivers = set(pyodbc.drivers())
    driver = next(
        (name for name in ODBC_DRIVER_PREFERENCE if name in installed_drivers),
        None,
    )

    if driver is None:
        return build_engine(sql_cfg)

    logger.info(f"Using SQL ODBC driver: {driver}")
    params = urllib.parse.quote_plus(
        f"DRIVER={{{driver}}};"
        f"SERVER={sql_cfg['server']},{sql_cfg['port']};"
        f"DATABASE={sql_cfg['database']};"
        f"UID={sql_cfg['username']};"
        f"PWD={sql_cfg['password']};"
        "Encrypt=no;"
        "TrustServerCertificate=yes;"
        "Unicode_Results=Yes;"
    )

    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        fast_executemany=True,
        pool_pre_ping=True,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Find missing Entry_No ranges in SQL and reload them from BC OData."
    )
    parser.add_argument(
        "--table",
        required=True,
        help="SQL table name under raw schema, for example Item_Ledger_Entries.",
    )
    parser.add_argument(
        "--odata",
        help="BC OData page name. Defaults to --table or known table mapping.",
    )
    parser.add_argument(
        "--pk",
        nargs="+",
        help="Primary key columns for upsert. Defaults to known table mapping or Entry_No.",
    )
    parser.add_argument(
        "--schema",
        default=SCHEMA,
        help="SQL schema name. Default: raw.",
    )
    parser.add_argument(
        "--from-entry",
        type=int,
        help="Only check gaps at or after this Entry_No.",
    )
    parser.add_argument(
        "--to-entry",
        type=int,
        help="Only check gaps at or before this Entry_No.",
    )
    parser.add_argument(
        "--max-ranges",
        type=int,
        default=500,
        help="Maximum gap ranges to repair in one run. Default: 500.",
    )
    parser.add_argument(
        "--max-gap-width",
        type=int,
        default=100000,
        help="Skip any single gap wider than this many Entry_No values. Default: 100000.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print missing ranges; do not fetch or insert.",
    )
    return parser.parse_args()


def resolve_table_config(args: argparse.Namespace) -> tuple[str, list[str]]:
    defaults = TABLE_DEFAULTS.get(args.table, {})
    odata_page = args.odata or defaults.get("odata") or args.table
    pk_cols = args.pk or defaults.get("pk") or [ENTRY_COL]
    return odata_page, pk_cols


def validate_sql_identifier(value: str, label: str) -> None:
    if not SQL_IDENTIFIER_RE.match(value):
        raise ValueError(f"Invalid {label}: {value!r}")


def get_entry_bounds(engine, schema: str, table: str) -> tuple[int | None, int | None]:
    with engine.connect() as conn:
        row = conn.execute(
            text(
                f"""
                SELECT
                    MIN(TRY_CONVERT(bigint, [{ENTRY_COL}])) AS min_entry,
                    MAX(TRY_CONVERT(bigint, [{ENTRY_COL}])) AS max_entry
                FROM [{schema}].[{table}]
                WHERE TRY_CONVERT(bigint, [{ENTRY_COL}]) IS NOT NULL
                """
            )
        ).one()

    min_entry, max_entry = row
    return (
        int(min_entry) if min_entry is not None else None,
        int(max_entry) if max_entry is not None else None,
    )


def find_missing_ranges(
    engine,
    schema: str,
    table: str,
    from_entry: int | None,
    to_entry: int | None,
    max_ranges: int,
) -> list[tuple[int, int, int]]:
    min_entry, max_entry = get_entry_bounds(engine, schema, table)
    if min_entry is None or max_entry is None:
        return []

    lower_bound = from_entry if from_entry is not None else min_entry
    upper_bound = to_entry if to_entry is not None else max_entry
    if lower_bound > upper_bound:
        return []

    max_ranges = max(1, int(max_ranges))
    params = {
        "lower_sentinel": lower_bound - 1,
        "upper_sentinel": upper_bound + 1,
        "lower_bound": lower_bound,
        "upper_bound": upper_bound,
    }

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f"""
                WITH entries AS (
                    SELECT DISTINCT TRY_CONVERT(bigint, [{ENTRY_COL}]) AS entry_no
                    FROM [{schema}].[{table}]
                    WHERE TRY_CONVERT(bigint, [{ENTRY_COL}]) IS NOT NULL
                      AND TRY_CONVERT(bigint, [{ENTRY_COL}]) >= :lower_bound
                      AND TRY_CONVERT(bigint, [{ENTRY_COL}]) <= :upper_bound
                    UNION ALL
                    SELECT :lower_sentinel
                    UNION ALL
                    SELECT :upper_sentinel
                ),
                paired AS (
                    SELECT
                        entry_no,
                        LEAD(entry_no) OVER (ORDER BY entry_no) AS next_entry_no
                    FROM entries
                )
                SELECT TOP ({max_ranges})
                    entry_no + 1 AS start_entry,
                    next_entry_no - 1 AS end_entry,
                    next_entry_no - entry_no - 1 AS missing_count
                FROM paired
                WHERE next_entry_no IS NOT NULL
                  AND next_entry_no > entry_no + 1
                ORDER BY entry_no
                """
            ),
            params,
        ).fetchall()

    return [(int(r[0]), int(r[1]), int(r[2])) for r in rows]


def build_base_url(bc_cfg: dict, odata_page: str) -> str:
    company = quote(bc_cfg["company"])
    return (
        f"{bc_cfg['resource']}/v2.0/{bc_cfg['tenant_id']}/{bc_cfg['environment']}"
        f"/ODataV4/Company('{company}')/{odata_page}"
    )


def fetch_missing_range(
    session,
    headers: dict,
    bc_cfg: dict,
    base_url: str,
    start_entry: int,
    end_entry: int,
) -> tuple[list[dict], dict]:
    odata_url = (
        f"{base_url}"
        f"?$filter={ENTRY_COL} ge {start_entry} and {ENTRY_COL} le {end_entry}"
        f"&$orderby={ENTRY_COL} asc"
    )

    records: list[dict] = []
    while odata_url:
        page_records, next_link, headers = fetch_page_with_token_refresh(
            session,
            odata_url,
            headers,
            bc_cfg,
            logger,
        )
        records.extend(page_records)
        odata_url = next_link

    return records, headers


def repair_missing_entries(args: argparse.Namespace) -> None:
    odata_page, pk_cols = resolve_table_config(args)
    validate_sql_identifier(args.schema, "schema")
    validate_sql_identifier(args.table, "table")
    for pk_col in pk_cols:
        validate_sql_identifier(pk_col, "primary key column")

    logger.info(
        f"===== START Entry_No repair: [{args.schema}].[{args.table}] "
        f"from BC page {odata_page} ====="
    )

    bc_cfg, sql_cfg = load_runtime_configs()
    engine = build_runtime_engine(sql_cfg)

    if not table_exists(engine, args.schema, args.table):
        logger.warning(f"Table [{args.schema}].[{args.table}] not found.")
        return

    sql_columns = get_sql_columns(engine, args.schema, args.table)
    if ENTRY_COL not in sql_columns:
        logger.warning(f"Column [{ENTRY_COL}] not found in [{args.schema}].[{args.table}].")
        return

    missing_ranges = find_missing_ranges(
        engine,
        args.schema,
        args.table,
        args.from_entry,
        args.to_entry,
        args.max_ranges,
    )

    total_missing_numbers = sum(r[2] for r in missing_ranges)
    logger.info(
        f"Found {len(missing_ranges):,} missing range(s), "
        f"{total_missing_numbers:,} Entry_No value(s)."
    )

    for start_entry, end_entry, missing_count in missing_ranges[:20]:
        logger.info(
            f"Missing range: {start_entry} - {end_entry} "
            f"({missing_count:,} numbers)"
        )
    if len(missing_ranges) > 20:
        logger.info(f"... plus {len(missing_ranges) - 20:,} more range(s)")

    if args.dry_run or not missing_ranges:
        logger.info("Dry run or no missing ranges. No rows inserted.")
        logger.info("===== END Entry_No repair =====")
        return

    session = get_session()
    token = get_access_token(session, bc_cfg)
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    base_url = build_base_url(bc_cfg, odata_page)

    buffer: list[dict] = []
    total_fetched = 0
    total_inserted = 0
    skipped_ranges = 0

    for start_entry, end_entry, missing_count in missing_ranges:
        if missing_count > args.max_gap_width:
            skipped_ranges += 1
            logger.warning(
                f"Skipping wide gap {start_entry}-{end_entry} "
                f"({missing_count:,} numbers). Increase --max-gap-width to include it."
            )
            continue

        records, headers = fetch_missing_range(
            session,
            headers,
            bc_cfg,
            base_url,
            start_entry,
            end_entry,
        )
        total_fetched += len(records)
        buffer.extend(records)

        logger.info(
            f"Fetched {len(records):,} row(s) from BC for gap "
            f"{start_entry}-{end_entry}."
        )

        if len(buffer) >= ROWS_PER_COMMIT:
            inserted = flush_via_staging(
                engine,
                buffer,
                args.schema,
                args.table,
                sql_columns,
                pk_cols,
                logger,
            )
            total_inserted += inserted
            logger.info(f"Inserted batch: {inserted:,} row(s).")
            buffer = []

    if buffer:
        inserted = flush_via_staging(
            engine,
            buffer,
            args.schema,
            args.table,
            sql_columns,
            pk_cols,
            logger,
        )
        total_inserted += inserted
        logger.info(f"Inserted final batch: {inserted:,} row(s).")

    logger.info(f"Fetched from BC: {total_fetched:,} row(s).")
    logger.info(f"Inserted into SQL: {total_inserted:,} row(s).")
    logger.info(f"Skipped wide ranges: {skipped_ranges:,}.")
    logger.info("===== END Entry_No repair =====")


if __name__ == "__main__":
    try:
        repair_missing_entries(parse_args())
    except (InterfaceError, OperationalError) as exc:
        message = str(exc)
        if "IM002" in message or "ODBC Driver Manager" in message:
            logger.error(
                "SQL ODBC driver was not found on this machine. "
                "Install Microsoft ODBC Driver for SQL Server or run inside the Airflow container."
            )
        elif "Encryption not supported" in message or "SSL Security error" in message:
            logger.error(
                "Windows could not connect to SQL Server because of the ODBC/TLS setup. "
                "Run this script inside the Airflow container, where the DAG SQL driver/config already works."
            )
        else:
            logger.error(f"SQL connection failed: {exc}")
        raise SystemExit(1)
