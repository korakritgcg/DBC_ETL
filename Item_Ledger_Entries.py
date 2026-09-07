"""
Item_Ledger_Entries.py — Incremental load (Entry_No watermark).
OData page / SQL table : raw.Item_Ledger_Entries
PK                     : Entry_No
"""
from urllib.parse import quote
from sqlalchemy import text

from _base_etl import (
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
AUTO_REPAIR_MISSING_ENTRY_NO = True
AUTO_REPAIR_MAX_RANGES = 100
AUTO_REPAIR_MAX_GAP_WIDTH = 50000
AUTO_REPAIR_SCAN_EXISTING_GAPS = True


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


def get_entry_bounds(engine) -> tuple[int | None, int | None]:
    with engine.connect() as conn:
        row = conn.execute(
            text(
                f"""
                SELECT
                    MIN(TRY_CONVERT(bigint, [{WATERMARK_COL}])) AS min_entry,
                    MAX(TRY_CONVERT(bigint, [{WATERMARK_COL}])) AS max_entry
                FROM [{SCHEMA}].[{TABLE_NAME}]
                WHERE TRY_CONVERT(bigint, [{WATERMARK_COL}]) IS NOT NULL
                """
            )
        ).one()

    min_entry, max_entry = row
    return (
        int(min_entry) if min_entry is not None else None,
        int(max_entry) if max_entry is not None else None,
    )


def find_missing_entry_ranges(
    engine,
    from_entry: int,
    to_entry: int,
) -> list[tuple[int, int, int]]:
    if from_entry > to_entry:
        return []

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f"""
                WITH entries AS (
                    SELECT DISTINCT TRY_CONVERT(bigint, [{WATERMARK_COL}]) AS entry_no
                    FROM [{SCHEMA}].[{TABLE_NAME}]
                    WHERE TRY_CONVERT(bigint, [{WATERMARK_COL}]) >= :from_entry
                      AND TRY_CONVERT(bigint, [{WATERMARK_COL}]) <= :to_entry
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
                SELECT TOP ({AUTO_REPAIR_MAX_RANGES})
                    entry_no + 1 AS start_entry,
                    next_entry_no - 1 AS end_entry,
                    next_entry_no - entry_no - 1 AS missing_count
                FROM paired
                WHERE next_entry_no IS NOT NULL
                  AND next_entry_no > entry_no + 1
                ORDER BY entry_no
                """
            ),
            {
                "from_entry": from_entry,
                "to_entry": to_entry,
                "lower_sentinel": from_entry - 1,
                "upper_sentinel": to_entry + 1,
            },
        ).fetchall()

    return [(int(r[0]), int(r[1]), int(r[2])) for r in rows]


def fetch_entry_range(
    session,
    headers: dict,
    bc_cfg: dict,
    base_url: str,
    start_entry: int,
    end_entry: int,
) -> tuple[list[dict], dict]:
    odata_url = (
        f"{base_url}"
        f"?$filter={WATERMARK_COL} ge {start_entry} and {WATERMARK_COL} le {end_entry}"
        f"&$orderby={WATERMARK_COL} asc"
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


def repair_missing_entries_after_insert(
    engine,
    session,
    headers: dict,
    bc_cfg: dict,
    base_url: str,
    sql_columns: list,
    from_entry: int,
    to_entry: int,
) -> tuple[int, int, dict]:
    missing_ranges = find_missing_entry_ranges(engine, from_entry, to_entry)

    if not missing_ranges:
        logger.info(
            f"Auto repair check: no missing {WATERMARK_COL} "
            f"between {from_entry} and {to_entry}"
        )
        return 0, 0, headers

    total_missing_numbers = sum(r[2] for r in missing_ranges)
    logger.warning(
        f"Auto repair found {len(missing_ranges):,} missing range(s), "
        f"{total_missing_numbers:,} {WATERMARK_COL} value(s), "
        f"between {from_entry} and {to_entry}"
    )

    buffer: list[dict] = []
    total_fetched = 0
    total_inserted = 0

    for start_entry, end_entry, missing_count in missing_ranges:
        if missing_count > AUTO_REPAIR_MAX_GAP_WIDTH:
            logger.warning(
                f"Skipping wide missing range {start_entry}-{end_entry} "
                f"({missing_count:,} numbers)"
            )
            continue

        records, headers = fetch_entry_range(
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
            f"Auto repair fetched {len(records):,} row(s) "
            f"for missing range {start_entry}-{end_entry}"
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
            logger.info(f"Auto repair inserted batch: {inserted:,} row(s)")
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
        logger.info(f"Auto repair inserted final batch: {inserted:,} row(s)")

    return total_fetched, total_inserted, headers


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

    repair_fetched = 0
    repair_inserted = 0
    if AUTO_REPAIR_MISSING_ENTRY_NO:
        if AUTO_REPAIR_SCAN_EXISTING_GAPS:
            repair_from_entry, repair_to_entry = get_entry_bounds(engine)
        else:
            repair_from_entry = max_entry_no + 1
            repair_to_entry = int(last_entry_no)

        if repair_from_entry is not None and repair_to_entry is not None:
            repair_fetched, repair_inserted, headers = repair_missing_entries_after_insert(
                engine,
                session,
                headers,
                bc_cfg,
                base_url,
                sql_columns,
                repair_from_entry,
                repair_to_entry,
            )

    if total_fetched == 0:
        logger.info("No new records")

    with engine.connect() as conn:
        after = conn.execute(
            text(f"SELECT COUNT(*) FROM [{SCHEMA}].[{TABLE_NAME}]")
        ).scalar()

    logger.info(
        f"Rows before: {before:,} | after: {after:,} | inserted: {total_inserted:,}"
    )
    logger.info(
        f"Auto repair fetched: {repair_fetched:,} | inserted: {repair_inserted:,}"
    )
    logger.info(f"Last Entry_No: {last_entry_no}")
    logger.info("===== END ETL =====")
