from urllib.parse import quote

from sqlalchemy import text

from DBC_ETL._base_etl import ROWS_PER_COMMIT, fetch_page_with_token_refresh, flush_via_staging


def get_entry_bounds(engine, schema: str, table: str, entry_col: str) -> tuple[int | None, int | None]:
    with engine.connect() as conn:
        row = conn.execute(
            text(
                f"""
                SELECT
                    MIN(TRY_CONVERT(bigint, [{entry_col}])) AS min_entry,
                    MAX(TRY_CONVERT(bigint, [{entry_col}])) AS max_entry
                FROM [{schema}].[{table}]
                WHERE TRY_CONVERT(bigint, [{entry_col}]) IS NOT NULL
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
    schema: str,
    table: str,
    entry_col: str,
    from_entry: int,
    to_entry: int,
    max_ranges: int,
) -> list[tuple[int, int, int]]:
    if from_entry > to_entry:
        return []

    max_ranges = max(1, int(max_ranges))

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f"""
                WITH entries AS (
                    SELECT DISTINCT TRY_CONVERT(bigint, [{entry_col}]) AS entry_no
                    FROM [{schema}].[{table}]
                    WHERE TRY_CONVERT(bigint, [{entry_col}]) >= :from_entry
                      AND TRY_CONVERT(bigint, [{entry_col}]) <= :to_entry
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
            {
                "from_entry": from_entry,
                "to_entry": to_entry,
                "lower_sentinel": from_entry - 1,
                "upper_sentinel": to_entry + 1,
            },
        ).fetchall()

    return [(int(r[0]), int(r[1]), int(r[2])) for r in rows]


def build_odata_base_url(bc_cfg: dict, odata_page: str) -> str:
    company = quote(bc_cfg["company"])
    return (
        f"{bc_cfg['resource']}/v2.0/{bc_cfg['tenant_id']}/{bc_cfg['environment']}"
        f"/ODataV4/Company('{company}')/{odata_page}"
    )


def fetch_entry_range(
    session,
    headers: dict,
    bc_cfg: dict,
    base_url: str,
    entry_col: str,
    start_entry: int,
    end_entry: int,
    logger,
) -> tuple[list[dict], dict]:
    odata_url = (
        f"{base_url}"
        f"?$filter={entry_col} ge {start_entry} and {entry_col} le {end_entry}"
        f"&$orderby={entry_col} asc"
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


def repair_existing_entry_no_gaps(
    engine,
    session,
    headers: dict,
    bc_cfg: dict,
    schema: str,
    table: str,
    odata_page: str,
    entry_col: str,
    sql_columns: list,
    pk_cols: list,
    logger,
    max_ranges: int = 100,
    max_gap_width: int = 50000,
) -> tuple[int, int, dict]:
    from_entry, to_entry = get_entry_bounds(engine, schema, table, entry_col)
    if from_entry is None or to_entry is None:
        logger.info(f"Auto repair check: no [{entry_col}] values found in [{schema}].[{table}]")
        return 0, 0, headers

    base_url = build_odata_base_url(bc_cfg, odata_page)
    buffer: list[dict] = []
    total_fetched = 0
    total_inserted = 0
    total_missing_ranges = 0
    total_missing_numbers = 0
    scan_from_entry = from_entry

    while scan_from_entry <= to_entry:
        missing_ranges = find_missing_entry_ranges(
            engine,
            schema,
            table,
            entry_col,
            scan_from_entry,
            to_entry,
            max_ranges,
        )

        if not missing_ranges:
            if total_missing_ranges == 0:
                logger.info(
                    f"Auto repair check: no missing {entry_col} "
                    f"between {from_entry} and {to_entry}"
                )
            break

        batch_missing_numbers = sum(r[2] for r in missing_ranges)
        total_missing_ranges += len(missing_ranges)
        total_missing_numbers += batch_missing_numbers
        logger.warning(
            f"Auto repair found {len(missing_ranges):,} missing range(s), "
            f"{batch_missing_numbers:,} {entry_col} value(s), "
            f"between {scan_from_entry} and {to_entry}"
        )

        for start_entry, end_entry, missing_count in missing_ranges:
            scan_from_entry = end_entry + 1
            if missing_count > max_gap_width:
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
                entry_col,
                start_entry,
                end_entry,
                logger,
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
                    schema,
                    table,
                    sql_columns,
                    pk_cols,
                    logger,
                )
                total_inserted += inserted
                logger.info(f"Auto repair inserted batch: {inserted:,} row(s)")
                buffer = []

        if len(missing_ranges) < max_ranges:
            break

    if total_missing_ranges:
        logger.warning(
            f"Auto repair scanned {total_missing_ranges:,} missing range(s), "
            f"{total_missing_numbers:,} {entry_col} value(s), "
            f"between {from_entry} and {to_entry}"
        )

    if buffer:
        inserted = flush_via_staging(
            engine,
            buffer,
            schema,
            table,
            sql_columns,
            pk_cols,
            logger,
        )
        total_inserted += inserted
        logger.info(f"Auto repair inserted final batch: {inserted:,} row(s)")

    return total_fetched, total_inserted, headers
