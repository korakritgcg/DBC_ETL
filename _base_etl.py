"""
_base_etl.py — Shared helpers for all BC ETL scripts.
"""
import json
import time
import logging
import urllib.parse
import uuid
import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from sqlalchemy import create_engine, text
from sqlalchemy.types import NVARCHAR
from sqlalchemy.dialects.mssql import DATETIMEOFFSET, DATETIME2
from urllib3.util.retry import Retry

MAX_RETRIES = 5
RETRY_BACKOFF = 10
ROWS_PER_COMMIT = 15000
TOKEN_TTL = 3000
LOCAL_DIR = Path(__file__).resolve().parent
DEFAULT_DAGS_DIR = Path("/opt/airflow/dags")


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def load_configs():
    # The DAGs are deployed below /opt/airflow/dags/DBC_ETL.  Resolve config
    # relative to this module first, while retaining the old root-level path
    # as a backwards-compatible fallback for older deployments.
    bc_path = LOCAL_DIR / "_config_DBC.json"
    sql_path = LOCAL_DIR / "_config_sql.json"
    if not bc_path.exists():
        bc_path = DEFAULT_DAGS_DIR / "_config_DBC.json"
    if not sql_path.exists():
        sql_path = DEFAULT_DAGS_DIR / "_config_sql.json"

    with bc_path.open(encoding="utf-8") as f:
        bc = json.load(f)

    with sql_path.open(encoding="utf-8") as f:
        sql = json.load(f)

    return bc, sql


def get_session() -> requests.Session:
    session = requests.Session()

    retry = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )

    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def get_access_token(session: requests.Session, bc_cfg: dict) -> str:
    url = f"https://login.microsoftonline.com/{bc_cfg['tenant_id']}/oauth2/token"

    resp = session.post(
        url,
        data={
            "grant_type": "client_credentials",
            "client_id": bc_cfg["client_id"],
            "client_secret": bc_cfg["client_secret"],
            "resource": bc_cfg["resource"],
        },
        timeout=30,
    )

    resp.raise_for_status()
    return resp.json()["access_token"]


def build_engine(sql_cfg: dict):
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_cfg['server']},{sql_cfg['port']};"
        f"DATABASE={sql_cfg['database']};"
        f"UID={sql_cfg['username']};"
        f"PWD={sql_cfg['password']};"
        "TrustServerCertificate=yes;"
        "Unicode_Results=Yes;"
    )

    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        fast_executemany=True,
        pool_pre_ping=True,
    )


def fetch_page(session, url, headers, max_retries=MAX_RETRIES):
    """
    GET a single OData page with retry.
    If response is 401, raise immediately so caller can refresh token.
    """
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, headers=headers, timeout=600, stream=False)

            if resp.status_code == 401:
                raise requests.exceptions.HTTPError(
                    "401 Unauthorized — token expired",
                    response=resp,
                )

            resp.raise_for_status()
            data = resp.json()

            return data.get("value", []), data.get("@odata.nextLink")

        except requests.exceptions.HTTPError as e:
            if "401" in str(e):
                raise

            if attempt == max_retries:
                raise

            wait = RETRY_BACKOFF * attempt
            logging.warning(
                f"Fetch failed attempt {attempt}/{max_retries}: {e}. Retry in {wait}s"
            )
            time.sleep(wait)

        except (
            requests.exceptions.ChunkedEncodingError,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        ) as e:
            if attempt == max_retries:
                raise

            wait = RETRY_BACKOFF * attempt
            logging.warning(
                f"Fetch failed attempt {attempt}/{max_retries}: {e}. Retry in {wait}s"
            )
            time.sleep(wait)


def fetch_page_with_token_refresh(
    session,
    url,
    headers,
    bc_cfg,
    logger,
    max_retries=MAX_RETRIES,
):
    """
    fetch_page + auto token refresh when 401 occurs.
    Use this for large tables that may run longer than token lifetime.
    """
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, headers=headers, timeout=600, stream=False)

            if resp.status_code == 401:
                logger.warning("Token expired (401) — refreshing token and retrying...")
                new_token = get_access_token(session, bc_cfg)
                headers["Authorization"] = f"Bearer {new_token}"

                resp = session.get(url, headers=headers, timeout=600, stream=False)

            resp.raise_for_status()
            data = resp.json()

            return data.get("value", []), data.get("@odata.nextLink"), headers

        except (
            requests.exceptions.ChunkedEncodingError,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        ) as e:
            if attempt == max_retries:
                raise

            wait = RETRY_BACKOFF * attempt
            logger.warning(
                f"Fetch failed attempt {attempt}/{max_retries}: {e}. Retry in {wait}s"
            )
            time.sleep(wait)

        except requests.exceptions.HTTPError as e:
            if attempt == max_retries:
                raise

            wait = RETRY_BACKOFF * attempt
            logger.warning(
                f"HTTP failed attempt {attempt}/{max_retries}: {e}. Retry in {wait}s"
            )
            time.sleep(wait)


def get_sql_columns(engine, schema: str, table: str) -> list:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = :schema
                  AND TABLE_NAME = :table
                ORDER BY ORDINAL_POSITION
                """
            ),
            {"schema": schema, "table": table},
        )

        return [r[0] for r in rows]


def get_sql_column_types(engine, schema: str, table: str) -> dict:
    """Return {column_name: data_type_lowercase} for a SQL table."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT COLUMN_NAME, DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = :schema
                  AND TABLE_NAME = :table
                ORDER BY ORDINAL_POSITION
                """
            ),
            {"schema": schema, "table": table},
        )

        return {r[0]: (r[1] or "").lower() for r in rows}


_SQL_STRING_TYPES = {
    "nvarchar", "varchar", "char", "nchar", "text", "ntext", "sql_variant",
    "xml", "varbinary", "binary", "image",
}


def _null_bc_zero_dates(series: "pd.Series") -> "pd.Series":
    """Replace BC's year-0001 sentinel before dateutil can read it as 2001."""
    zero_date = series.astype("string").str.match(
        r"^\s*0*1-0?1-0?1(?:[ T]|$)",
        na=False,
    )
    return series.mask(zero_date)


def _build_dtype_map(df: "pd.DataFrame", col_types: dict) -> dict:
    """
    Build the dtype map passed to df.to_sql().

    - object columns        -> NVARCHAR(4000)   (unchanged behaviour)
    - datetime columns      -> EXPLICIT SQL type (DATETIMEOFFSET / DATETIME2)

    Why the datetime branch matters:
    When BC sends a datetime WITH a timezone offset (e.g.
    '2026-05-20 03:33:42.707000 +00:00'), pd.to_datetime() produces a
    timezone-AWARE dtype (datetime64[ns, UTC]). When pandas then auto-creates
    a staging table for such a column, it maps the column to the ANSI SQL
    type TIMESTAMP. On SQL Server, TIMESTAMP is a synonym for ROWVERSION — an
    auto-generated binary column that rejects explicit inserts, producing:
        "Cannot insert an explicit value into a timestamp column."
    Pinning an explicit type here stops pandas from ever emitting TIMESTAMP,
    regardless of whether the source value carries a tz offset.

    `col_types` maps column name -> target SQL type (lowercase). A column
    whose target is 'datetimeoffset' is pinned to DATETIMEOFFSET so the tz
    offset is preserved; everything else datetime-like falls back to
    DATETIME2 (which also covers tz-naive values safely).
    """
    dtype_map: dict = {}
    for c in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[c]):
            t = (col_types.get(c) or "").lower()
            dtype_map[c] = DATETIMEOFFSET() if t == "datetimeoffset" else DATETIME2()
        elif df[c].dtype == object:
            dtype_map[c] = NVARCHAR(4000)
    return dtype_map


def _coerce_for_sql_type(series: "pd.Series", sql_type: str) -> "pd.Series":
    """
    Cast a pandas Series so it can be bound into a SQL column of `sql_type`.
    Empty strings / invalid values become NaN/NaT/None, which pyodbc sends
    as NULL. Out-of-range dates for DATETIME/SMALLDATETIME also become NULL
    (BC's '0001-01-01' sentinel is the common case).
    """
    t = (sql_type or "").lower()

    if t in ("date", "datetime2", "datetimeoffset"):
        return pd.to_datetime(_null_bc_zero_dates(series), errors="coerce")

    if t in ("datetime", "smalldatetime"):
        d = pd.to_datetime(_null_bc_zero_dates(series), errors="coerce")
        if t == "smalldatetime":
            lo, hi = pd.Timestamp("1900-01-01"), pd.Timestamp("2079-06-06")
        else:
            lo, hi = pd.Timestamp("1753-01-01"), pd.Timestamp("9999-12-31")
        return d.where((d >= lo) & (d <= hi), pd.NaT)

    if t == "time":
        return pd.to_datetime(series, errors="coerce")

    if t in (
        "tinyint", "smallint", "int", "bigint",
        "decimal", "numeric", "money", "smallmoney",
        "float", "real",
    ):
        return pd.to_numeric(series, errors="coerce")

    if t == "bit":
        def _bit(v):
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return None
            if isinstance(v, bool):
                return 1 if v else 0
            s = str(v).strip().lower()
            if s in ("true", "1", "yes", "y"):
                return 1
            if s in ("false", "0", "no", "n", ""):
                return 0
            return None

        return pd.to_numeric(
            pd.Series([_bit(v) for v in series], index=series.index),
            errors="coerce",
        )

    if t == "uniqueidentifier":
        def _guid(v):
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return None
            s = str(v).strip()
            return s if s else None

        return pd.Series([_guid(v) for v in series], index=series.index)

    return series


def _coerce_df_to_sql(df: "pd.DataFrame", col_types: dict) -> "pd.DataFrame":
    """Coerce every df column whose target SQL type is non-string."""
    for col in list(df.columns):
        t = col_types.get(col)
        if t and t not in _SQL_STRING_TYPES:
            df[col] = _coerce_for_sql_type(df[col], t)
    return df


def table_exists(engine, schema: str, table: str) -> bool:
    with engine.connect() as conn:
        result = conn.execute(
            text(
                """
                SELECT 1
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = :schema
                  AND TABLE_NAME = :table
                """
            ),
            {"schema": schema, "table": table},
        ).scalar()

        return result is not None


def flush_via_staging(
    engine,
    records: list,
    schema: str,
    table: str,
    sql_columns: list,
    pk_cols: list,
    logger,
) -> int:
    """
    Incremental load strategy:
    - Load records into unique staging table
    - Insert only rows not already existing in target
    - Retry SQL deadlock automatically
    """
    if not records:
        return 0

    col_types = get_sql_column_types(engine, schema, table)

    max_deadlock_retries = 5

    for attempt in range(1, max_deadlock_retries + 1):
        staging = f"stg_{table}_{uuid.uuid4().hex[:8]}"

        try:
            df = pd.DataFrame(records)
            df = df.loc[:, ~(df.columns.astype(str).str.startswith("@") | df.columns.astype(str).str.endswith("_Filter"))]

            valid_cols = [c for c in df.columns if c in sql_columns]
            dropped = [c for c in df.columns if c not in sql_columns]

            if dropped:
                logger.warning(
                    f"Dropping {len(dropped)} columns not in SQL table: {dropped}"
                )

            df = df[valid_cols]

            if df.empty:
                return 0

            df = _coerce_df_to_sql(df, col_types)

            dtype_map = _build_dtype_map(df, col_types)

            col_list = ", ".join(f"[{c}]" for c in valid_cols)
            pk_join = " AND ".join(
                f"tgt.[{p}] = src.[{p}]"
                for p in pk_cols
            )

            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS [{schema}].[{staging}]"))

                df.to_sql(
                    staging,
                    conn,
                    schema=schema,
                    if_exists="append",
                    index=False,
                    chunksize=1000,
                    dtype=dtype_map,
                )

                result = conn.execute(
                    text(
                        f"""
                        INSERT INTO [{schema}].[{table}] ({col_list})
                        SELECT {col_list}
                        FROM [{schema}].[{staging}] AS src
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM [{schema}].[{table}] AS tgt WITH (ROWLOCK, UPDLOCK)
                            WHERE {pk_join}
                        )
                        """
                    )
                )

                rows_inserted = result.rowcount

                conn.execute(text(f"DROP TABLE IF EXISTS [{schema}].[{staging}]"))

            if rows_inserted < len(df):
                logger.warning(
                    f"Skipped {len(df) - rows_inserted} duplicate rows"
                )

            return rows_inserted

        except Exception as e:
            msg = str(e)

            if "deadlocked" in msg or "1205" in msg or "40001" in msg:
                wait = attempt * 5
                logger.warning(
                    f"SQL deadlock during flush attempt "
                    f"{attempt}/{max_deadlock_retries}. Retry in {wait}s. Error: {e}"
                )
                time.sleep(wait)
                continue

            raise

    raise RuntimeError("flush_via_staging failed after deadlock retries")


def flush_fullload_truncate(
    engine,
    df: pd.DataFrame,
    schema: str,
    table: str,
    logger,
) -> int:
    """
    Legacy full-reload strategy:
    - First run: create table
    - Existing table: TRUNCATE then append
    Note:
    - Do not use this inside batch loop.
    - For large tables, prefer batched full-load scripts.
    """
    df = df.loc[:, ~(df.columns.astype(str).str.startswith("@") | df.columns.astype(str).str.endswith("_Filter"))]

    if df.empty:
        logger.info("Empty dataframe. Nothing to load.")
        return 0

    col_types = (
        get_sql_column_types(engine, schema, table)
        if table_exists(engine, schema, table)
        else {}
    )
    dtype_map = _build_dtype_map(df, col_types)

    if not table_exists(engine, schema, table):
        logger.info(
            f"Table [{schema}].[{table}] not found — creating and loading {len(df):,} rows."
        )

        with engine.begin() as conn:
            df.to_sql(
                table,
                conn,
                schema=schema,
                if_exists="replace",
                index=False,
                chunksize=1000,
                dtype=dtype_map,
            )
    else:
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE [{schema}].[{table}]"))

            df.to_sql(
                table,
                conn,
                schema=schema,
                if_exists="append",
                index=False,
                chunksize=1000,
                dtype=dtype_map,
            )

    logger.info(f"Loaded {len(df):,} rows into [{schema}].[{table}]")
    return len(df)


# ---------------------------------------------------------------------------
# Schema evolution: auto-add columns that BC added on the source side.
# Type is resolved from the BC OData $metadata (CSDL) document so the new
# SQL column matches the source data type instead of defaulting to NVARCHAR.
# $metadata is fetched only when a new column is actually detected, so the
# normal path adds no network overhead and never serves stale type info.
# ---------------------------------------------------------------------------


def _edm_to_sql(edm: str, max_length, precision, scale):
    """Map an OData EDM type to (sql_type_str, coercion_category)."""
    name = (edm or "").split(".")[-1].lower()

    if name == "string":
        if max_length and str(max_length).isdigit():
            ml = int(max_length)
            if 0 < ml <= 4000:
                return f"NVARCHAR({ml})", "string"
        return "NVARCHAR(4000)", "string"

    if name == "boolean":
        return "BIT", "bool"
    if name in ("byte", "sbyte"):
        return "SMALLINT", "int"
    if name == "int16":
        return "SMALLINT", "int"
    if name == "int32":
        return "INT", "int"
    if name == "int64":
        return "BIGINT", "int"
    if name == "decimal":
        if precision and str(precision).isdigit():
            p = min(int(precision), 38)
            s = int(scale) if scale and str(scale).isdigit() else 0
            s = min(s, p)
            return f"DECIMAL({p},{s})", "number"
        return "DECIMAL(38,12)", "number"
    if name == "double":
        return "FLOAT", "number"
    if name == "single":
        return "REAL", "number"
    if name == "date":
        return "DATE", "date"
    if name in ("datetimeoffset", "datetime"):
        return "DATETIMEOFFSET", "datetime"
    if name in ("timeofday", "time", "duration"):
        return "TIME", "time"
    if name == "guid":
        return "UNIQUEIDENTIFIER", "guid"

    return "NVARCHAR(4000)", "string"


def fetch_edm_types(odata_page: str, logger) -> dict:
    """
    Fetch + parse the BC OData $metadata document and return:
        { property_name: {"sql_type": str, "category": str} }
    for the EntityType backing `odata_page`.

    Resilient by design: any failure returns {} so the ETL falls back to
    NVARCHAR(4000) for new columns instead of crashing.
    """
    try:
        bc_cfg, _ = load_configs()
        session = get_session()
        token = get_access_token(session, bc_cfg)

        url = (
            f"{bc_cfg['resource']}/v2.0/{bc_cfg['tenant_id']}"
            f"/{bc_cfg['environment']}/ODataV4/$metadata"
        )
        resp = session.get(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/xml",
            },
            timeout=120,
        )
        resp.raise_for_status()

        root = ET.fromstring(resp.content)

        def _local(tag: str) -> str:
            return tag.split("}")[-1]

        entity_sets: dict[str, str] = {}
        entity_types: dict[str, dict] = {}

        for el in root.iter():
            tag = _local(el.tag)

            if tag == "EntitySet":
                name = el.get("Name")
                etype = el.get("EntityType", "")
                if name:
                    entity_sets[name] = etype.split(".")[-1]

            elif tag == "EntityType":
                tname = el.get("Name")
                if not tname:
                    continue
                props: dict[str, dict] = {}
                for child in el:
                    if _local(child.tag) != "Property":
                        continue
                    pname = child.get("Name")
                    if not pname:
                        continue
                    sql_type, category = _edm_to_sql(
                        child.get("Type"),
                        child.get("MaxLength"),
                        child.get("Precision"),
                        child.get("Scale"),
                    )
                    props[pname] = {"sql_type": sql_type, "category": category}
                entity_types[tname] = props

        type_name = entity_sets.get(odata_page, odata_page)
        result = entity_types.get(type_name) or entity_types.get(odata_page) or {}

        if not result:
            logger.warning(
                f"No EDM metadata found for '{odata_page}' "
                f"— new columns will fall back to NVARCHAR(4000)."
            )

        return result

    except Exception as e:
        logger.warning(
            f"Could not fetch/parse $metadata for '{odata_page}': {e} "
            f"— new columns will fall back to NVARCHAR(4000)."
        )
        return {}


def add_missing_columns(
    engine, schema: str, table: str, df: pd.DataFrame, odata_page: str, logger
) -> dict:
    """
    ALTER TABLE ADD any column present in df but missing from the SQL table.
    Returns {column: coercion_category} for the columns that were added,
    so the caller can cast their values before insert.

    $metadata is only fetched when at least one missing column is detected,
    and it is fetched fresh each time so the resolved type is never stale.
    """
    if not table_exists(engine, schema, table):
        return {}

    sql_columns = set(get_sql_columns(engine, schema, table))
    if not sql_columns:
        return {}

    missing = []
    for col in df.columns:
        col = str(col)
        if col.startswith("@") or col in sql_columns:
            continue
        if "]" in col or not col.strip():
            logger.warning(f"Skipping unsupported column name: {col!r}")
            continue
        missing.append(col)

    if not missing:
        return {}

    edm_types = fetch_edm_types(odata_page, logger)
    added: dict[str, str] = {}

    with engine.begin() as conn:
        for col in missing:
            info = edm_types.get(col)
            sql_type = info["sql_type"] if info else "NVARCHAR(4000)"
            category = info["category"] if info else "string"

            conn.execute(
                text(
                    f"ALTER TABLE [{schema}].[{table}] "
                    f"ADD [{col}] {sql_type} NULL"
                )
            )
            added[col] = category
            logger.warning(
                f"Source added new column '{col}' "
                f"— added to [{schema}].[{table}] as {sql_type}"
            )

    return added


def _coerce_series(series: pd.Series, category: str) -> pd.Series:
    """
    Cast string-ish OData values to a pandas dtype that matches the new SQL
    column. Numeric -> float64, datetime/date/time -> datetime64[ns]. The
    non-object dtype keeps the column out of the NVARCHAR dtype_map so SQL
    Server receives a typed value (NaN/NaT become NULL on insert).
    """
    if category in ("number", "int"):
        return pd.to_numeric(series, errors="coerce")

    if category == "bool":
        def _to_bit(v):
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return None
            if isinstance(v, bool):
                return 1 if v else 0
            t = str(v).strip().lower()
            if t in ("true", "1", "yes", "y"):
                return 1
            if t in ("false", "0", "no", "n", ""):
                return 0
            return None

        return pd.to_numeric(
            pd.Series([_to_bit(v) for v in series], index=series.index),
            errors="coerce",
        )

    if category in ("datetime", "date", "time"):
        return pd.to_datetime(series, errors="coerce")

    return series


def flush_fullload_append(
    engine,
    records: list,
    schema: str,
    table: str,
    pk_cols: list,
    odata_page: str,
    logger,
) -> int:
    """
    Full-load batched APPEND with source-driven schema evolution.

    Replaces the per-script _prepare_df/_append_batch pair. When BC adds a
    new column on the source side, the column is added to the SQL table with
    the correct type (resolved from $metadata) and its values are coerced
    accordingly, so the insert no longer fails.
    """
    df = pd.DataFrame(records)
    if df.empty:
        return 0

    df = df.loc[:, ~(df.columns.astype(str).str.startswith("@") | df.columns.astype(str).str.endswith("_Filter"))]
    if pk_cols:
        df = df.drop_duplicates(subset=pk_cols, keep="last")

    if df.empty:
        return 0

    add_missing_columns(engine, schema, table, df, odata_page, logger)

    col_types = get_sql_column_types(engine, schema, table)
    df = _coerce_df_to_sql(df, col_types)

    dtype_map = _build_dtype_map(df, col_types)

    with engine.begin() as conn:
        df.to_sql(
            table,
            conn,
            schema=schema,
            if_exists="append",
            index=False,
            chunksize=1000,
            dtype=dtype_map,
        )

    return len(df)


# ---------------------------------------------------------------------------
# Monthly rolling-window reconciliation for incremental tables.
# Incremental loads only ever append rows with a new watermark, so retroactive
# changes BC makes to already-loaded rows are never picked up. This re-pulls
# the previous + current calendar month and replaces that window atomically.
# ---------------------------------------------------------------------------


def reload_recent_window(
    engine,
    session,
    headers,
    bc_cfg,
    schema: str,
    table: str,
    odata_page: str,
    watermark_col: str,
    pk_cols: list,
    date_col: str,
    logger,
) -> None:
    """
    Window cutoff = MIN(watermark) of rows whose `date_col` is on/after the
    first day of the *previous* calendar month. Every row with
    watermark >= cutoff is re-fetched from BC into a staging clone, then the
    window is replaced in a single transaction (DELETE window + INSERT from
    staging). The existing data is only ever deleted after the full window
    was successfully re-staged, so there is no data-loss gap on API failure.
    """
    if not table_exists(engine, schema, table):
        logger.warning(
            f"[{schema}].[{table}] not found — skipping monthly reload."
        )
        return

    sql_columns = get_sql_columns(engine, schema, table)
    if not sql_columns:
        logger.warning(f"No columns for [{schema}].[{table}] — skipping.")
        return

    with engine.connect() as conn:
        cutoff = conn.execute(
            text(
                f"""
                SELECT MIN([{watermark_col}])
                FROM [{schema}].[{table}]
                WHERE [{date_col}] >= DATEADD(
                    MONTH, -1,
                    DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1)
                )
                """
            )
        ).scalar()

    if cutoff is None:
        logger.info(
            f"[{schema}].[{table}] — no rows on/after previous month start; "
            f"nothing to reconcile."
        )
        return

    cutoff = int(cutoff)
    logger.info(
        f"[{schema}].[{table}] reload window: {watermark_col} >= {cutoff}"
    )

    company = urllib.parse.quote(bc_cfg["company"])
    odata_url = (
        f"{bc_cfg['resource']}/v2.0/{bc_cfg['tenant_id']}"
        f"/{bc_cfg['environment']}/ODataV4/Company('{company}')/{odata_page}"
        f"?$filter={watermark_col} ge {cutoff}"
        f"&$orderby={watermark_col} asc"
    )

    staging = f"stg_{table}_{uuid.uuid4().hex[:8]}"
    col_list = ", ".join(f"[{c}]" for c in sql_columns)
    col_types = get_sql_column_types(engine, schema, table)

    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS [{schema}].[{staging}]"))
        conn.execute(
            text(
                f"SELECT TOP 0 * INTO [{schema}].[{staging}] "
                f"FROM [{schema}].[{table}]"
            )
        )

    def _stage_batch(records: list) -> int:
        df = pd.DataFrame(records)
        df = df.loc[:, ~(df.columns.astype(str).str.startswith("@") | df.columns.astype(str).str.endswith("_Filter"))]
        df = df[[c for c in df.columns if c in sql_columns]]
        if pk_cols:
            df = df.drop_duplicates(subset=pk_cols, keep="last")
        if df.empty:
            return 0

        df = _coerce_df_to_sql(df, col_types)

        dtype_map = _build_dtype_map(df, col_types)
        with engine.begin() as conn:
            df.to_sql(
                staging,
                conn,
                schema=schema,
                if_exists="append",
                index=False,
                chunksize=1000,
                dtype=dtype_map,
            )
        return len(df)

    buffer: list = []
    total_fetched = 0
    total_staged = 0

    try:
        while odata_url:
            records, next_link, headers = fetch_page_with_token_refresh(
                session, odata_url, headers, bc_cfg, logger,
            )
            odata_url = next_link

            if not records:
                continue

            buffer.extend(records)
            total_fetched += len(records)

            if len(buffer) >= ROWS_PER_COMMIT:
                total_staged += _stage_batch(buffer)
                logger.info(
                    f"[{table}] staged {total_staged:,} / "
                    f"fetched {total_fetched:,}"
                )
                buffer = []

        if buffer:
            total_staged += _stage_batch(buffer)

        if total_fetched == 0:
            logger.warning(
                f"[{schema}].[{table}] — BC returned 0 rows for window "
                f"{watermark_col} >= {cutoff}. Keeping existing data; "
                f"window NOT deleted."
            )
            return

        with engine.begin() as conn:
            before = conn.execute(
                text(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
            ).scalar()

            deleted = conn.execute(
                text(
                    f"DELETE FROM [{schema}].[{table}] "
                    f"WHERE [{watermark_col}] >= :cut"
                ),
                {"cut": cutoff},
            ).rowcount

            inserted = conn.execute(
                text(
                    f"INSERT INTO [{schema}].[{table}] ({col_list}) "
                    f"SELECT {col_list} FROM [{schema}].[{staging}]"
                )
            ).rowcount

            after = conn.execute(
                text(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
            ).scalar()

        logger.info(
            f"[{schema}].[{table}] reconciled — fetched: {total_fetched:,} | "
            f"deleted: {deleted:,} | inserted: {inserted:,} | "
            f"rows {before:,} -> {after:,}"
        )

    finally:
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS [{schema}].[{staging}]"))
