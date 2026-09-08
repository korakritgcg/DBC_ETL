"""
Config-driven Business Central Query Web Service -> SQL Server bronze ETL.

The runner creates bronze tables from OData metadata. Full loads truncate the
target and insert fetched rows in 20,000-row batches. Incremental modes delete
either the latest EntryNo window, the last successful watermark window, or a
configured Posting_Date window, then insert fetched rows directly into the
target in 20,000-row batches.
"""
from __future__ import annotations

import argparse
import difflib
import json
import logging
import time
import urllib.parse
import uuid
import xml.etree.ElementTree as ET
from collections import OrderedDict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from sqlalchemy import create_engine, text
from sqlalchemy.types import DATE, TIME, NVARCHAR
from sqlalchemy.dialects.mssql import DATETIME2, DATETIMEOFFSET
from urllib3.util.retry import Retry


CONFIG_BC = "_config_DBC.json"
CONFIG_SQL = "_config_sql.json"
CONFIG_RAW = "_config_bc_query_raw.json"
DEFAULT_DAGS_DIR = Path("/opt/airflow/dags")
LOCAL_DIR = Path(__file__).resolve().parent

MAX_RETRIES = 5
RETRY_BACKOFF = 10
TO_SQL_CHUNKSIZE = 1000
ENTRY_NO_AUTO_REPAIR_MAX_RANGES = 100
ENTRY_NO_AUTO_REPAIR_MAX_GAP_WIDTH = 50000
DEFAULT_WATERMARK_CANDIDATES = (
    "LastDateModified",
    "SystemModifiedAt",
    "Last_Date_Modified",
    "LastModifiedDateTime",
    "Last_Modified_Date_Time",
)

logger = logging.getLogger("bc_query_raw_etl")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )


class EtlConfigError(RuntimeError):
    """Raised when job metadata is insufficient to run safely."""


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    sql_type: str
    category: str
    edm_type: str
    nullable: bool = True


@dataclass(frozen=True)
class EntityMetadata:
    service_name: str
    columns: list[ColumnSpec]
    key_columns: list[str]


def _config_path(filename: str) -> Path:
    for base in (LOCAL_DIR, DEFAULT_DAGS_DIR):
        candidate = base / filename
        if candidate.exists():
            return candidate
    return LOCAL_DIR / filename


def _load_json(filename: str) -> dict[str, Any]:
    with _config_path(filename).open(encoding="utf-8") as f:
        return json.load(f)


def load_configs() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    return _load_json(CONFIG_BC), _load_json(CONFIG_SQL), _load_json(CONFIG_RAW)


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


def get_access_token(session: requests.Session, bc_cfg: dict[str, Any]) -> str:
    resp = session.post(
        f"https://login.microsoftonline.com/{bc_cfg['tenant_id']}/oauth2/token",
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


def build_engine(sql_cfg: dict[str, Any]):
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


def qident(identifier: str) -> str:
    if not identifier or not str(identifier).strip():
        raise EtlConfigError("SQL identifier cannot be empty")
    return f"[{str(identifier).replace(']', ']]')}]"


def qtable(schema: str, table: str) -> str:
    return f"{qident(schema)}.{qident(table)}"


def _local_name(tag: str) -> str:
    return tag.split("}")[-1]


def _case_insensitive_get(mapping: dict[str, Any], key: str) -> tuple[str, Any] | tuple[None, None]:
    if key in mapping:
        return key, mapping[key]
    folded_key = key.casefold()
    for candidate, value in mapping.items():
        if candidate.casefold() == folded_key:
            return candidate, value
    return None, None


def edm_to_sql(edm: str, max_length: str | None, precision: str | None, scale: str | None) -> tuple[str, str]:
    name = (edm or "").split(".")[-1].lower()

    if name == "string":
        # Raw tables favor load safety over narrow source lengths. Some BC
        # query metadata reports a shorter MaxLength than the values returned.
        return "NVARCHAR(4000)", "string"

    if name == "boolean":
        return "BIT", "bool"
    if name in ("byte", "sbyte", "int16"):
        return "SMALLINT", "int"
    if name == "int32":
        return "INT", "int"
    if name == "int64":
        return "BIGINT", "int"
    if name == "decimal":
        if precision and str(precision).isdigit():
            p = min(int(precision), 38)
            s = int(scale) if scale and str(scale).isdigit() else min(12, p)
            return f"DECIMAL({p},{min(s, p)})", "number"
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
    if name == "binary":
        return "VARBINARY(MAX)", "binary"

    return "NVARCHAR(4000)", "string"


def fetch_metadata_xml(
    session: requests.Session,
    headers: dict[str, str],
    bc_cfg: dict[str, Any],
) -> bytes:
    url = (
        f"{bc_cfg['resource']}/v2.0/{bc_cfg['tenant_id']}"
        f"/{bc_cfg['environment']}/ODataV4/$metadata"
    )
    resp = session.get(url, headers={**headers, "Accept": "application/xml"}, timeout=120)
    if resp.status_code == 401:
        token = get_access_token(session, bc_cfg)
        headers["Authorization"] = f"Bearer {token}"
        resp = session.get(url, headers={**headers, "Accept": "application/xml"}, timeout=120)
    resp.raise_for_status()
    return resp.content


def _read_entity_metadata_catalog(
    metadata_xml: bytes,
) -> tuple[dict[str, str], dict[str, tuple[list[ColumnSpec], list[str]]]]:
    root = ET.fromstring(metadata_xml)
    entity_sets: dict[str, str] = {}
    entity_types: dict[str, tuple[list[ColumnSpec], list[str]]] = {}

    for el in root.iter():
        tag = _local_name(el.tag)
        if tag == "EntitySet":
            name = el.get("Name")
            entity_type = (el.get("EntityType") or "").split(".")[-1]
            if name and entity_type:
                entity_sets[name] = entity_type
            continue

        if tag != "EntityType":
            continue

        type_name = el.get("Name")
        if not type_name:
            continue

        keys: list[str] = []
        columns: list[ColumnSpec] = []
        for child in el:
            child_tag = _local_name(child.tag)
            if child_tag == "Key":
                for key_child in child:
                    if _local_name(key_child.tag) == "PropertyRef":
                        key_name = key_child.get("Name")
                        if key_name:
                            keys.append(key_name)
            elif child_tag == "Property":
                col_name = child.get("Name")
                if not col_name:
                    continue
                sql_type, category = edm_to_sql(
                    child.get("Type") or "",
                    child.get("MaxLength"),
                    child.get("Precision"),
                    child.get("Scale"),
                )
                columns.append(
                    ColumnSpec(
                        name=col_name,
                        sql_type=sql_type,
                        category=category,
                        edm_type=child.get("Type") or "",
                        nullable=(child.get("Nullable", "true").lower() != "false"),
                    )
                )
        entity_types[type_name] = (columns, keys)

    return entity_sets, entity_types


def _metadata_name_candidates(entity_sets: dict[str, str], service_name: str) -> list[str]:
    names = sorted(entity_sets)
    folded_service = service_name.casefold()
    tokens = [
        token
        for token in service_name.replace("-", "_").split("_")
        if len(token) >= 3 and not token.isdigit()
    ]

    candidates: list[str] = []
    for name in names:
        folded_name = name.casefold()
        if folded_service in folded_name or folded_name in folded_service:
            candidates.append(name)
            continue
        if any(token.casefold() in folded_name for token in tokens):
            candidates.append(name)

    for name in difflib.get_close_matches(service_name, names, n=10, cutoff=0.45):
        if name not in candidates:
            candidates.append(name)

    return candidates[:20]


def parse_entity_metadata(metadata_xml: bytes, service_name: str) -> EntityMetadata:
    entity_sets, entity_types = _read_entity_metadata_catalog(metadata_xml)

    resolved_service_name, type_name = _case_insensitive_get(entity_sets, service_name)
    if type_name is None:
        resolved_service_name = service_name
        type_name = service_name

    _, found = _case_insensitive_get(entity_types, type_name)
    if found is None:
        _, found = _case_insensitive_get(entity_types, service_name)
    if not found:
        candidates = _metadata_name_candidates(entity_sets, service_name)
        hint = f" Similar entity sets: {', '.join(candidates)}" if candidates else ""
        raise EtlConfigError(f"No OData metadata found for service '{service_name}'.{hint}")

    columns, key_columns = found
    if not columns:
        raise EtlConfigError(f"Service '{service_name}' has no metadata columns")

    return EntityMetadata(service_name=resolved_service_name, columns=columns, key_columns=key_columns)


def diagnose_metadata(service_name: str) -> None:
    bc_cfg, _, _ = load_configs()
    session = get_session()
    token = get_access_token(session, bc_cfg)
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    metadata_xml = fetch_metadata_xml(session, headers, bc_cfg)
    entity_sets, _ = _read_entity_metadata_catalog(metadata_xml)
    candidates = _metadata_name_candidates(entity_sets, service_name)

    print(f"Metadata entity sets: {len(entity_sets)}")
    print(f"Requested service: {service_name}")
    if candidates:
        print("Candidate entity sets:")
        for name in candidates:
            print(f"  - {name} -> {entity_sets[name]}")
    else:
        print("No candidate entity sets found.")


def create_schema_if_missing(engine, schema: str) -> None:
    schema_q = qident(schema)
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    f"""
                    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = :schema)
                    EXEC('CREATE SCHEMA {schema_q}')
                    """
                ),
                {"schema": schema},
            )
    except Exception as exc:
        if "15247" in str(exc) or "permission" in str(exc).lower():
            raise EtlConfigError(
                f"SQL login cannot create schema '{schema}'. "
                "Ask a SQL admin to run sql_bootstrap_bronze_schema.sql once, "
                "then rerun this DAG."
            ) from exc
        raise


def ensure_control_table(engine, schema: str, control_table: str) -> None:
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    f"""
                    IF OBJECT_ID(N'{schema}.{control_table}', N'U') IS NULL
                    CREATE TABLE {qtable(schema, control_table)} (
                        [job_name] NVARCHAR(256) NOT NULL PRIMARY KEY,
                        [service_name] NVARCHAR(256) NOT NULL,
                        [target_schema] SYSNAME NOT NULL,
                        [target_table] SYSNAME NOT NULL,
                        [full_load_completed] BIT NOT NULL CONSTRAINT
                            [DF_{control_table}_full_load_completed] DEFAULT (0),
                        [last_watermark] NVARCHAR(128) NULL,
                        [last_success_at] DATETIMEOFFSET NULL,
                        [last_run_type] NVARCHAR(32) NULL,
                        [last_rows_fetched] BIGINT NULL,
                        [last_rows_staged] BIGINT NULL,
                        [last_rows_inserted] BIGINT NULL,
                        [last_error] NVARCHAR(MAX) NULL
                    )
                    """
                )
            )
            conn.execute(
                text(
                    f"""
                    IF EXISTS (
                        SELECT 1
                        FROM sys.columns c
                        JOIN sys.types t ON c.user_type_id = t.user_type_id
                        WHERE c.object_id = OBJECT_ID(N'{schema}.{control_table}', N'U')
                          AND c.name = N'last_watermark'
                          AND t.name <> N'nvarchar'
                    )
                    ALTER TABLE {qtable(schema, control_table)}
                    ALTER COLUMN [last_watermark] NVARCHAR(128) NULL
                    """
                )
            )
    except Exception as exc:
        if "2760" in str(exc) or "permission" in str(exc).lower():
            raise EtlConfigError(
                f"SQL login cannot create or use tables in schema '{schema}'. "
                "Run sql_bootstrap_bronze_schema.sql as a SQL admin, verify both "
                "diagnostic columns return 1, then rerun this DAG."
            ) from exc
        raise


def schema_exists(engine, schema: str) -> bool:
    with engine.connect() as conn:
        return (
            conn.execute(
                text("SELECT 1 FROM sys.schemas WHERE name = :schema"),
                {"schema": schema},
            ).scalar()
            is not None
        )


def table_exists(engine, schema: str, table_name: str) -> bool:
    with engine.connect() as conn:
        return (
            conn.execute(
                text(
                    """
                    SELECT 1
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = :schema
                      AND TABLE_NAME = :table_name
                    """
                ),
                {"schema": schema, "table_name": table_name},
            ).scalar()
            is not None
        )


def get_sql_columns(engine, schema: str, table_name: str) -> list[str]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = :schema
                  AND TABLE_NAME = :table_name
                ORDER BY ORDINAL_POSITION
                """
            ),
            {"schema": schema, "table_name": table_name},
        )
        return [row[0] for row in rows]


def get_sql_column_info(engine, schema: str, table_name: str) -> dict[str, dict[str, Any]]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = :schema
                  AND TABLE_NAME = :table_name
                """
            ),
            {"schema": schema, "table_name": table_name},
        )
        return {
            row[0]: {
                "data_type": (row[1] or "").lower(),
                "max_length": row[2],
            }
            for row in rows
        }


def widen_raw_string_columns(
    engine,
    schema: str,
    table_name: str,
    metadata: EntityMetadata,
) -> None:
    info = get_sql_column_info(engine, schema, table_name)
    string_columns = [col for col in metadata.columns if col.category == "string"]
    to_widen: list[ColumnSpec] = []

    for col in string_columns:
        existing = info.get(col.name)
        if not existing:
            continue
        data_type = existing["data_type"]
        max_length = existing["max_length"]
        if data_type not in ("nvarchar", "varchar", "nchar", "char"):
            continue
        if max_length == -1:
            continue
        if max_length is None or int(max_length) < 4000 or data_type != "nvarchar":
            to_widen.append(col)

    if not to_widen:
        return

    with engine.begin() as conn:
        for col in to_widen:
            conn.execute(
                text(
                    f"ALTER TABLE {qtable(schema, table_name)} "
                    f"ALTER COLUMN {qident(col.name)} NVARCHAR(4000) NULL"
                )
            )
            logger.warning(
                "Widened raw string column %s.%s to NVARCHAR(4000)",
                table_name,
                col.name,
            )


def create_or_update_target_table(
    engine,
    schema: str,
    table_name: str,
    metadata: EntityMetadata,
) -> None:
    if not table_exists(engine, schema, table_name):
        col_defs = ",\n                    ".join(
            f"{qident(col.name)} {col.sql_type} NULL" for col in metadata.columns
        )
        with engine.begin() as conn:
            conn.execute(
                text(
                    f"""
                    CREATE TABLE {qtable(schema, table_name)} (
                    {col_defs}
                    )
                    """
                )
            )
        logger.info("Created table %s", qtable(schema, table_name))
        return

    existing = set(get_sql_columns(engine, schema, table_name))
    missing = [col for col in metadata.columns if col.name not in existing]
    if not missing:
        return

    with engine.begin() as conn:
        for col in missing:
            conn.execute(
                text(
                    f"ALTER TABLE {qtable(schema, table_name)} "
                    f"ADD {qident(col.name)} {col.sql_type} NULL"
                )
            )
            logger.warning(
                "Added missing column %s.%s as %s",
                table_name,
                col.name,
                col.sql_type,
            )


def validate_job_metadata(
    job: dict[str, Any],
    metadata: EntityMetadata,
    watermark_col: str | None,
) -> list[str]:
    column_names = {col.name for col in metadata.columns}
    if watermark_col and watermark_col not in column_names:
        raise EtlConfigError(
            f"Service '{metadata.service_name}' does not expose watermark column "
            f"'{watermark_col}'"
        )

    configured_keys = [str(k) for k in job.get("primary_keys", []) if str(k).strip()]
    key_columns = metadata.key_columns or configured_keys
    if not key_columns:
        raise EtlConfigError(
            f"Service '{metadata.service_name}' has no metadata key. "
            "Fill primary_keys in _config_bc_query_raw.json."
        )

    missing_keys = [key for key in key_columns if key not in column_names]
    if missing_keys:
        raise EtlConfigError(
            f"Service '{metadata.service_name}' key columns not found in metadata: "
            f"{missing_keys}"
        )

    return key_columns


def resolve_watermark_column(
    job: dict[str, Any],
    raw_cfg: dict[str, Any],
    metadata: EntityMetadata,
) -> str:
    column_names = {col.name for col in metadata.columns}
    candidates: list[str] = []
    for candidate in (
        job.get("watermark_column"),
        raw_cfg.get("watermark_column"),
        *DEFAULT_WATERMARK_CANDIDATES,
    ):
        if candidate and candidate not in candidates:
            candidates.append(str(candidate))

    for candidate in candidates:
        if candidate in column_names:
            return candidate

    modified_columns = [
        col.name
        for col in metadata.columns
        if "modified" in col.name.casefold() and col.category in ("date", "datetime")
    ]
    hint = f" Candidate modified columns: {modified_columns}" if modified_columns else ""
    raise EtlConfigError(
        f"Service '{metadata.service_name}' does not expose a supported watermark column. "
        f"Tried {candidates}.{hint}"
    )


def get_control_state(engine, schema: str, control_table: str, job_name: str) -> dict[str, Any] | None:
    with engine.connect() as conn:
        row = conn.execute(
            text(
                f"""
                SELECT [full_load_completed], [last_watermark], [last_success_at]
                FROM {qtable(schema, control_table)}
                WHERE [job_name] = :job_name
                """
            ),
            {"job_name": job_name},
        ).mappings().first()
        return dict(row) if row else None


def update_control_success(
    conn,
    schema: str,
    control_table: str,
    job_name: str,
    service_name: str,
    target_schema: str,
    target_table: str,
    full_load_completed: bool,
    run_type: str,
    rows_fetched: int,
    rows_staged: int,
    rows_inserted: int,
    last_watermark: Any,
) -> None:
    conn.execute(
        text(
            f"""
            IF EXISTS (
                SELECT 1 FROM {qtable(schema, control_table)}
                WHERE [job_name] = :job_name
            )
            UPDATE {qtable(schema, control_table)}
               SET [service_name] = :service_name,
                   [target_schema] = :target_schema,
                   [target_table] = :target_table,
                   [full_load_completed] = :full_load_completed,
                   [last_watermark] = :last_watermark,
                   [last_success_at] = SYSDATETIMEOFFSET(),
                   [last_run_type] = :run_type,
                   [last_rows_fetched] = :rows_fetched,
                   [last_rows_staged] = :rows_staged,
                   [last_rows_inserted] = :rows_inserted,
                   [last_error] = NULL
             WHERE [job_name] = :job_name
            ELSE
            INSERT INTO {qtable(schema, control_table)} (
                [job_name], [service_name], [target_schema], [target_table],
                [full_load_completed], [last_watermark], [last_success_at],
                [last_run_type], [last_rows_fetched], [last_rows_staged],
                [last_rows_inserted], [last_error]
            )
            VALUES (
                :job_name, :service_name, :target_schema, :target_table,
                :full_load_completed, :last_watermark, SYSDATETIMEOFFSET(),
                :run_type, :rows_fetched, :rows_staged, :rows_inserted, NULL
            )
            """
        ),
        {
            "job_name": job_name,
            "service_name": service_name,
            "target_schema": target_schema,
            "target_table": target_table,
            "full_load_completed": 1 if full_load_completed else 0,
            "last_watermark": last_watermark,
            "run_type": run_type,
            "rows_fetched": rows_fetched,
            "rows_staged": rows_staged,
            "rows_inserted": rows_inserted,
        },
    )


def update_control_error(
    engine,
    schema: str,
    control_table: str,
    job_name: str,
    service_name: str,
    target_table: str,
    error: Exception,
) -> None:
    try:
        if not schema_exists(engine, schema) or not table_exists(engine, schema, control_table):
            logger.warning(
                "Skipping control error write because %s does not exist yet",
                qtable(schema, control_table),
            )
            return

        with engine.begin() as conn:
            conn.execute(
                text(
                    f"""
                    IF EXISTS (
                        SELECT 1 FROM {qtable(schema, control_table)}
                        WHERE [job_name] = :job_name
                    )
                    UPDATE {qtable(schema, control_table)}
                       SET [last_error] = :last_error
                     WHERE [job_name] = :job_name
                    ELSE
                    INSERT INTO {qtable(schema, control_table)} (
                        [job_name], [service_name], [target_schema], [target_table],
                        [full_load_completed], [last_error]
                    )
                    VALUES (
                        :job_name, :service_name, :target_schema, :target_table,
                        0, :last_error
                    )
                    """
                ),
                {
                    "job_name": job_name,
                    "service_name": service_name,
                    "target_schema": schema,
                    "target_table": target_table,
                    "last_error": str(error)[:4000],
                },
            )
    except Exception:
        logger.exception("Could not write control error for %s", job_name)


def get_max_watermark(engine, schema: str, table_name: str, watermark_col: str):
    with engine.connect() as conn:
        return conn.execute(
            text(f"SELECT MAX({qident(watermark_col)}) FROM {qtable(schema, table_name)}")
        ).scalar()


def get_row_count(conn, schema: str, table_name: str) -> int:
    return int(conn.execute(text(f"SELECT COUNT(*) FROM {qtable(schema, table_name)}")).scalar() or 0)


def format_odata_datetime(value: Any) -> str:
    if isinstance(value, (int, float, Decimal)) and not isinstance(value, bool):
        return str(value)

    if isinstance(value, str):
        parsed = pd.to_datetime(value, errors="coerce", utc=True)
        if not pd.isna(parsed):
            return format_odata_datetime(parsed)
        return value.strip().replace("+00:00", "Z")

    if isinstance(value, pd.Timestamp):
        value = value.to_pydatetime()

    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        text_value = value.isoformat()
        return text_value.replace("+00:00", "Z")

    if isinstance(value, date):
        return value.isoformat()

    raise EtlConfigError(f"Unsupported watermark value for OData filter: {value!r}")


def build_odata_url(
    bc_cfg: dict[str, Any],
    service_name: str,
    params: OrderedDict[str, str | int],
) -> str:
    company = urllib.parse.quote(str(bc_cfg["company"]), safe="")
    service = urllib.parse.quote(service_name, safe="_")
    query = urllib.parse.urlencode(params)
    return (
        f"{bc_cfg['resource']}/v2.0/{bc_cfg['tenant_id']}/{bc_cfg['environment']}"
        f"/ODataV4/Company('{company}')/{service}?{query}"
    )


def fetch_page_with_token_refresh(
    session: requests.Session,
    url: str,
    headers: dict[str, str],
    bc_cfg: dict[str, Any],
) -> tuple[list[dict[str, Any]], str | None, dict[str, str]]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, headers=headers, timeout=600, stream=False)

            if resp.status_code == 401:
                logger.warning("Token expired; refreshing and retrying request")
                token = get_access_token(session, bc_cfg)
                headers["Authorization"] = f"Bearer {token}"
                resp = session.get(url, headers=headers, timeout=600, stream=False)

            resp.raise_for_status()
            data = resp.json()
            return data.get("value", []), data.get("@odata.nextLink"), headers

        except (
            requests.exceptions.ChunkedEncodingError,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.HTTPError,
        ) as exc:
            if attempt == MAX_RETRIES:
                raise
            wait = RETRY_BACKOFF * attempt
            logger.warning(
                "Fetch failed attempt %s/%s: %s. Retry in %ss",
                attempt,
                MAX_RETRIES,
                exc,
                wait,
            )
            time.sleep(wait)

    raise RuntimeError("unreachable")


def _coerce_series(series: pd.Series, category: str) -> pd.Series:
    if category in ("number", "int"):
        return pd.to_numeric(series, errors="coerce")

    if category == "bool":
        def to_bit(value):
            if value is None or (isinstance(value, float) and pd.isna(value)):
                return None
            if isinstance(value, bool):
                return 1 if value else 0
            text_value = str(value).strip().lower()
            if text_value in ("true", "1", "yes", "y"):
                return 1
            if text_value in ("false", "0", "no", "n", ""):
                return 0
            return None

        return pd.to_numeric(pd.Series([to_bit(v) for v in series], index=series.index), errors="coerce")

    if category == "datetime":
        return pd.to_datetime(_null_bc_zero_dates(series), errors="coerce")

    if category == "date":
        values = pd.to_datetime(_null_bc_zero_dates(series), errors="coerce")
        return pd.Series(
            [v.date() if not pd.isna(v) else None for v in values],
            index=series.index,
        )

    if category == "time":
        values = pd.to_datetime(series, errors="coerce")
        return pd.Series(
            [v.time() if not pd.isna(v) else None for v in values],
            index=series.index,
        )

    if category == "guid":
        return series.where(series.notna(), None).astype(object)

    return series


def _null_bc_zero_dates(series: pd.Series) -> pd.Series:
    """Replace BC's year-0001 sentinel before dateutil can read it as 2001."""
    zero_date = series.astype("string").str.match(
        r"^\s*0*1-0?1-0?1(?:[ T]|$)",
        na=False,
    )
    return series.mask(zero_date)


def _coerce_series_for_target_sql(series: pd.Series, sql_type: str) -> pd.Series:
    """Coerce values using the existing SQL table type, not only OData metadata.

    Query web services can expose date/time fields as Edm.String even when the
    bronze table was originally created as a SQL date/time type.  In that case
    binding the raw BC zero-date (year 0001) as text makes pyodbc fail with
    22018.  Pandas cannot represent that sentinel and safely converts it to
    NaT/NULL, while preserving normal SQL-compatible values.
    """
    data_type = (sql_type or "").lower()

    if data_type in ("datetimeoffset", "datetime2", "datetime", "smalldatetime"):
        values = pd.to_datetime(
            _null_bc_zero_dates(series),
            errors="coerce",
            utc=(data_type == "datetimeoffset"),
        )
        if data_type == "datetime":
            values = values.where(values >= pd.Timestamp("1753-01-01"), pd.NaT)
        elif data_type == "smalldatetime":
            values = values.where(
                (values >= pd.Timestamp("1900-01-01"))
                & (values <= pd.Timestamp("2079-06-06")),
                pd.NaT,
            )
        return values

    if data_type == "date":
        values = pd.to_datetime(_null_bc_zero_dates(series), errors="coerce")
        return pd.Series(
            [value.date() if not pd.isna(value) else None for value in values],
            index=series.index,
        )

    if data_type == "time":
        values = pd.to_datetime(series, errors="coerce")
        return pd.Series(
            [value.time() if not pd.isna(value) else None for value in values],
            index=series.index,
        )

    if data_type in (
        "tinyint", "smallint", "int", "bigint", "decimal", "numeric",
        "money", "smallmoney", "float", "real",
    ):
        return pd.to_numeric(series, errors="coerce")

    if data_type == "bit":
        return _coerce_series(series, "bool")

    if data_type == "uniqueidentifier":
        return series.where(series.notna(), None).astype(object)

    return series


def prepare_records_df(
    records: list[dict[str, Any]],
    table_columns: list[str],
    column_specs: dict[str, ColumnSpec],
    target_column_info: dict[str, dict[str, Any]] | None = None,
) -> pd.DataFrame:
    df = pd.DataFrame(records)
    if df.empty:
        return df

    df = df.loc[
        :,
        ~(
            df.columns.astype(str).str.startswith("@")
            | df.columns.astype(str).str.endswith("_Filter")
        ),
    ]

    valid_columns = [col for col in table_columns if col in df.columns]
    df = df[valid_columns]

    for col in valid_columns:
        target = (target_column_info or {}).get(col)
        if target:
            df[col] = _coerce_series_for_target_sql(df[col], target["data_type"])
            continue
        spec = column_specs.get(col)
        if not spec:
            continue
        if spec.category == "string":
            df[col] = df[col].where(df[col].notna(), None).astype(object)
        else:
            df[col] = _coerce_series(df[col], spec.category)

    return df


def build_insert_dtype_map(
    df: pd.DataFrame,
    column_specs: dict[str, ColumnSpec],
    target_column_info: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    dtype_map: dict[str, Any] = {}
    for col in df.columns:
        target = (target_column_info or {}).get(col)
        if target:
            data_type = target["data_type"]
            if data_type == "datetimeoffset":
                dtype_map[col] = DATETIMEOFFSET()
            elif data_type in ("datetime2", "datetime", "smalldatetime"):
                dtype_map[col] = DATETIME2()
            elif data_type == "date":
                dtype_map[col] = DATE()
            elif data_type == "time":
                dtype_map[col] = TIME()
            elif data_type in ("nvarchar", "varchar", "nchar", "char", "text", "ntext"):
                dtype_map[col] = NVARCHAR(length=4000)
            continue
        spec = column_specs.get(col)
        if spec and spec.category == "string":
            dtype_map[col] = NVARCHAR(length=4000)
    return dtype_map


def create_staging_table(engine, schema: str, target_table: str) -> str:
    staging_table = f"stg_{target_table}_{uuid.uuid4().hex[:8]}"
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {qtable(schema, staging_table)}"))
        conn.execute(
            text(
                f"SELECT TOP 0 * INTO {qtable(schema, staging_table)} "
                f"FROM {qtable(schema, target_table)}"
            )
        )
    return staging_table


def drop_table_if_exists(engine, schema: str, table_name: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {qtable(schema, table_name)}"))


def insert_batch_to_table(
    engine,
    schema: str,
    table_name: str,
    records: list[dict[str, Any]],
    table_columns: list[str],
    column_specs: dict[str, ColumnSpec],
) -> int:
    target_column_info = get_sql_column_info(engine, schema, table_name)
    df = prepare_records_df(
        records,
        table_columns,
        column_specs,
        target_column_info=target_column_info,
    )
    if df.empty:
        return 0

    dtype_map = build_insert_dtype_map(
        df,
        column_specs,
        target_column_info=target_column_info,
    )
    with engine.begin() as conn:
        df.to_sql(
            table_name,
            conn,
            schema=schema,
            if_exists="append",
            index=False,
            chunksize=TO_SQL_CHUNKSIZE,
            dtype=dtype_map,
        )
    return len(df)


def insert_batch_to_staging(
    engine,
    schema: str,
    staging_table: str,
    records: list[dict[str, Any]],
    table_columns: list[str],
    column_specs: dict[str, ColumnSpec],
) -> int:
    return insert_batch_to_table(engine, schema, staging_table, records, table_columns, column_specs)


def fetch_and_insert_full_load(
    engine,
    schema: str,
    target_table: str,
    table_columns: list[str],
    column_specs: dict[str, ColumnSpec],
    session: requests.Session,
    headers: dict[str, str],
    bc_cfg: dict[str, Any],
    start_url: str,
    batch_size: int,
) -> tuple[int, int, dict[str, str]]:
    with engine.begin() as conn:
        before = get_row_count(conn, schema, target_table)
        logger.info(
            "Direct full load: truncating %s before inserting fetched batches; previous rows %s",
            qtable(schema, target_table),
            f"{before:,}",
        )
        conn.execute(text(f"TRUNCATE TABLE {qtable(schema, target_table)}"))

    url: str | None = start_url
    buffer: list[dict[str, Any]] = []
    total_fetched = 0
    total_inserted = 0

    while url:
        records, next_link, headers = fetch_page_with_token_refresh(session, url, headers, bc_cfg)
        url = next_link

        if not records:
            continue

        buffer.extend(records)
        total_fetched += len(records)
        logger.info("Fetched %s rows so far", f"{total_fetched:,}")

        while len(buffer) >= batch_size:
            batch = buffer[:batch_size]
            del buffer[:batch_size]
            inserted = insert_batch_to_table(
                engine,
                schema,
                target_table,
                batch,
                table_columns,
                column_specs,
            )
            total_inserted += inserted
            logger.info(
                "Inserted batch %s rows into target; total inserted %s",
                f"{inserted:,}",
                f"{total_inserted:,}",
            )

    if buffer:
        inserted = insert_batch_to_table(
            engine,
            schema,
            target_table,
            buffer,
            table_columns,
            column_specs,
        )
        total_inserted += inserted
        logger.info(
            "Inserted final batch %s rows into target; total inserted %s",
            f"{inserted:,}",
            f"{total_inserted:,}",
        )

    return total_fetched, total_inserted, headers


def fetch_and_insert_incremental_load(
    engine,
    schema: str,
    target_table: str,
    table_columns: list[str],
    column_specs: dict[str, ColumnSpec],
    session: requests.Session,
    headers: dict[str, str],
    bc_cfg: dict[str, Any],
    start_url: str,
    batch_size: int,
    watermark_col: str,
    max_watermark: Any,
) -> tuple[int, int, int, dict[str, str]]:
    with engine.begin() as conn:
        before = get_row_count(conn, schema, target_table)
        deleted = conn.execute(
            text(
                f"DELETE FROM {qtable(schema, target_table)} "
                f"WHERE {qident(watermark_col)} >= :max_watermark"
            ),
            {"max_watermark": max_watermark},
        ).rowcount
        logger.info(
            "Direct incremental load: deleted %s rows from %s at/after watermark %s; previous rows %s",
            f"{deleted or 0:,}",
            qtable(schema, target_table),
            max_watermark,
            f"{before:,}",
        )

    url: str | None = start_url
    buffer: list[dict[str, Any]] = []
    total_fetched = 0
    total_inserted = 0

    while url:
        records, next_link, headers = fetch_page_with_token_refresh(session, url, headers, bc_cfg)
        url = next_link

        if not records:
            continue

        buffer.extend(records)
        total_fetched += len(records)
        logger.info("Fetched %s rows so far", f"{total_fetched:,}")

        while len(buffer) >= batch_size:
            batch = buffer[:batch_size]
            del buffer[:batch_size]
            inserted = insert_batch_to_table(
                engine,
                schema,
                target_table,
                batch,
                table_columns,
                column_specs,
            )
            total_inserted += inserted
            logger.info(
                "Inserted incremental batch %s rows into target; total inserted %s",
                f"{inserted:,}",
                f"{total_inserted:,}",
            )

    if buffer:
        inserted = insert_batch_to_table(
            engine,
            schema,
            target_table,
            buffer,
            table_columns,
            column_specs,
        )
        total_inserted += inserted
        logger.info(
            "Inserted final incremental batch %s rows into target; total inserted %s",
            f"{inserted:,}",
            f"{total_inserted:,}",
        )

    return total_fetched, total_inserted, int(deleted or 0), headers


def get_entry_no_reload_cutoff(
    engine,
    schema: str,
    target_table: str,
    entry_no_col: str,
    reload_rows: int,
) -> Any:
    with engine.connect() as conn:
        value = conn.execute(
            text(
                f"""
                SELECT MIN({qident(entry_no_col)})
                FROM (
                    SELECT TOP (:reload_rows) {qident(entry_no_col)}
                    FROM {qtable(schema, target_table)}
                    WHERE {qident(entry_no_col)} IS NOT NULL
                    ORDER BY {qident(entry_no_col)} DESC
                ) AS latest_rows
                """
            ),
            {"reload_rows": reload_rows},
        ).scalar()
    return value


def fetch_and_insert_entry_no_window_load(
    engine,
    schema: str,
    target_table: str,
    table_columns: list[str],
    column_specs: dict[str, ColumnSpec],
    session: requests.Session,
    headers: dict[str, str],
    bc_cfg: dict[str, Any],
    start_url: str,
    batch_size: int,
    entry_no_col: str,
    cutoff_value: Any,
) -> tuple[int, int, int, dict[str, str]]:
    with engine.begin() as conn:
        before = get_row_count(conn, schema, target_table)
        deleted = conn.execute(
            text(
                f"DELETE FROM {qtable(schema, target_table)} "
                f"WHERE {qident(entry_no_col)} >= :cutoff_value"
            ),
            {"cutoff_value": cutoff_value},
        ).rowcount
        logger.info(
            "Entry-no reload: deleted %s rows from %s where %s >= %s; previous rows %s",
            f"{deleted or 0:,}",
            qtable(schema, target_table),
            entry_no_col,
            cutoff_value,
            f"{before:,}",
        )

    url: str | None = start_url
    buffer: list[dict[str, Any]] = []
    total_fetched = 0
    total_inserted = 0

    while url:
        records, next_link, headers = fetch_page_with_token_refresh(session, url, headers, bc_cfg)
        url = next_link

        if not records:
            continue

        buffer.extend(records)
        total_fetched += len(records)
        logger.info("Fetched %s rows so far", f"{total_fetched:,}")

        while len(buffer) >= batch_size:
            batch = buffer[:batch_size]
            del buffer[:batch_size]
            inserted = insert_batch_to_table(
                engine,
                schema,
                target_table,
                batch,
                table_columns,
                column_specs,
            )
            total_inserted += inserted
            logger.info(
                "Inserted entry-no batch %s rows into target; total inserted %s",
                f"{inserted:,}",
                f"{total_inserted:,}",
            )

    if buffer:
        inserted = insert_batch_to_table(
            engine,
            schema,
            target_table,
            buffer,
            table_columns,
            column_specs,
        )
        total_inserted += inserted
        logger.info(
            "Inserted final entry-no batch %s rows into target; total inserted %s",
            f"{inserted:,}",
            f"{total_inserted:,}",
        )

    return total_fetched, total_inserted, int(deleted or 0), headers


def get_entry_no_bounds(
    engine,
    schema: str,
    target_table: str,
    entry_no_col: str,
) -> tuple[int | None, int | None]:
    with engine.connect() as conn:
        row = conn.execute(
            text(
                f"""
                SELECT
                    MIN(TRY_CONVERT(bigint, {qident(entry_no_col)})) AS min_entry,
                    MAX(TRY_CONVERT(bigint, {qident(entry_no_col)})) AS max_entry
                FROM {qtable(schema, target_table)}
                WHERE TRY_CONVERT(bigint, {qident(entry_no_col)}) IS NOT NULL
                """
            )
        ).one()

    min_entry, max_entry = row
    return (
        int(min_entry) if min_entry is not None else None,
        int(max_entry) if max_entry is not None else None,
    )


def find_missing_entry_no_ranges(
    engine,
    schema: str,
    target_table: str,
    entry_no_col: str,
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
                    SELECT DISTINCT TRY_CONVERT(bigint, {qident(entry_no_col)}) AS entry_no
                    FROM {qtable(schema, target_table)}
                    WHERE TRY_CONVERT(bigint, {qident(entry_no_col)}) >= :from_entry
                      AND TRY_CONVERT(bigint, {qident(entry_no_col)}) <= :to_entry
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


def fetch_and_insert_missing_entry_no_gaps(
    engine,
    schema: str,
    target_table: str,
    table_columns: list[str],
    column_specs: dict[str, ColumnSpec],
    session: requests.Session,
    headers: dict[str, str],
    bc_cfg: dict[str, Any],
    service_name: str,
    batch_size: int,
    entry_no_col: str,
    max_ranges: int = ENTRY_NO_AUTO_REPAIR_MAX_RANGES,
    max_gap_width: int = ENTRY_NO_AUTO_REPAIR_MAX_GAP_WIDTH,
) -> tuple[int, int, dict[str, str]]:
    from_entry, to_entry = get_entry_no_bounds(engine, schema, target_table, entry_no_col)
    if from_entry is None or to_entry is None:
        logger.info("Entry-no auto repair: no %s values found in %s", entry_no_col, qtable(schema, target_table))
        return 0, 0, headers

    buffer: list[dict[str, Any]] = []
    total_fetched = 0
    total_inserted = 0
    total_missing_ranges = 0
    total_missing_values = 0
    scan_from_entry = from_entry

    while scan_from_entry <= to_entry:
        missing_ranges = find_missing_entry_no_ranges(
            engine,
            schema,
            target_table,
            entry_no_col,
            scan_from_entry,
            to_entry,
            max_ranges,
        )

        if not missing_ranges:
            if total_missing_ranges == 0:
                logger.info(
                    "Entry-no auto repair: no missing %s between %s and %s for %s",
                    entry_no_col,
                    from_entry,
                    to_entry,
                    qtable(schema, target_table),
                )
            break

        batch_missing_values = sum(r[2] for r in missing_ranges)
        total_missing_ranges += len(missing_ranges)
        total_missing_values += batch_missing_values
        logger.warning(
            "Entry-no auto repair found %s missing range(s), %s value(s), between %s and %s for %s",
            f"{len(missing_ranges):,}",
            f"{batch_missing_values:,}",
            scan_from_entry,
            to_entry,
            qtable(schema, target_table),
        )

        for start_entry, end_entry, missing_count in missing_ranges:
            scan_from_entry = end_entry + 1
            if missing_count > max_gap_width:
                logger.warning(
                    "Entry-no auto repair skipping wide missing range %s-%s (%s numbers)",
                    start_entry,
                    end_entry,
                    f"{missing_count:,}",
                )
                continue

            params: OrderedDict[str, str | int] = OrderedDict()
            params["$filter"] = f"{entry_no_col} ge {start_entry} and {entry_no_col} le {end_entry}"
            params["$orderby"] = f"{entry_no_col} asc"
            url: str | None = build_odata_url(bc_cfg, service_name, params)

            while url:
                records, next_link, headers = fetch_page_with_token_refresh(session, url, headers, bc_cfg)
                url = next_link
                if not records:
                    continue

                total_fetched += len(records)
                buffer.extend(records)
                logger.info(
                    "Entry-no auto repair fetched %s row(s) for missing range %s-%s",
                    f"{len(records):,}",
                    start_entry,
                    end_entry,
                )

                while len(buffer) >= batch_size:
                    batch = buffer[:batch_size]
                    del buffer[:batch_size]
                    inserted = insert_batch_to_table(
                        engine,
                        schema,
                        target_table,
                        batch,
                        table_columns,
                        column_specs,
                    )
                    total_inserted += inserted
                    logger.info(
                        "Entry-no auto repair inserted batch %s rows; total inserted %s",
                        f"{inserted:,}",
                        f"{total_inserted:,}",
                    )

        if len(missing_ranges) < max_ranges:
            break

    if total_missing_ranges:
        logger.warning(
            "Entry-no auto repair scanned %s missing range(s), %s value(s), between %s and %s for %s",
            f"{total_missing_ranges:,}",
            f"{total_missing_values:,}",
            from_entry,
            to_entry,
            qtable(schema, target_table),
        )

    if buffer:
        inserted = insert_batch_to_table(
            engine,
            schema,
            target_table,
            buffer,
            table_columns,
            column_specs,
        )
        total_inserted += inserted
        logger.info(
            "Entry-no auto repair inserted final batch %s rows; total inserted %s",
            f"{inserted:,}",
            f"{total_inserted:,}",
        )

    return total_fetched, total_inserted, headers


def fetch_and_insert_rolling_window_load(
    engine,
    schema: str,
    target_table: str,
    table_columns: list[str],
    column_specs: dict[str, ColumnSpec],
    session: requests.Session,
    headers: dict[str, str],
    bc_cfg: dict[str, Any],
    start_url: str,
    batch_size: int,
    reload_date_col: str,
    cutoff_value: date,
) -> tuple[int, int, int, dict[str, str]]:
    with engine.begin() as conn:
        before = get_row_count(conn, schema, target_table)
        deleted = conn.execute(
            text(
                f"DELETE FROM {qtable(schema, target_table)} "
                f"WHERE {qident(reload_date_col)} >= :cutoff_value"
            ),
            {"cutoff_value": cutoff_value},
        ).rowcount
        logger.info(
            "Posting-date reload: deleted %s rows from %s where %s >= %s; previous rows %s",
            f"{deleted or 0:,}",
            qtable(schema, target_table),
            reload_date_col,
            cutoff_value.isoformat(),
            f"{before:,}",
        )

    url: str | None = start_url
    buffer: list[dict[str, Any]] = []
    total_fetched = 0
    total_inserted = 0

    while url:
        records, next_link, headers = fetch_page_with_token_refresh(session, url, headers, bc_cfg)
        url = next_link

        if not records:
            continue

        buffer.extend(records)
        total_fetched += len(records)
        logger.info("Fetched %s rows so far", f"{total_fetched:,}")

        while len(buffer) >= batch_size:
            batch = buffer[:batch_size]
            del buffer[:batch_size]
            inserted = insert_batch_to_table(
                engine,
                schema,
                target_table,
                batch,
                table_columns,
                column_specs,
            )
            total_inserted += inserted
            logger.info(
                "Inserted posting-date batch %s rows into target; total inserted %s",
                f"{inserted:,}",
                f"{total_inserted:,}",
            )

    if buffer:
        inserted = insert_batch_to_table(
            engine,
            schema,
            target_table,
            buffer,
            table_columns,
            column_specs,
        )
        total_inserted += inserted
        logger.info(
            "Inserted final posting-date batch %s rows into target; total inserted %s",
            f"{inserted:,}",
            f"{total_inserted:,}",
        )

    return total_fetched, total_inserted, int(deleted or 0), headers


def fetch_and_stage(
    engine,
    schema: str,
    staging_table: str,
    table_columns: list[str],
    column_specs: dict[str, ColumnSpec],
    session: requests.Session,
    headers: dict[str, str],
    bc_cfg: dict[str, Any],
    start_url: str,
    batch_size: int,
) -> tuple[int, int, dict[str, str]]:
    url: str | None = start_url
    buffer: list[dict[str, Any]] = []
    total_fetched = 0
    total_staged = 0

    while url:
        records, next_link, headers = fetch_page_with_token_refresh(session, url, headers, bc_cfg)
        url = next_link

        if not records:
            continue

        buffer.extend(records)
        total_fetched += len(records)
        logger.info("Fetched %s rows so far", f"{total_fetched:,}")

        while len(buffer) >= batch_size:
            batch = buffer[:batch_size]
            del buffer[:batch_size]
            inserted = insert_batch_to_staging(
                engine,
                schema,
                staging_table,
                batch,
                table_columns,
                column_specs,
            )
            total_staged += inserted
            logger.info("Staged batch %s rows; total staged %s", f"{inserted:,}", f"{total_staged:,}")

    if buffer:
        inserted = insert_batch_to_staging(
            engine,
            schema,
            staging_table,
            buffer,
            table_columns,
            column_specs,
        )
        total_staged += inserted
        logger.info("Staged final batch %s rows; total staged %s", f"{inserted:,}", f"{total_staged:,}")

    return total_fetched, total_staged, headers


def deduped_source_cte(schema: str, staging_table: str, columns: list[str], key_columns: list[str], watermark_col: str) -> str:
    col_list = ", ".join(qident(col) for col in columns)
    partition_cols = ", ".join(qident(col) for col in key_columns)
    return (
        "src AS ("
        f"SELECT {col_list}, "
        f"ROW_NUMBER() OVER (PARTITION BY {partition_cols} "
        f"ORDER BY {qident(watermark_col)} DESC) AS [__bc_raw_rn] "
        f"FROM {qtable(schema, staging_table)}"
        ")"
    )


def insert_deduped_from_staging(
    conn,
    schema: str,
    target_table: str,
    staging_table: str,
    columns: list[str],
    key_columns: list[str],
    watermark_col: str,
) -> int:
    col_list = ", ".join(qident(col) for col in columns)
    staged_rows = count_deduped_staging(conn, schema, staging_table, columns, key_columns, watermark_col)

    if not key_columns:
        result = conn.execute(
            text(
                f"""
                INSERT INTO {qtable(schema, target_table)} ({col_list})
                SELECT {col_list}
                FROM {qtable(schema, staging_table)}
                """
            )
        )
        return staged_rows if result.rowcount is None or result.rowcount < 0 else int(result.rowcount)

    result = conn.execute(
        text(
            f"""
            WITH {deduped_source_cte(schema, staging_table, columns, key_columns, watermark_col)}
            INSERT INTO {qtable(schema, target_table)} ({col_list})
            SELECT {col_list}
            FROM src
            WHERE [__bc_raw_rn] = 1
            """
        )
    )
    return staged_rows if result.rowcount is None or result.rowcount < 0 else int(result.rowcount)


def count_deduped_staging(
    conn,
    schema: str,
    staging_table: str,
    columns: list[str],
    key_columns: list[str],
    watermark_col: str,
) -> int:
    if not key_columns:
        return get_row_count(conn, schema, staging_table)

    value = conn.execute(
        text(
            f"""
            WITH {deduped_source_cte(schema, staging_table, columns, key_columns, watermark_col)}
            SELECT COUNT(*)
            FROM src
            WHERE [__bc_raw_rn] = 1
            """
        )
    ).scalar()
    return int(value or 0)


def null_safe_join(alias_left: str, alias_right: str, columns: list[str]) -> str:
    return " AND ".join(
        (
            f"({alias_left}.{qident(col)} = {alias_right}.{qident(col)} "
            f"OR ({alias_left}.{qident(col)} IS NULL AND {alias_right}.{qident(col)} IS NULL))"
        )
        for col in columns
    )


def replace_target_from_staging(
    engine,
    schema: str,
    target_table: str,
    staging_table: str,
    columns: list[str],
    key_columns: list[str],
    watermark_col: str,
    control_table: str,
    job_name: str,
    service_name: str,
    rows_fetched: int,
    rows_staged: int,
) -> int:
    with engine.begin() as conn:
        before = get_row_count(conn, schema, target_table)
        conn.execute(text(f"TRUNCATE TABLE {qtable(schema, target_table)}"))
        inserted = insert_deduped_from_staging(
            conn,
            schema,
            target_table,
            staging_table,
            columns,
            key_columns,
            watermark_col,
        )
        after = get_row_count(conn, schema, target_table)
        last_watermark = conn.execute(
            text(f"SELECT MAX({qident(watermark_col)}) FROM {qtable(schema, target_table)}")
        ).scalar()

        update_control_success(
            conn,
            schema,
            control_table,
            job_name,
            service_name,
            schema,
            target_table,
            True,
            "full",
            rows_fetched,
            rows_staged,
            inserted,
            last_watermark,
        )

    logger.info(
        "Full load replaced %s: rows %s -> %s, inserted %s",
        qtable(schema, target_table),
        f"{before:,}",
        f"{after:,}",
        f"{inserted:,}",
    )
    return inserted


def apply_incremental_from_staging(
    engine,
    schema: str,
    target_table: str,
    staging_table: str,
    columns: list[str],
    key_columns: list[str],
    watermark_col: str,
    max_watermark: Any,
    control_table: str,
    job_name: str,
    service_name: str,
    rows_fetched: int,
    rows_staged: int,
) -> int:
    if rows_fetched == 0:
        with engine.begin() as conn:
            last_watermark = conn.execute(
                text(f"SELECT MAX({qident(watermark_col)}) FROM {qtable(schema, target_table)}")
            ).scalar()
            update_control_success(
                conn,
                schema,
                control_table,
                job_name,
                service_name,
                schema,
                target_table,
                True,
                "incremental",
                rows_fetched,
                rows_staged,
                0,
                last_watermark,
            )
        logger.info("No incremental rows returned for %s; target untouched", target_table)
        return 0

    join_condition = null_safe_join("tgt", "src", key_columns)
    with engine.begin() as conn:
        before = get_row_count(conn, schema, target_table)
        deleted_watermark = conn.execute(
            text(
                f"DELETE FROM {qtable(schema, target_table)} "
                f"WHERE {qident(watermark_col)} = :max_watermark"
            ),
            {"max_watermark": max_watermark},
        ).rowcount

        deleted_keys = conn.execute(
            text(
                f"""
                WITH {deduped_source_cte(schema, staging_table, columns, key_columns, watermark_col)}
                DELETE tgt
                FROM {qtable(schema, target_table)} AS tgt
                WHERE EXISTS (
                    SELECT 1
                    FROM src
                    WHERE src.[__bc_raw_rn] = 1
                      AND {join_condition}
                )
                """
            )
        ).rowcount

        inserted = insert_deduped_from_staging(
            conn,
            schema,
            target_table,
            staging_table,
            columns,
            key_columns,
            watermark_col,
        )
        after = get_row_count(conn, schema, target_table)
        last_watermark = conn.execute(
            text(f"SELECT MAX({qident(watermark_col)}) FROM {qtable(schema, target_table)}")
        ).scalar()

        update_control_success(
            conn,
            schema,
            control_table,
            job_name,
            service_name,
            schema,
            target_table,
            True,
            "incremental",
            rows_fetched,
            rows_staged,
            inserted,
            last_watermark,
        )

    logger.info(
        "Incremental applied to %s: deleted watermark %s, deleted keys %s, inserted %s, rows %s -> %s",
        qtable(schema, target_table),
        f"{deleted_watermark or 0:,}",
        f"{deleted_keys or 0:,}",
        f"{inserted:,}",
        f"{before:,}",
        f"{after:,}",
    )
    return inserted


def enabled_jobs(raw_cfg: dict[str, Any]) -> list[dict[str, Any]]:
    return [job for job in raw_cfg.get("jobs", []) if job.get("enabled", True)]


def get_job(raw_cfg: dict[str, Any], service_name: str) -> dict[str, Any]:
    for job in enabled_jobs(raw_cfg):
        if job["service_name"] == service_name:
            return job
    raise EtlConfigError(f"Enabled service not found in {CONFIG_RAW}: {service_name}")


def run_job(service_name: str) -> None:
    bc_cfg, sql_cfg, raw_cfg = load_configs()
    schema = raw_cfg.get("schema", "bronze")
    control_table = raw_cfg.get("control_table", "_bc_query_etl_control")
    batch_size = int(raw_cfg.get("batch_size", 20000))
    if batch_size != 20000:
        raise EtlConfigError("batch_size must remain 20000 for this bronze ETL")

    job = get_job(raw_cfg, service_name)
    job_name = job.get("job_name") or service_name
    target_table = job.get("target_table") or service_name
    odata_service_name = job.get("odata_service_name") or job.get("metadata_service_name") or service_name

    engine = build_engine(sql_cfg)
    session = get_session()
    token = get_access_token(session, bc_cfg)
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    staging_table: str | None = None

    logger.info("===== START bronze BC query ETL: %s =====", service_name)
    try:
        create_schema_if_missing(engine, schema)
        ensure_control_table(engine, schema, control_table)

        metadata_xml = fetch_metadata_xml(session, headers, bc_cfg)
        metadata = parse_entity_metadata(metadata_xml, odata_service_name)
        load_mode = str(job.get("load_mode", "")).casefold()
        force_full_load = load_mode == "full"
        posting_date_window = load_mode in ("posting_date_window", "rolling_window")
        entry_no_window = load_mode in ("entry_no_window", "entry_no_reload")
        metadata_column_names = {col.name for col in metadata.columns}
        reload_date_col = str(job.get("reload_date_column", "Posting_Date"))
        if posting_date_window and reload_date_col not in metadata_column_names:
            raise EtlConfigError(
                f"Service '{metadata.service_name}' does not expose reload date column "
                f"'{reload_date_col}'"
            )
        entry_no_col = str(job.get("entry_no_column", "EntryNo"))
        if entry_no_window and entry_no_col not in metadata_column_names:
            raise EtlConfigError(
                f"Service '{metadata.service_name}' does not expose entry no column "
                f"'{entry_no_col}'"
            )
        watermark_col: str | None = None
        if not force_full_load and not posting_date_window and not entry_no_window:
            watermark_col = resolve_watermark_column(job, raw_cfg, metadata)
            configured_watermark = job.get("watermark_column") or raw_cfg.get("watermark_column")
            if configured_watermark and watermark_col != configured_watermark:
                logger.info(
                    "Resolved watermark column for %s from %s to %s",
                    metadata.service_name,
                    configured_watermark,
                    watermark_col,
                )
        else:
            logger.info("Running %s without watermark; load_mode=%s", service_name, load_mode or "full")
        key_columns = validate_job_metadata(job, metadata, watermark_col)

        create_or_update_target_table(engine, schema, target_table, metadata)
        widen_raw_string_columns(engine, schema, target_table, metadata)
        table_columns = get_sql_columns(engine, schema, target_table)
        column_specs = {col.name: col for col in metadata.columns}

        state = get_control_state(engine, schema, control_table, job_name)
        target_max_watermark = (
            get_max_watermark(engine, schema, target_table, watermark_col)
            if watermark_col
            else None
        )
        control_watermark = state.get("last_watermark") if state else None
        max_watermark = control_watermark if control_watermark is not None else target_max_watermark
        is_full_load = (
            force_full_load
            or not state
            or not state.get("full_load_completed")
            or (not posting_date_window and not entry_no_window and max_watermark is None)
        )

        params: OrderedDict[str, str | int] = OrderedDict()
        if is_full_load:
            run_type = "full"
        elif entry_no_window:
            run_type = "entry_no_window"
            reload_rows = int(job.get("reload_rows", raw_cfg.get("entry_no_reload_rows", 1000000)))
            entry_no_cutoff = get_entry_no_reload_cutoff(
                engine,
                schema,
                target_table,
                entry_no_col,
                reload_rows,
            )
            if entry_no_cutoff is None:
                logger.info(
                    "No existing %s rows found for %s; falling back to full load",
                    entry_no_col,
                    qtable(schema, target_table),
                )
                is_full_load = True
                run_type = "full"
            else:
                params["$filter"] = f"{entry_no_col} ge {entry_no_cutoff}"
                params["$orderby"] = f"{entry_no_col} asc"
                logger.info(
                    "Using entry-no reload window on %s: last %s rows from cutoff %s",
                    entry_no_col,
                    f"{reload_rows:,}",
                    entry_no_cutoff,
                )
        elif posting_date_window:
            run_type = "posting_date_window"
            reload_days = int(job.get("reload_days", raw_cfg.get("reload_days", 90)))
            cutoff_value = date.today() - timedelta(days=reload_days)
            params["$filter"] = f"{reload_date_col} ge {cutoff_value.isoformat()}"
            logger.info(
                "Using %s-day posting-date reload window on %s from %s",
                reload_days,
                reload_date_col,
                cutoff_value.isoformat(),
            )
        else:
            run_type = "incremental"
            if control_watermark is not None:
                logger.info(
                    "Using control watermark for incremental reload: %s",
                    control_watermark,
                )
            watermark_literal = format_odata_datetime(max_watermark)
            params["$filter"] = f"{watermark_col} ge {watermark_literal}"

        resolved_odata_service_name = metadata.service_name
        if resolved_odata_service_name != service_name:
            logger.info(
                "Resolved OData service name %s to metadata entity set %s",
                service_name,
                resolved_odata_service_name,
            )

        start_url = build_odata_url(bc_cfg, resolved_odata_service_name, params)
        logger.info(
            "Running %s load for %s with keys %s",
            run_type,
            qtable(schema, target_table),
            key_columns,
        )

        if is_full_load:
            rows_fetched, rows_inserted, headers = fetch_and_insert_full_load(
                engine,
                schema,
                target_table,
                table_columns,
                column_specs,
                session,
                headers,
                bc_cfg,
                start_url,
                batch_size,
            )
            with engine.begin() as conn:
                after = get_row_count(conn, schema, target_table)
                last_watermark = (
                    conn.execute(
                        text(
                            f"SELECT MAX({qident(entry_no_col if entry_no_window else reload_date_col if posting_date_window else watermark_col)}) "
                            f"FROM {qtable(schema, target_table)}"
                        )
                    ).scalar()
                    if watermark_col or posting_date_window or entry_no_window
                    else None
                )
                update_control_success(
                    conn,
                    schema,
                    control_table,
                    job_name,
                    service_name,
                    schema,
                    target_table,
                    True,
                    "full",
                    rows_fetched,
                    rows_inserted,
                    rows_inserted,
                    last_watermark,
                )
            logger.info(
                "Direct full load completed for %s: fetched %s, inserted %s, target rows %s",
                qtable(schema, target_table),
                f"{rows_fetched:,}",
                f"{rows_inserted:,}",
                f"{after:,}",
            )
        else:
            if entry_no_window:
                rows_fetched, rows_inserted, rows_deleted, headers = fetch_and_insert_entry_no_window_load(
                    engine,
                    schema,
                    target_table,
                    table_columns,
                    column_specs,
                    session,
                    headers,
                    bc_cfg,
                    start_url,
                    batch_size,
                    entry_no_col,
                    entry_no_cutoff,
                )
                repair_fetched, repair_inserted, headers = fetch_and_insert_missing_entry_no_gaps(
                    engine,
                    schema,
                    target_table,
                    table_columns,
                    column_specs,
                    session,
                    headers,
                    bc_cfg,
                    resolved_odata_service_name,
                    batch_size,
                    entry_no_col,
                )
                rows_fetched += repair_fetched
                rows_inserted += repair_inserted
                logger.info(
                    "Entry-no auto repair completed for %s: fetched %s, inserted %s",
                    qtable(schema, target_table),
                    f"{repair_fetched:,}",
                    f"{repair_inserted:,}",
                )
            elif posting_date_window:
                rows_fetched, rows_inserted, rows_deleted, headers = fetch_and_insert_rolling_window_load(
                    engine,
                    schema,
                    target_table,
                    table_columns,
                    column_specs,
                    session,
                    headers,
                    bc_cfg,
                    start_url,
                    batch_size,
                    reload_date_col,
                    cutoff_value,
                )
            else:
                rows_fetched, rows_inserted, rows_deleted, headers = fetch_and_insert_incremental_load(
                    engine,
                    schema,
                    target_table,
                    table_columns,
                    column_specs,
                    session,
                    headers,
                    bc_cfg,
                    start_url,
                    batch_size,
                    watermark_col,
                    max_watermark,
                )
            with engine.begin() as conn:
                after = get_row_count(conn, schema, target_table)
                if entry_no_window:
                    last_watermark = conn.execute(
                        text(f"SELECT MAX({qident(entry_no_col)}) FROM {qtable(schema, target_table)}")
                    ).scalar()
                elif posting_date_window:
                    last_watermark = conn.execute(
                        text(f"SELECT MAX({qident(reload_date_col)}) FROM {qtable(schema, target_table)}")
                    ).scalar()
                else:
                    last_watermark = conn.execute(
                        text(f"SELECT MAX({qident(watermark_col)}) FROM {qtable(schema, target_table)}")
                    ).scalar()
                update_control_success(
                    conn,
                    schema,
                    control_table,
                    job_name,
                    service_name,
                    schema,
                    target_table,
                    True,
                    "incremental",
                    rows_fetched,
                    rows_inserted,
                    rows_inserted,
                    last_watermark,
                )
            if entry_no_window:
                logger.info(
                    "Entry-no reload completed for %s: deleted %s, fetched %s, inserted %s, target rows %s",
                    qtable(schema, target_table),
                    f"{rows_deleted:,}",
                    f"{rows_fetched:,}",
                    f"{rows_inserted:,}",
                    f"{after:,}",
                )
            elif posting_date_window:
                logger.info(
                    "Posting-date reload completed for %s: deleted %s, fetched %s, inserted %s, target rows %s",
                    qtable(schema, target_table),
                    f"{rows_deleted:,}",
                    f"{rows_fetched:,}",
                    f"{rows_inserted:,}",
                    f"{after:,}",
                )
            else:
                logger.info(
                    "Direct incremental completed for %s: deleted %s, fetched %s, inserted %s, target rows %s",
                    qtable(schema, target_table),
                    f"{rows_deleted:,}",
                    f"{rows_fetched:,}",
                    f"{rows_inserted:,}",
                    f"{after:,}",
                )

        logger.info("===== END bronze BC query ETL: %s =====", service_name)

    except Exception as exc:
        logger.exception("Bronze BC query ETL failed for %s", service_name)
        update_control_error(engine, schema, control_table, job_name, service_name, target_table, exc)
        raise
    finally:
        if staging_table:
            drop_table_if_exists(engine, schema, staging_table)


def run_all_jobs() -> None:
    _, _, raw_cfg = load_configs()
    for job in enabled_jobs(raw_cfg):
        run_job(job["service_name"])


def main() -> None:
    parser = argparse.ArgumentParser(description="Business Central query bronze ETL")
    parser.add_argument(
        "--diagnose-metadata",
        metavar="SERVICE_NAME",
        help="Fetch $metadata and print entity sets similar to SERVICE_NAME.",
    )
    args = parser.parse_args()

    if args.diagnose_metadata:
        diagnose_metadata(args.diagnose_metadata)
        return

    run_all_jobs()


if __name__ == "__main__":
    main()
