"""Load a fixed Supermetrics campaign report into SQL Server bronze.

The saved-query URL is a credential.  This module deliberately never includes
request URLs, database connection strings, or source rows in log messages.
"""

from __future__ import annotations

import logging
import os
import random
import re
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.parse import urljoin, urlsplit

import pyodbc
import requests
from dotenv import dotenv_values


LOGGER = logging.getLogger("supermetrics_ingestion")

FACEBOOK_EXPECTED_HEADERS = (
    "Date",
    "Account ID",
    "Campaign ID",
    "Campaign name",
    "Campaign objective",
    "Ad set ID",
    "Ad set name",
    "Ad ID",
    "Ad name",
    "Reach",
    "Impressions",
    "Frequency",
    "CPM (cost per 1000 impressions)",
    "CTR (all)",
    "Cost",
    "Cost per action (CPA)",
    "Post comments",
    "Post engagements",
    "Post reactions",
    "Post Saves",
    "Post shares",
    "Video watches at 100%",
    "Video watches at 50%",
    "Video watches at 25%",
    "Purchase conversion value",
    "Omni purchase conversion value (shared item)",
)

TIKTOK_EXPECTED_HEADERS = (
    "Date",
    "Advertiser ID",
    "Campaign ID",
    "Campaign name",
    "Campaign objective type",
    "Ad group ID",
    "Ad group name",
    "Optimization goal",
    "Ad ID",
    "Ad name",
    "Reach",
    "Impressions",
    "Frequency",
    "CPM",
    "CTR",
    "Cost",
    "Video views",
    "6-second video views (focused view)",
    "15-second video views (focused view)",
    "Video views at 25%",
    "Video views at 50%",
    "Video views at 75%",
    "Video views at 100%",
    "Results",
    "Cost per result",
    "Paid likes",
    "Paid shares",
    "Paid comments",
    "Paid follows",
)


@dataclass(frozen=True)
class IngestionTarget:
    """Fixed source and SQL target contract for one advertising platform."""

    source_name: str
    api_url_env_name: str
    schema_name: str
    table_name: str
    primary_key_name: str
    app_lock_resource: str
    expected_headers: tuple[str, ...]
    ctr_column: str

    def __post_init__(self) -> None:
        identifiers = (
            self.schema_name,
            self.table_name,
            self.primary_key_name,
            self.ctr_column,
        )
        if any(
            re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value) is None
            for value in identifiers
        ):
            raise ValueError("SQL target identifiers must be simple identifiers")
        if not self.expected_headers:
            raise ValueError("Ingestion targets must define at least one header")

    @property
    def qualified_table(self) -> str:
        return f"[{self.schema_name}].[{self.table_name}]"


FACEBOOK_TARGET = IngestionTarget(
    source_name="facebook",
    api_url_env_name="SUPERMETRICS_API_URL",
    schema_name="bronze",
    table_name="supermetrics_facebook_ad",
    primary_key_name="PK_bronze_supermetrics_facebook_ad",
    # Keep the original logical lock name so an older process and the renamed
    # version cannot write concurrently during rollout.
    app_lock_resource="bronze.supermetrics_campaign_daily:ingest:v1",
    expected_headers=FACEBOOK_EXPECTED_HEADERS,
    ctr_column="ctr_all",
)

TIKTOK_TARGET = IngestionTarget(
    source_name="tiktok",
    api_url_env_name="SUPERMETRICS_TIKTOK_API_URL",
    schema_name="bronze",
    table_name="supermetrics_tiktok_ad",
    primary_key_name="PK_bronze_supermetrics_tiktok_ad",
    app_lock_resource="bronze.supermetrics_tiktok_ad:ingest:v1",
    expected_headers=TIKTOK_EXPECTED_HEADERS,
    ctr_column="ctr",
)

# Backwards-compatible Facebook constants for existing callers.
EXPECTED_HEADERS = FACEBOOK_TARGET.expected_headers
SCHEMA_NAME = FACEBOOK_TARGET.schema_name
TABLE_NAME = FACEBOOK_TARGET.table_name
QUALIFIED_TABLE = FACEBOOK_TARGET.qualified_table
APP_LOCK_RESOURCE = FACEBOOK_TARGET.app_lock_resource
HTTP_TIMEOUT = (10, 120)
MAX_HTTP_RETRIES = 4
MAX_PAGES = 100
MAX_RETRY_AFTER_SECONDS = 300.0
BIGINT_MIN = -(2**63)
BIGINT_MAX = 2**63 - 1
CAMPAIGN_MAX_LENGTH = 400
REACH_RAW_MAX_LENGTH = 500
AD_NAME_MAX_LENGTH = 1000


class IngestionError(Exception):
    """Base class for errors whose messages are safe to log."""


class ConfigurationError(IngestionError):
    """Configuration is incomplete or invalid."""


class ApiRequestError(IngestionError):
    """The source API could not be read successfully."""


class PayloadValidationError(IngestionError):
    """The source payload cannot be loaded safely."""


class DatabaseLoadError(IngestionError):
    """The database transaction failed or the target schema is incompatible."""


@dataclass(frozen=True)
class Config:
    api_url: str
    db_server: str
    db_name: str
    db_user: str
    db_password: str
    db_driver: str
    db_encrypt: bool = True
    db_trust_server_certificate: bool = True


@dataclass(frozen=True)
class FetchedBatch:
    rows: tuple[tuple[Any, ...], ...]
    request_id: str | None
    page_count: int
    row_request_ids: tuple[str | None, ...] = ()


@dataclass(frozen=True)
class PreparedBatch:
    rows: tuple[tuple[Any, ...], ...]
    run_id: uuid.UUID
    loaded_at_utc: datetime
    reach_error_count: int
    frequency_null_count: int


@dataclass(frozen=True)
class LoadResult:
    inserted: int
    updated: int
    target_total: int


def _parse_bool(name: str, value: str | None, default: bool) -> bool:
    if value is None or not value.strip():
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise ConfigurationError(f"{name} must be a yes/no boolean")


def _normalize_db_driver(value: str) -> str:
    driver = value.strip()
    if driver.startswith("{") and driver.endswith("}"):
        driver = driver[1:-1]
    return driver


def _validate_odbc_component(value: str) -> None:
    # SQL Server ODBC treats the first closing brace as the end of a braced
    # value; it has no portable escape sequence inside a connection string.
    if "}" in value:
        raise ConfigurationError(
            "Database connection values cannot contain a closing brace"
        )


def _validate_api_url(
    api_url: str,
    api_url_env_name: str = FACEBOOK_TARGET.api_url_env_name,
) -> None:
    parsed = urlsplit(api_url)
    if (
        parsed.scheme.lower() != "https"
        or parsed.hostname != "api.supermetrics.com"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise ConfigurationError(
            f"{api_url_env_name} must be an HTTPS api.supermetrics.com URL "
            "without user info or a fragment"
        )


def load_config(
    env_path: str | os.PathLike[str] = ".env",
    environ: Mapping[str, str] | None = None,
    *,
    api_url_env_name: str = FACEBOOK_TARGET.api_url_env_name,
) -> Config:
    """Load .env values, allowing process environment variables to override them."""

    file_values: dict[str, str] = {}
    path = Path(env_path)
    if path.exists():
        file_values = {
            key: value
            for key, value in dotenv_values(path).items()
            if value is not None
        }

    process_values = dict(os.environ if environ is None else environ)

    def get_value(name: str) -> str | None:
        value = process_values.get(name, file_values.get(name))
        if not isinstance(value, str):
            return value
        # Password whitespace can be significant.  Other config values are
        # identifiers/options and are normalized for predictable validation.
        return value if name == "DB_PASSWORD" else value.strip()

    required_names = (
        api_url_env_name,
        "DB_SERVER",
        "DB_NAME",
        "DB_USER",
        "DB_PASSWORD",
        "DB_DRIVER",
    )
    missing = [name for name in required_names if not get_value(name)]
    if missing:
        raise ConfigurationError(
            "Missing required environment variables: " + ", ".join(missing)
        )

    api_url = get_value(api_url_env_name)
    assert api_url is not None
    _validate_api_url(api_url, api_url_env_name)

    config = Config(
        api_url=api_url,
        db_server=get_value("DB_SERVER") or "",
        db_name=get_value("DB_NAME") or "",
        db_user=get_value("DB_USER") or "",
        db_password=get_value("DB_PASSWORD") or "",
        db_driver=get_value("DB_DRIVER") or "",
        db_encrypt=_parse_bool(
            "DB_ENCRYPT", get_value("DB_ENCRYPT"), default=True
        ),
        db_trust_server_certificate=_parse_bool(
            "DB_TRUST_SERVER_CERTIFICATE",
            get_value("DB_TRUST_SERVER_CERTIFICATE"),
            default=True,
        ),
    )
    for component in (
        config.db_server,
        config.db_name,
        config.db_user,
        config.db_password,
        _normalize_db_driver(config.db_driver),
    ):
        _validate_odbc_component(component)
    return config


def _effective_port(parsed: Any) -> int | None:
    if parsed.port is not None:
        return parsed.port
    return 443 if parsed.scheme.lower() == "https" else None


def _validate_page_url(candidate: str, initial_url: str) -> None:
    parsed = urlsplit(candidate)
    initial = urlsplit(initial_url)
    if (
        parsed.scheme.lower() != "https"
        or parsed.hostname != initial.hostname
        or _effective_port(parsed) != _effective_port(initial)
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise PayloadValidationError(
            "Pagination URL must remain on the original HTTPS origin"
        )


def _api_error_code(response: requests.Response) -> str:
    try:
        payload = response.json()
    except (ValueError, TypeError):
        return "unknown"
    if not isinstance(payload, dict):
        return "unknown"
    error = payload.get("error")
    if not isinstance(error, dict):
        return "unknown"
    code = error.get("code")
    if isinstance(code, (str, int)):
        text = str(code)
        return text[:80] if re.fullmatch(r"[A-Za-z0-9_.:-]+", text) else "unknown"
    return "unknown"


def _log_safe_identifier(value: str | None) -> str:
    """Return a bounded single-token value that cannot inject content into logs."""

    if value is None:
        return "none"
    if not re.fullmatch(r"[A-Za-z0-9_.:-]{1,100}", value):
        return "redacted"
    return value


def _retry_delay(
    response: requests.Response | None,
    retry_number: int,
    now: Callable[[], datetime] | None = None,
) -> float:
    retry_after = response.headers.get("Retry-After") if response is not None else None
    if retry_after:
        try:
            seconds = float(retry_after)
        except ValueError:
            try:
                retry_at = parsedate_to_datetime(retry_after)
                if retry_at.tzinfo is None:
                    retry_at = retry_at.replace(tzinfo=timezone.utc)
                current = (now or (lambda: datetime.now(timezone.utc)))()
                seconds = (retry_at - current).total_seconds()
            except (TypeError, ValueError, OverflowError):
                seconds = -1
        if seconds >= 0:
            return min(seconds, MAX_RETRY_AFTER_SECONDS)

    exponential = min(2 ** (retry_number - 1), 30)
    return exponential + random.uniform(0.0, 0.25)


def _request_json(
    session: requests.Session,
    request_url: str,
    *,
    sleep_fn: Callable[[float], None] = time.sleep,
    logger: logging.Logger = LOGGER,
) -> dict[str, Any]:
    """Request one page with four retries (five total attempts)."""

    for attempt in range(MAX_HTTP_RETRIES + 1):
        response: requests.Response | None = None
        try:
            response = session.get(
                request_url,
                headers={"Accept": "application/json"},
                timeout=HTTP_TIMEOUT,
                allow_redirects=False,
            )
        except (requests.Timeout, requests.ConnectionError):
            if attempt >= MAX_HTTP_RETRIES:
                raise ApiRequestError(
                    "API connection failed after all retry attempts"
                ) from None
            retry_number = attempt + 1
            delay = _retry_delay(None, retry_number)
            logger.warning(
                "api_retry retry=%d/%d reason=connection wait_seconds=%.2f",
                retry_number,
                MAX_HTTP_RETRIES,
                delay,
            )
            sleep_fn(delay)
            continue
        except requests.RequestException:
            raise ApiRequestError("API request failed before receiving a response") from None

        status = response.status_code
        retryable = status == 429 or 500 <= status <= 599
        if retryable and attempt < MAX_HTTP_RETRIES:
            retry_number = attempt + 1
            delay = _retry_delay(response, retry_number)
            logger.warning(
                "api_retry retry=%d/%d status=%d wait_seconds=%.2f",
                retry_number,
                MAX_HTTP_RETRIES,
                status,
                delay,
            )
            response.close()
            sleep_fn(delay)
            continue

        if status < 200 or status >= 300:
            error_code = _api_error_code(response)
            response.close()
            raise ApiRequestError(
                f"API returned HTTP {status} with error_code={error_code}"
            )

        content_type = response.headers.get("Content-Type", "")
        if "json" not in content_type.lower():
            response.close()
            raise ApiRequestError("API response Content-Type is not JSON")

        try:
            payload = response.json()
        except (ValueError, TypeError):
            response.close()
            raise ApiRequestError("API response is not valid JSON") from None
        finally:
            response.close()

        if not isinstance(payload, dict):
            raise ApiRequestError("API JSON root must be an object")
        return payload

    raise ApiRequestError("API request retry loop ended unexpectedly")


def _parse_api_page(
    payload: Mapping[str, Any],
    page_number: int,
    expected_headers: tuple[str, ...] = EXPECTED_HEADERS,
    allow_header_superset: bool = False,
) -> tuple[list[tuple[Any, ...]], str | None, str | None]:
    meta = payload.get("meta")
    if not isinstance(meta, dict):
        raise PayloadValidationError(f"Page {page_number} has no valid meta object")

    status = meta.get("status_code")
    if status not in (200, "200", "SUCCESS", "success"):
        raise PayloadValidationError(
            f"Page {page_number} did not report a successful status"
        )

    data = payload.get("data")
    if not isinstance(data, list) or not data:
        raise PayloadValidationError(f"Page {page_number} has no header row")

    header = data[0]
    if not isinstance(header, list):
        raise PayloadValidationError(
            f"Page {page_number} header does not match the fixed table schema"
        )

    if allow_header_superset:
        if any(header.count(name) != 1 for name in expected_headers):
            raise PayloadValidationError(
                f"Page {page_number} header does not contain each required column once"
            )
        selected_indexes = tuple(header.index(name) for name in expected_headers)
    else:
        if tuple(header) != expected_headers:
            raise PayloadValidationError(
                f"Page {page_number} header does not match the fixed table schema"
            )
        selected_indexes = tuple(range(len(expected_headers)))

    parsed_rows: list[tuple[Any, ...]] = []
    for row_number, row in enumerate(data[1:], start=2):
        if not isinstance(row, list) or len(row) != len(header):
            raise PayloadValidationError(
                f"Page {page_number} row {row_number} has an invalid column count"
            )
        parsed_rows.append(tuple(row[index] for index in selected_indexes))

    request_id_raw = meta.get("request_id")
    request_id = str(request_id_raw) if request_id_raw is not None else None

    paginate = meta.get("paginate")
    if not isinstance(paginate, dict) or "next" not in paginate:
        raise PayloadValidationError(
            f"Page {page_number} has an invalid paginate object"
        )
    next_value = paginate["next"]
    if next_value is not None and not isinstance(next_value, str):
        raise PayloadValidationError(
            f"Page {page_number} has an invalid pagination value"
        )
    if isinstance(next_value, str) and not next_value.strip():
        raise PayloadValidationError(
            f"Page {page_number} has an empty pagination URL"
        )
    next_url = next_value

    return parsed_rows, request_id, next_url


def fetch_all_pages(
    config: Config,
    *,
    session: requests.Session | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
    expected_headers: tuple[str, ...] = EXPECTED_HEADERS,
    allow_header_superset: bool = False,
    logger: logging.Logger = LOGGER,
) -> FetchedBatch:
    own_session = session is None
    active_session = session or requests.Session()
    current_url = config.api_url
    seen_urls: set[str] = set()
    all_rows: list[tuple[Any, ...]] = []
    all_row_request_ids: list[str | None] = []
    first_request_id: str | None = None
    page_count = 0

    try:
        while current_url:
            if page_count >= MAX_PAGES:
                raise PayloadValidationError(
                    f"Pagination exceeded the maximum of {MAX_PAGES} pages"
                )
            _validate_page_url(current_url, config.api_url)
            if current_url in seen_urls:
                raise PayloadValidationError("Pagination URL cycle detected")
            seen_urls.add(current_url)

            payload = _request_json(
                active_session,
                current_url,
                sleep_fn=sleep_fn,
                logger=logger,
            )
            page_count += 1
            page_rows, request_id, next_url = _parse_api_page(
                payload,
                page_count,
                expected_headers,
                allow_header_superset,
            )
            all_rows.extend(page_rows)
            all_row_request_ids.extend([request_id] * len(page_rows))
            if first_request_id is None and request_id:
                first_request_id = request_id

            logger.info(
                "api_page_received page=%d rows=%d request_id=%s",
                page_count,
                len(page_rows),
                _log_safe_identifier(request_id),
            )

            if next_url:
                current_url = urljoin(current_url, next_url)
            else:
                current_url = ""
    finally:
        if own_session:
            active_session.close()

    if not all_rows:
        raise PayloadValidationError("API returned no data rows")

    return FetchedBatch(
        rows=tuple(all_rows),
        request_id=first_request_id,
        page_count=page_count,
        row_request_ids=tuple(all_row_request_ids),
    )


def _nvarchar_units(value: str) -> int:
    return len(value.encode("utf-16-le")) // 2


def _truncate_nvarchar(value: str | None, max_units: int) -> str | None:
    if value is None:
        return None
    result: list[str] = []
    units = 0
    for char in value:
        char_units = _nvarchar_units(char)
        if units + char_units > max_units:
            break
        result.append(char)
        units += char_units
    return "".join(result)


def _is_blank(value: Any) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def _looks_numeric_text(value: str) -> bool:
    try:
        Decimal(value.strip())
    except (InvalidOperation, ValueError):
        return False
    return True


def _parse_bigint(value: Any, field: str, *, allow_blank: bool) -> int | None:
    if _is_blank(value):
        if allow_blank:
            return None
        raise PayloadValidationError(f"{field} cannot be blank")
    if isinstance(value, bool):
        raise PayloadValidationError(f"{field} must be an integer")

    parsed: int
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, Decimal):
        if not value.is_finite() or value != value.to_integral_value():
            raise PayloadValidationError(f"{field} must be an integer")
        parsed = int(value)
    elif isinstance(value, float):
        if not Decimal(str(value)).is_finite() or not value.is_integer():
            raise PayloadValidationError(f"{field} must be an integer")
        parsed = int(value)
    elif isinstance(value, str) and re.fullmatch(r"[+-]?\d+", value.strip()):
        parsed = int(value.strip())
    else:
        raise PayloadValidationError(f"{field} must be an integer")

    if parsed < BIGINT_MIN or parsed > BIGINT_MAX:
        raise PayloadValidationError(f"{field} is outside the SQL BIGINT range")
    return parsed


def _parse_decimal(value: Any, field: str) -> Decimal | None:
    if _is_blank(value):
        return None
    if isinstance(value, bool) or isinstance(value, (dict, list)):
        raise PayloadValidationError(f"{field} must be numeric")

    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value).strip())
    except (InvalidOperation, ValueError, AttributeError):
        raise PayloadValidationError(f"{field} must be numeric") from None

    if not parsed.is_finite():
        raise PayloadValidationError(f"{field} must be a finite number")

    normalized = parsed.normalize() if parsed else Decimal(0)
    _, digits, exponent = normalized.as_tuple()
    if exponent >= 0:
        integer_digits = len(digits) + exponent
        scale = 0
    else:
        scale = -exponent
        integer_digits = max(len(digits) - scale, 0)
    if integer_digits > 28 or scale > 10:
        raise PayloadValidationError(
            f"{field} is outside the SQL DECIMAL(38,10) range"
        )
    return parsed


def _parse_report_date(value: Any) -> date:
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise PayloadValidationError("Date must use YYYY-MM-DD format")
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise PayloadValidationError("Date contains an invalid calendar date") from None


def _parse_text(
    value: Any,
    field: str,
    row_number: int,
    max_units: int,
    *,
    required: bool,
) -> str | None:
    if _is_blank(value):
        if required:
            raise PayloadValidationError(
                f"Row {row_number} {field} must be a non-empty string"
            )
        return None
    text = str(value).strip()
    if _nvarchar_units(text) > max_units:
        raise PayloadValidationError(
            f"Row {row_number} {field} exceeds {max_units} characters"
        )
    return text


def _parse_reach_value(
    value: Any,
    row_number: int,
) -> tuple[int | None, str | None, bool]:
    try:
        return _parse_bigint(value, "Reach", allow_blank=True), None, False
    except PayloadValidationError:
        if not isinstance(value, str) or not value.strip():
            raise
        if _looks_numeric_text(value):
            raise
        if _nvarchar_units(value) > REACH_RAW_MAX_LENGTH:
            raise PayloadValidationError(
                f"Row {row_number} Reach error text exceeds "
                f"{REACH_RAW_MAX_LENGTH} characters"
            ) from None
        return None, value, True


def _prepare_facebook_row(
    raw: tuple[Any, ...],
    row_number: int,
    request_id: str | None,
    loaded_at_utc: datetime,
    run_id: uuid.UUID,
) -> tuple[tuple[Any, ...], tuple[date, str], bool, bool]:
    report_date = _parse_report_date(raw[0])
    account_id = _parse_text(raw[1], "Account ID", row_number, 100, required=False)
    campaign_id = _parse_text(raw[2], "Campaign ID", row_number, 100, required=False)
    campaign_name = _parse_text(
        raw[3], "Campaign name", row_number, CAMPAIGN_MAX_LENGTH, required=True
    )
    campaign_objective = _parse_text(
        raw[4], "Campaign objective", row_number, 200, required=False
    )
    ad_set_id = _parse_text(raw[5], "Ad set ID", row_number, 100, required=False)
    ad_set_name = _parse_text(raw[6], "Ad set name", row_number, 400, required=False)
    ad_id = _parse_text(raw[7], "Ad ID", row_number, 100, required=True)
    assert ad_id is not None
    ad_name = _parse_text(raw[8], "Ad name", row_number, 400, required=True)

    reach, reach_raw, had_reach_error = _parse_reach_value(raw[9], row_number)
    frequency = _parse_decimal(raw[11], "Frequency")

    row = (
        report_date,
        account_id,
        campaign_id,
        campaign_name,
        campaign_objective,
        ad_set_id,
        ad_set_name,
        ad_id,
        ad_name,
        reach,
        reach_raw,
        _parse_bigint(raw[10], "Impressions", allow_blank=True),
        frequency,
        _parse_decimal(raw[12], "CPM (cost per 1000 impressions)"),
        _parse_decimal(raw[13], "CTR (all)"),
        _parse_decimal(raw[14], "Cost"),
        _parse_decimal(raw[15], "Cost per action (CPA)"),
        _parse_bigint(raw[16], "Post comments", allow_blank=True),
        _parse_bigint(raw[17], "Post engagements", allow_blank=True),
        _parse_bigint(raw[18], "Post reactions", allow_blank=True),
        _parse_bigint(raw[19], "Post Saves", allow_blank=True),
        _parse_bigint(raw[20], "Post shares", allow_blank=True),
        _parse_bigint(raw[21], "Video watches at 100%", allow_blank=True),
        _parse_bigint(raw[22], "Video watches at 50%", allow_blank=True),
        _parse_bigint(raw[23], "Video watches at 25%", allow_blank=True),
        _parse_decimal(raw[24], "Purchase conversion value"),
        _parse_decimal(raw[25], "Omni purchase conversion value (shared item)"),
        request_id,
        loaded_at_utc,
        str(run_id),
    )
    return row, (report_date, ad_id), had_reach_error, frequency is None


def _prepare_tiktok_ad_row(
    raw: tuple[Any, ...],
    row_number: int,
    request_id: str | None,
    loaded_at_utc: datetime,
    run_id: uuid.UUID,
) -> tuple[tuple[Any, ...], tuple[date, str], bool, bool]:
    report_date = _parse_report_date(raw[0])
    advertiser_id = _parse_text(raw[1], "Advertiser ID", row_number, 100, required=False)
    campaign_id = _parse_text(raw[2], "Campaign ID", row_number, 100, required=False)
    campaign_name = _parse_text(
        raw[3], "Campaign name", row_number, CAMPAIGN_MAX_LENGTH, required=True
    )
    campaign_objective_type = _parse_text(
        raw[4], "Campaign objective type", row_number, 200, required=False
    )
    ad_group_id = _parse_text(raw[5], "Ad group ID", row_number, 100, required=False)
    ad_group_name = _parse_text(raw[6], "Ad group name", row_number, 400, required=False)
    optimization_goal = _parse_text(
        raw[7], "Optimization goal", row_number, 200, required=False
    )
    ad_id = _parse_text(raw[8], "Ad ID", row_number, 100, required=True)
    assert ad_id is not None
    ad_name = _parse_text(raw[9], "Ad name", row_number, AD_NAME_MAX_LENGTH, required=True)

    reach, reach_raw, had_reach_error = _parse_reach_value(raw[10], row_number)
    frequency = _parse_decimal(raw[12], "Frequency")

    row = (
        report_date,
        advertiser_id,
        campaign_id,
        campaign_name,
        campaign_objective_type,
        ad_group_id,
        ad_group_name,
        optimization_goal,
        ad_id,
        ad_name,
        reach,
        reach_raw,
        _parse_bigint(raw[11], "Impressions", allow_blank=True),
        frequency,
        _parse_decimal(raw[13], "CPM"),
        _parse_decimal(raw[14], "CTR"),
        _parse_decimal(raw[15], "Cost"),
        _parse_bigint(raw[16], "Video views", allow_blank=True),
        _parse_bigint(raw[17], "6-second video views (focused view)", allow_blank=True),
        _parse_bigint(raw[18], "15-second video views (focused view)", allow_blank=True),
        _parse_bigint(raw[19], "Video views at 25%", allow_blank=True),
        _parse_bigint(raw[20], "Video views at 50%", allow_blank=True),
        _parse_bigint(raw[21], "Video views at 75%", allow_blank=True),
        _parse_bigint(raw[22], "Video views at 100%", allow_blank=True),
        _parse_bigint(raw[23], "Results", allow_blank=True),
        _parse_decimal(raw[24], "Cost per result"),
        _parse_bigint(raw[25], "Paid likes", allow_blank=True),
        _parse_bigint(raw[26], "Paid shares", allow_blank=True),
        _parse_bigint(raw[27], "Paid comments", allow_blank=True),
        _parse_bigint(raw[28], "Paid follows", allow_blank=True),
        request_id,
        loaded_at_utc,
        str(run_id),
    )
    return row, (report_date, ad_id), had_reach_error, frequency is None


def prepare_rows(
    fetched: FetchedBatch,
    *,
    run_id: uuid.UUID | None = None,
    loaded_at_utc: datetime | None = None,
    ctr_field_name: str | None = None,
    target: IngestionTarget = FACEBOOK_TARGET,
) -> PreparedBatch:
    active_run_id = run_id or uuid.uuid4()
    active_loaded_at = loaded_at_utc or datetime.now(timezone.utc).replace(
        tzinfo=None
    )
    if active_loaded_at.tzinfo is not None:
        active_loaded_at = active_loaded_at.astimezone(timezone.utc).replace(
            tzinfo=None
        )

    if fetched.row_request_ids and len(fetched.row_request_ids) != len(fetched.rows):
        raise PayloadValidationError(
            "Per-row request metadata does not match the fetched row count"
        )
    prepared: list[tuple[Any, ...]] = []
    seen_keys: set[tuple[Any, ...]] = set()
    reach_error_count = 0
    frequency_null_count = 0

    for row_number, raw in enumerate(fetched.rows, start=1):
        if len(raw) != len(target.expected_headers):
            raise PayloadValidationError(
                f"Row {row_number} does not match the expected source column count"
            )
        row_request_id = (
            fetched.row_request_ids[row_number - 1]
            if fetched.row_request_ids
            else fetched.request_id
        )
        request_id = _truncate_nvarchar(row_request_id, 100)

        if target is FACEBOOK_TARGET:
            prepared_row, key, had_reach_error, had_frequency_null = (
                _prepare_facebook_row(
                    raw,
                    row_number,
                    request_id,
                    active_loaded_at,
                    active_run_id,
                )
            )
            if key in seen_keys:
                raise PayloadValidationError(
                    f"Row {row_number} duplicates an earlier Date and Ad ID key"
                )
            seen_keys.add(key)
            reach_error_count += int(had_reach_error)
            frequency_null_count += int(had_frequency_null)
            prepared.append(prepared_row)
            continue

        if target is TIKTOK_TARGET and len(target.expected_headers) > 8:
            prepared_row, key, had_reach_error, had_frequency_null = (
                _prepare_tiktok_ad_row(
                    raw,
                    row_number,
                    request_id,
                    active_loaded_at,
                    active_run_id,
                )
            )
            if key in seen_keys:
                raise PayloadValidationError(
                    f"Row {row_number} duplicates an earlier Date and Ad ID key"
                )
            seen_keys.add(key)
            reach_error_count += int(had_reach_error)
            frequency_null_count += int(had_frequency_null)
            prepared.append(prepared_row)
            continue

        active_ctr_field_name = ctr_field_name or target.expected_headers[6]
        campaign = raw[0]
        if not isinstance(campaign, str) or not campaign.strip():
            raise PayloadValidationError(
                f"Row {row_number} Campaign name must be a non-empty string"
            )
        if _nvarchar_units(campaign) > CAMPAIGN_MAX_LENGTH:
            raise PayloadValidationError(
                f"Row {row_number} Campaign name exceeds {CAMPAIGN_MAX_LENGTH} characters"
            )

        report_date = _parse_report_date(raw[1])
        key = (report_date, campaign)
        if key in seen_keys:
            raise PayloadValidationError(
                f"Row {row_number} duplicates an earlier Campaign name and Date key"
            )
        seen_keys.add(key)

        try:
            reach = _parse_bigint(raw[2], "Reach", allow_blank=True)
            reach_raw = None
        except PayloadValidationError:
            if not isinstance(raw[2], str) or not raw[2].strip():
                raise
            # Numeric-looking strings must still satisfy integer/range rules;
            # only genuine source error text belongs in reach_raw.
            if _looks_numeric_text(raw[2]):
                raise
            if _nvarchar_units(raw[2]) > REACH_RAW_MAX_LENGTH:
                raise PayloadValidationError(
                    f"Row {row_number} Reach error text exceeds "
                    f"{REACH_RAW_MAX_LENGTH} characters"
                ) from None
            reach = None
            reach_raw = raw[2]
            reach_error_count += 1

        impressions = _parse_bigint(
            raw[3], "Impressions", allow_blank=True
        )
        frequency = _parse_decimal(raw[4], "Frequency")
        if frequency is None:
            frequency_null_count += 1
        cpm = _parse_decimal(raw[5], "CPM")
        ctr_all = _parse_decimal(raw[6], active_ctr_field_name)
        cost = _parse_decimal(raw[7], "Cost")

        prepared.append(
            (
                campaign,
                report_date,
                reach,
                reach_raw,
                impressions,
                frequency,
                cpm,
                ctr_all,
                cost,
                request_id,
                active_loaded_at,
                str(active_run_id),
            )
        )

    return PreparedBatch(
        rows=tuple(prepared),
        run_id=active_run_id,
        loaded_at_utc=active_loaded_at,
        reach_error_count=reach_error_count,
        frequency_null_count=frequency_null_count,
    )


def _odbc_brace(value: str) -> str:
    _validate_odbc_component(value)
    return "{" + value + "}"


def build_connection_string(config: Config) -> str:
    driver = _normalize_db_driver(config.db_driver)
    if not driver:
        raise ConfigurationError("DB_DRIVER cannot be empty")

    parts = (
        f"DRIVER={_odbc_brace(driver)}",
        f"SERVER={_odbc_brace(config.db_server)}",
        f"DATABASE={_odbc_brace(config.db_name)}",
        f"UID={_odbc_brace(config.db_user)}",
        f"PWD={_odbc_brace(config.db_password)}",
        f"Encrypt={'yes' if config.db_encrypt else 'no'}",
        "TrustServerCertificate="
        + ("yes" if config.db_trust_server_certificate else "no"),
        "Connection Timeout=30",
        "APP=Supermetrics Bronze Ingestion",
        # ODBC Driver 17 needs this to discover parameters for local temp tables
        # while pyodbc fast_executemany is enabled.
        "UseFMTONLY=Yes",
    )
    return ";".join(parts) + ";"


def _create_schema_sql(target: IngestionTarget) -> str:
    return f"""
IF SCHEMA_ID(N'{target.schema_name}') IS NULL
    EXEC(N'CREATE SCHEMA [{target.schema_name}]');
"""


def _quote_column(name: str) -> str:
    return f"[{name}]"


def _target_key_columns(target: IngestionTarget) -> tuple[str, ...]:
    if target in (FACEBOOK_TARGET, TIKTOK_TARGET) and len(target.expected_headers) > 8:
        return ("report_date", "ad_id")
    return ("report_date", "campaign_name")


def _target_column_definitions(
    target: IngestionTarget,
) -> tuple[tuple[str, str, tuple[str, str, int, int, int, bool]], ...]:
    if target is FACEBOOK_TARGET:
        return (
            ("report_date", "DATE NOT NULL", ("report_date", "date", 3, 10, 0, False)),
            ("account_id", "NVARCHAR(100) NULL", ("account_id", "nvarchar", 200, 0, 0, True)),
            ("campaign_id", "NVARCHAR(100) NULL", ("campaign_id", "nvarchar", 200, 0, 0, True)),
            ("campaign_name", "NVARCHAR(400) NOT NULL", ("campaign_name", "nvarchar", 800, 0, 0, False)),
            ("campaign_objective", "NVARCHAR(200) NULL", ("campaign_objective", "nvarchar", 400, 0, 0, True)),
            ("ad_set_id", "NVARCHAR(100) NULL", ("ad_set_id", "nvarchar", 200, 0, 0, True)),
            ("ad_set_name", "NVARCHAR(400) NULL", ("ad_set_name", "nvarchar", 800, 0, 0, True)),
            ("ad_id", "NVARCHAR(100) NOT NULL", ("ad_id", "nvarchar", 200, 0, 0, False)),
            ("ad_name", "NVARCHAR(400) NOT NULL", ("ad_name", "nvarchar", 800, 0, 0, False)),
            ("reach", "BIGINT NULL", ("reach", "bigint", 8, 19, 0, True)),
            ("reach_raw", "NVARCHAR(500) NULL", ("reach_raw", "nvarchar", 1000, 0, 0, True)),
            ("impressions", "BIGINT NULL", ("impressions", "bigint", 8, 19, 0, True)),
            ("frequency", "DECIMAL(38,10) NULL", ("frequency", "decimal", 17, 38, 10, True)),
            ("cpm", "DECIMAL(38,10) NULL", ("cpm", "decimal", 17, 38, 10, True)),
            ("ctr_all", "DECIMAL(38,10) NULL", ("ctr_all", "decimal", 17, 38, 10, True)),
            ("cost", "DECIMAL(38,10) NULL", ("cost", "decimal", 17, 38, 10, True)),
            ("cost_per_action_cpa", "DECIMAL(38,10) NULL", ("cost_per_action_cpa", "decimal", 17, 38, 10, True)),
            ("post_comments", "BIGINT NULL", ("post_comments", "bigint", 8, 19, 0, True)),
            ("post_engagements", "BIGINT NULL", ("post_engagements", "bigint", 8, 19, 0, True)),
            ("post_reactions", "BIGINT NULL", ("post_reactions", "bigint", 8, 19, 0, True)),
            ("post_saves", "BIGINT NULL", ("post_saves", "bigint", 8, 19, 0, True)),
            ("post_shares", "BIGINT NULL", ("post_shares", "bigint", 8, 19, 0, True)),
            ("video_watches_100_percent", "BIGINT NULL", ("video_watches_100_percent", "bigint", 8, 19, 0, True)),
            ("video_watches_50_percent", "BIGINT NULL", ("video_watches_50_percent", "bigint", 8, 19, 0, True)),
            ("video_watches_25_percent", "BIGINT NULL", ("video_watches_25_percent", "bigint", 8, 19, 0, True)),
            ("purchase_conversion_value", "DECIMAL(38,10) NULL", ("purchase_conversion_value", "decimal", 17, 38, 10, True)),
            ("omni_purchase_conversion_value_shared_item", "DECIMAL(38,10) NULL", ("omni_purchase_conversion_value_shared_item", "decimal", 17, 38, 10, True)),
            ("source_request_id", "NVARCHAR(100) NULL", ("source_request_id", "nvarchar", 200, 0, 0, True)),
            ("loaded_at_utc", "DATETIME2(3) NOT NULL", ("loaded_at_utc", "datetime2", 7, -1, 3, False)),
            ("run_id", "UNIQUEIDENTIFIER NOT NULL", ("run_id", "uniqueidentifier", 16, 0, 0, False)),
        )

    if target is TIKTOK_TARGET and len(target.expected_headers) > 8:
        return (
            ("report_date", "DATE NOT NULL", ("report_date", "date", 3, 10, 0, False)),
            ("advertiser_id", "NVARCHAR(100) NULL", ("advertiser_id", "nvarchar", 200, 0, 0, True)),
            ("campaign_id", "NVARCHAR(100) NULL", ("campaign_id", "nvarchar", 200, 0, 0, True)),
            ("campaign_name", "NVARCHAR(400) NOT NULL", ("campaign_name", "nvarchar", 800, 0, 0, False)),
            ("campaign_objective_type", "NVARCHAR(200) NULL", ("campaign_objective_type", "nvarchar", 400, 0, 0, True)),
            ("ad_group_id", "NVARCHAR(100) NULL", ("ad_group_id", "nvarchar", 200, 0, 0, True)),
            ("ad_group_name", "NVARCHAR(400) NULL", ("ad_group_name", "nvarchar", 800, 0, 0, True)),
            ("optimization_goal", "NVARCHAR(200) NULL", ("optimization_goal", "nvarchar", 400, 0, 0, True)),
            ("ad_id", "NVARCHAR(100) NOT NULL", ("ad_id", "nvarchar", 200, 0, 0, False)),
            ("ad_name", "NVARCHAR(1000) NOT NULL", ("ad_name", "nvarchar", 2000, 0, 0, False)),
            ("reach", "BIGINT NULL", ("reach", "bigint", 8, 19, 0, True)),
            ("reach_raw", "NVARCHAR(500) NULL", ("reach_raw", "nvarchar", 1000, 0, 0, True)),
            ("impressions", "BIGINT NULL", ("impressions", "bigint", 8, 19, 0, True)),
            ("frequency", "DECIMAL(38,10) NULL", ("frequency", "decimal", 17, 38, 10, True)),
            ("cpm", "DECIMAL(38,10) NULL", ("cpm", "decimal", 17, 38, 10, True)),
            ("ctr", "DECIMAL(38,10) NULL", ("ctr", "decimal", 17, 38, 10, True)),
            ("cost", "DECIMAL(38,10) NULL", ("cost", "decimal", 17, 38, 10, True)),
            ("video_views", "BIGINT NULL", ("video_views", "bigint", 8, 19, 0, True)),
            ("six_second_video_views_focused_view", "BIGINT NULL", ("six_second_video_views_focused_view", "bigint", 8, 19, 0, True)),
            ("fifteen_second_video_views_focused_view", "BIGINT NULL", ("fifteen_second_video_views_focused_view", "bigint", 8, 19, 0, True)),
            ("video_views_25_percent", "BIGINT NULL", ("video_views_25_percent", "bigint", 8, 19, 0, True)),
            ("video_views_50_percent", "BIGINT NULL", ("video_views_50_percent", "bigint", 8, 19, 0, True)),
            ("video_views_75_percent", "BIGINT NULL", ("video_views_75_percent", "bigint", 8, 19, 0, True)),
            ("video_views_100_percent", "BIGINT NULL", ("video_views_100_percent", "bigint", 8, 19, 0, True)),
            ("results", "BIGINT NULL", ("results", "bigint", 8, 19, 0, True)),
            ("cost_per_result", "DECIMAL(38,10) NULL", ("cost_per_result", "decimal", 17, 38, 10, True)),
            ("paid_likes", "BIGINT NULL", ("paid_likes", "bigint", 8, 19, 0, True)),
            ("paid_shares", "BIGINT NULL", ("paid_shares", "bigint", 8, 19, 0, True)),
            ("paid_comments", "BIGINT NULL", ("paid_comments", "bigint", 8, 19, 0, True)),
            ("paid_follows", "BIGINT NULL", ("paid_follows", "bigint", 8, 19, 0, True)),
            ("source_request_id", "NVARCHAR(100) NULL", ("source_request_id", "nvarchar", 200, 0, 0, True)),
            ("loaded_at_utc", "DATETIME2(3) NOT NULL", ("loaded_at_utc", "datetime2", 7, -1, 3, False)),
            ("run_id", "UNIQUEIDENTIFIER NOT NULL", ("run_id", "uniqueidentifier", 16, 0, 0, False)),
        )

    return (
        ("campaign_name", "NVARCHAR(400) NOT NULL", ("campaign_name", "nvarchar", 800, 0, 0, False)),
        ("report_date", "DATE NOT NULL", ("report_date", "date", 3, 10, 0, False)),
        ("reach", "BIGINT NULL", ("reach", "bigint", 8, 19, 0, True)),
        ("reach_raw", "NVARCHAR(500) NULL", ("reach_raw", "nvarchar", 1000, 0, 0, True)),
        ("impressions", "BIGINT NULL", ("impressions", "bigint", 8, 19, 0, True)),
        ("frequency", "DECIMAL(38,10) NULL", ("frequency", "decimal", 17, 38, 10, True)),
        ("cpm", "DECIMAL(38,10) NULL", ("cpm", "decimal", 17, 38, 10, True)),
        (target.ctr_column, "DECIMAL(38,10) NULL", (target.ctr_column, "decimal", 17, 38, 10, True)),
        ("cost", "DECIMAL(38,10) NULL", ("cost", "decimal", 17, 38, 10, True)),
        ("source_request_id", "NVARCHAR(100) NULL", ("source_request_id", "nvarchar", 200, 0, 0, True)),
        ("loaded_at_utc", "DATETIME2(3) NOT NULL", ("loaded_at_utc", "datetime2", 7, -1, 3, False)),
        ("run_id", "UNIQUEIDENTIFIER NOT NULL", ("run_id", "uniqueidentifier", 16, 0, 0, False)),
    )


def _target_column_names(target: IngestionTarget) -> tuple[str, ...]:
    return tuple(column[0] for column in _target_column_definitions(target))


def _create_table_sql(target: IngestionTarget) -> str:
    column_lines = ",\n        ".join(
        f"{_quote_column(name)} {sql_type}"
        for name, sql_type, _expected in _target_column_definitions(target)
    )
    key_columns = ", ".join(_quote_column(name) for name in _target_key_columns(target))
    return f"""
IF OBJECT_ID(N'{target.schema_name}.{target.table_name}', N'U') IS NULL
BEGIN
    CREATE TABLE {target.qualified_table} (
        {column_lines},
        CONSTRAINT [{target.primary_key_name}]
            PRIMARY KEY CLUSTERED ({key_columns})
    );
END;
"""


def _create_stage_sql(target: IngestionTarget) -> str:
    columns = ", ".join(_quote_column(name) for name in _target_column_names(target))
    key_columns = ", ".join(_quote_column(name) for name in _target_key_columns(target))
    return f"""
SELECT TOP (0)
    {columns}
INTO #supermetrics_stage
FROM {target.qualified_table};

ALTER TABLE #supermetrics_stage
    ADD PRIMARY KEY ({key_columns});
"""


def _insert_stage_sql(target: IngestionTarget) -> str:
    columns = ", ".join(_quote_column(name) for name in _target_column_names(target))
    placeholders = ", ".join("?" for _name in _target_column_names(target))
    return f"""
INSERT INTO #supermetrics_stage (
    {columns}
) VALUES ({placeholders});
"""


def _update_target_sql(target: IngestionTarget) -> str:
    key_columns = _target_key_columns(target)
    set_lines = ",\n    ".join(
        f"{_quote_column(name)} = source.{_quote_column(name)}"
        for name in _target_column_names(target)
        if name not in key_columns
    )
    join_conditions = "\n   AND ".join(
        f"source.{_quote_column(name)} = target.{_quote_column(name)}"
        for name in key_columns
    )
    return f"""
DECLARE @updated BIGINT;
UPDATE target
SET
    {set_lines}
FROM {target.qualified_table} AS target
INNER JOIN #supermetrics_stage AS source
    ON {join_conditions};
SET @updated = @@ROWCOUNT;
SELECT @updated;
"""


def _insert_target_sql(target: IngestionTarget) -> str:
    columns = ", ".join(_quote_column(name) for name in _target_column_names(target))
    source_columns = ", ".join(
        f"source.{_quote_column(name)}" for name in _target_column_names(target)
    )
    join_conditions = "\n      AND ".join(
        f"target.{_quote_column(name)} = source.{_quote_column(name)}"
        for name in _target_key_columns(target)
    )
    return f"""
DECLARE @inserted BIGINT;
INSERT INTO {target.qualified_table} (
    {columns}
)
SELECT
    {source_columns}
FROM #supermetrics_stage AS source
WHERE NOT EXISTS (
    SELECT 1
    FROM {target.qualified_table} AS target WITH (UPDLOCK, HOLDLOCK)
    WHERE {join_conditions}
);
SET @inserted = @@ROWCOUNT;
SELECT @inserted;
"""


def _expected_db_columns(
    target: IngestionTarget,
) -> tuple[tuple[str, str, int, int, int, bool], ...]:
    return tuple(column[2] for column in _target_column_definitions(target))


# Backwards-compatible Facebook SQL constants for existing callers and tests.
CREATE_SCHEMA_SQL = _create_schema_sql(FACEBOOK_TARGET)
CREATE_TABLE_SQL = _create_table_sql(FACEBOOK_TARGET)
CREATE_STAGE_SQL = _create_stage_sql(FACEBOOK_TARGET)
INSERT_STAGE_SQL = _insert_stage_sql(FACEBOOK_TARGET)
UPDATE_TARGET_SQL = _update_target_sql(FACEBOOK_TARGET)
INSERT_TARGET_SQL = _insert_target_sql(FACEBOOK_TARGET)
EXPECTED_DB_COLUMNS = _expected_db_columns(FACEBOOK_TARGET)


def _validate_table_schema(
    cursor: pyodbc.Cursor,
    target: IngestionTarget = FACEBOOK_TARGET,
) -> None:
    expected_db_columns = _expected_db_columns(target)
    cursor.execute(
        f"""
        SELECT
            c.[name], TYPE_NAME(c.[user_type_id]), c.[max_length],
            c.[precision], c.[scale], c.[is_nullable],
            c.[is_identity], c.[is_computed]
        FROM sys.columns AS c
        WHERE c.[object_id] = OBJECT_ID(
            N'{target.schema_name}.{target.table_name}', N'U'
        )
        ORDER BY c.[column_id];
        """
    )
    actual_rows = cursor.fetchall()
    if len(actual_rows) != len(expected_db_columns):
        raise DatabaseLoadError(
            "Target table column count does not match the required schema"
        )

    for actual, expected in zip(actual_rows, expected_db_columns):
        actual_name = str(actual[0])
        actual_type = str(actual[1]).lower()
        actual_max_length = int(actual[2])
        actual_precision = int(actual[3])
        actual_scale = int(actual[4])
        actual_nullable = bool(actual[5])
        actual_identity = bool(actual[6])
        actual_computed = bool(actual[7])
        (
            expected_name,
            expected_type,
            expected_max_length,
            expected_precision,
            expected_scale,
            expected_nullable,
        ) = expected

        precision_matches = (
            expected_precision < 0 or actual_precision == expected_precision
        )
        if (
            actual_name != expected_name
            or actual_type != expected_type
            or actual_max_length != expected_max_length
            or not precision_matches
            or actual_scale != expected_scale
            or actual_nullable != expected_nullable
            or actual_identity
            or actual_computed
        ):
            raise DatabaseLoadError(
                f"Target column {expected_name} does not match the required schema"
            )

    cursor.execute(
        f"""
        SELECT c.[name]
        FROM sys.key_constraints AS kc
        INNER JOIN sys.index_columns AS ic
            ON ic.[object_id] = kc.[parent_object_id]
           AND ic.[index_id] = kc.[unique_index_id]
        INNER JOIN sys.columns AS c
            ON c.[object_id] = ic.[object_id]
           AND c.[column_id] = ic.[column_id]
        WHERE kc.[type] = 'PK'
          AND kc.[parent_object_id] = OBJECT_ID(
              N'{target.schema_name}.{target.table_name}', N'U'
          )
          AND ic.[key_ordinal] > 0
        ORDER BY ic.[key_ordinal];
        """
    )
    primary_key = tuple(str(row[0]) for row in cursor.fetchall())
    if primary_key != _target_key_columns(target):
        raise DatabaseLoadError("Target table primary key does not match the plan")


def _rollback_quietly(connection: Any) -> None:
    try:
        connection.rollback()
    except Exception:
        pass


def load_rows(
    config: Config,
    batch: PreparedBatch,
    *,
    connect_fn: Callable[..., Any] | None = None,
    target: IngestionTarget = FACEBOOK_TARGET,
) -> LoadResult:
    if not batch.rows:
        raise PayloadValidationError("No prepared rows are available for loading")

    connector = connect_fn or pyodbc.connect
    connection: Any = None
    cursor: Any = None
    try:
        connection = connector(
            build_connection_string(config), autocommit=False, timeout=30
        )
        cursor = connection.cursor()
        cursor.execute("SET XACT_ABORT ON; SET NOCOUNT ON;")
        cursor.execute(
            """
            DECLARE @lock_result INT;
            EXEC @lock_result = sys.sp_getapplock
                @Resource = ?,
                @LockMode = 'Exclusive',
                @LockOwner = 'Transaction',
                @LockTimeout = 60000;
            SELECT @lock_result;
            """,
            target.app_lock_resource,
        )
        lock_row = cursor.fetchone()
        if lock_row is None or int(lock_row[0]) < 0:
            raise DatabaseLoadError("Could not acquire the ingestion application lock")

        # CREATE SCHEMA must complete as its own batch before SQL Server compiles
        # the CREATE TABLE batch that references it.
        cursor.execute(_create_schema_sql(target))
        cursor.execute(_create_table_sql(target))
        _validate_table_schema(cursor, target)
        cursor.execute(_create_stage_sql(target))
        cursor.fast_executemany = True
        cursor.executemany(_insert_stage_sql(target), batch.rows)

        cursor.execute(_update_target_sql(target))
        updated_row = cursor.fetchone()
        updated = int(updated_row[0]) if updated_row is not None else 0

        cursor.execute(_insert_target_sql(target))
        inserted_row = cursor.fetchone()
        inserted = int(inserted_row[0]) if inserted_row is not None else 0

        cursor.execute(f"SELECT COUNT_BIG(*) FROM {target.qualified_table};")
        total_row = cursor.fetchone()
        target_total = int(total_row[0]) if total_row is not None else 0
        connection.commit()
        return LoadResult(
            inserted=inserted,
            updated=updated,
            target_total=target_total,
        )
    except IngestionError:
        if connection is not None:
            _rollback_quietly(connection)
        raise
    except Exception:
        if connection is not None:
            _rollback_quietly(connection)
        raise DatabaseLoadError(
            "Database transaction failed and was rolled back"
        ) from None
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def main() -> int:
    configure_logging()
    started = time.perf_counter()
    run_id = uuid.uuid4()
    LOGGER.info("ingestion_started run_id=%s", run_id)

    try:
        config = load_config()
        fetched = fetch_all_pages(config)
        batch = prepare_rows(fetched, run_id=run_id)
        LOGGER.info(
            "payload_validated run_id=%s pages=%d rows=%d "
            "reach_errors=%d frequency_nulls=%d",
            run_id,
            fetched.page_count,
            len(batch.rows),
            batch.reach_error_count,
            batch.frequency_null_count,
        )
        result = load_rows(config, batch)
    except IngestionError as exc:
        LOGGER.error(
            "ingestion_failed run_id=%s error_type=%s message=%s duration_seconds=%.2f",
            run_id,
            type(exc).__name__,
            str(exc),
            time.perf_counter() - started,
        )
        return 1
    except Exception:
        # Never log an unexpected exception object: third-party exceptions can
        # contain the signed URL or full ODBC connection string.
        LOGGER.error(
            "ingestion_failed run_id=%s error_type=UnexpectedError "
            "message=Unexpected_internal_error duration_seconds=%.2f",
            run_id,
            time.perf_counter() - started,
        )
        return 1

    LOGGER.info(
        "ingestion_succeeded run_id=%s request_id=%s fetched=%d inserted=%d "
        "updated=%d target_total=%d duration_seconds=%.2f",
        run_id,
        _log_safe_identifier(fetched.request_id),
        len(batch.rows),
        result.inserted,
        result.updated,
        result.target_total,
        time.perf_counter() - started,
    )
    return 0



EMBEDDED_SUPERMETRICS_API_URL = "https://api.supermetrics.com/enterprise/query/s/2215c812acd311f189d142010a0820226357ee3f86fad8d9eb9ab41a5311c16d/json"


def load_config(
    env_path: str | os.PathLike[str] = ".env",
    environ: Mapping[str, str] | None = None,
    *,
    api_url_env_name: str = 'SUPERMETRICS_TIKTOK_API_URL',
) -> Config:
    """Load the embedded Supermetrics URL plus DB settings from env/.env."""

    file_values: dict[str, str] = {}
    path = Path(env_path)
    if path.exists():
        file_values = {
            key: value
            for key, value in dotenv_values(path).items()
            if value is not None
        }

    process_values = dict(os.environ if environ is None else environ)

    def get_value(name: str) -> str | None:
        value = process_values.get(name, file_values.get(name))
        if not isinstance(value, str):
            return value
        return value if name == "DB_PASSWORD" else value.strip()

    required_names = (
        "DB_SERVER",
        "DB_NAME",
        "DB_USER",
        "DB_PASSWORD",
        "DB_DRIVER",
    )
    missing = [name for name in required_names if not get_value(name)]
    if missing:
        raise ConfigurationError(
            "Missing required environment variables: " + ", ".join(missing)
        )

    api_url = EMBEDDED_SUPERMETRICS_API_URL
    _validate_api_url(api_url, api_url_env_name)

    config = Config(
        api_url=api_url,
        db_server=get_value("DB_SERVER") or "",
        db_name=get_value("DB_NAME") or "",
        db_user=get_value("DB_USER") or "",
        db_password=get_value("DB_PASSWORD") or "",
        db_driver=get_value("DB_DRIVER") or "",
        db_encrypt=_parse_bool(
            "DB_ENCRYPT", get_value("DB_ENCRYPT"), default=True
        ),
        db_trust_server_certificate=_parse_bool(
            "DB_TRUST_SERVER_CERTIFICATE",
            get_value("DB_TRUST_SERVER_CERTIFICATE"),
            default=True,
        ),
    )
    for component in (
        config.db_server,
        config.db_name,
        config.db_user,
        config.db_password,
        _normalize_db_driver(config.db_driver),
    ):
        _validate_odbc_component(component)
    return config

LOGGER = logging.getLogger("supermetrics_tiktok_ingestion")
TARGET = TIKTOK_TARGET


def main() -> int:
    configure_logging()
    started = time.perf_counter()
    run_id = uuid.uuid4()
    LOGGER.info("ingestion_started source=tiktok run_id=%s", run_id)

    try:
        config = load_config(api_url_env_name=TARGET.api_url_env_name)
        fetched = fetch_all_pages(
            config,
            expected_headers=TARGET.expected_headers,
            allow_header_superset=True,
            logger=LOGGER,
        )
        batch = prepare_rows(fetched, run_id=run_id, target=TARGET)
        LOGGER.info(
            "payload_validated source=tiktok run_id=%s pages=%d rows=%d "
            "reach_errors=%d frequency_nulls=%d",
            run_id,
            fetched.page_count,
            len(batch.rows),
            batch.reach_error_count,
            batch.frequency_null_count,
        )
        result = load_rows(config, batch, target=TARGET)
    except IngestionError as exc:
        LOGGER.error(
            "ingestion_failed source=tiktok run_id=%s error_type=%s "
            "message=%s duration_seconds=%.2f",
            run_id,
            type(exc).__name__,
            str(exc),
            time.perf_counter() - started,
        )
        return 1
    except Exception:
        LOGGER.error(
            "ingestion_failed source=tiktok run_id=%s error_type=UnexpectedError "
            "message=Unexpected_internal_error duration_seconds=%.2f",
            run_id,
            time.perf_counter() - started,
        )
        return 1

    LOGGER.info(
        "ingestion_succeeded source=tiktok run_id=%s request_id=%s fetched=%d "
        "inserted=%d updated=%d target_total=%d duration_seconds=%.2f",
        run_id,
        _log_safe_identifier(fetched.request_id),
        len(batch.rows),
        result.inserted,
        result.updated,
        result.target_total,
        time.perf_counter() - started,
    )
    return 0


def run() -> int:
    return main()


if __name__ == "__main__":
    sys.exit(run())
