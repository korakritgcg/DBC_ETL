"""
Test_Insert_Pipeline.py — End-to-end test of the ETL insert path for ONE entry.

Fetches Entry_No 9151960 from BC OData, runs it through the exact same
DataFrame -> coerce -> staging clone -> to_sql logic that the production
pipeline uses, then queries the result back. If the values stored differ
from what OData returned, the bug is in the insert path.

Creates and drops a temporary table [raw].[_test_diag_9151960].
Safe to re-run; cleans up in finally.
"""
import json
import urllib.parse
from urllib.parse import quote

import pandas as pd
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.types import NVARCHAR


SCHEMA = "raw"
SOURCE = "Item_Ledger_Entries"
TEST_TABLE = "_test_diag_9151960"
ENTRY_NO = 9151960

_SQL_STRING_TYPES = {
    "nvarchar", "varchar", "char", "nchar", "text", "ntext", "sql_variant",
    "xml", "varbinary", "binary", "image",
}


def _coerce(series: pd.Series, sql_type: str) -> pd.Series:
    t = (sql_type or "").lower()
    if t in ("date", "datetime2", "datetimeoffset"):
        return pd.to_datetime(series, errors="coerce")
    if t in ("datetime", "smalldatetime"):
        d = pd.to_datetime(series, errors="coerce")
        if t == "smalldatetime":
            lo, hi = pd.Timestamp("1900-01-01"), pd.Timestamp("2079-06-06")
        else:
            lo, hi = pd.Timestamp("1753-01-01"), pd.Timestamp("9999-12-31")
        return d.where((d >= lo) & (d <= hi), pd.NaT)
    if t == "time":
        return pd.to_datetime(series, errors="coerce")
    if t in ("tinyint", "smallint", "int", "bigint",
             "decimal", "numeric", "money", "smallmoney",
             "float", "real"):
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
    return series


def main() -> None:
    with open("_config_DBC.json", encoding="utf-8") as f:
        bc_cfg = json.load(f)
    with open("_config_sql.json", encoding="utf-8") as f:
        sql_cfg = json.load(f)

    # Engine (use Driver 17 locally; production uses 18 but binding is the same)
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={sql_cfg['server']},{sql_cfg['port']};"
        f"DATABASE={sql_cfg['database']};"
        f"UID={sql_cfg['username']};"
        f"PWD={sql_cfg['password']};"
        "TrustServerCertificate=yes;"
        "Unicode_Results=Yes;"
    )
    engine = create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        fast_executemany=True,
        pool_pre_ping=True,
    )

    # OAuth token
    token_resp = requests.post(
        f"https://login.microsoftonline.com/{bc_cfg['tenant_id']}/oauth2/token",
        data={
            "grant_type": "client_credentials",
            "client_id": bc_cfg["client_id"],
            "client_secret": bc_cfg["client_secret"],
            "resource": bc_cfg["resource"],
        },
        timeout=30,
    )
    token = token_resp.json()["access_token"]

    # OData fetch
    company = quote(bc_cfg["company"])
    url = (
        f"{bc_cfg['resource']}/v2.0/{bc_cfg['tenant_id']}"
        f"/{bc_cfg['environment']}/ODataV4/Company('{company}')/{SOURCE}"
        f"?$filter=Entry_No eq {ENTRY_NO}"
    )
    resp = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        timeout=60,
    )
    records = resp.json().get("value", [])
    if not records:
        print(f"No record found for Entry_No={ENTRY_NO}")
        return

    rec = records[0]
    focus = ["Sales_Amount_Expected", "Sales_Amount_Actual",
             "Cost_Amount_Expected", "Cost_Amount_Actual"]

    print("\n[1] === OData raw values ===")
    for k in focus:
        print(f"    {k}: {rec.get(k)}")

    # Get SQL columns + types
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT COLUMN_NAME, DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table
                ORDER BY ORDINAL_POSITION
                """
            ),
            {"schema": SCHEMA, "table": SOURCE},
        ).fetchall()

    sql_columns = [r[0] for r in rows]
    col_types = {r[0]: (r[1] or "").lower() for r in rows}

    print("\n[2] === SQL column data types for focus fields ===")
    for k in focus:
        print(f"    {k}: {col_types.get(k)}")

    # Build df + filter + coerce (same as pipeline)
    df = pd.DataFrame(records)
    df = df.loc[:, ~df.columns.astype(str).str.startswith("@")]
    valid_cols = [c for c in df.columns if c in sql_columns]
    df = df[valid_cols]
    for col in list(df.columns):
        t = col_types.get(col)
        if t and t not in _SQL_STRING_TYPES:
            df[col] = _coerce(df[col], t)

    print("\n[3] === Values in pandas DataFrame just before to_sql ===")
    for k in focus:
        if k in df.columns:
            print(f"    {k}: {df[k].iloc[0]}  (dtype={df[k].dtype})")

    try:
        # Create test table cloned from real table structure
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS [{SCHEMA}].[{TEST_TABLE}]"))
            conn.execute(
                text(
                    f"SELECT TOP 0 * INTO [{SCHEMA}].[{TEST_TABLE}] "
                    f"FROM [{SCHEMA}].[{SOURCE}]"
                )
            )

        # Manual parameterized INSERT (mimics what pandas to_sql does
        # internally — bind values to named columns via SQLAlchemy).
        cols = list(df.columns)
        col_str = ", ".join(f"[{c}]" for c in cols)
        placeholders = ", ".join(f":{c}" for c in cols)
        insert_sql = text(
            f"INSERT INTO [{SCHEMA}].[{TEST_TABLE}] ({col_str}) "
            f"VALUES ({placeholders})"
        )

        records_to_insert = df.to_dict(orient="records")
        for r in records_to_insert:
            for k, v in list(r.items()):
                if v is None:
                    continue
                if isinstance(v, float) and pd.isna(v):
                    r[k] = None
                elif hasattr(v, "item"):
                    r[k] = v.item()

        with engine.begin() as conn:
            conn.execute(insert_sql, records_to_insert)

        # Query back from the test table
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    f"""
                    SELECT [Sales_Amount_Expected], [Sales_Amount_Actual],
                           [Cost_Amount_Expected],  [Cost_Amount_Actual]
                    FROM [{SCHEMA}].[{TEST_TABLE}]
                    WHERE [Entry_No] = :en
                    """
                ),
                {"en": ENTRY_NO},
            ).fetchone()

        print("\n[4] === Values queried back from [raw].[_test_diag_9151960] ===")
        print(f"    Sales_Amount_Expected: {row[0]}")
        print(f"    Sales_Amount_Actual:   {row[1]}")
        print(f"    Cost_Amount_Expected:  {row[2]}")
        print(f"    Cost_Amount_Actual:    {row[3]}")

        # Compare
        print("\n[5] === Verdict ===")
        odata = {k: rec.get(k) for k in focus}
        stored = {
            "Sales_Amount_Expected": float(row[0] or 0),
            "Sales_Amount_Actual":   float(row[1] or 0),
            "Cost_Amount_Expected":  float(row[2] or 0),
            "Cost_Amount_Actual":    float(row[3] or 0),
        }
        mismatch = []
        for k in focus:
            o = float(odata.get(k) or 0)
            s = stored.get(k, 0)
            if abs(o - s) > 0.001:
                mismatch.append(f"{k}: OData={o} vs stored={s}")
        if mismatch:
            print("    MISMATCH — insert path IS swapping/altering values:")
            for m in mismatch:
                print(f"      - {m}")
        else:
            print("    OK — insert path stored exactly what OData returned. "
                  "No swap in the pipeline.")

    finally:
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS [{SCHEMA}].[{TEST_TABLE}]"))
        print("\n[*] Test table [raw].[_test_diag_9151960] dropped.")


if __name__ == "__main__":
    main()
