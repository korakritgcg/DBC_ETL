"""Sweep a range of Entry_Nos around 9151960 to see if staleness is localized."""
import json
import urllib.parse
from urllib.parse import quote

import requests
from sqlalchemy import create_engine, text


with open("_config_sql.json", encoding="utf-8") as f:
    sql_cfg = json.load(f)
with open("_config_DBC.json", encoding="utf-8") as f:
    bc_cfg = json.load(f)

odbc = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={sql_cfg['server']},{sql_cfg['port']};"
    f"DATABASE={sql_cfg['database']};"
    f"UID={sql_cfg['username']};"
    f"PWD={sql_cfg['password']};"
    "TrustServerCertificate=yes;"
)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(odbc)}")

token = requests.post(
    f"https://login.microsoftonline.com/{bc_cfg['tenant_id']}/oauth2/token",
    data={
        "grant_type": "client_credentials",
        "client_id": bc_cfg["client_id"],
        "client_secret": bc_cfg["client_secret"],
        "resource": bc_cfg["resource"],
    },
    timeout=30,
).json()["access_token"]

company = quote(bc_cfg["company"])
hdr = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

# Fetch a range from OData
LO, HI = 9151950, 9151970
url = (
    f"{bc_cfg['resource']}/v2.0/{bc_cfg['tenant_id']}"
    f"/{bc_cfg['environment']}/ODataV4/Company('{company}')/Item_Ledger_Entries"
    f"?$filter=Entry_No ge {LO} and Entry_No le {HI}&$orderby=Entry_No asc"
)
od_records = requests.get(url, headers=hdr, timeout=60).json()["value"]
od_map = {
    r["Entry_No"]: (
        r.get("Sales_Amount_Expected"), r.get("Sales_Amount_Actual"),
        r.get("Cost_Amount_Expected"),  r.get("Cost_Amount_Actual"),
    )
    for r in od_records
}

# Fetch same range from SQL
with engine.connect() as conn:
    sql_rows = conn.execute(
        text(
            f"""SELECT Entry_No, Sales_Amount_Expected, Sales_Amount_Actual,
                       Cost_Amount_Expected,  Cost_Amount_Actual
                FROM raw.Item_Ledger_Entries
                WHERE Entry_No BETWEEN {LO} AND {HI}
                ORDER BY Entry_No"""
        )
    ).fetchall()
sql_map = {r[0]: tuple(float(x) for x in r[1:]) for r in sql_rows}

print(f"{'Entry_No':>10} | {'OData (Exp,Act / Exp,Act)':<50} | {'SQL (Exp,Act / Exp,Act)':<50} | Status")
print("-" * 140)
all_keys = sorted(set(od_map.keys()) | set(sql_map.keys()))
for k in all_keys:
    o = od_map.get(k)
    s = sql_map.get(k)
    o_str = f"{o[0]},{o[1]} / {o[2]},{o[3]}" if o else "NOT IN ODATA"
    s_str = f"{s[0]},{s[1]} / {s[2]},{s[3]}" if s else "NOT IN SQL"
    if o and s and all(abs((o[i] or 0) - s[i]) < 0.001 for i in range(4)):
        status = "OK"
    elif o and s:
        # Check if simple swap
        if (abs((o[0] or 0) - s[1]) < 0.001 and abs((o[1] or 0) - s[0]) < 0.001):
            status = "SWAPPED (Exp<->Act)"
        else:
            status = "DIFFERENT"
    else:
        status = "MISSING"
    print(f"{k:>10} | {o_str:<50} | {s_str:<50} | {status}")
