"""
Diagnose_BC_Entry.py — Print what BC OData actually returns for a single
Item Ledger Entry. Use this to compare against BC UI / SQL row when values
look mismatched.

Usage (inside the airflow container):
  docker exec airflow-webserver python /opt/airflow/dags/Diagnose_BC_Entry.py 9151960
"""
import json
import sys
from urllib.parse import quote

from _base_etl import load_configs, get_session, get_access_token


KEY_FIELDS = [
    "Entry_No",
    "Posting_Date",
    "Entry_Type",
    "Document_Type",
    "Document_No",
    "Item_No",
    "Quantity",
    "Invoiced_Quantity",
    "Remaining_Quantity",
    "Sales_Amount_Expected",
    "Sales_Amount_Actual",
    "Cost_Amount_Expected",
    "Cost_Amount_Actual",
    "Cost_Amount_Non_Invtbl",
    "Cost_Amount_Expected_ACY",
    "Cost_Amount_Actual_ACY",
    "Completely_Invoiced",
    "Open",
]


def main(entry_no: str) -> None:
    bc_cfg, _ = load_configs()
    session = get_session()
    token = get_access_token(session, bc_cfg)
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    company = quote(bc_cfg["company"])
    url = (
        f"{bc_cfg['resource']}/v2.0/{bc_cfg['tenant_id']}"
        f"/{bc_cfg['environment']}/ODataV4/Company('{company}')/Item_Ledger_Entries"
        f"?$filter=Entry_No eq {entry_no}"
    )

    print(f"GET {url}\n")
    resp = session.get(url, headers=headers, timeout=60)
    resp.raise_for_status()

    records = resp.json().get("value", [])
    if not records:
        print(f"No record found for Entry_No={entry_no}")
        return

    for rec in records:
        focus = {k: rec.get(k) for k in KEY_FIELDS}
        print("=== Key fields (what OData returns) ===")
        print(json.dumps(focus, indent=2, ensure_ascii=False, default=str))
        print("\n=== Full record ===")
        print(json.dumps(rec, indent=2, ensure_ascii=False, default=str))
        print()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python Diagnose_BC_Entry.py <Entry_No>")
        sys.exit(1)
    main(sys.argv[1])
