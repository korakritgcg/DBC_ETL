"""
Diagnose_Purchase_Order_List.py — Find out WHY the Purchase_Order_List
OData V4 endpoint returns 404 even though the web service shows as published.

Usage (inside the airflow container):
  docker exec airflow-webserver python /opt/airflow/dags/Diagnose_Purchase_Order_List.py
"""
import json
from urllib.parse import quote

from _base_etl import load_configs, get_session, get_access_token

PAGE = "Purchase_Order_List"


def _show(resp, label, limit=2000):
    print(f"--- {label} ---")
    print(f"HTTP {resp.status_code} {resp.reason}")
    body = resp.text or ""
    print(body[:limit] + ("..." if len(body) > limit else ""))
    print()


def main() -> None:
    bc_cfg, _ = load_configs()
    session = get_session()
    token = get_access_token(session, bc_cfg)
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    base = (
        f"{bc_cfg['resource']}/v2.0/{bc_cfg['tenant_id']}"
        f"/{bc_cfg['environment']}/ODataV4"
    )
    company_cfg = bc_cfg["company"]
    company = quote(company_cfg)

    print(f"Config company string : {company_cfg!r}")
    print(f"Base OData V4 URL      : {base}\n")

    # 1) Which companies does this OAuth app actually see?
    r = session.get(f"{base}/Company", headers=headers, timeout=60)
    if r.status_code == 200:
        try:
            names = [c.get("Name") for c in r.json().get("value", [])]
            print("--- Companies visible via OData V4 ---")
            for n in names:
                match = "  <-- MATCHES config" if n == company_cfg else ""
                print(f"  {n!r}{match}")
            print()
        except Exception as e:
            _show(r, "Company list (could not parse JSON): " + str(e))
    else:
        _show(r, "Company list")

    # 2) Service document for this company — lists every entity set OData exposes.
    r = session.get(f"{base}/Company('{company}')", headers=headers, timeout=120)
    if r.status_code == 200:
        try:
            sets = sorted(e.get("name", "") for e in r.json().get("value", []))
            print(f"--- Entity sets exposed for Company('{company_cfg}') "
                  f"({len(sets)} total) ---")
            print(f"  '{PAGE}' present? "
                  f"{'YES' if PAGE in sets else 'NO  <-- this is the problem'}")
            # show near matches to catch spelling/casing differences
            near = [s for s in sets if "purchase" in s.lower() or "order" in s.lower()]
            print(f"  related sets: {near}")
            print()
        except Exception as e:
            _show(r, "Service document (could not parse JSON): " + str(e))
    else:
        _show(r, f"Service document for Company('{company_cfg}')")

    # 3) The exact call the ETL makes — print the real 404 body.
    url = f"{base}/Company('{company}')/{PAGE}?$top=1"
    print(f"GET {url}")
    r = session.get(url, headers=headers, timeout=120)
    _show(r, "Direct endpoint call (what the ETL does)")


if __name__ == "__main__":
    main()
