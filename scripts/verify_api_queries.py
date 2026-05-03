"""
Verify reporting API database queries without starting the HTTP server.

Why this script exists:
- Confirms service-layer queries can read Gold tables.
- Useful before running FastAPI.
- Can later become part of CI/CD checks.

Run:

    python scripts/verify_api_queries.py
"""

from __future__ import annotations

import sys

from api.app.db.connection import SessionLocal
from api.app.services.reporting_service import (
    fetch_campaign_roi,
    fetch_daily_revenue,
    fetch_inventory_risk,
    fetch_latest_executive_kpis,
    fetch_top_customers,
    fetch_top_products,
)


def main() -> int:
    """
    Run basic reporting query checks.

    Returns:
        Process exit code.
    """

    db = SessionLocal()

    try:
        kpis = fetch_latest_executive_kpis(db)
        daily_revenue = fetch_daily_revenue(db, limit=5)
        products = fetch_top_products(db, limit=5)
        customers = fetch_top_customers(db, limit=5)
        inventory = fetch_inventory_risk(db, limit=5)
        campaigns = fetch_campaign_roi(db, limit=5)

    finally:
        db.close()

    checks = {
        "executive_kpis": kpis is not None,
        "daily_revenue": len(daily_revenue) > 0,
        "top_products": len(products) > 0,
        "top_customers": len(customers) > 0,
        "inventory_risk": len(inventory) > 0,
        "campaign_roi": len(campaigns) > 0,
    }

    failed_checks = [name for name, passed in checks.items() if not passed]

    if failed_checks:
        print(f"API query verification failed: {failed_checks}", file=sys.stderr)
        return 1

    print("API query verification passed.")
    print("Gold reporting queries returned data.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())