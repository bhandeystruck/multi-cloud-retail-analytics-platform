"""
Inventory risk routes.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from api.app.db.connection import get_db_session
from api.app.schemas.responses import InventoryRiskResponse
from api.app.services.reporting_service import fetch_inventory_risk

router = APIRouter(prefix="/api/v1/inventory", tags=["inventory"])

DatabaseSession = Annotated[Session, Depends(get_db_session)]
LimitQuery = Annotated[int, Query(ge=1, le=200)]
StockStatusQuery = Annotated[
    str | None,
    Query(description="Optional filter: out_of_stock, low_stock, or healthy"),
]


@router.get("/risk", response_model=list[InventoryRiskResponse])
def get_inventory_risk(
    db: DatabaseSession,
    limit: LimitQuery = 50,
    stock_status: StockStatusQuery = None,
) -> list[InventoryRiskResponse]:
    """
    Return inventory risk records.
    """

    rows = fetch_inventory_risk(
        db=db,
        limit=limit,
        stock_status=stock_status,
    )

    return [InventoryRiskResponse(**row) for row in rows]