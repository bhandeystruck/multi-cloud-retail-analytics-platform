"""
Customer analytics routes.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from api.app.db.connection import get_db_session
from api.app.schemas.responses import CustomerLifetimeValueResponse
from api.app.services.reporting_service import fetch_top_customers

router = APIRouter(prefix="/api/v1/customers", tags=["customers"])

DatabaseSession = Annotated[Session, Depends(get_db_session)]
LimitQuery = Annotated[int, Query(ge=1, le=100)]
LoyaltyTierQuery = Annotated[str | None, Query()]


@router.get("/top", response_model=list[CustomerLifetimeValueResponse])
def get_top_customers(
    db: DatabaseSession,
    limit: LimitQuery = 20,
    loyalty_tier: LoyaltyTierQuery = None,
) -> list[CustomerLifetimeValueResponse]:
    """
    Return top customers by lifetime value.
    """

    rows = fetch_top_customers(
        db=db,
        limit=limit,
        loyalty_tier=loyalty_tier,
    )

    return [CustomerLifetimeValueResponse(**row) for row in rows]