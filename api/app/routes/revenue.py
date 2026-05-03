"""
Revenue reporting routes.
"""

from __future__ import annotations

from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from api.app.db.connection import get_db_session
from api.app.schemas.responses import DailyRevenueResponse
from api.app.services.reporting_service import fetch_daily_revenue

router = APIRouter(prefix="/api/v1/revenue", tags=["revenue"])

DatabaseSession = Annotated[Session, Depends(get_db_session)]
StartDateQuery = Annotated[date | None, Query()]
EndDateQuery = Annotated[date | None, Query()]
LimitQuery = Annotated[int, Query(ge=1, le=365)]


@router.get("/daily", response_model=list[DailyRevenueResponse])
def get_daily_revenue(
    db: DatabaseSession,
    start_date: StartDateQuery = None,
    end_date: EndDateQuery = None,
    limit: LimitQuery = 30,
) -> list[DailyRevenueResponse]:
    """
    Return daily revenue metrics.

    Query params:
    - start_date: optional YYYY-MM-DD filter
    - end_date: optional YYYY-MM-DD filter
    - limit: max number of rows
    """

    rows = fetch_daily_revenue(
        db=db,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
    )

    return [DailyRevenueResponse(**row) for row in rows]