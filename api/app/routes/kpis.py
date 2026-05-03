"""
Executive KPI routes.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from api.app.db.connection import get_db_session
from api.app.schemas.responses import ExecutiveKPIsResponse
from api.app.services.reporting_service import fetch_latest_executive_kpis

router = APIRouter(prefix="/api/v1/kpis", tags=["kpis"])

DatabaseSession = Annotated[Session, Depends(get_db_session)]


@router.get("/overview", response_model=ExecutiveKPIsResponse)
def get_kpi_overview(db: DatabaseSession) -> ExecutiveKPIsResponse:
    """
    Return the latest executive KPI snapshot.
    """

    result = fetch_latest_executive_kpis(db)

    if result is None:
        raise HTTPException(
            status_code=404,
            detail="No executive KPI data found. Run Gold transformations first.",
        )

    return ExecutiveKPIsResponse(**result)