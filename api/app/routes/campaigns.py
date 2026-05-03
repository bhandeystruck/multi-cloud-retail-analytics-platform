"""
Campaign ROI routes.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from api.app.db.connection import get_db_session
from api.app.schemas.responses import CampaignROIResponse
from api.app.services.reporting_service import fetch_campaign_roi

router = APIRouter(prefix="/api/v1/campaigns", tags=["campaigns"])

DatabaseSession = Annotated[Session, Depends(get_db_session)]
LimitQuery = Annotated[int, Query(ge=1, le=100)]
ChannelQuery = Annotated[str | None, Query()]


@router.get("/roi", response_model=list[CampaignROIResponse])
def get_campaign_roi(
    db: DatabaseSession,
    limit: LimitQuery = 20,
    channel: ChannelQuery = None,
) -> list[CampaignROIResponse]:
    """
    Return campaign ROI metrics.
    """

    rows = fetch_campaign_roi(
        db=db,
        limit=limit,
        channel=channel,
    )

    return [CampaignROIResponse(**row) for row in rows]