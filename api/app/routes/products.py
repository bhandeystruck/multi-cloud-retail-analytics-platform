"""
Product analytics routes.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from api.app.db.connection import get_db_session
from api.app.schemas.responses import ProductPerformanceResponse
from api.app.services.reporting_service import fetch_top_products

router = APIRouter(prefix="/api/v1/products", tags=["products"])

DatabaseSession = Annotated[Session, Depends(get_db_session)]
LimitQuery = Annotated[int, Query(ge=1, le=100)]
CategoryQuery = Annotated[str | None, Query()]


@router.get("/top-selling", response_model=list[ProductPerformanceResponse])
def get_top_selling_products(
    db: DatabaseSession,
    limit: LimitQuery = 20,
    category: CategoryQuery = None,
) -> list[ProductPerformanceResponse]:
    """
    Return top-selling products by gross revenue.
    """

    rows = fetch_top_products(
        db=db,
        limit=limit,
        category=category,
    )

    return [ProductPerformanceResponse(**row) for row in rows]