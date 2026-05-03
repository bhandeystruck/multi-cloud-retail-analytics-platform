"""
Health routes for the reporting API.

Why this file exists:
- Provides a simple endpoint for checking API and database connectivity.
- Useful for Docker, ECS, load balancers, and local development.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.app.core.config import settings
from api.app.db.connection import get_db_session
from api.app.schemas.responses import HealthResponse

router = APIRouter(tags=["health"])

DatabaseSession = Annotated[Session, Depends(get_db_session)]


@router.get("/health", response_model=HealthResponse)
def health_check(db: DatabaseSession) -> HealthResponse:
    """
    Check API and database health.

    Returns:
        HealthResponse if the API can connect to the database.
    """

    try:
        db.execute(text("SELECT 1;"))

    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Database health check failed: {exc}",
        ) from exc

    return HealthResponse(
        status="healthy",
        app_name=settings.app_name,
        environment=settings.app_env,
        database=settings.postgres_db,
    )