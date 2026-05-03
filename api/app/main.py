"""
FastAPI reporting API for the Multi-Cloud Retail Analytics Data Platform.

Why this API exists:
- Exposes business-ready Gold analytics through REST endpoints.
- Keeps consumers away from direct warehouse access.
- Provides a backend layer for dashboards, BI tools, or frontend apps.
- Demonstrates backend engineering on top of a data platform.

Run locally:

    uvicorn api.app.main:app --reload
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.app.core.config import settings
from api.app.routes import campaigns, customers, health, inventory, kpis, products, revenue


def create_app() -> FastAPI:
    """
    Create and configure the FastAPI application.

    Returns:
        Configured FastAPI app.
    """

    app = FastAPI(
        title="Multi-Cloud Retail Analytics API",
        description=(
            "Reporting API for Gold analytics models in the "
            "Multi-Cloud Retail Analytics Data Platform."
        ),
        version="0.1.0",
    )

    # CORS is permissive for local development.
    # In production, this should be restricted to trusted dashboard/frontend domains.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(health.router)
    app.include_router(kpis.router)
    app.include_router(revenue.router)
    app.include_router(products.router)
    app.include_router(customers.router)
    app.include_router(inventory.router)
    app.include_router(campaigns.router)

    @app.get("/")
    def root() -> dict[str, str]:
        """
        Root API endpoint.
        """

        return {
            "app": settings.app_name,
            "environment": settings.app_env,
            "docs": "/docs",
            "health": "/health",
        }

    return app


app = create_app()