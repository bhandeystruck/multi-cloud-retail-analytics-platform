"""
Unit tests for FastAPI application setup.

These tests do not require the database to contain data.
"""

from __future__ import annotations

from fastapi.testclient import TestClient

from api.app.main import app


def test_root_endpoint_returns_api_metadata() -> None:
    """
    Verify root endpoint returns basic API metadata.
    """

    client = TestClient(app)

    response = client.get("/")

    assert response.status_code == 200

    payload = response.json()

    assert "app" in payload
    assert payload["docs"] == "/docs"
    assert payload["health"] == "/health"


def test_openapi_schema_is_available() -> None:
    """
    Verify OpenAPI schema is generated successfully.
    """

    client = TestClient(app)

    response = client.get("/openapi.json")

    assert response.status_code == 200

    payload = response.json()

    assert payload["info"]["title"] == "Multi-Cloud Retail Analytics API"