"""
Verify the Dockerized FastAPI reporting API.

Why this script exists:
- Confirms the API is reachable through localhost:8000.
- Confirms core reporting endpoints return successful responses.
- Provides a reusable validation command for local development and CI/CD.

Run after starting the API container:

    docker compose up -d api
    python scripts/verify_api_container.py
"""

from __future__ import annotations

import json
import sys
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

BASE_URL = "http://localhost:8000"

ENDPOINTS = [
    "/health",
    "/api/v1/kpis/overview",
    "/api/v1/revenue/daily?limit=5",
    "/api/v1/products/top-selling?limit=5",
    "/api/v1/customers/top?limit=5",
    "/api/v1/inventory/risk?limit=5",
    "/api/v1/campaigns/roi?limit=5",
]


class APIContainerVerificationError(Exception):
    """
    Raised when API container verification fails.
    """


def fetch_json(path: str) -> object:
    """
    Fetch JSON from an API endpoint.

    Args:
        path: API path beginning with slash.

    Returns:
        Parsed JSON response.

    Raises:
        APIContainerVerificationError: If request fails or response is invalid.
    """

    url = f"{BASE_URL}{path}"
    request = Request(url, headers={"Accept": "application/json"})

    try:
        with urlopen(request, timeout=10) as response:
            response_body = response.read().decode("utf-8")

    except HTTPError as exc:
        raise APIContainerVerificationError(
            f"Endpoint {path} returned HTTP {exc.code}: {exc.reason}",
        ) from exc

    except URLError as exc:
        raise APIContainerVerificationError(
            f"Failed to connect to API endpoint {path}: {exc}",
        ) from exc

    try:
        return json.loads(response_body)

    except json.JSONDecodeError as exc:
        raise APIContainerVerificationError(
            f"Endpoint {path} did not return valid JSON",
        ) from exc


def verify_api_container() -> None:
    """
    Verify all expected API endpoints return JSON.
    """

    for endpoint in ENDPOINTS:
        payload = fetch_json(endpoint)

        if payload is None:
            raise APIContainerVerificationError(
                f"Endpoint {endpoint} returned empty response",
            )

        print(f"Verified endpoint: {endpoint}")


def main() -> int:
    """
    CLI entry point.

    Returns:
        Exit code.
    """

    try:
        verify_api_container()

    except APIContainerVerificationError as exc:
        print(f"API container verification failed: {exc}", file=sys.stderr)
        return 1

    print("API container verification passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())