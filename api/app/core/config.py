"""
Application configuration for the FastAPI reporting API.

Why this file exists:
- Centralizes environment-based settings.
- Keeps database connection details out of route handlers.
- Makes the API easier to configure locally and later in Docker/ECS.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(frozen=True)
class APISettings:
    """
    Runtime settings for the reporting API.
    """

    app_name: str
    app_env: str
    log_level: str
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str

    @classmethod
    def from_env(cls) -> APISettings:
        """
        Load API settings from environment variables.

        Returns:
            APISettings instance.
        """

        load_dotenv()

        return cls(
            app_name=os.getenv("APP_NAME", "multi-cloud-retail-analytics-platform"),
            app_env=os.getenv("APP_ENV", "local"),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            postgres_host=os.getenv("POSTGRES_HOST", "localhost"),
            postgres_port=int(os.getenv("POSTGRES_PORT", "5432")),
            postgres_db=os.getenv("POSTGRES_DB", "retail_analytics"),
            postgres_user=os.getenv("POSTGRES_USER", "retail_user"),
            postgres_password=os.getenv("POSTGRES_PASSWORD", "retail_password"),
        )


settings = APISettings.from_env()