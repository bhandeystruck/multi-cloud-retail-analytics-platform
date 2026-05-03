"""
Database connection utilities for the FastAPI reporting API.

Why this file exists:
- Keeps database engine/session setup in one place.
- Allows route handlers to depend on a clean database session.
- Makes the API easier to test and maintain.
"""

from __future__ import annotations

from collections.abc import Generator

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from api.app.core.config import settings


def build_database_url() -> str:
    """
    Build SQLAlchemy database URL from environment settings.

    Returns:
        PostgreSQL SQLAlchemy connection string.
    """

    return (
        f"postgresql+psycopg2://{settings.postgres_user}:"
        f"{settings.postgres_password}@{settings.postgres_host}:"
        f"{settings.postgres_port}/{settings.postgres_db}"
    )


engine: Engine = create_engine(
    build_database_url(),
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
)


def get_db_session() -> Generator[Session, None, None]:
    """
    FastAPI dependency that provides a database session.

    Yields:
        SQLAlchemy session.

    Why:
    Each request gets a session, and the session is safely closed after the
    request finishes.
    """

    db = SessionLocal()

    try:
        yield db

    finally:
        db.close()