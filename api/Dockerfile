# ============================================================
# FastAPI Reporting API Dockerfile
#
# Purpose:
# - Containerizes the reporting API.
# - Provides a reproducible runtime for local Docker Compose.
# - Prepares the API for future ECS/container deployment.
# ============================================================

FROM python:3.11-slim

# Prevents Python from writing .pyc files and ensures logs appear immediately.
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set the working directory inside the container.
WORKDIR /app

# Install system dependencies.
#
# Why gcc/libpq-dev?
# - psycopg2 may need PostgreSQL client libraries.
# - Even though we use psycopg2-binary locally, this keeps the image safer
#   if dependencies change later.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc \
        libpq-dev \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency metadata first.
#
# Why:
# Docker layer caching can reuse dependency installation if source code changes
# but pyproject.toml does not.
COPY pyproject.toml README.md ./

# Copy source packages needed by the API.
#
# We copy the project modules because pyproject editable install needs them.
COPY api ./api
COPY scripts ./scripts
COPY storage ./storage
COPY warehouse ./warehouse
COPY ingestion ./ingestion

# Install API and warehouse dependencies.
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -e ".[api,warehouse,storage]"

# Expose FastAPI port.
EXPOSE 8000

# Healthcheck verifies the API process is reachable.
HEALTHCHECK --interval=30s --timeout=10s --retries=5 \
    CMD curl --fail http://localhost:8000/health || exit 1

# Start the API.
#
# We bind to 0.0.0.0 so the container can receive traffic from Docker.
CMD ["uvicorn", "api.app.main:app", "--host", "0.0.0.0", "--port", "8000"]