# ============================================================
# Custom Airflow image for the retail analytics platform
#
# Purpose:
# - Extends the official Airflow image.
# - Installs only the project dependencies needed by DAG tasks.
# - Avoids upgrading Airflow core dependencies such as SQLAlchemy.
#
# Important:
# Airflow manages its own dependency constraints.
# Do NOT install "sqlalchemy>=2.0.0" here because it can break Airflow's ORM.
# ============================================================

FROM apache/airflow:2.9.3

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Airflow 2.9.3 in this image runs on Python 3.12.
# We use Airflow's official constraints file so pip does not accidentally
# upgrade dependencies that Airflow relies on internally.
ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.12
ARG CONSTRAINT_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt

RUN pip install --no-cache-dir \
    --constraint "${CONSTRAINT_URL}" \
    "pydantic[email]>=2.7.0" \
    "python-dotenv>=1.0.1" \
    "PyYAML>=6.0.1" \
    "minio>=7.2.0" \
    "psycopg2-binary>=2.9.9" \
    "fastapi>=0.111.0" \
    "uvicorn[standard]>=0.30.0"