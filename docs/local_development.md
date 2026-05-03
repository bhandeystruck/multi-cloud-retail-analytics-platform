# Local Development Guide

## Overview

This guide explains how to run the Multi-Cloud Retail Analytics Data Platform locally.

The local environment uses Docker Compose to run the core infrastructure services and Python scripts to generate, ingest, load, and transform retail data.

Local services:

| Service | Purpose | URL / Host |
|---|---|---|
| PostgreSQL | Local warehouse simulation and Airflow metadata database | localhost:5432 |
| MinIO | S3-compatible object storage | http://localhost:9001 |
| Airflow Webserver | Workflow orchestration UI | http://localhost:8080 |

---

## Prerequisites

Install:

- Docker
- Docker Compose
- Python 3.11+
- Git
- Optional: DBeaver for visual database inspection

---

## Environment Setup

Copy the example environment file:

```bash
cp .env.example .env
```

On Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

Never commit your real `.env` file.

---

## Python Environment

Create a virtual environment:

```bash
python -m venv .venv
```

Activate it.

Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

macOS/Linux:

```bash
source .venv/bin/activate
```

Install dependencies:

```bash
pip install -e ".[dev,storage,warehouse]"
```

---

## Start Local Services

```bash
docker compose up -d
```

Check status:

```bash
docker compose ps
```

---

## Access MinIO

Open:

```text
http://localhost:9001
```

Default local credentials:

```text
Username: minioadmin
Password: minioadmin
```

Expected bucket:

```text
retail-bronze
```

---

## Access Airflow

Open:

```text
http://localhost:8080
```

Default local credentials:

```text
Username: admin
Password: admin
```

The health check DAG should be visible after Step 2.

---

## Access PostgreSQL from Terminal

```bash
docker exec -it retail_postgres psql -U retail_user -d retail_analytics
```

Inside PostgreSQL:

```sql
SELECT current_database();
```

Exit:

```sql
\q
```

---

## Access PostgreSQL with DBeaver

Use these connection details:

```text
Host: localhost
Port: 5432
Database: retail_analytics
Username: retail_user
Password: retail_password
```

If DBeaver shows this timezone error:

```text
FATAL: invalid value for parameter "TimeZone": "Asia/Katmandu"
```

Set the JDBC URL to:

```text
jdbc:postgresql://localhost:5432/retail_analytics?options=-c%20TimeZone=UTC
```

Then reconnect and run:

```sql
SHOW timezone;
```

Expected:

```text
UTC
```

---

## Verify MinIO Storage

Make sure Docker services are running:

```bash
docker compose up -d
```

Run:

```bash
python scripts/verify_minio_storage.py
```

This verifies:

- bucket creation
- file upload
- object existence check
- object listing
- file download
- downloaded content verification

---

## Generate Local Source Data

Run:

```bash
python ingestion/generate_retail_data.py
```

This creates generated source JSON files in:

```text
data/generated/
```

Expected files:

```text
products.json
customers.json
stores.json
campaigns.json
sales.json
inventory.json
returns.json
```

Custom small test run:

```bash
python ingestion/generate_retail_data.py --products 10 --customers 20 --stores 5 --campaigns 3 --sales 50 --inventory 30 --return-rate 0.10 --seed 123
```

---

## Run Bronze Ingestion to Object Storage

After generating source data, run:

```bash
python ingestion/ingest_to_object_storage.py
```

This uploads files from:

```text
data/generated/
```

to MinIO using this layout:

```text
bronze/{dataset}/dt={YYYY-MM-DD}/run_id={run_id}/{dataset}.json
```

A manifest is also created locally and uploaded to MinIO:

```text
data/manifests/
manifests/bronze/dt={YYYY-MM-DD}/run_id={run_id}/
```

Run with a custom date and run ID:

```bash
python ingestion/ingest_to_object_storage.py --ingestion-date 2026-05-03 --run-id manual_test_001
```

---

## Initialize Local Warehouse

After starting Docker services, initialize the local PostgreSQL warehouse:

```bash
python scripts/init_local_warehouse.py
```

Verify required schemas and tables:

```bash
python scripts/verify_local_warehouse.py
```

Expected output:

```text
Local warehouse verification passed.
Required schemas and tables exist.
```

The initialization script creates:

```text
bronze
silver
gold
ops
```

---

## Load Bronze Files Into Warehouse

After generating data and ingesting it to MinIO:

```bash
python ingestion/generate_retail_data.py
python ingestion/ingest_to_object_storage.py
```

Load the latest Bronze ingestion manifest into PostgreSQL:

```bash
python scripts/load_bronze_warehouse.py
```

Verify row counts:

```bash
docker exec -it retail_postgres psql -U retail_user -d retail_analytics -c "SELECT COUNT(*) FROM bronze.raw_sales;"
```

The loader is idempotent. If you run it again for the same manifest, already-loaded files should be skipped.

---

## Run Silver Transformations

After Bronze files are loaded into PostgreSQL, run:

```bash
python scripts/run_silver_transformations.py
```

Verify Silver row counts:

```bash
python scripts/verify_silver_tables.py
```

You can visually inspect the tables in DBeaver:

```text
retail_analytics
└── Schemas
    └── silver
        └── Tables
```

Useful query:

```sql
SELECT
    s.order_id,
    s.total_amount,
    s.order_status,
    c.email,
    p.product_name,
    st.store_name
FROM silver.sales s
JOIN silver.customers c
    ON s.customer_id = c.customer_id
JOIN silver.products p
    ON s.product_id = p.product_id
JOIN silver.stores st
    ON s.store_id = st.store_id
LIMIT 50;
```

---

## Run Gold Transformations

After Silver tables are populated, run:

```bash
python scripts/run_gold_transformations.py
```

Verify Gold row counts:

```bash
python scripts/verify_gold_tables.py
```

You can visually inspect the tables in DBeaver:

```text
retail_analytics
└── Schemas
    └── gold
        └── Tables
```

Useful executive KPI query:

```sql
SELECT *
FROM gold.executive_kpis
ORDER BY snapshot_at DESC
LIMIT 5;
```

Useful product performance query:

```sql
SELECT
    product_name,
    category,
    units_sold,
    gross_revenue,
    estimated_gross_margin
FROM gold.product_sales_performance
ORDER BY gross_revenue DESC
LIMIT 20;
```

---

## Full Local Pipeline to Gold

Run this from a clean local environment after Docker is running:

```bash
docker compose up -d
python scripts/init_local_warehouse.py
python ingestion/generate_retail_data.py
python ingestion/ingest_to_object_storage.py
python scripts/load_bronze_warehouse.py
python scripts/run_silver_transformations.py
python scripts/verify_silver_tables.py
python scripts/run_gold_transformations.py
python scripts/verify_gold_tables.py
```

---

## Testing and Linting

Run tests:

```bash
python -m pytest
```

Run Ruff:

```bash
ruff check .
```

Auto-fix safe issues:

```bash
ruff check . --fix
```

---

## Stop Services

```bash
docker compose down
```

Stop services and delete volumes:

```bash
docker compose down -v
```

Warning: `down -v` deletes local PostgreSQL data, MinIO files, and Airflow logs.

---

## Common Troubleshooting

### Port 8080 Already in Use

Airflow uses port 8080. Stop the conflicting service or change the host port in `docker-compose.yml`.

### Port 5432 Already in Use

PostgreSQL uses port 5432. If you already have PostgreSQL installed locally, change the host port mapping.

Example:

```yaml
ports:
  - "5433:5432"
```

Then connect using port `5433`.

### Airflow DAG Does Not Appear

Check scheduler logs:

```bash
docker compose logs airflow-scheduler
```

### MinIO Bucket Missing

Check the MinIO initializer logs:

```bash
docker compose logs minio-init
```

### Silver Transformation Duplicate Conflict

If Silver transformations fail with:

```text
ON CONFLICT DO UPDATE command cannot affect row a second time
```

It means Bronze has multiple raw versions of the same business key. The Silver SQL should use `DISTINCT ON` to deduplicate before insert.
