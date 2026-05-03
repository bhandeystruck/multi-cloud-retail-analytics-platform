# Multi-Cloud Retail Analytics Data Platform

A production-inspired data engineering and backend platform for retail analytics, designed to demonstrate cloud infrastructure, workflow orchestration, data warehousing, SQL analytics, API development, and DevOps automation.

---

## Project Goal

This platform simulates a real retail company analytics system.

It generates realistic retail data, lands raw source files in object storage, loads those files into a local warehouse, transforms raw records into clean relational tables, builds business-ready Gold analytics models, and prepares the foundation for reporting APIs and multi-cloud deployment.

The platform is designed to map closely to responsibilities expected from a Senior Software Engineer – Data Platform role, including:

- cloud infrastructure design
- data pipeline development
- object storage ingestion
- warehouse modeling
- SQL transformations
- workflow orchestration
- backend reporting APIs
- CI/CD automation
- AWS and GCP architecture readiness

---

## Business Problem

Retail companies collect data from many different systems:

- online orders
- physical stores
- customer profiles
- product catalogs
- inventory systems
- marketing campaigns
- returns and refunds

Without a centralized analytics platform, business teams struggle to answer questions such as:

- What is daily revenue?
- Which products are top sellers?
- Which customers are most valuable?
- Which stores are underperforming?
- Which inventory items are at risk?
- Which campaigns produce the best ROI?

This project solves that problem by creating a structured analytics platform from raw data generation to business-ready reporting models.

---

## High-Level Architecture

```text
Retail Source Data Generator
        |
        v
Generated JSON Source Files
        |
        v
Bronze Object Storage
MinIO locally / AWS S3 later
        |
        v
Bronze Warehouse Loader
        |
        v
PostgreSQL Local Warehouse
Bronze -> Silver -> Gold
        |
        v
Gold Analytics Models
        |
        v
FastAPI Reporting API
        |
        v
Dashboards / BI / API Consumers
```

---

## Technology Stack

### Local Development

- Python 3.11+
- Docker and Docker Compose
- MinIO for S3-compatible object storage
- PostgreSQL as local warehouse simulation
- Apache Airflow for orchestration foundation
- Pydantic for schema validation
- psycopg2 for PostgreSQL access
- pytest for testing
- Ruff for linting
- DBeaver for visual database inspection

### Cloud Target

Later phases are designed to map to:

- AWS S3
- AWS Redshift
- AWS EC2 Spot Instances
- AWS ECS
- AWS IAM and VPC concepts
- GCP Cloud Storage
- GCP BigQuery
- Jenkins CI/CD
- FastAPI reporting services

---

## Current Project Status

Current completed phase:

```text
Step 11: API Dockerization
```

Completed so far:

```text
[✓] Step 1: Repository structure and documentation foundation
[✓] Step 2: Local Docker infrastructure with PostgreSQL, MinIO, and Airflow
[✓] Step 3: Retail source data generator
[✓] Step 4: Object storage abstraction and MinIO client
[✓] Step 5: Bronze ingestion into object storage
[✓] Step 6: Local warehouse foundation with Bronze/Silver/Gold/Ops schemas
[✓] Step 7: Bronze warehouse loader from MinIO to PostgreSQL
[✓] Step 8: Silver transformations from raw JSONB to typed relational tables
[✓] Step 9: Gold analytics models for business reporting
[✓] Step 10: FastAPI reporting API
[✓] Step 11: API Dockerization
```

Next planned phase:

```text
Step 12: Airflow dynamic DAG orchestration
```

---

## Repository Structure

```text
multi-cloud-retail-analytics-platform/
|
├── README.md
├── .env.example
├── .gitignore
├── pyproject.toml
├── docker-compose.yml
|
├── config/
│   └── datasets.yml
|
├── docs/
│   ├── architecture.md
│   ├── data_model.md
│   ├── local_development.md
│   └── pipeline_design.md
|
├── ingestion/
│   ├── generate_retail_data.py
│   ├── ingest_to_object_storage.py
│   └── schemas.py
|
├── storage/
│   ├── exceptions.py
│   ├── object_storage_client.py
│   └── minio_client.py
|
├── warehouse/
│   └── local_postgres/
│       ├── ddl/
│       │   ├── 001_create_warehouse_schemas.sql
│       │   ├── 002_create_silver_tables.sql
│       │   └── 003_create_gold_tables.sql
│       └── load/
│           └── bronze_loader.py
|
├── transformations/
│   ├── silver/
│   │   └── 001_transform_bronze_to_silver.sql
│   └── gold/
│       └── 001_transform_silver_to_gold.sql
|
├── scripts/
│   ├── init_local_warehouse.py
│   ├── verify_local_warehouse.py
│   ├── verify_minio_storage.py
│   ├── run_local_bronze_ingestion.py
│   ├── load_bronze_warehouse.py
│   ├── run_silver_transformations.py
│   ├── verify_silver_tables.py
│   ├── run_gold_transformations.py
│   └── verify_gold_tables.py
|
├── airflow/
│   └── dags/
│       └── health_check_dag.py
|
├── tests/
│   ├── unit/
│   ├── integration/
│   └── data_quality/
|
└── infrastructure/
    ├── aws/
    ├── gcp/
    └── docker/
```

---

## Data Layers

### Bronze Layer

The Bronze layer stores raw source records as JSONB.

Purpose:

- preserve raw data
- support replay and reprocessing
- maintain auditability
- keep source structure intact

Example tables:

```text
bronze.raw_sales
bronze.raw_products
bronze.raw_customers
bronze.raw_stores
bronze.raw_inventory
bronze.raw_campaigns
bronze.raw_returns
```

---

### Silver Layer

The Silver layer converts raw JSONB records into clean typed relational tables.

Purpose:

- enforce SQL data types
- simplify joins
- standardize fields
- deduplicate business records
- prepare data for analytics

Example tables:

```text
silver.sales
silver.products
silver.customers
silver.stores
silver.inventory
silver.campaigns
silver.returns
```

---

### Gold Layer

The Gold layer stores business-ready analytics models.

Purpose:

- power reports
- support dashboard queries
- prepare data for FastAPI endpoints
- avoid expensive calculations inside the API layer

Gold tables:

```text
gold.daily_revenue
gold.product_sales_performance
gold.customer_lifetime_value
gold.store_performance
gold.inventory_risk
gold.campaign_roi
gold.executive_kpis
```

---

## Core Datasets

The source data generator creates these datasets:

| Dataset | File | Purpose |
|---|---|---|
| Products | data/generated/products.json | Product catalog |
| Customers | data/generated/customers.json | Customer profiles |
| Stores | data/generated/stores.json | Retail locations |
| Campaigns | data/generated/campaigns.json | Marketing campaigns |
| Sales | data/generated/sales.json | Sales transactions |
| Inventory | data/generated/inventory.json | Stock records |
| Returns | data/generated/returns.json | Refund and return records |

Generated data preserves relationships such as:

- `sales.product_id` references `products.product_id`
- `sales.customer_id` references `customers.customer_id`
- `sales.store_id` references `stores.store_id`
- `sales.campaign_id` optionally references `campaigns.campaign_id`
- `returns.order_id` references `sales.order_id`
- `inventory.product_id` references `products.product_id`
- `inventory.store_id` references `stores.store_id`

---

## Local Development Setup

### 1. Create and activate virtual environment

```bash
python -m venv .venv
```

Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

macOS/Linux:

```bash
source .venv/bin/activate
```

Upgrade pip:

```bash
python -m pip install --upgrade pip
```

Install dependencies:

```bash
pip install -e ".[dev,storage,warehouse]"
```

---

### 2. Create local environment file

```bash
cp .env.example .env
```

Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

---

### 3. Start Docker services

```bash
docker compose up -d
```

Check service status:

```bash
docker compose ps
```

Local service URLs:

| Service | URL |
|---|---|
| MinIO Console | http://localhost:9001 |
| Airflow UI | http://localhost:8080 |
| PostgreSQL | localhost:5432 |

Default MinIO credentials:

```text
Username: minioadmin
Password: minioadmin
```

Default Airflow credentials:

```text
Username: admin
Password: admin
```

---

## Full Local Pipeline Run

Run the full pipeline from source generation to Gold analytics:

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

Expected final result:

```text
Gold verification passed.
Gold table row counts:
- gold.daily_revenue: ...
- gold.product_sales_performance: 100
- gold.customer_lifetime_value: 500
- gold.store_performance: 20
- gold.inventory_risk: 500
- gold.campaign_roi: 12
- gold.executive_kpis: 1+
```

---

## Generate Local Source Data

Run:

```bash
python ingestion/generate_retail_data.py
```

This creates JSON source files in:

```text
data/generated/
```

Custom smaller test run:

```bash
python ingestion/generate_retail_data.py --products 10 --customers 20 --stores 5 --campaigns 3 --sales 50 --inventory 30 --return-rate 0.10 --seed 123
```

Windows PowerShell one-line version:

```powershell
python ingestion/generate_retail_data.py --products 10 --customers 20 --stores 5 --campaigns 3 --sales 50 --inventory 30 --return-rate 0.10 --seed 123
```

---

## Verify Local Object Storage

Make sure Docker services are running:

```bash
docker compose up -d
```

Run:

```bash
python scripts/verify_minio_storage.py
```

This validates that Python can:

- connect to MinIO
- create or verify the Bronze bucket
- upload a test object
- list objects
- download the object
- verify downloaded content

---

## Run Bronze Ingestion

Generate source data first:

```bash
python ingestion/generate_retail_data.py
```

Run Bronze ingestion:

```bash
python ingestion/ingest_to_object_storage.py
```

This uploads generated JSON files from:

```text
data/generated/
```

to MinIO using this layout:

```text
bronze/{dataset}/dt={YYYY-MM-DD}/run_id={run_id}/{dataset}.json
```

A manifest is also written locally and uploaded to object storage:

```text
manifests/bronze/dt={YYYY-MM-DD}/run_id={run_id}/
```

Run with custom date and run ID:

```bash
python ingestion/ingest_to_object_storage.py --ingestion-date 2026-05-03 --run-id manual_test_001
```

---

## Initialize Local Warehouse

Run:

```bash
python scripts/init_local_warehouse.py
```

Verify required schemas and tables:

```bash
python scripts/verify_local_warehouse.py
```

This creates:

```text
bronze
silver
gold
ops
```

The `ops` schema tracks pipeline runs, loaded files, and data quality results.

---

## Load Bronze Data Into Local Warehouse

Run:

```bash
python scripts/load_bronze_warehouse.py
```

Verify Bronze row counts:

```bash
docker exec -it retail_postgres psql -U retail_user -d retail_analytics -c "SELECT COUNT(*) FROM bronze.raw_sales;"
```

Verify loaded file audit records:

```bash
docker exec -it retail_postgres psql -U retail_user -d retail_analytics -c "SELECT dataset_name, load_status, record_count FROM ops.loaded_files;"
```

Run the loader again to test idempotency:

```bash
python scripts/load_bronze_warehouse.py
```

Already-loaded files should be skipped instead of duplicated.

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

Example Silver query:

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

Example Gold query:

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

## FastAPI Reporting API

After Gold tables are populated, start the reporting API:

```bash
uvicorn api.app.main:app --reload
```

API runs at:

```text
http://localhost:8000
```

Interactive docs at:

```text
http://localhost:8000/docs
```

### Available Endpoints

| Method | Endpoint | Description |
|---|---|---|
| GET | `/` | Root — app name, environment, links |
| GET | `/health` | Health check |
| GET | `/api/v1/kpis/overview` | Latest executive KPI snapshot |
| GET | `/api/v1/revenue/daily` | Daily revenue (filterable by date range) |
| GET | `/api/v1/products/performance` | Product sales performance |
| GET | `/api/v1/customers/lifetime-value` | Customer lifetime value |
| GET | `/api/v1/inventory/risk` | Inventory risk by stock status |
| GET | `/api/v1/campaigns/roi` | Campaign ROI metrics |

Query parameters supported on list endpoints:

```text
start_date  YYYY-MM-DD filter
end_date    YYYY-MM-DD filter
limit       max rows returned (default 30, max 365)
```

All endpoints read directly from Gold analytics tables. No calculations happen inside the API layer.

The API uses environment variables for database connection. Defaults match local Docker setup:

```text
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=retail_analytics
POSTGRES_USER=retail_user
POSTGRES_PASSWORD=retail_password
```

---

## API Dockerization

The reporting API is containerized for reproducible local runs and future ECS/cloud deployment.

Build the API image:

```bash
docker compose build api
```

Start all services including the API:

```bash
docker compose up -d
```

API available at:

```text
http://localhost:8000
```

### What the Dockerfile does

- Base image: `python:3.11-slim`
- Installs system deps: `gcc`, `libpq-dev`, `curl`
- Copies project source and installs with `pip install -e ".[api,warehouse,storage]"`
- Exposes port `8000`
- Healthcheck hits `/health` every 30 seconds
- Starts with `uvicorn api.app.main:app --host 0.0.0.0 --port 8000`

Docker layer caching is optimized — `pyproject.toml` is copied before source so dependency installation is only re-run when dependencies change.

Check API container health:

```bash
docker compose ps
```

View API logs:

```bash
docker compose logs api
```

---

## Useful Business Queries

### Executive KPIs

```sql
SELECT *
FROM gold.executive_kpis
ORDER BY snapshot_at DESC
LIMIT 5;
```

### Daily Revenue

```sql
SELECT *
FROM gold.daily_revenue
ORDER BY revenue_date DESC
LIMIT 30;
```

### Top Products

```sql
SELECT
    product_name,
    category,
    brand,
    units_sold,
    gross_revenue,
    estimated_gross_margin,
    return_rate
FROM gold.product_sales_performance
ORDER BY gross_revenue DESC
LIMIT 20;
```

### Best Customers

```sql
SELECT
    customer_name,
    email,
    country,
    loyalty_tier,
    completed_orders,
    total_spent,
    average_order_value
FROM gold.customer_lifetime_value
ORDER BY total_spent DESC
LIMIT 20;
```

### Inventory Risk

```sql
SELECT
    product_name,
    category,
    store_name,
    region,
    stock_quantity,
    reorder_level,
    stock_status
FROM gold.inventory_risk
WHERE stock_status IN ('low_stock', 'out_of_stock')
ORDER BY stock_status, stock_quantity ASC
LIMIT 50;
```

### Campaign ROI

```sql
SELECT
    campaign_name,
    channel,
    target_region,
    budget,
    attributed_orders,
    attributed_revenue,
    estimated_roi
FROM gold.campaign_roi
ORDER BY estimated_roi DESC;
```

---

## Visual Database Inspection

Use DBeaver with this PostgreSQL connection:

```text
Host: localhost
Port: 5432
Database: retail_analytics
Username: retail_user
Password: retail_password
```

If DBeaver shows a timezone error like:

```text
FATAL: invalid value for parameter "TimeZone": "Asia/Katmandu"
```

Set the JDBC URL to:

```text
jdbc:postgresql://localhost:5432/retail_analytics?options=-c%20TimeZone=UTC
```

Then run:

```sql
SHOW timezone;
```

Expected:

```text
UTC
```

---

## Testing and Code Quality

Run tests:

```bash
python -m pytest
```

Run Ruff:

```bash
ruff check .
```

Auto-fix where possible:

```bash
ruff check . --fix
```

---

## Engineering Principles

This project follows:

- clean architecture
- separation of concerns
- environment-based configuration
- defensive error handling
- idempotent pipeline behavior
- layered warehouse modeling
- operational audit tracking
- data quality readiness
- object storage abstraction
- repeatable local development
- documentation-first development

---

## Roadmap

Completed:

1. Repository structure and documentation
2. Docker Compose local infrastructure
3. Retail source data generator
4. Object storage abstraction and MinIO client
5. Bronze ingestion to object storage
6. Local warehouse schemas and audit tables
7. Bronze warehouse loader
8. Silver transformations
9. Gold analytics models
10. FastAPI reporting API
11. API Dockerization

Next:

12. Airflow dynamic DAG orchestration
13. Data quality checks
14. Jenkins CI/CD
15. AWS S3 and Redshift support
16. GCP GCS and BigQuery replication
17. EC2 spot worker orchestration
18. Final portfolio documentation and interview notes

---
