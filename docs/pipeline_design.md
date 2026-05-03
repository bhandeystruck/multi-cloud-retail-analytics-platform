# Pipeline Design

## Overview

The Multi-Cloud Retail Analytics Data Platform uses a staged data pipeline design.

The local pipeline currently supports:

```text
Generate source data
        ↓
Upload raw files to MinIO Bronze object storage
        ↓
Load raw JSON records into PostgreSQL Bronze tables
        ↓
Transform Bronze JSONB records into Silver relational tables
        ↓
Transform Silver tables into Gold analytics models
```

The design is intentionally close to production-style data engineering systems.

---

## Pipeline Goals

The main goals are:

1. Generate realistic retail source data.
2. Preserve raw source files in object storage.
3. Track every ingestion run with a manifest.
4. Load raw records into warehouse Bronze tables.
5. Transform raw JSONB into typed Silver tables.
6. Transform Silver data into Gold analytics models.
7. Maintain operational audit logs.
8. Support future Airflow orchestration, AWS S3, Redshift, GCS, and BigQuery.

---

## Pipeline Stages

### 1. Source Data Generation

The generator is located at:

```text
ingestion/generate_retail_data.py
```

It creates:

```text
data/generated/products.json
data/generated/customers.json
data/generated/stores.json
data/generated/campaigns.json
data/generated/sales.json
data/generated/inventory.json
data/generated/returns.json
```

The generated data is interconnected. For example, sales reference valid customers, products, stores, and optional campaigns.

---

### 2. Bronze Object Storage Ingestion

The Bronze ingestion script is located at:

```text
ingestion/ingest_to_object_storage.py
```

It reads dataset definitions from:

```text
config/datasets.yml
```

It uploads generated JSON files to MinIO using this layout:

```text
bronze/{dataset}/dt={YYYY-MM-DD}/run_id={run_id}/{dataset}.json
```

Example:

```text
bronze/sales/dt=2026-05-03/run_id=20260503T120000Z/sales.json
```

Why this layout matters:

| Path Component | Purpose |
|---|---|
| bronze | Identifies the raw data lake zone |
| dataset | Separates sales, products, customers, etc. |
| dt | Logical ingestion date partition |
| run_id | Identifies the exact pipeline execution |
| file name | Preserves the source dataset file |

---

## Object Storage Layer

The platform includes a provider-independent object storage interface located in:

```text
storage/object_storage_client.py
```

The current implementation is:

```text
storage/minio_client.py
```

MinIO is used locally as an S3-compatible object store. This allows the platform to simulate AWS S3 behavior without using paid cloud resources during local development.

Current provider:

| Provider | Purpose |
|---|---|
| MinIO | Local S3-compatible object storage |

Future providers:

| Provider | Purpose |
|---|---|
| AWS S3 | AWS production object storage |
| GCS | GCP replication storage |

The ingestion and pipeline layers should not depend directly on MinIO, AWS S3, or GCS SDKs. Instead, pipeline code should depend on the common `ObjectStorageClient` interface.

Local verification:

```bash
python scripts/verify_minio_storage.py
```

---

## Manifest Files

Each Bronze ingestion run creates a manifest file containing:

- run ID
- ingestion date
- source directory
- bucket name
- dataset names
- object paths
- record counts
- file sizes
- SHA-256 file hashes

Local manifests are written to:

```text
data/manifests/
```

The manifest is also uploaded to object storage under:

```text
manifests/bronze/dt={YYYY-MM-DD}/run_id={run_id}/
```

Manifests help answer:

- Which files were uploaded?
- How many records were in each file?
- Which run uploaded the data?
- What object paths should the warehouse loader process?
- Was the same file already loaded before?

---

## Local Warehouse Initialization

The local PostgreSQL warehouse is initialized by running:

```bash
python scripts/init_local_warehouse.py
```

This applies SQL files from:

```text
warehouse/local_postgres/ddl/
```

in sorted order.

Verification:

```bash
python scripts/verify_local_warehouse.py
```

The warehouse foundation includes:

```text
bronze
silver
gold
ops
```

Why use a script instead of only Docker init SQL?

Docker PostgreSQL init scripts only run when the database volume is first created. During development, we need a repeatable way to reapply idempotent DDL without deleting volumes.

---

## Bronze Warehouse Loader

The Bronze warehouse loader is implemented in:

```text
warehouse/local_postgres/load/bronze_loader.py
```

The CLI entrypoint is:

```text
scripts/load_bronze_warehouse.py
```

The loader reads the latest local Bronze ingestion manifest from:

```text
data/manifests/
```

Then it downloads each object from MinIO and inserts raw JSON records into PostgreSQL Bronze tables.

### Bronze Load Flow

```text
Bronze ingestion manifest
        ↓
MinIO object download
        ↓
JSON record parsing
        ↓
bronze.raw_* JSONB insert
        ↓
ops.loaded_files update
        ↓
ops.pipeline_runs update
```

### Idempotency

The loader checks `ops.loaded_files` before loading each object.

If a file was already loaded successfully, it is skipped.

Bronze raw tables also include a unique constraint on:

```text
(object_name, record_index)
```

This protects the warehouse from duplicate raw records if the loader is rerun.

### Operational Tracking

The loader writes to:

| Table | Purpose |
|---|---|
| ops.pipeline_runs | Tracks the Bronze warehouse load run |
| ops.loaded_files | Tracks each object loaded or skipped |
| bronze.raw_* | Stores raw JSONB records |

---

## Silver Transformations

Silver transformations convert raw Bronze JSONB records into typed relational tables.

The SQL transformation file is:

```text
transformations/silver/001_transform_bronze_to_silver.sql
```

The runner script is:

```bash
python scripts/run_silver_transformations.py
```

Verification:

```bash
python scripts/verify_silver_tables.py
```

### Transformation Pattern

Silver uses:

```sql
INSERT INTO silver.table (...)
SELECT payload ->> 'field'
FROM bronze.raw_table
ON CONFLICT (primary_key)
DO UPDATE SET ...
```

Silver transformations are idempotent. If the same transformation runs again, rows are updated instead of duplicated.

Because Bronze may contain multiple raw versions of the same business key, the Silver SQL should deduplicate using `DISTINCT ON` before insert.

### Why Silver Matters

Bronze is optimized for preservation.

Silver is optimized for clean relational use.

Silver prepares data for Gold analytics models.

---

## Gold Transformations

Gold transformations convert clean Silver tables into business-ready analytics models.

The SQL transformation file is:

```text
transformations/gold/001_transform_silver_to_gold.sql
```

The runner script is:

```bash
python scripts/run_gold_transformations.py
```

Verification:

```bash
python scripts/verify_gold_tables.py
```

### Gold Flow

```text
silver.products
silver.customers
silver.stores
silver.campaigns
silver.sales
silver.inventory
silver.returns
        ↓
gold.daily_revenue
gold.product_sales_performance
gold.customer_lifetime_value
gold.store_performance
gold.inventory_risk
gold.campaign_roi
gold.executive_kpis
```

### Reporting Purpose

Gold tables are designed to be read by:

- FastAPI reporting endpoints
- dashboards
- BI tools
- executive summaries

This keeps expensive business aggregations out of the API layer and makes reporting faster.

---

## Error Handling Strategy

The pipeline should fail clearly when critical data is invalid.

Critical failures include:

- missing source files
- invalid dataset config
- invalid JSON files
- missing required fields
- object storage connection failure
- warehouse connection failure
- SQL transformation failure

Non-critical or expected rerun cases may be skipped safely:

- object already loaded
- Bronze duplicate source records
- Silver upsert conflicts handled by business keys

---

## Retry and Idempotency Strategy

The platform supports reruns through:

- unique `run_id` values
- object path partitioning by run ID
- manifest files
- `ops.loaded_files`
- Bronze unique constraints on `(object_name, record_index)`
- Silver `ON CONFLICT` upserts
- Gold `DELETE + INSERT` aggregate refreshes

This makes local development safer and prepares the project for Airflow retries.

---

## Observability

The platform tracks operational data in the `ops` schema.

| Table | Purpose |
|---|---|
| ops.pipeline_runs | Tracks pipeline run status |
| ops.loaded_files | Tracks file load status |
| ops.data_quality_results | Stores data quality results |

Current pipeline scripts print execution summaries to the terminal. Later, Airflow will provide scheduled orchestration, task-level logs, retries, and monitoring.

---

## Current Full Local Pipeline

Run this sequence to go from source generation to Gold analytics:

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

## Future Enhancements

Planned enhancements include:

- Airflow dynamic DAG orchestration
- data quality checks written to `ops.data_quality_results`
- FastAPI reporting endpoints over Gold tables
- Jenkins CI/CD pipeline
- AWS S3 integration
- Redshift loading
- GCS replication
- BigQuery loading
- EC2 spot worker orchestration
- dashboard integration
