-- ============================================================
-- Local Warehouse Foundation
--
-- Purpose:
-- This file creates the local PostgreSQL warehouse structure for
-- the Multi-Cloud Retail Analytics Data Platform.
--
-- Local PostgreSQL acts as our development warehouse simulation.
-- Later, these concepts map to:
-- - AWS Redshift
-- - GCP BigQuery
--
-- Warehouse layers:
-- - bronze: raw records loaded from object storage
-- - silver: cleaned and normalized records
-- - gold: business-ready reporting models
-- - ops: operational metadata, audit logs, and data quality results
-- ============================================================


-- ============================================================
-- Schemas
-- ============================================================

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS ops;


-- ============================================================
-- OPS: Pipeline Runs
--
-- Stores one row per pipeline execution.
--
-- Why this table matters:
-- - Tracks pipeline run status.
-- - Helps debug failures.
-- - Gives Airflow and future loaders a place to record execution metadata.
-- ============================================================

CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    run_id TEXT PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT pipeline_runs_status_check
        CHECK (status IN ('running', 'success', 'failed', 'skipped'))
);


CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status
    ON ops.pipeline_runs (status);


CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started_at
    ON ops.pipeline_runs (started_at);


-- ============================================================
-- OPS: Loaded Files
--
-- Stores one row per file loaded or prepared for loading.
--
-- Why this table matters:
-- - Prevents duplicate warehouse loads.
-- - Tracks source object paths from Bronze storage.
-- - Stores file hashes for integrity and deduplication.
-- ============================================================

CREATE TABLE IF NOT EXISTS ops.loaded_files (
    loaded_file_id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES ops.pipeline_runs(run_id),
    dataset_name TEXT NOT NULL,
    bucket_name TEXT NOT NULL,
    object_name TEXT NOT NULL,
    source_file TEXT,
    record_count INTEGER NOT NULL DEFAULT 0,
    file_size_bytes BIGINT NOT NULL DEFAULT 0,
    content_sha256 TEXT NOT NULL,
    load_status TEXT NOT NULL DEFAULT 'pending',
    loaded_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    error_message TEXT,

    CONSTRAINT loaded_files_status_check
        CHECK (load_status IN ('pending', 'loaded', 'failed', 'skipped')),

    CONSTRAINT loaded_files_record_count_check
        CHECK (record_count >= 0),

    CONSTRAINT loaded_files_size_check
        CHECK (file_size_bytes >= 0),

    CONSTRAINT loaded_files_unique_object
        UNIQUE (bucket_name, object_name)
);


CREATE INDEX IF NOT EXISTS idx_loaded_files_run_id
    ON ops.loaded_files (run_id);


CREATE INDEX IF NOT EXISTS idx_loaded_files_dataset_name
    ON ops.loaded_files (dataset_name);


CREATE INDEX IF NOT EXISTS idx_loaded_files_load_status
    ON ops.loaded_files (load_status);


CREATE INDEX IF NOT EXISTS idx_loaded_files_content_sha256
    ON ops.loaded_files (content_sha256);


-- ============================================================
-- OPS: Data Quality Results
--
-- Stores validation outcomes.
--
-- Why this table matters:
-- - Gives us visibility into data quality checks.
-- - Records failures without hiding them in logs only.
-- - Supports future dashboarding and alerting.
-- ============================================================

CREATE TABLE IF NOT EXISTS ops.data_quality_results (
    data_quality_result_id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES ops.pipeline_runs(run_id),
    dataset_name TEXT NOT NULL,
    check_name TEXT NOT NULL,
    check_status TEXT NOT NULL,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    records_checked INTEGER,
    records_failed INTEGER,
    details JSONB,

    CONSTRAINT data_quality_status_check
        CHECK (check_status IN ('passed', 'failed', 'warning')),

    CONSTRAINT data_quality_records_checked_check
        CHECK (records_checked IS NULL OR records_checked >= 0),

    CONSTRAINT data_quality_records_failed_check
        CHECK (records_failed IS NULL OR records_failed >= 0)
);


CREATE INDEX IF NOT EXISTS idx_data_quality_results_run_id
    ON ops.data_quality_results (run_id);


CREATE INDEX IF NOT EXISTS idx_data_quality_results_dataset_name
    ON ops.data_quality_results (dataset_name);


CREATE INDEX IF NOT EXISTS idx_data_quality_results_check_status
    ON ops.data_quality_results (check_status);


-- ============================================================
-- Bronze Raw Tables
--
-- Design:
-- Each Bronze table stores one raw JSON record per row.
--
-- Why JSONB?
-- - Bronze should preserve source shape.
-- - We do not want to over-clean raw records at this layer.
-- - Silver transformations will later extract typed columns.
--
-- Common metadata fields:
-- - run_id: pipeline execution
-- - source_file: local file path if available
-- - bucket_name: object storage bucket
-- - object_name: object storage path/key
-- - record_index: position of the record inside the source file
-- - payload: raw JSON object
-- - payload_hash: hash of individual record payload
-- - ingested_at: warehouse ingestion timestamp
-- ============================================================


CREATE TABLE IF NOT EXISTS bronze.raw_sales (
    raw_sales_id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES ops.pipeline_runs(run_id),
    source_file TEXT,
    bucket_name TEXT NOT NULL,
    object_name TEXT NOT NULL,
    record_index INTEGER NOT NULL,
    payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT raw_sales_record_index_check
        CHECK (record_index >= 0),

    CONSTRAINT raw_sales_unique_record
        UNIQUE (object_name, record_index)
);


CREATE TABLE IF NOT EXISTS bronze.raw_products (
    raw_products_id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES ops.pipeline_runs(run_id),
    source_file TEXT,
    bucket_name TEXT NOT NULL,
    object_name TEXT NOT NULL,
    record_index INTEGER NOT NULL,
    payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT raw_products_record_index_check
        CHECK (record_index >= 0),

    CONSTRAINT raw_products_unique_record
        UNIQUE (object_name, record_index)
);


CREATE TABLE IF NOT EXISTS bronze.raw_customers (
    raw_customers_id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES ops.pipeline_runs(run_id),
    source_file TEXT,
    bucket_name TEXT NOT NULL,
    object_name TEXT NOT NULL,
    record_index INTEGER NOT NULL,
    payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT raw_customers_record_index_check
        CHECK (record_index >= 0),

    CONSTRAINT raw_customers_unique_record
        UNIQUE (object_name, record_index)
);


CREATE TABLE IF NOT EXISTS bronze.raw_stores (
    raw_stores_id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES ops.pipeline_runs(run_id),
    source_file TEXT,
    bucket_name TEXT NOT NULL,
    object_name TEXT NOT NULL,
    record_index INTEGER NOT NULL,
    payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT raw_stores_record_index_check
        CHECK (record_index >= 0),

    CONSTRAINT raw_stores_unique_record
        UNIQUE (object_name, record_index)
);


CREATE TABLE IF NOT EXISTS bronze.raw_inventory (
    raw_inventory_id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES ops.pipeline_runs(run_id),
    source_file TEXT,
    bucket_name TEXT NOT NULL,
    object_name TEXT NOT NULL,
    record_index INTEGER NOT NULL,
    payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT raw_inventory_record_index_check
        CHECK (record_index >= 0),

    CONSTRAINT raw_inventory_unique_record
        UNIQUE (object_name, record_index)
);


CREATE TABLE IF NOT EXISTS bronze.raw_campaigns (
    raw_campaigns_id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES ops.pipeline_runs(run_id),
    source_file TEXT,
    bucket_name TEXT NOT NULL,
    object_name TEXT NOT NULL,
    record_index INTEGER NOT NULL,
    payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT raw_campaigns_record_index_check
        CHECK (record_index >= 0),

    CONSTRAINT raw_campaigns_unique_record
        UNIQUE (object_name, record_index)
);


CREATE TABLE IF NOT EXISTS bronze.raw_returns (
    raw_returns_id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES ops.pipeline_runs(run_id),
    source_file TEXT,
    bucket_name TEXT NOT NULL,
    object_name TEXT NOT NULL,
    record_index INTEGER NOT NULL,
    payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT raw_returns_record_index_check
        CHECK (record_index >= 0),

    CONSTRAINT raw_returns_unique_record
        UNIQUE (object_name, record_index)
);


-- ============================================================
-- Bronze Indexes
--
-- These indexes support future loading, debugging, and filtering.
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_raw_sales_run_id
    ON bronze.raw_sales (run_id);

CREATE INDEX IF NOT EXISTS idx_raw_sales_object_name
    ON bronze.raw_sales (object_name);

CREATE INDEX IF NOT EXISTS idx_raw_products_run_id
    ON bronze.raw_products (run_id);

CREATE INDEX IF NOT EXISTS idx_raw_products_object_name
    ON bronze.raw_products (object_name);

CREATE INDEX IF NOT EXISTS idx_raw_customers_run_id
    ON bronze.raw_customers (run_id);

CREATE INDEX IF NOT EXISTS idx_raw_customers_object_name
    ON bronze.raw_customers (object_name);

CREATE INDEX IF NOT EXISTS idx_raw_stores_run_id
    ON bronze.raw_stores (run_id);

CREATE INDEX IF NOT EXISTS idx_raw_stores_object_name
    ON bronze.raw_stores (object_name);

CREATE INDEX IF NOT EXISTS idx_raw_inventory_run_id
    ON bronze.raw_inventory (run_id);

CREATE INDEX IF NOT EXISTS idx_raw_inventory_object_name
    ON bronze.raw_inventory (object_name);

CREATE INDEX IF NOT EXISTS idx_raw_campaigns_run_id
    ON bronze.raw_campaigns (run_id);

CREATE INDEX IF NOT EXISTS idx_raw_campaigns_object_name
    ON bronze.raw_campaigns (object_name);

CREATE INDEX IF NOT EXISTS idx_raw_returns_run_id
    ON bronze.raw_returns (run_id);

CREATE INDEX IF NOT EXISTS idx_raw_returns_object_name
    ON bronze.raw_returns (object_name);