# Pipeline Design

## Overview

The platform uses Airflow to orchestrate data pipelines from ingestion to analytics reporting.

The pipeline is designed to be configuration-driven. Dataset definitions are stored in `config/datasets.yml`, and Airflow dynamically creates tasks based on those definitions.

## Pipeline Goals

The main goals are:

1. Ingest raw retail data
2. Store raw files in object storage
3. Validate data quality
4. Load raw data into Bronze warehouse tables
5. Transform Bronze data into Silver tables
6. Transform Silver data into Gold analytics models
7. Track pipeline metadata and errors
8. Support future AWS and GCP deployment

## Pipeline Stages

### 1. Start Pipeline Run

The pipeline starts by creating a unique run ID.

The run ID is used across files, logs, warehouse rows, and audit tables.

Example:

```text
run_id=20260430T104500Z
```

## Object Storage Layer

The platform includes a provider-independent object storage interface located in:

```text
storage/object_storage_client.py
```

## Bronze Ingestion Pipeline

The Bronze ingestion pipeline is implemented in:

```text
ingestion/ingest_to_object_storage.py