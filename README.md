# Multi-Cloud Retail Analytics Data Platform

A data engineering and backend platform for retail analytics, designed to demonstrate cloud infrastructure, workflow orchestration, data warehousing, SQL analytics, API development, and DevOps automation.

## Project Goal

This project simulates a real retail company's analytics platform.

It ingests raw retail data, stores it in an object-storage data lake, orchestrates processing with Airflow, loads data into warehouse layers, transforms raw records into analytics-ready models, replicates data across cloud providers, and exposes business KPIs through FastAPI.

## Business Problem

Retail companies often collect data from many systems:

- Online orders
- Physical stores
- Product catalogs
- Customer profiles
- Inventory systems
- Marketing campaigns
- Returns and refunds

Without a centralized analytics platform, business teams struggle to answer important questions:

- What is daily revenue?
- Which products are top sellers?
- Which customers are repeat buyers?
- Which stores are underperforming?
- Which inventory items are at risk?
- Which campaigns generate the best ROI?

This project solves that problem by creating a structured data platform from ingestion to reporting.

## High-Level Architecture

```text
Retail Source Data
        |
        v
Python Ingestion Service
        |
        v
Object Storage Data Lake
MinIO locally / AWS S3 in cloud
        |
        v
Airflow Dynamic DAGs
        |
        v
Warehouse Layers
Bronze -> Silver -> Gold
        |
        v
Cross-Cloud Replication
GCS / BigQuery
        |
        v
FastAPI Reporting API
        |
        v
Analytics Consumers / Dashboards

```

## Local Development

- Python 3.11+
- Docker and Docker Compose
- MinIO for S3-compatible object storage
- PostgreSQL as a local warehouse simulation
- Apache Airflow for orchestration
- FastAPI for reporting APIs
- pytest for testing
- Ruff for linting
- mypy for type checking