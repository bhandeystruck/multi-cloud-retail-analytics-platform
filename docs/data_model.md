# Data Model

## Overview

The Multi-Cloud Retail Analytics Data Platform models a retail business that sells products through multiple channels and stores.

The data model supports analytics use cases such as:

- revenue analysis
- product performance
- customer lifetime value
- store performance
- inventory risk
- campaign ROI
- executive KPI reporting

The platform uses a layered warehouse design:

```text
Bronze → Silver → Gold
```

Each layer has a clear purpose.

| Layer | Purpose |
|---|---|
| Bronze | Preserve raw source records as JSONB |
| Silver | Convert raw records into clean typed relational tables |
| Gold | Create business-ready analytics models and KPIs |

The local warehouse is implemented in PostgreSQL. Later, this design can map to AWS Redshift and GCP BigQuery.

---

## Core Retail Entities

The platform currently models these retail datasets:

- Sales
- Products
- Customers
- Stores
- Inventory
- Campaigns
- Returns

---

## Entity: Sales

Sales represent customer transactions.

| Field | Description |
|---|---|
| order_id | Unique order identifier |
| customer_id | Customer who placed the order |
| product_id | Product purchased |
| store_id | Store or channel location |
| campaign_id | Optional marketing campaign attribution |
| channel | Sales channel such as online, store, marketplace, or mobile app |
| quantity | Number of units purchased |
| unit_price | Price per unit |
| discount_amount | Discount applied |
| tax_amount | Tax amount |
| total_amount | Final order amount |
| order_status | completed, cancelled, refunded, or pending |
| payment_method | card, cash, wallet, or bank_transfer |
| ordered_at | Timestamp of order |

---

## Entity: Products

Products represent the retail catalog.

| Field | Description |
|---|---|
| product_id | Unique product identifier |
| product_name | Product display name |
| category | Product category |
| brand | Product brand |
| cost_price | Internal product cost |
| selling_price | Customer-facing selling price |
| supplier_id | Supplier identifier |
| created_at | Product creation timestamp |
| is_active | Whether the product is active |

---

## Entity: Customers

Customers represent people or accounts that buy products.

| Field | Description |
|---|---|
| customer_id | Unique customer identifier |
| first_name | Customer first name |
| last_name | Customer last name |
| email | Customer email |
| phone | Customer phone |
| city | City |
| state | State or province |
| country | Country |
| signup_date | Date the customer signed up |
| loyalty_tier | bronze, silver, gold, or platinum |

---

## Entity: Stores

Stores represent physical stores, online stores, or marketplaces.

| Field | Description |
|---|---|
| store_id | Unique store identifier |
| store_name | Store name |
| region | Sales region |
| city | Store city |
| country | Store country |
| store_type | physical, online, or marketplace |
| opened_at | Store opening date |

---

## Entity: Inventory

Inventory represents product stock by store.

| Field | Description |
|---|---|
| inventory_id | Unique inventory record identifier |
| product_id | Product identifier |
| store_id | Store identifier |
| stock_quantity | Current stock count |
| reorder_level | Threshold for reordering |
| last_updated_at | Last inventory update timestamp |

---

## Entity: Campaigns

Campaigns represent marketing efforts.

| Field | Description |
|---|---|
| campaign_id | Unique campaign identifier |
| campaign_name | Campaign name |
| channel | Marketing channel |
| start_date | Campaign start date |
| end_date | Campaign end date |
| budget | Campaign spend |
| target_region | Target region |

---

## Entity: Returns

Returns represent refunded or returned products.

| Field | Description |
|---|---|
| return_id | Unique return identifier |
| order_id | Related order identifier |
| product_id | Returned product |
| return_reason | Reason for return |
| refund_amount | Refunded amount |
| returned_at | Return timestamp |

---

## Generated Source Data

The project includes a local source data generator located at:

```text
ingestion/generate_retail_data.py
```

The generator creates realistic interconnected source datasets.

| Dataset | Output File | Purpose |
|---|---|---|
| Products | data/generated/products.json | Product catalog |
| Customers | data/generated/customers.json | Customer profiles |
| Stores | data/generated/stores.json | Retail locations |
| Campaigns | data/generated/campaigns.json | Marketing campaigns |
| Sales | data/generated/sales.json | Transaction records |
| Inventory | data/generated/inventory.json | Stock levels |
| Returns | data/generated/returns.json | Refund and return records |

The generated datasets preserve relationships between records.

Examples:

- `sales.product_id` references `products.product_id`
- `sales.customer_id` references `customers.customer_id`
- `sales.store_id` references `stores.store_id`
- `sales.campaign_id` optionally references `campaigns.campaign_id`
- `returns.order_id` references `sales.order_id`
- `inventory.product_id` references `products.product_id`
- `inventory.store_id` references `stores.store_id`

---

## Local Warehouse Foundation

The local warehouse is implemented in PostgreSQL and initialized with:

```text
warehouse/local_postgres/ddl/001_create_warehouse_schemas.sql
```

The warehouse uses four schemas:

| Schema | Purpose |
|---|---|
| bronze | Raw records loaded from object storage |
| silver | Cleaned and normalized records |
| gold | Business-ready analytics models |
| ops | Pipeline metadata, audit logs, and data quality results |

---

## Bronze Layer

Bronze tables store raw records loaded from object storage. Bronze is designed for raw data preservation, not business reporting.

Each Bronze table stores one source JSON record per row.

Bronze tables include:

| Column | Purpose |
|---|---|
| run_id | Pipeline execution identifier |
| source_file | Original local source file path if available |
| bucket_name | Object storage bucket |
| object_name | Object storage key/path |
| record_index | Record position inside the source file |
| payload | Raw JSONB record |
| payload_hash | Hash of the raw record payload |
| ingested_at | Warehouse ingestion timestamp |

Bronze raw tables:

| Table | Purpose |
|---|---|
| bronze.raw_sales | Raw sales records |
| bronze.raw_products | Raw product records |
| bronze.raw_customers | Raw customer records |
| bronze.raw_stores | Raw store records |
| bronze.raw_inventory | Raw inventory records |
| bronze.raw_campaigns | Raw campaign records |
| bronze.raw_returns | Raw return records |

The Bronze layer allows multiple raw copies across runs. This is intentional because it preserves history and supports replay.

---

## Operational Tables

The `ops` schema stores audit and pipeline metadata.

| Table | Purpose |
|---|---|
| ops.pipeline_runs | Tracks pipeline execution status |
| ops.loaded_files | Tracks files prepared for or loaded into the warehouse |
| ops.data_quality_results | Tracks validation results |

These tables make the platform auditable and prepare it for Airflow orchestration.

---

## Silver Layer

The Silver layer converts raw Bronze JSONB records into typed relational tables.

Silver tables are created by:

```text
warehouse/local_postgres/ddl/002_create_silver_tables.sql
```

Silver transformations are defined in:

```text
transformations/silver/001_transform_bronze_to_silver.sql
```

The transformation runner is:

```text
scripts/run_silver_transformations.py
```

Silver tables:

| Table | Purpose |
|---|---|
| silver.products | Clean product catalog |
| silver.customers | Clean customer profiles |
| silver.stores | Clean retail locations |
| silver.campaigns | Clean marketing campaigns |
| silver.sales | Clean sales transactions |
| silver.inventory | Clean stock records |
| silver.returns | Clean return/refund records |

Silver tables provide:

- proper SQL data types
- primary keys
- business-friendly columns
- easier joins
- cleaner downstream transformations
- traceability back to source run/object

Each Silver table includes metadata columns:

| Column | Purpose |
|---|---|
| source_run_id | Links back to the pipeline run |
| source_object_name | Links back to the Bronze object file |
| loaded_at | Timestamp when the Silver row was loaded |

Silver is deduplicated by business key. Bronze may contain multiple copies across runs, but Silver keeps one clean current row per business key.

---

## Gold Layer

The Gold layer stores business-ready analytics models.

Gold tables are created by:

```text
warehouse/local_postgres/ddl/003_create_gold_tables.sql
```

Gold transformations are defined in:

```text
transformations/gold/001_transform_silver_to_gold.sql
```

The runner script is:

```text
scripts/run_gold_transformations.py
```

Gold tables:

| Table | Purpose |
|---|---|
| gold.daily_revenue | Daily revenue and order metrics |
| gold.product_sales_performance | Product-level sales, margin, and return KPIs |
| gold.customer_lifetime_value | Customer spend and order behavior |
| gold.store_performance | Store and region performance |
| gold.inventory_risk | Low-stock and out-of-stock monitoring |
| gold.campaign_roi | Campaign attribution and ROI |
| gold.executive_kpis | Executive summary snapshot |

Gold models answer questions like:

- How much revenue did we make?
- Which products are top sellers?
- Which customers are most valuable?
- Which stores perform best?
- Which inventory items are at risk?
- Which campaigns generate the best ROI?

Gold tables will later power FastAPI reporting endpoints.
