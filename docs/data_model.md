# Data Model

## Overview

The data model represents a retail business that sells products through multiple channels and stores.

The main entities are:

- Sales
- Products
- Customers
- Stores
- Inventory
- Campaigns
- Returns

The model is designed to support reporting use cases such as revenue analysis, product performance, customer segmentation, inventory risk, and campaign ROI.

## Entity: Sales

Sales represent customer orders.

### Fields

| Field | Description |
|---|---|
| order_id | Unique order identifier |
| customer_id | Customer who placed the order |
| product_id | Product purchased |
| store_id | Store or channel location |
| campaign_id | Optional campaign attribution |
| channel | Sales channel such as online, store, marketplace |
| quantity | Number of units purchased |
| unit_price | Price per unit |
| discount_amount | Discount applied |
| tax_amount | Tax amount |
| total_amount | Final order amount |
| order_status | completed, cancelled, refunded, pending |
| payment_method | card, cash, wallet, bank_transfer |
| ordered_at | Timestamp of order |

## Entity: Products

Products represent the retail catalog.

### Fields

| Field | Description |
|---|---|
| product_id | Unique product identifier |
| product_name | Product display name |
| category | Product category |
| brand | Product brand |
| cost_price | Internal product cost |
| selling_price | Customer-facing price |
| supplier_id | Supplier identifier |
| created_at | Product creation timestamp |
| is_active | Whether product is active |

## Entity: Customers

Customers represent people or accounts that buy products.

### Fields

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
| loyalty_tier | bronze, silver, gold, platinum |

## Entity: Stores

Stores represent physical stores or online sales locations.

### Fields

| Field | Description |
|---|---|
| store_id | Unique store identifier |
| store_name | Store name |
| region | Sales region |
| city | Store city |
| country | Store country |
| store_type | physical, online, marketplace |
| opened_at | Store opening date |

## Entity: Inventory

Inventory represents product stock by store.

### Fields

| Field | Description |
|---|---|
| inventory_id | Unique inventory record identifier |
| product_id | Product identifier |
| store_id | Store identifier |
| stock_quantity | Current stock count |
| reorder_level | Threshold for reordering |
| last_updated_at | Last inventory update timestamp |

## Entity: Campaigns

Campaigns represent marketing efforts.

### Fields

| Field | Description |
|---|---|
| campaign_id | Unique campaign identifier |
| campaign_name | Campaign name |
| channel | Marketing channel |
| start_date | Campaign start date |
| end_date | Campaign end date |
| budget | Campaign spend |
| target_region | Target region |

## Entity: Returns

Returns represent refunded or returned products.

### Fields

| Field | Description |
|---|---|
| return_id | Unique return identifier |
| order_id | Related order identifier |
| product_id | Returned product |
| return_reason | Reason for return |
| refund_amount | Refunded amount |
| returned_at | Return timestamp |

## Warehouse Layers

### Bronze Layer

Bronze tables store raw ingested data with metadata.

Common metadata fields:

| Field | Description |
|---|---|
| _source_file | Original file path |
| _run_id | Pipeline run identifier |
| _ingested_at | Warehouse ingestion timestamp |
| _payload_hash | Hash used for deduplication/debugging |

### Silver Layer

Silver tables store cleaned and normalized data.

Responsibilities:

- Cast data types
- Remove duplicates
- Standardize values
- Validate required fields
- Prepare relationships between entities

### Gold Layer

Gold tables store business-ready metrics.

Planned Gold models:

| Model | Purpose |
|---|---|
| gold.daily_revenue | Revenue by day |
| gold.product_sales_performance | Product-level sales KPIs |
| gold.customer_lifetime_value | Customer revenue contribution |
| gold.store_performance | Store and regional performance |
| gold.inventory_risk | Low-stock and out-of-stock risks |
| gold.campaign_roi | Marketing return on investment |
| gold.executive_kpis | Executive summary metrics |

## Local Warehouse Foundation

The local warehouse is implemented in PostgreSQL and initialized with:

```text
warehouse/local_postgres/ddl/001_create_warehouse_schemas.sql