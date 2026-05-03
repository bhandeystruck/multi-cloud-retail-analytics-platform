-- ============================================================
-- Gold Analytics Tables
--
-- Purpose:
-- Gold tables store business-ready analytics models.
--
-- Bronze = raw preservation
-- Silver = cleaned relational data
-- Gold   = business KPIs and reporting models
--
-- These tables are designed to power:
-- - FastAPI reporting endpoints
-- - dashboards
-- - executive summaries
-- - business analytics
-- ============================================================


-- ============================================================
-- gold.daily_revenue
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.daily_revenue (
    revenue_date DATE PRIMARY KEY,
    total_orders INTEGER NOT NULL,
    completed_orders INTEGER NOT NULL,
    cancelled_orders INTEGER NOT NULL,
    refunded_orders INTEGER NOT NULL,
    gross_revenue NUMERIC(14, 2) NOT NULL,
    discount_total NUMERIC(14, 2) NOT NULL,
    tax_total NUMERIC(14, 2) NOT NULL,
    net_revenue NUMERIC(14, 2) NOT NULL,
    average_order_value NUMERIC(14, 2) NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


-- ============================================================
-- gold.product_sales_performance
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.product_sales_performance (
    product_id TEXT PRIMARY KEY,
    product_name TEXT NOT NULL,
    category TEXT NOT NULL,
    brand TEXT NOT NULL,
    units_sold INTEGER NOT NULL,
    total_orders INTEGER NOT NULL,
    gross_revenue NUMERIC(14, 2) NOT NULL,
    estimated_gross_margin NUMERIC(14, 2) NOT NULL,
    return_count INTEGER NOT NULL,
    return_rate NUMERIC(8, 4) NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE INDEX IF NOT EXISTS idx_gold_product_sales_category
    ON gold.product_sales_performance (category);


CREATE INDEX IF NOT EXISTS idx_gold_product_sales_revenue
    ON gold.product_sales_performance (gross_revenue DESC);


-- ============================================================
-- gold.customer_lifetime_value
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.customer_lifetime_value (
    customer_id TEXT PRIMARY KEY,
    customer_name TEXT NOT NULL,
    email TEXT NOT NULL,
    country TEXT NOT NULL,
    loyalty_tier TEXT NOT NULL,
    total_orders INTEGER NOT NULL,
    completed_orders INTEGER NOT NULL,
    total_spent NUMERIC(14, 2) NOT NULL,
    average_order_value NUMERIC(14, 2) NOT NULL,
    first_order_at TIMESTAMPTZ,
    most_recent_order_at TIMESTAMPTZ,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE INDEX IF NOT EXISTS idx_gold_customer_ltv_total_spent
    ON gold.customer_lifetime_value (total_spent DESC);


CREATE INDEX IF NOT EXISTS idx_gold_customer_ltv_loyalty_tier
    ON gold.customer_lifetime_value (loyalty_tier);


-- ============================================================
-- gold.store_performance
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.store_performance (
    store_id TEXT PRIMARY KEY,
    store_name TEXT NOT NULL,
    region TEXT NOT NULL,
    city TEXT NOT NULL,
    country TEXT NOT NULL,
    store_type TEXT NOT NULL,
    total_orders INTEGER NOT NULL,
    completed_orders INTEGER NOT NULL,
    gross_revenue NUMERIC(14, 2) NOT NULL,
    average_order_value NUMERIC(14, 2) NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE INDEX IF NOT EXISTS idx_gold_store_performance_region
    ON gold.store_performance (region);


CREATE INDEX IF NOT EXISTS idx_gold_store_performance_revenue
    ON gold.store_performance (gross_revenue DESC);


-- ============================================================
-- gold.inventory_risk
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.inventory_risk (
    inventory_id TEXT PRIMARY KEY,
    product_id TEXT NOT NULL,
    product_name TEXT NOT NULL,
    category TEXT NOT NULL,
    store_id TEXT NOT NULL,
    store_name TEXT NOT NULL,
    region TEXT NOT NULL,
    stock_quantity INTEGER NOT NULL,
    reorder_level INTEGER NOT NULL,
    stock_status TEXT NOT NULL,
    estimated_stock_value NUMERIC(14, 2) NOT NULL,
    last_updated_at TIMESTAMPTZ NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT inventory_risk_status_check
        CHECK (stock_status IN ('out_of_stock', 'low_stock', 'healthy'))
);


CREATE INDEX IF NOT EXISTS idx_gold_inventory_risk_status
    ON gold.inventory_risk (stock_status);


CREATE INDEX IF NOT EXISTS idx_gold_inventory_risk_category
    ON gold.inventory_risk (category);


-- ============================================================
-- gold.campaign_roi
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.campaign_roi (
    campaign_id TEXT PRIMARY KEY,
    campaign_name TEXT NOT NULL,
    channel TEXT NOT NULL,
    target_region TEXT NOT NULL,
    budget NUMERIC(14, 2) NOT NULL,
    attributed_orders INTEGER NOT NULL,
    attributed_revenue NUMERIC(14, 2) NOT NULL,
    estimated_roi NUMERIC(14, 4) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE INDEX IF NOT EXISTS idx_gold_campaign_roi_channel
    ON gold.campaign_roi (channel);


CREATE INDEX IF NOT EXISTS idx_gold_campaign_roi_estimated_roi
    ON gold.campaign_roi (estimated_roi DESC);


-- ============================================================
-- gold.executive_kpis
--
-- Design:
-- This table stores one row per refresh snapshot.
--
-- Why not a single-row table?
-- Keeping snapshots lets us track KPI movement over time later.
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.executive_kpis (
    snapshot_at TIMESTAMPTZ PRIMARY KEY,
    total_revenue NUMERIC(14, 2) NOT NULL,
    total_orders INTEGER NOT NULL,
    completed_orders INTEGER NOT NULL,
    total_customers INTEGER NOT NULL,
    active_products INTEGER NOT NULL,
    low_stock_items INTEGER NOT NULL,
    out_of_stock_items INTEGER NOT NULL,
    average_order_value NUMERIC(14, 2) NOT NULL,
    refund_amount NUMERIC(14, 2) NOT NULL,
    return_count INTEGER NOT NULL
);