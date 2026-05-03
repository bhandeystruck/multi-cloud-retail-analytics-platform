-- ============================================================
-- Silver Tables
--
-- Purpose:
-- Silver tables convert raw JSONB Bronze records into typed,
-- cleaned, relational tables.
--
-- Bronze preserves source shape.
-- Silver standardizes the data for joins and analytics.
--
-- Design principles:
-- - One clean row per business entity/transaction.
-- - Stronger data types.
-- - Primary keys based on business identifiers.
-- - Metadata columns for traceability back to Bronze.
-- ============================================================


-- ============================================================
-- silver.products
-- ============================================================

CREATE TABLE IF NOT EXISTS silver.products (
    product_id TEXT PRIMARY KEY,
    product_name TEXT NOT NULL,
    category TEXT NOT NULL,
    brand TEXT NOT NULL,
    cost_price NUMERIC(12, 2) NOT NULL,
    selling_price NUMERIC(12, 2) NOT NULL,
    supplier_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    is_active BOOLEAN NOT NULL,
    source_run_id TEXT NOT NULL,
    source_object_name TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT products_cost_price_check
        CHECK (cost_price >= 0),

    CONSTRAINT products_selling_price_check
        CHECK (selling_price > 0),

    CONSTRAINT products_margin_check
        CHECK (selling_price > cost_price)
);


CREATE INDEX IF NOT EXISTS idx_silver_products_category
    ON silver.products (category);


CREATE INDEX IF NOT EXISTS idx_silver_products_brand
    ON silver.products (brand);


-- ============================================================
-- silver.customers
-- ============================================================

CREATE TABLE IF NOT EXISTS silver.customers (
    customer_id TEXT PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT NOT NULL,
    phone TEXT NOT NULL,
    city TEXT NOT NULL,
    state TEXT NOT NULL,
    country TEXT NOT NULL,
    signup_date DATE NOT NULL,
    loyalty_tier TEXT NOT NULL,
    source_run_id TEXT NOT NULL,
    source_object_name TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT customers_email_unique
        UNIQUE (email),

    CONSTRAINT customers_loyalty_tier_check
        CHECK (loyalty_tier IN ('bronze', 'silver', 'gold', 'platinum'))
);


CREATE INDEX IF NOT EXISTS idx_silver_customers_country
    ON silver.customers (country);


CREATE INDEX IF NOT EXISTS idx_silver_customers_loyalty_tier
    ON silver.customers (loyalty_tier);


-- ============================================================
-- silver.stores
-- ============================================================

CREATE TABLE IF NOT EXISTS silver.stores (
    store_id TEXT PRIMARY KEY,
    store_name TEXT NOT NULL,
    region TEXT NOT NULL,
    city TEXT NOT NULL,
    country TEXT NOT NULL,
    store_type TEXT NOT NULL,
    opened_at DATE NOT NULL,
    source_run_id TEXT NOT NULL,
    source_object_name TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT stores_store_type_check
        CHECK (store_type IN ('physical', 'online', 'marketplace'))
);


CREATE INDEX IF NOT EXISTS idx_silver_stores_region
    ON silver.stores (region);


CREATE INDEX IF NOT EXISTS idx_silver_stores_country
    ON silver.stores (country);


-- ============================================================
-- silver.campaigns
-- ============================================================

CREATE TABLE IF NOT EXISTS silver.campaigns (
    campaign_id TEXT PRIMARY KEY,
    campaign_name TEXT NOT NULL,
    channel TEXT NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    budget NUMERIC(14, 2) NOT NULL,
    target_region TEXT NOT NULL,
    source_run_id TEXT NOT NULL,
    source_object_name TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT campaigns_budget_check
        CHECK (budget > 0),

    CONSTRAINT campaigns_date_check
        CHECK (end_date >= start_date)
);


CREATE INDEX IF NOT EXISTS idx_silver_campaigns_channel
    ON silver.campaigns (channel);


CREATE INDEX IF NOT EXISTS idx_silver_campaigns_target_region
    ON silver.campaigns (target_region);


-- ============================================================
-- silver.sales
-- ============================================================

CREATE TABLE IF NOT EXISTS silver.sales (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    store_id TEXT NOT NULL,
    campaign_id TEXT,
    channel TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(12, 2) NOT NULL,
    discount_amount NUMERIC(12, 2) NOT NULL,
    tax_amount NUMERIC(12, 2) NOT NULL,
    total_amount NUMERIC(12, 2) NOT NULL,
    order_status TEXT NOT NULL,
    payment_method TEXT NOT NULL,
    ordered_at TIMESTAMPTZ NOT NULL,
    source_run_id TEXT NOT NULL,
    source_object_name TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT sales_quantity_check
        CHECK (quantity > 0),

    CONSTRAINT sales_unit_price_check
        CHECK (unit_price > 0),

    CONSTRAINT sales_discount_check
        CHECK (discount_amount >= 0),

    CONSTRAINT sales_tax_check
        CHECK (tax_amount >= 0),

    CONSTRAINT sales_total_amount_check
        CHECK (total_amount >= 0),

    CONSTRAINT sales_order_status_check
        CHECK (order_status IN ('completed', 'cancelled', 'refunded', 'pending')),

    CONSTRAINT sales_payment_method_check
        CHECK (payment_method IN ('card', 'cash', 'wallet', 'bank_transfer')),

    CONSTRAINT sales_channel_check
        CHECK (channel IN ('online', 'store', 'marketplace', 'mobile_app'))
);


CREATE INDEX IF NOT EXISTS idx_silver_sales_customer_id
    ON silver.sales (customer_id);


CREATE INDEX IF NOT EXISTS idx_silver_sales_product_id
    ON silver.sales (product_id);


CREATE INDEX IF NOT EXISTS idx_silver_sales_store_id
    ON silver.sales (store_id);


CREATE INDEX IF NOT EXISTS idx_silver_sales_campaign_id
    ON silver.sales (campaign_id);


CREATE INDEX IF NOT EXISTS idx_silver_sales_ordered_at
    ON silver.sales (ordered_at);


CREATE INDEX IF NOT EXISTS idx_silver_sales_order_status
    ON silver.sales (order_status);


-- ============================================================
-- silver.inventory
-- ============================================================

CREATE TABLE IF NOT EXISTS silver.inventory (
    inventory_id TEXT PRIMARY KEY,
    product_id TEXT NOT NULL,
    store_id TEXT NOT NULL,
    stock_quantity INTEGER NOT NULL,
    reorder_level INTEGER NOT NULL,
    last_updated_at TIMESTAMPTZ NOT NULL,
    source_run_id TEXT NOT NULL,
    source_object_name TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT inventory_stock_quantity_check
        CHECK (stock_quantity >= 0),

    CONSTRAINT inventory_reorder_level_check
        CHECK (reorder_level >= 0)
);


CREATE INDEX IF NOT EXISTS idx_silver_inventory_product_id
    ON silver.inventory (product_id);


CREATE INDEX IF NOT EXISTS idx_silver_inventory_store_id
    ON silver.inventory (store_id);


-- ============================================================
-- silver.returns
-- ============================================================

CREATE TABLE IF NOT EXISTS silver.returns (
    return_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    return_reason TEXT NOT NULL,
    refund_amount NUMERIC(12, 2) NOT NULL,
    returned_at TIMESTAMPTZ NOT NULL,
    source_run_id TEXT NOT NULL,
    source_object_name TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT returns_refund_amount_check
        CHECK (refund_amount >= 0)
);


CREATE INDEX IF NOT EXISTS idx_silver_returns_order_id
    ON silver.returns (order_id);


CREATE INDEX IF NOT EXISTS idx_silver_returns_product_id
    ON silver.returns (product_id);


CREATE INDEX IF NOT EXISTS idx_silver_returns_returned_at
    ON silver.returns (returned_at);