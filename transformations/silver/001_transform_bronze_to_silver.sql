-- ============================================================
-- Bronze to Silver Transformations
--
-- Purpose:
-- Convert raw JSONB Bronze records into typed relational Silver tables.
--
-- Important:
-- Bronze may contain multiple raw versions of the same business record
-- because ingestion can run multiple times.
--
-- To avoid PostgreSQL's:
--   ON CONFLICT DO UPDATE command cannot affect row a second time
--
-- each section uses DISTINCT ON (business_primary_key) to select only one
-- source row per primary key before inserting into Silver.
--
-- We keep the most recently ingested Bronze record.
-- ============================================================


-- ============================================================
-- Products
-- ============================================================

WITH deduped_products AS (
    SELECT DISTINCT ON (payload ->> 'product_id')
        payload,
        run_id,
        object_name,
        ingested_at
    FROM bronze.raw_products
    WHERE payload ? 'product_id'
    ORDER BY
        payload ->> 'product_id',
        ingested_at DESC,
        record_index DESC
)
INSERT INTO silver.products (
    product_id,
    product_name,
    category,
    brand,
    cost_price,
    selling_price,
    supplier_id,
    created_at,
    is_active,
    source_run_id,
    source_object_name
)
SELECT
    payload ->> 'product_id' AS product_id,
    payload ->> 'product_name' AS product_name,
    payload ->> 'category' AS category,
    payload ->> 'brand' AS brand,
    (payload ->> 'cost_price')::NUMERIC(12, 2) AS cost_price,
    (payload ->> 'selling_price')::NUMERIC(12, 2) AS selling_price,
    payload ->> 'supplier_id' AS supplier_id,
    (payload ->> 'created_at')::TIMESTAMPTZ AS created_at,
    (payload ->> 'is_active')::BOOLEAN AS is_active,
    run_id AS source_run_id,
    object_name AS source_object_name
FROM deduped_products
ON CONFLICT (product_id)
DO UPDATE SET
    product_name = EXCLUDED.product_name,
    category = EXCLUDED.category,
    brand = EXCLUDED.brand,
    cost_price = EXCLUDED.cost_price,
    selling_price = EXCLUDED.selling_price,
    supplier_id = EXCLUDED.supplier_id,
    created_at = EXCLUDED.created_at,
    is_active = EXCLUDED.is_active,
    source_run_id = EXCLUDED.source_run_id,
    source_object_name = EXCLUDED.source_object_name,
    loaded_at = NOW();


-- ============================================================
-- Customers
-- ============================================================

WITH deduped_customers AS (
    SELECT DISTINCT ON (payload ->> 'customer_id')
        payload,
        run_id,
        object_name,
        ingested_at
    FROM bronze.raw_customers
    WHERE payload ? 'customer_id'
    ORDER BY
        payload ->> 'customer_id',
        ingested_at DESC,
        record_index DESC
)
INSERT INTO silver.customers (
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    city,
    state,
    country,
    signup_date,
    loyalty_tier,
    source_run_id,
    source_object_name
)
SELECT
    payload ->> 'customer_id' AS customer_id,
    payload ->> 'first_name' AS first_name,
    payload ->> 'last_name' AS last_name,
    LOWER(payload ->> 'email') AS email,
    payload ->> 'phone' AS phone,
    payload ->> 'city' AS city,
    payload ->> 'state' AS state,
    payload ->> 'country' AS country,
    (payload ->> 'signup_date')::DATE AS signup_date,
    LOWER(payload ->> 'loyalty_tier') AS loyalty_tier,
    run_id AS source_run_id,
    object_name AS source_object_name
FROM deduped_customers
ON CONFLICT (customer_id)
DO UPDATE SET
    first_name = EXCLUDED.first_name,
    last_name = EXCLUDED.last_name,
    email = EXCLUDED.email,
    phone = EXCLUDED.phone,
    city = EXCLUDED.city,
    state = EXCLUDED.state,
    country = EXCLUDED.country,
    signup_date = EXCLUDED.signup_date,
    loyalty_tier = EXCLUDED.loyalty_tier,
    source_run_id = EXCLUDED.source_run_id,
    source_object_name = EXCLUDED.source_object_name,
    loaded_at = NOW();


-- ============================================================
-- Stores
-- ============================================================

WITH deduped_stores AS (
    SELECT DISTINCT ON (payload ->> 'store_id')
        payload,
        run_id,
        object_name,
        ingested_at
    FROM bronze.raw_stores
    WHERE payload ? 'store_id'
    ORDER BY
        payload ->> 'store_id',
        ingested_at DESC,
        record_index DESC
)
INSERT INTO silver.stores (
    store_id,
    store_name,
    region,
    city,
    country,
    store_type,
    opened_at,
    source_run_id,
    source_object_name
)
SELECT
    payload ->> 'store_id' AS store_id,
    payload ->> 'store_name' AS store_name,
    payload ->> 'region' AS region,
    payload ->> 'city' AS city,
    payload ->> 'country' AS country,
    LOWER(payload ->> 'store_type') AS store_type,
    (payload ->> 'opened_at')::DATE AS opened_at,
    run_id AS source_run_id,
    object_name AS source_object_name
FROM deduped_stores
ON CONFLICT (store_id)
DO UPDATE SET
    store_name = EXCLUDED.store_name,
    region = EXCLUDED.region,
    city = EXCLUDED.city,
    country = EXCLUDED.country,
    store_type = EXCLUDED.store_type,
    opened_at = EXCLUDED.opened_at,
    source_run_id = EXCLUDED.source_run_id,
    source_object_name = EXCLUDED.source_object_name,
    loaded_at = NOW();


-- ============================================================
-- Campaigns
-- ============================================================

WITH deduped_campaigns AS (
    SELECT DISTINCT ON (payload ->> 'campaign_id')
        payload,
        run_id,
        object_name,
        ingested_at
    FROM bronze.raw_campaigns
    WHERE payload ? 'campaign_id'
    ORDER BY
        payload ->> 'campaign_id',
        ingested_at DESC,
        record_index DESC
)
INSERT INTO silver.campaigns (
    campaign_id,
    campaign_name,
    channel,
    start_date,
    end_date,
    budget,
    target_region,
    source_run_id,
    source_object_name
)
SELECT
    payload ->> 'campaign_id' AS campaign_id,
    payload ->> 'campaign_name' AS campaign_name,
    LOWER(payload ->> 'channel') AS channel,
    (payload ->> 'start_date')::DATE AS start_date,
    (payload ->> 'end_date')::DATE AS end_date,
    (payload ->> 'budget')::NUMERIC(14, 2) AS budget,
    payload ->> 'target_region' AS target_region,
    run_id AS source_run_id,
    object_name AS source_object_name
FROM deduped_campaigns
ON CONFLICT (campaign_id)
DO UPDATE SET
    campaign_name = EXCLUDED.campaign_name,
    channel = EXCLUDED.channel,
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    budget = EXCLUDED.budget,
    target_region = EXCLUDED.target_region,
    source_run_id = EXCLUDED.source_run_id,
    source_object_name = EXCLUDED.source_object_name,
    loaded_at = NOW();


-- ============================================================
-- Sales
-- ============================================================

WITH deduped_sales AS (
    SELECT DISTINCT ON (payload ->> 'order_id')
        payload,
        run_id,
        object_name,
        ingested_at
    FROM bronze.raw_sales
    WHERE payload ? 'order_id'
    ORDER BY
        payload ->> 'order_id',
        ingested_at DESC,
        record_index DESC
)
INSERT INTO silver.sales (
    order_id,
    customer_id,
    product_id,
    store_id,
    campaign_id,
    channel,
    quantity,
    unit_price,
    discount_amount,
    tax_amount,
    total_amount,
    order_status,
    payment_method,
    ordered_at,
    source_run_id,
    source_object_name
)
SELECT
    payload ->> 'order_id' AS order_id,
    payload ->> 'customer_id' AS customer_id,
    payload ->> 'product_id' AS product_id,
    payload ->> 'store_id' AS store_id,
    NULLIF(payload ->> 'campaign_id', '') AS campaign_id,
    LOWER(payload ->> 'channel') AS channel,
    (payload ->> 'quantity')::INTEGER AS quantity,
    (payload ->> 'unit_price')::NUMERIC(12, 2) AS unit_price,
    (payload ->> 'discount_amount')::NUMERIC(12, 2) AS discount_amount,
    (payload ->> 'tax_amount')::NUMERIC(12, 2) AS tax_amount,
    (payload ->> 'total_amount')::NUMERIC(12, 2) AS total_amount,
    LOWER(payload ->> 'order_status') AS order_status,
    LOWER(payload ->> 'payment_method') AS payment_method,
    (payload ->> 'ordered_at')::TIMESTAMPTZ AS ordered_at,
    run_id AS source_run_id,
    object_name AS source_object_name
FROM deduped_sales
ON CONFLICT (order_id)
DO UPDATE SET
    customer_id = EXCLUDED.customer_id,
    product_id = EXCLUDED.product_id,
    store_id = EXCLUDED.store_id,
    campaign_id = EXCLUDED.campaign_id,
    channel = EXCLUDED.channel,
    quantity = EXCLUDED.quantity,
    unit_price = EXCLUDED.unit_price,
    discount_amount = EXCLUDED.discount_amount,
    tax_amount = EXCLUDED.tax_amount,
    total_amount = EXCLUDED.total_amount,
    order_status = EXCLUDED.order_status,
    payment_method = EXCLUDED.payment_method,
    ordered_at = EXCLUDED.ordered_at,
    source_run_id = EXCLUDED.source_run_id,
    source_object_name = EXCLUDED.source_object_name,
    loaded_at = NOW();


-- ============================================================
-- Inventory
-- ============================================================

WITH deduped_inventory AS (
    SELECT DISTINCT ON (payload ->> 'inventory_id')
        payload,
        run_id,
        object_name,
        ingested_at
    FROM bronze.raw_inventory
    WHERE payload ? 'inventory_id'
    ORDER BY
        payload ->> 'inventory_id',
        ingested_at DESC,
        record_index DESC
)
INSERT INTO silver.inventory (
    inventory_id,
    product_id,
    store_id,
    stock_quantity,
    reorder_level,
    last_updated_at,
    source_run_id,
    source_object_name
)
SELECT
    payload ->> 'inventory_id' AS inventory_id,
    payload ->> 'product_id' AS product_id,
    payload ->> 'store_id' AS store_id,
    (payload ->> 'stock_quantity')::INTEGER AS stock_quantity,
    (payload ->> 'reorder_level')::INTEGER AS reorder_level,
    (payload ->> 'last_updated_at')::TIMESTAMPTZ AS last_updated_at,
    run_id AS source_run_id,
    object_name AS source_object_name
FROM deduped_inventory
ON CONFLICT (inventory_id)
DO UPDATE SET
    product_id = EXCLUDED.product_id,
    store_id = EXCLUDED.store_id,
    stock_quantity = EXCLUDED.stock_quantity,
    reorder_level = EXCLUDED.reorder_level,
    last_updated_at = EXCLUDED.last_updated_at,
    source_run_id = EXCLUDED.source_run_id,
    source_object_name = EXCLUDED.source_object_name,
    loaded_at = NOW();


-- ============================================================
-- Returns
-- ============================================================

WITH deduped_returns AS (
    SELECT DISTINCT ON (payload ->> 'return_id')
        payload,
        run_id,
        object_name,
        ingested_at
    FROM bronze.raw_returns
    WHERE payload ? 'return_id'
    ORDER BY
        payload ->> 'return_id',
        ingested_at DESC,
        record_index DESC
)
INSERT INTO silver.returns (
    return_id,
    order_id,
    product_id,
    return_reason,
    refund_amount,
    returned_at,
    source_run_id,
    source_object_name
)
SELECT
    payload ->> 'return_id' AS return_id,
    payload ->> 'order_id' AS order_id,
    payload ->> 'product_id' AS product_id,
    payload ->> 'return_reason' AS return_reason,
    (payload ->> 'refund_amount')::NUMERIC(12, 2) AS refund_amount,
    (payload ->> 'returned_at')::TIMESTAMPTZ AS returned_at,
    run_id AS source_run_id,
    object_name AS source_object_name
FROM deduped_returns
ON CONFLICT (return_id)
DO UPDATE SET
    order_id = EXCLUDED.order_id,
    product_id = EXCLUDED.product_id,
    return_reason = EXCLUDED.return_reason,
    refund_amount = EXCLUDED.refund_amount,
    returned_at = EXCLUDED.returned_at,
    source_run_id = EXCLUDED.source_run_id,
    source_object_name = EXCLUDED.source_object_name,
    loaded_at = NOW();