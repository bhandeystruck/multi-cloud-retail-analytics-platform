-- ============================================================
-- Silver to Gold Transformations
--
-- Purpose:
-- Convert clean Silver tables into business-ready analytics models.
--
-- Design:
-- - Use DELETE + INSERT for aggregate tables.
-- - This is simple, deterministic, and safe for local analytics.
-- - Later, we can optimize with incremental strategies.
--
-- Gold tables are meant to power APIs, dashboards, and reporting.
-- ============================================================


-- ============================================================
-- gold.daily_revenue
-- ============================================================

DELETE FROM gold.daily_revenue;

INSERT INTO gold.daily_revenue (
    revenue_date,
    total_orders,
    completed_orders,
    cancelled_orders,
    refunded_orders,
    gross_revenue,
    discount_total,
    tax_total,
    net_revenue,
    average_order_value
)
SELECT
    ordered_at::DATE AS revenue_date,
    COUNT(*)::INTEGER AS total_orders,
    COUNT(*) FILTER (WHERE order_status = 'completed')::INTEGER AS completed_orders,
    COUNT(*) FILTER (WHERE order_status = 'cancelled')::INTEGER AS cancelled_orders,
    COUNT(*) FILTER (WHERE order_status = 'refunded')::INTEGER AS refunded_orders,
    COALESCE(SUM(total_amount), 0)::NUMERIC(14, 2) AS gross_revenue,
    COALESCE(SUM(discount_amount), 0)::NUMERIC(14, 2) AS discount_total,
    COALESCE(SUM(tax_amount), 0)::NUMERIC(14, 2) AS tax_total,
    COALESCE(
        SUM(total_amount) FILTER (WHERE order_status = 'completed'),
        0
    )::NUMERIC(14, 2) AS net_revenue,
    COALESCE(AVG(total_amount), 0)::NUMERIC(14, 2) AS average_order_value
FROM silver.sales
GROUP BY ordered_at::DATE;


-- ============================================================
-- gold.product_sales_performance
-- ============================================================

DELETE FROM gold.product_sales_performance;

INSERT INTO gold.product_sales_performance (
    product_id,
    product_name,
    category,
    brand,
    units_sold,
    total_orders,
    gross_revenue,
    estimated_gross_margin,
    return_count,
    return_rate
)
SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.brand,
    COALESCE(SUM(s.quantity) FILTER (WHERE s.order_status = 'completed'), 0)::INTEGER
        AS units_sold,
    COUNT(s.order_id)::INTEGER AS total_orders,
    COALESCE(
        SUM(s.total_amount) FILTER (WHERE s.order_status = 'completed'),
        0
    )::NUMERIC(14, 2) AS gross_revenue,
    COALESCE(
        SUM((s.unit_price - p.cost_price) * s.quantity)
            FILTER (WHERE s.order_status = 'completed'),
        0
    )::NUMERIC(14, 2) AS estimated_gross_margin,
    COUNT(r.return_id)::INTEGER AS return_count,
    CASE
        WHEN COUNT(s.order_id) = 0 THEN 0
        ELSE (COUNT(r.return_id)::NUMERIC / COUNT(s.order_id)::NUMERIC)
    END::NUMERIC(8, 4) AS return_rate
FROM silver.products p
LEFT JOIN silver.sales s
    ON p.product_id = s.product_id
LEFT JOIN silver.returns r
    ON p.product_id = r.product_id
GROUP BY
    p.product_id,
    p.product_name,
    p.category,
    p.brand;


-- ============================================================
-- gold.customer_lifetime_value
-- ============================================================

DELETE FROM gold.customer_lifetime_value;

INSERT INTO gold.customer_lifetime_value (
    customer_id,
    customer_name,
    email,
    country,
    loyalty_tier,
    total_orders,
    completed_orders,
    total_spent,
    average_order_value,
    first_order_at,
    most_recent_order_at
)
SELECT
    c.customer_id,
    CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
    c.email,
    c.country,
    c.loyalty_tier,
    COUNT(s.order_id)::INTEGER AS total_orders,
    COUNT(s.order_id) FILTER (WHERE s.order_status = 'completed')::INTEGER
        AS completed_orders,
    COALESCE(
        SUM(s.total_amount) FILTER (WHERE s.order_status = 'completed'),
        0
    )::NUMERIC(14, 2) AS total_spent,
    COALESCE(
        AVG(s.total_amount) FILTER (WHERE s.order_status = 'completed'),
        0
    )::NUMERIC(14, 2) AS average_order_value,
    MIN(s.ordered_at) AS first_order_at,
    MAX(s.ordered_at) AS most_recent_order_at
FROM silver.customers c
LEFT JOIN silver.sales s
    ON c.customer_id = s.customer_id
GROUP BY
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.country,
    c.loyalty_tier;


-- ============================================================
-- gold.store_performance
-- ============================================================

DELETE FROM gold.store_performance;

INSERT INTO gold.store_performance (
    store_id,
    store_name,
    region,
    city,
    country,
    store_type,
    total_orders,
    completed_orders,
    gross_revenue,
    average_order_value
)
SELECT
    st.store_id,
    st.store_name,
    st.region,
    st.city,
    st.country,
    st.store_type,
    COUNT(s.order_id)::INTEGER AS total_orders,
    COUNT(s.order_id) FILTER (WHERE s.order_status = 'completed')::INTEGER
        AS completed_orders,
    COALESCE(
        SUM(s.total_amount) FILTER (WHERE s.order_status = 'completed'),
        0
    )::NUMERIC(14, 2) AS gross_revenue,
    COALESCE(
        AVG(s.total_amount) FILTER (WHERE s.order_status = 'completed'),
        0
    )::NUMERIC(14, 2) AS average_order_value
FROM silver.stores st
LEFT JOIN silver.sales s
    ON st.store_id = s.store_id
GROUP BY
    st.store_id,
    st.store_name,
    st.region,
    st.city,
    st.country,
    st.store_type;


-- ============================================================
-- gold.inventory_risk
-- ============================================================

DELETE FROM gold.inventory_risk;

INSERT INTO gold.inventory_risk (
    inventory_id,
    product_id,
    product_name,
    category,
    store_id,
    store_name,
    region,
    stock_quantity,
    reorder_level,
    stock_status,
    estimated_stock_value,
    last_updated_at
)
SELECT
    i.inventory_id,
    i.product_id,
    p.product_name,
    p.category,
    i.store_id,
    st.store_name,
    st.region,
    i.stock_quantity,
    i.reorder_level,
    CASE
        WHEN i.stock_quantity = 0 THEN 'out_of_stock'
        WHEN i.stock_quantity <= i.reorder_level THEN 'low_stock'
        ELSE 'healthy'
    END AS stock_status,
    (i.stock_quantity * p.cost_price)::NUMERIC(14, 2) AS estimated_stock_value,
    i.last_updated_at
FROM silver.inventory i
JOIN silver.products p
    ON i.product_id = p.product_id
JOIN silver.stores st
    ON i.store_id = st.store_id;


-- ============================================================
-- gold.campaign_roi
-- ============================================================

DELETE FROM gold.campaign_roi;

INSERT INTO gold.campaign_roi (
    campaign_id,
    campaign_name,
    channel,
    target_region,
    budget,
    attributed_orders,
    attributed_revenue,
    estimated_roi,
    start_date,
    end_date
)
SELECT
    c.campaign_id,
    c.campaign_name,
    c.channel,
    c.target_region,
    c.budget,
    COUNT(s.order_id)::INTEGER AS attributed_orders,
    COALESCE(
        SUM(s.total_amount) FILTER (WHERE s.order_status = 'completed'),
        0
    )::NUMERIC(14, 2) AS attributed_revenue,
    CASE
        WHEN c.budget = 0 THEN 0
        ELSE (
            (
                COALESCE(
                    SUM(s.total_amount) FILTER (WHERE s.order_status = 'completed'),
                    0
                ) - c.budget
            ) / c.budget
        )
    END::NUMERIC(14, 4) AS estimated_roi,
    c.start_date,
    c.end_date
FROM silver.campaigns c
LEFT JOIN silver.sales s
    ON c.campaign_id = s.campaign_id
GROUP BY
    c.campaign_id,
    c.campaign_name,
    c.channel,
    c.target_region,
    c.budget,
    c.start_date,
    c.end_date;


-- ============================================================
-- gold.executive_kpis
-- ============================================================

INSERT INTO gold.executive_kpis (
    snapshot_at,
    total_revenue,
    total_orders,
    completed_orders,
    total_customers,
    active_products,
    low_stock_items,
    out_of_stock_items,
    average_order_value,
    refund_amount,
    return_count
)
SELECT
    NOW() AS snapshot_at,
    COALESCE(
        (SELECT SUM(total_amount)
         FROM silver.sales
         WHERE order_status = 'completed'),
        0
    )::NUMERIC(14, 2) AS total_revenue,

    (SELECT COUNT(*) FROM silver.sales)::INTEGER AS total_orders,

    (SELECT COUNT(*)
     FROM silver.sales
     WHERE order_status = 'completed')::INTEGER AS completed_orders,

    (SELECT COUNT(*) FROM silver.customers)::INTEGER AS total_customers,

    (SELECT COUNT(*)
     FROM silver.products
     WHERE is_active = TRUE)::INTEGER AS active_products,

    (SELECT COUNT(*)
     FROM gold.inventory_risk
     WHERE stock_status = 'low_stock')::INTEGER AS low_stock_items,

    (SELECT COUNT(*)
     FROM gold.inventory_risk
     WHERE stock_status = 'out_of_stock')::INTEGER AS out_of_stock_items,

    COALESCE(
        (SELECT AVG(total_amount)
         FROM silver.sales
         WHERE order_status = 'completed'),
        0
    )::NUMERIC(14, 2) AS average_order_value,

    COALESCE(
        (SELECT SUM(refund_amount)
         FROM silver.returns),
        0
    )::NUMERIC(14, 2) AS refund_amount,

    (SELECT COUNT(*) FROM silver.returns)::INTEGER AS return_count;