"""
Reporting service for Gold analytics queries.

Why this file exists:
- Keeps SQL queries out of FastAPI route handlers.
- Centralizes reporting logic.
- Makes endpoint code easier to read and test.
- Keeps the API focused on request/response behavior.

The API reads from Gold tables only.
That is intentional because Gold tables are business-ready and optimized for reporting.
"""

from __future__ import annotations

from datetime import date

from sqlalchemy import text
from sqlalchemy.orm import Session


class ReportingServiceError(Exception):
    """
    Raised when reporting queries fail.
    """


def fetch_latest_executive_kpis(db: Session) -> dict | None:
    """
    Fetch the latest executive KPI snapshot.

    Args:
        db: SQLAlchemy database session.

    Returns:
        Latest KPI row as a dictionary, or None if no data exists.
    """

    query = text(
        """
        SELECT
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
        FROM gold.executive_kpis
        ORDER BY snapshot_at DESC
        LIMIT 1;
        """,
    )

    row = db.execute(query).mappings().first()
    return dict(row) if row else None


def fetch_daily_revenue(
    db: Session,
    start_date: date | None = None,
    end_date: date | None = None,
    limit: int = 30,
) -> list[dict]:
    """
    Fetch daily revenue metrics.

    Args:
        db: SQLAlchemy database session.
        start_date: Optional start date filter.
        end_date: Optional end date filter.
        limit: Maximum number of rows.

    Returns:
        List of daily revenue rows.
    """

    query = text(
        """
        SELECT
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
        FROM gold.daily_revenue
        WHERE (:start_date IS NULL OR revenue_date >= :start_date)
          AND (:end_date IS NULL OR revenue_date <= :end_date)
        ORDER BY revenue_date DESC
        LIMIT :limit;
        """,
    )

    rows = db.execute(
        query,
        {
            "start_date": start_date,
            "end_date": end_date,
            "limit": limit,
        },
    ).mappings()

    return [dict(row) for row in rows]


def fetch_top_products(
    db: Session,
    limit: int = 20,
    category: str | None = None,
) -> list[dict]:
    """
    Fetch top-selling products by gross revenue.

    Args:
        db: SQLAlchemy database session.
        limit: Maximum rows to return.
        category: Optional category filter.

    Returns:
        List of product performance rows.
    """

    query = text(
        """
        SELECT
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
        FROM gold.product_sales_performance
        WHERE (:category IS NULL OR category = :category)
        ORDER BY gross_revenue DESC, units_sold DESC
        LIMIT :limit;
        """,
    )

    rows = db.execute(
        query,
        {
            "limit": limit,
            "category": category,
        },
    ).mappings()

    return [dict(row) for row in rows]


def fetch_top_customers(
    db: Session,
    limit: int = 20,
    loyalty_tier: str | None = None,
) -> list[dict]:
    """
    Fetch top customers by total spend.

    Args:
        db: SQLAlchemy database session.
        limit: Maximum rows to return.
        loyalty_tier: Optional loyalty tier filter.

    Returns:
        List of customer lifetime value rows.
    """

    query = text(
        """
        SELECT
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
        FROM gold.customer_lifetime_value
        WHERE (:loyalty_tier IS NULL OR loyalty_tier = :loyalty_tier)
        ORDER BY total_spent DESC, completed_orders DESC
        LIMIT :limit;
        """,
    )

    rows = db.execute(
        query,
        {
            "limit": limit,
            "loyalty_tier": loyalty_tier,
        },
    ).mappings()

    return [dict(row) for row in rows]


def fetch_inventory_risk(
    db: Session,
    limit: int = 50,
    stock_status: str | None = None,
) -> list[dict]:
    """
    Fetch inventory risk records.

    Args:
        db: SQLAlchemy database session.
        limit: Maximum rows to return.
        stock_status: Optional stock status filter.

    Returns:
        List of inventory risk rows.
    """

    query = text(
        """
        SELECT
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
        FROM gold.inventory_risk
        WHERE (:stock_status IS NULL OR stock_status = :stock_status)
        ORDER BY
            CASE stock_status
                WHEN 'out_of_stock' THEN 1
                WHEN 'low_stock' THEN 2
                ELSE 3
            END,
            stock_quantity ASC
        LIMIT :limit;
        """,
    )

    rows = db.execute(
        query,
        {
            "limit": limit,
            "stock_status": stock_status,
        },
    ).mappings()

    return [dict(row) for row in rows]


def fetch_campaign_roi(
    db: Session,
    limit: int = 20,
    channel: str | None = None,
) -> list[dict]:
    """
    Fetch campaign ROI results.

    Args:
        db: SQLAlchemy database session.
        limit: Maximum rows to return.
        channel: Optional campaign channel filter.

    Returns:
        List of campaign ROI rows.
    """

    query = text(
        """
        SELECT
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
        FROM gold.campaign_roi
        WHERE (:channel IS NULL OR channel = :channel)
        ORDER BY estimated_roi DESC
        LIMIT :limit;
        """,
    )

    rows = db.execute(
        query,
        {
            "limit": limit,
            "channel": channel,
        },
    ).mappings()

    return [dict(row) for row in rows]