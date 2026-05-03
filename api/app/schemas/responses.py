"""
Pydantic response schemas for the reporting API.

Why this file exists:
- Defines clear API response contracts.
- Makes OpenAPI docs more useful.
- Prevents route handlers from returning inconsistent shapes.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict


class HealthResponse(BaseModel):
    """
    Health endpoint response.
    """

    status: str
    app_name: str
    environment: str
    database: str


class ExecutiveKPIsResponse(BaseModel):
    """
    Executive KPI response from gold.executive_kpis.
    """

    model_config = ConfigDict(from_attributes=True)

    snapshot_at: datetime
    total_revenue: Decimal
    total_orders: int
    completed_orders: int
    total_customers: int
    active_products: int
    low_stock_items: int
    out_of_stock_items: int
    average_order_value: Decimal
    refund_amount: Decimal
    return_count: int


class DailyRevenueResponse(BaseModel):
    """
    Daily revenue response from gold.daily_revenue.
    """

    model_config = ConfigDict(from_attributes=True)

    revenue_date: date
    total_orders: int
    completed_orders: int
    cancelled_orders: int
    refunded_orders: int
    gross_revenue: Decimal
    discount_total: Decimal
    tax_total: Decimal
    net_revenue: Decimal
    average_order_value: Decimal


class ProductPerformanceResponse(BaseModel):
    """
    Product performance response from gold.product_sales_performance.
    """

    model_config = ConfigDict(from_attributes=True)

    product_id: str
    product_name: str
    category: str
    brand: str
    units_sold: int
    total_orders: int
    gross_revenue: Decimal
    estimated_gross_margin: Decimal
    return_count: int
    return_rate: Decimal


class CustomerLifetimeValueResponse(BaseModel):
    """
    Customer lifetime value response from gold.customer_lifetime_value.
    """

    model_config = ConfigDict(from_attributes=True)

    customer_id: str
    customer_name: str
    email: str
    country: str
    loyalty_tier: str
    total_orders: int
    completed_orders: int
    total_spent: Decimal
    average_order_value: Decimal
    first_order_at: datetime | None
    most_recent_order_at: datetime | None


class InventoryRiskResponse(BaseModel):
    """
    Inventory risk response from gold.inventory_risk.
    """

    model_config = ConfigDict(from_attributes=True)

    inventory_id: str
    product_id: str
    product_name: str
    category: str
    store_id: str
    store_name: str
    region: str
    stock_quantity: int
    reorder_level: int
    stock_status: str
    estimated_stock_value: Decimal
    last_updated_at: datetime


class CampaignROIResponse(BaseModel):
    """
    Campaign ROI response from gold.campaign_roi.
    """

    model_config = ConfigDict(from_attributes=True)

    campaign_id: str
    campaign_name: str
    channel: str
    target_region: str
    budget: Decimal
    attributed_orders: int
    attributed_revenue: Decimal
    estimated_roi: Decimal
    start_date: date
    end_date: date