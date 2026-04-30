"""
Schema definitions for the retail analytics platform.

Why this file exists:
- Defines the data contracts for our generated retail datasets.
- Helps keep field names and types consistent across the project.
- Makes it easier to validate data before writing it to object storage.
- Gives future ingestion, warehouse loading, and API layers a shared understanding
  of what each dataset should look like.

We use Pydantic because it provides:
- Type validation
- Clear model definitions
- Easy conversion to dictionaries/JSON
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from pydantic import BaseModel, EmailStr, Field, field_validator


class Product(BaseModel):
    """
    Product catalog record.

    A product represents something the retail company sells.
    """

    product_id: str
    product_name: str
    category: str
    brand: str
    cost_price: Decimal = Field(ge=0)
    selling_price: Decimal = Field(gt=0)
    supplier_id: str
    created_at: datetime
    is_active: bool

    @field_validator("selling_price")
    @classmethod
    def selling_price_must_be_greater_than_cost(
        cls,
        selling_price: Decimal,
        info,
    ) -> Decimal:
        """
        Validate that the selling price is higher than the cost price.

        Why:
        In a normal retail business, selling below cost usually means either
        a data issue or a special loss-leader strategy. For this project, we
        keep the generator simple and enforce positive margin.
        """

        cost_price = info.data.get("cost_price")

        if cost_price is not None and selling_price <= cost_price:
            raise ValueError("selling_price must be greater than cost_price")

        return selling_price


class Customer(BaseModel):
    """
    Customer profile record.

    Customers are linked to sales through customer_id.
    """

    customer_id: str
    first_name: str
    last_name: str
    email: EmailStr
    phone: str
    city: str
    state: str
    country: str
    signup_date: date
    loyalty_tier: str


class Store(BaseModel):
    """
    Store/location record.

    A store may represent a physical store, an online store, or a marketplace.
    """

    store_id: str
    store_name: str
    region: str
    city: str
    country: str
    store_type: str
    opened_at: date


class Campaign(BaseModel):
    """
    Marketing campaign record.

    Campaigns can optionally be attached to sales to support campaign ROI reporting.
    """

    campaign_id: str
    campaign_name: str
    channel: str
    start_date: date
    end_date: date
    budget: Decimal = Field(gt=0)
    target_region: str

    @field_validator("end_date")
    @classmethod
    def end_date_must_not_be_before_start_date(cls, end_date: date, info) -> date:
        """
        Validate that campaign end date is not before start date.
        """

        start_date = info.data.get("start_date")

        if start_date is not None and end_date < start_date:
            raise ValueError("end_date cannot be before start_date")

        return end_date


class Sale(BaseModel):
    """
    Sales transaction record.

    Sales are the fact records that power most revenue analytics.
    """

    order_id: str
    customer_id: str
    product_id: str
    store_id: str
    campaign_id: str | None
    channel: str
    quantity: int = Field(gt=0)
    unit_price: Decimal = Field(gt=0)
    discount_amount: Decimal = Field(ge=0)
    tax_amount: Decimal = Field(ge=0)
    total_amount: Decimal = Field(ge=0)
    order_status: str
    payment_method: str
    ordered_at: datetime


class Inventory(BaseModel):
    """
    Inventory record.

    Inventory links products to stores and tracks stock availability.
    """

    inventory_id: str
    product_id: str
    store_id: str
    stock_quantity: int = Field(ge=0)
    reorder_level: int = Field(ge=0)
    last_updated_at: datetime


class ReturnRecord(BaseModel):
    """
    Return/refund record.

    Returns link back to an order and product.
    """

    return_id: str
    order_id: str
    product_id: str
    return_reason: str
    refund_amount: Decimal = Field(ge=0)
    returned_at: datetime