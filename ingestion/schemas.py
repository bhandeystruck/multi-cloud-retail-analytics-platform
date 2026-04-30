"""
Schema definitions for the retail analytics platform.

This module will eventually contain typed data models for the main retail
entities such as Sales, Products, Customers, Stores, Inventory, Campaigns,
and Returns.

Why this file exists:
- Keeps data contracts in one place.
- Makes validation easier.
- Helps prevent pipeline bugs caused by inconsistent field names.
"""

from __future__ import annotations

from pydantic import BaseModel


class BaseRetailRecord(BaseModel):
    """
    Base class for retail records.

    We keep this class small for now. Later, shared validation behavior can
    be added here if multiple datasets need common rules.
    """

    pass