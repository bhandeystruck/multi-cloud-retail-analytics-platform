"""
Unit tests for the retail data generator.

Why these tests exist:
- Protects the generator from breaking as the project grows.
- Confirms generated datasets have expected relationships.
- Gives us confidence before building ingestion and warehouse loading.
"""

from __future__ import annotations

import json
from pathlib import Path

from ingestion.generate_retail_data import generate_all_datasets


def test_generate_all_datasets_creates_expected_files(tmp_path: Path) -> None:
    """
    Verify that the generator writes one JSON file per dataset.
    """

    counts = generate_all_datasets(
        output_dir=tmp_path,
        product_count=10,
        customer_count=20,
        store_count=5,
        campaign_count=3,
        sales_count=50,
        inventory_count=30,
        return_rate=0.10,
        seed=123,
    )

    expected_files = {
        "products.json",
        "customers.json",
        "stores.json",
        "campaigns.json",
        "sales.json",
        "inventory.json",
        "returns.json",
    }

    actual_files = {path.name for path in tmp_path.iterdir()}

    assert expected_files == actual_files

    assert counts["products"] == 10
    assert counts["customers"] == 20
    assert counts["stores"] == 5
    assert counts["campaigns"] == 3
    assert counts["sales"] == 50
    assert counts["inventory"] == 30


def test_sales_reference_valid_parent_records(tmp_path: Path) -> None:
    """
    Verify that generated sales reference valid customers, products, and stores.

    This matters because later warehouse joins depend on these relationships.
    """

    generate_all_datasets(
        output_dir=tmp_path,
        product_count=10,
        customer_count=20,
        store_count=5,
        campaign_count=3,
        sales_count=50,
        inventory_count=30,
        return_rate=0.10,
        seed=123,
    )

    products = json.loads((tmp_path / "products.json").read_text(encoding="utf-8"))
    customers = json.loads((tmp_path / "customers.json").read_text(encoding="utf-8"))
    stores = json.loads((tmp_path / "stores.json").read_text(encoding="utf-8"))
    sales = json.loads((tmp_path / "sales.json").read_text(encoding="utf-8"))

    product_ids = {product["product_id"] for product in products}
    customer_ids = {customer["customer_id"] for customer in customers}
    store_ids = {store["store_id"] for store in stores}

    for sale in sales:
        assert sale["product_id"] in product_ids
        assert sale["customer_id"] in customer_ids
        assert sale["store_id"] in store_ids


def test_returns_reference_valid_sales(tmp_path: Path) -> None:
    """
    Verify that generated returns reference valid sales orders.
    """

    generate_all_datasets(
        output_dir=tmp_path,
        product_count=10,
        customer_count=20,
        store_count=5,
        campaign_count=3,
        sales_count=100,
        inventory_count=30,
        return_rate=0.20,
        seed=123,
    )

    sales = json.loads((tmp_path / "sales.json").read_text(encoding="utf-8"))
    returns = json.loads((tmp_path / "returns.json").read_text(encoding="utf-8"))

    sale_order_ids = {sale["order_id"] for sale in sales}

    for return_record in returns:
        assert return_record["order_id"] in sale_order_ids