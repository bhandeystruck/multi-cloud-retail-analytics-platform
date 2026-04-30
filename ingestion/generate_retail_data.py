"""
Retail data generator.

This module generates realistic, interconnected retail source data for local
development.

Why this module exists:
- Real data platforms ingest data from many source systems.
- For this portfolio project, we need reliable sample data without depending on
  external APIs.
- Generated data lets us test ingestion, object storage, warehouse loading,
  transformations, data quality checks, and API endpoints.

Datasets generated:
- products
- customers
- stores
- campaigns
- sales
- inventory
- returns

Design goals:
- Deterministic output when a seed is provided.
- Clear relationships between entities.
- Defensive error handling.
- Human-readable JSON output.
- Easy CLI usage.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from pydantic import ValidationError

# Importing schemas gives us type-safe validation for generated records.
from ingestion.schemas import Campaign, Customer, Inventory, Product, ReturnRecord, Sale, Store

DEFAULT_OUTPUT_DIR = Path("data/generated")


PRODUCT_CATEGORIES = {
    "Electronics": [
        "Wireless Mouse",
        "Bluetooth Speaker",
        "USB-C Charger",
        "Noise Cancelling Headphones",
        "Smart Watch",
        "Mechanical Keyboard",
    ],
    "Home": [
        "Ceramic Dinner Set",
        "Cotton Bedsheet",
        "Desk Lamp",
        "Storage Basket",
        "Non-stick Pan",
        "Scented Candle",
    ],
    "Beauty": [
        "Face Serum",
        "Moisturizer",
        "Lip Balm",
        "Sunscreen",
        "Hair Oil",
        "Body Lotion",
    ],
    "Grocery": [
        "Organic Honey",
        "Green Tea",
        "Coffee Beans",
        "Olive Oil",
        "Granola",
        "Dark Chocolate",
    ],
    "Fashion": [
        "Cotton T-Shirt",
        "Denim Jacket",
        "Running Shoes",
        "Leather Wallet",
        "Wool Scarf",
        "Canvas Tote Bag",
    ],
}


BRANDS = [
    "UrbanNest",
    "DailyCo",
    "NovaMart",
    "EcoLine",
    "BrightHome",
    "FreshBay",
    "StyleRoot",
    "PeakGear",
]


FIRST_NAMES = [
    "Aarav",
    "Aarya",
    "Bibhuti",
    "Nisha",
    "Sanjay",
    "Priya",
    "Rohan",
    "Anika",
    "Kabir",
    "Meera",
    "Suman",
    "Rita",
    "David",
    "Sarah",
    "Michael",
    "Emma",
    "Liam",
    "Olivia",
]


LAST_NAMES = [
    "Karki",
    "Shrestha",
    "Rai",
    "Gurung",
    "Tamang",
    "Maharjan",
    "Smith",
    "Johnson",
    "Brown",
    "Williams",
    "Miller",
    "Davis",
]


LOCATIONS = [
    {
        "city": "Kathmandu",
        "state": "Bagmati",
        "country": "Nepal",
        "region": "South Asia",
    },
    {
        "city": "Lalitpur",
        "state": "Bagmati",
        "country": "Nepal",
        "region": "South Asia",
    },
    {
        "city": "Pokhara",
        "state": "Gandaki",
        "country": "Nepal",
        "region": "South Asia",
    },
    {
        "city": "Sydney",
        "state": "NSW",
        "country": "Australia",
        "region": "Oceania",
    },
    {
        "city": "Melbourne",
        "state": "VIC",
        "country": "Australia",
        "region": "Oceania",
    },
    {
        "city": "New York",
        "state": "NY",
        "country": "USA",
        "region": "North America",
    },
    {
        "city": "San Francisco",
        "state": "CA",
        "country": "USA",
        "region": "North America",
    },
]


LOYALTY_TIERS = ["bronze", "silver", "gold", "platinum"]

STORE_TYPES = ["physical", "online", "marketplace"]

SALES_CHANNELS = ["online", "store", "marketplace", "mobile_app"]

PAYMENT_METHODS = ["card", "cash", "wallet", "bank_transfer"]

ORDER_STATUSES = ["completed", "completed", "completed", "completed", "cancelled", "refunded"]

CAMPAIGN_CHANNELS = ["email", "social", "search", "display", "affiliate"]

RETURN_REASONS = [
    "damaged_item",
    "wrong_size",
    "late_delivery",
    "changed_mind",
    "not_as_described",
    "defective_product",
]


class RetailDataGenerationError(Exception):
    """
    Custom exception for data generation failures.

    Why:
    A custom exception helps us separate expected generator-level failures from
    unexpected Python/runtime errors.
    """



def decimal_to_json(value: Any) -> Any:
    """
    Convert objects that JSON cannot serialize by default.

    Python's json module does not know how to serialize Decimal, date, or datetime.
    We convert them to simple JSON-compatible values.

    Args:
        value: Any Python object.

    Returns:
        A JSON-compatible representation.

    Raises:
        TypeError: If the value cannot be serialized.
    """

    if isinstance(value, Decimal):
        return float(value)

    if isinstance(value, datetime | date):
        return value.isoformat()

    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")



def money(value: float) -> Decimal:
    """
    Convert a float to a two-decimal Decimal value.

    Why:
    Money should not be handled as raw floating-point values in serious systems.
    For generated source data, this function keeps prices consistent and readable.
    """

    return Decimal(str(round(value, 2)))



def random_datetime_between(start: datetime, end: datetime) -> datetime:
    """
    Generate a random datetime between two datetime values.

    Args:
        start: Lower datetime bound.
        end: Upper datetime bound.

    Returns:
        Random datetime between start and end.

    Raises:
        ValueError: If end is before start.
    """

    if end < start:
        raise ValueError("end datetime cannot be before start datetime")

    total_seconds = int((end - start).total_seconds())
    random_offset = random.randint(0, total_seconds)

    return start + timedelta(seconds=random_offset)



def validate_and_dump_records(records: list[Any]) -> list[dict[str, Any]]:
    """
    Convert Pydantic model instances into plain dictionaries.

    Args:
        records: List of Pydantic model instances.

    Returns:
        List of dictionaries ready for JSON serialization.
    """

    return [record.model_dump() for record in records]



def generate_products(count: int) -> list[Product]:
    """
    Generate product catalog records.

    Args:
        count: Number of products to generate.

    Returns:
        List of Product records.
    """

    products: list[Product] = []

    category_names = list(PRODUCT_CATEGORIES.keys())

    for index in range(1, count + 1):
        category = random.choice(category_names)
        base_product_name = random.choice(PRODUCT_CATEGORIES[category])
        brand = random.choice(BRANDS)

        cost_price = money(random.uniform(5, 250))
        margin_multiplier = random.uniform(1.25, 2.25)
        selling_price = money(float(cost_price) * margin_multiplier)

        product = Product(
            product_id=f"PROD-{index:05d}",
            product_name=f"{brand} {base_product_name}",
            category=category,
            brand=brand,
            cost_price=cost_price,
            selling_price=selling_price,
            supplier_id=f"SUP-{random.randint(1, 25):03d}",
            created_at=random_datetime_between(
                datetime(2024, 1, 1, tzinfo=UTC),
                datetime.now(UTC),
            ),
            is_active=random.choice([True, True, True, False]),
        )

        products.append(product)

    return products



def generate_customers(count: int) -> list[Customer]:
    """
    Generate customer profile records.

    Args:
        count: Number of customers to generate.

    Returns:
        List of Customer records.
    """

    customers: list[Customer] = []

    for index in range(1, count + 1):
        first_name = random.choice(FIRST_NAMES)
        last_name = random.choice(LAST_NAMES)
        location = random.choice(LOCATIONS)

        # We include the index in the email to ensure uniqueness.
        email = f"{first_name.lower()}.{last_name.lower()}.{index}@example.com"

        customer = Customer(
            customer_id=f"CUST-{index:06d}",
            first_name=first_name,
            last_name=last_name,
            email=email,
            phone=f"+1-555-{random.randint(1000, 9999)}",
            city=location["city"],
            state=location["state"],
            country=location["country"],
            signup_date=random_datetime_between(
                datetime(2023, 1, 1, tzinfo=UTC),
                datetime.now(UTC),
            ).date(),
            loyalty_tier=random.choice(LOYALTY_TIERS),
        )

        customers.append(customer)

    return customers



def generate_stores(count: int) -> list[Store]:
    """
    Generate store/location records.

    Args:
        count: Number of stores to generate.

    Returns:
        List of Store records.
    """

    stores: list[Store] = []

    for index in range(1, count + 1):
        location = random.choice(LOCATIONS)
        store_type = random.choice(STORE_TYPES)

        store = Store(
            store_id=f"STORE-{index:04d}",
            store_name=f"{location['city']} {store_type.title()} Store {index}",
            region=location["region"],
            city=location["city"],
            country=location["country"],
            store_type=store_type,
            opened_at=random_datetime_between(
                datetime(2020, 1, 1, tzinfo=UTC),
                datetime(2025, 12, 31, tzinfo=UTC),
            ).date(),
        )

        stores.append(store)

    return stores



def generate_campaigns(count: int) -> list[Campaign]:
    """
    Generate marketing campaign records.

    Args:
        count: Number of campaigns to generate.

    Returns:
        List of Campaign records.
    """

    campaigns: list[Campaign] = []

    for index in range(1, count + 1):
        channel = random.choice(CAMPAIGN_CHANNELS)
        location = random.choice(LOCATIONS)

        start_date = random_datetime_between(
            datetime(2025, 1, 1, tzinfo=UTC),
            datetime.now(UTC),
        ).date()
        duration_days = random.randint(7, 60)
        end_date = start_date + timedelta(days=duration_days)

        campaign = Campaign(
            campaign_id=f"CAMP-{index:04d}",
            campaign_name=f"{channel.title()} Campaign {index}",
            channel=channel,
            start_date=start_date,
            end_date=end_date,
            budget=money(random.uniform(500, 25_000)),
            target_region=location["region"],
        )

        campaigns.append(campaign)

    return campaigns



def generate_sales(
    count: int,
    products: list[Product],
    customers: list[Customer],
    stores: list[Store],
    campaigns: list[Campaign],
) -> list[Sale]:
    """
    Generate sales transaction records.

    Args:
        count: Number of sales records to generate.
        products: Product records used for valid product_id references.
        customers: Customer records used for valid customer_id references.
        stores: Store records used for valid store_id references.
        campaigns: Campaign records used for optional campaign_id references.

    Returns:
        List of Sale records.

    Raises:
        RetailDataGenerationError: If required parent datasets are empty.
    """

    if not products:
        raise RetailDataGenerationError("Cannot generate sales without products")

    if not customers:
        raise RetailDataGenerationError("Cannot generate sales without customers")

    if not stores:
        raise RetailDataGenerationError("Cannot generate sales without stores")

    sales: list[Sale] = []

    for index in range(1, count + 1):
        product = random.choice(products)
        customer = random.choice(customers)
        store = random.choice(stores)

        quantity = random.randint(1, 5)
        unit_price = product.selling_price

        # Some sales receive no discount. Others receive a small discount.
        discount_rate = random.choice([0, 0, 0.05, 0.10, 0.15])
        gross_amount = float(unit_price) * quantity
        discount_amount = money(gross_amount * discount_rate)

        taxable_amount = gross_amount - float(discount_amount)
        tax_amount = money(taxable_amount * 0.08)
        total_amount = money(taxable_amount + float(tax_amount))

        # Not every sale is associated with a campaign.
        campaign_id = None

        if campaigns and random.random() < 0.35:
            campaign_id = random.choice(campaigns).campaign_id

        sale = Sale(
            order_id=f"ORD-{index:08d}",
            customer_id=customer.customer_id,
            product_id=product.product_id,
            store_id=store.store_id,
            campaign_id=campaign_id,
            channel=random.choice(SALES_CHANNELS),
            quantity=quantity,
            unit_price=unit_price,
            discount_amount=discount_amount,
            tax_amount=tax_amount,
            total_amount=total_amount,
            order_status=random.choice(ORDER_STATUSES),
            payment_method=random.choice(PAYMENT_METHODS),
            ordered_at=random_datetime_between(
                datetime(2025, 1, 1, tzinfo=UTC),
                datetime.now(UTC),
            ),
        )

        sales.append(sale)

    return sales



def generate_inventory(
    products: list[Product],
    stores: list[Store],
    max_records: int | None = None,
) -> list[Inventory]:
    """
    Generate inventory records for product/store combinations.

    Args:
        products: Product records.
        stores: Store records.
        max_records: Optional maximum number of inventory records.

    Returns:
        List of Inventory records.

    Raises:
        RetailDataGenerationError: If required parent datasets are empty.
    """

    if not products:
        raise RetailDataGenerationError("Cannot generate inventory without products")

    if not stores:
        raise RetailDataGenerationError("Cannot generate inventory without stores")

    inventory_records: list[Inventory] = []
    inventory_index = 1

    # Build product/store stock combinations.
    # We do not need every product in every store, so we randomly sample combinations.
    combinations = [(product, store) for product in products for store in stores]
    random.shuffle(combinations)

    if max_records is not None:
        combinations = combinations[:max_records]

    for product, store in combinations:
        stock_quantity = random.randint(0, 500)
        reorder_level = random.randint(10, 75)

        inventory = Inventory(
            inventory_id=f"INV-{inventory_index:07d}",
            product_id=product.product_id,
            store_id=store.store_id,
            stock_quantity=stock_quantity,
            reorder_level=reorder_level,
            last_updated_at=random_datetime_between(
                datetime(2025, 1, 1, tzinfo=UTC),
                datetime.now(UTC),
            ),
        )

        inventory_records.append(inventory)
        inventory_index += 1

    return inventory_records



def generate_returns(
    sales: list[Sale],
    return_rate: float,
) -> list[ReturnRecord]:
    """
    Generate return/refund records from existing sales.

    Args:
        sales: Existing sales records.
        return_rate: Approximate share of eligible sales that become returns.

    Returns:
        List of ReturnRecord records.

    Raises:
        ValueError: If return_rate is outside 0 to 1.
    """

    if return_rate < 0 or return_rate > 1:
        raise ValueError("return_rate must be between 0 and 1")

    eligible_sales = [
        sale for sale in sales if sale.order_status in {"completed", "refunded"}
    ]

    random.shuffle(eligible_sales)

    return_count = int(len(eligible_sales) * return_rate)
    selected_sales = eligible_sales[:return_count]

    returns: list[ReturnRecord] = []

    for index, sale in enumerate(selected_sales, start=1):
        # Refund amount is usually less than or equal to the original total.
        refund_ratio = random.uniform(0.5, 1.0)
        refund_amount = money(float(sale.total_amount) * refund_ratio)

        returned_at = sale.ordered_at + timedelta(days=random.randint(1, 45))

        return_record = ReturnRecord(
            return_id=f"RET-{index:07d}",
            order_id=sale.order_id,
            product_id=sale.product_id,
            return_reason=random.choice(RETURN_REASONS),
            refund_amount=refund_amount,
            returned_at=returned_at,
        )

        returns.append(return_record)

    return returns



def write_json_file(output_path: Path, records: list[dict[str, Any]]) -> None:
    """
    Write records to a JSON file.

    Args:
        output_path: Destination JSON file path.
        records: List of dictionaries to write.

    Raises:
        RetailDataGenerationError: If the file cannot be written.
    """

    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with output_path.open("w", encoding="utf-8") as file:
            json.dump(
                records,
                file,
                indent=2,
                default=decimal_to_json,
                ensure_ascii=False,
            )

    except OSError as exc:
        raise RetailDataGenerationError(
            f"Failed to write JSON file at {output_path}: {exc}",
        ) from exc



def generate_all_datasets(
    output_dir: Path,
    product_count: int,
    customer_count: int,
    store_count: int,
    campaign_count: int,
    sales_count: int,
    inventory_count: int | None,
    return_rate: float,
    seed: int,
) -> dict[str, int]:
    """
    Generate all retail datasets and write them to JSON files.

    Args:
        output_dir: Directory where JSON files will be written.
        product_count: Number of products.
        customer_count: Number of customers.
        store_count: Number of stores.
        campaign_count: Number of campaigns.
        sales_count: Number of sales transactions.
        inventory_count: Optional max number of inventory records.
        return_rate: Approximate percentage of sales returned.
        seed: Random seed for deterministic generation.

    Returns:
        Dictionary mapping dataset name to record count.

    Raises:
        RetailDataGenerationError: If validation or writing fails.
    """

    random.seed(seed)

    try:
        products = generate_products(product_count)
        customers = generate_customers(customer_count)
        stores = generate_stores(store_count)
        campaigns = generate_campaigns(campaign_count)
        sales = generate_sales(sales_count, products, customers, stores, campaigns)
        inventory = generate_inventory(products, stores, inventory_count)
        returns = generate_returns(sales, return_rate)

    except ValidationError as exc:
        raise RetailDataGenerationError(f"Generated data failed schema validation: {exc}") from exc

    datasets: dict[str, list[Any]] = {
        "products": products,
        "customers": customers,
        "stores": stores,
        "campaigns": campaigns,
        "sales": sales,
        "inventory": inventory,
        "returns": returns,
    }

    counts: dict[str, int] = {}

    for dataset_name, records in datasets.items():
        dumped_records = validate_and_dump_records(records)
        output_path = output_dir / f"{dataset_name}.json"

        write_json_file(output_path, dumped_records)

        counts[dataset_name] = len(records)

    return counts



def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.

    Returns:
        Parsed CLI arguments.
    """

    parser = argparse.ArgumentParser(
        description="Generate realistic retail analytics source data.",
    )

    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory where generated JSON files will be written.",
    )

    parser.add_argument(
        "--products",
        type=int,
        default=100,
        help="Number of product records to generate.",
    )

    parser.add_argument(
        "--customers",
        type=int,
        default=500,
        help="Number of customer records to generate.",
    )

    parser.add_argument(
        "--stores",
        type=int,
        default=20,
        help="Number of store records to generate.",
    )

    parser.add_argument(
        "--campaigns",
        type=int,
        default=12,
        help="Number of campaign records to generate.",
    )

    parser.add_argument(
        "--sales",
        type=int,
        default=2_000,
        help="Number of sales transaction records to generate.",
    )

    parser.add_argument(
        "--inventory",
        type=int,
        default=500,
        help="Maximum number of inventory records to generate.",
    )

    parser.add_argument(
        "--return-rate",
        type=float,
        default=0.08,
        help="Approximate percentage of eligible sales that become returns.",
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for repeatable output.",
    )

    return parser.parse_args()



def validate_positive_count(name: str, value: int) -> None:
    """
    Validate that a generated record count is positive.

    Args:
        name: Name of the count argument.
        value: Count value.

    Raises:
        ValueError: If the value is not positive.
    """

    if value <= 0:
        raise ValueError(f"{name} must be greater than 0")



def main() -> int:
    """
    CLI entry point.

    Returns:
        Process exit code.
    """

    args = parse_args()

    try:
        validate_positive_count("products", args.products)
        validate_positive_count("customers", args.customers)
        validate_positive_count("stores", args.stores)
        validate_positive_count("campaigns", args.campaigns)
        validate_positive_count("sales", args.sales)

        if args.inventory is not None and args.inventory <= 0:
            raise ValueError("inventory must be greater than 0")

        counts = generate_all_datasets(
            output_dir=args.output_dir,
            product_count=args.products,
            customer_count=args.customers,
            store_count=args.stores,
            campaign_count=args.campaigns,
            sales_count=args.sales,
            inventory_count=args.inventory,
            return_rate=args.return_rate,
            seed=args.seed,
        )

    except (RetailDataGenerationError, ValueError) as exc:
        print(f"Data generation failed: {exc}", file=sys.stderr)
        return 1

    print("Retail data generated successfully.")
    print(f"Output directory: {args.output_dir}")

    for dataset_name, record_count in counts.items():
        print(f"- {dataset_name}: {record_count} records")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())