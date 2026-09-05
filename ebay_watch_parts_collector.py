#!/usr/bin/env python3
"""Collect current eBay listings for vintage watch spare parts.

This uses eBay's Browse API with the client-credentials OAuth flow. The access
token is short-lived, so the script caches it locally and refreshes it when it
is close to expiry.
"""

from __future__ import annotations

import argparse
import csv
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ebay_api_common import (
    DEFAULT_SCOPE,
    MARKETPLACES,
    first_value,
    get_access_token,
    load_dotenv,
    search_items as _search_items,
)


BASE_DIR = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / "data"


def flatten_item(item: dict[str, Any], keyword: str, collected_at: str) -> dict[str, str]:
    shipping_options = item.get("shippingOptions") or []
    shipping = shipping_options[0] if shipping_options else {}
    seller = item.get("seller") or {}
    categories = item.get("categories") or []
    category_names = " > ".join(c.get("categoryName", "") for c in categories if c.get("categoryName"))
    category_ids = " > ".join(c.get("categoryId", "") for c in categories if c.get("categoryId"))

    return {
        "collected_at_utc": collected_at,
        "keyword": keyword,
        "source_country": str(item.get("source_country", "")),
        "source_marketplace_id": str(item.get("source_marketplace_id", "")),
        "item_id": first_value(item, ["itemId"]),
        "legacy_item_id": first_value(item, ["legacyItemId"]),
        "title": first_value(item, ["title"]),
        "price_value": first_value(item, ["price", "value"]),
        "price_currency": first_value(item, ["price", "currency"]),
        "condition": first_value(item, ["condition"]),
        "condition_id": first_value(item, ["conditionId"]),
        "buying_options": "|".join(item.get("buyingOptions") or []),
        "item_web_url": first_value(item, ["itemWebUrl"]),
        "image_url": first_value(item, ["image", "imageUrl"]),
        "seller_username": str(seller.get("username", "")),
        "seller_feedback_score": str(seller.get("feedbackScore", "")),
        "seller_feedback_percentage": str(seller.get("feedbackPercentage", "")),
        "shipping_cost_value": first_value(shipping, ["shippingCost", "value"]),
        "shipping_cost_currency": first_value(shipping, ["shippingCost", "currency"]),
        "item_location_country": first_value(item, ["itemLocation", "country"]),
        "item_location_city": first_value(item, ["itemLocation", "city"]),
        "category_ids": category_ids,
        "category_names": category_names,
        "listing_marketplace_id": first_value(item, ["listingMarketplaceId"]),
        "item_creation_date": first_value(item, ["itemCreationDate"]),
    }



def search_items(
    *,
    token: str,
    keyword: str,
    limit: int,
    max_items: int | None,
    sort: str,
    filters: list[str],
) -> list[dict[str, Any]]:
    """Thin wrapper preserving this module's original call signature —
    the actual multi-marketplace pagination now lives in ebay_api_common
    so it's shared with scripts/04_collect_targeted_active.py."""
    return _search_items(
        token=token,
        keyword=keyword,
        limit=limit,
        max_items=max_items,
        sort=sort,
        filters=filters,
    )


def write_csv(rows: list[dict[str, str]], output_path: Path) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys()) if rows else [
        "collected_at_utc",
        "keyword",
        "item_id",
        "title",
    ]
    with output_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def inspect_csv(path: Path = OUTPUT_DIR / "latest.csv", sample_size: int = 2) -> None:
    if not path.exists():
        raise SystemExit(f"Could not find {path}. Run the collector first.")

    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        columns = reader.fieldnames or []
        sample_rows: list[dict[str, str]] = []
        null_counts = {column: 0 for column in columns}
        row_count = 0

        for row in reader:
            row_count += 1
            if len(sample_rows) < sample_size:
                sample_rows.append(row)
            for column in columns:
                if row.get(column, "") == "":
                    null_counts[column] += 1

    print("=== ACTIVE COLUMNS ===")
    print(columns)
    print()
    print("=== ACTIVE SAMPLE ===")
    if sample_rows:
        for index, row in enumerate(sample_rows, start=1):
            print(f"Row {index}:")
            for column in columns:
                print(f"  {column}: {row.get(column, '')}")
    else:
        print("(no rows)")

    print()
    print("=== ACTIVE SHAPE ===")
    print((row_count, len(columns)))
    print()
    print("=== ACTIVE NULLS ===")
    for column, count in null_counts.items():
        print(f"{column}: {count}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect current vintage watch spare-part listings from eBay.")
    parser.add_argument("--keyword", default="vintage watch spare parts")
    parser.add_argument("--limit", type=int, default=200, help="Page size. eBay Browse API max is 200.")
    parser.add_argument(
        "--max-items",
        type=int,
        default=None,
        help="Optional maximum listings to collect. By default, collect all pages eBay returns.",
    )
    parser.add_argument("--sort", default="newlyListed", help="Examples: newlyListed, price, -price, endingSoonest.")
    parser.add_argument(
        "--filter",
        action="append",
        default=["buyingOptions:{FIXED_PRICE|AUCTION}"],
        help="Repeatable eBay Browse filter, for example conditionIds:{3000|7000}.",
    )
    parser.add_argument(
        "--inspect",
        action="store_true",
        help="Show columns, sample rows, shape, and null counts for data/latest.csv instead of collecting new data.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.inspect:
        inspect_csv()
        return

    load_dotenv()
    if not 1 <= args.limit <= 200:
        raise SystemExit("--limit must be between 1 and 200.")

    token = get_access_token(DEFAULT_SCOPE)
    raw_items = search_items(
        token=token,
        keyword=args.keyword,
        limit=args.limit,
        max_items=args.max_items,
        sort=args.sort,
        filters=args.filter,
    )

    collected_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    rows = [flatten_item(item, args.keyword, collected_at) for item in raw_items]
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_path = OUTPUT_DIR / f"ebay_watch_parts_{timestamp}.csv"
    latest_path = OUTPUT_DIR / "latest.csv"
    write_csv(rows, output_path)
    write_csv(rows, latest_path)
    print(f"Collected {len(rows)} listings")
    print(f"Wrote {output_path}")
    print(f"Wrote {latest_path}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped by user before the current eBay request finished.", file=sys.stderr)
        raise SystemExit(130) from None
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1) from None
