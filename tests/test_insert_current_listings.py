"""
tests/test_insert_current_listings.py
=======================================
Pytest tests for scripts/01_ingest.py::insert_current_listings() — the
raw_active_broad ("current listings" / active-broad) ingestion path.

Covers the freshness fix: dedup must be row_hash-based (title/price_value/
price_currency/seller_username/item_location_country/item_location_city),
never item_id-only. Before this fix, once an item_id had been ingested
once, every later re-ingestion of latest.csv containing that same item_id
was silently treated as a duplicate and dropped — even if the price had
changed. Confirmed on live data before the fix: 0 of 3,890 item_ids in
raw_active_broad had ever had a second observation survive ingestion.

Every test runs against an isolated on-disk DuckDB file under pytest's own
tmp_path and an isolated CURRENT_LISTINGS_CSV path monkeypatched onto the
01_ingest module — never database/watchparts.duckdb, never data/raw/latest.csv.
A module-scoped autouse fixture hashes the real project database before/after
and fails loudly if it changed.
"""

from __future__ import annotations

import hashlib
import importlib.util
import sys
from pathlib import Path

import duckdb
import pandas as pd
import pytest

TESTS_DIR = Path(__file__).parent
BASE_DIR = TESTS_DIR.parent
SCRIPTS_DIR = BASE_DIR / "scripts"
SCHEMA_PATH = SCRIPTS_DIR / "schema.sql"


def _load_module(name: str, path: Path):
    sys.path.insert(0, str(SCRIPTS_DIR))
    sys.path.insert(0, str(BASE_DIR))
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ingest01 = _load_module("ingest01_current_listings", SCRIPTS_DIR / "01_ingest.py")

CURRENT_LISTINGS_COLUMNS = [
    "collected_at_utc", "keyword", "source_country", "source_marketplace_id",
    "item_id", "legacy_item_id", "title", "price_value", "price_currency",
    "condition", "condition_id", "buying_options", "item_web_url", "image_url",
    "seller_username", "seller_feedback_score", "seller_feedback_percentage",
    "shipping_cost_value", "shipping_cost_currency", "item_location_country",
    "item_location_city", "category_ids", "category_names",
    "listing_marketplace_id", "item_creation_date",
]


def _row(item_id: str, price_value: str = "100.0", collected_at_utc: str = "2026-07-10T09:00:00Z") -> dict:
    return {
        "collected_at_utc": collected_at_utc, "keyword": "rolex", "source_country": "DE",
        "source_marketplace_id": "EBAY_DE", "item_id": item_id, "legacy_item_id": "",
        "title": "Rolex bridge", "price_value": price_value, "price_currency": "EUR",
        "condition": "Used", "condition_id": "3000", "buying_options": "FIXED_PRICE",
        "item_web_url": f"https://ebay.de/itm/{item_id}", "image_url": "",
        "seller_username": "seller1", "seller_feedback_score": "100",
        "seller_feedback_percentage": "99.0", "shipping_cost_value": "5.0",
        "shipping_cost_currency": "EUR", "item_location_country": "DE",
        "item_location_city": "Berlin", "category_ids": "173696", "category_names": "Watch Parts",
        "listing_marketplace_id": "EBAY_DE", "item_creation_date": "2026-01-01T00:00:00Z",
    }


def _write_csv(path: Path, rows: list[dict]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)[CURRENT_LISTINGS_COLUMNS]
    df.to_csv(path, index=False)
    return path


def _file_digest(path: Path):
    if not path.exists():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture(scope="module", autouse=True)
def guard_production_files_untouched():
    real_db = ingest01.DB_PATH
    real_csv = ingest01.CURRENT_LISTINGS_CSV
    before_db = _file_digest(real_db)
    before_csv = _file_digest(real_csv)
    yield
    after_db = _file_digest(real_db)
    after_csv = _file_digest(real_csv)
    assert before_db == after_db, "database/watchparts.duckdb changed — test isolation is broken"
    assert before_csv == after_csv, "data/raw/latest.csv changed — test isolation is broken"


@pytest.fixture()
def conn(tmp_path, monkeypatch):
    db_path = tmp_path / "test.duckdb"
    assert db_path.resolve() != ingest01.DB_PATH.resolve()
    csv_path = tmp_path / "latest.csv"
    monkeypatch.setattr(ingest01, "CURRENT_LISTINGS_CSV", csv_path)

    connection = duckdb.connect(str(db_path))
    connection.execute(SCHEMA_PATH.read_text())
    yield connection, csv_path
    connection.close()


def test_basic_ingestion_inserts_all_rows(conn):
    connection, csv_path = conn
    _write_csv(csv_path, [_row("i1"), _row("i2"), _row("i3")])
    n = ingest01.insert_current_listings(connection)
    assert n == 3
    assert connection.execute("SELECT COUNT(*) FROM raw_active_broad").fetchone()[0] == 3


def test_reingesting_identical_snapshot_is_a_no_op(conn):
    """Same item_id, same price, re-exported and re-ingested — this is
    genuinely the SAME observation and must still be skipped."""
    connection, csv_path = conn
    _write_csv(csv_path, [_row("i1", price_value="100.0")])
    ingest01.insert_current_listings(connection)
    _write_csv(csv_path, [_row("i1", price_value="100.0")])
    n = ingest01.insert_current_listings(connection)
    assert n == 0
    assert connection.execute("SELECT COUNT(*) FROM raw_active_broad").fetchone()[0] == 1


def test_price_change_is_retained_as_a_new_observation(conn):
    """The core freshness fix: the SAME item_id re-appearing with a
    DIFFERENT price must be stored as a second row, not silently dropped."""
    connection, csv_path = conn
    _write_csv(csv_path, [_row("i1", price_value="100.0", collected_at_utc="2026-07-10T09:00:00Z")])
    ingest01.insert_current_listings(connection)
    _write_csv(csv_path, [_row("i1", price_value="130.0", collected_at_utc="2026-07-20T09:00:00Z")])
    n = ingest01.insert_current_listings(connection)
    assert n == 1, "a genuinely changed price must be inserted as a new observation, not skipped"

    rows = connection.execute(
        "SELECT price_value, collected_at_utc FROM raw_active_broad WHERE item_id = 'i1' ORDER BY collected_at_utc"
    ).fetchall()
    assert len(rows) == 2, "both the old and new observation must be preserved (append-only)"
    assert [r[0] for r in rows] == [100.0, 130.0]


def test_unchanged_item_among_changed_items_only_new_ones_inserted(conn):
    connection, csv_path = conn
    _write_csv(csv_path, [_row("i1", price_value="100.0"), _row("i2", price_value="200.0")])
    ingest01.insert_current_listings(connection)
    _write_csv(csv_path, [_row("i1", price_value="100.0"), _row("i2", price_value="250.0")])
    n = ingest01.insert_current_listings(connection)
    assert n == 1
    i2_prices = [r[0] for r in connection.execute(
        "SELECT price_value FROM raw_active_broad WHERE item_id = 'i2' ORDER BY id"
    ).fetchall()]
    assert i2_prices == [200.0, 250.0]
    i1_prices = [r[0] for r in connection.execute(
        "SELECT price_value FROM raw_active_broad WHERE item_id = 'i1'"
    ).fetchall()]
    assert i1_prices == [100.0]


def test_raw_row_never_modified_only_appended(conn):
    """Raw-layer immutability: re-ingesting with a changed price must not
    UPDATE the existing row's price — the old observation stays exactly
    as it was, a new row is appended alongside it."""
    connection, csv_path = conn
    _write_csv(csv_path, [_row("i1", price_value="100.0")])
    ingest01.insert_current_listings(connection)
    original_id = connection.execute("SELECT id FROM raw_active_broad WHERE item_id = 'i1'").fetchone()[0]

    _write_csv(csv_path, [_row("i1", price_value="999.0")])
    ingest01.insert_current_listings(connection)

    original_row_price = connection.execute(
        "SELECT price_value FROM raw_active_broad WHERE id = ?", [original_id]
    ).fetchone()[0]
    assert original_row_price == 100.0, "the original observation must never be overwritten in place"
