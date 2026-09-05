"""
tests/test_historical_ebay_sold_ingestion.py
=============================================
Pytest tests for Module 4's first real acquisition path:
scripts/01_ingest.py::insert_historical_ebay_sold_exports(), ingesting the
eBay item-wise sold-listing source into its own raw_historical_ebay_sold
table.

Covers:
  - whole-file dedup via file hash (re-ingesting the identical file is a no-op)
  - row-level dedup by item_number (eBay's own listing id)
  - idempotent reruns
  - raw_historical (the OTHER historical source, VCP aggregate) is never
    touched by this ingestion path — the two sources are never concatenated
  - raw_historical_ebay_sold itself is treated as immutable once written
    (this ingestion function never UPDATEs an existing row)
  - missing expected columns are rejected loudly, not silently guessed

Every test runs against an isolated on-disk DuckDB file under pytest's own
tmp_path and an isolated EBAY_SOLD_EXPORTS_DIR monkeypatched onto the
01_ingest module — never database/watchparts.duckdb, never
data/raw/historical_exports_ebay_sold/. A module-scoped autouse fixture
hashes the real project database before/after and fails loudly if it changed.

Does NOT implement or exercise: matching, TMV, turnover, or any live
scraping — this is raw ingestion only.
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


ingest01 = _load_module("ingest01_ebay_sold", SCRIPTS_DIR / "01_ingest.py")

EBAY_SOLD_CSV_COLUMNS = [
    "item_number", "title", "price_eur", "currency", "condition", "seller_type",
    "sold_date_iso", "sold_date_raw", "is_sold", "shipping_eur", "free_shipping",
    "best_offer", "location", "seller", "url", "source_page",
]


def _row(item_number: str, title: str = "Rolex part", price_eur: str = "50.00", source_page: str = "p1") -> dict:
    return {
        "item_number": item_number, "title": title, "price_eur": price_eur, "currency": "EUR",
        "condition": "Gebraucht", "seller_type": "Privat", "sold_date_iso": "2026-07-01",
        "sold_date_raw": "1. Jul 2026", "is_sold": "1", "shipping_eur": "5.00",
        "free_shipping": "0", "best_offer": "0", "location": "Berlin",
        "seller": "seller1 99% positiv (10)", "url": f"https://www.ebay.de/itm/{item_number}",
        "source_page": source_page,
    }


def _write_csv(path: Path, rows: list[dict]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    for col in EBAY_SOLD_CSV_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    df = df[EBAY_SOLD_CSV_COLUMNS]
    df.to_csv(path, index=False)
    return path


def _file_digest(path: Path):
    if not path.exists():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture(scope="module", autouse=True)
def guard_production_files_untouched():
    real_db = ingest01.DB_PATH
    real_dir = ingest01.EBAY_SOLD_EXPORTS_DIR
    before_db = _file_digest(real_db)
    before_dir_listing = set(p.name for p in real_dir.iterdir()) if real_dir.exists() else set()
    yield
    after_db = _file_digest(real_db)
    after_dir_listing = set(p.name for p in real_dir.iterdir()) if real_dir.exists() else set()
    assert before_db == after_db, "database/watchparts.duckdb changed — test isolation is broken"
    assert before_dir_listing == after_dir_listing, "data/raw/historical_exports_ebay_sold/ changed — test isolation is broken"


@pytest.fixture()
def conn(tmp_path, monkeypatch):
    db_path = tmp_path / "test.duckdb"
    assert db_path.resolve() != ingest01.DB_PATH.resolve()
    export_dir = tmp_path / "ebay_sold_exports"
    monkeypatch.setattr(ingest01, "EBAY_SOLD_EXPORTS_DIR", export_dir)

    connection = duckdb.connect(str(db_path))
    connection.execute(SCHEMA_PATH.read_text())
    yield connection, export_dir
    connection.close()


def test_no_export_dir_is_a_clean_no_op(conn):
    connection, export_dir = conn
    n = ingest01.insert_historical_ebay_sold_exports(connection)
    assert n == 0


def test_basic_ingestion_inserts_all_rows(conn):
    connection, export_dir = conn
    _write_csv(export_dir / "ebay_sold_items.csv", [_row("111"), _row("222"), _row("333")])
    n = ingest01.insert_historical_ebay_sold_exports(connection)
    assert n == 3
    count = connection.execute("SELECT COUNT(*) FROM raw_historical_ebay_sold").fetchone()[0]
    assert count == 3


def test_reingesting_identical_file_is_a_no_op(conn):
    connection, export_dir = conn
    csv_path = _write_csv(export_dir / "ebay_sold_items.csv", [_row("111"), _row("222")])
    first = ingest01.insert_historical_ebay_sold_exports(connection)
    second = ingest01.insert_historical_ebay_sold_exports(connection)
    assert first == 2
    assert second == 0
    count = connection.execute("SELECT COUNT(*) FROM raw_historical_ebay_sold").fetchone()[0]
    assert count == 2


def test_row_level_dedup_by_item_number_within_one_file(conn):
    """The same item_number appearing twice in one export (e.g. captured
    via two overlapping search-result pages) must be ingested once, not
    twice — it's the same real listing, not a new observation."""
    connection, export_dir = conn
    _write_csv(export_dir / "ebay_sold_items.csv", [
        _row("111", source_page="p1"), _row("111", source_page="p2"), _row("222"),
    ])
    n = ingest01.insert_historical_ebay_sold_exports(connection)
    assert n == 2
    ids = connection.execute("SELECT item_number FROM raw_historical_ebay_sold ORDER BY item_number").fetchall()
    assert [i[0] for i in ids] == ["111", "222"]


def test_row_level_dedup_by_item_number_across_separate_files(conn):
    """The same real listing reappearing in a LATER, separate export file
    must not be duplicated — a sold listing is a closed, immutable fact."""
    connection, export_dir = conn
    _write_csv(export_dir / "batch1.csv", [_row("111"), _row("222")])
    ingest01.insert_historical_ebay_sold_exports(connection)
    _write_csv(export_dir / "batch2.csv", [_row("111"), _row("333")])
    n = ingest01.insert_historical_ebay_sold_exports(connection)
    assert n == 1  # only "333" is new
    total = connection.execute("SELECT COUNT(*) FROM raw_historical_ebay_sold").fetchone()[0]
    assert total == 3


def test_missing_expected_column_stops_with_error(conn):
    connection, export_dir = conn
    export_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame([_row("111")]).drop(columns=["price_eur"])
    (export_dir / "malformed.csv").write_text(df.to_csv(index=False))
    with pytest.raises(SystemExit):
        ingest01.insert_historical_ebay_sold_exports(connection)


def test_never_touches_raw_historical_the_other_source(conn):
    """The two Module 4 sources (VCP aggregate vs eBay item-wise) must
    never be concatenated — verified directly: ingesting eBay sold-listing
    data must leave raw_historical (VCP's table) completely untouched."""
    connection, export_dir = conn
    connection.execute(
        "INSERT INTO raw_historical (id, row_hash, title, avg_price_eur, source_file) "
        "VALUES (1, 'preexisting', 'a VCP aggregate row', 100.0, 'terapeak_sold_last.csv')"
    )
    _write_csv(export_dir / "ebay_sold_items.csv", [_row("111")])
    ingest01.insert_historical_ebay_sold_exports(connection)

    vcp_rows = connection.execute("SELECT COUNT(*) FROM raw_historical").fetchone()[0]
    assert vcp_rows == 1, "raw_historical (VCP source) must never be touched by eBay sold-listing ingestion"
    ebay_rows = connection.execute("SELECT COUNT(*) FROM raw_historical_ebay_sold").fetchone()[0]
    assert ebay_rows == 1


def test_boolean_and_numeric_fields_parsed_correctly(conn):
    connection, export_dir = conn
    _write_csv(export_dir / "ebay_sold_items.csv", [{
        "item_number": "999", "title": "Test", "price_eur": "45.50", "currency": "EUR",
        "condition": "Neu", "seller_type": "Gewerblich", "sold_date_iso": "2026-07-13",
        "sold_date_raw": "13. Jul 2026", "is_sold": "1", "shipping_eur": "20.0",
        "free_shipping": "0", "best_offer": "1", "location": "Spanien",
        "seller": "seller2", "url": "https://www.ebay.de/itm/999", "source_page": "p1",
    }])
    ingest01.insert_historical_ebay_sold_exports(connection)
    row = connection.execute(
        "SELECT price_eur, is_sold, free_shipping, best_offer, shipping_eur FROM raw_historical_ebay_sold WHERE item_number='999'"
    ).fetchone()
    assert row == (45.50, True, False, True, 20.0)


def test_row_hash_populated_for_every_row(conn):
    connection, export_dir = conn
    _write_csv(export_dir / "ebay_sold_items.csv", [_row("111"), _row("222")])
    ingest01.insert_historical_ebay_sold_exports(connection)
    hashes = connection.execute("SELECT row_hash FROM raw_historical_ebay_sold").fetchall()
    assert all(h[0] for h in hashes)
    assert len(set(h[0] for h in hashes)) == 2  # distinct item_numbers -> distinct hashes
