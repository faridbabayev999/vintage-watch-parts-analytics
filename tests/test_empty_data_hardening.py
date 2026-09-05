"""
tests/test_empty_data_hardening.py
====================================
Pytest tests for the empty/all-NULL-aggregate crash pattern found in four
locations across scripts/02_clean.py:

  - clean_historical()
  - clean_active_broad()
  - build_competitor_snapshot()
  - report_active_vs_historical_gap() (split out of main()'s inline
    gap-finding block specifically so it's independently testable)

Each of these had a summary/reporting block that formatted a DuckDB
aggregate result (MEDIAN/MIN/MAX/SUM) directly into an f-string with a
":.2f"/":.0f" format spec — on an empty or all-NULL input, the aggregate
returns SQL NULL (Python None), and formatting None with a numeric spec
raises TypeError, halting the entire pipeline mid-run. Fixed via a small
shared guard (_aggregates_available) plus each function's own
empty-case message — never a single over-generalized helper.

Isolation: every test runs against a duckdb file under pytest's tmp_path —
never database/watchparts.duckdb. A module-scoped autouse fixture hashes
the real project database before/after and fails loudly if it changed.
"""

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
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


clean02 = _load_module("clean02_empty_hardening", SCRIPTS_DIR / "02_clean.py")


def _file_digest(path: Path):
    if not path.exists():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture(scope="module", autouse=True)
def guard_production_files_untouched():
    real_db = clean02.DB_PATH
    before = _file_digest(real_db)
    yield
    after = _file_digest(real_db)
    assert before == after, "database/watchparts.duckdb changed — test isolation is broken"


def _seed_ref_tables(connection) -> None:
    connection.execute("""
        INSERT INTO ref_exchange_rates (from_currency, to_currency, rate, valid_date, source) VALUES
        ('USD', 'EUR', 0.90, DATE '2025-12-01', 'test'),
        ('EUR', 'USD', 1.10, DATE '2025-12-01', 'test'),
        ('EUR', 'EUR', 1.00, DATE '2025-12-01', 'identity')
    """)
    connection.execute("""
        INSERT INTO ref_condition_map (condition_raw, condition_standard, language) VALUES
        ('Used', 'Good', 'EN')
    """)


@pytest.fixture()
def conn(tmp_path):
    db_path = tmp_path / "test.duckdb"
    assert db_path.resolve() != clean02.DB_PATH.resolve()
    connection = duckdb.connect(str(db_path))
    connection.execute(SCHEMA_PATH.read_text())
    _seed_ref_tables(connection)
    yield connection
    connection.close()


def _insert_raw_historical(connection, rows: list[dict]) -> None:
    df = pd.DataFrame(rows)
    defaults = {
        "row_hash": "", "format": "Auction", "avg_shipping_eur": 5.0,
        "free_shipping_pct": 50, "total_sold": 1, "total_sales_eur": 100.0,
        "bids": "-", "removed": "No", "source_file": "test.html",
    }
    for key, value in defaults.items():
        if key not in df.columns:
            df[key] = value
    connection.register("tmp_raw_hist", df)
    cols = list(df.columns)
    connection.execute(f"INSERT INTO raw_historical ({','.join(cols)}) SELECT {','.join(cols)} FROM tmp_raw_hist")
    connection.unregister("tmp_raw_hist")


def _insert_raw_broad(connection, rows: list[dict]) -> None:
    df = pd.DataFrame(rows)
    defaults = {
        "row_hash": "", "keyword": "rolex", "source_country": "DE",
        "source_marketplace_id": "EBAY_DE", "legacy_item_id": None,
        "price_currency": "EUR", "condition": "Used", "condition_id": 3000.0,
        "buying_options": "FIXED_PRICE", "item_web_url": "", "image_url": "",
        "seller_username": "seller1", "seller_feedback_score": 100,
        "seller_feedback_percentage": 99.0, "shipping_cost_value": 5.0,
        "shipping_cost_currency": "EUR", "item_location_country": "DE",
        "item_location_city": "Berlin", "category_ids": "173696",
        "category_names": "Watch Parts", "listing_marketplace_id": "EBAY_DE",
        "item_creation_date": "2026-01-01T00:00:00Z",
    }
    for key, value in defaults.items():
        if key not in df.columns:
            df[key] = value
    connection.register("tmp_raw_broad", df)
    cols = list(df.columns)
    connection.execute(f"INSERT INTO raw_active_broad ({','.join(cols)}) SELECT {','.join(cols)} FROM tmp_raw_broad")
    connection.unregister("tmp_raw_broad")


def _insert_stg_active_broad(connection, rows: list[dict]) -> None:
    """Seeds stg_active_broad directly — build_competitor_snapshot() reads
    this table straight, no need to run clean_active_broad() first."""
    df = pd.DataFrame(rows)
    defaults = {
        "raw_id": None, "title": "T", "normalized_title": "t",
        "price_original": 100.0, "price_currency_original": "EUR",
        "price_eur": 100.0, "shipping_eur": 5.0, "landed_cost_eur": 105.0,
        "price_usd": None, "landed_cost_usd": None, "fx_to_eur_rate_used": 1.0,
        "eur_usd_rate_used": None, "fx_rate_date": None, "fx_rate_is_fallback": False,
        "price_virtual_eur": None, "landed_cost_de_eur": None, "landed_cost_us_eur": None,
        "shipping_de_eur": None, "shipping_us_eur": None, "estimated_import_charges_us_eur": None,
        "condition_raw": "Used", "condition_standard": "Good",
        "is_auction": False, "accepts_best_offer": False,
        "seller_username": "seller1", "seller_feedback_score": 100, "seller_feedback_percentage": 99.0,
        "item_location_country": "DE", "marketplace": "EBAY_DE",
        "collected_at_utc": "2026-01-10T09:00:00Z", "item_creation_date": "2026-01-01T00:00:00Z",
        "days_listed": 9,
        "matched_product_id": None, "match_confidence": None, "match_method": None, "match_score": None,
    }
    for key, value in defaults.items():
        if key not in df.columns:
            df[key] = value
    connection.register("tmp_stg_broad", df)
    cols = list(df.columns)
    connection.execute(f"INSERT INTO stg_active_broad ({','.join(cols)}) SELECT {','.join(cols)} FROM tmp_stg_broad")
    connection.unregister("tmp_stg_broad")


# ── A. clean_historical() ───────────────────────────────────────────────────

def test_clean_historical_zero_rows_does_not_crash(conn):
    clean02.clean_historical(conn)  # empty raw_historical — must not raise
    assert conn.execute("SELECT COUNT(*) FROM stg_historical").fetchone()[0] == 0


def test_clean_historical_normal_data_unchanged(conn):
    _insert_raw_historical(conn, [
        dict(id=1, title="Rolex bridge", avg_price_eur=100.0, last_sold="1. Jun 2024"),
    ])
    clean02.clean_historical(conn)
    row = conn.execute("SELECT avg_price_eur, avg_price_usd FROM stg_historical WHERE id=1").fetchone()
    assert row[0] == pytest.approx(100.0)
    assert row[1] is not None


# ── B. clean_active_broad() ─────────────────────────────────────────────────

def test_clean_active_broad_zero_rows_does_not_crash(conn):
    clean02.clean_active_broad(conn)  # empty raw_active_broad
    assert conn.execute("SELECT COUNT(*) FROM stg_active_broad").fetchone()[0] == 0


def test_clean_active_broad_all_rows_dropped_for_missing_price_does_not_crash(conn):
    """Every raw row has no price — all rows get dropped before landed
    cost is ever computed, leaving stg_active_broad empty (not just
    '0 input rows')."""
    _insert_raw_broad(conn, [
        dict(id=1, item_id="i1", title="T", price_value=None),
        dict(id=2, item_id="i2", title="T", price_value=0.0),
    ])
    clean02.clean_active_broad(conn)
    assert conn.execute("SELECT COUNT(*) FROM stg_active_broad").fetchone()[0] == 0


def test_clean_active_broad_partial_data_normal_rows_still_summarized(conn):
    """A mix of one row with a resolvable currency and one with an
    unresolvable one — the summary must still reflect the resolvable row,
    not be suppressed just because one row failed to convert."""
    _insert_raw_broad(conn, [
        dict(id=1, item_id="i1", title="T", price_value=100.0, price_currency="EUR"),
        dict(id=2, item_id="i2", title="T", price_value=100.0, price_currency="ZZZ"),
    ])
    clean02.clean_active_broad(conn)
    rows = conn.execute("SELECT item_id, landed_cost_eur FROM stg_active_broad ORDER BY item_id").fetchall()
    assert rows == [("i1", 105.0), ("i2", None)]


def test_clean_active_broad_normal_behaviour_unchanged(conn):
    _insert_raw_broad(conn, [
        dict(id=1, item_id="i1", title="T", price_value=100.0, price_currency="EUR"),
    ])
    clean02.clean_active_broad(conn)
    count = conn.execute("SELECT COUNT(*) FROM stg_active_broad").fetchone()[0]
    assert count == 1


# ── C. build_competitor_snapshot() ──────────────────────────────────────────

def test_build_competitor_snapshot_zero_sellers_does_not_crash(conn):
    clean02.build_competitor_snapshot(conn)  # empty stg_active_broad
    assert conn.execute("SELECT COUNT(*) FROM feat_competitor").fetchone()[0] == 0


def test_build_competitor_snapshot_all_null_seller_username_does_not_crash(conn):
    _insert_stg_active_broad(conn, [
        dict(id=1, item_id="i1", seller_username=None),
        dict(id=2, item_id="i2", seller_username=None),
    ])
    clean02.build_competitor_snapshot(conn)
    assert conn.execute("SELECT COUNT(*) FROM feat_competitor").fetchone()[0] == 0


def test_build_competitor_snapshot_normal_behaviour_unchanged(conn):
    _insert_stg_active_broad(conn, [
        dict(id=1, item_id="i1", seller_username="alice", landed_cost_eur=100.0),
        dict(id=2, item_id="i2", seller_username="bob", landed_cost_eur=200.0),
    ])
    clean02.build_competitor_snapshot(conn)
    rows = conn.execute("SELECT seller_username, total_listings FROM feat_competitor ORDER BY seller_username").fetchall()
    assert rows == [("alice", 1), ("bob", 1)]


# ── D. report_active_vs_historical_gap() (main()'s gap-finding block) ───────

def test_gap_comparison_insufficient_data_prints_message_not_crash(conn, capsys):
    """Both stg_historical and stg_active_broad empty — must print the
    exact required message and return normally, never raise."""
    clean02.report_active_vs_historical_gap(conn)  # must not raise
    captured = capsys.readouterr()
    assert "insufficient data for gap comparison" in captured.out


def test_gap_comparison_partial_data_one_side_empty_prints_message(conn, capsys):
    """Historical has data but active_broad is empty — still insufficient
    for a two-sided comparison, must not crash."""
    _insert_raw_historical(conn, [
        dict(id=1, title="Rolex bridge", avg_price_eur=100.0, last_sold="1. Jun 2024"),
    ])
    clean02.clean_historical(conn)
    clean02.report_active_vs_historical_gap(conn)  # must not raise
    captured = capsys.readouterr()
    assert "insufficient data for gap comparison" in captured.out


def test_gap_comparison_normal_behaviour_unchanged(conn, capsys):
    _insert_raw_historical(conn, [
        dict(id=1, title="Rolex bridge", avg_price_eur=100.0, last_sold="1. Jun 2024"),
    ])
    clean02.clean_historical(conn)
    _insert_raw_broad(conn, [
        dict(id=1, item_id="i1", title="T", price_value=50.0, price_currency="EUR"),
    ])
    clean02.clean_active_broad(conn)
    clean02.report_active_vs_historical_gap(conn)
    captured = capsys.readouterr()
    assert "Gap (EUR)" in captured.out
    assert "insufficient data" not in captured.out
