"""
tests/test_clean_orchestration.py
===================================
Regression test for the pipeline-orchestration gap confirmed in
docs/MODULE5_LINEAGE_INTEGRITY_AUDIT.md / MODULE5_MIGRATION_READINESS_AUDIT_V2.md:
scripts/02_clean.py's main() previously never called
clean_historical_vcp_aggregate() or clean_historical_ebay_sold(), so
`python scripts/02_clean.py` silently left those two staging tables
un-rebuilt. This test pins down that main() rebuilds ALL FIVE staging
tables, plus that each of the four Module-5-relevant tables gets a
populated stable_evidence_uid (not just non-empty rows).

Isolation: runs against a duckdb file under pytest's tmp_path, never
database/watchparts.duckdb. A module-scoped autouse fixture hashes the
real project database before/after and fails loudly if it changed.
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


def _load_clean_module():
    sys.path.insert(0, str(SCRIPTS_DIR))
    spec = importlib.util.spec_from_file_location("clean02_orchestration", SCRIPTS_DIR / "02_clean.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


clean02 = _load_clean_module()


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


@pytest.fixture()
def db_path(tmp_path):
    path = tmp_path / "test.duckdb"
    assert path.resolve() != clean02.DB_PATH.resolve()
    seed_conn = duckdb.connect(str(path))
    seed_conn.execute(SCHEMA_PATH.read_text())
    seed_conn.close()
    return path


@pytest.fixture()
def conn(db_path):
    # main() calls conn.close() itself at the end -- reconnect fresh for
    # seeding, then reconnect again after main() runs for assertions.
    connection = duckdb.connect(str(db_path))
    yield connection


def _seed_fx(connection) -> None:
    connection.execute("""
        INSERT INTO ref_exchange_rates (from_currency, to_currency, rate, valid_date, source) VALUES
        ('EUR', 'USD', 1.10, DATE '2025-01-01', 'test'),
        ('EUR', 'EUR', 1.00, DATE '2025-01-01', 'test')
    """)


def _seed_minimal_raw(connection) -> None:
    """One row each for raw_historical, raw_historical_ebay_sold,
    raw_active_broad, raw_active_targeted, raw_inventory — the minimum
    needed for every one of main()'s five cleaners to have something to
    clean, so a skipped cleaner shows up as a genuinely empty staging
    table, not merely an empty-input no-op."""
    # clean_inventory() (unrelated to this fix) does not tolerate a fully
    # empty raw_inventory — seed one row so main()'s first step doesn't
    # error before reaching the four cleaners under test.
    connection.execute("""
        INSERT INTO raw_inventory (id, upload_batch_id, raw_rolex_tudor, raw_calibre, raw_p_number, raw_stock)
        VALUES (1, 'batch1', 'Rolex', '3135', '266', '1')
    """)
    connection.execute("""
        INSERT INTO raw_historical (id, title, avg_price_eur, total_sold, last_sold, free_shipping_pct, format, bids, removed)
        VALUES (1, 'Rolex 3135 crown', '100.0', '1', '1. Jul 2025', '0', 'Festpreis', '-', 'false')
    """)
    connection.execute("""
        INSERT INTO raw_historical_ebay_sold (id, item_number, title, price_eur, currency,
            condition, seller_type, sold_date_iso, is_sold, shipping_eur, free_shipping, best_offer, location, seller, url)
        VALUES (1, '111222333', 'Rolex 3135 bridge', 50.0, 'EUR', 'Gebraucht', 'Privat',
            DATE '2025-01-01', TRUE, 5.0, FALSE, FALSE, 'Berlin', 'seller1', 'https://ebay.de/itm/111222333')
    """)
    connection.execute("""
        INSERT INTO raw_active_broad (id, item_id, title, price_value, price_currency,
            condition, buying_options, seller_username, item_location_country,
            listing_marketplace_id, collected_at_utc)
        VALUES (1, '444555666', 'Rolex 3135 wheel', 80.0, 'EUR', 'Used', 'FIXED_PRICE',
            'seller2', 'DE', 'EBAY_DE', TIMESTAMP '2025-01-01 00:00:00')
    """)
    connection.execute("""
        INSERT INTO raw_active_targeted (id, collection_batch_id, inventory_uid,
            marketplace_id, fetched_at, item_id, title, price_value, price_currency, condition)
        VALUES (1, 'batch1', 'iuid_test1', 'EBAY_DE', '2025-01-01T00:00:00Z',
            '777888999', 'Rolex 3135 lever', 60.0, 'EUR', 'Used')
    """)
    # staging_inventory intentionally not seeded — not read by any of the
    # four Module-5-relevant cleaners under test here.


def test_main_rebuilds_all_five_staging_tables(conn, db_path, monkeypatch):
    _seed_fx(conn)
    _seed_minimal_raw(conn)
    monkeypatch.setattr(clean02, "get_connection", lambda: conn)
    monkeypatch.setattr(clean02, "setup_logging", lambda *a, **k: None)

    clean02.main()  # closes `conn` itself at the end

    verify = duckdb.connect(str(db_path))
    counts = {
        t: verify.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        for t in [
            "stg_historical", "stg_historical_vcp_aggregate", "stg_historical_ebay_sold",
            "stg_active_broad", "stg_active_targeted",
        ]
    }
    verify.close()
    empty = [t for t, c in counts.items() if c == 0]
    assert not empty, (
        f"main() left these staging tables empty despite seeded raw data: {empty} — "
        f"the orchestration gap (docs/MODULE5_LINEAGE_INTEGRITY_AUDIT.md) has regressed"
    )


def test_main_populates_stable_evidence_uid_on_every_module5_source(conn, db_path, monkeypatch):
    _seed_fx(conn)
    _seed_minimal_raw(conn)
    monkeypatch.setattr(clean02, "get_connection", lambda: conn)
    monkeypatch.setattr(clean02, "setup_logging", lambda *a, **k: None)

    clean02.main()  # closes `conn` itself at the end

    verify = duckdb.connect(str(db_path))
    for t in ["stg_historical_vcp_aggregate", "stg_historical_ebay_sold", "stg_active_broad", "stg_active_targeted"]:
        null_count = verify.execute(f"SELECT COUNT(*) FROM {t} WHERE stable_evidence_uid IS NULL").fetchone()[0]
        total = verify.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        assert total > 0, f"{t} is empty — cannot assert identity population"
        assert null_count == 0, f"{t} has {null_count}/{total} rows with a NULL stable_evidence_uid"
    verify.close()
