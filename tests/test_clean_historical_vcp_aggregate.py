"""
tests/test_clean_historical_vcp_aggregate.py
==============================================
Pytest tests for clean_historical_vcp_aggregate() in scripts/02_clean.py —
Module 4's VCP/Terapeak-aggregate-specific staging cleaner, replacing the
VCP portion of the deprecated shared stg_historical.

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


def _load_clean_module():
    sys.path.insert(0, str(SCRIPTS_DIR))
    spec = importlib.util.spec_from_file_location("clean02_vcp", SCRIPTS_DIR / "02_clean.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


clean02 = _load_clean_module()

RAW_HIST_COLUMNS = [
    "id", "title", "avg_price_eur", "format", "avg_shipping_eur", "free_shipping_pct",
    "total_sold", "total_sales_eur", "last_sold", "bids", "removed",
    "source_file", "original_source_file", "physical_container_file",
]


def _row(id_, title="Rolex generic part", avg_price_eur="50.0", source_file="terapeak_rolex_caliber_p1.html",
         original_source_file=None, format_="Festpreis", total_sold="1", total_sales_eur="50.0",
         last_sold="1. Jul 2025", avg_shipping_eur="5.0", free_shipping_pct="0", bids="-", removed="false"):
    return {
        "id": id_, "title": title, "avg_price_eur": avg_price_eur, "format": format_,
        "avg_shipping_eur": avg_shipping_eur, "free_shipping_pct": free_shipping_pct,
        "total_sold": total_sold, "total_sales_eur": total_sales_eur, "last_sold": last_sold,
        "bids": bids, "removed": removed, "source_file": source_file,
        "original_source_file": original_source_file, "physical_container_file": None,
    }


def _seed_raw_historical(connection, rows: list[dict]) -> None:
    df = pd.DataFrame(rows)
    for col in RAW_HIST_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df["ingested_at"] = "2026-01-01T00:00:00"
    df["row_hash"] = None
    connection.register("tmp_seed", df)
    cols = RAW_HIST_COLUMNS + ["ingested_at", "row_hash"]
    connection.execute(f"INSERT INTO raw_historical ({','.join(cols)}) SELECT {','.join(cols)} FROM tmp_seed")
    connection.unregister("tmp_seed")


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


def _seed_fx(connection) -> None:
    connection.execute("""
        INSERT INTO ref_exchange_rates (from_currency, to_currency, rate, valid_date, source) VALUES
        ('EUR', 'USD', 1.10, DATE '2025-01-01', 'test')
    """)


@pytest.fixture()
def conn(tmp_path):
    db_path = tmp_path / "test.duckdb"
    assert db_path.resolve() != clean02.DB_PATH.resolve()
    connection = duckdb.connect(str(db_path))
    connection.execute(SCHEMA_PATH.read_text())
    _seed_fx(connection)
    yield connection
    connection.close()


def _raw_snapshot(connection):
    return connection.execute("SELECT * FROM raw_historical ORDER BY id").fetchall()


def test_valid_aggregate_row_staged(conn):
    _seed_raw_historical(conn, [_row(1)])
    clean02.clean_historical_vcp_aggregate(conn)
    row = conn.execute(
        "SELECT title, avg_price_eur, brand, format_standard, is_auction FROM stg_historical_vcp_aggregate"
    ).fetchone()
    assert row == ("Rolex generic part", 50.0, "Rolex", "fixed_price", False)


def test_aggregate_semantics_preserved_not_transaction(conn):
    """total_sold/total_sales_eur/avg_price_eur pass through as aggregate
    fields — nothing here reinterprets them as a single transaction."""
    _seed_raw_historical(conn, [_row(1, total_sold="5", total_sales_eur="250.0", avg_price_eur="50.0")])
    clean02.clean_historical_vcp_aggregate(conn)
    row = conn.execute("SELECT total_sold, total_sales_eur, avg_price_eur FROM stg_historical_vcp_aggregate").fetchone()
    assert row == (5, 250.0, 50.0)


def test_no_artificial_transaction_expansion(conn):
    """total_sold=5 must produce exactly ONE staged row, not five."""
    _seed_raw_historical(conn, [_row(1, total_sold="5")])
    clean02.clean_historical_vcp_aggregate(conn)
    count = conn.execute("SELECT COUNT(*) FROM stg_historical_vcp_aggregate").fetchone()[0]
    assert count == 1


def test_german_last_sold_date_parsed(conn):
    _seed_raw_historical(conn, [_row(1, last_sold="7. Aug 2025")])
    clean02.clean_historical_vcp_aggregate(conn)
    row = conn.execute("SELECT last_sold_date, last_sold_year, last_sold_month FROM stg_historical_vcp_aggregate").fetchone()
    assert str(row[0]) == "2025-08-07"
    assert row[1] == 2025
    assert row[2] == 8


def test_duplicate_title_group_size_and_id(conn):
    _seed_raw_historical(conn, [
        _row(1, title="Duplicate Part X", avg_price_eur="10"),
        _row(2, title="Duplicate Part X", avg_price_eur="20"),
        _row(3, title="Duplicate Part X", avg_price_eur="30"),
        _row(4, title="Unique Part Y", avg_price_eur="40"),
    ])
    clean02.clean_historical_vcp_aggregate(conn)
    rows = conn.execute(
        "SELECT title, title_duplicate_group_size, duplicate_group_id FROM stg_historical_vcp_aggregate ORDER BY id"
    ).fetchall()
    dup_rows = [r for r in rows if r[0] == "Duplicate Part X"]
    unique_row = [r for r in rows if r[0] == "Unique Part Y"][0]
    assert all(r[1] == 3 for r in dup_rows)
    assert len({r[2] for r in dup_rows}) == 1  # same group id shared by all 3
    assert unique_row[1] == 1
    assert unique_row[2] != dup_rows[0][2]


def test_no_duplicate_title_collapse(conn):
    """All 3 duplicate-title rows must survive as 3 separate staged rows —
    never collapsed or summed."""
    _seed_raw_historical(conn, [
        _row(1, title="Duplicate Part X", avg_price_eur="10", total_sold="1"),
        _row(2, title="Duplicate Part X", avg_price_eur="20", total_sold="1"),
        _row(3, title="Duplicate Part X", avg_price_eur="30", total_sold="1"),
    ])
    clean02.clean_historical_vcp_aggregate(conn)
    rows = conn.execute(
        "SELECT avg_price_eur, total_sold FROM stg_historical_vcp_aggregate ORDER BY avg_price_eur"
    ).fetchall()
    assert rows == [(10.0, 1), (20.0, 1), (30.0, 1)], "each duplicate-title row must remain independent, never summed"


def test_zero_shipping_confirmed_free_when_pct_100(conn):
    _seed_raw_historical(conn, [_row(1, avg_shipping_eur="0", free_shipping_pct="100")])
    clean02.clean_historical_vcp_aggregate(conn)
    row = conn.execute("SELECT shipping_value_reliability FROM stg_historical_vcp_aggregate").fetchone()
    assert row[0] == "ZERO_CONFIRMED_FREE_SHIPPING"


def test_zero_shipping_ambiguous_when_pct_not_100(conn):
    _seed_raw_historical(conn, [_row(1, avg_shipping_eur="0", free_shipping_pct="50")])
    clean02.clean_historical_vcp_aggregate(conn)
    row = conn.execute("SELECT shipping_value_reliability FROM stg_historical_vcp_aggregate").fetchone()
    assert row[0] == "ZERO_AMBIGUOUS", "a zero must never be silently trusted as free shipping without corroborating evidence"


def test_nonzero_shipping_observed(conn):
    _seed_raw_historical(conn, [_row(1, avg_shipping_eur="12.5", free_shipping_pct="0")])
    clean02.clean_historical_vcp_aggregate(conn)
    row = conn.execute("SELECT shipping_value_reliability FROM stg_historical_vcp_aggregate").fetchone()
    assert row[0] == "OBSERVED_NONZERO"


def test_invalid_price_rows_rejected(conn):
    _seed_raw_historical(conn, [
        _row(1, avg_price_eur="0"),
        _row(2, avg_price_eur=None),
        _row(3, avg_price_eur="-5"),
        _row(4, avg_price_eur="10"),
    ])
    clean02.clean_historical_vcp_aggregate(conn)
    ids = conn.execute("SELECT raw_id FROM stg_historical_vcp_aggregate").fetchall()
    assert [i[0] for i in ids] == [4]


def test_deterministic_row_hash(conn):
    _seed_raw_historical(conn, [_row(1)])
    clean02.clean_historical_vcp_aggregate(conn)
    h1 = conn.execute("SELECT row_hash FROM stg_historical_vcp_aggregate").fetchone()[0]
    clean02.clean_historical_vcp_aggregate(conn)
    h2 = conn.execute("SELECT row_hash FROM stg_historical_vcp_aggregate").fetchone()[0]
    assert h1 == h2
    assert h1 is not None and len(h1) == 64


def test_idempotent_rebuild(conn):
    """Full-rebuild determinism, excluding cleaned_at (a real wall-clock
    timestamp that legitimately differs between two runs by design)."""
    _seed_raw_historical(conn, [_row(1), _row(2, title="Another part")])
    cols = [c[1] for c in conn.execute("PRAGMA table_info('stg_historical_vcp_aggregate')").fetchall() if c[1] != "cleaned_at"]
    query = f"SELECT {','.join(cols)} FROM stg_historical_vcp_aggregate ORDER BY id"
    clean02.clean_historical_vcp_aggregate(conn)
    first = conn.execute(query).fetchall()
    clean02.clean_historical_vcp_aggregate(conn)
    second = conn.execute(query).fetchall()
    assert first == second


def test_raw_historical_never_modified(conn):
    _seed_raw_historical(conn, [_row(1), _row(2, title="Another")])
    before = _raw_snapshot(conn)
    clean02.clean_historical_vcp_aggregate(conn)
    after = _raw_snapshot(conn)
    assert before == after


def test_brand_grounded_via_source_file_when_original_missing(conn):
    """The Step 0 provenance resolution: original_source_file NULL ->
    fall back to source_file, which already carries genuine per-row page
    provenance in the current data."""
    _seed_raw_historical(conn, [
        _row(1, source_file="terapeak_tudor_original_p1.html", original_source_file=None),
    ])
    clean02.clean_historical_vcp_aggregate(conn)
    row = conn.execute("SELECT brand, search_keyword FROM stg_historical_vcp_aggregate").fetchone()
    assert row == ("Tudor", "original")


def test_brand_prefers_original_source_file_when_present(conn):
    _seed_raw_historical(conn, [
        _row(1, source_file="terapeak_rolex_p1.html", original_source_file="terapeak_tudor_genuine_p1.html"),
    ])
    clean02.clean_historical_vcp_aggregate(conn)
    row = conn.execute("SELECT brand, search_keyword FROM stg_historical_vcp_aggregate").fetchone()
    assert row == ("Tudor", "genuine")


def test_regression_sources_never_cross_contaminate(conn):
    """VCP rows can only ever enter stg_historical_vcp_aggregate; the
    deprecated shared stg_historical is untouched by the new cleaner."""
    _seed_raw_historical(conn, [_row(1)])
    conn.execute("""
        INSERT INTO raw_historical_ebay_sold (id, row_hash, item_number, title, price_eur, currency, sold_date_iso, is_sold)
        VALUES (1, 'h1', '9999', 'an eBay sold row', 50.0, 'EUR', DATE '2026-01-01', TRUE)
    """)

    clean02.clean_historical_vcp_aggregate(conn)

    vcp_count = conn.execute("SELECT COUNT(*) FROM stg_historical_vcp_aggregate").fetchone()[0]
    ebay_count = conn.execute("SELECT COUNT(*) FROM stg_historical_ebay_sold").fetchone()[0]
    legacy_count = conn.execute("SELECT COUNT(*) FROM stg_historical").fetchone()[0]
    assert vcp_count == 1
    assert ebay_count == 0, "clean_historical_vcp_aggregate must never write to the eBay staging table"
    assert legacy_count == 0, "clean_historical_vcp_aggregate must never touch the deprecated shared stg_historical"
