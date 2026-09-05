"""
tests/test_clean_historical_ebay_sold.py
==========================================
Pytest tests for clean_historical_ebay_sold() in scripts/02_clean.py —
Module 4's eBay item-wise sold-listing staging cleaner.

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
    spec = importlib.util.spec_from_file_location("clean02_ebay_hist", SCRIPTS_DIR / "02_clean.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


clean02 = _load_clean_module()

RAW_EBAY_COLUMNS = [
    "id", "item_number", "title", "price_eur", "currency", "condition", "seller_type",
    "sold_date_iso", "sold_date_raw", "is_sold", "shipping_eur", "free_shipping",
    "best_offer", "location", "seller", "url", "source_page",
    "upload_batch_id", "source_filename", "file_hash",
]


def _row(id_, item_number, title="Rolex part", price_eur="50.0", currency="EUR",
         condition="Gebraucht", seller_type="Privat", sold_date_iso="2026-01-15",
         shipping_eur="5.0", free_shipping=False, best_offer=False, location="Berlin"):
    return {
        "id": id_, "item_number": item_number, "title": title, "price_eur": price_eur,
        "currency": currency, "condition": condition, "seller_type": seller_type,
        "sold_date_iso": sold_date_iso, "sold_date_raw": "raw", "is_sold": True,
        "shipping_eur": shipping_eur, "free_shipping": free_shipping, "best_offer": best_offer,
        "location": location, "seller": "seller1", "url": f"https://ebay.de/itm/{item_number}",
        "source_page": "p1",
    }


def _seed_raw_ebay(connection, rows: list[dict]) -> None:
    df = pd.DataFrame(rows)
    for col in RAW_EBAY_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df["ingested_at"] = "2026-01-01T00:00:00"
    df["row_hash"] = df["item_number"].apply(lambda v: hashlib.sha256(str(v).encode()).hexdigest())
    connection.register("tmp_seed", df)
    cols = RAW_EBAY_COLUMNS + ["ingested_at", "row_hash"]
    connection.execute(
        f"INSERT INTO raw_historical_ebay_sold ({','.join(cols)}) SELECT {','.join(cols)} FROM tmp_seed"
    )
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
        ('EUR', 'USD', 1.10, DATE '2025-01-01', 'test'),
        ('EUR', 'EUR', 1.00, DATE '2025-01-01', 'test'),
        ('GBP', 'EUR', 1.15, DATE '2025-01-01', 'test')
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
    return connection.execute("SELECT * FROM raw_historical_ebay_sold ORDER BY id").fetchall()


def test_valid_sold_observation_staged(conn):
    _seed_raw_ebay(conn, [_row(1, "1001")])
    clean02.clean_historical_ebay_sold(conn)
    row = conn.execute(
        "SELECT item_number, title, price_eur, sold_date FROM stg_historical_ebay_sold"
    ).fetchone()
    assert row == ("1001", "Rolex part", 50.0, __import__("datetime").date(2026, 1, 15))


def test_best_offer_proxy_classification(conn):
    _seed_raw_ebay(conn, [_row(1, "1001", best_offer=True)])
    clean02.clean_historical_ebay_sold(conn)
    row = conn.execute("SELECT price_reliability FROM stg_historical_ebay_sold").fetchone()
    assert row[0] == "LISTED_PRICE_PROXY_BEST_OFFER"


def test_non_best_offer_displayed_price_classification(conn):
    _seed_raw_ebay(conn, [_row(1, "1001", best_offer=False)])
    clean02.clean_historical_ebay_sold(conn)
    row = conn.execute("SELECT price_reliability FROM stg_historical_ebay_sold").fetchone()
    assert row[0] == "CONFIRMED_DISPLAYED_SOLD_PRICE"


def test_currency_retained_and_converted(conn):
    _seed_raw_ebay(conn, [_row(1, "1001", price_eur="100", currency="GBP", shipping_eur="10")])
    clean02.clean_historical_ebay_sold(conn)
    row = conn.execute(
        "SELECT price_original, currency_original, price_eur FROM stg_historical_ebay_sold"
    ).fetchone()
    assert row[0] == 100.0
    assert row[1] == "GBP"
    assert row[2] == pytest.approx(115.0)  # 100 * 1.15 GBP->EUR


def test_sold_date_parsed(conn):
    _seed_raw_ebay(conn, [_row(1, "1001", sold_date_iso="2025-12-25")])
    clean02.clean_historical_ebay_sold(conn)
    row = conn.execute("SELECT sold_date FROM stg_historical_ebay_sold").fetchone()
    assert str(row[0]) == "2025-12-25"


def test_missing_shipping_with_free_shipping_true_gives_zero(conn):
    _seed_raw_ebay(conn, [_row(1, "1001", shipping_eur=None, free_shipping=True, price_eur="50")])
    clean02.clean_historical_ebay_sold(conn)
    row = conn.execute("SELECT shipping_eur, landed_cost_eur FROM stg_historical_ebay_sold").fetchone()
    assert row == (0.0, 50.0)


def test_missing_shipping_without_free_proof_stays_null(conn):
    _seed_raw_ebay(conn, [_row(1, "1001", shipping_eur=None, free_shipping=False, price_eur="50")])
    clean02.clean_historical_ebay_sold(conn)
    row = conn.execute("SELECT shipping_eur, landed_cost_eur FROM stg_historical_ebay_sold").fetchone()
    assert row == (None, None), "unknown shipping must never be silently replaced with zero"


def test_contaminated_condition_remains_unmapped(conn):
    _seed_raw_ebay(conn, [_row(1, "1001", condition="Referenz 126200")])
    clean02.clean_historical_ebay_sold(conn)
    row = conn.execute("SELECT condition_raw, condition_standard FROM stg_historical_ebay_sold").fetchone()
    assert row == ("Referenz 126200", None)


def test_neu_and_neu_sonstige_map_correctly(conn):
    _seed_raw_ebay(conn, [
        _row(1, "1001", condition="Neu"),
        _row(2, "1002", condition="Neu (Sonstige)"),
    ])
    clean02.clean_historical_ebay_sold(conn)
    rows = dict(conn.execute("SELECT item_number, condition_standard FROM stg_historical_ebay_sold").fetchall())
    assert rows == {"1001": "New", "1002": "New"}


def test_multi_unit_title_flag(conn):
    _seed_raw_ebay(conn, [
        _row(1, "1001", title="2x Rolex balance screws"),
        _row(2, "1002", title="Konvolut Rolex Ersatzteile"),
        _row(3, "1003", title="Single Rolex crown, genuine"),
    ])
    clean02.clean_historical_ebay_sold(conn)
    rows = dict(conn.execute("SELECT item_number, possible_multi_unit_lot FROM stg_historical_ebay_sold").fetchall())
    assert rows["1001"] is True
    assert rows["1002"] is True
    assert rows["1003"] is False


def test_item_number_uniqueness_preserved(conn):
    _seed_raw_ebay(conn, [_row(1, "1001"), _row(2, "1002"), _row(3, "1003")])
    clean02.clean_historical_ebay_sold(conn)
    ids = conn.execute("SELECT item_number FROM stg_historical_ebay_sold ORDER BY item_number").fetchall()
    assert [i[0] for i in ids] == ["1001", "1002", "1003"]


def test_fx_fallback_traceability(conn):
    """A currency/date combo with no ASOF match must fall back to the
    latest known rate AND be flagged via fx_rate_is_fallback."""
    _seed_raw_ebay(conn, [_row(1, "1001", price_eur="100", currency="GBP", sold_date_iso="2020-01-01")])
    clean02.clean_historical_ebay_sold(conn)
    row = conn.execute("SELECT price_eur, fx_rate_is_fallback FROM stg_historical_ebay_sold").fetchone()
    assert row[0] == pytest.approx(115.0)  # falls back to the only GBP rate (1.15), 100 * 1.15
    assert row[1] is True


def test_invalid_price_rows_rejected(conn):
    _seed_raw_ebay(conn, [
        _row(1, "1001", price_eur="0"),
        _row(2, "1002", price_eur=None),
        _row(3, "1003", price_eur="10"),
    ])
    clean02.clean_historical_ebay_sold(conn)
    ids = conn.execute("SELECT item_number FROM stg_historical_ebay_sold").fetchall()
    assert [i[0] for i in ids] == ["1003"]


def test_idempotent_rebuild(conn):
    _seed_raw_ebay(conn, [_row(1, "1001"), _row(2, "1002")])
    cols = [c[1] for c in conn.execute("PRAGMA table_info('stg_historical_ebay_sold')").fetchall() if c[1] != "cleaned_at"]
    query = f"SELECT {','.join(cols)} FROM stg_historical_ebay_sold ORDER BY id"
    clean02.clean_historical_ebay_sold(conn)
    first = conn.execute(query).fetchall()
    clean02.clean_historical_ebay_sold(conn)
    second = conn.execute(query).fetchall()
    assert first == second


def test_raw_historical_ebay_sold_never_modified(conn):
    _seed_raw_ebay(conn, [_row(1, "1001"), _row(2, "1002")])
    before = _raw_snapshot(conn)
    clean02.clean_historical_ebay_sold(conn)
    after = _raw_snapshot(conn)
    assert before == after


def test_extraction_completeness_unknown(conn):
    _seed_raw_ebay(conn, [_row(1, "1001")])
    clean02.clean_historical_ebay_sold(conn)
    row = conn.execute("SELECT extraction_completeness FROM stg_historical_ebay_sold").fetchone()
    assert row[0] == "UNKNOWN"


def test_regression_sources_never_cross_contaminate(conn):
    """VCP rows can only ever enter stg_historical_vcp_aggregate; eBay sold
    rows can only ever enter stg_historical_ebay_sold; the deprecated
    shared stg_historical is untouched by either new cleaner."""
    conn.execute("""
        INSERT INTO raw_historical (id, row_hash, title, avg_price_eur, source_file)
        VALUES (1, 'h1', 'a VCP row', 100.0, 'terapeak_rolex_p1.html')
    """)
    _seed_raw_ebay(conn, [_row(1, "1001")])

    clean02.clean_historical_ebay_sold(conn)

    ebay_count = conn.execute("SELECT COUNT(*) FROM stg_historical_ebay_sold").fetchone()[0]
    vcp_count = conn.execute("SELECT COUNT(*) FROM stg_historical_vcp_aggregate").fetchone()[0]
    legacy_count = conn.execute("SELECT COUNT(*) FROM stg_historical").fetchone()[0]
    assert ebay_count == 1
    assert vcp_count == 0, "clean_historical_ebay_sold must never write to the VCP staging table"
    assert legacy_count == 0, "clean_historical_ebay_sold must never touch the deprecated shared stg_historical"
