"""
Module 5 — dashboard data-access layer tests.

STANDING RULE (post live-DB incident): every dashboard/lineage test asserts
WHICH database it is connected to (assert_db_target) BEFORE running any query.
A test that confirms the right table/columns but silently runs against the
wrong database instance is not actually verified.
"""
import importlib.util
import sys
from pathlib import Path

import duckdb
import pytest

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
SCHEMA = SCRIPTS / "schema.sql"


def _load():
    spec = importlib.util.spec_from_file_location("dash16", SCRIPTS / "16_dashboard_data.py")
    m = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(SCRIPTS))
    spec.loader.exec_module(m)
    return m


@pytest.fixture
def db(tmp_path):
    p = tmp_path / "dash.duckdb"
    duckdb.connect(str(p)).execute(SCHEMA.read_text())
    return p


def _ro(db):
    return duckdb.connect(str(db), read_only=True)


def _insert_tmv(db):
    c = duckdb.connect(str(db))
    c.execute("""INSERT INTO staging_inventory (inventory_uid, canonical_inventory_id, brand, caliber, part_number, validation_status)
                 VALUES ('inv1','rolex_3135_x','Rolex','3135','3135-570','PASS')""")
    c.execute("""INSERT INTO tmv_results (canonical_inventory_id, tmv_eur, tmv_low_eur, tmv_high_eur, confidence_tier)
                 VALUES ('rolex_3135_x', 180.0, 153.0, 207.0, 'MEDIUM')""")
    c.execute("""INSERT INTO turnover_survival (canonical_inventory_id, median_days_to_sell, probability_sell_30d, probability_sell_90d)
                 VALUES ('rolex_3135_x', 92.5, 0.20, 0.49)""")
    c.execute("""INSERT INTO feat_pricing (canonical_inventory_id, brand, caliber, part_number)
                 VALUES ('rolex_3135_x','Rolex','3135','3135-570')""")
    c.close()


# ---- DB-target discipline -----------------------------------------------------

def test_assert_db_target(db, tmp_path):
    m = _load()
    conn = _ro(db)
    assert m.assert_db_target(conn, db)           # correct target passes
    with pytest.raises(AssertionError):
        m.assert_db_target(conn, tmp_path / "some_other.duckdb")   # wrong target caught
    conn.close()


# ---- empty state --------------------------------------------------------------

def test_awaiting_state_when_no_evidence(db):
    m = _load()
    conn = _ro(db)
    m.assert_db_target(conn, db)                   # RULE: target first
    st = m.dashboard_state(conn)
    assert st["state"] == "AWAITING_EVIDENCE"
    assert st["message"] == m.AWAITING_MESSAGE
    assert st["n_confirmed"] == 0 and st["n_tmv"] == 0
    assert m.load_items(conn) == []                # no fabricated rows/zeros
    conn.close()


def test_ready_state_shows_backend_values(db):
    m = _load()
    _insert_tmv(db)
    conn = _ro(db)
    m.assert_db_target(conn, db)
    st = m.dashboard_state(conn)
    assert st["state"] == "READY"
    items = m.load_items(conn)
    assert len(items) == 1
    it = items[0]
    assert it["tmv_eur"] == 180.0 and it["confidence_tier"] == "MEDIUM"
    assert it["median_days_to_sell"] == 92.5
    assert it["turnover_note"] == m.TURNOVER_DISCLAIMER
    conn.close()


# ---- FX binding fix -----------------------------------------------------------

def test_fx_reads_ref_exchange_rates_not_ref_fx_rates(db):
    m = _load()
    c = duckdb.connect(str(db))
    c.execute("""INSERT INTO ref_exchange_rates (from_currency, to_currency, rate, valid_date, source)
                 VALUES ('USD','EUR', 0.92, DATE '2025-06-01', 'ECB')""")
    c.close()
    conn = _ro(db)
    m.assert_db_target(conn, db)
    assert m.latest_usd_eur_rate(conn) == 0.92     # correct table/column
    # the table B queried does not exist -> proves we bind to the right one
    with pytest.raises(Exception):
        conn.execute("SELECT 1 FROM ref_fx_rates")
    conn.close()


# ---- market + freshness -------------------------------------------------------

def test_market_summary_and_freshness(db):
    m = _load()
    c = duckdb.connect(str(db))
    c.execute("INSERT INTO stg_active_targeted (id, marketplace) VALUES (1,'EBAY_DE'),(2,'EBAY_US')")
    c.execute("""INSERT INTO stg_historical_ebay_sold (id, currency_original, sold_date) VALUES
                 (1,'EUR', DATE '2025-06-01'),(2,'USD', DATE '2025-05-01')""")
    c.close()
    conn = _ro(db)
    m.assert_db_target(conn, db)
    ms = m.market_summary(conn)
    assert ms["active_eu"] == 1 and ms["active_us"] == 1
    assert ms["sold_eur"] == 1 and ms["sold_usd"] == 1
    fr = m.data_freshness(conn)
    assert str(fr["latest_sold_date"]) == "2025-06-01"
    conn.close()


# ---- turnover safety ----------------------------------------------------------

def test_turnover_disclaimer_is_velocity_not_elasticity():
    m = _load()
    d = m.TURNOVER_DISCLAIMER.lower()
    assert "velocity" in d
    assert "not a price elasticity model" in d
    assert "price response" in d
