"""
Module 5 — dashboard render tests. Every test asserts its DB target before
querying (standing rule). Verifies empty-state ('Awaiting validated evidence',
no numbers) and the ready-state card render from backend values, plus turnover
safety (velocity language, never price sensitivity).
"""
import importlib.util
import sys
from pathlib import Path

import duckdb
import pytest

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
SCHEMA = SCRIPTS / "schema.sql"


def _load(name, fname):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / fname)
    m = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(SCRIPTS))
    spec.loader.exec_module(m)
    return m


@pytest.fixture
def db(tmp_path):
    p = tmp_path / "dash.duckdb"
    duckdb.connect(str(p)).execute(SCHEMA.read_text())
    return p


def test_empty_state_render_shows_awaiting_no_numbers(db):
    data = _load("dash16_data_r1", "16_dashboard_data.py")
    render = _load("dash16_r1", "16_dashboard.py")
    conn = duckdb.connect(str(db), read_only=True)
    data.assert_db_target(conn, db)                       # RULE: target first
    htmlout = render.build_dashboard_html(conn, data=data)
    assert "Awaiting validated evidence" in htmlout
    assert "€" not in htmlout                             # no fabricated prices
    assert 'class="wp-card"' not in htmlout               # no rendered item cards (CSS def excepted)
    conn.close()


def test_ready_state_render_shows_backend_values(db):
    c = duckdb.connect(str(db))
    c.execute("""INSERT INTO staging_inventory (inventory_uid, canonical_inventory_id, brand, validation_status)
                 VALUES ('inv1','rolex_3135_x','Rolex','PASS')""")
    c.execute("""INSERT INTO tmv_results (canonical_inventory_id, tmv_eur, tmv_low_eur, tmv_high_eur, confidence_tier)
                 VALUES ('rolex_3135_x', 180.0, 153.0, 207.0, 'MEDIUM')""")
    c.execute("""INSERT INTO turnover_survival (canonical_inventory_id, median_days_to_sell, probability_sell_30d, probability_sell_90d)
                 VALUES ('rolex_3135_x', 92.5, 0.20, 0.49)""")
    c.execute("""INSERT INTO feat_pricing (canonical_inventory_id, brand) VALUES ('rolex_3135_x','Rolex')""")
    c.close()
    data = _load("dash16_data_r2", "16_dashboard_data.py")
    render = _load("dash16_r2", "16_dashboard.py")
    conn = duckdb.connect(str(db), read_only=True)
    data.assert_db_target(conn, db)
    htmlout = render.build_dashboard_html(conn, data=data)
    assert "rolex_3135_x" in htmlout
    assert "180.00" in htmlout and "MEDIUM" in htmlout
    assert "Awaiting validated evidence" not in htmlout
    # turnover framed as velocity, never elasticity/price sensitivity
    assert "not a price elasticity model" in htmlout
    conn.close()


def test_render_deterministic(db):
    """Same DB → identical HTML across renders (deterministic output)."""
    data = _load("dash16_data_r3", "16_dashboard_data.py")
    render = _load("dash16_r3", "16_dashboard.py")
    conn = duckdb.connect(str(db), read_only=True)
    data.assert_db_target(conn, db)
    a = render.build_dashboard_html(conn, data=data)
    b = render.build_dashboard_html(conn, data=data)
    assert a == b
    conn.close()
