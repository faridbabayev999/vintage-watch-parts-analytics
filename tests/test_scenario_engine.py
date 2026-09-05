"""
Tests for scripts/17_scenario_engine.py (Module 6 -- TMV-level scenario engine).

Verifies: reproducibility (same TMV + same reference data = same output),
reference-table lookup (ASOF by valid_from), missing-configuration handling
(raises, never silently defaults to 0), correct US/DE/Virtual calculations,
and no hidden constants (every non-zero rate traces to a ref_* row with a
source citation).
"""
import importlib.util
import sys
from datetime import date
from pathlib import Path

import duckdb
import pytest

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
SCHEMA = SCRIPTS / "schema.sql"


def _load():
    spec = importlib.util.spec_from_file_location("scenario17", SCRIPTS / "17_scenario_engine.py")
    m = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(SCRIPTS))
    spec.loader.exec_module(m)
    return m


def _seeded_db(tmp_path):
    db = tmp_path / "scen.duckdb"
    conn = duckdb.connect(str(db))
    conn.execute(SCHEMA.read_text())
    seed_date = date(2026, 1, 1)
    conn.execute("INSERT INTO ref_shipping_rates (country, shipping_cost, currency, valid_from, source) VALUES "
                 "('DE', 5.0, 'EUR', ?, 'test seed DE'), ('US', 25.0, 'EUR', ?, 'test seed US')", [seed_date, seed_date])
    conn.execute("INSERT INTO ref_customs_rates (hs_code, country, duty_rate, valid_from, source) VALUES "
                 "('9114.90', 'US', 0.03, ?, 'test seed US customs'), ('9114.90', 'DE', 0.0, ?, 'test seed DE customs (domestic)')",
                 [seed_date, seed_date])
    conn.execute("INSERT INTO ref_tax_rates (country, tax_type, rate, valid_from, source) VALUES "
                 "('US', 'sales_tax', 0.0975, ?, 'test seed US tax'), ('DE', 'import_tax', 0.0, ?, 'test seed DE tax (domestic)')",
                 [seed_date, seed_date])
    return conn


# ── correct calculations, hand-verified ────────────────────────────────────────

def test_scenarios_hand_computed(tmp_path):
    m = _load()
    conn = _seeded_db(tmp_path)
    result = m.compute_scenarios(conn, tmv_eur=200.0, as_of=date(2026, 6, 1))

    # C: virtual -- price only
    assert result["C"]["landed_cost_eur"] == 200.0
    assert result["C"]["shipping_eur"] == 0.0 and result["C"]["customs_eur"] == 0.0 and result["C"]["tax_eur"] == 0.0

    # B: Germany -- price + shipping, no customs/tax (both seeded 0)
    assert result["B"]["shipping_eur"] == 5.0
    assert result["B"]["customs_eur"] == 0.0
    assert result["B"]["tax_eur"] == 0.0
    assert result["B"]["landed_cost_eur"] == pytest.approx(205.0)

    # A: US -- price + shipping + customs(on price+ship) + tax(on taxable subtotal)
    price, ship = 200.0, 25.0
    customs = round((price + ship) * 0.03, 2)  # duty 3%
    taxable = price + ship + customs
    tax = round(taxable * 0.0975, 2)
    expected_landed = round(price + ship + customs + tax, 2)
    assert result["A"]["customs_eur"] == customs
    assert result["A"]["tax_eur"] == tax
    assert result["A"]["landed_cost_eur"] == expected_landed
    conn.close()


# ── reproducibility ──────────────────────────────────────────────────────────────

def test_same_input_same_reference_data_same_output(tmp_path):
    m = _load()
    conn = _seeded_db(tmp_path)
    r1 = m.compute_scenarios(conn, tmv_eur=180.0, as_of=date(2026, 6, 1))
    r2 = m.compute_scenarios(conn, tmv_eur=180.0, as_of=date(2026, 6, 1))
    assert r1 == r2
    conn.close()


# ── reference-table ASOF lookup ───────────────────────────────────────────────────

def test_lookup_uses_latest_rate_not_after_as_of_date(tmp_path):
    """A rate change effective in the future must not apply to an earlier as_of."""
    m = _load()
    conn = _seeded_db(tmp_path)
    conn.execute("INSERT INTO ref_shipping_rates (country, shipping_cost, currency, valid_from, source) "
                 "VALUES ('DE', 8.0, 'EUR', ?, 'future rate change')", [date(2027, 1, 1)])
    # as_of before the future change -> old rate (5.0)
    old = m.lookup_shipping(conn, "DE", as_of=date(2026, 6, 1))
    assert old["amount_eur"] == 5.0
    # as_of after the future change -> new rate (8.0)
    new = m.lookup_shipping(conn, "DE", as_of=date(2027, 6, 1))
    assert new["amount_eur"] == 8.0
    conn.close()


def test_lookup_returns_source_citation(tmp_path):
    m = _load()
    conn = _seeded_db(tmp_path)
    ship = m.lookup_shipping(conn, "US", as_of=date(2026, 6, 1))
    assert ship["source"] == "test seed US"
    conn.close()


# ── missing configuration handling ────────────────────────────────────────────────

def test_missing_shipping_rate_raises_not_silently_zero(tmp_path):
    m = _load()
    db = tmp_path / "empty.duckdb"
    conn = duckdb.connect(str(db))
    conn.execute(SCHEMA.read_text())  # no seed data at all
    with pytest.raises(m.ConfigurationError):
        m.lookup_shipping(conn, "US", as_of=date(2026, 6, 1))
    conn.close()


def test_compute_scenarios_raises_if_us_config_missing(tmp_path):
    m = _load()
    db = tmp_path / "partial.duckdb"
    conn = duckdb.connect(str(db))
    conn.execute(SCHEMA.read_text())
    conn.execute("INSERT INTO ref_shipping_rates (country, shipping_cost, currency, valid_from, source) "
                 "VALUES ('DE', 5.0, 'EUR', ?, 'seed')", [date(2026, 1, 1)])
    # DE shipping present, but customs/tax and all of US missing -> must raise, not default to 0
    with pytest.raises(m.ConfigurationError):
        m.compute_scenarios(conn, tmv_eur=100.0, as_of=date(2026, 6, 1))
    conn.close()


# ── no hidden constants ────────────────────────────────────────────────────────────

def test_no_hardcoded_rate_values_in_engine_source():
    """The engine module itself must contain no bare shipping/duty/tax number
    -- every rate must come from a ref_* table lookup."""
    src = (SCRIPTS / "17_scenario_engine.py").read_text()
    for bad in ["25.0", "= 5.0", "0.0975", "0.03,", "SHIPPING_DE_EUR", "SHIPPING_US_EUR", "US_DUTY_RATE", "US_SALES_TAX_RATE"]:
        assert bad not in src, f"engine source contains a value/constant it should be looking up: {bad!r}"


# ── price/time simulator (owner decision 2026-07-30) ──────────────────────────

def _seeded_db_with_epsilon(tmp_path, epsilon=1.5, active=True, name="sim.duckdb"):
    db = tmp_path / name
    conn = duckdb.connect(str(db))
    conn.execute(SCHEMA.read_text())
    conn.execute("""INSERT INTO ref_tmv_parameters (parameter_name, parameter_value, active_flag, description)
                    VALUES ('price_elasticity_epsilon', ?, ?, 'test')""", [epsilon, active])
    return conn


def test_zero_price_change_returns_baseline_days(tmp_path):
    m = _load()
    conn = _seeded_db_with_epsilon(tmp_path)
    r = m.simulate_price_time(conn, tmv_eur=100.0, base_days=120.0, scenario_price_eur=100.0)
    assert r["simulated_days"] == 120.0
    assert r["price_change_pct"] == 0.0
    conn.close()


def test_higher_price_increases_simulated_days(tmp_path):
    m = _load()
    conn = _seeded_db_with_epsilon(tmp_path)
    r = m.simulate_price_time(conn, tmv_eur=100.0, base_days=120.0, scenario_price_eur=120.0)
    assert r["simulated_days"] > 120.0
    assert r["price_change_pct"] == 20.0
    conn.close()


def test_lower_price_decreases_simulated_days(tmp_path):
    m = _load()
    conn = _seeded_db_with_epsilon(tmp_path)
    r = m.simulate_price_time(conn, tmv_eur=100.0, base_days=120.0, scenario_price_eur=80.0)
    assert r["simulated_days"] < 120.0
    assert r["price_change_pct"] == -20.0
    conn.close()


def test_simulated_days_matches_hand_computed_formula(tmp_path):
    m = _load()
    conn = _seeded_db_with_epsilon(tmp_path, epsilon=1.5)
    r = m.simulate_price_time(conn, tmv_eur=100.0, base_days=120.0, scenario_price_eur=120.0)
    expected = round(120.0 * (120.0 / 100.0) ** 1.5, 1)
    assert r["simulated_days"] == expected
    conn.close()


def test_changing_epsilon_changes_only_scenario_output(tmp_path):
    """Different epsilon values must change simulate_price_time's output but
    must NOT be readable by / affect 13_build_tmv.py's TMV formula at all --
    verified by grep: the TMV module never queries price_elasticity_epsilon."""
    m = _load()
    conn1 = _seeded_db_with_epsilon(tmp_path, epsilon=1.0, name="sim_eps1.duckdb")
    r1 = m.simulate_price_time(conn1, tmv_eur=100.0, base_days=120.0, scenario_price_eur=150.0)
    conn1.close()
    conn2 = _seeded_db_with_epsilon(tmp_path, epsilon=2.0, name="sim_eps2.duckdb")
    r2 = m.simulate_price_time(conn2, tmv_eur=100.0, base_days=120.0, scenario_price_eur=150.0)
    conn2.close()
    assert r1["simulated_days"] != r2["simulated_days"]
    tmv_src = (SCRIPTS / "13_build_tmv.py").read_text()
    assert "price_elasticity_epsilon" not in tmv_src


def test_tmv_baseline_unaffected_by_simulator(tmp_path):
    """The simulator must not write anything back to tmv_results or mutate
    the TMV calculation -- it is purely a read + compute function."""
    import importlib.util as _ilu
    spec = _ilu.spec_from_file_location("tmv13_sim", SCRIPTS / "13_build_tmv.py")
    tmv13 = _ilu.module_from_spec(spec)
    sys.path.insert(0, str(SCRIPTS))
    spec.loader.exec_module(tmv13)
    conn = _seeded_db_with_epsilon(tmp_path)
    conn.execute("""INSERT INTO staging_inventory (inventory_uid, canonical_inventory_id, brand, caliber, part_number, stock, validation_status)
                    VALUES ('inv1','c1','Rolex','3135','pn',1,'PASS')""")
    conn.execute("""INSERT INTO stg_historical_ebay_sold (id, stable_evidence_uid, price_eur, sold_date, has_best_offer_option)
                    VALUES (1,'ev1',180.0, DATE '2025-06-01', FALSE)""")
    conn.execute("""INSERT INTO match_decisions (decision_id, decision_version, decision_run_id, candidate_key,
        inventory_uid, source_table, source_id, matching_rule, evidence_tier, match_status, match_reason_code,
        collection_relationship, price_evidence_status, evidence_uid) VALUES
        (1,'1','run1','ck1','inv1','match_candidates_ebay_sold',1,'CALIBER_PART_NUMBER','A','MATCH_CONFIRMED','OK','NONE','NOT_APPLICABLE','ev1')""")
    m = _load()
    res = tmv13.build(conn)
    tmv13.write(conn, res["df"])
    before = conn.execute("SELECT tmv_eur FROM tmv_results WHERE canonical_inventory_id='c1'").fetchone()[0]
    m.simulate_price_time(conn, tmv_eur=before, base_days=100.0, scenario_price_eur=200.0)  # simulator call
    after = conn.execute("SELECT tmv_eur FROM tmv_results WHERE canonical_inventory_id='c1'").fetchone()[0]
    assert before == after
    conn.close()


def test_every_nonzero_component_has_a_source_citation(tmp_path):
    m = _load()
    conn = _seeded_db(tmp_path)
    result = m.compute_scenarios(conn, tmv_eur=150.0, as_of=date(2026, 6, 1))
    for key in ("A", "B"):
        sc = result[key]
        assert sc["sources"]["shipping"], f"{key} shipping has no source citation"
        assert sc["sources"]["customs"] is not None
        assert sc["sources"]["tax"] is not None
    conn.close()
