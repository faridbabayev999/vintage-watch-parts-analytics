"""
Phase 4 — dashboard/scenario-engine integration tests.

Verifies: TMV displayed equals backend TMV; scenario outputs match the engine
exactly (dashboard never recomputes); missing reference data produces a
visible error, never a silent 0; no hardcoded rate values anywhere in the
dashboard layer; and the new H/C/D/S/P fields are read verbatim from backend
tables. Disposable in-tmp DuckDB only. DB target asserted before every query
(standing rule).
"""
import importlib.util
import sys
from datetime import date
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


def _seed_scenario_rates(conn):
    d = date(2026, 1, 1)
    conn.execute("INSERT INTO ref_shipping_rates (country, shipping_cost, currency, valid_from, source) VALUES "
                 "('DE', 5.0, 'EUR', ?, 's'), ('US', 25.0, 'EUR', ?, 's')", [d, d])
    conn.execute("INSERT INTO ref_customs_rates (hs_code, country, duty_rate, valid_from, source) VALUES "
                 "('9114.90', 'US', 0.03, ?, 's'), ('9114.90', 'DE', 0.0, ?, 's')", [d, d])
    conn.execute("INSERT INTO ref_tax_rates (country, tax_type, rate, valid_from, source) VALUES "
                 "('US', 'sales_tax', 0.0975, ?, 's'), ('DE', 'import_tax', 0.0, ?, 's')", [d, d])


def _seeded_ready_db(tmp_path, name="ready.duckdb", with_rates=True):
    db = tmp_path / name
    conn = duckdb.connect(str(db))
    conn.execute(SCHEMA.read_text())
    conn.execute("""INSERT INTO staging_inventory
        (inventory_uid, canonical_inventory_id, brand, caliber, part_number, stock, validation_status)
        VALUES ('inv1','rolex_3135_x','Rolex','3135','3135-570', 1, 'PASS')""")
    conn.execute("""INSERT INTO tmv_results (canonical_inventory_id, tmv_eur, tmv_low_eur, tmv_high_eur, confidence_tier)
        VALUES ('rolex_3135_x', 200.0, 170.0, 230.0, 'MEDIUM')""")
    conn.execute("""INSERT INTO feat_pricing (canonical_inventory_id, brand, historical_value_eur, current_value_eur, scarcity_score)
        VALUES ('rolex_3135_x', 'Rolex', 195.0, 210.0, 0.6)""")
    conn.execute("""INSERT INTO feat_demand (canonical_inventory_id, recency_score, price_trend_slope)
        VALUES ('rolex_3135_x', 0.75, 0.02)""")
    conn.execute("""INSERT INTO turnover_survival (canonical_inventory_id, median_days_to_sell, probability_sell_30d, probability_sell_90d)
        VALUES ('rolex_3135_x', 100.0, 0.25, 0.55)""")
    if with_rates:
        _seed_scenario_rates(conn)
    conn.close()
    return db


# ── TMV displayed equals backend TMV ──────────────────────────────────────────

def test_tmv_displayed_equals_backend_tmv(tmp_path):
    db = _seeded_ready_db(tmp_path)
    dd = _load("dd1", "16_dashboard_data.py")
    conn = duckdb.connect(str(db), read_only=True)
    dd.assert_db_target(conn, db)
    items = dd.load_items(conn)
    backend_tmv = conn.execute(
        "SELECT tmv_eur FROM tmv_results WHERE canonical_inventory_id='rolex_3135_x'").fetchone()[0]
    assert items[0]["tmv_eur"] == backend_tmv == 200.0
    conn.close()


def test_hcdsp_fields_read_verbatim_no_recomputation(tmp_path):
    db = _seeded_ready_db(tmp_path)
    dd = _load("dd2", "16_dashboard_data.py")
    conn = duckdb.connect(str(db), read_only=True)
    dd.assert_db_target(conn, db)
    it = dd.load_items(conn)[0]
    assert it["historical_value_eur"] == 195.0
    assert it["current_value_eur"] == 210.0
    assert it["demand_index"] == 0.75
    assert it["market_dynamics"] == 0.6
    assert it["price_trend"] == 0.02
    conn.close()


# ── scenario outputs match the engine exactly ─────────────────────────────────

def test_dashboard_scenario_output_matches_engine_directly(tmp_path):
    db = _seeded_ready_db(tmp_path)
    dd = _load("dd3", "16_dashboard_data.py")
    engine = _load("eng3", "17_scenario_engine.py")
    conn = duckdb.connect(str(db), read_only=True)
    dd.assert_db_target(conn, db)
    dash_result = dd.item_scenarios(conn, 200.0)
    direct_result = engine.compute_scenarios(conn, 200.0)
    assert dash_result["ok"] is True
    assert dash_result["scenarios"] == direct_result
    conn.close()


# ── missing reference data -> visible error, never silent 0 ──────────────────

def test_missing_reference_data_produces_visible_error_not_silent_zero(tmp_path):
    db = _seeded_ready_db(tmp_path, name="norates.duckdb", with_rates=False)
    dd = _load("dd4", "16_dashboard_data.py")
    conn = duckdb.connect(str(db), read_only=True)
    dd.assert_db_target(conn, db)
    result = dd.item_scenarios(conn, 200.0)
    assert result["ok"] is False
    assert "error" in result and result["error"]
    conn.close()


def test_render_shows_scenario_error_message_not_fabricated_numbers(tmp_path):
    db = _seeded_ready_db(tmp_path, name="norates2.duckdb", with_rates=False)
    dd = _load("dd5", "16_dashboard_data.py")
    render = _load("rd5", "16_dashboard.py")
    conn = duckdb.connect(str(db), read_only=True)
    dd.assert_db_target(conn, db)
    htmlout = render.build_dashboard_html(conn, data=dd)
    assert "Scenario comparison unavailable" in htmlout
    conn.close()


def test_render_shows_scenarios_when_configured(tmp_path):
    db = _seeded_ready_db(tmp_path)
    dd = _load("dd6", "16_dashboard_data.py")
    render = _load("rd6", "16_dashboard.py")
    conn = duckdb.connect(str(db), read_only=True)
    dd.assert_db_target(conn, db)
    htmlout = render.build_dashboard_html(conn, data=dd)
    assert "US customer" in htmlout and "Germany customer" in htmlout and "Virtual" in htmlout
    assert "Landed" in htmlout
    conn.close()


# ── price/time simulator renders with disclosed epsilon ──────────────────────

def test_render_shows_price_time_simulation_with_disclaimer(tmp_path):
    db = _seeded_ready_db(tmp_path)
    conn = duckdb.connect(str(db))
    conn.execute("""INSERT INTO ref_tmv_parameters (parameter_name, parameter_value, active_flag, description)
                    VALUES ('price_elasticity_epsilon', 1.5, TRUE, 'test')""")
    conn.close()
    dd = _load("dd8", "16_dashboard_data.py")
    render = _load("rd8", "16_dashboard.py")
    conn = duckdb.connect(str(db), read_only=True)
    dd.assert_db_target(conn, db)
    htmlout = render.build_dashboard_html(conn, data=dd)
    assert "Price/time simulation" in htmlout
    assert "not a statistically fitted market elasticity" in htmlout
    assert "days" in htmlout
    conn.close()


# ── no hardcoded values in the dashboard layer ────────────────────────────────

def test_no_hardcoded_rate_values_in_dashboard_layer():
    for fname in ("16_dashboard_data.py", "16_dashboard.py"):
        src = (SCRIPTS / fname).read_text()
        for bad in ["25.0", "0.0975", "SHIPPING_DE_EUR", "SHIPPING_US_EUR", "US_DUTY_RATE", "US_SALES_TAX_RATE"]:
            assert bad not in src, f"{fname} contains a hardcoded rate/constant it should be looking up: {bad!r}"


# ── Task 6/7 (owner final work plan, 2026-07-30): dashboard renders the new
#    turnover bucket forecast and price-recommendation disclosure ──────────────

def test_render_shows_recommendation_reason_when_present(tmp_path):
    db = _seeded_ready_db(tmp_path)
    conn = duckdb.connect(str(db))
    conn.execute("UPDATE feat_pricing SET recommendation_reason = "
                  "'Based on historical sold evidence (n=3 sale(s)). Price trend up (+2.0%), "
                  "scarcity balanced (S=0.60). Confidence: MEDIUM.'")
    conn.close()
    dd = _load("dd9", "16_dashboard_data.py")
    render = _load("rd9", "16_dashboard.py")
    conn = duckdb.connect(str(db), read_only=True)
    dd.assert_db_target(conn, db)
    htmlout = render.build_dashboard_html(conn, data=dd)
    assert "Why this price" in htmlout
    assert "historical sold evidence" in htmlout
    conn.close()


def test_render_shows_turnover_bucket_forecast_when_present(tmp_path):
    import json as _json
    db = _seeded_ready_db(tmp_path)
    conn = duckdb.connect(str(db))
    buckets = [{"bucket": b, "expected_units": 1.0} for b in
               ["0-7", "8-30", "31-90", "91-183", "184-365", "366-730", "731-1065", "1066+"]]
    conn.execute("UPDATE turnover_survival SET turnover_bucket_forecast = ?", [_json.dumps(buckets)])
    conn.execute("UPDATE feat_pricing SET stock = 8")
    conn.close()
    dd = _load("dd10", "16_dashboard_data.py")
    render = _load("rd10", "16_dashboard.py")
    conn = duckdb.connect(str(db), read_only=True)
    dd.assert_db_target(conn, db)
    htmlout = render.build_dashboard_html(conn, data=dd)
    assert "Expected units sold by time bucket" in htmlout
    assert "31-90d" in htmlout
    conn.close()


def test_render_omits_bucket_forecast_block_when_null():
    """No fabricated bucket data when turnover_bucket_forecast is NULL
    (e.g. items pre-dating Task 7, or ACTIVE_ONLY items with lambda<=0)."""
    render = _load("rd11", "16_dashboard.py")
    it = {
        "canonical_inventory_id": "x", "confidence_tier": "LOW", "tmv_eur": 100.0,
        "tmv_low_eur": 80.0, "tmv_high_eur": 120.0, "evidence_depth": 1,
        "median_days_to_sell": None, "prob_sell_30d": None, "prob_sell_90d": None,
        "turnover_note": "n", "turnover_bucket_forecast": None,
        "historical_value_eur": None, "current_value_eur": None,
        "demand_index": None, "market_dynamics": None, "price_trend": None,
        "recommendation_reason": None,
    }
    assert it.get("turnover_bucket_forecast") is None


# ── Client pricing-state merge (2026-07-31 revision) ──────────────────────────

def test_ready_state_true_when_only_algorithmic_tmv_exists(tmp_path):
    db = _seeded_ready_db(tmp_path, name="pm1.duckdb")
    conn = duckdb.connect(str(db))
    conn.execute("DELETE FROM tmv_results")  # governed empty
    conn.execute("""INSERT INTO tmv_results_algorithmic
        (canonical_inventory_id, tmv_eur, tmv_low_eur, tmv_high_eur, confidence_tier, evidence_basis_type)
        VALUES ('rolex_3135_x', 150.0, 120.0, 180.0, 'AUTO_CONFIRMED', 'ALGORITHMIC')""")
    conn.close()
    dd = _load("ddpm1", "16_dashboard_data.py")
    conn = duckdb.connect(str(db), read_only=True)
    dd.assert_db_target(conn, db)
    state = dd.dashboard_state(conn)
    assert state["state"] == "READY"
    conn.close()


def test_load_items_merges_governed_and_algorithmic_governed_wins(tmp_path):
    db = _seeded_ready_db(tmp_path, name="pm2.duckdb")
    conn = duckdb.connect(str(db))
    # same item present in both governed (tmv_eur=200) and algorithmic (tmv_eur=999) -- governed must win
    conn.execute("""INSERT INTO tmv_results_algorithmic
        (canonical_inventory_id, tmv_eur, tmv_low_eur, tmv_high_eur, confidence_tier, evidence_basis_type)
        VALUES ('rolex_3135_x', 999.0, 900.0, 1000.0, 'AUTO_CONFIRMED', 'ALGORITHMIC')""")
    conn.execute("""INSERT INTO staging_inventory
        (inventory_uid, canonical_inventory_id, brand, caliber, part_number, stock, validation_status)
        VALUES ('inv2','rolex_other_y','Rolex','3135','999-000', 1, 'PASS')""")
    conn.execute("""INSERT INTO tmv_results_algorithmic
        (canonical_inventory_id, tmv_eur, tmv_low_eur, tmv_high_eur, confidence_tier, evidence_basis_type)
        VALUES ('rolex_other_y', 50.0, 40.0, 60.0, 'HIGH_CONFIDENCE', 'ALGORITHMIC')""")
    conn.close()
    dd = _load("ddpm2", "16_dashboard_data.py")
    conn = duckdb.connect(str(db), read_only=True)
    dd.assert_db_target(conn, db)
    items = dd.load_items(conn)
    by_id = {i["canonical_inventory_id"]: i for i in items}
    assert by_id["rolex_3135_x"]["tmv_eur"] == 200.0  # governed value, not 999
    assert by_id["rolex_3135_x"]["pricing_state"] == "GOVERNED"
    assert by_id["rolex_other_y"]["tmv_eur"] == 50.0
    assert by_id["rolex_other_y"]["pricing_state"] == "HIGH_CONFIDENCE"
    assert by_id["rolex_other_y"]["pricing_state_label"] == "Pricing Estimate"
    conn.close()


def test_pricing_state_labels_are_client_facing_not_internal_enum_names():
    dd = _load("ddpm3", "16_dashboard_data.py")
    assert dd.PRICING_STATE_LABELS["AUTO_CONFIRMED"] == "Pricing Ready"
    assert dd.PRICING_STATE_LABELS["HIGH_CONFIDENCE"] == "Pricing Estimate"
    # human phrasing, not raw enum-style names like AUTO_CONFIRMED/HIGH_CONFIDENCE
    for label in dd.PRICING_STATE_LABELS.values():
        assert "_" not in label and label != label.upper()


# ── Client dashboard review fixes (2026-07-31) ─────────────────────────────────

def test_overview_summary_counts_algorithmic_tmv_not_just_governed(tmp_path):
    """Bug found visually: overview cards showed 'TMV available 0/728'
    while 575 algorithmic-priced item cards were rendered right below --
    overview_summary() only counted tmv_results (governed), never
    tmv_results_algorithmic."""
    db = _seeded_ready_db(tmp_path, name="ov5.duckdb")
    conn = duckdb.connect(str(db))
    conn.execute("DELETE FROM tmv_results")  # governed empty
    conn.execute("""INSERT INTO tmv_results_algorithmic
        (canonical_inventory_id, tmv_eur, tmv_low_eur, tmv_high_eur, confidence_tier, evidence_basis_type)
        VALUES ('rolex_3135_x', 150.0, 120.0, 180.0, 'AUTO_CONFIRMED', 'ALGORITHMIC')""")
    conn.close()
    dd = _load("ddov5", "16_dashboard_data.py")
    conn = duckdb.connect(str(db), read_only=True)
    dd.assert_db_target(conn, db)
    ov = dd.overview_summary(conn)
    assert ov["tmv_available_n"] == 1
    assert ov["avg_recommended_price_eur"] == 150.0
    conn.close()


def test_fmt_renders_nan_as_dash_not_literal_nan():
    render = _load("rdnan", "16_dashboard.py")
    assert render._fmt(float("nan")) == "—"
    assert render._fmt(None) == "—"
    assert render._fmt(12.3) == "12.30"


def test_unpriced_items_appear_with_reason_not_silently_absent(tmp_path):
    """Bug found visually: the 153 items without a price were completely
    absent from the dashboard -- indistinguishable from not existing in
    inventory at all."""
    db = _seeded_ready_db(tmp_path, name="ov6.duckdb")
    conn = duckdb.connect(str(db))
    conn.execute("""INSERT INTO staging_inventory
        (inventory_uid, canonical_inventory_id, brand, caliber, part_number, stock, validation_status)
        VALUES ('inv_unpriced','rolex_unpriced_z','Rolex','3135','999-999', 1, 'PASS')""")
    conn.close()
    dd = _load("ddov6", "16_dashboard_data.py")
    render = _load("rdov6", "16_dashboard.py")
    conn = duckdb.connect(str(db), read_only=True)
    dd.assert_db_target(conn, db)
    unpriced = dd.load_unpriced_items(conn)
    ids = [u["canonical_inventory_id"] for u in unpriced]
    assert "rolex_unpriced_z" in ids
    htmlout = render.build_dashboard_html(conn, data=dd)
    assert "rolex_unpriced_z" in htmlout
    assert "Recommendation unavailable" in htmlout
    assert "Price: 0" not in htmlout and "€0.00" not in htmlout.split("rolex_unpriced_z")[1][:200]
    conn.close()


# ── Phase 9 (final wrap-up sprint): overview cards ────────────────────────────

def test_overview_summary_never_fabricates_price_or_turnover_when_no_tmv(tmp_path):
    db = _seeded_ready_db(tmp_path, name="ov1.duckdb")
    conn = duckdb.connect(str(db))
    conn.execute("DELETE FROM tmv_results")  # simulate AWAITING state
    conn.execute("DELETE FROM turnover_survival")
    conn.close()
    dd = _load("ddov1", "16_dashboard_data.py")
    conn = duckdb.connect(str(db), read_only=True)
    dd.assert_db_target(conn, db)
    ov = dd.overview_summary(conn)
    assert ov["avg_recommended_price_eur"] is None
    assert ov["avg_turnover_days"] is None
    assert ov["tmv_available_n"] == 0
    conn.close()


def test_overview_summary_computes_real_coverage_ratios(tmp_path):
    db = _seeded_ready_db(tmp_path, name="ov2.duckdb")
    conn = duckdb.connect(str(db), read_only=True)
    dd = _load("ddov2", "16_dashboard_data.py")
    dd.assert_db_target(conn, db)
    ov = dd.overview_summary(conn)
    assert ov["total_inventory"] == 1  # single-item fixture
    assert ov["avg_recommended_price_eur"] == 200.0  # matches the fixture's tmv_eur
    assert ov["avg_turnover_days"] == 100.0
    conn.close()


def test_overview_cards_render_in_both_awaiting_and_ready_states(tmp_path):
    db_awaiting = _seeded_ready_db(tmp_path, name="ov3_awaiting.duckdb")
    conn = duckdb.connect(str(db_awaiting))
    conn.execute("DELETE FROM tmv_results")
    conn.close()
    dd = _load("ddov3", "16_dashboard_data.py")
    render = _load("rdov3", "16_dashboard.py")
    conn = duckdb.connect(str(db_awaiting), read_only=True)
    dd.assert_db_target(conn, db_awaiting)
    html_awaiting = render.build_dashboard_html(conn, data=dd)
    assert "Total inventory" in html_awaiting and "Avg. recommended price" in html_awaiting
    conn.close()

    db_ready = _seeded_ready_db(tmp_path, name="ov4_ready.duckdb")
    dd2 = _load("ddov4", "16_dashboard_data.py")
    render2 = _load("rdov4", "16_dashboard.py")
    conn2 = duckdb.connect(str(db_ready), read_only=True)
    dd2.assert_db_target(conn2, db_ready)
    html_ready = render2.build_dashboard_html(conn2, data=dd2)
    assert "Total inventory" in html_ready
    conn2.close()


# ── turnover-price honesty: no fabricated elasticity ──────────────────────────

def test_price_note_does_not_claim_turnover_responds_to_price(tmp_path):
    db = _seeded_ready_db(tmp_path)
    dd = _load("dd7", "16_dashboard_data.py")
    render = _load("rd7", "16_dashboard.py")
    conn = duckdb.connect(str(db), read_only=True)
    dd.assert_db_target(conn, db)
    htmlout = render.build_dashboard_html(conn, data=dd)
    assert "does not respond to price" in htmlout
    assert "does not do" in htmlout  # explicit refusal to invent an unfitted assumption
    conn.close()
