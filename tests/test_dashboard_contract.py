"""
Dashboard contract tests.

These tests protect the client-facing contract from the audit failures found
in August 2026: inflated evidence counts, stale confidence wording, missing
S/D/P values, and displaying the internal 3650-day cap as a real forecast.
"""
import importlib.util
import sys
from pathlib import Path

import duckdb

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
SCHEMA = SCRIPTS / "schema.sql"


def _load(name, filename):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / filename)
    module = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(SCRIPTS))
    spec.loader.exec_module(module)
    return module


def _seed_contract_case(conn):
    conn.execute("""
        INSERT INTO ref_shipping_rates (country, shipping_cost, valid_from, source)
        VALUES
        ('DE', 5.0, DATE '2025-01-01', 'test'),
        ('US', 25.0, DATE '2025-01-01', 'test')
    """)
    conn.execute("""
        INSERT INTO ref_customs_rates (hs_code, country, duty_rate, valid_from, source)
        VALUES
        ('9114.90', 'DE', 0.0, DATE '2025-01-01', 'test'),
        ('9114.90', 'US', 0.03, DATE '2025-01-01', 'test')
    """)
    conn.execute("""
        INSERT INTO ref_tax_rates (country, tax_type, rate, valid_from, source)
        VALUES
        ('DE', 'import_tax', 0.0, DATE '2025-01-01', 'test'),
        ('US', 'sales_tax', 0.0975, DATE '2025-01-01', 'test')
    """)
    conn.execute("""INSERT INTO staging_inventory
        (inventory_uid, canonical_inventory_id, brand, caliber, part_number, stock, validation_status)
        VALUES
        ('inv1', 'rolex_1030_7004', 'Rolex', '1030', '7004', 2, 'PASS'),
        ('inv2', 'rolex_25_10', 'Rolex', '25', '10', 38, 'PASS'),
        ('inv_fail', 'rolex_24_unknown', 'Rolex', '24', NULL, 1, 'FAIL')
    """)
    conn.execute("""INSERT INTO tmv_results_algorithmic
        (canonical_inventory_id, tmv_eur, tmv_low_eur, tmv_high_eur, confidence_tier,
         evidence_basis_type, valuation_basis, historical_value_eur, current_value_eur,
         scarcity_score, price_trend, demand_index, recommendation_reason)
        VALUES
        ('rolex_1030_7004', 477.07, 286.24, 667.90, 'AUTO_CONFIRMED',
         'ALGORITHMIC', 'ACTIVE_ONLY', NULL, 463.50, 0.75, 0.0, 0.42,
         'Based on active asking prices only (n=2 listing(s)). Price trend flat (+0.0%), scarcity scarce (S=0.75). Confidence: LOW.')
    """)
    conn.execute("""INSERT INTO turnover_survival_algorithmic
        (canonical_inventory_id, median_days_to_sell, probability_sell_30d,
         probability_sell_90d, turnover_bucket_forecast)
        VALUES ('rolex_1030_7004', 3650.0, 0.0, 0.0,
                '[{"bucket":"0-7","expected_units":0.0},{"bucket":"8-30","expected_units":0.0},{"bucket":"31-90","expected_units":0.0},{"bucket":"91-183","expected_units":0.0},{"bucket":"184-365","expected_units":0.0},{"bucket":"366-730","expected_units":0.0},{"bucket":"731-1065","expected_units":0.0},{"bucket":"1066+","expected_units":0.0}]')
    """)
    # Two raw observations of the same physical listing; contract count must
    # follow evidence_uid, not raw staging row id.
    conn.execute("""INSERT INTO stg_active_targeted
        (id, inventory_uid, stable_evidence_uid, item_id, price_eur, fetched_at)
        VALUES
        (1, 'inv1', 'active_ev_1', 'v1|123|0', 500.0, '2026-07-01 10:00:00'),
        (2, 'inv1', 'active_ev_1', 'v1|123|0', 500.0, '2026-07-02 10:00:00')
    """)
    conn.execute("""INSERT INTO evidence_confidence_classification
        (classification_id, classification_run_id, candidate_key, inventory_uid,
         matching_rule, source_table, source_id, evidence_uid, v2_score,
         confidence_tier, tier_reason)
        VALUES
        (1, 'run1', 'ck1', 'inv1', 'PART_NUMBER_EXACT',
         'match_candidates_active', 1, 'active_ev_1', 1.0,
         'AUTO_CONFIRMED', 'test')
    """)


def test_dashboard_contract_one_row_per_eligible_item_and_clean_display(tmp_path):
    db = tmp_path / "contract.duckdb"
    conn = duckdb.connect(str(db))
    conn.execute(SCHEMA.read_text())
    _seed_contract_case(conn)

    builder = _load("contract_builder", "23_build_dashboard_contract.py")
    result = builder.build_and_write(conn)
    assert result["eligible"] == 2
    assert result["priced"] == 1

    rows = conn.execute("""
        SELECT COUNT(*) AS n, COUNT(DISTINCT inventory_uid) AS uid_n
        FROM dashboard_inventory_pricing
    """).fetchone()
    assert rows == (2, 2)

    priced = conn.execute("""
        SELECT scarcity_score_s, demand_index_d, price_trend_p,
               active_evidence_count, historical_evidence_count,
               unique_active_evidence_count, unique_historical_evidence_count,
               sell_time_display, turnover_evidence_status,
               recommendation_reason, pricing_confidence, confidence_label,
               confidence_score, validation_status, potential_revenue,
               potential_revenue_eur, germany_shipping_eur, us_shipping_eur,
               us_tax_eur, us_duty_eur, source_run_id, pricing_method,
               price_lower_bound_eur, price_upper_bound_eur,
               turnover_confidence, turnover_method
        FROM dashboard_inventory_pricing
        WHERE canonical_inventory_id='rolex_1030_7004'
    """).fetchone()
    assert priced[0] == 0.75
    assert priced[1] == 0.42
    assert priced[2] == 0.0
    assert priced[3] == 1
    assert priced[4] == 0
    assert priced[5] == 1
    assert priced[6] == 0
    assert priced[7] == "Sell-time estimate unavailable - insufficient sold evidence"
    assert priced[8] == "ACTIVE_ONLY_NO_VELOCITY"
    assert "Confidence:" not in priced[9]
    assert priced[10] == "LOW"
    assert priced[11] == "Low confidence"
    assert priced[12] == 1.0
    assert priced[13] == "PASS"
    assert priced[14] == priced[15] == 954.14
    assert priced[16] == 5.0
    assert priced[17] == 25.0
    assert priced[18] is not None
    assert priced[19] is not None
    assert priced[20] == "run1"
    assert priced[21] == "ACTIVE_ONLY_CALIBRATED"
    assert priced[22] == 286.24
    assert priced[23] == 667.90
    assert priced[24] == "LOW"
    assert priced[25] == "NO_SOLD_VELOCITY"

    unpriced = conn.execute("""
        SELECT no_recommendation_reason, turnover_method, sell_time_display
        FROM dashboard_inventory_pricing
        WHERE canonical_inventory_id='rolex_25_10'
    """).fetchone()
    assert unpriced[0] == "NO_CANDIDATES"
    assert unpriced[1] == "NO_PRICE_RECOMMENDATION"
    assert unpriced[2] == "Sell-time estimate unavailable - no price recommendation"
    conn.close()


def test_dashboard_data_prefers_contract_when_available(tmp_path):
    db = tmp_path / "contract_dash.duckdb"
    conn = duckdb.connect(str(db))
    conn.execute(SCHEMA.read_text())
    _seed_contract_case(conn)
    _load("contract_builder2", "23_build_dashboard_contract.py").build_and_write(conn)
    conn.close()

    dd = _load("dash_contract", "16_dashboard_data.py")
    ro = duckdb.connect(str(db), read_only=True)
    dd.assert_db_target(ro, db)
    items = dd.load_items(ro)
    assert len(items) == 1
    item = items[0]
    assert item["market_evidence_active"] == 1
    assert item["market_evidence_sold"] == 0
    assert item["sell_time_display"] == "Sell-time estimate unavailable - insufficient sold evidence"
    assert item["market_dynamics"] == 0.75
    assert item["pricing_state_label"] == "Low confidence"
    assert item["turnover_confidence"] == "LOW"

    ov = dd.overview_summary(ro)
    assert ov["total_inventory"] == 2
    assert ov["tmv_available_n"] == 1
    assert ov["avg_turnover_days"] is None
    ro.close()


def test_stable_active_only_direct_evidence_can_be_medium_pricing(tmp_path):
    db = tmp_path / "active_medium.duckdb"
    conn = duckdb.connect(str(db))
    conn.execute(SCHEMA.read_text())
    _seed_contract_case(conn)
    conn.execute("""INSERT INTO staging_inventory
        (inventory_uid, canonical_inventory_id, brand, caliber, part_number, stock, validation_status)
        VALUES ('inv3', 'rolex_3135_240', 'Rolex', '3135', '240', 1, 'PASS')
    """)
    conn.execute("""INSERT INTO tmv_results_algorithmic
        (canonical_inventory_id, tmv_eur, tmv_low_eur, tmv_high_eur, confidence_tier,
         evidence_basis_type, valuation_basis, historical_value_eur, current_value_eur,
         scarcity_score, price_trend, demand_index, recommendation_reason)
        VALUES
        ('rolex_3135_240', 50.0, 30.0, 70.0, 'AUTO_CONFIRMED',
         'ALGORITHMIC', 'ACTIVE_ONLY', NULL, 50.0, 0.5, 0.0, 0.5,
         'Based on active asking prices only (n=5 listing(s)).')
    """)
    conn.execute("""INSERT INTO turnover_survival_algorithmic
        (canonical_inventory_id, median_days_to_sell, probability_sell_30d,
         probability_sell_90d)
        VALUES ('rolex_3135_240', 3650.0, 0.0, 0.0)
    """)
    for i, price in enumerate([62.0, 63.0, 64.0, 65.0, 66.0], start=1):
        conn.execute("""INSERT INTO stg_active_targeted
            (id, inventory_uid, stable_evidence_uid, item_id, price_eur, fetched_at)
            VALUES (?, 'inv3', ?, ?, ?, TIMESTAMP '2026-07-02 10:00:00')
        """, [100 + i, f"active_ev_{i}", f"item-{i}", price])
        conn.execute("""INSERT INTO evidence_confidence_classification
            (classification_id, classification_run_id, candidate_key, inventory_uid,
             matching_rule, source_table, source_id, evidence_uid, v2_score,
             confidence_tier, tier_reason)
            VALUES (?, 'run2', ?, 'inv3', 'PART_NUMBER_EXACT',
             'match_candidates_active', ?, ?, 1.0, 'AUTO_CONFIRMED', 'test')
        """, [100 + i, f"ck-{i}", 100 + i, f"active_ev_{i}"])

    _load("contract_builder_medium", "23_build_dashboard_contract.py").build_and_write(conn)
    row = conn.execute("""
        SELECT pricing_confidence, pricing_method, active_price_median_eur,
               active_price_iqr_ratio, turnover_confidence, sell_time_display
        FROM dashboard_inventory_pricing
        WHERE inventory_uid='inv3'
    """).fetchone()
    assert row[0] == "MEDIUM"
    assert row[1] == "ACTIVE_ONLY_CALIBRATED"
    assert row[2] == 64.0
    assert row[3] < 0.1
    assert row[4] == "LOW"
    assert row[5] == "Sell-time estimate unavailable - insufficient sold evidence"
    conn.close()


def test_active_only_extreme_range_stays_low_even_when_iqr_is_calm(tmp_path):
    db = tmp_path / "active_outlier_low.duckdb"
    conn = duckdb.connect(str(db))
    conn.execute(SCHEMA.read_text())
    _seed_contract_case(conn)
    conn.execute("""INSERT INTO staging_inventory
        (inventory_uid, canonical_inventory_id, brand, caliber, part_number, stock, validation_status)
        VALUES ('inv4', 'rolex_3135_999', 'Rolex', '3135', '999', 1, 'PASS')
    """)
    conn.execute("""INSERT INTO tmv_results_algorithmic
        (canonical_inventory_id, tmv_eur, tmv_low_eur, tmv_high_eur, confidence_tier,
         evidence_basis_type, valuation_basis, historical_value_eur, current_value_eur,
         scarcity_score, price_trend, demand_index, recommendation_reason)
        VALUES
        ('rolex_3135_999', 45.0, 27.0, 63.0, 'AUTO_CONFIRMED',
         'ALGORITHMIC', 'ACTIVE_ONLY', NULL, 45.0, 0.5, 0.0, 0.5,
         'Based on active asking prices only.')
    """)
    for i, price in enumerate([48.0, 50.0, 55.0, 58.0, 300.0], start=1):
        conn.execute("""INSERT INTO stg_active_targeted
            (id, inventory_uid, stable_evidence_uid, item_id, price_eur, fetched_at)
            VALUES (?, 'inv4', ?, ?, ?, TIMESTAMP '2026-07-02 10:00:00')
        """, [300 + i, f"outlier_ev_{i}", f"outlier-item-{i}", price])
        conn.execute("""INSERT INTO evidence_confidence_classification
            (classification_id, classification_run_id, candidate_key, inventory_uid,
             matching_rule, source_table, source_id, evidence_uid, v2_score,
             confidence_tier, tier_reason)
            VALUES (?, 'run_outlier', ?, 'inv4', 'PART_NUMBER_EXACT',
             'match_candidates_active', ?, ?, 1.0, 'AUTO_CONFIRMED', 'test')
        """, [300 + i, f"outlier_ck-{i}", 300 + i, f"outlier_ev_{i}"])

    _load("contract_builder_outlier", "23_build_dashboard_contract.py").build_and_write(conn)
    row = conn.execute("""
        SELECT pricing_confidence, active_price_median_eur, active_price_range_ratio,
               active_outlier_count, condition_assumption,
               authenticity_assessment_status, active_pricing_caveat
        FROM dashboard_inventory_pricing
        WHERE inventory_uid='inv4'
    """).fetchone()
    assert row[0] == "LOW"
    assert row[1] == 55.0
    assert row[2] > 6.0
    assert row[3] >= 1
    assert row[4] == "LIKELY_UNIFORM_NEW_NOS_UNCONFIRMED"
    assert row[5] == "NOT_TAGGED_IN_MATCHING_LAYER"
    assert "condition" in row[6].lower()
    conn.close()


def test_comparable_cohort_turnover_rescues_active_only_item(tmp_path):
    db = tmp_path / "cohort_turnover.duckdb"
    conn = duckdb.connect(str(db))
    conn.execute(SCHEMA.read_text())
    _seed_contract_case(conn)
    conn.execute("""INSERT INTO staging_inventory
        (inventory_uid, canonical_inventory_id, brand, caliber, part_number, stock, validation_status)
        VALUES
        ('inv3', 'rolex_3135_240', 'Rolex', '3135', '240', 2, 'PASS'),
        ('peer1', 'rolex_3135_p1', 'Rolex', '3135', 'p1', 1, 'PASS'),
        ('peer2', 'rolex_3135_p2', 'Rolex', '3135', 'p2', 1, 'PASS'),
        ('peer3', 'rolex_3135_p3', 'Rolex', '3135', 'p3', 1, 'PASS')
    """)
    conn.execute("""INSERT INTO tmv_results_algorithmic
        (canonical_inventory_id, tmv_eur, tmv_low_eur, tmv_high_eur, confidence_tier,
         evidence_basis_type, valuation_basis, historical_value_eur, current_value_eur,
         scarcity_score, price_trend, demand_index, recommendation_reason)
        VALUES
        ('rolex_3135_240', 50.0, 30.0, 70.0, 'AUTO_CONFIRMED',
         'ALGORITHMIC', 'ACTIVE_ONLY', NULL, 50.0, 0.5, 0.0, 0.5,
         'Based on active asking prices only.')
    """)
    for idx, inv in enumerate(["peer1", "peer1", "peer2", "peer2", "peer3"], start=1):
        conn.execute("""INSERT INTO stg_historical_ebay_sold
            (id, title, sold_date, price_eur, stable_evidence_uid)
            VALUES (?, ?, DATE '2026-06-01' - (? * INTERVAL 10 DAY), 80.0, ?)
        """, [idx, f"Rolex 3135 peer {idx}", idx, f"hist_ev_{idx}"])
        conn.execute("""INSERT INTO evidence_confidence_classification
            (classification_id, classification_run_id, candidate_key, inventory_uid,
             matching_rule, source_table, source_id, evidence_uid, v2_score,
             confidence_tier, tier_reason)
            VALUES (?, 'run_hist', ?, ?, 'PART_NUMBER_EXACT',
             'match_candidates_ebay_sold', ?, ?, 1.0, 'AUTO_CONFIRMED', 'test')
        """, [200 + idx, f"hist_ck_{idx}", inv, idx, f"hist_ev_{idx}"])

    _load("contract_builder_cohort", "23_build_dashboard_contract.py").build_and_write(conn)
    row = conn.execute("""
        SELECT turnover_confidence, turnover_method, turnover_support_level,
               turnover_support_evidence_count, turnover_support_item_count,
               sell_time_display, median_days_to_sell
        FROM dashboard_inventory_pricing
        WHERE inventory_uid='inv3'
    """).fetchone()
    assert row[0] == "MEDIUM"
    assert row[1] == "COMPARABLE_COHORT_HAZARD_FIT"
    assert row[2] == "BRAND_CALIBER"
    assert row[3] == 5
    assert row[4] == 3
    assert row[5] != "Sell-time estimate unavailable - insufficient sold evidence"
    assert row[6] is not None
    conn.close()


def test_single_noisy_historical_observation_can_be_low_pricing(tmp_path):
    db = tmp_path / "thin_history.duckdb"
    conn = duckdb.connect(str(db))
    conn.execute(SCHEMA.read_text())
    _seed_contract_case(conn)
    conn.execute("""INSERT INTO staging_inventory
        (inventory_uid, canonical_inventory_id, brand, caliber, part_number, stock, validation_status)
        VALUES ('inv_hist', 'rolex_1570_999', 'Rolex', '1570', '999', 1, 'PASS')
    """)
    conn.execute("""INSERT INTO tmv_results_algorithmic
        (canonical_inventory_id, tmv_eur, tmv_low_eur, tmv_high_eur, confidence_tier,
         evidence_basis_type, valuation_basis, historical_value_eur, current_value_eur,
         scarcity_score, price_trend, demand_index, recommendation_reason)
        VALUES
        ('rolex_1570_999', 100.0, 60.0, 140.0, 'AUTO_CONFIRMED',
         'ALGORITHMIC', 'HISTORICAL', 100.0, NULL, 0.5, 0.0, 0.5,
         'Based on sold evidence.')
    """)
    conn.execute("""INSERT INTO stg_historical_ebay_sold
        (id, title, sold_date, price_eur, stable_evidence_uid)
        VALUES (50, 'Rolex 1570 999', DATE '2026-06-01', 100.0, 'hist_single')
    """)
    conn.execute("""INSERT INTO evidence_confidence_classification
        (classification_id, classification_run_id, candidate_key, inventory_uid,
         matching_rule, source_table, source_id, evidence_uid, v2_score,
         confidence_tier, tier_reason)
        VALUES (250, 'run_single', 'single_ck', 'inv_hist', 'PART_NUMBER_EXACT',
         'match_candidates_ebay_sold', 50, 'hist_single', 1.0, 'AUTO_CONFIRMED', 'test')
    """)

    _load("contract_builder_thin", "23_build_dashboard_contract.py").build_and_write(conn)
    row = conn.execute("""
        SELECT pricing_confidence, confidence_reason
        FROM dashboard_inventory_pricing
        WHERE inventory_uid='inv_hist'
    """).fetchone()
    assert row[0] == "LOW"
    assert "thin" in row[1].lower() or "dispersed" in row[1].lower()
    conn.close()
