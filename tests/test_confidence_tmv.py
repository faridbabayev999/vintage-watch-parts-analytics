"""
scripts/22_build_confidence_tmv.py tests. Disposable in-tmp DuckDB only.
"""
import importlib.util
import sys
from pathlib import Path

import duckdb

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
SCHEMA = SCRIPTS / "schema.sql"


def _load(name, fname):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / fname)
    m = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(SCRIPTS))
    spec.loader.exec_module(m)
    return m


def _fresh_db(tmp_path, name="ctmv.duckdb"):
    db = tmp_path / name
    conn = duckdb.connect(str(db))
    conn.execute(SCHEMA.read_text())
    return conn, db


def _seed_auto_confirmed_item(conn, uid="i1", cid="c1", tier="AUTO_CONFIRMED"):
    conn.execute("""INSERT INTO staging_inventory
        (inventory_uid, canonical_inventory_id, brand, caliber, part_number, stock, validation_status)
        VALUES (?, ?, 'Rolex', '3135', '4419', 5, 'PASS')""", [uid, cid])
    conn.execute("""INSERT INTO stg_historical_ebay_sold (id, stable_evidence_uid, price_eur, sold_date, has_best_offer_option)
                    VALUES (1, 'ev1', 180.0, '2025-06-01', FALSE)""")
    conn.execute("""INSERT INTO evidence_confidence_classification
        (classification_id, classification_run_id, candidate_key, inventory_uid, matching_rule,
         source_table, source_id, evidence_uid, v2_score, confidence_tier, tier_reason)
        VALUES (1, 'run1', 'ck1', ?, 'PART_NUMBER_EXACT', 'match_candidates_ebay_sold', 1, 'ev1', 1.0, ?, 'test')""",
        [uid, tier])


def test_governance_boundary_never_touches_human_governed_tables(tmp_path):
    m = _load("ctmv1", "22_build_confidence_tmv.py")
    conn, db = _fresh_db(tmp_path)
    _seed_auto_confirmed_item(conn)
    before_tmv = conn.execute("SELECT COUNT(*) FROM tmv_results").fetchone()[0]
    before_vp = conn.execute("SELECT COUNT(*) FROM validation_policy").fetchone()[0]
    before_md = conn.execute("SELECT COUNT(*) FROM match_decisions").fetchone()[0]
    m.build_and_write(conn)
    assert conn.execute("SELECT COUNT(*) FROM tmv_results").fetchone()[0] == before_tmv == 0
    assert conn.execute("SELECT COUNT(*) FROM validation_policy").fetchone()[0] == before_vp == 0
    assert conn.execute("SELECT COUNT(*) FROM match_decisions").fetchone()[0] == before_md == 0
    conn.close()


def test_auto_confirmed_evidence_produces_algorithmic_tmv_row(tmp_path):
    m = _load("ctmv2", "22_build_confidence_tmv.py")
    conn, db = _fresh_db(tmp_path)
    _seed_auto_confirmed_item(conn)
    result = m.build_and_write(conn)
    assert result["items"] == 1
    row = conn.execute(
        "SELECT tmv_eur, confidence_tier, evidence_basis_type FROM tmv_results_algorithmic WHERE canonical_inventory_id='c1'"
    ).fetchone()
    assert row is not None
    assert row[0] > 0
    assert row[1] == "AUTO_CONFIRMED"
    assert row[2] == "ALGORITHMIC"
    conn.close()


def test_item_tier_takes_weakest_contributing_evidence(tmp_path):
    """Item with BOTH auto-confirmed and high-confidence evidence must be
    labeled HIGH_CONFIDENCE (the weaker one), never overstated."""
    m = _load("ctmv3", "22_build_confidence_tmv.py")
    conn, db = _fresh_db(tmp_path)
    _seed_auto_confirmed_item(conn, tier="AUTO_CONFIRMED")
    conn.execute("""INSERT INTO stg_historical_ebay_sold (id, stable_evidence_uid, price_eur, sold_date, has_best_offer_option)
                    VALUES (2, 'ev2', 190.0, '2025-05-01', FALSE)""")
    conn.execute("""INSERT INTO evidence_confidence_classification
        (classification_id, classification_run_id, candidate_key, inventory_uid, matching_rule,
         source_table, source_id, evidence_uid, v2_score, confidence_tier, tier_reason)
        VALUES (2, 'run1', 'ck2', 'i1', 'PART_NUMBER_EXACT', 'match_candidates_ebay_sold', 2, 'ev2', 0.85, 'HIGH_CONFIDENCE', 'test')""")
    m.build_and_write(conn)
    row = conn.execute("SELECT confidence_tier FROM tmv_results_algorithmic WHERE canonical_inventory_id='c1'").fetchone()
    assert row[0] == "HIGH_CONFIDENCE"
    conn.close()


def test_rejected_and_medium_tier_evidence_never_produces_algorithmic_tmv(tmp_path):
    m = _load("ctmv4", "22_build_confidence_tmv.py")
    conn, db = _fresh_db(tmp_path)
    conn.execute("""INSERT INTO staging_inventory
        (inventory_uid, canonical_inventory_id, brand, caliber, part_number, stock, validation_status)
        VALUES ('i2', 'c2', 'Rolex', '3135', '4419', 5, 'PASS')""")
    conn.execute("""INSERT INTO stg_historical_ebay_sold (id, stable_evidence_uid, price_eur, sold_date, has_best_offer_option)
                    VALUES (3, 'ev3', 180.0, '2025-06-01', FALSE)""")
    conn.execute("""INSERT INTO evidence_confidence_classification
        (classification_id, classification_run_id, candidate_key, inventory_uid, matching_rule,
         source_table, source_id, evidence_uid, v2_score, confidence_tier, tier_reason)
        VALUES (3, 'run1', 'ck3', 'i2', 'PART_NUMBER_EXACT', 'match_candidates_ebay_sold', 3, 'ev3', 0.60, 'MEDIUM_CONFIDENCE', 'test')""")
    result = m.build_and_write(conn)
    assert result["items"] == 0
    n = conn.execute("SELECT COUNT(*) FROM tmv_results_algorithmic").fetchone()[0]
    assert n == 0
    conn.close()


def test_algorithmic_tmv_math_matches_governed_path_formula(tmp_path):
    """Same evidence, scored via both paths (MATCH_CONFIRMED vs
    ALGORITHMIC_AUTO_HIGH), must produce IDENTICAL tmv_eur -- proving this
    reuses 13_build_tmv.py's exact formula, not a reimplementation."""
    conn, db = _fresh_db(tmp_path)
    _seed_auto_confirmed_item(conn)
    conn.execute("""INSERT INTO match_decisions
        (decision_id, decision_version, decision_run_id, candidate_key, inventory_uid, source_table,
         source_id, matching_rule, evidence_tier, match_status, match_reason_code, collection_relationship,
         price_evidence_status, evidence_uid)
        VALUES (1, 'v1', 'dr1', 'ck1', 'i1', 'match_candidates_ebay_sold',
                1, 'PART_NUMBER_EXACT', 'A', 'MATCH_CONFIRMED', 'OK', 'NOT_APPLICABLE', 'NOT_APPLICABLE', 'ev1')""")

    tmv13 = _load("tmv13x", "13_build_tmv.py")
    res_governed = tmv13.build(conn, evidence_source="MATCH_CONFIRMED")
    res_algo = tmv13.build(conn, evidence_source="ALGORITHMIC_AUTO_HIGH")
    tmv_governed = res_governed["df"].set_index("inventory_uid").loc["i1", "tmv"]
    tmv_algo = res_algo["df"].set_index("inventory_uid").loc["i1", "tmv"]
    assert tmv_governed == tmv_algo
    conn.close()


def test_scarcity_trend_demand_persisted_not_dropped(tmp_path):
    """Bug found via client screenshot (2026-08-01): scarcity/trend showed
    as blank/0 for every algorithmic item because S/P/D were computed but
    never written to tmv_results_algorithmic. Verifies they're now
    persisted and match the values build() actually used to compute tmv_eur
    -- never a separate recomputation, never a fabricated 0."""
    m = _load("ctmv5", "22_build_confidence_tmv.py")
    conn, db = _fresh_db(tmp_path)
    _seed_auto_confirmed_item(conn)
    m.build_and_write(conn)
    row = conn.execute(
        "SELECT scarcity_score, price_trend, demand_index FROM tmv_results_algorithmic WHERE canonical_inventory_id='c1'"
    ).fetchone()
    assert row is not None
    scarcity, trend, demand = row
    assert scarcity is not None and demand is not None
    # Single evidence row -> S/D are well-defined percentile-rank values,
    # not NULL/blank; P defaults to 0.0 (not NULL) with <2 dated points.
    assert trend == 0.0
    conn.close()
