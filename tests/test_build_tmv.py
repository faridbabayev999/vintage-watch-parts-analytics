"""
Tests for scripts/13_build_tmv.py (Module 3 — TMV ported onto A).

Verifies the three Module-3 contract points:
  1. Empty MATCH_CONFIRMED -> pipeline completes, tmv_results has 0 rows,
     no fabricated confirmations, no fallback rows.
  2. Synthetic APPROVED/MATCH_CONFIRMED evidence -> TMV computes a real value.
  3. Lineage trace: tmv_results -> match_decisions.evidence_uid ->
     stg.stable_evidence_uid -> raw evidence resolves end to end.

All against a disposable in-tmp DuckDB; never the live database.
"""
import importlib.util
from pathlib import Path

import duckdb
import pytest

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
SCHEMA = SCRIPTS / "schema.sql"


def _load():
    spec = importlib.util.spec_from_file_location("tmv13", SCRIPTS / "13_build_tmv.py")
    m = importlib.util.module_from_spec(spec)
    import sys
    sys.path.insert(0, str(SCRIPTS))
    spec.loader.exec_module(m)
    return m


def _fresh_db(tmp_path):
    db = tmp_path / "tmv_test.duckdb"
    conn = duckdb.connect(str(db))
    conn.execute(SCHEMA.read_text())
    # one eligible inventory item
    conn.execute("""INSERT INTO staging_inventory
        (inventory_uid, canonical_inventory_id, brand, caliber, part_number, stock, validation_status)
        VALUES ('inv1','rolex_3135_test','Rolex','3135','3135-570', 2, 'PASS')""")
    return conn, db


def _add_confirmed_ebay(conn, *, evidence_uid="ev_sold_1", price=200.0, best_offer=False):
    conn.execute("""INSERT INTO stg_historical_ebay_sold (id, stable_evidence_uid, price_eur, sold_date, has_best_offer_option)
                    VALUES (1, ?, ?, DATE '2025-06-01', ?)""", [evidence_uid, price, best_offer])
    conn.execute("""INSERT INTO match_decisions
        (decision_id, decision_version, decision_run_id, candidate_key, inventory_uid, source_table,
         source_id, matching_rule, evidence_tier, match_status, match_reason_code, collection_relationship,
         price_evidence_status, evidence_uid)
        VALUES (1, '1', 'run1', 'ck1', 'inv1', 'match_candidates_ebay_sold',
                1, 'CALIBER_PART_NUMBER', 'A', 'MATCH_CONFIRMED', 'TIER_A_CLEAN_NO_CONTRADICTION_NO_RISK',
                'NONE', 'NOT_APPLICABLE', ?)""", [evidence_uid])


def test_empty_match_confirmed_yields_zero_rows(tmp_path):
    m = _load()
    conn, _ = _fresh_db(tmp_path)  # inventory present, but NO match_decisions
    res = m.build(conn)
    m.write(conn, res["df"])
    assert conn.execute("SELECT COUNT(*) FROM tmv_results").fetchone()[0] == 0
    # price_reliability column still provisioned even when empty
    cols = [r[0] for r in conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name='tmv_results'").fetchall()]
    assert "price_reliability" in cols
    conn.close()


def test_synthetic_confirmed_produces_tmv(tmp_path):
    m = _load()
    conn, _ = _fresh_db(tmp_path)
    _add_confirmed_ebay(conn, price=200.0, best_offer=False)
    res = m.build(conn)
    m.write(conn, res["df"])
    row = conn.execute("""SELECT tmv_eur, valuation_basis, price_reliability
                          FROM tmv_results WHERE canonical_inventory_id='rolex_3135_test'""").fetchone()
    assert row is not None, "a confirmed match must yield a TMV row"
    tmv_eur, basis, reliability = row
    assert tmv_eur and tmv_eur > 0
    assert basis == "HISTORICAL"                 # sold evidence exists -> historical-only
    assert reliability == "CONFIRMED_SALE_PRICE"  # not a best-offer listing
    conn.close()


def test_best_offer_evidence_flagged_as_proxy(tmp_path):
    m = _load()
    conn, _ = _fresh_db(tmp_path)
    _add_confirmed_ebay(conn, price=200.0, best_offer=True)
    res = m.build(conn)
    m.write(conn, res["df"])
    reliability = conn.execute(
        "SELECT price_reliability FROM tmv_results WHERE canonical_inventory_id='rolex_3135_test'").fetchone()[0]
    assert reliability == "BEST_OFFER_PROXY"
    conn.close()


def test_tmv_lineage_trace_resolves_to_evidence(tmp_path):
    """tmv_results -> match_decisions.evidence_uid -> stg.stable_evidence_uid."""
    m = _load()
    conn, _ = _fresh_db(tmp_path)
    _add_confirmed_ebay(conn, evidence_uid="ev_trace_9", price=250.0)
    res = m.build(conn)
    m.write(conn, res["df"])
    trace = conn.execute("""
        SELECT t.canonical_inventory_id, d.evidence_uid, e.stable_evidence_uid, e.price_eur
        FROM tmv_results t
        JOIN staging_inventory i ON i.canonical_inventory_id = t.canonical_inventory_id
        JOIN match_decisions d ON d.inventory_uid = i.inventory_uid AND d.match_status='MATCH_CONFIRMED'
        JOIN stg_historical_ebay_sold e ON e.stable_evidence_uid = d.evidence_uid
    """).fetchone()
    assert trace is not None, "TMV output must trace back to source evidence via evidence_uid"
    assert trace[1] == trace[2] == "ev_trace_9"
    assert trace[3] == 250.0
    conn.close()


def test_tmv_never_consumes_low_confidence_or_review(tmp_path):
    """A LOW_CONFIDENCE_CANDIDATE / REVIEW_REQUIRED row must NOT feed TMV."""
    m = _load()
    conn, _ = _fresh_db(tmp_path)
    conn.execute("""INSERT INTO stg_historical_ebay_sold (id, stable_evidence_uid, price_eur, sold_date, has_best_offer_option)
                    VALUES (1, 'ev_low', 999.0, DATE '2025-06-01', FALSE)""")
    for i, status in enumerate(("LOW_CONFIDENCE_CANDIDATE", "REVIEW_REQUIRED"), start=1):
        conn.execute("""INSERT INTO match_decisions
            (decision_id, decision_version, decision_run_id, candidate_key, inventory_uid, source_table,
             source_id, matching_rule, evidence_tier, match_status, match_reason_code, collection_relationship,
             price_evidence_status, evidence_uid)
            VALUES (?, '1', 'run1', ?, 'inv1', 'match_candidates_ebay_sold',
                    1, 'BRAND_CALIBER', 'B', ?, 'X', 'NONE', 'NOT_APPLICABLE', 'ev_low')""",
            [i, f"ck_{status}", status])
    res = m.build(conn)
    m.write(conn, res["df"])
    assert conn.execute("SELECT COUNT(*) FROM tmv_results").fetchone()[0] == 0
    conn.close()
