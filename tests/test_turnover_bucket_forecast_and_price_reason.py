"""
Tasks 6/7 (owner-approved final work plan, 2026-07-30) — regression tests.

Task 7: expected-units-sold-per-time-bucket forecast (turnover_survival.
turnover_bucket_forecast), integrated from the SAME hazard-rate survival curve
that already produces median_days_to_sell/probability_sell_30d/90d — no new
methodology, just integrated over each of the 8 buckets instead of solved for
the median, and scaled by stock quantity Q.

Task 6: recommendation_reason (feat_pricing) — plain-text disclosure of why
recommended_price_eur has its value. recommended_price_eur itself MUST remain
numerically identical to tmv_eur (no invented multiplier, owner instruction).

Disposable in-tmp DuckDB only, never the live database.
"""
import importlib.util
import json
import math
import sys
from pathlib import Path

import duckdb
import pytest

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
SCHEMA = SCRIPTS / "schema.sql"


def _load():
    spec = importlib.util.spec_from_file_location("tmv13_bf", SCRIPTS / "13_build_tmv.py")
    m = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(SCRIPTS))
    spec.loader.exec_module(m)
    return m


def _fresh_db(tmp_path, name="bf.duckdb"):
    db = tmp_path / name
    conn = duckdb.connect(str(db))
    conn.execute(SCHEMA.read_text())
    return conn, db


def _inventory(conn, uid, cid, stock=10, caliber="3135", brand="Rolex"):
    conn.execute("""INSERT INTO staging_inventory
        (inventory_uid, canonical_inventory_id, brand, caliber, part_number, stock, validation_status)
        VALUES (?, ?, ?, ?, ?, ?, 'PASS')""", [uid, cid, brand, caliber, f"{cid}-pn", stock])


def _confirmed_sold(conn, rid, evidence_uid, inv_uid, price, sold_date):
    conn.execute("""INSERT INTO stg_historical_ebay_sold (id, stable_evidence_uid, price_eur, sold_date, has_best_offer_option)
                    VALUES (?, ?, ?, ?, FALSE)""", [rid, evidence_uid, price, sold_date])
    conn.execute("""INSERT INTO match_decisions
        (decision_id, decision_version, decision_run_id, candidate_key, inventory_uid, source_table,
         source_id, matching_rule, evidence_tier, match_status, match_reason_code, collection_relationship,
         price_evidence_status, evidence_uid)
        VALUES (?, '1', 'run1', ?, ?, 'match_candidates_ebay_sold',
                ?, 'CALIBER_PART_NUMBER', 'A', 'MATCH_CONFIRMED', 'OK', 'NONE', 'NOT_APPLICABLE', ?)""",
        [rid, f"ck{rid}", inv_uid, rid, evidence_uid])


def _confirmed_active(conn, rid, evidence_uid, inv_uid, price):
    conn.execute("""INSERT INTO stg_active_targeted (id, stable_evidence_uid, price_eur, marketplace)
                    VALUES (?, ?, ?, 'EBAY_DE')""", [rid, evidence_uid, price])
    conn.execute("""INSERT INTO match_decisions
        (decision_id, decision_version, decision_run_id, candidate_key, inventory_uid, source_table,
         source_id, matching_rule, evidence_tier, match_status, match_reason_code, collection_relationship,
         price_evidence_status, evidence_uid)
        VALUES (?, '1', 'run1', ?, ?, 'match_candidates_active',
                ?, 'CALIBER_PART_NUMBER', 'A', 'MATCH_CONFIRMED', 'OK', 'NONE', 'NOT_APPLICABLE', ?)""",
        [rid, f"cka{rid}", inv_uid, rid, evidence_uid])


# ── Task 7: bucket forecast ────────────────────────────────────────────────

def test_bucket_forecast_sums_to_stock_quantity(tmp_path):
    """Expected units across all 8 buckets must sum to the item's stock
    (all probability mass is accounted for; the last bucket is open-ended)."""
    m = _load()
    conn, _ = _fresh_db(tmp_path)
    _inventory(conn, "i1", "c1", stock=25)
    for i in range(5):
        _confirmed_sold(conn, i + 1, f"ev{i+1}", "i1", 150.0, f"2025-0{i+1}-01")
    res = m.build(conn)
    row = res["df"].set_index("inventory_uid").loc["i1"]
    buckets = json.loads(row["turnover_bucket_forecast"])
    assert len(buckets) == 8
    assert sum(b["expected_units"] for b in buckets) == pytest.approx(25.0, abs=0.05)
    conn.close()


def test_bucket_forecast_matches_hand_computed_survival_curve(tmp_path):
    """Each bucket's expected units must equal stock * (F(hi) - F(lo-1)) for
    the exact same lambda_monthly the median/probability fields already use --
    independently recomputed here, not copied from the implementation."""
    m = _load()
    conn, _ = _fresh_db(tmp_path)
    _inventory(conn, "i1", "c1", stock=40)
    for i in range(6):
        _confirmed_sold(conn, i + 1, f"ev{i+1}", "i1", 150.0, f"2025-0{(i % 6) + 1}-01")
    res = m.build(conn)
    row = res["df"].set_index("inventory_uid").loc["i1"]
    lam = row["lambda_monthly"]
    buckets = json.loads(row["turnover_bucket_forecast"])
    BUCKETS = [(0, 7), (8, 30), (31, 90), (91, 183), (184, 365), (366, 730), (731, 1065), (1066, 100000)]

    def F(t):
        t = max(0.0, t)
        return 1.0 - math.exp(-lam * t / 30.0)

    for (lo, hi), b in zip(BUCKETS, buckets):
        f_lo = F(lo - 1)
        f_hi = 1.0 if hi >= 100000 else F(hi)
        expected = round(40 * max(0.0, f_hi - f_lo), 3)
        assert b["expected_units"] == pytest.approx(expected, abs=0.01)
    conn.close()


def test_bucket_forecast_all_zero_when_no_confirmed_sales_velocity(tmp_path):
    """ACTIVE_ONLY items (no historical sold evidence -> lambda_monthly<=0)
    must get an all-zero bucket forecast, never a fabricated distribution."""
    m = _load()
    conn, _ = _fresh_db(tmp_path)
    _inventory(conn, "i1", "c1", stock=10)
    _confirmed_active(conn, 1, "ev1", "i1", 200.0)
    res = m.build(conn)
    row = res["df"].set_index("inventory_uid").loc["i1"]
    buckets = json.loads(row["turnover_bucket_forecast"])
    assert all(b["expected_units"] == 0.0 for b in buckets)
    conn.close()


def test_bucket_forecast_persisted_and_readable_from_turnover_survival(tmp_path):
    m = _load()
    conn, db = _fresh_db(tmp_path)
    _inventory(conn, "i1", "c1", stock=12)
    for i in range(3):
        _confirmed_sold(conn, i + 1, f"ev{i+1}", "i1", 150.0, f"2025-0{i+1}-01")
    res = m.build(conn)
    m.write(conn, res["df"])
    row = conn.execute(
        "SELECT turnover_bucket_forecast FROM turnover_survival WHERE canonical_inventory_id='c1'"
    ).fetchone()
    assert row is not None and row[0] is not None
    buckets = json.loads(row[0])
    assert len(buckets) == 8
    assert {b["bucket"] for b in buckets} == {
        "0-7", "8-30", "31-90", "91-183", "184-365", "366-730", "731-1065", "1066+"
    }
    conn.close()


# ── Task 6: recommendation transparency ────────────────────────────────────

def test_recommended_price_still_equals_tmv_exactly(tmp_path):
    """The disclosure text must NOT change the price -- recommended_price_eur
    stays bit-for-bit equal to tmv_eur (no invented multiplier)."""
    m = _load()
    conn, db = _fresh_db(tmp_path)
    _inventory(conn, "i1", "c1", stock=5)
    for i in range(3):
        _confirmed_sold(conn, i + 1, f"ev{i+1}", "i1", 150.0, f"2025-0{i+1}-01")
    res = m.build(conn)
    m.write(conn, res["df"])
    row = conn.execute(
        "SELECT f.recommended_price_eur, t.tmv_eur FROM feat_pricing f "
        "JOIN tmv_results t USING (canonical_inventory_id) WHERE f.canonical_inventory_id='c1'"
    ).fetchone()
    assert row[0] == row[1]
    conn.close()


def test_recommendation_reason_cites_historical_basis_when_h_exists(tmp_path):
    m = _load()
    conn, _ = _fresh_db(tmp_path)
    _inventory(conn, "i1", "c1", stock=5)
    for i in range(3):
        _confirmed_sold(conn, i + 1, f"ev{i+1}", "i1", 150.0, f"2025-0{i+1}-01")
    res = m.build(conn)
    row = res["df"].set_index("inventory_uid").loc["i1"]
    assert "historical sold evidence" in row["recommendation_reason"]
    assert row["confidence_tier"] in row["recommendation_reason"]
    conn.close()


def test_recommendation_reason_cites_active_only_basis_when_no_history(tmp_path):
    m = _load()
    conn, _ = _fresh_db(tmp_path)
    _inventory(conn, "i1", "c1", stock=5)
    _confirmed_active(conn, 1, "ev1", "i1", 200.0)
    res = m.build(conn)
    row = res["df"].set_index("inventory_uid").loc["i1"]
    assert "active asking prices only" in row["recommendation_reason"]
    assert "ask" in row["recommendation_reason"].lower()
    conn.close()


def test_recommendation_reason_persisted_alongside_tmv(tmp_path):
    m = _load()
    conn, db = _fresh_db(tmp_path)
    _inventory(conn, "i1", "c1", stock=5)
    for i in range(3):
        _confirmed_sold(conn, i + 1, f"ev{i+1}", "i1", 150.0, f"2025-0{i+1}-01")
    res = m.build(conn)
    m.write(conn, res["df"])
    row = conn.execute(
        "SELECT recommendation_reason FROM feat_pricing WHERE canonical_inventory_id='c1'"
    ).fetchone()
    assert row is not None and row[0]
    conn.close()
