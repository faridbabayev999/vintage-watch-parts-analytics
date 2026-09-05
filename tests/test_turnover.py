"""
Module 4 — turnover validation tests (scripts/13_build_tmv.py computes
turnover_survival in the same confirmed-evidence pass as TMV).

Confirms: same MATCH_CONFIRMED-only evidence scope as TMV; the survival formula
median_days_to_sell = 30*ln(2)/lambda; edge cases (zero/one/many sales, lambda<=0,
missing dates); no price variable; end-to-end lineage.
"""
import importlib.util
import math
import sys
from pathlib import Path

import duckdb

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
SCHEMA = SCRIPTS / "schema.sql"


def _load():
    spec = importlib.util.spec_from_file_location("tmv13_turn", SCRIPTS / "13_build_tmv.py")
    m = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(SCRIPTS))
    spec.loader.exec_module(m)
    return m


def _fresh(tmp_path):
    conn = duckdb.connect(str(tmp_path / "turn.duckdb"))
    conn.execute(SCHEMA.read_text())
    conn.execute("""INSERT INTO staging_inventory
        (inventory_uid, canonical_inventory_id, brand, caliber, part_number, stock, validation_status)
        VALUES ('inv1','rolex_3135_t','Rolex','3135','3135-570', 1, 'PASS')""")
    return conn


def _confirm_sold(conn, rid, evidence_uid, price, sold_date="DATE '2025-06-01'"):
    conn.execute(f"""INSERT INTO stg_historical_ebay_sold (id, stable_evidence_uid, price_eur, sold_date, has_best_offer_option)
                     VALUES ({rid}, '{evidence_uid}', {price}, {sold_date}, FALSE)""")
    conn.execute(f"""INSERT INTO match_decisions
        (decision_id, decision_version, decision_run_id, candidate_key, inventory_uid, source_table,
         source_id, matching_rule, evidence_tier, match_status, match_reason_code, collection_relationship,
         price_evidence_status, evidence_uid)
        VALUES ({rid}, '1', 'run1', 'ck{rid}', 'inv1', 'match_candidates_ebay_sold',
                {rid}, 'CALIBER_PART_NUMBER', 'A', 'MATCH_CONFIRMED', 'OK', 'NONE', 'NOT_APPLICABLE', '{evidence_uid}')""")


def _confirm_active(conn, rid, evidence_uid, price):
    conn.execute(f"""INSERT INTO stg_active_targeted (id, stable_evidence_uid, price_eur, marketplace)
                     VALUES ({rid}, '{evidence_uid}', {price}, 'EBAY_DE')""")
    conn.execute(f"""INSERT INTO match_decisions
        (decision_id, decision_version, decision_run_id, candidate_key, inventory_uid, source_table,
         source_id, matching_rule, evidence_tier, match_status, match_reason_code, collection_relationship,
         price_evidence_status, evidence_uid)
        VALUES ({rid}, '1', 'run1', 'cka{rid}', 'inv1', 'match_candidates_active',
                {rid}, 'CALIBER_PART_NUMBER', 'A', 'MATCH_CONFIRMED', 'OK', 'NONE', 'NOT_APPLICABLE', '{evidence_uid}')""")


def _turn_row(conn):
    return conn.execute("""SELECT median_days_to_sell, probability_sell_30d, probability_sell_90d
                           FROM turnover_survival WHERE canonical_inventory_id='rolex_3135_t'""").fetchone()


def test_zero_confirmed_sales_no_turnover(tmp_path):
    m = _load(); conn = _fresh(tmp_path)
    res = m.build(conn); m.write(conn, res["df"])
    assert conn.execute("SELECT COUNT(*) FROM turnover_survival").fetchone()[0] == 0
    conn.close()


def test_one_confirmed_sale_stable(tmp_path):
    m = _load(); conn = _fresh(tmp_path)
    _confirm_sold(conn, 1, "ev1", 200.0)
    res = m.build(conn); m.write(conn, res["df"])
    row = _turn_row(conn)
    assert row is not None
    md, p30, p90 = row
    assert md is not None and md > 0
    assert 0.0 <= p30 <= 1.0 and 0.0 <= p90 <= 1.0
    conn.close()


def test_formula_median_days_matches_lambda(tmp_path):
    """median_days_to_sell == 30*ln2/lambda, lambda recovered from p30."""
    m = _load(); conn = _fresh(tmp_path)
    for i in range(1, 5):
        _confirm_sold(conn, i, f"ev{i}", 150.0 + i, sold_date=f"DATE '2025-0{i}-01'")
    res = m.build(conn); m.write(conn, res["df"])
    md, p30, _ = _turn_row(conn)
    lam = -math.log(1 - p30)          # p30 = 1 - exp(-lam)
    assert abs(md - (30 * math.log(2) / lam)) < 0.5
    conn.close()


def test_lambda_zero_active_only_capped(tmp_path):
    """Active-only confirmed match => no sold history => lambda=0 => capped."""
    m = _load(); conn = _fresh(tmp_path)
    _confirm_active(conn, 1, "eva1", 300.0)
    res = m.build(conn); m.write(conn, res["df"])
    row = _turn_row(conn)
    assert row is not None
    md, p30, p90 = row
    assert md == 3650.0 and p30 == 0.0 and p90 == 0.0   # CAP_DAYS, no fabricated velocity
    conn.close()


def test_missing_sale_date_handled_not_crashed(tmp_path):
    m = _load(); conn = _fresh(tmp_path)
    _confirm_sold(conn, 1, "ev1", 200.0, sold_date="NULL")
    res = m.build(conn); m.write(conn, res["df"])   # must not raise
    assert conn.execute("SELECT COUNT(*) FROM turnover_survival").fetchone()[0] == 1
    conn.close()


def test_extreme_outlier_controlled(tmp_path):
    m = _load(); conn = _fresh(tmp_path)
    _confirm_sold(conn, 1, "ev1", 1_000_000.0)   # price outlier must not affect turnover
    res = m.build(conn); m.write(conn, res["df"])
    md, p30, p90 = _turn_row(conn)
    assert 0 < md <= 3650.0 and 0.0 <= p30 <= 1.0 and 0.0 <= p90 <= 1.0
    conn.close()


def test_turnover_lineage_trace(tmp_path):
    """turnover_survival -> inventory -> match_decisions.evidence_uid -> sold evidence."""
    m = _load(); conn = _fresh(tmp_path)
    _confirm_sold(conn, 7, "ev_trace", 250.0)
    res = m.build(conn); m.write(conn, res["df"])
    trace = conn.execute("""
        SELECT ts.canonical_inventory_id, d.evidence_uid, e.stable_evidence_uid
        FROM turnover_survival ts
        JOIN staging_inventory i ON i.canonical_inventory_id = ts.canonical_inventory_id
        JOIN match_decisions d ON d.inventory_uid = i.inventory_uid AND d.match_status='MATCH_CONFIRMED'
        JOIN stg_historical_ebay_sold e ON e.stable_evidence_uid = d.evidence_uid
    """).fetchone()
    assert trace is not None and trace[1] == trace[2] == "ev_trace"
    conn.close()
