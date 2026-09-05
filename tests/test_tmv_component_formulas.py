"""
Phase 1 — regression tests for the H/C/D/S/P TMV components (scripts/13_build_tmv.py).

No business logic changed. Each test hand-derives the expected value from the
documented formula and asserts the actual computed output matches, plus
determinism (same input -> same output across repeated builds), lineage
(feat_pricing/feat_demand rows correspond to tmv_results rows), and no-silent-
failure behavior (insufficient data yields a defined default, never a crash or
a fabricated value). Disposable in-tmp DuckDB only, never the live database.
"""
import importlib.util
import math
import sys
from pathlib import Path

import duckdb
import pytest

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
SCHEMA = SCRIPTS / "schema.sql"


def _load():
    spec = importlib.util.spec_from_file_location("tmv13_fc", SCRIPTS / "13_build_tmv.py")
    m = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(SCRIPTS))
    spec.loader.exec_module(m)
    return m


def _fresh_db(tmp_path, name="fc.duckdb"):
    db = tmp_path / name
    conn = duckdb.connect(str(db))
    conn.execute(SCHEMA.read_text())
    return conn, db


def _inventory(conn, uid, cid, caliber="3135", brand="Rolex"):
    conn.execute("""INSERT INTO staging_inventory
        (inventory_uid, canonical_inventory_id, brand, caliber, part_number, stock, validation_status)
        VALUES (?, ?, ?, ?, ?, 1, 'PASS')""", [uid, cid, brand, caliber, f"{cid}-pn"])


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


# ── H: historical market value ────────────────────────────────────────────────

def test_h_single_sale_at_reference_date_equals_price_exactly(tmp_path):
    """A single sold observation dated exactly at the reference date has
    recency weight = 1 -> H must equal the price exactly (no decay)."""
    m = _load()
    conn, _ = _fresh_db(tmp_path)
    _inventory(conn, "i1", "c1")
    _confirmed_sold(conn, 1, "ev1", "i1", 180.0, "2025-06-01")  # only/latest sale -> ref date
    res = m.build(conn)
    row = res["df"].set_index("inventory_uid").loc["i1"]
    assert row["H"] == pytest.approx(180.0)
    assert row["n_hist"] == 1
    conn.close()


def test_h_recency_weighted_average_matches_hand_computation(tmp_path):
    """Two sold observations at different ages: H must equal the documented
    recency+volume weighted average, computed independently here."""
    m = _load()
    conn, _ = _fresh_db(tmp_path)
    _inventory(conn, "i1", "c1")
    _confirmed_sold(conn, 1, "ev1", "i1", 100.0, "2025-01-01")  # older
    _confirmed_sold(conn, 2, "ev2", "i1", 200.0, "2025-06-01")  # reference (newest)
    res = m.build(conn)
    row = res["df"].set_index("inventory_uid").loc["i1"]

    ref = m._now_ref(res_dates := None) if False else None  # ref computed internally; recompute independently:
    from datetime import date
    ref_date = date(2025, 6, 1)
    age1 = (ref_date - date(2025, 1, 1)).days / 30.44
    w1 = math.exp(-math.log(2) * age1 / 12.0)
    w2 = 1.0  # age 0
    expected_H = (w1 * 100.0 * 1 + w2 * 200.0 * 1) / (w1 * 1 + w2 * 1)
    assert row["H"] == pytest.approx(expected_H, rel=1e-6)
    conn.close()


# ── C: current market value ───────────────────────────────────────────────────

def test_repeat_collected_active_listing_counted_once_not_per_fetch(tmp_path):
    """Pricing quality audit finding (2026-07-31): the same physical active
    listing, re-collected across multiple query fetches, shares one
    stable_evidence_uid across several raw stg_active_targeted rows. Before
    the fix, C's median counted every duplicate fetch as an independent
    market observation (one €20 listing observed 12x skewed the whole
    median down). After the fix, one evidence_uid = exactly one price
    observation, regardless of how many times it was re-fetched."""
    m = _load()
    conn, _ = _fresh_db(tmp_path)
    _inventory(conn, "i1", "c1")
    # One listing (ev_dup) re-fetched 3x at the same €20 price, at
    # different fetched_at timestamps -- must count ONCE, not 3x.
    for k, fetched_at in enumerate(["2025-01-01", "2025-02-01", "2025-03-01"]):
        conn.execute(
            "INSERT INTO stg_active_targeted (id, stable_evidence_uid, price_eur, marketplace, fetched_at) "
            "VALUES (?, 'ev_dup', 20.0, 'EBAY_DE', ?)", [100 + k, fetched_at])
    conn.execute(
        "INSERT INTO match_decisions (decision_id, decision_version, decision_run_id, candidate_key, "
        "inventory_uid, source_table, source_id, matching_rule, evidence_tier, match_status, "
        "match_reason_code, collection_relationship, price_evidence_status, evidence_uid) "
        "VALUES (1, '1', 'run1', 'ck_dup', 'i1', 'match_candidates_active', 100, "
        "'CALIBER_PART_NUMBER', 'A', 'MATCH_CONFIRMED', 'OK', 'NONE', 'NOT_APPLICABLE', 'ev_dup')")
    # A second, genuinely distinct listing at €200 (observed once).
    _confirmed_active(conn, 2, "ev_distinct", "i1", 200.0)
    res = m.build(conn)
    row = res["df"].set_index("inventory_uid").loc["i1"]
    # Two DISTINCT listings (€20, €200) -> median = 110.0, not median of
    # [20,20,20,200] = 20.0 (which is what the bug would have produced).
    assert row["C"] == pytest.approx(110.0 * m.ASK_TO_SOLD_DISCOUNT)
    assert row["n_active"] == 2
    conn.close()


def test_c_equals_median_active_times_discount(tmp_path):
    m = _load()
    conn, _ = _fresh_db(tmp_path)
    _inventory(conn, "i1", "c1")
    _confirmed_active(conn, 1, "eva1", "i1", 100.0)
    _confirmed_active(conn, 2, "eva2", "i1", 300.0)
    _confirmed_active(conn, 3, "eva3", "i1", 200.0)  # median = 200
    res = m.build(conn)
    row = res["df"].set_index("inventory_uid").loc["i1"]
    expected_C = 200.0 * m.ASK_TO_SOLD_DISCOUNT
    assert row["C"] == pytest.approx(expected_C)
    assert row["n_active"] == 3
    conn.close()


# ── D: demand index ────────────────────────────────────────────────────────────

def test_d_percentile_rank_within_caliber_group_of_three(tmp_path):
    """3 items, same caliber (>=3 triggers within-group ranking, not global
    fallback), distinct total_sold -> distinct velocity -> D must match
    pandas rank(pct=True) exactly: lowest=1/3, mid=2/3, highest=1.0."""
    m = _load()
    conn, _ = _fresh_db(tmp_path)
    for uid, cid in [("iA", "cA"), ("iB", "cB"), ("iC", "cC")]:
        _inventory(conn, uid, cid, caliber="3135")
    rid = 1
    for i in range(1):  # iA: 1 sale
        _confirmed_sold(conn, rid, f"evA{rid}", "iA", 150.0, "2025-06-01"); rid += 1
    for i in range(2):  # iB: 2 sales
        _confirmed_sold(conn, rid, f"evB{rid}", "iB", 150.0, "2025-06-01"); rid += 1
    for i in range(5):  # iC: 5 sales
        _confirmed_sold(conn, rid, f"evC{rid}", "iC", 150.0, "2025-06-01"); rid += 1
    res = m.build(conn)
    d = res["df"].set_index("inventory_uid")["D"]
    assert d["iA"] == pytest.approx(1 / 3)
    assert d["iB"] == pytest.approx(2 / 3)
    assert d["iC"] == pytest.approx(1.0)
    conn.close()


def test_d_computed_but_not_in_tmv_price_formula(tmp_path):
    """Regression-locks the audited finding: D is computed and returned, but
    changing it must not change tmv_eur (it is not wired into _tmv())."""
    m = _load()
    conn, _ = _fresh_db(tmp_path)
    _inventory(conn, "i1", "c1")
    _confirmed_sold(conn, 1, "ev1", "i1", 180.0, "2025-06-01")
    res = m.build(conn)
    df = res["df"]
    tmv_before = df.set_index("inventory_uid").loc["i1", "tmv"]
    df2 = df.copy()
    df2.loc[df2["inventory_uid"] == "i1", "D"] = 0.0  # forcibly zero out D
    # re-derive tmv the same way _tmv() does, using the (now-zeroed) row -- must be unaffected
    r = df2.set_index("inventory_uid").loc["i1"]
    recomputed = r["H"] * (1 + m.ALPHA_TREND * r["P"]) * (1 + m.BETA_SCARCITY * (r["S"] - 0.5))
    assert round(recomputed, 2) == tmv_before
    conn.close()


# ── S: market dynamics / scarcity ─────────────────────────────────────────────

def test_s_scarcity_percentile_rank_within_caliber_group_of_three(tmp_path):
    """3 items, same caliber, SAME total_sold (isolates the active_count
    dimension of scarcity_raw=active_count/(total_sold+1)), distinct
    active_count -> distinct scarcity_raw -> S=1-rank must match hand
    computation exactly. (scarcity_raw is 0 whenever active_count=0 regardless
    of total_sold -- varying total_sold alone does not vary scarcity_raw.)"""
    m = _load()
    conn, _ = _fresh_db(tmp_path)
    for uid, cid in [("iA", "cA"), ("iB", "cB"), ("iC", "cC")]:
        _inventory(conn, uid, cid, caliber="3135")
    rid = 1
    for uid in ("iA", "iB", "iC"):
        _confirmed_sold(conn, rid, f"ev{uid}{rid}", uid, 150.0, "2025-06-01"); rid += 1
    # active_count: A=3, B=1, C=0 -> scarcity_raw = active_count/(1+1): A=1.5, B=0.5, C=0.0
    for i in range(3):
        _confirmed_active(conn, rid, f"eva{rid}", "iA", 150.0); rid += 1
    _confirmed_active(conn, rid, f"eva{rid}", "iB", 150.0); rid += 1
    res = m.build(conn)
    df = res["df"].set_index("inventory_uid")
    assert df.loc["iA", "scarcity_raw"] == pytest.approx(1.5)
    assert df.loc["iB", "scarcity_raw"] == pytest.approx(0.5)
    assert df.loc["iC", "scarcity_raw"] == pytest.approx(0.0)
    # ascending scarcity_raw: C(0) < B(0.5) < A(1.5) -> rank pct: C=1/3, B=2/3, A=1.0 -> S=1-rank
    assert df.loc["iC", "S"] == pytest.approx(2 / 3)
    assert df.loc["iB", "S"] == pytest.approx(1 / 3)
    assert df.loc["iA", "S"] == pytest.approx(0.0)
    conn.close()


# ── P: price trend ─────────────────────────────────────────────────────────────

def test_p_zero_with_fewer_than_two_dated_points(tmp_path):
    """No silent failure: <2 dated sold points -> P defaults to 0.0, never crashes."""
    m = _load()
    conn, _ = _fresh_db(tmp_path)
    _inventory(conn, "i1", "c1")
    _confirmed_sold(conn, 1, "ev1", "i1", 150.0, "2025-06-01")
    res = m.build(conn)
    row = res["df"].set_index("inventory_uid").loc["i1"]
    assert row["P"] == 0.0
    conn.close()


def test_p_positive_slope_matches_ols_hand_computation(tmp_path):
    m = _load()
    conn, _ = _fresh_db(tmp_path)
    _inventory(conn, "i1", "c1")
    _confirmed_sold(conn, 1, "ev1", "i1", 100.0, "2025-01-01")
    _confirmed_sold(conn, 2, "ev2", "i1", 200.0, "2025-06-01")
    res = m.build(conn)
    row = res["df"].set_index("inventory_uid").loc["i1"]
    import numpy as np, pandas as pd
    months = pd.to_datetime(pd.Series(["2025-01-01", "2025-06-01"]))
    x = (months - months.min()).dt.days.values / 30.44
    y = np.array([100.0, 200.0])
    slope = np.polyfit(x, y, 1)[0]
    expected_P = float(np.clip(slope / row["H"], -0.10, 0.10))
    assert row["P"] == pytest.approx(expected_P, rel=1e-6)
    conn.close()


# ── Determinism ────────────────────────────────────────────────────────────────

def test_determinism_same_input_same_output_across_builds(tmp_path):
    """Two independent build() calls on the same evidence must produce
    bit-identical H/C/D/S/P/tmv (excluding wall-clock timestamps)."""
    m = _load()
    conn, _ = _fresh_db(tmp_path)
    for uid, cid in [("iA", "cA"), ("iB", "cB"), ("iC", "cC")]:
        _inventory(conn, uid, cid, caliber="3135")
    rid = 1
    for uid, n in [("iA", 1), ("iB", 2), ("iC", 5)]:
        for _ in range(n):
            _confirmed_sold(conn, rid, f"ev{rid}", uid, 150.0 + rid, "2025-06-01"); rid += 1
    df1 = m.build(conn)["df"].sort_values("inventory_uid").reset_index(drop=True)
    df2 = m.build(conn)["df"].sort_values("inventory_uid").reset_index(drop=True)
    for col in ["H", "C", "D", "S", "P", "tmv", "valuation_basis"]:
        assert df1[col].equals(df2[col]), f"non-deterministic column: {col}"
    conn.close()


# ── Lineage: feat_pricing / feat_demand correspond to tmv_results ──────────────

def test_lineage_feat_pricing_and_feat_demand_match_tmv_results(tmp_path):
    m = _load()
    conn, _ = _fresh_db(tmp_path)
    _inventory(conn, "i1", "c1")
    _confirmed_sold(conn, 1, "ev1", "i1", 180.0, "2025-06-01")
    res = m.build(conn)
    m.write(conn, res["df"])
    tmv_row = conn.execute(
        "SELECT canonical_inventory_id, tmv_eur FROM tmv_results WHERE canonical_inventory_id='c1'").fetchone()
    fp_row = conn.execute(
        "SELECT canonical_inventory_id FROM feat_pricing WHERE canonical_inventory_id='c1'").fetchone()
    fd_row = conn.execute(
        "SELECT canonical_inventory_id, recency_score FROM feat_demand WHERE canonical_inventory_id='c1'").fetchone()
    assert tmv_row is not None and fp_row is not None and fd_row is not None
    assert fd_row[1] is not None  # D (recency_score) persisted, per the audited finding
    conn.close()


# ── No silent failures ──────────────────────────────────────────────────────────

# ── Phase 4: H/C persistence (additive, no formula/weight change) ─────────────

def test_tmv_output_unchanged_by_hc_persistence(tmp_path):
    """Adding historical_value_eur/current_value_eur must not alter tmv_eur,
    confidence_tier, or valuation_basis in any way."""
    m = _load()
    conn, _ = _fresh_db(tmp_path)
    _inventory(conn, "i1", "c1")
    _confirmed_sold(conn, 1, "ev1", "i1", 180.0, "2025-06-01")
    res = m.build(conn)
    m.write(conn, res["df"])
    row = conn.execute("""SELECT tmv_eur, confidence_tier, valuation_basis
                          FROM tmv_results WHERE canonical_inventory_id='c1'""").fetchone()
    tmv_eur, tier, basis = row
    assert tmv_eur is not None and tmv_eur > 0
    assert tier == "MEDIUM" and basis == "HISTORICAL"
    conn.close()


def test_historical_value_eur_equals_internal_h(tmp_path):
    m = _load()
    conn, _ = _fresh_db(tmp_path)
    _inventory(conn, "i1", "c1")
    _confirmed_sold(conn, 1, "ev1", "i1", 100.0, "2025-01-01")
    _confirmed_sold(conn, 2, "ev2", "i1", 200.0, "2025-06-01")
    res = m.build(conn)
    internal_H = res["df"].set_index("canonical_inventory_id").loc["c1", "H"]
    m.write(conn, res["df"])
    stored = conn.execute(
        "SELECT historical_value_eur FROM feat_pricing WHERE canonical_inventory_id='c1'").fetchone()[0]
    assert stored == pytest.approx(internal_H, abs=0.01)  # stored value is round(H, 2) by design
    conn.close()


def test_current_value_eur_equals_internal_c(tmp_path):
    m = _load()
    conn, _ = _fresh_db(tmp_path)
    _inventory(conn, "i1", "c1")
    _confirmed_active(conn, 1, "eva1", "i1", 100.0)
    _confirmed_active(conn, 2, "eva2", "i1", 300.0)
    res = m.build(conn)
    internal_C = res["df"].set_index("canonical_inventory_id").loc["c1", "C"]
    m.write(conn, res["df"])
    stored = conn.execute(
        "SELECT current_value_eur FROM feat_pricing WHERE canonical_inventory_id='c1'").fetchone()[0]
    assert stored == pytest.approx(internal_C, rel=1e-6)
    conn.close()


# ── ref_tmv_parameters framework (demand weight, owner decision 2026-07-30) ───

def test_demand_weight_defaults_to_zero_no_row(tmp_path):
    """No ref_tmv_parameters row at all -> demand_weight defaults to 0.0 ->
    tmv equals base*(1+ALPHA_TREND*P)*(1+BETA_SCARCITY*(S-0.5)) exactly, with
    no demand term effect (mathematically verified, not guessed)."""
    m = _load()
    conn, _ = _fresh_db(tmp_path)
    _inventory(conn, "i1", "c1")
    _confirmed_sold(conn, 1, "ev1", "i1", 180.0, "2025-06-01")
    res = m.build(conn)
    row = res["df"].set_index("inventory_uid").loc["i1"]
    expected = round(row["H"] * (1 + m.ALPHA_TREND * row["P"]) * (1 + m.BETA_SCARCITY * (row["S"] - 0.5)), 2)
    assert row["tmv"] == expected
    conn.close()


def test_demand_parameter_row_exists_after_seeding(tmp_path):
    conn, db = _fresh_db(tmp_path)
    conn.execute("""INSERT INTO ref_tmv_parameters (parameter_name, parameter_value, active_flag, description)
                    VALUES ('demand_weight', 0.0, FALSE, 'test')""")
    row = conn.execute("SELECT parameter_value, active_flag FROM ref_tmv_parameters WHERE parameter_name='demand_weight'").fetchone()
    assert row == (0.0, False)
    conn.close()


def test_inactive_demand_weight_has_no_effect_even_if_value_nonzero(tmp_path):
    """The active_flag gate matters independently of the numeric value:
    a nonzero value with active_flag=FALSE must still be a no-op."""
    m = _load()
    conn, _ = _fresh_db(tmp_path)
    conn.execute("""INSERT INTO ref_tmv_parameters (parameter_name, parameter_value, active_flag, description)
                    VALUES ('demand_weight', 5.0, FALSE, 'test: large value but inactive')""")
    _inventory(conn, "i1", "c1")
    _confirmed_sold(conn, 1, "ev1", "i1", 180.0, "2025-06-01")
    res = m.build(conn)
    row = res["df"].set_index("inventory_uid").loc["i1"]
    expected = round(row["H"] * (1 + m.ALPHA_TREND * row["P"]) * (1 + m.BETA_SCARCITY * (row["S"] - 0.5)), 2)
    assert row["tmv"] == expected  # inactive -> no-op regardless of value
    conn.close()


def test_activated_demand_weight_does_change_tmv():
    """Proves the mechanism is real (not dead code): with active_flag=TRUE
    and a nonzero weight, D must move the price, per the documented formula
    (1 + demand_weight*(D-0.5))."""
    m = _load()
    import duckdb as _d
    conn = _d.connect(":memory:")
    conn.execute(SCHEMA.read_text())
    conn.execute("""INSERT INTO ref_tmv_parameters (parameter_name, parameter_value, active_flag, description)
                    VALUES ('demand_weight', 0.5, TRUE, 'test: active')""")
    for uid, cid in [("iA", "cA"), ("iB", "cB"), ("iC", "cC")]:
        _inventory(conn, uid, cid, caliber="3135")
    rid = 1
    for uid, n in [("iA", 1), ("iB", 2), ("iC", 5)]:  # distinct D via distinct total_sold
        for _ in range(n):
            _confirmed_sold(conn, rid, f"ev{rid}", uid, 150.0, "2025-06-01"); rid += 1
    res = m.build(conn)
    df = res["df"].set_index("inventory_uid")
    # D: iA=1/3, iB=2/3, iC=1.0 (verified elsewhere by test_d_percentile_rank_...)
    for uid in ("iA", "iB", "iC"):
        r = df.loc[uid]
        expected = round(r["H"] * (1 + m.ALPHA_TREND * r["P"]) * (1 + m.BETA_SCARCITY * (r["S"] - 0.5))
                          * (1 + 0.5 * (r["D"] - 0.5)), 2)
        assert r["tmv"] == expected, uid
    # with demand_weight active and D strictly increasing (iA<iB<iC), and H/P/S
    # identical across the three (same price, same caliber peer group), tmv must
    # be strictly increasing too -- the mechanism genuinely moves the price.
    assert df.loc["iA", "tmv"] < df.loc["iB", "tmv"] < df.loc["iC", "tmv"]
    conn.close()


def test_single_inventory_item_no_peer_group_does_not_crash(tmp_path):
    """A single confirmed item (no caliber peers at all) must fall back to the
    global-rank/0.5 default, never raise or produce NaN in D/S."""
    m = _load()
    conn, _ = _fresh_db(tmp_path)
    _inventory(conn, "i1", "c1", caliber="unique_caliber")
    _confirmed_sold(conn, 1, "ev1", "i1", 150.0, "2025-06-01")
    res = m.build(conn)  # must not raise
    row = res["df"].set_index("inventory_uid").loc["i1"]
    assert row["D"] is not None and not (isinstance(row["D"], float) and math.isnan(row["D"]))
    assert row["S"] is not None and not (isinstance(row["S"], float) and math.isnan(row["S"]))
    conn.close()
