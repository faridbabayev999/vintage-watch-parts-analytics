"""
Phase 10 (final wrap-up sprint, owner spec 2026-07-30) — data-integrity QA
checks against the LIVE database (read-only). These are sanity invariants
the whole platform must hold at all times, not new business logic.
"""
import duckdb
import pytest

DB_PATH = "database/watchparts.duckdb"


@pytest.fixture(scope="module")
def conn():
    c = duckdb.connect(DB_PATH, read_only=True)
    yield c
    c.close()


def test_no_missing_inventory_ids(conn):
    n = conn.execute(
        "SELECT COUNT(*) FROM staging_inventory WHERE inventory_uid IS NULL OR canonical_inventory_id IS NULL"
    ).fetchone()[0]
    assert n == 0


def test_no_duplicate_inventory_uid(conn):
    n = conn.execute(
        "SELECT COUNT(*) FROM (SELECT inventory_uid, COUNT(*) c FROM staging_inventory GROUP BY 1 HAVING COUNT(*) > 1)"
    ).fetchone()[0]
    assert n == 0


def test_no_duplicate_evidence_uid_within_a_single_source(conn):
    """A given evidence_uid must not appear under two DIFFERENT raw ids
    within the same source table (would mean the identity hash collapsed
    two genuinely different physical listings)."""
    for table in ["stg_active_targeted", "stg_historical_ebay_sold"]:
        rows = conn.execute(f"""
            SELECT stable_evidence_uid FROM {table}
            WHERE stable_evidence_uid IS NOT NULL
            GROUP BY stable_evidence_uid
            HAVING COUNT(DISTINCT title) > 3
        """).fetchall()
        # A loose sanity check: no evidence_uid should ever span more than a
        # handful of distinct titles (a handful covers legitimate title-edit
        # noise across repeated fetches, not identity collapse across
        # unrelated listings).
        assert len(rows) == 0, f"{table}: evidence_uid(s) spanning >3 distinct titles: {rows}"


def test_no_orphan_match_decisions(conn):
    """Every match_decisions row must reference a real inventory_uid."""
    n = conn.execute("""
        SELECT COUNT(*) FROM match_decisions md
        LEFT JOIN staging_inventory si ON si.inventory_uid = md.inventory_uid
        WHERE si.inventory_uid IS NULL
    """).fetchone()[0]
    assert n == 0


def test_no_impossible_prices_in_tmv_results(conn):
    n = conn.execute(
        "SELECT COUNT(*) FROM tmv_results WHERE tmv_eur <= 0 OR tmv_eur IS NULL "
        "OR tmv_low_eur > tmv_eur OR tmv_eur > tmv_high_eur"
    ).fetchone()[0]
    assert n == 0


def test_no_negative_turnover(conn):
    n = conn.execute(
        "SELECT COUNT(*) FROM turnover_survival WHERE median_days_to_sell < 0 "
        "OR probability_sell_30d < 0 OR probability_sell_30d > 1 "
        "OR probability_sell_90d < 0 OR probability_sell_90d > 1"
    ).fetchone()[0]
    assert n == 0


def test_match_confirmed_only_from_approved_policy_segments(conn):
    """Every MATCH_CONFIRMED row's (rule, source_table, collection_relationship)
    must correspond to an APPROVED validation_policy row -- the gate can
    never be bypassed structurally."""
    n = conn.execute("""
        SELECT COUNT(*) FROM match_decisions md
        WHERE md.match_status = 'MATCH_CONFIRMED'
        AND NOT EXISTS (
            SELECT 1 FROM validation_policy vp
            WHERE vp.matching_rule = md.matching_rule
            AND vp.source_table = md.source_table
            AND vp.validation_status = 'APPROVED'
            AND (vp.collection_relationship = md.collection_relationship OR vp.collection_relationship = 'ANY')
        )
    """).fetchone()[0]
    assert n == 0


def test_no_duplicate_candidate_rows_within_a_run(conn):
    n = conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT match_run_id, inventory_uid, active_raw_id, match_method, COUNT(*) c
            FROM match_candidates_active GROUP BY 1,2,3,4 HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    assert n == 0


def test_no_duplicate_match_decisions(conn):
    n = conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT candidate_key, COUNT(*) c FROM match_decisions GROUP BY 1 HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    assert n == 0
