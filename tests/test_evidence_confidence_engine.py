"""
Evidence confidence engine tests (scripts/21_evidence_confidence_engine.py).
Pure classify() logic + DB integration on disposable DuckDB only.
"""
import importlib.util
import sys
from pathlib import Path

import duckdb
import pytest

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
SCHEMA = SCRIPTS / "schema.sql"


def _load():
    spec = importlib.util.spec_from_file_location("confeng", SCRIPTS / "21_evidence_confidence_engine.py")
    m = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(SCRIPTS))
    spec.loader.exec_module(m)
    return m


# ── classify(): pure logic ─────────────────────────────────────────────────

def test_auto_confirmed_requires_perfect_conjunction_not_just_high_score():
    m = _load()
    v2 = _load().matching_v2
    # perfect: exact part number, exact brand, WATCH_PART, zero negative keywords
    # ("caliber 3135" not "for caliber 3135" -- "for" is itself a negative keyword)
    res = v2.score_candidate("Rolex", "4419", "3135", "Genuine Rolex 4419 spring bar caliber 3135")
    tier, reason = m.classify(res, has_contradiction=False)
    assert tier == "AUTO_CONFIRMED"


def test_negative_keyword_blocks_auto_confirmed_even_with_exact_identity_match():
    m = _load()
    v2 = m.matching_v2
    # exact part number + exact brand, but "for" (negative keyword) present
    res = v2.score_candidate("Rolex", "4419", "3135", "Genuine Rolex 4419 spring bar for caliber 3135")
    tier, reason = m.classify(res, has_contradiction=False)
    assert tier != "AUTO_CONFIRMED"
    assert res["score"] < 1.0


def test_wrong_brand_is_rejected_regardless_of_other_scores():
    m = _load()
    v2 = m.matching_v2
    res = v2.score_candidate("Rolex", "4419", "3135", "Omega Seamaster 4419 spring bar")
    tier, reason = m.classify(res, has_contradiction=False)
    assert tier == "REJECTED"
    assert "brand" in reason.lower()


def test_explicit_contradiction_is_rejected_even_with_perfect_v2_score():
    m = _load()
    v2 = m.matching_v2
    res = v2.score_candidate("Rolex", "4419", "3135", "Genuine Rolex 4419 spring bar for caliber 3135")
    tier, reason = m.classify(res, has_contradiction=True)
    assert tier == "REJECTED"
    assert "contradiction" in reason.lower()


def test_medium_score_band():
    m = _load()
    v2 = m.matching_v2
    # component type accessory (tool) drags score down but not to REJECTED
    res = v2.score_candidate("Rolex", "530-0", "24", "Genuine Rolex winding tool 24-530-0 new")
    tier, reason = m.classify(res, has_contradiction=False)
    assert tier in ("MEDIUM_CONFIDENCE", "LOW_CONFIDENCE", "HIGH_CONFIDENCE")
    assert tier != "AUTO_CONFIRMED" and tier != "REJECTED"


def test_tier_thresholds_are_contiguous_and_ordered():
    m = _load()
    assert m.MEDIUM_CONFIDENCE_FLOOR < m.HIGH_CONFIDENCE_FLOOR < 1.0


# ── build()/write(): DB integration ────────────────────────────────────────

def _fresh_db(tmp_path, name="ce.duckdb"):
    db = tmp_path / name
    conn = duckdb.connect(str(db))
    conn.execute(SCHEMA.read_text())
    return conn, db


def _seed(conn, brand="Rolex", caliber="3135", part_number="4419", title="Genuine Rolex 4419 spring bar caliber 3135"):
    conn.execute("""INSERT INTO staging_inventory
        (inventory_uid, canonical_inventory_id, brand, caliber, part_number, stock, validation_status)
        VALUES ('i1','c1',?,?,?,1,'PASS')""", [brand, caliber, part_number])
    conn.execute("""INSERT INTO stg_active_targeted (id, normalized_title, price_eur, marketplace)
        VALUES (1, ?, 100.0, 'EBAY_DE')""", [title])
    conn.execute("""INSERT INTO match_decisions
        (decision_id, decision_version, decision_run_id, candidate_key, inventory_uid, source_table,
         source_id, matching_rule, evidence_tier, match_status, match_reason_code, collection_relationship,
         price_evidence_status, contradiction_flags)
        VALUES (1, 'v1', 'dr1', 'ck1', 'i1', 'match_candidates_active',
                1, 'PART_NUMBER_EXACT', 'A', 'REVIEW_REQUIRED', 'OK', 'NOT_APPLICABLE', 'NOT_APPLICABLE', NULL)""")


def test_build_classifies_clean_tier_a_candidate_as_auto_confirmed(tmp_path):
    m = _load()
    conn, db = _fresh_db(tmp_path)
    _seed(conn)
    df = m.build(conn)
    assert len(df) == 1
    assert df.iloc[0]["confidence_tier"] == "AUTO_CONFIRMED"
    conn.close()


def test_write_is_idempotent_no_duplicates_on_rerun(tmp_path):
    m = _load()
    conn, db = _fresh_db(tmp_path)
    _seed(conn)
    df = m.build(conn)
    m.write(conn, df, "run1")
    m.write(conn, df, "run1")  # rerun same run_id
    n = conn.execute("SELECT COUNT(*) FROM evidence_confidence_classification WHERE classification_run_id='run1'").fetchone()[0]
    assert n == 1
    conn.close()


def test_write_never_touches_validation_policy_or_match_decisions(tmp_path):
    """Governance boundary: this module must never write to the governed
    tables, under any circumstance."""
    m = _load()
    conn, db = _fresh_db(tmp_path)
    _seed(conn)
    vp_before = conn.execute("SELECT COUNT(*) FROM validation_policy").fetchone()[0]
    md_before = conn.execute("SELECT * FROM match_decisions").fetchall()
    df = m.build(conn)
    m.write(conn, df, "run1")
    vp_after = conn.execute("SELECT COUNT(*) FROM validation_policy").fetchone()[0]
    md_after = conn.execute("SELECT * FROM match_decisions").fetchall()
    assert vp_before == vp_after == 0
    assert md_before == md_after
    conn.close()


def test_contradiction_flagged_candidate_classified_rejected(tmp_path):
    m = _load()
    conn, db = _fresh_db(tmp_path)
    _seed(conn)
    conn.execute("UPDATE match_decisions SET contradiction_flags='brand_conflict' WHERE decision_id=1")
    df = m.build(conn)
    assert df.iloc[0]["confidence_tier"] == "REJECTED"
    conn.close()
