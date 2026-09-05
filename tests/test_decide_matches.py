"""
tests/test_decide_matches.py
==============================
Pytest tests for scripts/06_decide_matches.py — Module 5 deterministic
MATCHING-DECISION layer. Answers only "does this evidence row correspond
to this inventory item?" — no scoring, no TMV, no price eligibility.

Isolation: every test runs against a duckdb file under pytest's tmp_path —
never database/watchparts.duckdb. A module-scoped autouse fixture hashes
the real project database before/after and fails loudly if it changed.
"""

import hashlib
import importlib.util
import sys
from pathlib import Path

import duckdb
import pandas as pd
import pytest

TESTS_DIR = Path(__file__).parent
BASE_DIR = TESTS_DIR.parent
SCRIPTS_DIR = BASE_DIR / "scripts"
SCHEMA_PATH = SCRIPTS_DIR / "schema.sql"


def _load_module(name: str, path: Path):
    sys.path.insert(0, str(SCRIPTS_DIR))
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


m5 = _load_module("m5_candidates_for_decisions", SCRIPTS_DIR / "05_generate_match_candidates.py")
dec = _load_module("m6_decide_matches", SCRIPTS_DIR / "06_decide_matches.py")
import evidence_identity as ei  # noqa: E402 — SCRIPTS_DIR already on sys.path via _load_module


def _file_digest(path: Path):
    if not path.exists():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture(scope="module", autouse=True)
def guard_production_files_untouched():
    real_db = dec.DB_PATH
    before = _file_digest(real_db)
    yield
    after = _file_digest(real_db)
    assert before == after, "database/watchparts.duckdb changed — test isolation is broken"


def _seed_inventory(connection, rows: list[dict]) -> None:
    df = pd.DataFrame(rows)
    df["upload_batch_id"] = "batch_test1"
    df["source_filename"] = "inventory.csv"
    connection.register("tmp_seed", df)
    cols = ["canonical_inventory_id", "inventory_uid", "brand", "caliber", "part_number",
            "stock", "validation_status", "upload_batch_id", "source_filename"]
    connection.execute(f"INSERT INTO staging_inventory ({','.join(cols)}) SELECT {','.join(cols)} FROM tmp_seed")
    connection.unregister("tmp_seed")


def _seed_active(connection, rows: list[dict]) -> None:
    df = pd.DataFrame(rows)
    connection.register("tmp_seed", df)
    cols = list(df.columns)
    connection.execute(f"INSERT INTO stg_active_targeted ({','.join(cols)}) SELECT {','.join(cols)} FROM tmp_seed")
    connection.unregister("tmp_seed")


def _active_row(id_, title, inventory_uid=None, item_id="item1", marketplace="EBAY_DE"):
    return {
        "id": id_, "raw_id": id_, "inventory_uid": inventory_uid, "item_id": item_id,
        "title": title, "normalized_title": title.lower(), "marketplace": marketplace,
    }


def _inv(uid, brand="Rolex", caliber="3135", part_number="12345"):
    return dict(
        canonical_inventory_id=f"{brand}_{caliber}_{part_number}_{uid}", inventory_uid=uid,
        brand=brand, caliber=caliber, part_number=part_number, stock=1, validation_status="PASS",
    )


def _approve_segment(connection, *, matching_rule, source_table="match_candidates_active",
                      collection_relationship="ANY", version="test_v1", policy_id=1):
    """Inserts an isolated-fixture APPROVED validation_policy row -- the ONLY
    way any test (or real run) can get a MATCH_CONFIRMED decision, per the
    validation-policy gate (docs/MODULE5_VALIDATION_POLICY_ANALYSIS.md)."""
    connection.execute(
        """
        INSERT INTO validation_policy (
            validation_policy_id, confirmation_policy_version, validation_segment,
            matching_rule, source_table, collection_relationship, required_risk_profile,
            validation_status, policy_reason, approved_by, approved_at
        ) VALUES (?, ?, ?, ?, ?, ?, 'NO_CONTRADICTION_NO_RISK', 'APPROVED',
                  'isolated test fixture', 'test', current_timestamp)
        """,
        [policy_id, version, f"{matching_rule}|{source_table}|{collection_relationship}",
         matching_rule, source_table, collection_relationship],
    )


@pytest.fixture()
def conn(tmp_path):
    db_path = tmp_path / "test.duckdb"
    assert db_path.resolve() != dec.DB_PATH.resolve()
    connection = duckdb.connect(str(db_path))
    connection.execute(SCHEMA_PATH.read_text())
    yield connection
    connection.close()


# ── A. Contradiction flags -> NO_MATCH ──────────────────────────────────────

def test_brand_conflict_produces_no_match(conn):
    """Other brand present, own brand absent -> NO_MATCH, regardless of tier."""
    _seed_inventory(conn, [_inv("iuid_1", brand="Tudor", part_number="zz99999zz")])
    _seed_active(conn, [_active_row(1, "Rolex zz99999zz genuine part")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    row = df[(df["inventory_uid"] == "iuid_1") & (df["matching_rule"] == "PART_NUMBER_EXACT")].iloc[0]
    assert row["match_status"] == "NO_MATCH"
    assert row["match_reason_code"] == "BRAND_CONFLICT_OTHER_BRAND_PRESENT_OWN_ABSENT"
    assert "brand_conflict" in row["contradiction_flags"]


def test_brand_mentioned_together_is_not_a_conflict(conn):
    """Both brands mentioned (a cross-compatible listing) -> no brand_conflict."""
    _seed_inventory(conn, [_inv("iuid_1", brand="Rolex", part_number="zz99999zz")])
    _seed_active(conn, [_active_row(1, "Fits Rolex and Tudor zz99999zz genuine part")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    row = df[(df["inventory_uid"] == "iuid_1") & (df["matching_rule"] == "PART_NUMBER_EXACT")].iloc[0]
    assert pd.isna(row["contradiction_flags"]) or "brand_conflict" not in (row["contradiction_flags"] or "")


def test_calibre_conflict_produces_no_match(conn):
    """Explicit labelled caliber differs from the item's own -> NO_MATCH."""
    _seed_inventory(conn, [_inv("iuid_1", caliber="2130", part_number="50001")])
    _seed_active(conn, [_active_row(1, "Rolex Part 50001 Cal. 3135 Genuine")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    row = df[(df["inventory_uid"] == "iuid_1") & (df["matching_rule"] == "PART_NUMBER_EXACT")].iloc[0]
    assert row["match_status"] == "NO_MATCH"
    assert row["match_reason_code"] == "CALIBRE_CONFLICT_EXPLICIT_LABEL_MISMATCH"


def test_calibre_present_alongside_other_label_is_not_a_conflict(conn):
    """Own caliber IS present in the title (not absent+differing-labelled),
    so calibre_conflict never fires -- deterministic_checks_passed=TRUE
    confirms this candidate is technically clean; MATCH_CONFIRMED requires
    an APPROVED policy fixture on top of that (Phase 10 Test 2)."""
    _seed_inventory(conn, [_inv("iuid_1", caliber="3135", part_number="50001")])
    _seed_active(conn, [_active_row(1, "Rolex Part 50001 Cal. 3135 3130 3155 genuine")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    row = df[(df["inventory_uid"] == "iuid_1") & (df["matching_rule"] == "PART_NUMBER_EXACT")].iloc[0]
    assert pd.isna(row["contradiction_flags"]) or "calibre_conflict" not in (row["contradiction_flags"] or "")
    assert row["deterministic_checks_passed"] == True  # noqa: E712


def test_product_type_conflict_whole_watch(conn):
    _seed_inventory(conn, [_inv("iuid_1", part_number="114200")])
    _seed_active(conn, [_active_row(1, "Rolex Air King 114200 complete watch original box")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    row = df[(df["inventory_uid"] == "iuid_1") & (df["matching_rule"] == "PART_NUMBER_EXACT")].iloc[0]
    assert row["match_status"] == "NO_MATCH"
    assert row["match_reason_code"] == "PRODUCT_TYPE_CONFLICT_WHOLE_WATCH_SIGNAL"


# ── B. Risk flags -> REVIEW_REQUIRED (Tier A) ───────────────────────────────

def test_measurement_collision_on_caliber_side_of_rule3(conn):
    """The exact confirmed residual case: caliber matches only inside a
    millimeter dimension, part_number matches genuinely elsewhere."""
    _seed_inventory(conn, [_inv("iuid_1", caliber="25", part_number="104")])
    _seed_active(conn, [_active_row(
        1, "RLX Oysterdate Precision Mid Size 6466 Acrylic Cyclops 104 105 Glass 25.6mm"
    )])
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    row = df[(df["inventory_uid"] == "iuid_1") & (df["matching_rule"] == "CALIBER_PART_NUMBER")].iloc[0]
    assert row["match_status"] == "REVIEW_REQUIRED"
    assert row["match_reason_code"] == "MEASUREMENT_COLLISION_DIMENSION_PATTERN"
    assert "measurement_collision" in row["risk_flags"]


def test_clean_rule3_candidate_pending_policy_not_confirmed(conn):
    """A genuine adjacent compound-code RULE 3 match, no contradiction, no
    risk, but NO validation_policy row is APPROVED for this segment (the
    default, unmodified state) -> REVIEW_REQUIRED /
    AUTO_CONFIRM_POLICY_NOT_VALIDATED, with deterministic_checks_passed=TRUE
    preserving that this candidate is technically strong, just policy-gated.
    See test_clean_tier_a_approved_policy_match_confirmed for the APPROVED
    counterpart (docs/MODULE5_VALIDATION_POLICY_ANALYSIS.md Phase 1/7)."""
    _seed_inventory(conn, [_inv("iuid_1", caliber="24", part_number="530-0")])
    _seed_active(conn, [_active_row(1, "Genuine Swiss Rolex Crown 24-530-0 NOS Open Pack")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    row = df[(df["inventory_uid"] == "iuid_1") & (df["matching_rule"] == "CALIBER_PART_NUMBER")].iloc[0]
    assert row["match_status"] == "REVIEW_REQUIRED"
    assert row["match_reason_code"] == "AUTO_CONFIRM_POLICY_NOT_VALIDATED"
    assert pd.isna(row["contradiction_flags"])
    assert pd.isna(row["risk_flags"])
    assert row["deterministic_checks_passed"] == True  # noqa: E712 -- explicit bool check, not truthiness
    assert row["confirmation_policy_reason"] == "VALIDATION_PENDING"


def test_clean_tier_a_approved_policy_match_confirmed(conn):
    """Phase 10 Test 2: the SAME clean RULE 3 candidate as above, but with
    an isolated-fixture APPROVED validation_policy row for its exact
    segment -> MATCH_CONFIRMED. This is the ONLY way MATCH_CONFIRMED can be
    produced anywhere in this codebase."""
    _seed_inventory(conn, [_inv("iuid_1", caliber="24", part_number="530-0")])
    _seed_active(conn, [_active_row(1, "Genuine Swiss Rolex Crown 24-530-0 NOS Open Pack")])
    _approve_segment(conn, matching_rule="CALIBER_PART_NUMBER")
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    row = df[(df["inventory_uid"] == "iuid_1") & (df["matching_rule"] == "CALIBER_PART_NUMBER")].iloc[0]
    assert row["match_status"] == "MATCH_CONFIRMED"
    assert row["match_reason_code"] == "TIER_A_CLEAN_NO_CONTRADICTION_NO_RISK"
    assert row["deterministic_checks_passed"] == True  # noqa: E712
    assert row["confirmation_policy_reason"] == "APPROVED"
    assert row["confirmation_policy_version"] == "test_v1"


def test_approved_policy_but_v2_verification_fails_stays_review_required(conn):
    """Matching v2 structural safeguard (docs/MATCHING_AUTOMATION_
    IMPLEMENTATION_REPORT.md §11): even with an APPROVED validation_policy
    segment, a candidate whose LISTING CONTENT scores below the v2
    threshold (here: a 'compatible'/'replacement' listing that also isn't
    classified WATCH_PART by the reused G2 classifier) must NOT reach
    MATCH_CONFIRMED. This is the exact structural fix for the Implementation
    B failure mode -- an approved RULE never bypasses per-listing
    verification."""
    _seed_inventory(conn, [_inv("iuid_1", caliber="24", part_number="530-0")])
    # Clean under every v1 contradiction/risk detector (no brand conflict, no
    # calibre conflict, no bundle/lot language, no "for"/"fits"/"compatible"
    # compatibility wording) -- so v1 alone would call this Tier A clean.
    # "tool" classifies as G2 ACCESSORY (component_type_match=0 under v2),
    # which is the ONLY thing that should block MATCH_CONFIRMED here.
    _seed_active(conn, [_active_row(1, "Genuine Rolex winding tool 24-530-0 new")])
    _approve_segment(conn, matching_rule="CALIBER_PART_NUMBER")
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    row = df[(df["inventory_uid"] == "iuid_1") & (df["matching_rule"] == "CALIBER_PART_NUMBER")].iloc[0]
    assert row["match_status"] == "REVIEW_REQUIRED"
    assert row["match_reason_code"] == "MATCHING_V2_SCORE_BELOW_THRESHOLD"
    assert row["confirmation_policy_reason"] == "APPROVED"  # policy WAS approved -- v2 is what blocked it
    assert row["deterministic_checks_passed"] == True  # noqa: E712 -- v1 rules were clean; v2 is the new layer


def test_explicit_contradiction_overrides_approved_policy(conn):
    """Phase 10 Test 3: an APPROVED segment never overrides a contradiction
    -- brand_conflict still wins, unconditionally."""
    _seed_inventory(conn, [_inv("iuid_1", brand="Tudor", part_number="zz99999zz")])
    _seed_active(conn, [_active_row(1, "Rolex zz99999zz genuine part")])
    _approve_segment(conn, matching_rule="PART_NUMBER_EXACT")
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    row = df[(df["inventory_uid"] == "iuid_1") & (df["matching_rule"] == "PART_NUMBER_EXACT")].iloc[0]
    assert row["match_status"] == "NO_MATCH"
    assert row["match_reason_code"] == "BRAND_CONFLICT_OTHER_BRAND_PRESENT_OWN_ABSENT"


def test_model_name_number_collision_short_token(conn):
    _seed_inventory(conn, [_inv("iuid_1", part_number="2")])  # 1 alnum char -- below RULE1/2's >=5 floor
    # part_number="2" is not distinctive, so only test via a rule that doesn't
    # require distinctiveness could fire it -- but PART_NUMBER_EXACT requires
    # distinctiveness, so seed a distinctive-but-still-short-for-model-collision
    # scenario isn't possible for Tier A part-number rules by construction.
    # Verify instead that no PART_NUMBER_EXACT/BRAND_PART_NUMBER candidate is
    # even generated for a non-distinctive part number (confirms the guard,
    # rather than the model-name detector directly, which is exercised via
    # CALIBER_PART_NUMBER's caliber-side check in the next test).
    _seed_active(conn, [_active_row(1, "Saphirglas Rolex GMT Master 2 genuine")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    assert df.empty or "PART_NUMBER_EXACT" not in set(df["matching_rule"])


def test_bundle_or_lot_risk(conn):
    _seed_inventory(conn, [_inv("iuid_1", part_number="90002")])
    _seed_active(conn, [_active_row(1, "Rolex 3135 90002 Job Lot bundle x3")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    row = df[(df["inventory_uid"] == "iuid_1") & (df["matching_rule"] == "PART_NUMBER_EXACT")].iloc[0]
    assert row["match_status"] == "REVIEW_REQUIRED"
    assert row["match_reason_code"] == "BUNDLE_OR_LOT_LANGUAGE_PRESENT"


def test_multiple_inventory_collision_each_item_independent(conn):
    """Same evidence row matched to 2 inventory items -> each gets its OWN
    decision, both flagged, neither decision inferred from the other."""
    _seed_inventory(conn, [
        _inv("iuid_a", caliber="3135", part_number="11111"),
        _inv("iuid_b", caliber="3135", part_number="22222"),
    ])
    _seed_active(conn, [_active_row(1, "Rolex 3135 generic movement")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    caliber_rows = df[(df["source_id"] == 1) & (df["matching_rule"] == "CALIBER_EXACT")]
    assert set(caliber_rows["inventory_uid"]) == {"iuid_a", "iuid_b"}
    for _, r in caliber_rows.iterrows():
        assert "multiple_inventory_collision" in r["risk_flags"]
        # v1.1: calibre-only (Tier B) never MATCH_CONFIRMED; routes to
        # LOW_CONFIDENCE_CANDIDATE (or INSUFFICIENT_EVIDENCE for short caliber).
        assert r["match_status"] in ("LOW_CONFIDENCE_CANDIDATE", "INSUFFICIENT_EVIDENCE")


# ── C. Tier defaults ─────────────────────────────────────────────────────

def test_tier_b_brand_caliber_low_confidence_candidate(conn):
    # v1.1: BRAND_CALIBER (calibre-only, ~0% adjudicated precision) routes to
    # LOW_CONFIDENCE_CANDIDATE, out of the human review queue, not deleted.
    _seed_inventory(conn, [_inv("iuid_1", brand="Rolex", caliber="3135", part_number="99999zz")])
    _seed_active(conn, [_active_row(1, "Rolex 3135 vintage movement")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    row = df[(df["inventory_uid"] == "iuid_1") & (df["matching_rule"] == "BRAND_CALIBER")].iloc[0]
    assert row["match_status"] == "LOW_CONFIDENCE_CANDIDATE"
    assert row["evidence_tier"] == "B"


def test_tier_b_caliber_exact_short_caliber_insufficient(conn):
    _seed_inventory(conn, [_inv("iuid_1", brand="Tudor", caliber="24", part_number="99999zz")])
    _seed_active(conn, [_active_row(1, "Tudor 24 vintage movement genuine")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    row = df[(df["inventory_uid"] == "iuid_1") & (df["matching_rule"] == "CALIBER_EXACT")].iloc[0]
    assert row["match_status"] == "INSUFFICIENT_EVIDENCE"
    assert row["match_reason_code"] == "TIER_B_CALIBER_ONLY_SHORT_IDENTIFIER"


def test_tier_b_never_match_confirmed(conn):
    """No code path can produce MATCH_CONFIRMED for a Tier B rule."""
    _seed_inventory(conn, [_inv("iuid_1", brand="Rolex", caliber="3135", part_number="99999zz")])
    _seed_active(conn, [_active_row(1, "Rolex 3135 vintage movement genuine original")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    tier_b = df[df["evidence_tier"] == "B"]
    assert not tier_b.empty
    assert "MATCH_CONFIRMED" not in set(tier_b["match_status"])


def test_tier_c_default_insufficient_evidence(conn):
    _seed_inventory(conn, [_inv("iuid_1", brand="Tudor", caliber="24", part_number="99999zz")])
    _seed_active(conn, [_active_row(1, "Tudor 24 crown genuine")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    row = df[(df["inventory_uid"] == "iuid_1") & (df["matching_rule"] == "CALIBER_COMPONENT")].iloc[0]
    assert row["match_status"] == "INSUFFICIENT_EVIDENCE"


def test_tier_c_high_value_edge_case_low_confidence_candidate(conn):
    # v1.1: BRAND_CALIBER_COMPONENT (0/8 adjudicated) provisionally demoted to
    # LOW_CONFIDENCE_CANDIDATE — out of review queue, retained for re-evaluation.
    _seed_inventory(conn, [_inv("iuid_1", brand="Rolex", caliber="3135", part_number="99999zz")])
    _seed_active(conn, [_active_row(1, "Rolex 3135 crown genuine original")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    row = df[(df["inventory_uid"] == "iuid_1") & (df["matching_rule"] == "BRAND_CALIBER_COMPONENT")].iloc[0]
    assert row["match_status"] == "LOW_CONFIDENCE_CANDIDATE"
    assert row["match_reason_code"] == "TIER_C_HIGH_VALUE_EDGE_CASE"


def test_tier_c_never_match_confirmed(conn):
    _seed_inventory(conn, [_inv("iuid_1", brand="Rolex", caliber="3135", part_number="99999zz")])
    _seed_active(conn, [_active_row(1, "Rolex 3135 crown genuine original")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    tier_c = df[df["evidence_tier"] == "C"]
    assert not tier_c.empty
    assert "MATCH_CONFIRMED" not in set(tier_c["match_status"])


# ── D. Collection relationship (SELF_SOURCED / CROSS_REFERENCED / NOT_APPLICABLE) ──

def test_self_sourced_active_evidence(conn):
    _seed_inventory(conn, [_inv("iuid_1", part_number="12345")])
    _seed_active(conn, [_active_row(1, "Rolex 3135 bridge 12345 genuine", inventory_uid="iuid_1")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    row = df[(df["inventory_uid"] == "iuid_1") & (df["matching_rule"] == "PART_NUMBER_EXACT")].iloc[0]
    assert row["collection_relationship"] == "SELF_SOURCED"


def test_cross_referenced_active_evidence(conn):
    """Evidence collected FOR a different item, matched via rule to this one."""
    _seed_inventory(conn, [_inv("iuid_1", part_number="12345")])
    _seed_active(conn, [_active_row(1, "Rolex 3135 bridge 12345 genuine", inventory_uid="iuid_OTHER")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    row = df[(df["inventory_uid"] == "iuid_1") & (df["matching_rule"] == "PART_NUMBER_EXACT")].iloc[0]
    assert row["collection_relationship"] == "CROSS_REFERENCED"


def test_vcp_source_collection_relationship_not_applicable(conn):
    """VCP has no per-item collection concept -- must never be SELF_SOURCED/
    CROSS_REFERENCED, unconditionally."""
    _seed_inventory(conn, [_inv("iuid_1", part_number="12345")])
    conn.execute("""
        INSERT INTO stg_historical_vcp_aggregate (id, raw_id, title, normalized_title, avg_price_eur)
        VALUES (1, 1, 'Rolex 3135 bridge 12345 vintage', 'rolex 3135 bridge 12345 vintage', 100.0)
    """)
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    row = df[(df["inventory_uid"] == "iuid_1") & (df["source_table"] == "match_candidates_vcp")].iloc[0]
    assert row["collection_relationship"] == "NOT_APPLICABLE"


# ── E. price_evidence_status structural enforcement ─────────────────────────

def test_price_evidence_status_not_applicable_for_non_confirmed(conn):
    _seed_inventory(conn, [_inv("iuid_1", brand="Rolex", caliber="3135", part_number="99999zz")])
    _seed_active(conn, [_active_row(1, "Rolex 3135 vintage movement")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    non_confirmed = df[df["match_status"] != "MATCH_CONFIRMED"]
    assert not non_confirmed.empty
    assert set(non_confirmed["price_evidence_status"]) == {"NOT_APPLICABLE"}


def test_price_evidence_status_not_applicable_for_confirmed_too_in_this_task(conn):
    """Real price-eligibility rules are out of scope this task -- even
    MATCH_CONFIRMED rows get the structural placeholder, never a real
    status. Requires an APPROVED fixture segment, since MATCH_CONFIRMED
    can no longer occur otherwise (Phase 10 Test 11)."""
    _seed_inventory(conn, [_inv("iuid_1", part_number="12345")])
    _seed_active(conn, [_active_row(1, "Rolex 3135 bridge 12345 genuine")])
    _approve_segment(conn, matching_rule="PART_NUMBER_EXACT")
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    confirmed = df[df["match_status"] == "MATCH_CONFIRMED"]
    assert not confirmed.empty
    assert set(confirmed["price_evidence_status"]) == {"NOT_APPLICABLE"}


# ── F. Candidate key stability / idempotency / determinism ──────────────────

def test_candidate_key_stable_across_reruns(conn):
    _seed_inventory(conn, [_inv("iuid_1", part_number="12345")])
    _seed_active(conn, [_active_row(1, "Rolex 3135 bridge 12345 genuine")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    df1 = dec.build_match_decisions(conn, decision_run_id="d1")
    key1 = df1[df1["matching_rule"] == "PART_NUMBER_EXACT"]["candidate_key"].iloc[0]
    m5.run_candidate_generation(conn, match_run_id="run2")  # rediscover same evidence under a new run
    df2 = dec.build_match_decisions(conn, decision_run_id="d2")
    key2 = df2[df2["matching_rule"] == "PART_NUMBER_EXACT"]["candidate_key"].iloc[0]
    assert key1 == key2
    # rediscovery under a second match_run_id must NOT create a second decision row
    assert len(df2[df2["matching_rule"] == "PART_NUMBER_EXACT"]) == 1


def test_write_match_decisions_idempotent_full_rebuild(conn):
    _seed_inventory(conn, [_inv("iuid_1", part_number="12345")])
    _seed_active(conn, [_active_row(1, "Rolex 3135 bridge 12345 genuine")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    dec.write_match_decisions(conn, df)
    n1 = conn.execute("SELECT COUNT(*) FROM match_decisions").fetchone()[0]
    dec.write_match_decisions(conn, df)  # rerun the write
    n2 = conn.execute("SELECT COUNT(*) FROM match_decisions").fetchone()[0]
    assert n1 == n2


def test_decision_content_deterministic_across_separate_computations(conn):
    _seed_inventory(conn, [_inv("iuid_1", part_number="12345"), _inv("iuid_2", caliber="2824", part_number="678")])
    _seed_active(conn, [_active_row(1, "Rolex 3135 bridge 12345 genuine"), _active_row(2, "Rolex 2824 crown 678")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    df1 = dec.build_match_decisions(conn, decision_run_id="d1")
    df2 = dec.build_match_decisions(conn, decision_run_id="d2")
    cols = [c for c in df1.columns if c not in ("decision_run_id",)]
    pd.testing.assert_frame_equal(
        df1[cols].sort_values("candidate_key").reset_index(drop=True),
        df2[cols].sort_values("candidate_key").reset_index(drop=True),
    )


# ── G. Raw/staging/candidate immutability ────────────────────────────────────

def test_no_raw_staging_or_candidate_tables_modified(conn):
    _seed_inventory(conn, [_inv("iuid_1", part_number="12345")])
    _seed_active(conn, [_active_row(1, "Rolex 3135 bridge 12345 genuine")])
    m5.run_candidate_generation(conn, match_run_id="run1")

    before_inv = conn.execute("SELECT * FROM staging_inventory ORDER BY inventory_uid").fetchall()
    before_active = conn.execute("SELECT * FROM stg_active_targeted ORDER BY id").fetchall()
    before_candidates = conn.execute("SELECT * FROM match_candidates_active ORDER BY match_candidate_id").fetchall()

    dec.run_decision_layer(conn, decision_run_id="d1")

    after_inv = conn.execute("SELECT * FROM staging_inventory ORDER BY inventory_uid").fetchall()
    after_active = conn.execute("SELECT * FROM stg_active_targeted ORDER BY id").fetchall()
    after_candidates = conn.execute("SELECT * FROM match_candidates_active ORDER BY match_candidate_id").fetchall()

    assert before_inv == after_inv
    assert before_active == after_active
    assert before_candidates == after_candidates


# ── H. inventory_match_summary derivation ────────────────────────────────────

def test_summary_has_confirmed_match(conn):
    """Phase 10 Test 10 (HAS_CONFIRMED_MATCH slice): requires an isolated
    APPROVED fixture segment, since HAS_CONFIRMED_MATCH can only be derived
    from an actual MATCH_CONFIRMED decision."""
    _seed_inventory(conn, [_inv("iuid_1", part_number="12345")])
    _seed_active(conn, [_active_row(1, "Rolex 3135 bridge 12345 genuine")])
    _approve_segment(conn, matching_rule="PART_NUMBER_EXACT")
    m5.run_candidate_generation(conn, match_run_id="run1")
    dec.run_decision_layer(conn, decision_run_id="d1")
    row = conn.execute(
        "SELECT inventory_match_status FROM inventory_match_summary WHERE inventory_uid='iuid_1'"
    ).fetchone()
    assert row[0] == "HAS_CONFIRMED_MATCH"


def test_summary_no_candidates_not_confused_with_all_rejected(conn):
    """NO_CANDIDATES (zero evidence) and ALL_CANDIDATES_REJECTED (evidence
    existed, was disproved) must never collapse into one status."""
    _seed_inventory(conn, [
        _inv("iuid_none", brand="Rolex", caliber="9999zz", part_number="zzzz1234zzzz"),  # no matching evidence at all
        _inv("iuid_rejected", brand="Tudor", part_number="zz99999zz"),  # evidence exists, contradicts
    ])
    _seed_active(conn, [
        _active_row(1, "Rolex zz99999zz genuine part", item_id="i1"),  # matches iuid_rejected's part number, but says Rolex not Tudor
    ])
    m5.run_candidate_generation(conn, match_run_id="run1")
    dec.run_decision_layer(conn, decision_run_id="d1")
    rows = dict(conn.execute(
        "SELECT inventory_uid, inventory_match_status FROM inventory_match_summary "
        "WHERE inventory_uid IN ('iuid_none','iuid_rejected')"
    ).fetchall())
    assert rows["iuid_none"] == "NO_CANDIDATES"
    assert rows["iuid_rejected"] == "ALL_CANDIDATES_REJECTED"
    assert rows["iuid_none"] != rows["iuid_rejected"]


def test_summary_review_pending(conn):
    # REVIEW_PENDING requires a Tier A (part-number) candidate in REVIEW_REQUIRED
    # (policy not APPROVED). Post-v1.1, calibre-only rules alone no longer yield
    # REVIEW_PENDING (they are LOW_CONFIDENCE_CANDIDATE) — see
    # test_summary_only_low_confidence_candidates.
    _seed_inventory(conn, [_inv("iuid_1", brand="Rolex", caliber="3135", part_number="7788XY")])
    _seed_active(conn, [_active_row(1, "Rolex 3135 bridge 7788XY genuine")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    dec.run_decision_layer(conn, decision_run_id="d1")
    row = conn.execute(
        "SELECT inventory_match_status FROM inventory_match_summary WHERE inventory_uid='iuid_1'"
    ).fetchone()
    assert row[0] == "REVIEW_PENDING"


def test_summary_only_low_confidence_candidates(conn):
    # v1.1: an item whose only candidates are calibre-only (Tier B/C) is
    # ONLY_LOW_CONFIDENCE_CANDIDATES — out of the review queue, not rejected.
    _seed_inventory(conn, [_inv("iuid_1", brand="Rolex", caliber="3135", part_number="99999zz")])
    _seed_active(conn, [_active_row(1, "Rolex 3135 vintage movement")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    dec.run_decision_layer(conn, decision_run_id="d1")
    row = conn.execute(
        "SELECT inventory_match_status FROM inventory_match_summary WHERE inventory_uid='iuid_1'"
    ).fetchone()
    assert row[0] == "ONLY_LOW_CONFIDENCE_CANDIDATES"


def test_summary_only_insufficient_evidence(conn):
    """No brand in the title -> BRAND_CALIBER never fires (which is
    unconditionally REVIEW_REQUIRED by design), isolating the case where
    every candidate is the weakest measured Tier B/C combination."""
    _seed_inventory(conn, [_inv("iuid_1", brand="Tudor", caliber="24", part_number="99999zz")])
    _seed_active(conn, [_active_row(1, "Vintage 24 crown genuine part")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    dec.run_decision_layer(conn, decision_run_id="d1")
    row = conn.execute(
        "SELECT inventory_match_status FROM inventory_match_summary WHERE inventory_uid='iuid_1'"
    ).fetchone()
    assert row[0] == "ONLY_INSUFFICIENT_EVIDENCE"


def test_summary_counts_are_derived_not_independent(conn):
    _seed_inventory(conn, [_inv("iuid_1", part_number="12345")])
    _seed_active(conn, [_active_row(1, "Rolex 3135 bridge 12345 genuine")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    dec.run_decision_layer(conn, decision_run_id="d1")
    row = conn.execute("""
        SELECT confirmed_candidate_count, review_candidate_count, low_confidence_candidate_count,
               insufficient_candidate_count, rejected_candidate_count, total_candidate_count
        FROM inventory_match_summary WHERE inventory_uid='iuid_1'
    """).fetchone()
    confirmed, review, low_conf, insufficient, rejected, total = row
    assert total == confirmed + review + low_conf + insufficient + rejected
    n_decisions = conn.execute(
        "SELECT COUNT(*) FROM match_decisions WHERE inventory_uid='iuid_1'"
    ).fetchone()[0]
    assert total == n_decisions


# ── I. Validation-policy gate, reference-list/compatibility detectors, ──────
#     calibre-compatibility governance (docs/MODULE5_VALIDATION_POLICY_ANALYSIS.md)

def test_reference_list_ambiguity_review_required(conn):
    """Phase 10 Test 4. The exact analysed example: part_number '114200'
    matching inside a Rolex Air King watch-reference-compatibility list is
    NOT a distinct catalog code -- REVIEW_REQUIRED, never silently confirmed,
    regardless of validation-policy state."""
    _seed_inventory(conn, [_inv("iuid_1", caliber="316", part_number="114200")])
    _seed_active(conn, [_active_row(1, "Rolex Air King Silver Tritium Dial 14000/14010/114200/114234")])
    _approve_segment(conn, matching_rule="PART_NUMBER_EXACT")  # even APPROVED must not confirm this
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    row = df[(df["inventory_uid"] == "iuid_1") & (df["matching_rule"] == "PART_NUMBER_EXACT")].iloc[0]
    assert row["match_status"] == "REVIEW_REQUIRED"
    assert row["match_reason_code"] == "REFERENCE_OR_COMPATIBILITY_LIST_AMBIGUITY"
    assert "multiple_reference_list_risk" in row["risk_flags"]


def test_multiple_calibre_list_ambiguity_review_required(conn):
    """Phase 10 Test 5. A caliber label followed by a genuine comma-separated
    list of 2+ distinct calibers (a caliber-family compatibility list, e.g.
    'Kaliber: 3155, 3156, 3175') co-occurring with calibre_conflict downgrades
    the contradiction to REVIEW_REQUIRED / UNVERIFIED_CALIBRE_COMPATIBILITY
    rather than a hard NO_MATCH -- never straight to MATCH_CONFIRMED."""
    _seed_inventory(conn, [_inv("iuid_1", caliber="3130", part_number="3135-510")])
    _seed_active(conn, [_active_row(
        1, "ROLEX 3135 Fuehrungsrad der Spule Cod. 3135-510 Kaliber: 3155, 3156, 3175"
    )])
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    row = df[(df["inventory_uid"] == "iuid_1") & (df["matching_rule"] == "BRAND_PART_NUMBER")].iloc[0]
    assert row["match_status"] == "REVIEW_REQUIRED"
    assert pd.isna(row["contradiction_flags"]) or "calibre_conflict" not in (row["contradiction_flags"] or "")
    assert "multiple_calibre_list_risk" in row["risk_flags"]
    assert "unverified_calibre_compatibility" in row["risk_flags"]


def test_genuine_compound_part_number_not_flagged_as_reference_list(conn):
    """Phase 10 Test 6. A genuine hyphenated catalog code (e.g. '3135-250')
    must NOT be flagged as a reference list, even though a caliber-family
    list is present elsewhere in the same title -- the matched token itself
    must be a bare list member, not merely 'some list exists in this title'
    (docs/MODULE5_VALIDATION_POLICY_ANALYSIS.md Task 5/8, correcting the
    original over-broad structural check)."""
    _seed_inventory(conn, [_inv("iuid_1", caliber="3135", part_number="3135-250")])
    _seed_active(conn, [_active_row(
        1, "ROLEX 3135 Setting Wheel Cod. 3135-250 Calib: 3130, 3135, 3136, 3155, 3156"
    )])
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    row = df[(df["inventory_uid"] == "iuid_1") & (df["matching_rule"] == "PART_NUMBER_EXACT")].iloc[0]
    assert pd.isna(row["risk_flags"]) or "multiple_reference_list_risk" not in (row["risk_flags"] or "")


def test_caliber_part_number_still_applies_three_alphanumeric_safeguard(conn):
    """Phase 10 Test 7. The approved >=3-alphanumeric part-number floor
    (committed 632d079) is untouched by this task -- a part_number below
    that floor still produces NO CALIBER_PART_NUMBER candidate at all."""
    _seed_inventory(conn, [_inv("iuid_1", caliber="25", part_number="19")])  # 2 alnum chars
    _seed_active(conn, [_active_row(1, "Rolex 25-19 genuine vintage part")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    assert df.empty or "CALIBER_PART_NUMBER" not in set(df["matching_rule"])


def test_unverified_calibre_compatibility_review_required(conn):
    """Phase 10 Test 8. Same as test_multiple_calibre_list_ambiguity_review_required,
    phrased against the specific reason code -- unresolved calibre
    compatibility must route to REVIEW_REQUIRED, never NO_MATCH (silently
    rejecting real evidence) and never MATCH_CONFIRMED (silently trusting
    unverified reviewer inference)."""
    _seed_inventory(conn, [_inv("iuid_1", caliber="3130", part_number="3135-510")])
    _seed_active(conn, [_active_row(
        1, "ROLEX 3135 Fuehrungsrad der Spule Cod. 3135-510 Kaliber: 3155, 3156, 3175"
    )])
    _approve_segment(conn, matching_rule="BRAND_PART_NUMBER")  # even APPROVED must not confirm this
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    row = df[(df["inventory_uid"] == "iuid_1") & (df["matching_rule"] == "BRAND_PART_NUMBER")].iloc[0]
    assert row["match_status"] == "REVIEW_REQUIRED"
    assert row["match_status"] != "NO_MATCH"
    assert row["match_status"] != "MATCH_CONFIRMED"


def test_zero_match_confirmed_valid_when_no_approved_policy(conn):
    """Phase 10 Test 9. With no validation_policy row APPROVED anywhere
    (the default, unmodified state), zero MATCH_CONFIRMED rows across a
    varied population is a VALID outcome, not a failure -- REVIEW_REQUIRED/
    AUTO_CONFIRM_POLICY_NOT_VALIDATED absorbs every technically-clean
    Tier A candidate instead."""
    _seed_inventory(conn, [
        _inv("iuid_1", part_number="12345"),
        _inv("iuid_2", caliber="24", part_number="530-0"),
        _inv("iuid_3", brand="Tudor", caliber="2824", part_number="99999zz"),
    ])
    _seed_active(conn, [
        _active_row(1, "Rolex 3135 bridge 12345 genuine", item_id="i1"),
        _active_row(2, "Genuine Swiss Rolex Crown 24-530-0 NOS Open Pack", item_id="i2"),
        _active_row(3, "Tudor 2824 vintage movement genuine", item_id="i3"),
    ])
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    assert not df.empty
    assert "MATCH_CONFIRMED" not in set(df["match_status"])
    tier_a_clean = df[(df["evidence_tier"] == "A") & (df["deterministic_checks_passed"] == True)]  # noqa: E712
    assert not tier_a_clean.empty
    assert set(tier_a_clean["match_reason_code"]) == {"AUTO_CONFIRM_POLICY_NOT_VALIDATED"}


def test_reference_list_risk_precedence_over_policy_pending(conn):
    """Phase 10 Test 13. A candidate with BOTH reference-list ambiguity AND
    a VALIDATION_PENDING segment gets the reference-list reason as its
    PRIMARY match_reason_code (never masked by
    AUTO_CONFIRM_POLICY_NOT_VALIDATED), while confirmation_policy_reason
    still separately records the policy state that was consulted."""
    _seed_inventory(conn, [_inv("iuid_1", caliber="316", part_number="114200")])
    _seed_active(conn, [_active_row(1, "Rolex Air King Silver Tritium Dial 14000/14010/114200/114234")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    row = df[(df["inventory_uid"] == "iuid_1") & (df["matching_rule"] == "PART_NUMBER_EXACT")].iloc[0]
    assert row["match_reason_code"] == "REFERENCE_OR_COMPATIBILITY_LIST_AMBIGUITY"
    assert row["match_reason_code"] != "AUTO_CONFIRM_POLICY_NOT_VALIDATED"
    assert row["confirmation_policy_reason"] == "VALIDATION_PENDING"


def test_verified_reference_without_policy_authorization_stays_review_required(conn):
    """Phase 10 Test 14. A Layer 1 VERIFIED_* ref_calibre_compatibility row
    with NO matching Layer 2 compatibility_policy_authorization row must
    NOT suppress calibre_conflict -- verification alone is not enough."""
    _seed_inventory(conn, [_inv("iuid_1", caliber="3130", part_number="3135-410")])
    _seed_active(conn, [_active_row(1, "Original Rolex Kaliber 3135-410 Hemmungsrad")])
    conn.execute("""
        INSERT INTO ref_calibre_compatibility (
            compatibility_id, brand, inventory_calibre, evidence_calibre, relationship_type,
            source_reference, source_quality, verification_status, verification_version,
            verified_by, verified_at
        ) VALUES (1, 'Rolex', '3130', '3135', 'SAME_FAMILY_SHARED_PARTS', 'test fixture citation',
                  'MANUFACTURER_SERVICE_DOCUMENT', 'VERIFIED_EXTERNAL_REFERENCE', 'v1', 'test',
                  current_timestamp)
    """)
    # deliberately NO compatibility_policy_authorization row inserted
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    row = df[(df["inventory_uid"] == "iuid_1") & (df["matching_rule"] == "BRAND_PART_NUMBER")].iloc[0]
    assert row["match_status"] != "MATCH_CONFIRMED"
    assert "calibre_conflict" in (row["contradiction_flags"] or "")
    assert row["match_status"] == "NO_MATCH"


def test_policy_authorization_without_verified_reference_stays_review_required(conn):
    """Phase 10 Test 15. A Layer 2 compatibility_policy_authorization row
    with NO corresponding Layer 1 VERIFIED_* ref_calibre_compatibility row
    must NOT suppress calibre_conflict -- authorization alone is not
    enough; there is nothing verified for it to authorize."""
    _seed_inventory(conn, [_inv("iuid_1", caliber="3130", part_number="3135-410")])
    _seed_active(conn, [_active_row(1, "Original Rolex Kaliber 3135-410 Hemmungsrad")])
    conn.execute("""
        INSERT INTO compatibility_policy_authorization (
            authorization_id, confirmation_policy_version, relationship_type,
            accepted_verification_status, acceptable_source_quality, brand_limitation
        ) VALUES (1, 'test_v1', 'SAME_FAMILY_SHARED_PARTS', 'VERIFIED_EXTERNAL_REFERENCE',
                  'MANUFACTURER_SERVICE_DOCUMENT', 'Rolex')
    """)
    # deliberately NO ref_calibre_compatibility row inserted
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    row = df[(df["inventory_uid"] == "iuid_1") & (df["matching_rule"] == "BRAND_PART_NUMBER")].iloc[0]
    assert row["match_status"] == "NO_MATCH"
    assert "calibre_conflict" in (row["contradiction_flags"] or "")


def test_verified_and_authorized_plus_approved_policy_match_confirmed(conn):
    """Phase 10 Test 16. Only when BOTH governance layers agree (verified
    reference AND explicit policy authorization) AND the segment's own
    validation_policy is APPROVED does the candidate reach MATCH_CONFIRMED
    -- proving suppression happens ONLY in this fully-isolated fixture,
    never from either signal alone (see the two negative tests above), and
    that all other safety checks (the policy gate itself) still apply on
    top of the compatibility suppression."""
    _seed_inventory(conn, [_inv("iuid_1", caliber="3130", part_number="3135-410")])
    _seed_active(conn, [_active_row(1, "Original Rolex Kaliber 3135-410 Hemmungsrad")])
    conn.execute("""
        INSERT INTO ref_calibre_compatibility (
            compatibility_id, brand, inventory_calibre, evidence_calibre, relationship_type,
            source_reference, source_quality, verification_status, verification_version,
            verified_by, verified_at
        ) VALUES (1, 'Rolex', '3130', '3135', 'SAME_FAMILY_SHARED_PARTS', 'test fixture citation',
                  'MANUFACTURER_SERVICE_DOCUMENT', 'VERIFIED_EXTERNAL_REFERENCE', 'v1', 'test',
                  current_timestamp)
    """)
    conn.execute("""
        INSERT INTO compatibility_policy_authorization (
            authorization_id, confirmation_policy_version, relationship_type,
            accepted_verification_status, acceptable_source_quality, brand_limitation
        ) VALUES (1, 'test_v1', 'SAME_FAMILY_SHARED_PARTS', 'VERIFIED_EXTERNAL_REFERENCE',
                  'MANUFACTURER_SERVICE_DOCUMENT', 'Rolex')
    """)
    _approve_segment(conn, matching_rule="BRAND_PART_NUMBER", version="test_v1")
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    row = df[(df["inventory_uid"] == "iuid_1") & (df["matching_rule"] == "BRAND_PART_NUMBER")].iloc[0]
    assert row["match_status"] == "MATCH_CONFIRMED"
    assert pd.isna(row["contradiction_flags"])
    assert row["confirmation_policy_reason"] == "APPROVED"


# ── J. Cross-evidence calibre corroboration (reconciliation-audit narrow fix) ─
#     docs/MODULE5_RECONCILIATION_AUDIT_REPORT.md

def test_cross_evidence_corroboration_resolves_residual_ambiguous_pair(conn):
    """The exact real-data residual case (inventory_uid iuid_a541f0885f7d4c1a
    / part_number 3135-410): the conflicting evidence row's OWN title has no
    textual compatibility signal, but a SECOND, independent evidence row for
    the SAME part_number textually places both the item's own caliber (3130)
    and the conflicting evidence_calibre (3135) side by side -- this
    corroboration, from the project's own evidence corpus (not reviewer
    domain knowledge, not a hardcoded brand/calibre table), must downgrade
    the conflict to REVIEW_REQUIRED / UNVERIFIED_CALIBRE_COMPATIBILITY,
    never NO_MATCH and never MATCH_CONFIRMED."""
    _seed_inventory(conn, [_inv("iuid_1", caliber="3130", part_number="3135-410")])
    _seed_active(conn, [
        _active_row(1, "Original Rolex Kaliber 3135-410 Hemmungsrad", item_id="i1"),
        _active_row(2, "Rolex 3135-410 Neuwertig Hemmungsrad 3135 3130 3155 3156 3175 3185", item_id="i2"),
    ])
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    row = df[(df["inventory_uid"] == "iuid_1") & (df["source_id"] == 1)
             & (df["matching_rule"] == "PART_NUMBER_EXACT")].iloc[0]
    assert row["match_status"] == "REVIEW_REQUIRED"
    assert row["match_reason_code"] == "UNVERIFIED_CALIBRE_COMPATIBILITY"
    assert pd.isna(row["contradiction_flags"]) or "calibre_conflict" not in (row["contradiction_flags"] or "")
    assert "cross_evidence_calibre_corroboration" in row["risk_flags"]
    assert "unverified_calibre_compatibility" in row["risk_flags"]


def test_no_corroborating_evidence_calibre_conflict_stays_no_match(conn):
    """Without ANY second evidence row mentioning both calibers, the
    conflict remains a hard NO_MATCH -- the correction must not weaken
    calibre_conflict for genuinely unrelated calibers (the other 6 items in
    the real-data population, none of which have corroborating evidence)."""
    _seed_inventory(conn, [_inv("iuid_1", caliber="316", part_number="114200")])
    _seed_active(conn, [_active_row(
        1, "Zifferblatt Air King Ref 14000 14210 114200 Cal. 3000 - 3130", item_id="i1"
    )])
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    row = df[(df["inventory_uid"] == "iuid_1") & (df["matching_rule"] == "PART_NUMBER_EXACT")].iloc[0]
    assert row["match_status"] == "NO_MATCH"
    assert row["match_reason_code"] == "CALIBRE_CONFLICT_EXPLICIT_LABEL_MISMATCH"
    assert "calibre_conflict" in row["contradiction_flags"]


def test_corroborating_evidence_must_share_the_same_part_number(conn):
    """A second listing that mentions both calibers but for a DIFFERENT
    part_number must not corroborate -- corroboration is scoped to the
    exact catalog part number in conflict, not any nearby evidence."""
    _seed_inventory(conn, [_inv("iuid_1", caliber="3130", part_number="3135-410")])
    _seed_active(conn, [
        _active_row(1, "Original Rolex Kaliber 3135-410 Hemmungsrad", item_id="i1"),
        _active_row(2, "Rolex 3135-999 Neuwertig Teil 3135 3130 3155 3156", item_id="i2"),
    ])
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    row = df[(df["inventory_uid"] == "iuid_1") & (df["source_id"] == 1)
             & (df["matching_rule"] == "PART_NUMBER_EXACT")].iloc[0]
    assert row["match_status"] == "NO_MATCH"
    assert "calibre_conflict" in row["contradiction_flags"]


# ── K. Cross-evidence corroboration: canonical-identity independence ────────
#     docs/MODULE5_CROSS_EVIDENCE_INDEPENDENCE_AUDIT.md — the corroborating
#     record must be a genuinely different real-world listing, never the
#     same listing re-collected under a different inventory_uid query, a
#     different matching rule, or a different ingestion batch.

def test_same_listing_collected_twice_still_corroborates_once(conn):
    """The SAME real listing (same item_id) legitimately collected via two
    different inventory-target queries (two rows, two inventory_uid
    collection targets, same item_id) is still ONE genuine, real listing --
    deduplication by item_id must not erroneously suppress valid
    corroboration down to zero."""
    _seed_inventory(conn, [_inv("iuid_1", caliber="3130", part_number="3135-410")])
    _seed_active(conn, [
        _active_row(1, "Original Rolex Kaliber 3135-410 Hemmungsrad", item_id="listingA"),
        _active_row(2, "Rolex 3135-410 Neuwertig Hemmungsrad 3135 3130 3155 3156",
                     inventory_uid="iuid_OTHER", item_id="listingB"),
        _active_row(3, "Rolex 3135-410 Neuwertig Hemmungsrad 3135 3130 3155 3156",
                     inventory_uid="iuid_ANOTHER", item_id="listingB"),  # SAME item_id as row 2
    ])
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    row = df[(df["inventory_uid"] == "iuid_1") & (df["source_id"] == 1)
             & (df["matching_rule"] == "PART_NUMBER_EXACT")].iloc[0]
    assert row["match_status"] == "REVIEW_REQUIRED"
    assert "cross_evidence_calibre_corroboration" in row["risk_flags"]


def test_same_source_row_multiple_matching_rules_does_not_fabricate_corroboration(conn):
    """A single evidence row that fires BOTH PART_NUMBER_EXACT and
    BRAND_PART_NUMBER is still exactly one physical DB row -- it must not
    corroborate ITSELF via its own duplicate candidate-row presence. With
    no genuinely separate listing, the conflict remains NO_MATCH."""
    _seed_inventory(conn, [_inv("iuid_1", brand="Rolex", caliber="3130", part_number="3135-410")])
    _seed_active(conn, [_active_row(1, "Original Rolex Kaliber 3135-410 Hemmungsrad", item_id="listingA")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    both_rules = df[(df["inventory_uid"] == "iuid_1") & (df["source_id"] == 1)]
    assert set(both_rules["matching_rule"]) == {"PART_NUMBER_EXACT", "BRAND_PART_NUMBER"}
    for _, row in both_rules.iterrows():
        assert row["match_status"] == "NO_MATCH"
        assert "calibre_conflict" in row["contradiction_flags"]


def test_duplicate_ingestion_batch_does_not_create_false_corroboration(conn):
    """Two rows sharing the same item_id (a duplicate ingestion event, e.g.
    re-collected in a later batch) but where NEITHER row's content actually
    states both calibers together must not corroborate -- deduplication
    must not be mistaken for 'presence in the query result implies
    corroboration'; the actual textual content is still required."""
    _seed_inventory(conn, [_inv("iuid_1", caliber="3130", part_number="3135-410")])
    _seed_active(conn, [
        _active_row(1, "Original Rolex Kaliber 3135-410 Hemmungsrad", item_id="listingA"),
        _active_row(2, "Rolex 3135-410 generic spare part", inventory_uid="iuid_OTHER", item_id="listingC"),
        _active_row(3, "Rolex 3135-410 generic spare part", inventory_uid="iuid_ANOTHER", item_id="listingC"),
    ])
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    row = df[(df["inventory_uid"] == "iuid_1") & (df["source_id"] == 1)
             & (df["matching_rule"] == "PART_NUMBER_EXACT")].iloc[0]
    assert row["match_status"] == "NO_MATCH"
    assert "calibre_conflict" in row["contradiction_flags"]


def test_genuinely_different_listings_provide_corroborating_ambiguity(conn):
    """Two DISTINCT item_ids, each independently stating both calibers
    together for the same part_number, is genuine corroboration -- the
    positive case, confirming distinct listings are not incorrectly
    collapsed by the dedup logic."""
    _seed_inventory(conn, [_inv("iuid_1", caliber="3130", part_number="3135-410")])
    _seed_active(conn, [
        _active_row(1, "Original Rolex Kaliber 3135-410 Hemmungsrad", item_id="listingA"),
        _active_row(2, "Rolex 3135-410 Neuwertig Hemmungsrad 3135 3130 3155 3156",
                     inventory_uid="iuid_OTHER", item_id="listingD"),
    ])
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = dec.build_match_decisions(conn, decision_run_id="d1")
    row = df[(df["inventory_uid"] == "iuid_1") & (df["source_id"] == 1)
             & (df["matching_rule"] == "PART_NUMBER_EXACT")].iloc[0]
    assert row["match_status"] == "REVIEW_REQUIRED"
    assert "cross_evidence_calibre_corroboration" in row["risk_flags"]


# ── Module 5 evidence identity: the direct lineage-defect regression test ──

def test_decision_not_corrupted_by_staging_rebuild_reassigning_positional_id(conn):
    """The exact scenario docs/MODULE5_LINEAGE_INTEGRITY_AUDIT.md diagnosed:
    a candidate references positional id=1 (evidence A); staging is
    rebuilt and id=1 now points to a completely different listing
    (evidence B); build_match_decisions() re-reads evidence live at
    decision time (scripts/06_decide_matches.py's evidence lookup).

    Before this fix: the decision would silently attach evidence B's
    title to the candidate that was actually generated against evidence
    A — a human reviewer would then be shown the wrong "why this
    matched" explanation.

    After this fix: the candidate carries evidence_uid (content-derived,
    survives the rebuild); since evidence A's evidence_uid no longer
    exists in the rebuilt staging table, the candidate is skipped
    entirely rather than silently mismatched to evidence B.
    """
    _seed_inventory(conn, [_inv("iuid_1", caliber="3135", part_number="99999")])

    uid_a = ei.active_evidence_uid("EBAY_DE", "listing_A")
    row_a = _active_row(1, "Rolex 3135 99999 crown genuine part", item_id="listing_A")
    row_a["stable_evidence_uid"] = uid_a
    _seed_active(conn, [row_a])

    m5.run_candidate_generation(conn, match_run_id="run1")
    candidates_before = conn.execute(
        "SELECT active_raw_id, evidence_uid FROM match_candidates_active WHERE match_run_id='run1'"
    ).fetchall()
    assert candidates_before, "expected at least one candidate from run1"
    assert candidates_before[0][1] == uid_a

    # Simulate a staging rebuild that reassigns the positional id: same
    # id=1, but now a totally unrelated listing.
    conn.execute("DELETE FROM stg_active_targeted")
    uid_b = ei.active_evidence_uid("EBAY_DE", "listing_B_unrelated")
    row_b = _active_row(1, "Vintage pocket watch chain unrelated item", item_id="listing_B_unrelated")
    row_b["stable_evidence_uid"] = uid_b
    _seed_active(conn, [row_b])

    df = dec.build_match_decisions(conn, decision_run_id="d1")

    # The run1 candidate (still referencing id=1, but with evidence_uid=uid_a)
    # must NOT produce a decision at all now, since uid_a is unresolvable
    # in the rebuilt staging table -- it must never be silently attached
    # to evidence B's content.
    stale_decisions = (
        df if df.empty else
        df[(df["source_table"] == "match_candidates_active") & (df["source_id"] == 1)]
    )
    assert stale_decisions.empty, (
        "a decision was produced for a candidate whose evidence_uid no longer "
        "resolves — this means the fix fell back to the vulnerable positional-id "
        "lookup and silently attached the wrong evidence"
    )


def test_candidate_key_uses_evidence_uid_when_available(conn):
    key_with_uid = dec._candidate_key(
        "match_candidates_active", "iuid_1", 1, "PART_NUMBER_EXACT", evidence_uid="EV-ACTIVE-fixed",
    )
    key_without_uid = dec._candidate_key(
        "match_candidates_active", "iuid_1", 1, "PART_NUMBER_EXACT", evidence_uid=None,
    )
    assert key_with_uid != key_without_uid

    # Deterministic: same evidence_uid input -> same key, even if source_id differs
    # (proving the key no longer depends on the unstable positional id when
    # evidence_uid is present).
    key_with_uid_diff_source_id = dec._candidate_key(
        "match_candidates_active", "iuid_1", 999, "PART_NUMBER_EXACT", evidence_uid="EV-ACTIVE-fixed",
    )
    assert key_with_uid == key_with_uid_diff_source_id


# ── Module 5 post-Phase-1 fix: collision detection canonical identity ──────

def test_collision_detected_across_pairing_rows_sharing_evidence_uid(conn):
    """docs/MODULE5_POST_PHASE1_ARCHITECTURE_AUDIT.md Bug 2, reproduced and
    fixed: two different inventory items matching the SAME rule against
    TWO DIFFERENT staging rows that happen to represent the SAME
    real-world listing (same evidence_uid, different positional
    active_raw_id) must be flagged as a genuine collision -- previously
    this was silently missed because the collision map was keyed on the
    positional id, which differs per pairing-row even for identical
    evidence."""
    uid = ei.active_evidence_uid("EBAY_DE", "listing_SHARED")
    conn.execute(
        "INSERT INTO match_candidates_active (match_candidate_id, match_run_id, inventory_uid, "
        "active_raw_id, evidence_uid, match_method, evidence_json) VALUES (1, 'run1', 'iuid_X', 1, ?, 'CALIBER_EXACT', '{}')",
        [uid],
    )
    conn.execute(
        "INSERT INTO match_candidates_active (match_candidate_id, match_run_id, inventory_uid, "
        "active_raw_id, evidence_uid, match_method, evidence_json) VALUES (2, 'run1', 'iuid_Y', 2, ?, 'CALIBER_EXACT', '{}')",
        [uid],
    )
    collisions = dec._multiple_inventory_collision_map(conn, "match_candidates_active", "active_raw_id")
    assert (uid, "CALIBER_EXACT") in collisions


def test_no_false_collision_for_genuinely_different_evidence(conn):
    """Two different inventory items matching two GENUINELY different
    listings (different evidence_uid) under the same rule must NOT be
    flagged -- confirms the fix didn't overcorrect into false positives."""
    uid_a = ei.active_evidence_uid("EBAY_DE", "listing_A")
    uid_b = ei.active_evidence_uid("EBAY_DE", "listing_B")
    conn.execute(
        "INSERT INTO match_candidates_active (match_candidate_id, match_run_id, inventory_uid, "
        "active_raw_id, evidence_uid, match_method, evidence_json) VALUES (1, 'run1', 'iuid_X', 1, ?, 'CALIBER_EXACT', '{}')",
        [uid_a],
    )
    conn.execute(
        "INSERT INTO match_candidates_active (match_candidate_id, match_run_id, inventory_uid, "
        "active_raw_id, evidence_uid, match_method, evidence_json) VALUES (2, 'run1', 'iuid_Y', 2, ?, 'CALIBER_EXACT', '{}')",
        [uid_b],
    )
    collisions = dec._multiple_inventory_collision_map(conn, "match_candidates_active", "active_raw_id")
    assert collisions == set()


# ── Module 5 post-Phase-1 fix: deterministic evidence selection ────────────

def test_evidence_lookup_deterministic_regardless_of_staging_insertion_order(conn):
    """docs/MODULE5_POST_PHASE1_ARCHITECTURE_AUDIT.md Bug 4: which staging
    row's title gets attached to a shared evidence_uid must not depend on
    DuckDB's unenforced default scan order. Insert the SAME rows in two
    different orders (across two isolated connections) and confirm the
    resulting decision title is identical both times."""
    uid = ei.active_evidence_uid("EBAY_DE", "listing_SHARED")

    def _build_and_decide(insertion_order):
        local_conn = duckdb.connect(":memory:")
        local_conn.execute(SCHEMA_PATH.read_text())
        _seed_inventory(local_conn, [_inv("iuid_1", caliber="3135", part_number="99999")])
        for id_ in insertion_order:
            local_conn.execute(
                "INSERT INTO stg_active_targeted (id, raw_id, item_id, title, normalized_title, marketplace, stable_evidence_uid, observation_uid) "
                "VALUES (?, ?, 'listing_SHARED', ?, ?, 'EBAY_DE', ?, ?)",
                [id_, id_, f"Rolex 3135 99999 variant {id_}", f"rolex 3135 99999 variant {id_}", uid, ei.observation_uid(uid, id_)],
            )
        local_conn.execute(
            "INSERT INTO match_candidates_active (match_candidate_id, match_run_id, inventory_uid, active_raw_id, evidence_uid, match_method, evidence_json) "
            "VALUES (1, 'run1', 'iuid_1', ?, ?, 'CALIBER_EXACT', '{}')",
            [insertion_order[0], uid],
        )
        df = dec.build_match_decisions(local_conn, decision_run_id="d1")
        local_conn.close()
        return df

    result_a = _build_and_decide([3, 1, 2])
    result_b = _build_and_decide([1, 2, 3])
    # Both must resolve to the row with the SMALLEST id (deterministic
    # tie-break), regardless of insertion order.
    assert not result_a.empty and not result_b.empty
    assert result_a.iloc[0]["match_reason_text"] == result_b.iloc[0]["match_reason_text"]


# ── collection_relationship staleness fix (docs/MODULE5_STATUS_AND_RUNBOOK.md §6) ──

def test_collection_relationship_stable_when_staging_positional_id_reassigned(conn):
    """Reproduces and guards the last positional-id staleness defect:
    SELF_SOURCED/CROSS_REFERENCED must be computed from the collection
    target persisted at candidate-generation time, NOT re-read from the
    current staging positional id at decision time. Rebuilding staging
    (reassigning positional ids, so id=1 now points to a DIFFERENT
    listing collected by a different inventory item) between candidate
    generation and decision must NOT change the relationship."""
    # two items, same caliber (both match the listing via CALIBER_EXACT),
    # distinct part numbers to satisfy staging_inventory's unique key.
    _seed_inventory(conn, [_inv("iuid_A", caliber="3135", part_number="11111"),
                           _inv("iuid_B", caliber="3135", part_number="22222")])
    uidL = ei.active_evidence_uid("EBAY_DE", "listing_L")
    rowL = _active_row(1, "Rolex 3135 crown", inventory_uid="iuid_A", item_id="listing_L")
    rowL["stable_evidence_uid"] = uidL
    rowL["observation_uid"] = ei.observation_uid(uidL, 1)
    _seed_active(conn, [rowL])
    m5.run_candidate_generation(conn, match_run_id="run1")

    df_before = dec.build_match_decisions(conn, decision_run_id="d1")
    before = {r.inventory_uid: r.collection_relationship for r in df_before.itertuples()
              if r.source_table == "match_candidates_active" and r.matching_rule == "CALIBER_EXACT"}
    assert before == {"iuid_A": "SELF_SOURCED", "iuid_B": "CROSS_REFERENCED"}

    # rebuild staging: id=1 now a DECOY listing collected by iuid_B; the
    # real listing L reinserted under a new positional id=99.
    conn.execute("DELETE FROM stg_active_targeted")
    uidD = ei.active_evidence_uid("EBAY_DE", "decoy")
    rowD = _active_row(1, "unrelated pocket watch", inventory_uid="iuid_B", item_id="decoy")
    rowD["stable_evidence_uid"] = uidD
    rowD["observation_uid"] = ei.observation_uid(uidD, 2)
    rowL2 = _active_row(99, "Rolex 3135 crown", inventory_uid="iuid_A", item_id="listing_L")
    rowL2["stable_evidence_uid"] = uidL
    rowL2["observation_uid"] = ei.observation_uid(uidL, 99)
    _seed_active(conn, [rowD, rowL2])

    df_after = dec.build_match_decisions(conn, decision_run_id="d2")
    after = {r.inventory_uid: r.collection_relationship for r in df_after.itertuples()
             if r.source_table == "match_candidates_active" and r.matching_rule == "CALIBER_EXACT"}
    assert after == before, (
        f"collection_relationship changed after a staging positional-id "
        f"reassignment: before={before} after={after} -- the decision layer "
        f"is still re-reading the unstable staging id instead of the "
        f"persisted collection_inventory_uid"
    )


def test_collection_inventory_uid_persisted_on_active_candidates(conn):
    """The collection target is captured on the candidate at generation
    time (not NULL), so the decision layer never needs the live re-read."""
    _seed_inventory(conn, [_inv("iuid_A", caliber="3135", part_number="11111")])
    uidL = ei.active_evidence_uid("EBAY_DE", "listing_L")
    rowL = _active_row(1, "Rolex 3135 crown", inventory_uid="iuid_A", item_id="listing_L")
    rowL["stable_evidence_uid"] = uidL
    rowL["observation_uid"] = ei.observation_uid(uidL, 1)
    _seed_active(conn, [rowL])
    m5.run_candidate_generation(conn, match_run_id="run1")
    vals = conn.execute(
        "SELECT DISTINCT collection_inventory_uid FROM match_candidates_active WHERE match_run_id='run1'"
    ).fetchall()
    assert vals == [("iuid_A",)], f"expected collection_inventory_uid=iuid_A persisted, got {vals}"
