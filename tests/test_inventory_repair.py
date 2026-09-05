"""
tests/test_inventory_repair.py
===============================
Pytest tests for the inventory repair framework in scripts/02_clean.py:

    raw_inventory -> validation -> inventory_repair_candidates
                                        -> inventory_corrections (approved)
                                        -> clean_inventory (staging_inventory)

Seed rows reproduce the STRUCTURE of the real 12 Excel-date-corrupted rows
found in the live inventory (all part_number/caliber, all matching
EXCEL_DATE_CORRUPTION_RE) — not their literal values — so these tests
exercise the same general, deterministic classification rule the live data
does, without hardcoding to those specific 12 rows. Each of the three
required outcomes (AUTO_REPAIR_ALLOWED, USER_CONFIRMATION_REQUIRED,
UNRESOLVED) is reproduced from a distinct real pattern:

  - day=01, month=01, all sibling part numbers in the same (brand,
    caliber) family are bare (no dash)              -> AUTO_REPAIR_ALLOWED
  - day=01, month=01, but siblings are mixed/dashed
    or there are no siblings to compare against       -> USER_CONFIRMATION_REQUIRED
  - day=01, month!=01 (inherent 2-vs-02 digit
    ambiguity, regardless of sibling evidence)         -> USER_CONFIRMATION_REQUIRED
  - day!=01 (doesn't fit the known corruption shape
    at all)                                            -> UNRESOLVED

Isolation: every test runs against a throwaway duckdb file under pytest's
tmp_path — never database/watchparts.duckdb. A module-scoped autouse
fixture hashes the real project database before/after and fails loudly if
it changed.
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


def _load_clean_module():
    sys.path.insert(0, str(SCRIPTS_DIR))
    spec = importlib.util.spec_from_file_location("clean02_repair", SCRIPTS_DIR / "02_clean.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


clean02 = _load_clean_module()

# ── Seed rows, id -> (brand, caliber, part_number, stock) ──────────────────────
SEED_ROWS = [
    # id=1: HIGH-confidence case. day=01,month=01 -> bare "6655". Siblings
    # (ids 2-4, 12-18 -- 10 total, same brand+caliber) are ALL bare
    # numeric -> corroborated AND meets MIN_SIBLING_SAMPLE_FOR_AUTO_REPAIR.
    dict(id=1, raw_rolex_tudor="Rolex", raw_calibre="1120", raw_p_number="6655-01-01 00:00:00", raw_stock="2"),
    dict(id=2, raw_rolex_tudor="Rolex", raw_calibre="1120", raw_p_number="6717", raw_stock="1"),
    dict(id=3, raw_rolex_tudor="Rolex", raw_calibre="1120", raw_p_number="6682", raw_stock="1"),
    dict(id=4, raw_rolex_tudor="Rolex", raw_calibre="1120", raw_p_number="6678", raw_stock="1"),
    dict(id=12, raw_rolex_tudor="Rolex", raw_calibre="1120", raw_p_number="6707", raw_stock="1"),
    dict(id=13, raw_rolex_tudor="Rolex", raw_calibre="1120", raw_p_number="6654", raw_stock="1"),
    dict(id=14, raw_rolex_tudor="Rolex", raw_calibre="1120", raw_p_number="6648", raw_stock="1"),
    dict(id=15, raw_rolex_tudor="Rolex", raw_calibre="1120", raw_p_number="6637", raw_stock="1"),
    dict(id=16, raw_rolex_tudor="Rolex", raw_calibre="1120", raw_p_number="6612", raw_stock="1"),
    dict(id=17, raw_rolex_tudor="Rolex", raw_calibre="1120", raw_p_number="6632", raw_stock="1"),
    dict(id=18, raw_rolex_tudor="Rolex", raw_calibre="1120", raw_p_number="6643", raw_stock="1"),

    # id=5: CONFIRMATION-REQUIRED case (mixed sibling evidence). day=01,
    # month=01 -> bare "6020" guess, but sibling (id=6) uses a dash suffix.
    dict(id=5, raw_rolex_tudor="Rolex", raw_calibre="24", raw_p_number="6020-01-01 00:00:00", raw_stock="3"),
    dict(id=6, raw_rolex_tudor="Rolex", raw_calibre="24", raw_p_number="530-0", raw_stock="1"),

    # id=7: CONFIRMATION-REQUIRED case (variant-suffix ambiguity). month=02
    # (not 1) -> inherently ambiguous "6771-2" vs "6771-02".
    dict(id=7, raw_rolex_tudor="Rolex", raw_calibre="1135", raw_p_number="6771-02-01 00:00:00", raw_stock="1"),
    dict(id=8, raw_rolex_tudor="Rolex", raw_calibre="1135", raw_p_number="6770", raw_stock="2"),

    # id=9: UNRESOLVED case. day=16 (not 1) -> doesn't fit the known shape.
    dict(id=9, raw_rolex_tudor="Rolex", raw_calibre="22", raw_p_number="2022-01-16 00:00:00", raw_stock="1"),

    # id=10: CONFIRMATION-REQUIRED CALIBER repair (not part_number). day=01,
    # month=01 on the caliber field itself -> bare "7750" guess, but
    # caliber corrections never auto-qualify (no reliable sibling-based
    # corroboration mechanism exists for calibers).
    dict(id=10, raw_rolex_tudor="Tudor", raw_calibre="7750-01-01 00:00:00", raw_p_number="24-T7500-4H", raw_stock="2"),

    # id=19: CONFIRMATION-REQUIRED case (insufficient sample size). day=01,
    # month=01 -> bare "4521" guess, siblings (ids 20-22, only 3) are ALL
    # bare -- unanimous, but fewer than MIN_SIBLING_SAMPLE_FOR_AUTO_REPAIR
    # (10), so this must NOT auto-qualify even though nothing contradicts it.
    dict(id=19, raw_rolex_tudor="Rolex", raw_calibre="2035", raw_p_number="4521-01-01 00:00:00", raw_stock="1"),
    dict(id=20, raw_rolex_tudor="Rolex", raw_calibre="2035", raw_p_number="54516", raw_stock="1"),
    dict(id=21, raw_rolex_tudor="Rolex", raw_calibre="2035", raw_p_number="4511", raw_stock="1"),
    dict(id=22, raw_rolex_tudor="Rolex", raw_calibre="2035", raw_p_number="4514", raw_stock="1"),

    # A normal, valid row, unrelated to any corruption.
    dict(id=11, raw_rolex_tudor="Rolex", raw_calibre="3135", raw_p_number="5000", raw_stock="7"),
]


def _seed_raw_inventory(connection) -> None:
    df = pd.DataFrame(SEED_ROWS)
    df["upload_batch_id"] = "batch_test1"
    df["source_filename"] = "inventory.csv"
    df["file_hash"] = "testhash"
    df["ingested_at"] = "2026-01-01T00:00:00"
    df["validation_status"] = "not_validated"
    df["validation_notes"] = ""
    connection.register("tmp_seed", df)
    connection.execute(
        """
        INSERT INTO raw_inventory (
            id, upload_batch_id, source_filename, file_hash, ingested_at,
            raw_rolex_tudor, raw_calibre, raw_p_number, raw_stock,
            validation_status, validation_notes
        )
        SELECT
            id, upload_batch_id, source_filename, file_hash, ingested_at,
            raw_rolex_tudor, raw_calibre, raw_p_number, raw_stock,
            validation_status, validation_notes
        FROM tmp_seed
        """
    )
    connection.unregister("tmp_seed")


def _file_digest(path: Path):
    if not path.exists():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture(scope="module", autouse=True)
def guard_production_files_untouched():
    real_db = clean02.DB_PATH
    before = _file_digest(real_db)
    yield
    after = _file_digest(real_db)
    assert before == after, "database/watchparts.duckdb changed — test isolation is broken"


@pytest.fixture()
def conn(tmp_path):
    db_path = tmp_path / "test.duckdb"
    assert db_path.resolve() != clean02.DB_PATH.resolve()
    connection = duckdb.connect(str(db_path))
    connection.execute(SCHEMA_PATH.read_text())
    _seed_raw_inventory(connection)
    # Populate inventory_validation_report by running the real cleaning
    # pipeline once — this is what generate_inventory_repair_candidates
    # reads from, exactly as it would against real data.
    clean02.clean_inventory(connection, reports_dir=tmp_path / "reports")
    yield connection
    connection.close()


def _raw_snapshot(connection):
    return connection.execute(
        "SELECT id, raw_rolex_tudor, raw_calibre, raw_p_number, raw_stock FROM raw_inventory ORDER BY id"
    ).fetchall()


def test_high_confidence_bare_number_case_is_auto_repair_allowed(conn):
    candidates = clean02.generate_inventory_repair_candidates(conn)
    cand = next(c for c in candidates if c["raw_inventory_id"] == 1)
    assert cand["classification"] == "AUTO_REPAIR_ALLOWED"
    assert cand["confidence"] == "HIGH"
    assert cand["proposed_value"] == "6655"
    assert cand["column_name"] == "part_number"


def test_mixed_sibling_evidence_case_requires_confirmation(conn):
    candidates = clean02.generate_inventory_repair_candidates(conn)
    cand = next(c for c in candidates if c["raw_inventory_id"] == 5)
    assert cand["classification"] == "USER_CONFIRMATION_REQUIRED"
    assert cand["confidence"] == "MEDIUM"
    assert cand["proposed_value"] == "6020"  # the guess is still computed, just not auto-applied


def test_variant_suffix_ambiguity_case_requires_confirmation(conn):
    candidates = clean02.generate_inventory_repair_candidates(conn)
    cand = next(c for c in candidates if c["raw_inventory_id"] == 7)
    assert cand["classification"] == "USER_CONFIRMATION_REQUIRED"
    assert cand["confidence"] == "MEDIUM"
    assert cand["proposed_value"] == "6771-2"


def test_nondefault_day_case_is_unresolved(conn):
    candidates = clean02.generate_inventory_repair_candidates(conn)
    cand = next(c for c in candidates if c["raw_inventory_id"] == 9)
    assert cand["classification"] == "UNRESOLVED"
    assert cand["proposed_value"] is None


def test_caliber_column_never_auto_repairs(conn):
    """Caliber corrections never auto-qualify, even with day=01/month=01:
    there is no reliable sibling-based corroboration for a caliber value
    (caliber is itself the grouping key used for part_number siblings),
    and the project-wide caliber format is empirically NOT uniformly bare
    (some real calibers use a dash suffix, e.g. '247-2') — an earlier
    version of this classifier assumed it was and got this wrong."""
    candidates = clean02.generate_inventory_repair_candidates(conn)
    cand = next(c for c in candidates if c["raw_inventory_id"] == 10)
    assert cand["column_name"] == "caliber"
    assert cand["classification"] == "USER_CONFIRMATION_REQUIRED"
    assert cand["confidence"] == "MEDIUM"
    assert cand["proposed_value"] == "7750"


def test_valid_row_produces_no_candidate(conn):
    candidates = clean02.generate_inventory_repair_candidates(conn)
    assert all(c["raw_inventory_id"] != 11 for c in candidates)


def test_exactly_expected_number_of_candidates_generated(conn):
    """6 FAIL rows matching the Excel-date pattern in the seed: ids 1, 5,
    7, 9, 19 are part_number FAILs and id 10 is a caliber FAIL — one
    candidate per row, none for the sibling/valid rows."""
    candidates = clean02.generate_inventory_repair_candidates(conn)
    assert len(candidates) == 6
    assert sorted(c["raw_inventory_id"] for c in candidates) == [1, 5, 7, 9, 10, 19]


def test_insufficient_sample_size_case_requires_confirmation(conn):
    """day=01,month=01 -> bare '4521' is unanimous among all 3 siblings,
    but 3 < MIN_SIBLING_SAMPLE_FOR_AUTO_REPAIR (10) -- unanimous agreement
    on a small sample must not by itself justify AUTO_REPAIR_ALLOWED."""
    candidates = clean02.generate_inventory_repair_candidates(conn)
    cand = next(c for c in candidates if c["raw_inventory_id"] == 19)
    assert cand["classification"] == "USER_CONFIRMATION_REQUIRED"
    assert cand["confidence"] == "MEDIUM"
    assert cand["proposed_value"] == "4521"
    assert "fewer than the 10 required" in cand["repair_evidence"]


def test_generation_is_idempotent_no_duplicate_candidates(conn):
    first = clean02.generate_inventory_repair_candidates(conn)
    second = clean02.generate_inventory_repair_candidates(conn)
    assert len(first) == 6
    assert second == [], "rerunning against unchanged state must insert nothing new"
    total = conn.execute("SELECT COUNT(*) FROM inventory_repair_candidates").fetchone()[0]
    assert total == 6


def test_raw_inventory_never_modified_by_generation(conn):
    before = _raw_snapshot(conn)
    clean02.generate_inventory_repair_candidates(conn)
    after = _raw_snapshot(conn)
    assert before == after


def test_apply_auto_repairs_only_touches_auto_allowed(conn):
    clean02.generate_inventory_repair_candidates(conn)
    applied = clean02.apply_auto_repairs(conn)
    applied_ids = {a["id"] for a in applied}

    rows = conn.execute(
        "SELECT id, raw_inventory_id, classification, status FROM inventory_repair_candidates ORDER BY id"
    ).fetchall()
    for cid, raw_id, classification, status in rows:
        if classification == "AUTO_REPAIR_ALLOWED":
            assert status == "AUTO_APPLIED"
            assert cid in applied_ids
        else:
            assert status == "PROPOSED", f"non-AUTO_REPAIR_ALLOWED candidate {cid} must stay PROPOSED"

    # A correction was actually written for the one AUTO_REPAIR_ALLOWED
    # row (id=1 — a part_number repair, corroborated by 3 all-bare
    # siblings). id=10 (a caliber repair) is never auto-applied — see
    # test_caliber_column_never_auto_repairs. _write_correction_from_candidate
    # stores the full current (brand, caliber, part_number) snapshot, not
    # just the one changed field — row 1's caliber "1120" is carried
    # through unchanged alongside its repaired part_number, so a later
    # correction on a DIFFERENT column for the same row never silently
    # reverts this one.
    corrections = conn.execute(
        "SELECT raw_inventory_id, corrected_part_number, corrected_caliber FROM inventory_corrections ORDER BY raw_inventory_id"
    ).fetchall()
    assert corrections == [(1, "6655", "1120")]


def test_apply_auto_repairs_is_idempotent(conn):
    clean02.generate_inventory_repair_candidates(conn)
    clean02.apply_auto_repairs(conn)
    second = clean02.apply_auto_repairs(conn)
    assert second == []
    count = conn.execute("SELECT COUNT(*) FROM inventory_corrections").fetchone()[0]
    assert count == 1, "re-applying must not create duplicate correction rows"


def test_raw_inventory_never_modified_by_auto_repair(conn):
    before = _raw_snapshot(conn)
    clean02.generate_inventory_repair_candidates(conn)
    clean02.apply_auto_repairs(conn)
    after = _raw_snapshot(conn)
    assert before == after


def test_approve_repair_candidate_writes_correction_and_marks_approved(conn):
    clean02.generate_inventory_repair_candidates(conn)
    cand = conn.execute(
        "SELECT id FROM inventory_repair_candidates WHERE raw_inventory_id = 7"
    ).fetchone()
    candidate_id = cand[0]

    clean02.approve_repair_candidate(conn, candidate_id, decided_by="lead_data_engineer", notes="Confirmed via seller listing photo")

    status_row = conn.execute(
        "SELECT status, decided_by FROM inventory_repair_candidates WHERE id = ?", [candidate_id]
    ).fetchone()
    assert status_row == ("APPROVED", "lead_data_engineer")

    correction = conn.execute(
        "SELECT corrected_part_number FROM inventory_corrections WHERE raw_inventory_id = 7"
    ).fetchone()
    assert correction == ("6771-2",)


def test_reject_repair_candidate_writes_no_correction(conn):
    clean02.generate_inventory_repair_candidates(conn)
    cand = conn.execute(
        "SELECT id FROM inventory_repair_candidates WHERE raw_inventory_id = 5"
    ).fetchone()
    candidate_id = cand[0]

    clean02.reject_repair_candidate(conn, candidate_id, decided_by="lead_data_engineer", notes="Turned out to be a different part entirely")

    status_row = conn.execute(
        "SELECT status FROM inventory_repair_candidates WHERE id = ?", [candidate_id]
    ).fetchone()
    assert status_row == ("REJECTED",)
    correction = conn.execute(
        "SELECT COUNT(*) FROM inventory_corrections WHERE raw_inventory_id = 5"
    ).fetchone()[0]
    assert correction == 0


def test_approve_unresolved_candidate_with_no_value_raises(conn):
    clean02.generate_inventory_repair_candidates(conn)
    cand = conn.execute(
        "SELECT id FROM inventory_repair_candidates WHERE raw_inventory_id = 9"
    ).fetchone()
    candidate_id = cand[0]
    with pytest.raises(ValueError, match="no proposed_value"):
        clean02.approve_repair_candidate(conn, candidate_id, decided_by="lead_data_engineer")


def test_end_to_end_auto_repair_flows_into_staging_inventory(conn, tmp_path):
    """Full pipeline proof: raw_inventory -> validation -> repair_candidates
    -> inventory_corrections -> a SECOND clean_inventory() run -> the
    repaired value appears in staging_inventory, not NULL anymore."""
    clean02.generate_inventory_repair_candidates(conn)
    clean02.apply_auto_repairs(conn)

    clean02.clean_inventory(conn, reports_dir=tmp_path / "reports2")

    row = conn.execute(
        "SELECT part_number, validation_status FROM staging_inventory WHERE brand = 'Rolex' AND caliber = '1120' AND part_number = '6655'"
    ).fetchone()
    assert row == ("6655", "PASS")

    # id=10's caliber repair is USER_CONFIRMATION_REQUIRED, not auto-applied
    # (see test_caliber_column_never_auto_repairs) — its caliber must
    # still be NULL/FAIL in staging until a human explicitly approves it.
    caliber_row = conn.execute(
        "SELECT caliber, validation_status FROM staging_inventory WHERE brand = 'Tudor' AND part_number = '24-T7500-4H'"
    ).fetchone()
    assert caliber_row == (None, "FAIL")
