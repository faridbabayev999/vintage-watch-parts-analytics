"""
tests/test_evidence_coverage_audit.py
=======================================
Pytest tests for scripts/05b_evidence_coverage_audit.py — the Historical
Evidence Gap Audit. Read-only summary logic only: no scoring, no
confidence, no candidate generation lives in this file.

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


m5 = _load_module("m5_candidates_for_audit", SCRIPTS_DIR / "05_generate_match_candidates.py")
audit = _load_module("m5b_evidence_audit", SCRIPTS_DIR / "05b_evidence_coverage_audit.py")


def _file_digest(path: Path):
    if not path.exists():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture(scope="module", autouse=True)
def guard_production_files_untouched():
    real_db = audit.DB_PATH
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


def _active_row(id_, title, item_id="item1", marketplace="EBAY_DE"):
    return {
        "id": id_, "raw_id": id_, "item_id": item_id, "title": title,
        "normalized_title": title.lower(), "marketplace": marketplace,
    }


@pytest.fixture()
def conn(tmp_path):
    db_path = tmp_path / "test.duckdb"
    assert db_path.resolve() != audit.DB_PATH.resolve()
    connection = duckdb.connect(str(db_path))
    connection.execute(SCHEMA_PATH.read_text())
    yield connection
    connection.close()


def _inv(uid, brand="Rolex", caliber="3135", part_number="555222"):
    return dict(
        canonical_inventory_id=f"{brand}_{caliber}_{part_number}_{uid}", inventory_uid=uid,
        brand=brand, caliber=caliber, part_number=part_number, stock=1, validation_status="PASS",
    )


# ── Category correctness ────────────────────────────────────────────────────

def test_category_a_when_part_number_candidate_exists(conn):
    _seed_inventory(conn, [_inv("iuid_a", part_number="555222")])
    _seed_active(conn, [_active_row(1, "Rolex bridge 555222")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = audit.build_inventory_evidence_coverage(conn)
    row = df[df["inventory_uid"] == "iuid_a"].iloc[0]
    assert row["evidence_category"] == "A"
    assert row["part_number_candidate_count"] >= 1


def test_category_b_when_only_caliber_candidate_exists(conn):
    _seed_inventory(conn, [_inv("iuid_b", caliber="3135", part_number="9999")])
    _seed_active(conn, [_active_row(1, "Rolex 3135 movement genuine")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = audit.build_inventory_evidence_coverage(conn)
    row = df[df["inventory_uid"] == "iuid_b"].iloc[0]
    assert row["evidence_category"] == "B"
    assert row["part_number_candidate_count"] == 0
    assert row["caliber_candidate_count"] >= 1


def test_category_a_when_only_caliber_part_number_candidate_exists(conn):
    """CALIBER_PART_NUMBER (RULE 3) is part-number-tier evidence, exactly
    like PART_NUMBER_EXACT/BRAND_PART_NUMBER — an item whose ONLY evidence
    is CALIBER_PART_NUMBER must land in category A, not B. Regression test
    for the gap where PART_NUMBER_METHODS predated RULE 3 and silently
    excluded it, undercounting these items into category B."""
    # part_number "530-0" is 4 alphanumeric characters: passes RULE 3's
    # own >=3 floor but NOT utils.part_number_is_distinctive's >=5 floor,
    # so PART_NUMBER_EXACT/BRAND_PART_NUMBER do not fire here — isolates
    # CALIBER_PART_NUMBER as the item's only source of part-number-tier evidence.
    assert m5.utils.part_number_is_distinctive("530-0") is False
    _seed_inventory(conn, [_inv("iuid_rule3", caliber="24", part_number="530-0")])
    _seed_active(conn, [_active_row(1, "Genuine Swiss Made Rolex Steel Crown 24-530-0 NOS")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    methods = {r[0] for r in conn.execute(
        "SELECT match_method FROM match_candidates_active WHERE inventory_uid='iuid_rule3'"
    ).fetchall()}
    assert "CALIBER_PART_NUMBER" in methods
    assert "PART_NUMBER_EXACT" not in methods and "BRAND_PART_NUMBER" not in methods, (
        "test must isolate CALIBER_PART_NUMBER as the only part-number-tier evidence"
    )
    df = audit.build_inventory_evidence_coverage(conn)
    row = df[df["inventory_uid"] == "iuid_rule3"].iloc[0]
    assert row["evidence_category"] == "A"
    assert row["part_number_candidate_count"] >= 1
    assert row["caliber_candidate_count"] >= 1  # CALIBER_EXACT co-fires; both counted, category still A


def test_category_c_detects_quote_mark_normalization_variant(conn):
    """The exact example the audit was commissioned to catch: caliber
    '13"72' stored with a quote character never matches the exact-token
    rule engine, but the digits '1372' appear in a historical title."""
    _seed_inventory(conn, [_inv("iuid_c", caliber='13"72', part_number="99999zz")])
    _seed_active(conn, [_active_row(1, "Rolex 1372 movement no other match here")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = audit.build_inventory_evidence_coverage(conn)
    row = df[df["inventory_uid"] == "iuid_c"].iloc[0]
    assert row["active_candidate_count"] == 0
    assert row["evidence_category"] == "C"


def test_category_d_when_nothing_found_anywhere(conn):
    _seed_inventory(conn, [_inv("iuid_d", caliber="ZZZ000", part_number="QQQQQ111")])
    _seed_active(conn, [_active_row(1, "completely unrelated listing title")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = audit.build_inventory_evidence_coverage(conn)
    row = df[df["inventory_uid"] == "iuid_d"].iloc[0]
    assert row["evidence_category"] == "D"


def test_fail_status_inventory_excluded(conn):
    rows = [_inv("iuid_fail", part_number="555222")]
    rows[0]["validation_status"] = "FAIL"
    _seed_inventory(conn, rows)
    _seed_active(conn, [_active_row(1, "Rolex bridge 555222")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = audit.build_inventory_evidence_coverage(conn)
    assert "iuid_fail" not in set(df["inventory_uid"])


# ── Raw/staging immutability ────────────────────────────────────────────────

def test_no_raw_or_staging_tables_modified(conn):
    _seed_inventory(conn, [_inv("iuid_x", part_number="555222")])
    _seed_active(conn, [_active_row(1, "Rolex bridge 555222")])
    m5.run_candidate_generation(conn, match_run_id="run1")

    before = conn.execute("SELECT COUNT(*) FROM staging_inventory").fetchone()[0]
    before_active = conn.execute("SELECT COUNT(*) FROM stg_active_targeted").fetchone()[0]

    coverage_df = audit.build_inventory_evidence_coverage(conn)
    audit.write_inventory_evidence_coverage(conn, coverage_df)
    audit.build_unmatched_inventory_analysis(conn, coverage_df)

    after = conn.execute("SELECT COUNT(*) FROM staging_inventory").fetchone()[0]
    after_active = conn.execute("SELECT COUNT(*) FROM stg_active_targeted").fetchone()[0]
    assert before == after
    assert before_active == after_active


# ── Idempotency ──────────────────────────────────────────────────────────────

def test_rerun_produces_identical_coverage_content(conn):
    _seed_inventory(conn, [_inv("iuid_y", part_number="555222"), _inv("iuid_z", caliber="9001", part_number="1")])
    _seed_active(conn, [_active_row(1, "Rolex bridge 555222"), _active_row(2, "Rolex 9001 movement")])
    m5.run_candidate_generation(conn, match_run_id="run1")

    df1 = audit.build_inventory_evidence_coverage(conn, audit_run_id="fixed_id")
    df2 = audit.build_inventory_evidence_coverage(conn, audit_run_id="fixed_id")
    cols = [c for c in df1.columns if c != "computed_at"]
    pd.testing.assert_frame_equal(
        df1[cols].sort_values("inventory_uid").reset_index(drop=True),
        df2[cols].sort_values("inventory_uid").reset_index(drop=True),
    )


# ── Unmatched analysis shape ─────────────────────────────────────────────────

def test_unmatched_analysis_excludes_category_a_only(conn):
    _seed_inventory(conn, [_inv("iuid_a", part_number="555222"), _inv("iuid_b", caliber="3135", part_number="8888")])
    _seed_active(conn, [_active_row(1, "Rolex bridge 555222"), _active_row(2, "Rolex 3135 movement")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    coverage_df = audit.build_inventory_evidence_coverage(conn)
    unmatched_df = audit.build_unmatched_inventory_analysis(conn, coverage_df)
    assert "iuid_a" not in set(unmatched_df["inventory_uid"])
    assert "iuid_b" in set(unmatched_df["inventory_uid"])
    expected_cols = {
        "inventory_uid", "brand", "caliber", "part_number", "evidence_category",
        "possible_normalization_issue", "historical_mentions_found", "recommended_action",
    }
    assert expected_cols.issubset(set(unmatched_df.columns))


def test_non_distinctive_part_number_flagged_generic_not_extraction(conn):
    """A single-character part number can rack up spurious substring-style
    'mentions' via unrelated reference numbers; the recommended action
    must still call it out as generic rather than recommending extraction."""
    _seed_inventory(conn, [_inv("iuid_short", caliber="ZZZ999", part_number="8")])
    _seed_active(conn, [_active_row(1, "Rolex cellini 4112/8 18k yellow gold")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    coverage_df = audit.build_inventory_evidence_coverage(conn)
    unmatched_df = audit.build_unmatched_inventory_analysis(conn, coverage_df)
    row = unmatched_df[unmatched_df["inventory_uid"] == "iuid_short"].iloc[0]
    assert row["recommended_action"] in (
        "PART_NUMBER_TOO_GENERIC_AND_NO_EVIDENCE_FOUND",
        "PART_NUMBER_TOO_GENERIC_FOR_RELIABLE_MATCH",
    )


# ── Future-upload compatibility ─────────────────────────────────────────────

def test_no_hardcoded_snapshot_size(conn):
    """The audit must not assume any specific inventory count — it should
    work correctly on a freshly uploaded, differently-sized inventory."""
    rows = [_inv(f"iuid_{i}", part_number=f"{i}00000") for i in range(3)]
    _seed_inventory(conn, rows)
    _seed_active(conn, [_active_row(i, f"Rolex bridge {i}00000") for i in range(3)])
    m5.run_candidate_generation(conn, match_run_id="run1")
    df = audit.build_inventory_evidence_coverage(conn)
    assert len(df) == 3
    assert set(df["evidence_category"]) == {"A"}
