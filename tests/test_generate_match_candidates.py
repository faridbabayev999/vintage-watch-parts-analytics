"""
tests/test_generate_match_candidates.py
=========================================
Pytest tests for scripts/05_generate_match_candidates.py — Module 5
CANDIDATE GENERATION only. No scoring, no confidence, no accept/reject
exists to test here by design.

Covers: grain preservation, idempotency (content-level — match_run
intentionally accumulates history, like collection_batches, so row count
growing across runs is expected; CONTENT must not change), duplicate
prevention within one run, many-to-many relationship preservation.

Isolation: every test runs against a duckdb file under pytest's tmp_path —
never database/watchparts.duckdb. A module-scoped autouse fixture hashes
the real project database before/after and fails loudly if it changed.
"""

import hashlib
import importlib.util
import json
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


m5 = _load_module("m5_candidates", SCRIPTS_DIR / "05_generate_match_candidates.py")
import evidence_identity as ei  # noqa: E402 — SCRIPTS_DIR already on sys.path via _load_module


def _file_digest(path: Path):
    if not path.exists():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture(scope="module", autouse=True)
def guard_production_files_untouched():
    real_db = m5.DB_PATH
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
    assert db_path.resolve() != m5.DB_PATH.resolve()
    connection = duckdb.connect(str(db_path))
    connection.execute(SCHEMA_PATH.read_text())
    yield connection
    connection.close()


# ── Rule correctness ────────────────────────────────────────────────────────

def test_part_number_exact_rule(conn):
    _seed_inventory(conn, [dict(
        canonical_inventory_id="rolex_3135_12345", inventory_uid="iuid_1",
        brand="Rolex", caliber="3135", part_number="12345", stock=1, validation_status="PASS",
    )])
    _seed_active(conn, [_active_row(1, "Rolex 3135 bridge 12345")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    rows = conn.execute(
        "SELECT match_method, evidence_json FROM match_candidates_active WHERE inventory_uid='iuid_1'"
    ).fetchall()
    methods = {r[0] for r in rows}
    assert "PART_NUMBER_EXACT" in methods
    evidence = json.loads(next(r[1] for r in rows if r[0] == "PART_NUMBER_EXACT"))
    assert evidence == {
        "rule": "PART_NUMBER_EXACT", "inventory_value": "12345",
        "title": "Rolex 3135 bridge 12345", "matched_tokens": ["12345"],
    }


def test_exact_token_not_substring(conn):
    """Distinctive part number '12345' must NOT match inside the longer
    digit run '123456' — exact word-bounded token only, never a bare
    substring. Uses a 5-char part number specifically so the
    distinctiveness gate doesn't skip the rule for the wrong reason."""
    assert m5.utils.part_number_is_distinctive("12345") is True
    _seed_inventory(conn, [dict(
        canonical_inventory_id="rolex_3135_12345", inventory_uid="iuid_1",
        brand="Rolex", caliber="3135", part_number="12345", stock=1, validation_status="PASS",
    )])
    _seed_active(conn, [_active_row(1, "Rolex 3135 part 123456")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    count = conn.execute(
        "SELECT COUNT(*) FROM match_candidates_active WHERE inventory_uid='iuid_1' AND match_method='PART_NUMBER_EXACT'"
    ).fetchone()[0]
    assert count == 0, "'12345' must not match as a substring of '123456'"


def test_caliber_exact_rule(conn):
    _seed_inventory(conn, [dict(
        canonical_inventory_id="rolex_3135_9999", inventory_uid="iuid_1",
        brand="Rolex", caliber="3135", part_number="9999", stock=1, validation_status="PASS",
    )])
    _seed_active(conn, [_active_row(1, "Rolex 3135 movement genuine")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    methods = {r[0] for r in conn.execute(
        "SELECT match_method FROM match_candidates_active WHERE inventory_uid='iuid_1'"
    ).fetchall()}
    assert "CALIBER_EXACT" in methods


def test_brand_caliber_rule_requires_both(conn):
    _seed_inventory(conn, [dict(
        canonical_inventory_id="rolex_3135_9999", inventory_uid="iuid_1",
        brand="Rolex", caliber="3135", part_number="9999", stock=1, validation_status="PASS",
    )])
    _seed_active(conn, [
        _active_row(1, "Rolex 3135 movement", item_id="item1"),   # both brand+caliber -> BRAND_CALIBER
        _active_row(2, "Tudor 3135 movement", item_id="item2"),   # caliber only, wrong brand -> no BRAND_CALIBER
    ])
    m5.run_candidate_generation(conn, match_run_id="run1")
    rows = conn.execute(
        "SELECT active_raw_id, match_method FROM match_candidates_active WHERE inventory_uid='iuid_1' AND match_method='BRAND_CALIBER'"
    ).fetchall()
    assert [r[0] for r in rows] == [1]


def test_brand_part_number_rule_requires_both(conn):
    _seed_inventory(conn, [dict(
        canonical_inventory_id="rolex_3135_555222", inventory_uid="iuid_1",
        brand="Rolex", caliber="3135", part_number="555222", stock=1, validation_status="PASS",
    )])
    _seed_active(conn, [
        _active_row(1, "Rolex bridge 555222", item_id="item1"),
        _active_row(2, "Tudor bridge 555222", item_id="item2"),
    ])
    m5.run_candidate_generation(conn, match_run_id="run1")
    rows = conn.execute(
        "SELECT active_raw_id FROM match_candidates_active WHERE inventory_uid='iuid_1' AND match_method='BRAND_PART_NUMBER'"
    ).fetchall()
    assert [r[0] for r in rows] == [1]


def test_non_distinctive_part_number_skips_part_number_rules(conn):
    """A non-distinctive part number (per utils.part_number_is_distinctive)
    must not trigger PART_NUMBER_EXACT or BRAND_PART_NUMBER, per the
    documented scope limit."""
    assert m5.utils.part_number_is_distinctive("1") is False  # ground the test in the real utility
    _seed_inventory(conn, [dict(
        canonical_inventory_id="rolex_3135_1", inventory_uid="iuid_1",
        brand="Rolex", caliber="3135", part_number="1", stock=1, validation_status="PASS",
    )])
    _seed_active(conn, [_active_row(1, "Rolex 3135 part 1 genuine")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    methods = {r[0] for r in conn.execute(
        "SELECT match_method FROM match_candidates_active WHERE inventory_uid='iuid_1'"
    ).fetchall()}
    assert "PART_NUMBER_EXACT" not in methods
    assert "BRAND_PART_NUMBER" not in methods
    assert "CALIBER_EXACT" in methods  # caliber rule is unaffected


def test_fail_status_inventory_excluded(conn):
    _seed_inventory(conn, [dict(
        canonical_inventory_id="rolex_3135_12345", inventory_uid="iuid_fail",
        brand="Rolex", caliber="3135", part_number="12345", stock=1, validation_status="FAIL",
    )])
    _seed_active(conn, [_active_row(1, "Rolex 3135 bridge 12345")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    count = conn.execute("SELECT COUNT(*) FROM match_candidates_active WHERE inventory_uid='iuid_fail'").fetchone()[0]
    assert count == 0


# ── Grain preservation, many-to-many, duplicate prevention ─────────────────

def test_grain_preservation_one_row_per_staged_row_per_rule(conn):
    """Each qualifying (inventory_uid, source_row, rule) combination
    produces exactly one candidate row — no expansion, no collapsing.
    The title contains 'bridge' (a real component-vocabulary word), so
    all seven rules qualify: brand+caliber+part number (>=5, so both the
    >=5 and >=3 gates pass)+component all present."""
    _seed_inventory(conn, [dict(
        canonical_inventory_id="rolex_3135_12345", inventory_uid="iuid_1",
        brand="Rolex", caliber="3135", part_number="12345", stock=1, validation_status="PASS",
    )])
    _seed_active(conn, [_active_row(1, "Rolex 3135 bridge 12345")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    rows = conn.execute(
        "SELECT match_method FROM match_candidates_active WHERE inventory_uid='iuid_1' AND active_raw_id=1"
    ).fetchall()
    assert sorted(r[0] for r in rows) == [
        "BRAND_CALIBER", "BRAND_CALIBER_COMPONENT", "BRAND_PART_NUMBER",
        "CALIBER_COMPONENT", "CALIBER_EXACT", "CALIBER_PART_NUMBER", "PART_NUMBER_EXACT",
    ]


def test_many_to_many_one_listing_multiple_inventory_items(conn):
    """The same listing can be a candidate for multiple different
    inventory items sharing the same caliber — never collapsed to one."""
    _seed_inventory(conn, [
        dict(canonical_inventory_id="rolex_3135_a", inventory_uid="iuid_a",
             brand="Rolex", caliber="3135", part_number="1111", stock=1, validation_status="PASS"),
        dict(canonical_inventory_id="rolex_3135_b", inventory_uid="iuid_b",
             brand="Rolex", caliber="3135", part_number="2222", stock=1, validation_status="PASS"),
    ])
    _seed_active(conn, [_active_row(1, "Rolex 3135 generic movement")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    uids = conn.execute(
        "SELECT DISTINCT inventory_uid FROM match_candidates_active WHERE active_raw_id=1 ORDER BY inventory_uid"
    ).fetchall()
    assert [u[0] for u in uids] == ["iuid_a", "iuid_b"]


def test_many_to_many_one_inventory_item_multiple_listings(conn):
    _seed_inventory(conn, [dict(
        canonical_inventory_id="rolex_3135_12345", inventory_uid="iuid_1",
        brand="Rolex", caliber="3135", part_number="12345", stock=1, validation_status="PASS",
    )])
    _seed_active(conn, [
        _active_row(1, "Rolex 3135 bridge 12345", item_id="item1"),
        _active_row(2, "Rolex 3135 bridge 12345 genuine", item_id="item2"),
        _active_row(3, "Rolex 3135 bridge 12345 NOS", item_id="item3"),
    ])
    m5.run_candidate_generation(conn, match_run_id="run1")
    ids = conn.execute(
        "SELECT DISTINCT active_raw_id FROM match_candidates_active WHERE inventory_uid='iuid_1' ORDER BY active_raw_id"
    ).fetchall()
    assert [i[0] for i in ids] == [1, 2, 3]


def test_duplicate_prevention_within_one_run(conn):
    """Rerunning generation with the SAME match_run_id must never create a
    second row for the same (inventory_uid, source_row, rule)."""
    _seed_inventory(conn, [dict(
        canonical_inventory_id="rolex_3135_12345", inventory_uid="iuid_1",
        brand="Rolex", caliber="3135", part_number="12345", stock=1, validation_status="PASS",
    )])
    _seed_active(conn, [_active_row(1, "Rolex 3135 bridge 12345")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    m5.run_candidate_generation(conn, match_run_id="run1")  # same run_id, rerun
    count = conn.execute(
        "SELECT COUNT(*) FROM match_candidates_active WHERE inventory_uid='iuid_1' AND active_raw_id=1 AND match_method='PART_NUMBER_EXACT'"
    ).fetchone()[0]
    assert count == 1


# ── New component-vocabulary rules ──────────────────────────────────────────

def test_caliber_component_rule_requires_both(conn):
    _seed_inventory(conn, [dict(
        canonical_inventory_id="rolex_3135_x", inventory_uid="iuid_1",
        brand="Rolex", caliber="3135", part_number="99999", stock=1, validation_status="PASS",
    )])
    _seed_active(conn, [
        _active_row(1, "Rolex 3135 crown genuine", item_id="item1"),   # caliber + component -> fires
        _active_row(2, "Rolex 3135 vintage watch", item_id="item2"),   # caliber, no component -> no fire
    ])
    m5.run_candidate_generation(conn, match_run_id="run1")
    rows = conn.execute(
        "SELECT active_raw_id, evidence_json FROM match_candidates_active "
        "WHERE inventory_uid='iuid_1' AND match_method='CALIBER_COMPONENT'"
    ).fetchall()
    assert [r[0] for r in rows] == [1]
    evidence = json.loads(rows[0][1])
    assert evidence == {
        "rule": "CALIBER_COMPONENT", "caliber": "3135", "component": "CROWN",
        "matched_tokens": ["3135", "crown"], "source_id": 1,
    }


def test_brand_caliber_component_rule_requires_all_three(conn):
    _seed_inventory(conn, [dict(
        canonical_inventory_id="rolex_3135_x", inventory_uid="iuid_1",
        brand="Rolex", caliber="3135", part_number="99999", stock=1, validation_status="PASS",
    )])
    _seed_active(conn, [
        _active_row(1, "Rolex 3135 crown genuine", item_id="item1"),  # brand+caliber+component -> fires
        _active_row(2, "Tudor 3135 crown genuine", item_id="item2"),  # wrong brand -> no fire
    ])
    m5.run_candidate_generation(conn, match_run_id="run1")
    rows = conn.execute(
        "SELECT active_raw_id FROM match_candidates_active "
        "WHERE inventory_uid='iuid_1' AND match_method='BRAND_CALIBER_COMPONENT'"
    ).fetchall()
    assert [r[0] for r in rows] == [1]


def test_caliber_component_no_component_word_no_candidate(conn):
    """Caliber alone (no component vocabulary word present) must not
    trigger CALIBER_COMPONENT — this is what distinguishes it from the
    existing, unmodified CALIBER_EXACT rule."""
    _seed_inventory(conn, [dict(
        canonical_inventory_id="rolex_3135_x", inventory_uid="iuid_1",
        brand="Rolex", caliber="3135", part_number="99999", stock=1, validation_status="PASS",
    )])
    _seed_active(conn, [_active_row(1, "Rolex 3135 vintage collectible")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    methods = {r[0] for r in conn.execute(
        "SELECT match_method FROM match_candidates_active WHERE inventory_uid='iuid_1'"
    ).fetchall()}
    assert "CALIBER_EXACT" in methods  # existing rule unaffected
    assert "CALIBER_COMPONENT" not in methods
    assert "BRAND_CALIBER_COMPONENT" not in methods


def test_existing_rules_unmodified_by_component_addition(conn):
    """CALIBER_EXACT and BRAND_CALIBER must still fire exactly as before,
    regardless of whether a component word is present — the new rules are
    additive, not a replacement."""
    _seed_inventory(conn, [dict(
        canonical_inventory_id="rolex_3135_x", inventory_uid="iuid_1",
        brand="Rolex", caliber="3135", part_number="99999", stock=1, validation_status="PASS",
    )])
    _seed_active(conn, [_active_row(1, "Rolex 3135 crown genuine")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    methods = {r[0] for r in conn.execute(
        "SELECT match_method FROM match_candidates_active WHERE inventory_uid='iuid_1'"
    ).fetchall()}
    assert {"CALIBER_EXACT", "BRAND_CALIBER", "CALIBER_COMPONENT", "BRAND_CALIBER_COMPONENT"}.issubset(methods)


def test_component_vocabulary_german_terms(conn):
    """The vocabulary must include mined German terms, not just English —
    'krone' (crown) and 'schraube' (screw) specifically."""
    assert "krone" in m5.COMPONENT_VOCABULARY["CROWN"]
    assert "schraube" in m5.COMPONENT_VOCABULARY["SCREW"]
    _seed_inventory(conn, [dict(
        canonical_inventory_id="rolex_3135_x", inventory_uid="iuid_1",
        brand="Rolex", caliber="3135", part_number="99999", stock=1, validation_status="PASS",
    )])
    _seed_active(conn, [_active_row(1, "Rolex 3135 krone original")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    row = conn.execute(
        "SELECT evidence_json FROM match_candidates_active WHERE inventory_uid='iuid_1' AND match_method='CALIBER_COMPONENT'"
    ).fetchone()
    evidence = json.loads(row[0])
    assert evidence["component"] == "CROWN"
    assert evidence["matched_tokens"] == ["3135", "krone"]


# ── RULE 3: CALIBER_PART_NUMBER (>=3 alphanumeric part-number floor) ───────
#
# See docs/MODULE5_RULE3_SAFEGUARD_FINAL_VALIDATION.md for the full
# validation this rule and its floor are based on: a full-population check
# found the existing >=5 floor (utils.part_number_is_distinctive, used by
# PART_NUMBER_EXACT/BRAND_PART_NUMBER) discards verified-genuine compound
# codes like "24-530-0"/"25-104", while a rule-specific >=3 floor removes
# the entire confirmed false-positive pocket (measurement/quantity-prefix/
# model-edition-number collisions, e.g. part_number "2" matching "GMT
# Master 2") at a small, bounded coverage cost.

def test_caliber_part_number_rule_positive_match(conn):
    """Caliber AND part_number both present (part_number >=3 alnum chars),
    with brand also present on the seeded item — the required 'same
    brand/calibre/part_number creates a candidate' scenario."""
    _seed_inventory(conn, [dict(
        canonical_inventory_id="rolex_3135_204", inventory_uid="iuid_1",
        brand="Rolex", caliber="3135", part_number="204", stock=1, validation_status="PASS",
    )])
    _seed_active(conn, [_active_row(1, "Rolex 3135-204 Barrel Bridge genuine")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    rows = conn.execute(
        "SELECT match_method FROM match_candidates_active "
        "WHERE inventory_uid='iuid_1' AND match_method='CALIBER_PART_NUMBER'"
    ).fetchall()
    assert len(rows) == 1


def test_caliber_part_number_evidence_shape(conn):
    """Evidence JSON shape matches BRAND_CALIBER/BRAND_PART_NUMBER's
    convention (the shared _emit() shape: rule/inventory_value/title/
    matched_tokens, with inventory_value a dict of the two matched
    values) — not the special CALIBER_COMPONENT shape."""
    _seed_inventory(conn, [dict(
        canonical_inventory_id="rolex_3135_204", inventory_uid="iuid_1",
        brand="Rolex", caliber="3135", part_number="204", stock=1, validation_status="PASS",
    )])
    _seed_active(conn, [_active_row(1, "Rolex 3135-204 Barrel Bridge genuine")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    row = conn.execute(
        "SELECT evidence_json FROM match_candidates_active WHERE inventory_uid='iuid_1' AND match_method='CALIBER_PART_NUMBER'"
    ).fetchone()
    evidence = json.loads(row[0])
    assert evidence == {
        "rule": "CALIBER_PART_NUMBER",
        "inventory_value": {"caliber": "3135", "part_number": "204"},
        "title": "Rolex 3135-204 Barrel Bridge genuine",
        "matched_tokens": ["3135", "204"],
    }


def test_caliber_part_number_short_part_number_excluded(conn):
    """part_number values under the 3-alphanumeric-character floor must
    never create a CALIBER_PART_NUMBER candidate — the exact protection
    validated against the confirmed false-positive patterns (e.g.
    part_number '2' colliding with 'GMT Master 2', part_number '24'
    colliding with unrelated text)."""
    _seed_inventory(conn, [
        dict(canonical_inventory_id="rolex_25_2", inventory_uid="iuid_pn2",
             brand="Rolex", caliber="25", part_number="2", stock=1, validation_status="PASS"),
        dict(canonical_inventory_id="rolex_3135_24", inventory_uid="iuid_pn24",
             brand="Rolex", caliber="3135", part_number="24", stock=1, validation_status="PASS"),
    ])
    _seed_active(conn, [
        # Genuine reproduction of the confirmed false-positive pattern: caliber
        # "25" is present, and "2" is present too (inside "GMT Master 2"), but
        # this must not fire CALIBER_PART_NUMBER for either short part number.
        _active_row(1, "Saphirglas f Rolex GMT Master 2 25-295C2 genuine", item_id="item1"),
        _active_row(2, "Rolex 3135 movement genuine 24", item_id="item2"),
    ])
    m5.run_candidate_generation(conn, match_run_id="run1")
    methods_pn2 = {r[0] for r in conn.execute(
        "SELECT match_method FROM match_candidates_active WHERE inventory_uid='iuid_pn2'"
    ).fetchall()}
    methods_pn24 = {r[0] for r in conn.execute(
        "SELECT match_method FROM match_candidates_active WHERE inventory_uid='iuid_pn24'"
    ).fetchall()}
    assert "CALIBER_PART_NUMBER" not in methods_pn2, "part_number '2' (1 alnum char) must not fire RULE 3"
    assert "CALIBER_PART_NUMBER" not in methods_pn24, "part_number '24' (2 alnum chars) must not fire RULE 3"
    # CALIBER_EXACT (unaffected, no floor) still fires for both — confirms
    # the exclusion is specific to CALIBER_PART_NUMBER, not a caliber-matching bug.
    assert "CALIBER_EXACT" in methods_pn2
    assert "CALIBER_EXACT" in methods_pn24


def test_caliber_part_number_genuine_three_plus_char_identifiers_included(conn):
    """Real, verified-genuine compound-code identifiers from the
    validation gate ('24-530-0', '25-104' style — 4 and 3 alphanumeric
    characters respectively) must remain eligible for CALIBER_PART_NUMBER
    — this is exactly what the >=3 (not >=5) floor is calibrated to keep."""
    _seed_inventory(conn, [
        dict(canonical_inventory_id="rolex_24_5300", inventory_uid="iuid_530",
             brand="Rolex", caliber="24", part_number="530-0", stock=1, validation_status="PASS"),
        dict(canonical_inventory_id="rolex_25_104", inventory_uid="iuid_104",
             brand="Rolex", caliber="25", part_number="104", stock=1, validation_status="PASS"),
    ])
    _seed_active(conn, [
        _active_row(1, "Genuine Swiss Made Rolex Steel Crown 24-530-0 NOS Open Pack", item_id="item1"),
        _active_row(2, "Rolex Kunststoffglas Plexiglas 25-104 mit DATUM Top Qualitaet", item_id="item2"),
    ])
    m5.run_candidate_generation(conn, match_run_id="run1")
    methods_530 = {r[0] for r in conn.execute(
        "SELECT match_method FROM match_candidates_active WHERE inventory_uid='iuid_530'"
    ).fetchall()}
    methods_104 = {r[0] for r in conn.execute(
        "SELECT match_method FROM match_candidates_active WHERE inventory_uid='iuid_104'"
    ).fetchall()}
    assert "CALIBER_PART_NUMBER" in methods_530, "'530-0' (4 alnum chars) must remain eligible"
    assert "CALIBER_PART_NUMBER" in methods_104, "'104' (3 alnum chars) must remain eligible"
    # Neither is distinctive per the existing >=5 gate, so PART_NUMBER_EXACT/
    # BRAND_PART_NUMBER correctly do NOT fire -- confirms RULE 3 is genuinely
    # additive coverage, not a duplicate of the existing part-number rules.
    assert m5.utils.part_number_is_distinctive("530-0") is False
    assert m5.utils.part_number_is_distinctive("104") is False
    assert "PART_NUMBER_EXACT" not in methods_530
    assert "PART_NUMBER_EXACT" not in methods_104


def test_caliber_part_number_many_to_many(conn):
    """One evidence row maps to multiple inventory candidates when it
    genuinely mentions multiple compatible caliber+part-number pairs — the
    many-to-many contract (already proven for the original rules) must
    hold for CALIBER_PART_NUMBER too."""
    _seed_inventory(conn, [
        dict(canonical_inventory_id="rolex_24_5300", inventory_uid="iuid_a",
             brand="Rolex", caliber="24", part_number="530-0", stock=1, validation_status="PASS"),
        dict(canonical_inventory_id="rolex_24_5310", inventory_uid="iuid_b",
             brand="Rolex", caliber="24", part_number="531-0", stock=1, validation_status="PASS"),
    ])
    _seed_active(conn, [
        _active_row(1, "Rolex Crown 24-530-0 and 24-531-0 compatible NOS lot"),
    ])
    m5.run_candidate_generation(conn, match_run_id="run1")
    uids = conn.execute(
        "SELECT DISTINCT inventory_uid FROM match_candidates_active "
        "WHERE active_raw_id=1 AND match_method='CALIBER_PART_NUMBER' ORDER BY inventory_uid"
    ).fetchall()
    assert [u[0] for u in uids] == ["iuid_a", "iuid_b"]


def test_caliber_part_number_requires_caliber_present(conn):
    """No caliber on the inventory item -> CALIBER_PART_NUMBER must never
    fire, even with a qualifying part_number and a matching title."""
    _seed_inventory(conn, [dict(
        canonical_inventory_id="rolex_none_204", inventory_uid="iuid_1",
        brand="Rolex", caliber=None, part_number="204", stock=1, validation_status="PASS",
    )])
    _seed_active(conn, [_active_row(1, "Rolex 204 part genuine")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    methods = {r[0] for r in conn.execute(
        "SELECT match_method FROM match_candidates_active WHERE inventory_uid='iuid_1'"
    ).fetchall()}
    assert "CALIBER_PART_NUMBER" not in methods


def test_existing_rules_unchanged_by_caliber_part_number_addition(conn):
    """RULE 1 (PART_NUMBER_EXACT), RULE 2 (BRAND_PART_NUMBER), and RULE 4
    (CALIBER_EXACT/BRAND_CALIBER) must fire identically to their
    pre-RULE-3 behaviour — the new rule is additive only. Reuses the same
    fixture shape as test_grain_preservation_one_row_per_staged_row_per_rule
    to directly confirm all four original rules are still present alongside
    the new one, not replaced or altered."""
    _seed_inventory(conn, [dict(
        canonical_inventory_id="rolex_3135_12345", inventory_uid="iuid_1",
        brand="Rolex", caliber="3135", part_number="12345", stock=1, validation_status="PASS",
    )])
    _seed_active(conn, [_active_row(1, "Rolex 3135 bridge 12345")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    rows = {r[0]: json.loads(r[1]) for r in conn.execute(
        "SELECT match_method, evidence_json FROM match_candidates_active WHERE inventory_uid='iuid_1'"
    ).fetchall()}
    assert rows["PART_NUMBER_EXACT"] == {
        "rule": "PART_NUMBER_EXACT", "inventory_value": "12345",
        "title": "Rolex 3135 bridge 12345", "matched_tokens": ["12345"],
    }
    assert rows["BRAND_PART_NUMBER"] == {
        "rule": "BRAND_PART_NUMBER", "inventory_value": {"brand": "Rolex", "part_number": "12345"},
        "title": "Rolex 3135 bridge 12345", "matched_tokens": ["Rolex", "12345"],
    }
    assert rows["CALIBER_EXACT"] == {
        "rule": "CALIBER_EXACT", "inventory_value": "3135",
        "title": "Rolex 3135 bridge 12345", "matched_tokens": ["3135"],
    }
    assert rows["BRAND_CALIBER"] == {
        "rule": "BRAND_CALIBER", "inventory_value": {"brand": "Rolex", "caliber": "3135"},
        "title": "Rolex 3135 bridge 12345", "matched_tokens": ["Rolex", "3135"],
    }
    # And RULE 3 is present too, additive, not replacing any of the above.
    assert "CALIBER_PART_NUMBER" in rows


# ── Idempotency (content-level, across separate runs) ──────────────────────

def test_idempotency_content_identical_across_separate_runs(conn):
    """A NEW match_run_id produces its own tagged rows (match_run is an
    accumulating history table, like collection_batches — this is by
    design), but the underlying EVIDENCE CONTENT discovered must be
    identical to the first run's."""
    _seed_inventory(conn, [
        dict(canonical_inventory_id="rolex_3135_12345", inventory_uid="iuid_1",
             brand="Rolex", caliber="3135", part_number="12345", stock=1, validation_status="PASS"),
        dict(canonical_inventory_id="tudor_2824_5678", inventory_uid="iuid_2",
             brand="Tudor", caliber="2824", part_number="5678", stock=1, validation_status="WARNING"),
    ])
    _seed_active(conn, [
        _active_row(1, "Rolex 3135 bridge 12345"),
        _active_row(2, "Tudor 2824 crown 5678"),
        _active_row(3, "unrelated Omega listing"),
    ])

    def content():
        rows = conn.execute(
            "SELECT inventory_uid, active_raw_id, match_method, evidence_json FROM match_candidates_active"
        ).fetchall()
        return set(rows)

    m5.run_candidate_generation(conn, match_run_id="run1")
    content1 = content()
    m5.run_candidate_generation(conn, match_run_id="run2")
    content2 = content()

    assert content1 == content2, "the same evidence must be rediscovered identically across separate runs"
    assert len(content1) > 0


def test_evidence_json_reproducible(conn):
    _seed_inventory(conn, [dict(
        canonical_inventory_id="rolex_3135_12345", inventory_uid="iuid_1",
        brand="Rolex", caliber="3135", part_number="12345", stock=1, validation_status="PASS",
    )])
    _seed_active(conn, [_active_row(1, "Rolex 3135 bridge 12345")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    e1 = conn.execute(
        "SELECT evidence_json FROM match_candidates_active WHERE match_method='PART_NUMBER_EXACT'"
    ).fetchone()[0]
    m5.run_candidate_generation(conn, match_run_id="run2")
    e2 = conn.execute(
        "SELECT evidence_json FROM match_candidates_active WHERE match_method='PART_NUMBER_EXACT' AND match_run_id='run2'"
    ).fetchone()[0]
    assert e1 == e2


# ── Raw/staging immutability ────────────────────────────────────────────────

def test_raw_and_staging_tables_never_modified(conn):
    _seed_inventory(conn, [dict(
        canonical_inventory_id="rolex_3135_12345", inventory_uid="iuid_1",
        brand="Rolex", caliber="3135", part_number="12345", stock=1, validation_status="PASS",
    )])
    _seed_active(conn, [_active_row(1, "Rolex 3135 bridge 12345")])
    before_inv = conn.execute("SELECT * FROM staging_inventory ORDER BY inventory_uid").fetchall()
    before_active = conn.execute("SELECT * FROM stg_active_targeted ORDER BY id").fetchall()
    m5.run_candidate_generation(conn, match_run_id="run1")
    after_inv = conn.execute("SELECT * FROM staging_inventory ORDER BY inventory_uid").fetchall()
    after_active = conn.execute("SELECT * FROM stg_active_targeted ORDER BY id").fetchall()
    assert before_inv == after_inv
    assert before_active == after_active


def test_match_run_recorded(conn):
    _seed_inventory(conn, [dict(
        canonical_inventory_id="rolex_3135_12345", inventory_uid="iuid_1",
        brand="Rolex", caliber="3135", part_number="12345", stock=1, validation_status="PASS",
    )])
    _seed_active(conn, [_active_row(1, "Rolex 3135 bridge 12345")])
    m5.run_candidate_generation(conn, match_run_id="run1", inventory_snapshot_reference="test_snapshot")
    row = conn.execute(
        "SELECT match_run_id, algorithm_version, inventory_snapshot_reference FROM match_run WHERE match_run_id='run1'"
    ).fetchone()
    assert row == ("run1", m5.ALGORITHM_VERSION, "test_snapshot")


# ── Module 5 evidence identity: evidence_uid propagation ────────────────────

def test_evidence_uid_populated_from_staging_stable_evidence_uid(conn):
    _seed_inventory(conn, [dict(
        canonical_inventory_id="rolex_3135_12345", inventory_uid="iuid_1",
        brand="Rolex", caliber="3135", part_number="12345", stock=1, validation_status="PASS",
    )])
    row = _active_row(1, "Rolex 3135 bridge 12345")
    row["stable_evidence_uid"] = "EV-ACTIVE-testfixedvalue"
    _seed_active(conn, [row])
    m5.run_candidate_generation(conn, match_run_id="run1")
    result = conn.execute(
        "SELECT evidence_uid, active_raw_id FROM match_candidates_active WHERE match_run_id='run1'"
    ).fetchall()
    assert len(result) >= 1
    for evidence_uid, active_raw_id in result:
        assert evidence_uid == "EV-ACTIVE-testfixedvalue"
        assert active_raw_id == 1  # legacy positional column still populated, untouched


def test_evidence_uid_stable_when_positional_id_would_differ_across_runs(conn):
    """The direct regression test for the lineage defect
    (docs/MODULE5_LINEAGE_INTEGRITY_AUDIT.md): two 'runs' where the same
    real-world listing gets a DIFFERENT positional id (simulating a
    staging rebuild reassigning ids) must still produce the SAME
    evidence_uid, because evidence_uid is derived from content
    (marketplace + item_id), not position."""
    _seed_inventory(conn, [dict(
        canonical_inventory_id="rolex_3135_12345", inventory_uid="iuid_1",
        brand="Rolex", caliber="3135", part_number="12345", stock=1, validation_status="PASS",
    )])
    row_run1 = _active_row(1, "Rolex 3135 bridge 12345", item_id="item_XYZ")
    row_run1["stable_evidence_uid"] = ei.active_evidence_uid("EBAY_DE", "item_XYZ")
    _seed_active(conn, [row_run1])
    m5.run_candidate_generation(conn, match_run_id="run1")

    # Simulate a staging rebuild: DELETE + reinsert the SAME listing under
    # a DIFFERENT positional id (99 instead of 1) — exactly what
    # clean_active_targeted()'s old range(1, len(df)+1) pattern could do.
    conn.execute("DELETE FROM stg_active_targeted")
    row_run2 = _active_row(99, "Rolex 3135 bridge 12345", item_id="item_XYZ")
    row_run2["stable_evidence_uid"] = ei.active_evidence_uid("EBAY_DE", "item_XYZ")
    _seed_active(conn, [row_run2])
    m5.run_candidate_generation(conn, match_run_id="run2")

    uids = conn.execute(
        "SELECT DISTINCT evidence_uid FROM match_candidates_active WHERE match_run_id IN ('run1','run2')"
    ).fetchall()
    assert len(uids) == 1, f"evidence_uid changed across a simulated staging rebuild: {uids}"


# ── Module 5 post-Phase-1 fix: candidate relationship grain ─────────────────

def test_one_listing_multiple_pairing_rows_produces_one_candidate_per_rule(conn):
    """docs/MODULE5_POST_PHASE1_ARCHITECTURE_AUDIT.md Bug 1, reproduced and
    fixed: one real-world listing represented by multiple stg_active_targeted
    rows (because multiple different inventory items' collection queries
    independently found it -- confirmed common, up to 8 pairing-rows per
    listing in the pilot data) must produce exactly ONE candidate per rule
    for a given (inventory_uid, evidence_uid), not one per pairing-row."""
    _seed_inventory(conn, [dict(
        canonical_inventory_id="rolex_3135_99999_Z", inventory_uid="iuid_Z",
        brand="Rolex", caliber="3135", part_number="99999", stock=1, validation_status="PASS",
    )])
    uid = ei.active_evidence_uid("EBAY_DE", "listing_SHARED")
    rows = []
    for id_, inv_uid in [(1, "iuid_A"), (2, "iuid_B"), (3, "iuid_C")]:
        row = _active_row(id_, "Rolex 3135 99999 crown genuine", item_id="listing_SHARED")
        row["inventory_uid"] = inv_uid
        row["stable_evidence_uid"] = uid
        rows.append(row)
    _seed_active(conn, rows)

    m5.run_candidate_generation(conn, match_run_id="run1")

    result = conn.execute(
        "SELECT match_method, COUNT(*) FROM match_candidates_active "
        "WHERE match_run_id='run1' AND inventory_uid='iuid_Z' GROUP BY match_method"
    ).fetchall()
    over_counted = [r for r in result if r[1] > 1]
    assert not over_counted, (
        f"one evidence identity produced more than one candidate for the same rule: {over_counted} "
        f"-- three pairing-rows (ids 1,2,3) sharing evidence_uid={uid} were not deduplicated"
    )
    assert len(result) >= 1, "expected at least one rule to fire"


def test_dedup_tie_break_is_deterministic_across_runs(conn):
    """Which pairing-row's positional id survives dedup must be stable
    across repeated runs, not scan-order-dependent (the smallest
    source_id wins, by explicit stable sort before drop_duplicates)."""
    _seed_inventory(conn, [dict(
        canonical_inventory_id="rolex_3135_99999_Z", inventory_uid="iuid_Z",
        brand="Rolex", caliber="3135", part_number="99999", stock=1, validation_status="PASS",
    )])
    uid = ei.active_evidence_uid("EBAY_DE", "listing_SHARED")
    rows = []
    for id_, inv_uid in [(5, "iuid_A"), (2, "iuid_B"), (9, "iuid_C")]:
        row = _active_row(id_, "Rolex 3135 99999 crown genuine", item_id="listing_SHARED")
        row["inventory_uid"] = inv_uid
        row["stable_evidence_uid"] = uid
        rows.append(row)
    _seed_active(conn, rows)

    m5.run_candidate_generation(conn, match_run_id="run1")
    kept_ids = conn.execute(
        "SELECT DISTINCT active_raw_id FROM match_candidates_active "
        "WHERE match_run_id='run1' AND inventory_uid='iuid_Z'"
    ).fetchall()
    assert kept_ids == [(2,)], f"expected the smallest source_id (2) to deterministically survive dedup, got {kept_ids}"
