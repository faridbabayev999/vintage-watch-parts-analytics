"""
tests/test_generate_queries.py
===============================
Pytest tests for generate_search_queries() in scripts/03_generate_queries.py.

Run with:
    pytest tests/test_generate_queries.py

Isolation: every test runs against a duckdb file and reports_dir under
pytest's own tmp_path_factory directory — never database/watchparts.duckdb,
never reports/search_queries_worklist.csv. A module-scoped autouse fixture
hashes/stats the real project files before and after this file's tests run
and fails loudly if any of them changed.
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


def _load_module(name: str, filename: str):
    sys.path.insert(0, str(SCRIPTS_DIR))  # so `import utils` inside the loaded module resolves
    spec = importlib.util.spec_from_file_location(name, SCRIPTS_DIR / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


gen03 = _load_module("gen03", "03_generate_queries.py")

# Seeded directly into staging_inventory — 03_generate_queries.py only ever
# reads staging_inventory, so there's no need to go through raw ingestion
# and clean_inventory() to exercise it.
SEED_ROWS = [
    dict(
        canonical_inventory_id="rolex_1030_6941", inventory_uid="uid_pass_1",
        upload_batch_id="batch_test1", brand="Rolex", caliber="1030", part_number="6941",
        stock=8, validation_status="PASS", part_number_is_distinctive=False,
    ),
    # real distinctive part number from the live data (blank-caliber WARNING row)
    dict(
        canonical_inventory_id="rolex_unknown_24_26812_8", inventory_uid="uid_warning_1",
        upload_batch_id="batch_test1", brand="Rolex", caliber=None, part_number="24-26812-8",
        stock=2, validation_status="WARNING", part_number_is_distinctive=True,
    ),
    dict(
        canonical_inventory_id="rolex_1120_unknown", inventory_uid="uid_fail_1",
        upload_batch_id="batch_test1", brand="Rolex", caliber="1120", part_number=None,
        stock=2, validation_status="FAIL", part_number_is_distinctive=False,
    ),
]


def _seed_staging_inventory(connection) -> None:
    df = pd.DataFrame(SEED_ROWS)
    df["condition"] = None
    df["source_filename"] = "inventory.csv"
    df["ingested_at"] = "2026-01-01T00:00:00"
    connection.register("tmp_seed", df)
    cols = [
        "canonical_inventory_id", "upload_batch_id", "brand", "caliber", "part_number",
        "stock", "condition", "source_filename", "ingested_at",
        "inventory_uid", "validation_status", "part_number_is_distinctive",
    ]
    connection.execute(
        f"INSERT INTO staging_inventory ({','.join(cols)}) SELECT {','.join(cols)} FROM tmp_seed"
    )
    connection.unregister("tmp_seed")


def _file_digest(path: Path) -> str | None:
    if not path.exists():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


# ── Isolation guard ────────────────────────────────────────────────────────────

@pytest.fixture(scope="module", autouse=True)
def guard_production_files_untouched():
    real_db = gen03.DB_PATH
    real_worklist = gen03.REPORTS_DIR / "search_queries_worklist.csv"
    real_log = gen03.LOG_DIR / "03_generate_queries.log"

    before = {
        "db": _file_digest(real_db),
        "worklist": _file_digest(real_worklist),
        "log_mtime": real_log.stat().st_mtime if real_log.exists() else None,
    }

    yield

    after = {
        "db": _file_digest(real_db),
        "worklist": _file_digest(real_worklist),
        "log_mtime": real_log.stat().st_mtime if real_log.exists() else None,
    }

    assert before["db"] == after["db"], "database/watchparts.duckdb changed — test isolation is broken"
    assert before["worklist"] == after["worklist"], "reports/search_queries_worklist.csv changed — test isolation is broken"
    assert before["log_mtime"] == after["log_mtime"], "logs/03_generate_queries.log changed — test isolation is broken"


@pytest.fixture(scope="module")
def clean_paths(tmp_path_factory):
    base = tmp_path_factory.mktemp("generate_queries_test")
    db_path = base / "test.duckdb"
    reports_dir = base / "reports"

    assert db_path.resolve() != gen03.DB_PATH.resolve()
    assert reports_dir.resolve() != gen03.REPORTS_DIR.resolve()

    return {"db_path": db_path, "reports_dir": reports_dir}


def test_isolated_paths_are_not_production_paths(clean_paths):
    assert clean_paths["db_path"] != gen03.DB_PATH
    assert clean_paths["reports_dir"] != gen03.REPORTS_DIR


@pytest.fixture(scope="module")
def conn(clean_paths):
    connection = duckdb.connect(str(clean_paths["db_path"]))
    connection.execute(SCHEMA_PATH.read_text())
    _seed_staging_inventory(connection)
    yield connection
    connection.close()


def query_texts_for(conn, inventory_uid, tier=None):
    if tier is None:
        rows = conn.execute(
            "SELECT tier, query_text, uses_lexicon FROM search_queries WHERE inventory_uid = ? ORDER BY tier, query_text",
            [inventory_uid],
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT tier, query_text, uses_lexicon FROM search_queries WHERE inventory_uid = ? AND tier = ? ORDER BY query_text",
            [inventory_uid, tier],
        ).fetchall()
    return rows


# ── Tier generation behavior ───────────────────────────────────────────────────

def test_pass_row_gets_tiers_1_2_4_and_empty_tier_3(conn, clean_paths):
    gen03.generate_search_queries(conn, reports_dir=clean_paths["reports_dir"])

    rows = query_texts_for(conn, "uid_pass_1")
    tiers_present = sorted(set(r[0] for r in rows))
    assert tiers_present == [1, 2, 4], f"Expected tiers [1, 2, 4] for PASS row, got {tiers_present}"

    tier1 = query_texts_for(conn, "uid_pass_1", tier=1)
    assert len(tier1) == 1
    assert tier1[0][1] == "Rolex 1030 6941"
    assert tier1[0][2] is False  # uses_lexicon

    tier2 = query_texts_for(conn, "uid_pass_1", tier=2)
    assert len(tier2) == 4, f"Expected 4 Tier 2 rows (one per prefix), got {len(tier2)}"
    tier2_texts = {r[1] for r in tier2}
    assert tier2_texts == {
        "Rolex Cal 1030 6941", "Rolex Calibre 1030 6941",
        "Rolex Caliber 1030 6941", "Rolex Kaliber 1030 6941",
    }

    tier4 = query_texts_for(conn, "uid_pass_1", tier=4)
    assert len(tier4) == 1
    assert tier4[0][1] == "Rolex 1030"

    # Tier 3 attempted (lexicon lookup) but empty lexicon -> no rows, no error
    tier3 = query_texts_for(conn, "uid_pass_1", tier=3)
    assert tier3 == []


def test_warning_row_gets_only_tier_5(conn, clean_paths):
    gen03.generate_search_queries(conn, reports_dir=clean_paths["reports_dir"])

    rows = query_texts_for(conn, "uid_warning_1")
    tiers_present = sorted(set(r[0] for r in rows))
    assert tiers_present == [5], f"Expected only Tier 5 for WARNING row, got {tiers_present}"
    assert len(rows) == 1
    assert rows[0][1] == "24-26812-8"
    assert rows[0][2] is False


def test_fail_row_gets_zero_queries_and_manual_review(conn, clean_paths):
    result = gen03.generate_search_queries(conn, reports_dir=clean_paths["reports_dir"])

    rows = query_texts_for(conn, "uid_fail_1")
    assert rows == [], f"Expected zero queries for FAIL row, got {rows}"

    manual_review_uids = {item["inventory_uid"] for item in result["manual_review"]}
    assert "uid_fail_1" in manual_review_uids


# ── inventory_uid-keyed stability across a correction ─────────────────────────

def test_correction_does_not_orphan_queries(conn, clean_paths):
    gen03.generate_search_queries(conn, reports_dir=clean_paths["reports_dir"])
    before_rows = set(query_texts_for(conn, "uid_pass_1"))
    assert before_rows, "Expected queries for uid_pass_1 before the correction"

    # Simulate a correction changing this item's canonical_inventory_id
    # while its physical identity (inventory_uid) stays the same.
    conn.execute(
        "UPDATE staging_inventory SET canonical_inventory_id = 'rolex_1030_6941_corrected' "
        "WHERE inventory_uid = 'uid_pass_1'"
    )
    gen03.generate_search_queries(conn, reports_dir=clean_paths["reports_dir"])

    after_rows = set(query_texts_for(conn, "uid_pass_1"))
    assert after_rows == before_rows, (
        "Query tiers/text for uid_pass_1 should be unchanged by a canonical_inventory_id-only correction"
    )

    stale_canonical_rows = conn.execute(
        "SELECT COUNT(*) FROM search_queries WHERE canonical_inventory_id = 'rolex_1030_6941'"
    ).fetchone()[0]
    assert stale_canonical_rows == 0, "Old canonical_inventory_id must not linger anywhere in search_queries"

    fresh_canonical_rows = conn.execute(
        "SELECT COUNT(*) FROM search_queries WHERE inventory_uid = 'uid_pass_1' "
        "AND canonical_inventory_id = 'rolex_1030_6941_corrected'"
    ).fetchone()[0]
    assert fresh_canonical_rows == len(before_rows), "All of uid_pass_1's queries should reflect the corrected canonical id"

    # revert for subsequent tests
    conn.execute(
        "UPDATE staging_inventory SET canonical_inventory_id = 'rolex_1030_6941' "
        "WHERE inventory_uid = 'uid_pass_1'"
    )
    gen03.generate_search_queries(conn, reports_dir=clean_paths["reports_dir"])


# ── Rebuild vs. preserve split ──────────────────────────────────────────────────

def test_search_queries_fully_rebuilt_identically_twice(conn, clean_paths):
    gen03.generate_search_queries(conn, reports_dir=clean_paths["reports_dir"])
    first = set(
        conn.execute(
            "SELECT inventory_uid, canonical_inventory_id, tier, query_text, uses_lexicon FROM search_queries"
        ).fetchall()
    )
    first_count = conn.execute("SELECT COUNT(*) FROM search_queries").fetchone()[0]

    gen03.generate_search_queries(conn, reports_dir=clean_paths["reports_dir"])
    second = set(
        conn.execute(
            "SELECT inventory_uid, canonical_inventory_id, tier, query_text, uses_lexicon FROM search_queries"
        ).fetchall()
    )
    second_count = conn.execute("SELECT COUNT(*) FROM search_queries").fetchone()[0]

    assert first_count == second_count, f"search_queries row count changed: {first_count} -> {second_count}"
    assert first == second, "search_queries content changed between two identical runs"


def test_historical_extraction_status_byte_identical_across_repeat_run(conn, clean_paths):
    gen03.generate_search_queries(conn, reports_dir=clean_paths["reports_dir"])
    before = conn.execute(
        "SELECT inventory_uid, canonical_inventory_id, time_bucket, extraction_status, "
        "source_filename, extraction_date, ingestion_status, notes "
        "FROM historical_extraction_status ORDER BY inventory_uid, time_bucket"
    ).fetchall()

    gen03.generate_search_queries(conn, reports_dir=clean_paths["reports_dir"])  # repeat run, nothing new
    after = conn.execute(
        "SELECT inventory_uid, canonical_inventory_id, time_bucket, extraction_status, "
        "source_filename, extraction_date, ingestion_status, notes "
        "FROM historical_extraction_status ORDER BY inventory_uid, time_bucket"
    ).fetchall()

    assert before == after, "historical_extraction_status rows changed on a repeat run — must be append-only"


def test_historical_extraction_status_has_one_row_per_item(conn, clean_paths):
    gen03.generate_search_queries(conn, reports_dir=clean_paths["reports_dir"])
    count = conn.execute("SELECT COUNT(*) FROM historical_extraction_status").fetchone()[0]
    assert count == len(SEED_ROWS)
    statuses = conn.execute("SELECT DISTINCT extraction_status FROM historical_extraction_status").fetchall()
    assert statuses == [("not_started",)]
