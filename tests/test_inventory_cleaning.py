"""
tests/test_inventory_cleaning.py
=================================
Pytest tests for clean_inventory() and the Module 2 scenario-price helpers
in scripts/02_clean.py and scripts/utils.py.

Run with:
    pytest tests/test_inventory_cleaning.py

Isolation: every test runs against a duckdb file under pytest's own
tmp_path_factory directory and an isolated reports_dir passed explicitly
into clean_inventory() — never database/watchparts.duckdb, never
reports/inventory_validation_report.csv. A module-scoped autouse fixture
hashes/stats the real project files before and after this file's tests run
and fails loudly if any of them changed, so a future regression here can't
silently start touching production files again.

Builds a throwaway DuckDB database seeded with synthetic raw_inventory rows
that reproduce the real cases found in the live data (the actual
corrupted-date row, the actual blank-calibre duplicate pair, the actual
corrupted part-number shape), plus a correction scenario to verify
inventory_uid stability via the registry.
"""

import hashlib
import importlib.util
import inspect
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
    sys.path.insert(0, str(SCRIPTS_DIR))  # so `import utils` inside 02_clean.py resolves
    spec = importlib.util.spec_from_file_location("clean02", SCRIPTS_DIR / "02_clean.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


clean02 = _load_clean_module()
utils = clean02.utils

PASS = "PASS"
WARNING = "WARNING"
FAIL = "FAIL"

SEED_ROWS = [
    # mirrors the real corrupted-date caliber row (Tudor / 7750-01-01 00:00:00 / 24-T7500-4H)
    dict(id=1, upload_batch_id="batch_test1", source_filename="inventory.csv",
         raw_rolex_tudor="Tudor", raw_calibre="7750-01-01 00:00:00", raw_p_number="24-T7500-4H", raw_stock="2"),
    # blank-calibre duplicate pair — mirrors the real Rolex/blank/24-603-0 rows (4 + 1 -> 5)
    dict(id=2, upload_batch_id="batch_test1", source_filename="inventory.csv",
         raw_rolex_tudor="Rolex", raw_calibre="", raw_p_number="24-603-0", raw_stock="4"),
    dict(id=3, upload_batch_id="batch_test1", source_filename="inventory.csv",
         raw_rolex_tudor="Rolex", raw_calibre="", raw_p_number="24-603-0", raw_stock="1"),
    # a normal, valid row
    dict(id=4, upload_batch_id="batch_test1", source_filename="inventory.csv",
         raw_rolex_tudor="Rolex", raw_calibre="1030", raw_p_number="123-456", raw_stock="7"),
    # row that will be corrected in a later batch
    dict(id=5, upload_batch_id="batch_test1", source_filename="inventory.csv",
         raw_rolex_tudor="Rolex", raw_calibre="99", raw_p_number="TESTPN001", raw_stock="3"),
    # mirrors a real corrupted-date part number (e.g. Rolex/1120/6655-01-01 00:00:00)
    dict(id=6, upload_batch_id="batch_test1", source_filename="inventory.csv",
         raw_rolex_tudor="Rolex", raw_calibre="1120", raw_p_number="6655-01-01 00:00:00", raw_stock="2"),
]


def _seed_raw_inventory(connection) -> None:
    df = pd.DataFrame(SEED_ROWS)
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


def get_report_status(conn, row_id, check_name):
    result = conn.execute(
        "SELECT check_status FROM inventory_validation_report WHERE row_id = ? AND check_name = ?",
        [row_id, check_name],
    ).fetchone()
    return result[0] if result else None


def _file_digest(path: Path) -> str | None:
    if not path.exists():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


# ── Isolation guard: fails loudly if any test in this file touches the real
# project's database, log file, or report — instead of silently passing. ──────

@pytest.fixture(scope="module", autouse=True)
def guard_production_files_untouched():
    real_db = clean02.DB_PATH
    real_report = clean02.REPORTS_DIR / "inventory_validation_report.csv"
    real_log = clean02.LOG_DIR / "02_clean.log"

    before = {
        "db": _file_digest(real_db),
        "report": _file_digest(real_report),
        "log_mtime": real_log.stat().st_mtime if real_log.exists() else None,
    }

    yield

    after = {
        "db": _file_digest(real_db),
        "report": _file_digest(real_report),
        "log_mtime": real_log.stat().st_mtime if real_log.exists() else None,
    }

    assert before["db"] == after["db"], "database/watchparts.duckdb changed — test isolation is broken"
    assert before["report"] == after["report"], "reports/inventory_validation_report.csv changed — test isolation is broken"
    assert before["log_mtime"] == after["log_mtime"], "logs/02_clean.log changed — test isolation is broken"


@pytest.fixture(scope="module")
def clean_paths(tmp_path_factory):
    base = tmp_path_factory.mktemp("inventory_cleaning_test")
    db_path = base / "test.duckdb"
    reports_dir = base / "reports"

    # Verified, not assumed: the isolated paths must not resolve to the real ones.
    assert db_path.resolve() != clean02.DB_PATH.resolve()
    assert reports_dir.resolve() != clean02.REPORTS_DIR.resolve()

    return {"db_path": db_path, "reports_dir": reports_dir}


def test_isolated_paths_are_not_production_paths(clean_paths):
    assert clean_paths["db_path"] != clean02.DB_PATH
    assert clean_paths["reports_dir"] != clean02.REPORTS_DIR


@pytest.fixture(scope="module")
def conn(clean_paths):
    """
    Tests below share one connection and run in file order (top to bottom):
    the correction test mutates state (adds a correction, re-runs
    clean_inventory), and the idempotency test depends on running after it.
    """
    connection = duckdb.connect(str(clean_paths["db_path"]))
    connection.execute(SCHEMA_PATH.read_text())
    _seed_raw_inventory(connection)
    clean02.clean_inventory(connection, reports_dir=clean_paths["reports_dir"])
    yield connection
    connection.close()


# ── clean_inventory() behavior ────────────────────────────────────────────────

def test_corrupted_date_row_fails(conn):
    status = get_report_status(conn, 1, "caliber")
    assert status == FAIL, f"Expected FAIL for corrupted-date caliber row, got {status}"
    caliber_value = conn.execute(
        "SELECT caliber FROM staging_inventory WHERE canonical_inventory_id LIKE 'tudor_unknown_24_t7500_4h%'"
    ).fetchone()
    assert caliber_value is not None, "Expected a staging_inventory row for the Tudor corrupted-date item"
    assert caliber_value[0] is None, f"Expected caliber IS NULL for corrupted-date row, got {caliber_value[0]!r}"


def test_blank_calibre_warning(conn):
    status2 = get_report_status(conn, 2, "caliber")
    status3 = get_report_status(conn, 3, "caliber")
    assert status2 == WARNING, f"Expected WARNING for blank calibre row 2, got {status2}"
    assert status3 == WARNING, f"Expected WARNING for blank calibre row 3, got {status3}"
    row = conn.execute(
        "SELECT caliber FROM staging_inventory WHERE canonical_inventory_id = 'rolex_unknown_24_603_0'"
    ).fetchone()
    assert row is not None, "Expected merged rolex_unknown_24_603_0 row in staging_inventory"
    assert row[0] is None, f"Expected caliber IS NULL, got {row[0]!r}"


def test_duplicate_aggregation_sums_to_5(conn):
    row = conn.execute(
        "SELECT stock FROM staging_inventory WHERE canonical_inventory_id = 'rolex_unknown_24_603_0'"
    ).fetchone()
    assert row is not None, "Expected merged rolex_unknown_24_603_0 row"
    assert row[0] == 5, f"Expected stock 4+1=5, got {row[0]}"


def test_no_literal_unknown_string_in_caliber(conn):
    count = conn.execute("SELECT COUNT(*) FROM staging_inventory WHERE caliber = 'UNKNOWN'").fetchone()[0]
    assert count == 0, f"Expected zero literal 'UNKNOWN' caliber values, found {count}"


def test_corrupted_part_number_becomes_null(conn):
    status = get_report_status(conn, 6, "part_number")
    assert status == FAIL, f"Expected FAIL for corrupted-date part number row, got {status}"

    staged = conn.execute(
        "SELECT part_number FROM staging_inventory WHERE canonical_inventory_id LIKE 'rolex_1120_unknown%'"
    ).fetchone()
    assert staged is not None, "Expected a staging_inventory row for the corrupted part-number item"
    assert staged[0] is None, f"Expected part_number IS NULL in staging_inventory, got {staged[0]!r}"

    # raw value must still be fully intact elsewhere
    raw_value = conn.execute("SELECT raw_p_number FROM raw_inventory WHERE id = 6").fetchone()[0]
    assert raw_value == "6655-01-01 00:00:00", "raw_inventory must never be modified"

    report_raw_value = conn.execute(
        "SELECT raw_value FROM inventory_validation_report WHERE row_id = 6 AND check_name = 'part_number'"
    ).fetchone()[0]
    assert report_raw_value == "6655-01-01 00:00:00", "validation report must preserve the raw corrupted text"


def test_stock_history_has_populated_inventory_uid(conn):
    rows = conn.execute("SELECT canonical_inventory_id, inventory_uid FROM inventory_stock_history").fetchall()
    assert rows, "Expected inventory_stock_history to have rows"
    for canonical_id, inventory_uid in rows:
        assert inventory_uid, f"Expected populated inventory_uid for {canonical_id}, got {inventory_uid!r}"


def test_correction_preserves_inventory_uid_via_registry(conn, clean_paths):
    # Before correction: canonical id is rolex_99_testpn001
    before = conn.execute(
        "SELECT inventory_uid FROM staging_inventory WHERE canonical_inventory_id = 'rolex_99_testpn001'"
    ).fetchone()
    assert before is not None, "Expected pre-correction row rolex_99_testpn001"
    uid_before = before[0]
    assert uid_before, "Expected a non-empty inventory_uid before correction"

    registry_before = conn.execute(
        "SELECT inventory_uid FROM inventory_uid_registry WHERE raw_inventory_id = 5"
    ).fetchone()
    assert registry_before is not None, "Expected a registry entry anchored on raw_inventory_id=5"
    assert registry_before[0] == uid_before, "Registry entry should match the assigned inventory_uid"

    # Apply a correction that changes the resulting canonical_inventory_id
    conn.execute(
        """
        INSERT INTO inventory_corrections (raw_inventory_id, corrected_caliber, corrected_by, notes)
        VALUES (5, '100', 'test_suite', 'change calibre 99 -> 100')
        """
    )
    clean02.clean_inventory(conn, reports_dir=clean_paths["reports_dir"])

    old_id_row = conn.execute(
        "SELECT * FROM staging_inventory WHERE canonical_inventory_id = 'rolex_99_testpn001'"
    ).fetchone()
    assert old_id_row is None, "Old canonical id should no longer exist after the correction"

    after = conn.execute(
        "SELECT inventory_uid FROM staging_inventory WHERE canonical_inventory_id = 'rolex_100_testpn001'"
    ).fetchone()
    assert after is not None, "Expected new canonical id rolex_100_testpn001 after correction"
    uid_after = after[0]

    assert uid_after == uid_before, (
        f"inventory_uid should survive the correction via the registry anchor: "
        f"before={uid_before!r} after={uid_after!r}"
    )

    registry_after = conn.execute(
        "SELECT inventory_uid FROM inventory_uid_registry WHERE raw_inventory_id = 5"
    ).fetchone()
    assert registry_after[0] == uid_before, "Registry entry for anchor 5 must be unchanged"


def test_idempotent_repeat_run(conn, clean_paths):
    before_count = conn.execute("SELECT COUNT(*) FROM staging_inventory").fetchone()[0]
    before_uids = conn.execute(
        "SELECT canonical_inventory_id, inventory_uid FROM staging_inventory ORDER BY canonical_inventory_id"
    ).fetchall()
    before_history_count = conn.execute("SELECT COUNT(*) FROM inventory_stock_history").fetchone()[0]

    clean02.clean_inventory(conn, reports_dir=clean_paths["reports_dir"])  # run again, nothing changed

    after_count = conn.execute("SELECT COUNT(*) FROM staging_inventory").fetchone()[0]
    after_uids = conn.execute(
        "SELECT canonical_inventory_id, inventory_uid FROM staging_inventory ORDER BY canonical_inventory_id"
    ).fetchall()
    after_history_count = conn.execute("SELECT COUNT(*) FROM inventory_stock_history").fetchone()[0]

    assert before_count == after_count, f"Row count changed on repeat run: {before_count} -> {after_count}"
    assert before_uids == after_uids, "inventory_uid assignments changed on repeat run"
    assert before_history_count == after_history_count, (
        f"inventory_stock_history grew on a repeat run with no new batch: "
        f"{before_history_count} -> {after_history_count}"
    )


# ── utils.compute_scenario_prices() — no db needed ────────────────────────────

def test_scenario_price_virtual_equals_price_eur():
    result = utils.compute_scenario_prices(100.0)
    assert result["price_virtual_eur"] == 100.0


def test_scenario_prices_never_use_actual_shipping():
    sig = inspect.signature(utils.compute_scenario_prices)
    assert "shipping_eur_actual" not in sig.parameters, (
        "compute_scenario_prices must not accept a shipping_eur_actual parameter"
    )
    assert list(sig.parameters) == ["price_eur"], f"Unexpected signature: {sig}"

    # Same price must produce identical scenario outputs regardless of any
    # notion of "actual" shipping — there is no such input to vary.
    result_a = utils.compute_scenario_prices(100.0)
    result_b = utils.compute_scenario_prices(100.0)
    assert result_a == result_b


def test_scenario_price_formula():
    result = utils.compute_scenario_prices(100.0)
    assert result["shipping_de_eur"] == utils.SHIPPING_DE_EUR
    assert result["shipping_us_eur"] == utils.SHIPPING_US_EUR
    assert result["landed_cost_de_eur"] == 100.0 + utils.SHIPPING_DE_EUR
    expected_import = 100.0 * (utils.US_DUTY_RATE + utils.US_SALES_TAX_RATE)
    assert result["estimated_import_charges_us_eur"] == round(expected_import, 2)
    assert result["landed_cost_us_eur"] == round(100.0 + utils.SHIPPING_US_EUR + expected_import, 2)


def test_scenario_prices_missing_price_returns_none():
    result = utils.compute_scenario_prices(None)
    assert result["price_virtual_eur"] is None
    assert result["landed_cost_de_eur"] is None
    assert result["landed_cost_us_eur"] is None
