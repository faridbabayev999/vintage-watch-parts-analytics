"""
tests/test_pilot_cohort_manifest.py
=====================================
Focused tests for scripts/08_pilot_cohort_manifest.py -- pure selection
logic, never calls the eBay API, never writes to any staging table.
"""
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


def _load_module(name, path):
    sys.path.insert(0, str(SCRIPTS_DIR))
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


m5 = _load_module("m5c_pilot", SCRIPTS_DIR / "05_generate_match_candidates.py")
dec = _load_module("m6d_pilot", SCRIPTS_DIR / "06_decide_matches.py")
pilot = _load_module("m8pilot", SCRIPTS_DIR / "08_pilot_cohort_manifest.py")


def _inv(uid, brand="Rolex", caliber="3135", part_number="12345", stock=1):
    return dict(
        canonical_inventory_id=f"{brand}_{caliber}_{part_number}_{uid}", inventory_uid=uid,
        brand=brand, caliber=caliber, part_number=part_number, stock=stock, validation_status="PASS",
    )


def _seed_inventory(connection, rows):
    df = pd.DataFrame(rows)
    df["upload_batch_id"] = "batch_test1"
    df["source_filename"] = "inventory.csv"
    connection.register("tmp_seed", df)
    cols = ["canonical_inventory_id", "inventory_uid", "brand", "caliber", "part_number",
            "stock", "validation_status", "upload_batch_id", "source_filename"]
    connection.execute(f"INSERT INTO staging_inventory ({','.join(cols)}) SELECT {','.join(cols)} FROM tmp_seed")
    connection.unregister("tmp_seed")


@pytest.fixture()
def conn(tmp_path):
    db_path = tmp_path / "test.duckdb"
    connection = duckdb.connect(str(db_path))
    connection.execute(SCHEMA_PATH.read_text())
    yield connection
    connection.close()


def test_manifest_excludes_already_self_sourced_items(conn):
    _seed_inventory(conn, [_inv("iuid_self"), _inv("iuid_none", part_number="99988zz")])
    conn.execute("""
        INSERT INTO stg_active_targeted (id, raw_id, inventory_uid, item_id, title, normalized_title, marketplace)
        VALUES (1, 1, 'iuid_self', 'item1', 'Rolex 3135 bridge 12345 genuine', 'rolex 3135 bridge 12345 genuine', 'EBAY_DE')
    """)
    m5.run_candidate_generation(conn, match_run_id="run1")
    dec.run_decision_layer(conn, decision_run_id="d1")
    manifest = pilot.build_manifest(conn, pilot_size=10)
    assert "iuid_self" not in set(manifest["inventory_uid"])


def test_manifest_deterministic_across_reruns(conn):
    rows = [_inv(f"iuid_{i}", part_number=f"999{i:02d}zz") for i in range(30)]
    _seed_inventory(conn, rows)
    dec.run_decision_layer(conn, decision_run_id="d1")
    m1 = pilot.build_manifest(conn, pilot_size=10)
    m2 = pilot.build_manifest(conn, pilot_size=10)
    pd.testing.assert_frame_equal(m1.reset_index(drop=True), m2.reset_index(drop=True))


def test_manifest_reports_empty_category_honestly_not_fabricated(conn):
    """No Tier-C-only items in a tiny synthetic population -> the C stratum
    must not appear with fabricated rows."""
    rows = [_inv(f"iuid_{i}", part_number=f"999{i:02d}zz") for i in range(5)]
    _seed_inventory(conn, rows)
    dec.run_decision_layer(conn, decision_run_id="d1")
    manifest = pilot.build_manifest(conn, pilot_size=10)
    assert "C_TIER_C_ONLY" not in set(manifest["stratum"])


def test_manifest_never_duplicates_an_inventory_uid(conn):
    rows = [_inv(f"iuid_{i}", part_number=f"999{i:02d}zz") for i in range(40)]
    _seed_inventory(conn, rows)
    dec.run_decision_layer(conn, decision_run_id="d1")
    manifest = pilot.build_manifest(conn, pilot_size=20)
    assert manifest["inventory_uid"].duplicated().sum() == 0


def test_manifest_no_candidates_stratum_only_contains_items_with_zero_decisions(conn):
    _seed_inventory(conn, [_inv("iuid_orphan", part_number="zzzznone1234zzzz")])
    dec.run_decision_layer(conn, decision_run_id="d1")
    manifest = pilot.build_manifest(conn, pilot_size=10)
    no_cand_rows = manifest[manifest["stratum"] == "D_NO_CANDIDATES"]
    assert set(no_cand_rows["inventory_uid"]) == {"iuid_orphan"}
