"""
tests/test_export_review_sample.py
=====================================
Focused tests for scripts/09_export_review_sample.py -- must never assign
a reviewer_label; pure, reproducible, deduplicated stratified export.
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


m5 = _load_module("m5c_review", SCRIPTS_DIR / "05_generate_match_candidates.py")
dec = _load_module("m6d_review", SCRIPTS_DIR / "06_decide_matches.py")
review = _load_module("m9review", SCRIPTS_DIR / "09_export_review_sample.py")


def _inv(uid, brand="Rolex", caliber="3135", part_number="12345"):
    return dict(
        canonical_inventory_id=f"{brand}_{caliber}_{part_number}_{uid}", inventory_uid=uid,
        brand=brand, caliber=caliber, part_number=part_number, stock=1, validation_status="PASS",
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


def _active_row(id_, title, inventory_uid=None, item_id="item1"):
    return {
        "id": id_, "raw_id": id_, "inventory_uid": inventory_uid, "item_id": item_id,
        "title": title, "normalized_title": title.lower(), "marketplace": "EBAY_DE",
    }


def _seed_active(connection, rows):
    df = pd.DataFrame(rows)
    connection.register("tmp_seed", df)
    connection.execute(f"INSERT INTO stg_active_targeted ({','.join(df.columns)}) SELECT {','.join(df.columns)} FROM tmp_seed")
    connection.unregister("tmp_seed")


@pytest.fixture()
def conn(tmp_path):
    db_path = tmp_path / "test.duckdb"
    connection = duckdb.connect(str(db_path))
    connection.execute(SCHEMA_PATH.read_text())
    yield connection
    connection.close()


def test_review_sample_has_no_labels(conn):
    _seed_inventory(conn, [_inv("iuid_1", part_number="12345")])
    _seed_active(conn, [_active_row(1, "Rolex 3135 bridge 12345 genuine")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    dec.run_decision_layer(conn, decision_run_id="d1")
    sample = review.build_review_sample(conn, per_stratum_target=5)
    assert (sample["reviewer_label"] == "").all()
    assert (sample["reviewed_by"] == "").all()
    assert (sample["reviewed_at"] == "").all()


def test_review_sample_no_duplicate_candidate_keys_across_strata(conn):
    rows = [_inv(f"iuid_{i}", part_number=f"999{i:02d}zz") for i in range(20)]
    _seed_inventory(conn, rows)
    active = [_active_row(i, f"Rolex 3135 bridge 999{i:02d}zz genuine", item_id=f"item{i}") for i in range(20)]
    _seed_active(conn, active)
    m5.run_candidate_generation(conn, match_run_id="run1")
    dec.run_decision_layer(conn, decision_run_id="d1")
    sample = review.build_review_sample(conn, per_stratum_target=5)
    assert sample["candidate_key"].duplicated().sum() == 0


def test_review_sample_deterministic_across_reruns(conn):
    rows = [_inv(f"iuid_{i}", part_number=f"999{i:02d}zz") for i in range(15)]
    _seed_inventory(conn, rows)
    active = [_active_row(i, f"Rolex 3135 bridge 999{i:02d}zz genuine", item_id=f"item{i}") for i in range(15)]
    _seed_active(conn, active)
    m5.run_candidate_generation(conn, match_run_id="run1")
    dec.run_decision_layer(conn, decision_run_id="d1")
    s1 = review.build_review_sample(conn, per_stratum_target=5)
    s2 = review.build_review_sample(conn, per_stratum_target=5)
    pd.testing.assert_frame_equal(s1.reset_index(drop=True), s2.reset_index(drop=True))


def test_evidence_title_resolved_via_evidence_uid_not_null(conn):
    """docs/MODULE5_POST_PHASE1_ARCHITECTURE_AUDIT.md Phase 2: the export
    query must resolve evidence_title via the new evidence_uid-based join,
    not silently drop rows or leave the title NULL."""
    _seed_inventory(conn, [_inv("iuid_1", part_number="12345")])
    _seed_active(conn, [_active_row(1, "Rolex 3135 bridge 12345 genuine", item_id="listing1")])
    m5.run_candidate_generation(conn, match_run_id="run1")
    dec.run_decision_layer(conn, decision_run_id="d1")
    df = review._base_query(conn)
    assert not df.empty
    assert df["evidence_title"].notna().all()
    assert (df["evidence_title"].str.contains("bridge 12345")).any()


def test_no_duplicate_evidence_disguised_as_different_evidence_in_export(conn):
    """docs/MODULE5_POST_PHASE1_ARCHITECTURE_AUDIT.md Phase 2, the
    explicit requirement: a reviewer must never see the same real-world
    listing twice as if it were separate corroborating evidence. Seed one
    listing represented by two staging pairing-rows (as in the
    Bug 1 scenario) and confirm the export produces exactly one
    evidence_title per (inventory_uid, evidence_uid, matching_rule)."""
    _seed_inventory(conn, [_inv("iuid_Z", caliber="3135", part_number="99999")])
    uid = "EV-ACTIVE-testfixed"
    for id_, inv_uid in [(1, "iuid_A"), (2, "iuid_B")]:
        conn.execute(
            "INSERT INTO stg_active_targeted (id, raw_id, inventory_uid, item_id, title, "
            "normalized_title, marketplace, stable_evidence_uid, observation_uid) "
            "VALUES (?, ?, ?, 'listing_SHARED', 'Rolex 3135 99999 crown genuine', "
            "'rolex 3135 99999 crown genuine', 'EBAY_DE', ?, ?)",
            [id_, id_, inv_uid, uid, f"OBS-{id_}"],
        )
    m5.run_candidate_generation(conn, match_run_id="run1")
    dec.run_decision_layer(conn, decision_run_id="d1")
    df = review._base_query(conn)
    z_rows = df[df["inventory_uid"] == "iuid_Z"]
    dupe_groups = z_rows.groupby(["inventory_uid", "evidence_uid", "matching_rule"]).size()
    assert (dupe_groups <= 1).all(), (
        f"found duplicate evidence disguised as separate rows in the export: "
        f"{dupe_groups[dupe_groups > 1]}"
    )
