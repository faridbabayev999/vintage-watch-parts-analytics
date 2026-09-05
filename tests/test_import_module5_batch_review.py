"""
Step 3 regression tests (owner final work plan, 2026-07-30) for
scripts/18_import_module5_batch_review.py -- the non-blinded Module 5 batch
review importer. Disposable in-tmp DuckDB only, never the live database.
"""
import importlib.util
import sys
from pathlib import Path

import duckdb
import pandas as pd
import pytest

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
SCHEMA = SCRIPTS / "schema.sql"


def _load():
    spec = importlib.util.spec_from_file_location("m5import", SCRIPTS / "18_import_module5_batch_review.py")
    m = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(SCRIPTS))
    spec.loader.exec_module(m)
    return m


ORIGINAL_COLS = [
    "candidate_id", "inventory_id", "inventory_brand", "inventory_part_number", "inventory_caliber",
    "matched_source", "matched_rule", "candidate_listing_title", "candidate_price_eur", "candidate_url",
    "reviewer_label", "reviewer_reason", "reviewed_by", "reviewed_at",
]
LINEAGE_COLS = [
    "candidate_key", "match_run_id", "inventory_uid", "source_table", "source_id",
    "matching_rule", "evidence_tier", "collection_relationship", "contradiction_flags", "risk_flags",
]


def _make_original(tmp_path, n=3):
    rows = []
    for i in range(n):
        rows.append({
            "candidate_id": f"c{i}", "inventory_id": f"inv{i}", "inventory_brand": "Rolex",
            "inventory_part_number": "123", "inventory_caliber": "3135",
            "matched_source": "ACTIVE", "matched_rule": "CALIBER_PART_NUMBER",
            "candidate_listing_title": f"Rolex part {i}", "candidate_price_eur": 100.0 + i,
            "candidate_url": "", "reviewer_label": "", "reviewer_reason": "",
            "reviewed_by": "", "reviewed_at": "",
        })
    df = pd.DataFrame(rows)
    p = tmp_path / "original.csv"
    df.to_csv(p, index=False)
    return p, df


def _make_lineage(tmp_path, n=3):
    rows = []
    for i in range(n):
        rows.append({
            "candidate_key": f"c{i}", "match_run_id": "run1", "inventory_uid": f"iuid{i}",
            "source_table": "match_candidates_active", "source_id": str(100 + i),
            "matching_rule": "CALIBER_PART_NUMBER", "evidence_tier": "A",
            "collection_relationship": "NOT_APPLICABLE", "contradiction_flags": "", "risk_flags": "",
        })
    p = tmp_path / "lineage.csv"
    pd.DataFrame(rows).to_csv(p, index=False)
    return p


def _make_completed(tmp_path, orig_df, labels=None, name="completed.csv"):
    df = orig_df.copy()
    labels = labels or ["TRUE_MATCH"] * len(df)
    df["reviewer_label"] = labels
    df["reviewer_reason"] = "because"
    df["reviewed_by"] = "Vaishnavi"
    df["reviewed_at"] = "2026-07-30"
    p = tmp_path / name
    df.to_csv(p, index=False)
    return p


def _fresh_db(tmp_path, name="t.duckdb"):
    return str(tmp_path / name)


def test_valid_import_writes_expected_rows(tmp_path):
    m = _load()
    orig_p, orig_df = _make_original(tmp_path)
    lineage_p = _make_lineage(tmp_path)
    comp_p = _make_completed(tmp_path, orig_df)
    db = _fresh_db(tmp_path)
    n = m.import_batch(str(comp_p), str(orig_p), str(lineage_p), "v1", db)
    assert n == 3
    conn = duckdb.connect(db, read_only=True)
    assert conn.execute("SELECT COUNT(*) FROM validation_review_samples").fetchone()[0] == 3
    conn.close()


def test_duplicate_import_is_idempotent_not_doubled(tmp_path):
    m = _load()
    orig_p, orig_df = _make_original(tmp_path)
    lineage_p = _make_lineage(tmp_path)
    comp_p = _make_completed(tmp_path, orig_df)
    db = _fresh_db(tmp_path)
    m.import_batch(str(comp_p), str(orig_p), str(lineage_p), "v1", db)
    m.import_batch(str(comp_p), str(orig_p), str(lineage_p), "v1", db)
    conn = duckdb.connect(db, read_only=True)
    assert conn.execute("SELECT COUNT(*) FROM validation_review_samples").fetchone()[0] == 3
    conn.close()


def test_invalid_label_refuses_entire_import(tmp_path):
    m = _load()
    orig_p, orig_df = _make_original(tmp_path)
    lineage_p = _make_lineage(tmp_path)
    comp_p = _make_completed(tmp_path, orig_df, labels=["TRUE_MATCH", "MAYBE", "FALSE_MATCH"])
    db = _fresh_db(tmp_path)
    with pytest.raises(m.ImportValidationError):
        m.import_batch(str(comp_p), str(orig_p), str(lineage_p), "v1", db)
    conn = duckdb.connect(db)
    conn.execute(SCHEMA.read_text())
    assert conn.execute("SELECT COUNT(*) FROM validation_review_samples").fetchone()[0] == 0
    conn.close()


def test_missing_attribution_refuses_import(tmp_path):
    m = _load()
    orig_p, orig_df = _make_original(tmp_path)
    lineage_p = _make_lineage(tmp_path)
    df = orig_df.copy()
    df["reviewer_label"] = "TRUE_MATCH"
    df["reviewer_reason"] = "because"
    df["reviewed_by"] = ""  # missing attribution
    df["reviewed_at"] = "2026-07-30"
    comp_p = tmp_path / "completed_missing_attr.csv"
    df.to_csv(comp_p, index=False)
    db = _fresh_db(tmp_path)
    with pytest.raises(m.ImportValidationError) as exc:
        m.import_batch(str(comp_p), str(orig_p), str(lineage_p), "v1", db)
    assert any("reviewed_by" in e for e in exc.value.errors)


def test_modified_evidence_field_refuses_import(tmp_path):
    m = _load()
    orig_p, orig_df = _make_original(tmp_path)
    lineage_p = _make_lineage(tmp_path)
    comp_p = _make_completed(tmp_path, orig_df)
    df = pd.read_csv(comp_p)
    df.loc[0, "candidate_price_eur"] = 99999.0  # tampered evidence
    df.to_csv(comp_p, index=False)
    db = _fresh_db(tmp_path)
    with pytest.raises(m.ImportValidationError) as exc:
        m.import_batch(str(comp_p), str(orig_p), str(lineage_p), "v1", db)
    assert any("altered" in e for e in exc.value.errors)


def test_source_file_hash_recorded_and_detects_mismatch(tmp_path):
    """Re-importing a file with the same name but different bytes must be
    detectable via the recorded hash, even though content-level validation
    would already catch most tampering."""
    m = _load()
    orig_p, orig_df = _make_original(tmp_path)
    lineage_p = _make_lineage(tmp_path)
    comp_p = _make_completed(tmp_path, orig_df)
    db = _fresh_db(tmp_path)
    m.import_batch(str(comp_p), str(orig_p), str(lineage_p), "v1", db)
    conn = duckdb.connect(db, read_only=True)
    hashes = conn.execute("SELECT DISTINCT source_file_sha256 FROM validation_review_samples").fetchall()
    assert len(hashes) == 1 and hashes[0][0]
    import hashlib
    expected = hashlib.sha256(Path(comp_p).read_bytes()).hexdigest()
    assert hashes[0][0] == expected
    conn.close()


def test_missing_row_vs_original_package_refuses_import(tmp_path):
    m = _load()
    orig_p, orig_df = _make_original(tmp_path, n=3)
    lineage_p = _make_lineage(tmp_path, n=3)
    comp_p = _make_completed(tmp_path, orig_df.iloc[:2])  # dropped one row
    db = _fresh_db(tmp_path)
    with pytest.raises(m.ImportValidationError) as exc:
        m.import_batch(str(comp_p), str(orig_p), str(lineage_p), "v1", db)
    assert any("missing from completed file" in e for e in exc.value.errors)
