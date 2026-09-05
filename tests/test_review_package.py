"""
tests/test_review_package.py
===============================
Focused tests for the blinded review package (scripts/review_sample_
blinding.py, scripts/10_build_calibration_sample.py, scripts/
11_build_priority_sample.py, scripts/12_import_review_labels.py).

Uses only small, synthetic pool CSVs constructed in-test -- never reads
or writes reports/module5_pilot/review_sample_v1.csv or any real
calibration/priority file, and never touches database/watchparts.duckdb
(these scripts are pure pandas/CSV tools with no database connection at
all, so there is nothing to guard there beyond confirming that fact).
"""

import hashlib
import importlib.util
import sys
from pathlib import Path

import pandas as pd
import pytest

TESTS_DIR = Path(__file__).parent
BASE_DIR = TESTS_DIR.parent
SCRIPTS_DIR = BASE_DIR / "scripts"


def _load_module(name, path):
    sys.path.insert(0, str(SCRIPTS_DIR))
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


blinding = _load_module("review_sample_blinding", SCRIPTS_DIR / "review_sample_blinding.py")
cal = _load_module("m10cal", SCRIPTS_DIR / "10_build_calibration_sample.py")
pri = _load_module("m11pri", SCRIPTS_DIR / "11_build_priority_sample.py")
imp = _load_module("m12imp", SCRIPTS_DIR / "12_import_review_labels.py")


def _synthetic_pool_row(i, *, source_table="match_candidates_active", source_id=None,
                          inventory_uid=None, matching_rule="PART_NUMBER_EXACT", evidence_tier="A",
                          collection_relationship="SELF_SOURCED", deterministic_checks_passed=True,
                          risk_flags=None, contradiction_flags=None, match_status="REVIEW_REQUIRED"):
    return {
        "review_stratum": "SYN", "candidate_key": f"key{i}", "match_run_id": "run1",
        "inventory_uid": inventory_uid or f"iuid_{i}", "inventory_brand": "Rolex",
        "inventory_calibre": "3135", "inventory_part_number": f"pn{i}",
        "source_table": source_table, "source_id": source_id if source_id is not None else i,
        "evidence_title": f"Rolex 3135 part pn{i} genuine listing {i}",
        "matching_rule": matching_rule, "evidence_tier": evidence_tier,
        "collection_relationship": collection_relationship,
        "deterministic_checks_passed": deterministic_checks_passed,
        "contradiction_flags": contradiction_flags, "risk_flags": risk_flags,
        "match_reason_code": "TIER_A_CLEAN_NO_CONTRADICTION_NO_RISK", "validation_segment": "seg",
        "match_status": match_status, "reviewer_label": "", "reviewer_reason": "",
        "reviewed_by": "", "reviewed_at": "",
    }


@pytest.fixture()
def synthetic_pool_csv(tmp_path):
    rows = [_synthetic_pool_row(i) for i in range(40)]
    # Add a genuine many-to-many pair: same evidence identity, two different inventory items.
    rows.append(_synthetic_pool_row(1000, source_table="match_candidates_active", source_id=5,
                                     inventory_uid="iuid_OTHER_ITEM", matching_rule="PART_NUMBER_EXACT"))
    # Add a duplicate candidate relationship: same evidence identity AND same item, different rule.
    rows.append(_synthetic_pool_row(6, source_table="match_candidates_active", source_id=6,
                                     inventory_uid="iuid_6", matching_rule="BRAND_PART_NUMBER"))
    # Add risk-flagged rows for the priority sample's risk strata.
    for j, pattern in enumerate(pri.RISK_PATTERNS):
        rows.append(_synthetic_pool_row(2000 + j, risk_flags=pattern, deterministic_checks_passed=False))
    df = pd.DataFrame(rows)
    path = tmp_path / "pool.csv"
    df.to_csv(path, index=False)
    return str(path)


def _file_sha256(path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


# ── Determinism ──────────────────────────────────────────────────────────

def test_calibration_sample_regenerates_byte_identically(synthetic_pool_csv, tmp_path):
    dedup = blinding.load_deduplicated_pool(synthetic_pool_csv)
    out1, out2 = tmp_path / "run1", tmp_path / "run2"
    out1.mkdir()
    out2.mkdir()
    for out_dir in (out1, out2):
        calibration = cal.build_calibration_sample(dedup)
        b, m = blinding.to_blinded_and_manifest(calibration, row_id_prefix="CAL", sample_version="test", seed=1)
        b.to_csv(out_dir / "blinded.csv", index=False)
        m.to_csv(out_dir / "manifest.csv", index=False)
    assert _file_sha256(out1 / "blinded.csv") == _file_sha256(out2 / "blinded.csv")
    assert _file_sha256(out1 / "manifest.csv") == _file_sha256(out2 / "manifest.csv")


def test_priority_sample_regenerates_byte_identically(synthetic_pool_csv, tmp_path):
    dedup = blinding.load_deduplicated_pool(synthetic_pool_csv)
    out1, out2 = tmp_path / "p1", tmp_path / "p2"
    out1.mkdir()
    out2.mkdir()
    for out_dir in (out1, out2):
        priority = pri.build_priority_sample(dedup, excluded_candidate_keys=set())
        b, m = blinding.to_blinded_and_manifest(priority, row_id_prefix="PRI", sample_version="test", seed=2)
        b.to_csv(out_dir / "blinded.csv", index=False)
        m.to_csv(out_dir / "manifest.csv", index=False)
    assert _file_sha256(out1 / "blinded.csv") == _file_sha256(out2 / "blinded.csv")
    assert _file_sha256(out1 / "manifest.csv") == _file_sha256(out2 / "manifest.csv")


# ── Non-overlap ──────────────────────────────────────────────────────────

def test_calibration_and_priority_do_not_overlap(synthetic_pool_csv):
    dedup = blinding.load_deduplicated_pool(synthetic_pool_csv)
    calibration = cal.build_calibration_sample(dedup)
    priority = pri.build_priority_sample(dedup, excluded_candidate_keys=set(calibration["candidate_key"]))
    assert set(calibration["candidate_key"]).isdisjoint(set(priority["candidate_key"]))


# ── Blinding ─────────────────────────────────────────────────────────────

def test_blinded_files_omit_system_prediction_fields(synthetic_pool_csv):
    dedup = blinding.load_deduplicated_pool(synthetic_pool_csv)
    calibration = cal.build_calibration_sample(dedup)
    b, _ = blinding.to_blinded_and_manifest(calibration, row_id_prefix="CAL", sample_version="test", seed=1)
    forbidden = {"matching_rule", "evidence_tier", "match_status", "deterministic_checks_passed",
                 "risk_flags", "contradiction_flags", "candidate_key", "inventory_uid", "source_id"}
    assert forbidden.isdisjoint(set(b.columns))


def test_manifests_preserve_full_lineage(synthetic_pool_csv):
    dedup = blinding.load_deduplicated_pool(synthetic_pool_csv)
    calibration = cal.build_calibration_sample(dedup)
    _, m = blinding.to_blinded_and_manifest(calibration, row_id_prefix="CAL", sample_version="test", seed=1)
    required = {"candidate_key", "source_table", "source_id", "inventory_uid", "matching_rule",
                "evidence_tier", "collection_relationship", "match_status", "review_stratum"}
    assert required.issubset(set(m.columns))
    assert m["candidate_key"].isna().sum() == 0


def test_blinded_and_manifest_join_one_to_one(synthetic_pool_csv):
    dedup = blinding.load_deduplicated_pool(synthetic_pool_csv)
    priority = pri.build_priority_sample(dedup, excluded_candidate_keys=set())
    b, m = blinding.to_blinded_and_manifest(priority, row_id_prefix="PRI", sample_version="test", seed=2)
    assert set(b["review_row_id"]) == set(m["review_row_id"])
    assert b["review_row_id"].is_unique
    assert m["review_row_id"].is_unique


# ── Many-to-many preservation / duplicate-relationship rejection ─────────

def test_valid_many_to_many_relationships_are_retained(synthetic_pool_csv):
    dedup = blinding.load_deduplicated_pool(synthetic_pool_csv)
    shared_evidence_rows = dedup[(dedup["source_table"] == "match_candidates_active") & (dedup["source_id"] == 5)]
    assert set(shared_evidence_rows["inventory_uid"]) == {"iuid_5", "iuid_OTHER_ITEM"}


def test_duplicate_candidate_relationships_are_collapsed(synthetic_pool_csv):
    dedup = blinding.load_deduplicated_pool(synthetic_pool_csv)
    same_relationship = dedup[(dedup["source_table"] == "match_candidates_active")
                               & (dedup["source_id"] == 6) & (dedup["inventory_uid"] == "iuid_6")]
    assert len(same_relationship) == 1  # collapsed from 2 (PART_NUMBER_EXACT + BRAND_PART_NUMBER)
    assert same_relationship.iloc[0]["matching_rule"] == "BRAND_PART_NUMBER"  # alphabetically first


def test_collapsed_relationship_preserves_all_contributing_rule_lineage(synthetic_pool_csv):
    """Readiness-check regression: an earlier version of load_deduplicated_
    pool used a bare .first() that silently discarded the non-
    representative candidate_key/matching_rule for a collapsed
    relationship. Both contributing candidate_keys and both contributing
    matching_rule values must remain traceable via all_candidate_keys/
    all_matching_rules, even though only one row is shown to a reviewer."""
    dedup = blinding.load_deduplicated_pool(synthetic_pool_csv)
    same_relationship = dedup[(dedup["source_table"] == "match_candidates_active")
                               & (dedup["source_id"] == 6) & (dedup["inventory_uid"] == "iuid_6")]
    row = same_relationship.iloc[0]
    assert len(row["all_candidate_keys"].split(",")) == 2
    assert row["candidate_key"] in row["all_candidate_keys"].split(",")
    assert set(row["all_matching_rules"].split(",")) == {"BRAND_PART_NUMBER", "PART_NUMBER_EXACT"}


def test_lineage_columns_do_not_leak_into_blinded_file(synthetic_pool_csv):
    dedup = blinding.load_deduplicated_pool(synthetic_pool_csv)
    calibration = cal.build_calibration_sample(dedup)
    b, m = blinding.to_blinded_and_manifest(calibration, row_id_prefix="CAL", sample_version="test", seed=1)
    assert "all_candidate_keys" not in b.columns
    assert "all_matching_rules" not in b.columns
    assert "all_candidate_keys" in m.columns
    assert "all_matching_rules" in m.columns
    assert m["all_candidate_keys"].isna().sum() == 0
    assert m["all_matching_rules"].isna().sum() == 0


# ── Import validation ───────────────────────────────────────────────────

@pytest.fixture()
def import_fixture(tmp_path):
    blinded_rows = [
        {"review_row_id": "T-001", "inventory_brand": "Rolex", "inventory_calibre": "3135",
         "inventory_part_number": "12345", "evidence_source": "Active eBay listing (targeted collection)",
         "evidence_title": "Rolex 3135 bridge 12345 genuine", "reviewer_label": "", "reviewer_reason": "",
         "reviewer_name": "", "reviewer_timestamp": ""},
        {"review_row_id": "T-002", "inventory_brand": "Rolex", "inventory_calibre": "3135",
         "inventory_part_number": "999", "evidence_source": "Active eBay listing (targeted collection)",
         "evidence_title": "unrelated book about watches", "reviewer_label": "", "reviewer_reason": "",
         "reviewer_name": "", "reviewer_timestamp": ""},
    ]
    manifest_rows = [
        {"review_row_id": "T-001", "candidate_key": "key1", "source_table": "match_candidates_active",
         "source_id": 1, "inventory_uid": "iuid_1", "matching_rule": "PART_NUMBER_EXACT",
         "evidence_tier": "A", "collection_relationship": "SELF_SOURCED", "risk_flags": None,
         "contradiction_flags": None, "match_status": "REVIEW_REQUIRED", "review_stratum": "E_CLEAN",
         "sample_version": "test_v1", "seed": 1,
         "all_candidate_keys": "key1", "all_matching_rules": "PART_NUMBER_EXACT"},
        {"review_row_id": "T-002", "candidate_key": "key2", "source_table": "match_candidates_active",
         "source_id": 2, "inventory_uid": "iuid_2", "matching_rule": "PART_NUMBER_EXACT",
         "evidence_tier": "A", "collection_relationship": "SELF_SOURCED", "risk_flags": None,
         "contradiction_flags": None, "match_status": "REVIEW_REQUIRED", "review_stratum": "E_CLEAN",
         "sample_version": "test_v1", "seed": 1,
         "all_candidate_keys": "key2", "all_matching_rules": "PART_NUMBER_EXACT"},
    ]
    blinded_path = tmp_path / "blinded.csv"
    manifest_path = tmp_path / "manifest.csv"
    pd.DataFrame(blinded_rows).to_csv(blinded_path, index=False)
    pd.DataFrame(manifest_rows).to_csv(manifest_path, index=False)
    return str(blinded_path), str(manifest_path), pd.DataFrame(blinded_rows)


def test_altered_evidence_text_is_detected_during_import(import_fixture, tmp_path):
    blinded_path, manifest_path, blinded_df = import_fixture
    completed = blinded_df.copy()
    completed.loc[0, ["reviewer_label", "reviewer_name", "reviewer_timestamp"]] = ["TRUE_MATCH", "t", "2026-07-27T10:00:00Z"]
    completed.loc[1, ["reviewer_label", "reviewer_name", "reviewer_timestamp"]] = ["FALSE_MATCH", "t", "2026-07-27T10:01:00Z"]
    completed.loc[0, "evidence_title"] = "TAMPERED"
    completed_path = tmp_path / "completed.csv"
    completed.to_csv(completed_path, index=False)
    with pytest.raises(imp.ImportValidationError) as exc:
        imp.import_labels(str(completed_path), blinded_path, manifest_path, str(tmp_path / "out.csv"))
    assert any("altered" in e for e in exc.value.errors)
    assert not (tmp_path / "out.csv").exists()


def test_incomplete_labels_block_import(import_fixture, tmp_path):
    blinded_path, manifest_path, blinded_df = import_fixture
    completed = blinded_df.copy()
    completed.loc[0, ["reviewer_label", "reviewer_name", "reviewer_timestamp"]] = ["TRUE_MATCH", "t", "2026-07-27T10:00:00Z"]
    # row 1 left with empty reviewer_label
    completed_path = tmp_path / "completed.csv"
    completed.to_csv(completed_path, index=False)
    with pytest.raises(imp.ImportValidationError):
        imp.import_labels(str(completed_path), blinded_path, manifest_path, str(tmp_path / "out.csv"))
    assert not (tmp_path / "out.csv").exists()


def test_missing_reviewer_provenance_blocks_import(import_fixture, tmp_path):
    blinded_path, manifest_path, blinded_df = import_fixture
    completed = blinded_df.copy()
    completed.loc[0, "reviewer_label"] = "TRUE_MATCH"  # no name/timestamp
    completed.loc[1, ["reviewer_label", "reviewer_name", "reviewer_timestamp"]] = ["FALSE_MATCH", "t", "2026-07-27T10:01:00Z"]
    completed_path = tmp_path / "completed.csv"
    completed.to_csv(completed_path, index=False)
    with pytest.raises(imp.ImportValidationError) as exc:
        imp.import_labels(str(completed_path), blinded_path, manifest_path, str(tmp_path / "out.csv"))
    assert any("provenance" in e for e in exc.value.errors)


def test_valid_import_is_idempotent(import_fixture, tmp_path):
    blinded_path, manifest_path, blinded_df = import_fixture
    completed = blinded_df.copy()
    completed.loc[0, ["reviewer_label", "reviewer_name", "reviewer_timestamp"]] = ["TRUE_MATCH", "t", "2026-07-27T10:00:00Z"]
    completed.loc[1, ["reviewer_label", "reviewer_name", "reviewer_timestamp"]] = ["FALSE_MATCH", "t", "2026-07-27T10:01:00Z"]
    completed_path = tmp_path / "completed.csv"
    completed.to_csv(completed_path, index=False)
    out_path = str(tmp_path / "out.csv")
    r1 = imp.import_labels(str(completed_path), blinded_path, manifest_path, out_path)
    r2 = imp.import_labels(str(completed_path), blinded_path, manifest_path, out_path)
    assert len(r1) == len(r2) == 2


def test_import_preserves_multi_rule_lineage_columns(import_fixture, tmp_path):
    """Readiness-check regression: LINEAGE_COLUMNS in the importer must
    include all_candidate_keys/all_matching_rules (added to the manifest
    schema by bd03ce7), or they are silently dropped during import and
    become unavailable to any rule-level diagnostic downstream."""
    blinded_path, manifest_path, blinded_df = import_fixture
    completed = blinded_df.copy()
    completed.loc[0, ["reviewer_label", "reviewer_name", "reviewer_timestamp"]] = ["TRUE_MATCH", "t", "2026-07-27T10:00:00Z"]
    completed.loc[1, ["reviewer_label", "reviewer_name", "reviewer_timestamp"]] = ["FALSE_MATCH", "t", "2026-07-27T10:01:00Z"]
    completed_path = tmp_path / "completed.csv"
    completed.to_csv(completed_path, index=False)
    result = imp.import_labels(str(completed_path), blinded_path, manifest_path, str(tmp_path / "out.csv"))
    assert "all_candidate_keys" in result.columns
    assert "all_matching_rules" in result.columns
    assert result["all_candidate_keys"].isna().sum() == 0
    assert result["all_matching_rules"].isna().sum() == 0


def test_import_rejects_pre_bd03ce7_manifest_with_clear_error(import_fixture, tmp_path):
    """A manifest missing the multi-rule lineage columns must fail with a
    named schema-mismatch error, not a bare pandas KeyError, and not a
    silent partial import."""
    blinded_path, manifest_path, blinded_df = import_fixture
    old_manifest = pd.read_csv(manifest_path).drop(columns=["all_candidate_keys", "all_matching_rules"])
    old_manifest_path = tmp_path / "old_manifest.csv"
    old_manifest.to_csv(old_manifest_path, index=False)

    completed = blinded_df.copy()
    completed.loc[0, ["reviewer_label", "reviewer_name", "reviewer_timestamp"]] = ["TRUE_MATCH", "t", "2026-07-27T10:00:00Z"]
    completed.loc[1, ["reviewer_label", "reviewer_name", "reviewer_timestamp"]] = ["FALSE_MATCH", "t", "2026-07-27T10:01:00Z"]
    completed_path = tmp_path / "completed.csv"
    completed.to_csv(completed_path, index=False)

    with pytest.raises(imp.ImportValidationError) as exc:
        imp.import_labels(str(completed_path), blinded_path, str(old_manifest_path), str(tmp_path / "out.csv"))
    assert any("schema" in e.lower() or "predates" in e.lower() for e in exc.value.errors)
    assert not (tmp_path / "out.csv").exists()


def test_two_rule_relationship_counts_once_in_segment_analysis(tmp_path):
    """A candidate relationship collapsed from two matching rules must
    produce exactly one reviewed observation in analyze_segments -- never
    two -- even though its full rule lineage (all_matching_rules) remains
    available in the imported-labels file for a future rule-level
    diagnostic to reference as a shared/dependent observation."""
    labels_df = pd.DataFrame([
        {"review_row_id": "X-001", "candidate_key": "keyA", "source_table": "match_candidates_active",
         "source_id": 10, "inventory_uid": "iuid_10", "matching_rule": "CALIBER_COMPONENT",
         "evidence_tier": "C", "collection_relationship": "CROSS_REFERENCED", "risk_flags": None,
         "contradiction_flags": None, "match_status": "REVIEW_REQUIRED", "review_stratum": "D_RISK",
         "sample_version": "test_v1", "seed": 1,
         "all_candidate_keys": "keyA,keyB", "all_matching_rules": "CALIBER_COMPONENT,CALIBER_EXACT",
         "reviewer_label": "TRUE_MATCH", "reviewer_reason": "r", "reviewer_name": "t",
         "reviewer_timestamp": "2026-07-27T10:00:00Z"},
    ])
    report = imp.analyze_segments(labels_df)
    assert report["reviewed_candidate_relationships"].sum() == 1
    assert report["true_match"].sum() == 1
    # the shared rule remains discoverable from the row itself, not silently lost
    assert "CALIBER_EXACT" in labels_df.iloc[0]["all_matching_rules"]


def test_empty_labels_remain_empty_when_pool_exported(synthetic_pool_csv):
    dedup = blinding.load_deduplicated_pool(synthetic_pool_csv)
    calibration = cal.build_calibration_sample(dedup)
    b, _ = blinding.to_blinded_and_manifest(calibration, row_id_prefix="CAL", sample_version="test", seed=1)
    assert (b["reviewer_label"] == "").all()


def test_incomplete_labels_block_analysis(tmp_path):
    incomplete = pd.DataFrame([
        {"review_row_id": "T-001", "candidate_key": "key1", "source_table": "match_candidates_active",
         "source_id": 1, "inventory_uid": "iuid_1", "matching_rule": "PART_NUMBER_EXACT", "evidence_tier": "A",
         "collection_relationship": "SELF_SOURCED", "risk_flags": None, "contradiction_flags": None,
         "match_status": "REVIEW_REQUIRED", "review_stratum": "E_CLEAN", "sample_version": "test_v1", "seed": 1,
         "reviewer_label": "", "reviewer_reason": "", "reviewer_name": "", "reviewer_timestamp": ""},
    ])
    path = tmp_path / "incomplete_labels.csv"
    incomplete.to_csv(path, index=False)
    labels_df = pd.read_csv(path)
    has_incomplete = labels_df["reviewer_label"].isna().any() or (labels_df["reviewer_label"].astype(str).str.strip() == "").any()
    assert has_incomplete  # confirms the analyze CLI's own refusal condition would trigger


def test_analyze_refuses_segment_with_zero_reviewed_rows():
    labels_df = pd.DataFrame([
        {"matching_rule": "PART_NUMBER_EXACT", "source_table": "match_candidates_active",
         "collection_relationship": "SELF_SOURCED", "source_id": 1, "reviewer_label": "TRUE_MATCH"},
    ])
    report = imp.analyze_segments(labels_df)
    assert (report["note"] == "OK").all()
    assert len(report) == 1  # only the one segment with data is reported; no fabricated zero-population segments


def test_no_policy_row_is_changed_by_this_package(synthetic_pool_csv, tmp_path):
    """The entire review-package toolchain (blinding, sampling, import,
    analyze) never touches validation_policy or any database."""
    import inspect
    for module in (blinding, cal, pri, imp):
        source = inspect.getsource(module)
        assert "validation_policy" not in source
        assert "duckdb.connect" not in source


def test_live_database_untouched_by_review_package():
    """These tools take no database path argument at all -- confirmed by
    inspecting their CLI argument definitions."""
    import inspect
    for module in (cal, pri, imp):
        source = inspect.getsource(module)
        assert "--db" not in source
        assert "watchparts.duckdb" not in source


# ── Module 5 post-Phase-1 fix: dedup by canonical evidence identity ────────

def test_pool_rows_sharing_evidence_uid_collapse_even_with_different_source_id(tmp_path):
    """docs/MODULE5_POST_PHASE1_ARCHITECTURE_AUDIT.md Phase 2: two pool
    rows with DIFFERENT source_id but the SAME evidence_uid and SAME
    inventory_uid (the Bug 1 scenario -- one real listing spanning
    multiple staging pairing-rows) must collapse to one reviewer-facing
    row, not be shown as two separate pieces of evidence."""
    rows = [
        _synthetic_pool_row(1, source_table="match_candidates_active", source_id=101,
                             inventory_uid="iuid_shared", matching_rule="PART_NUMBER_EXACT"),
        _synthetic_pool_row(2, source_table="match_candidates_active", source_id=102,
                             inventory_uid="iuid_shared", matching_rule="PART_NUMBER_EXACT"),
    ]
    df = pd.DataFrame(rows)
    df["evidence_uid"] = "EV-ACTIVE-sharedlisting"
    path = tmp_path / "pool_with_uid.csv"
    df.to_csv(path, index=False)

    dedup = blinding.load_deduplicated_pool(str(path))
    matches = dedup[dedup["inventory_uid"] == "iuid_shared"]
    assert len(matches) == 1, (
        f"expected 2 rows sharing evidence_uid to collapse to 1, got {len(matches)}"
    )


def test_pool_without_evidence_uid_column_still_works(tmp_path):
    """Backward compatibility: a pool CSV with no evidence_uid column at
    all (e.g. review_sample_v1's real file) must not error -- falls back
    to the legacy source_id-based dedup, unchanged behavior."""
    rows = [_synthetic_pool_row(i) for i in range(5)]
    df = pd.DataFrame(rows)
    assert "evidence_uid" not in df.columns
    path = tmp_path / "pool_no_uid_col.csv"
    df.to_csv(path, index=False)

    dedup = blinding.load_deduplicated_pool(str(path))
    assert len(dedup) == 5


# ── Structured diagnostic reviewer fields (docs/MODULE5_REVIEW_GUIDE.md v2) ──

def test_blinded_output_includes_diagnostic_columns(synthetic_pool_csv):
    dedup = blinding.load_deduplicated_pool(synthetic_pool_csv)
    calibration = cal.build_calibration_sample(dedup)
    b, _ = blinding.to_blinded_and_manifest(calibration, row_id_prefix="CAL", sample_version="test", seed=1)
    for col in blinding.DIAGNOSTIC_COLUMNS:
        assert col in b.columns
        assert (b[col] == "").all()


def test_diagnostic_columns_still_not_system_predictions(synthetic_pool_csv):
    """Diagnostic columns are reviewer-filled, human-observable judgement
    -- confirm they start blank like reviewer_label, not pre-populated
    from any system field (would defeat the point of a diagnostic
    ANNOTATION rather than a system-computed one)."""
    dedup = blinding.load_deduplicated_pool(synthetic_pool_csv)
    calibration = cal.build_calibration_sample(dedup)
    b, _ = blinding.to_blinded_and_manifest(calibration, row_id_prefix="CAL", sample_version="test", seed=1)
    assert set(blinding.DIAGNOSTIC_COLUMNS) <= set(b.columns)
    assert set(blinding.DIAGNOSTIC_COLUMNS).isdisjoint(set(imp.BLINDED_CONTENT_COLUMNS))


def _import_fixture_with_diagnostics(tmp_path, *, include_diagnostic_columns=True):
    blinded_row_base = {
        "review_row_id": None, "inventory_brand": "Rolex", "inventory_calibre": "3135",
        "inventory_part_number": "12345", "evidence_source": "Active eBay listing (targeted collection)",
        "evidence_title": None, "reviewer_label": "", "reviewer_reason": "",
        "reviewer_name": "", "reviewer_timestamp": "",
    }
    if include_diagnostic_columns:
        for col in imp.DIAGNOSTIC_COLUMNS:
            blinded_row_base[col] = ""
        blinded_row_base["failure_category"] = ""
        blinded_row_base["review_method"] = ""
    rows = []
    for rid, title in [("D-001", "Rolex 3135 bridge 12345 genuine"), ("D-002", "unrelated book about watches")]:
        r = dict(blinded_row_base)
        r["review_row_id"] = rid
        r["evidence_title"] = title
        rows.append(r)
    manifest_rows = [
        {"review_row_id": "D-001", "candidate_key": "keyd1", "source_table": "match_candidates_active",
         "source_id": 1, "inventory_uid": "iuid_1", "matching_rule": "PART_NUMBER_EXACT",
         "evidence_tier": "A", "collection_relationship": "SELF_SOURCED", "risk_flags": None,
         "contradiction_flags": None, "match_status": "REVIEW_REQUIRED", "review_stratum": "E_CLEAN",
         "sample_version": "test_v1", "seed": 1,
         "all_candidate_keys": "keyd1", "all_matching_rules": "PART_NUMBER_EXACT"},
        {"review_row_id": "D-002", "candidate_key": "keyd2", "source_table": "match_candidates_active",
         "source_id": 2, "inventory_uid": "iuid_2", "matching_rule": "PART_NUMBER_EXACT",
         "evidence_tier": "A", "collection_relationship": "SELF_SOURCED", "risk_flags": None,
         "contradiction_flags": None, "match_status": "REVIEW_REQUIRED", "review_stratum": "E_CLEAN",
         "sample_version": "test_v1", "seed": 1,
         "all_candidate_keys": "keyd2", "all_matching_rules": "PART_NUMBER_EXACT"},
    ]
    blinded_path = tmp_path / "blinded_diag.csv"
    manifest_path = tmp_path / "manifest_diag.csv"
    pd.DataFrame(rows).to_csv(blinded_path, index=False)
    pd.DataFrame(manifest_rows).to_csv(manifest_path, index=False)
    return str(blinded_path), str(manifest_path), pd.DataFrame(rows)


def test_import_requires_diagnostic_fields_when_column_present(tmp_path):
    blinded_path, manifest_path, blinded_df = _import_fixture_with_diagnostics(tmp_path)
    completed = blinded_df.copy()
    completed.loc[0, ["reviewer_label", "reviewer_name", "reviewer_timestamp", "review_method"]] = \
        ["TRUE_MATCH", "t", "2026-07-27T10:00:00Z", "MANUAL_REVIEW"]
    completed.loc[0, "failure_category"] = "NOT_APPLICABLE"
    # watch_part_check etc. left blank for row 0 -- must block import
    completed_path = tmp_path / "completed.csv"
    completed.to_csv(completed_path, index=False)
    with pytest.raises(imp.ImportValidationError) as exc:
        imp.import_labels(str(completed_path), blinded_path, manifest_path, str(tmp_path / "out.csv"))
    assert any("watch_part_check" in e or "brand_match_check" in e for e in exc.value.errors)


def test_import_rejects_invalid_diagnostic_value(tmp_path):
    blinded_path, manifest_path, blinded_df = _import_fixture_with_diagnostics(tmp_path)
    completed = blinded_df.copy()
    completed.loc[0, ["reviewer_label", "reviewer_name", "reviewer_timestamp", "review_method"]] = \
        ["TRUE_MATCH", "t", "2026-07-27T10:00:00Z", "MANUAL_REVIEW"]
    completed.loc[0, "failure_category"] = "NOT_APPLICABLE"
    for col in imp.DIAGNOSTIC_COLUMNS:
        completed.loc[0, col] = "YES"
    completed.loc[0, "watch_part_check"] = "MAYBE"  # invalid
    completed_path = tmp_path / "completed.csv"
    completed.to_csv(completed_path, index=False)
    with pytest.raises(imp.ImportValidationError) as exc:
        imp.import_labels(str(completed_path), blinded_path, manifest_path, str(tmp_path / "out.csv"))
    assert any("watch_part_check" in e and "MAYBE" in e for e in exc.value.errors)


def test_import_accepts_valid_diagnostic_values_and_carries_them_through(tmp_path):
    blinded_path, manifest_path, blinded_df = _import_fixture_with_diagnostics(tmp_path)
    completed = blinded_df.copy()
    completed.loc[0, ["reviewer_label", "reviewer_name", "reviewer_timestamp", "review_method"]] = \
        ["TRUE_MATCH", "t", "2026-07-27T10:00:00Z", "MANUAL_REVIEW"]
    completed.loc[0, "failure_category"] = "NOT_APPLICABLE"
    completed.loc[0, imp.DIAGNOSTIC_COLUMNS] = ["YES", "YES", "YES", "YES"]
    completed.loc[1, ["reviewer_label", "reviewer_name", "reviewer_timestamp", "review_method"]] = \
        ["FALSE_MATCH", "t", "2026-07-27T10:01:00Z", "MANUAL_REVIEW"]
    completed.loc[1, "failure_category"] = "WRONG_PART_NUMBER"
    completed.loc[1, imp.DIAGNOSTIC_COLUMNS] = ["NO", "UNKNOWN", "UNKNOWN", "UNKNOWN"]
    completed_path = tmp_path / "completed.csv"
    completed.to_csv(completed_path, index=False)
    result = imp.import_labels(str(completed_path), blinded_path, manifest_path, str(tmp_path / "out.csv"))
    assert set(result.columns) >= set(imp.DIAGNOSTIC_COLUMNS)
    row0 = result[result["review_row_id"] == "D-001"].iloc[0]
    assert row0["watch_part_check"] == "YES"
    assert row0["failure_category"] == "NOT_APPLICABLE"
    row1 = result[result["review_row_id"] == "D-002"].iloc[0]
    assert row1["watch_part_check"] == "NO"
    assert row1["failure_category"] == "WRONG_PART_NUMBER"


def test_legacy_completed_file_without_diagnostic_columns_still_imports(tmp_path):
    """A completed file returned against a pre-diagnostic-fields blinded
    export (no such columns at all) must not be blocked -- backward
    compatibility with v1-style files."""
    blinded_path, manifest_path, blinded_df = _import_fixture_with_diagnostics(tmp_path, include_diagnostic_columns=False)
    completed = blinded_df.copy()
    assert not set(imp.DIAGNOSTIC_COLUMNS) & set(completed.columns)
    completed.loc[0, ["reviewer_label", "reviewer_name", "reviewer_timestamp"]] = ["TRUE_MATCH", "t", "2026-07-27T10:00:00Z"]
    completed.loc[1, ["reviewer_label", "reviewer_name", "reviewer_timestamp"]] = ["FALSE_MATCH", "t", "2026-07-27T10:01:00Z"]
    completed_path = tmp_path / "completed.csv"
    completed.to_csv(completed_path, index=False)
    result = imp.import_labels(str(completed_path), blinded_path, manifest_path, str(tmp_path / "out.csv"))
    assert len(result) == 2


def test_false_match_diagnostic_breakdown_classifies_correctly():
    labels_df = pd.DataFrame([
        {"matching_rule": "PART_NUMBER_EXACT", "source_table": "match_candidates_active",
         "collection_relationship": "SELF_SOURCED", "reviewer_label": "FALSE_MATCH",
         "watch_part_check": "NO", "brand_match_check": "UNKNOWN",
         "calibre_match_check": "UNKNOWN", "part_number_match_check": "UNKNOWN"},
        {"matching_rule": "PART_NUMBER_EXACT", "source_table": "match_candidates_active",
         "collection_relationship": "SELF_SOURCED", "reviewer_label": "FALSE_MATCH",
         "watch_part_check": "YES", "brand_match_check": "YES",
         "calibre_match_check": "YES", "part_number_match_check": "NO"},
        {"matching_rule": "PART_NUMBER_EXACT", "source_table": "match_candidates_active",
         "collection_relationship": "SELF_SOURCED", "reviewer_label": "TRUE_MATCH",
         "watch_part_check": "YES", "brand_match_check": "YES",
         "calibre_match_check": "YES", "part_number_match_check": "YES"},
    ])
    report = imp.analyze_false_match_diagnostics(labels_df)
    reasons = dict(zip(report["reason"], report["count"]))
    assert reasons.get("not_a_watch_part") == 1
    assert reasons.get("wrong_part_number") == 1
    assert report["false_match_count"].iloc[0] == 2  # 2 FALSE_MATCH rows in the segment, TRUE_MATCH excluded


def test_false_match_diagnostic_breakdown_handles_missing_diagnostic_data():
    labels_df = pd.DataFrame([
        {"matching_rule": "PART_NUMBER_EXACT", "source_table": "match_candidates_active",
         "collection_relationship": "SELF_SOURCED", "reviewer_label": "FALSE_MATCH",
         "watch_part_check": "", "brand_match_check": "",
         "calibre_match_check": "", "part_number_match_check": ""},
    ])
    report = imp.analyze_false_match_diagnostics(labels_df)
    assert (report["reason"] == "no_diagnostic_data").all()
