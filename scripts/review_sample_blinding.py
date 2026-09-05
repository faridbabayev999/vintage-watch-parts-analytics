"""
review_sample_blinding.py
============================
Module 5: shared blinding logic for the calibration (scripts/
10_build_calibration_sample.py) and priority (scripts/
11_build_priority_sample.py) review samples. Not a pipeline stage on its
own -- imported only. Pure functions: no database connection, no writes.

Both samples draw from reports/module5_pilot/review_sample_v1.csv (the
750-row audited pool, docs/MODULE5_REVIEW_POOL_AUDIT.md), deduplicated by
CANDIDATE RELATIONSHIP (source_table, source_id, inventory_uid) -- the 4
pairs where the same evidence+item combination appears twice only because
two different matching rules both fired are collapsed to one
reviewer-facing row (keeping the alphabetically-first matching_rule for
determinism), so a reviewer is never asked the same underlying judgement
twice. Genuine many-to-many relationships (the SAME evidence matched to
DIFFERENT items) are NEVER collapsed -- each remains its own row, per
explicit instruction.

Blinding contract:

Reviewer-facing (blinded) columns -- must never leak a system prediction:
    review_row_id, inventory_brand, inventory_calibre, inventory_part_number,
    evidence_source, evidence_title,
    watch_part_check, brand_match_check, calibre_match_check, part_number_match_check
        (structured diagnostic fields -- human-observable judgement only,
        YES/NO/UNKNOWN, filled by the reviewer alongside reviewer_label,
        never computed by this codebase and never a replacement for
        reviewer_label -- see docs/MODULE5_REVIEW_GUIDE.md),
    failure_category
        (one of the fixed FAILURE_CATEGORIES below -- required for
        FALSE_MATCH rows, NOT_APPLICABLE otherwise; explains WHY, never
        changes the reviewer_label taxonomy itself),
    reviewer_label, reviewer_reason,
    reviewer_name, reviewer_timestamp, review_method
        (provenance -- review_method is one of REVIEW_METHODS below;
        no tool name, assistant name, or model name is ever a permitted
        value in any of these three columns)

Audit-manifest-only columns -- full lineage, joinable back by review_row_id:
    review_row_id, candidate_key, source_table, source_id, inventory_uid,
    matching_rule, evidence_tier, collection_relationship, risk_flags,
    contradiction_flags, match_status (current system decision),
    review_stratum (sampling stratum), sample_version, seed,
    all_candidate_keys, all_matching_rules (see load_deduplicated_pool)
"""

import pandas as pd

SOURCE_LABELS = {
    "match_candidates_active": "Active eBay listing (targeted collection)",
    "match_candidates_vcp": "Historical — VCP aggregate",
    "match_candidates_ebay_sold": "Historical — eBay sold listing",
}

# Structured diagnostic fields (docs/MODULE5_REVIEW_GUIDE.md): human-
# observable judgement questions, filled by the reviewer alongside
# reviewer_label, never replacing it. Each takes YES/NO/UNKNOWN --
# UNKNOWN covers "the title doesn't say," not "I can't tell overall"
# (that's what UNREVIEWABLE, on reviewer_label, is for). These are NOT
# system predictions -- nothing here is computed by this codebase, every
# cell starts empty, exactly like reviewer_label.
DIAGNOSTIC_COLUMNS = [
    "watch_part_check", "brand_match_check", "calibre_match_check", "part_number_match_check",
]

# Explains WHY a FALSE_MATCH happened, from a fixed enum -- never a
# replacement for reviewer_label, never free text. NOT_APPLICABLE for
# any row that isn't FALSE_MATCH.
FAILURE_CATEGORIES = [
    "NOT_A_WATCH_PART", "WRONG_BRAND", "WRONG_CALIBRE", "WRONG_PART_NUMBER",
    "MODEL_REFERENCE_NOT_PART", "COMPATIBILITY_ONLY", "BUNDLE_OR_LOT",
    "INSUFFICIENT_INFORMATION", "OTHER", "NOT_APPLICABLE",
]

# Provenance: how the label was produced. No tool/assistant/model name is
# ever a valid value here or anywhere else in the reviewer-facing schema.
REVIEW_METHODS = ["MANUAL_REVIEW", "ADJUDICATION", "SECONDARY_REVIEW"]

BLINDED_COLUMNS = [
    "review_row_id", "inventory_brand", "inventory_calibre", "inventory_part_number",
    "evidence_source", "evidence_title",
    *DIAGNOSTIC_COLUMNS,
    "failure_category",
    "reviewer_label", "reviewer_reason",
    "reviewer_name", "reviewer_timestamp", "review_method",
]

MANIFEST_COLUMNS = [
    "review_row_id", "candidate_key", "source_table", "source_id", "evidence_uid", "inventory_uid",
    "matching_rule", "evidence_tier", "collection_relationship", "risk_flags",
    "contradiction_flags", "match_status", "review_stratum", "sample_version", "seed",
    "all_candidate_keys", "all_matching_rules",
]


def load_deduplicated_pool(pool_csv_path: str) -> pd.DataFrame:
    """Loads the pool and collapses same-evidence duplicate-candidate-
    relationship rows (docs/MODULE5_REVIEW_POOL_AUDIT.md) to one
    REVIEWER-FACING row each, keeping the alphabetically-first
    matching_rule/candidate_key as the row's primary (displayed)
    `candidate_key`/`matching_rule` for a deterministic, reproducible
    choice. Genuine many-to-many rows (same evidence, different
    inventory_uid) are untouched -- the groupby key includes
    inventory_uid, so those remain fully distinct rows.

    DEDUP KEY (docs/MODULE5_POST_PHASE1_ARCHITECTURE_AUDIT.md Phase 2):
    groups on evidence_uid (the canonical Module 5 evidence identity)
    when the pool CSV has that column and it's populated for a row;
    falls back to the legacy source_id for any row without one (e.g.
    review_sample_v1's pool, exported before evidence_uid existed --
    that file is never modified, this is purely a read-time fallback so
    load_deduplicated_pool keeps working against it unchanged). Grouping
    on source_id alone previously missed the case where the SAME
    real-world listing is represented by multiple staging rows (up to 8
    confirmed in the pilot data) with DIFFERENT source_id values but the
    SAME evidence_uid -- those would have shown a reviewer the same
    listing twice, disguised as independently corroborating evidence.

    LINEAGE PRESERVATION (fixed -- see docs/MODULE5_REVIEW_PACKAGE_
    READINESS_CHECK.md): every contributing candidate_key and
    matching_rule for a collapsed relationship is retained in the new
    `all_candidate_keys`/`all_matching_rules` columns (comma-joined,
    sorted, deterministic), even though only one representative row is
    shown to a reviewer. An earlier version of this function used a bare
    `.first()` which silently discarded the non-representative
    candidate_key/matching_rule entirely -- that data loss is what this
    fix closes; `candidate_key`/`matching_rule` alone are no longer a
    complete lineage record for a collapsed relationship, only
    `all_candidate_keys`/`all_matching_rules` are."""
    df = pd.read_csv(pool_csv_path)
    if "evidence_uid" not in df.columns:
        # Backward compatibility: review_sample_v1's pool (and any other
        # CSV exported before evidence_uid existed) has no such column.
        # Never modified -- this only fills it in-memory, at read time.
        df["evidence_uid"] = pd.NA
    df["_dedup_key"] = df["evidence_uid"].fillna(df["source_id"].astype(str))
    group_cols = ["source_table", "_dedup_key", "inventory_uid"]
    df = df.sort_values(group_cols + ["matching_rule"])

    lineage = (
        df.groupby(group_cols)
        .agg(
            all_candidate_keys=("candidate_key", lambda s: ",".join(sorted(s))),
            all_matching_rules=("matching_rule", lambda s: ",".join(sorted(s))),
        )
        .reset_index()
    )

    deduped = df.groupby(group_cols, as_index=False).first()
    deduped = deduped.merge(lineage, on=group_cols, how="left")
    deduped = deduped.drop(columns=["_dedup_key"])
    return deduped.sort_values("candidate_key").reset_index(drop=True)


def to_blinded_and_manifest(
    df: pd.DataFrame, *, row_id_prefix: str, sample_version: str, seed: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """df must already be in final selected-row order (the order
    review_row_id is assigned in). Returns (blinded_df, manifest_df),
    joinable 1:1 on review_row_id."""
    out = df.copy().reset_index(drop=True)
    out["review_row_id"] = [f"{row_id_prefix}-{i+1:03d}" for i in range(len(out))]
    out["evidence_source"] = out["source_table"].map(SOURCE_LABELS)
    for col in DIAGNOSTIC_COLUMNS:
        out[col] = ""
    out["failure_category"] = ""
    out["reviewer_label"] = ""
    out["reviewer_reason"] = ""
    out["reviewer_name"] = ""
    out["reviewer_timestamp"] = ""
    out["review_method"] = ""
    out["sample_version"] = sample_version
    out["seed"] = seed

    blinded = out[BLINDED_COLUMNS].copy()
    manifest = out[MANIFEST_COLUMNS].copy()
    return blinded, manifest
