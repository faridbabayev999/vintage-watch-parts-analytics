"""
18_import_module5_batch_review.py
==================================
Module 5: import a completed (non-blinded) review batch produced from
reports/module5_validation_batch_for_review.csv into the live
validation_review_samples table.

Distinct from scripts/12_import_review_labels.py, which implements the
STRICTER blinded/manifest review workflow (review_row_id, reviewer_name,
review_method, failure_category, diagnostic columns) -- this script's input
file predates that workflow and has a simpler, non-blinded schema
(candidate_id/matched_rule/matched_source/... + reviewer_label/reviewer_
reason/reviewed_by/reviewed_at). It is kept separate rather than forced into
12_import_review_labels.py's stricter format, which would silently claim a
level of process rigor (blinding, structured diagnostics) this batch never
had.

Validates BEFORE writing anything:
  - exactly the expected row count vs. the original (pre-review) package
  - no duplicate candidate_id
  - no removed/added rows vs. the original package
  - reviewer_label in {TRUE_MATCH, FALSE_MATCH, AMBIGUOUS} only
  - no blank reviewer_label/reviewer_reason/reviewed_by/reviewed_at
  - evidence columns (inventory_*, matched_*, candidate_listing_title,
    candidate_price_eur, candidate_url) byte-identical to the original
    package -- refuses on ANY mismatch (never a partial import)

Recovers full lineage (match_run_id, evidence_tier, collection_relationship,
contradiction_flags, risk_flags) by joining candidate_id back to
reports/module5_pilot/review_sample_v1.csv on candidate_key -- this
information was deliberately withheld from the reviewer's copy (evidence-only
review) but is required for validation_review_samples' schema.

Idempotent: re-running with the same completed file against the same
validation_sample_version deletes-then-reinserts only the rows in this
completed file (matched by (validation_sample_version, candidate_key), the
same UNIQUE key the table already enforces) -- never touches rows from a
different completed batch under the same sample_version, never duplicates.

Records the completed file's SHA-256 in every inserted row
(source_file_sha256, additive column) so a later hash mismatch on the same
nominal file is detectable, not silently trusted.
"""
from __future__ import annotations

import argparse
import hashlib
import sys
from pathlib import Path

import duckdb
import pandas as pd

BASE_DIR = Path(__file__).parent.parent
SCHEMA_PATH = BASE_DIR / "scripts" / "schema.sql"
ORIGINAL_PACKAGE_DEFAULT = BASE_DIR / "reports" / "module5_validation_batch_for_review.csv"
LINEAGE_SOURCE_DEFAULT = BASE_DIR / "reports" / "module5_pilot" / "review_sample_v1.csv"

ALLOWED_LABELS = {"TRUE_MATCH", "FALSE_MATCH", "AMBIGUOUS"}
EVIDENCE_COLUMNS = [
    "inventory_id", "inventory_brand", "inventory_part_number", "inventory_caliber",
    "matched_source", "matched_rule", "candidate_listing_title",
    "candidate_price_eur", "candidate_url",
]


class ImportValidationError(Exception):
    def __init__(self, errors: list):
        self.errors = errors
        super().__init__("\n".join(errors))


def _norm(v) -> str:
    if pd.isna(v):
        return ""
    return str(v).strip()


def validate_batch(completed: pd.DataFrame, original: pd.DataFrame) -> list:
    errors: list = []

    dupes = completed["candidate_id"][completed["candidate_id"].duplicated()].unique().tolist()
    if dupes:
        errors.append(f"Duplicate candidate_id values in completed file: {sorted(dupes)}")

    orig_ids = set(original["candidate_id"])
    comp_ids = set(completed["candidate_id"])
    missing = sorted(orig_ids - comp_ids)
    added = sorted(comp_ids - orig_ids)
    if missing:
        errors.append(f"candidate_id present in original package but missing from completed file: {missing}")
    if added:
        errors.append(f"candidate_id present in completed file but not in original package: {added}")

    orig_idx = original.set_index("candidate_id")
    for _, row in completed.iterrows():
        cid = row["candidate_id"]
        label = _norm(row.get("reviewer_label"))
        reason = _norm(row.get("reviewer_reason"))
        reviewer = _norm(row.get("reviewed_by"))
        reviewed_at = _norm(row.get("reviewed_at"))

        if not label:
            errors.append(f"{cid}: reviewer_label is missing/empty")
        elif label not in ALLOWED_LABELS:
            errors.append(f"{cid}: reviewer_label {label!r} is not one of {sorted(ALLOWED_LABELS)}")
        if not reason:
            errors.append(f"{cid}: reviewer_reason is missing/empty")
        if not reviewer:
            errors.append(f"{cid}: reviewed_by is missing/empty (provenance required)")
        if not reviewed_at:
            errors.append(f"{cid}: reviewed_at is missing/empty (provenance required)")

        if cid not in orig_idx.index:
            continue  # already reported above
        orig_row = orig_idx.loc[cid]
        for col in EVIDENCE_COLUMNS:
            ov, cv = _norm(orig_row.get(col)), _norm(row.get(col))
            if ov != cv:
                errors.append(f"{cid}: evidence column '{col}' was altered (original={ov!r}, completed={cv!r})")

    return errors


def import_batch(
    completed_path: str, original_path: str, lineage_path: str,
    validation_sample_version: str, db_path: str,
) -> int:
    completed = pd.read_csv(completed_path)
    original = pd.read_csv(original_path)
    lineage = pd.read_csv(lineage_path, dtype=str)

    errors = validate_batch(completed, original)
    if errors:
        raise ImportValidationError(errors)

    file_hash = hashlib.sha256(Path(completed_path).read_bytes()).hexdigest()

    lineage_cols = [
        "candidate_key", "match_run_id", "evidence_tier", "collection_relationship",
        "contradiction_flags", "risk_flags", "inventory_uid", "source_table", "source_id",
    ]
    lin = lineage[lineage_cols].rename(columns={"candidate_key": "candidate_id"})
    merged = completed.merge(lin, on="candidate_id", how="left")

    missing_lineage = merged[merged["inventory_uid"].isna()]["candidate_id"].tolist()
    if missing_lineage:
        raise ImportValidationError(
            [f"candidate_id {cid} not found in lineage source {lineage_path} -- cannot recover full lineage" for cid in missing_lineage]
        )

    conn = duckdb.connect(db_path)
    conn.execute(SCHEMA_PATH.read_text())
    conn.execute("ALTER TABLE validation_review_samples ADD COLUMN IF NOT EXISTS reviewed_by VARCHAR")
    conn.execute("ALTER TABLE validation_review_samples ADD COLUMN IF NOT EXISTS source_file_sha256 VARCHAR")

    # Idempotent upsert: delete only rows this exact completed file provides
    # (same sample_version + candidate_key), never touch other rows.
    conn.register("t_del", merged[["candidate_id"]].rename(columns={"candidate_id": "candidate_key"}))
    conn.execute(
        "DELETE FROM validation_review_samples WHERE validation_sample_version = ? "
        "AND candidate_key IN (SELECT candidate_key FROM t_del)",
        [validation_sample_version],
    )
    conn.unregister("t_del")

    next_id = conn.execute("SELECT COALESCE(MAX(reviewed_case_id), 0) + 1 FROM validation_review_samples").fetchone()[0]
    out = pd.DataFrame({
        "reviewed_case_id": range(next_id, next_id + len(merged)),
        "validation_sample_version": validation_sample_version,
        "candidate_key": merged["candidate_id"],
        "match_run_id": merged["match_run_id"],
        "inventory_uid": merged["inventory_uid"],
        "source_table": merged["source_table"],
        "source_id": pd.to_numeric(merged["source_id"], errors="coerce").astype("Int64"),
        "matching_rule": merged["matched_rule"],
        "evidence_tier": merged["evidence_tier"],
        "collection_relationship": merged["collection_relationship"],
        "evidence_text": merged["candidate_listing_title"],
        "matched_tokens": None,
        "contradiction_flags": merged["contradiction_flags"],
        "risk_flags": merged["risk_flags"],
        "reviewer_label": merged["reviewer_label"].str.strip(),
        "reviewer_reason": merged["reviewer_reason"],
        "reviewed_at": merged["reviewed_at"],
        "reviewed_by": merged["reviewed_by"],
        "source_file_sha256": file_hash,
    })
    conn.register("t_ins", out)
    conn.execute("INSERT INTO validation_review_samples SELECT * FROM t_ins")
    conn.unregister("t_ins")
    conn.close()
    return len(out)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--completed", required=True)
    ap.add_argument("--original", default=str(ORIGINAL_PACKAGE_DEFAULT))
    ap.add_argument("--lineage", default=str(LINEAGE_SOURCE_DEFAULT))
    ap.add_argument("--sample-version", default="pilot_post_collection_review_v1")
    ap.add_argument("--db", required=True)
    args = ap.parse_args()

    try:
        n = import_batch(args.completed, args.original, args.lineage, args.sample_version, args.db)
    except ImportValidationError as e:
        print("IMPORT REFUSED -- the following issues must be fixed before import:", file=sys.stderr)
        for err in e.errors:
            print(f"  - {err}", file=sys.stderr)
        sys.exit(1)
    print(f"Imported {n} reviewed rows into validation_review_samples ({args.db})")


if __name__ == "__main__":
    main()
