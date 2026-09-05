"""
12_import_review_labels.py
=============================
Module 5: validate, import, and analyze completed human review labels.

Two subcommands:

  import  -- validates a completed blinded review CSV against its
             original (pre-review) blinded copy and its audit manifest,
             then writes a joined, full-lineage labels file to the
             validation-lineage location. Refuses (writes nothing) if ANY
             row fails ANY check -- a safe stop, never a partial import.

  analyze -- computes per-segment reviewed counts, TRUE/FALSE/AMBIGUOUS/
             UNREVIEWABLE, reviewable denominator, observed precision,
             Clopper-Pearson exact 95% CI, sample overlap, and risk
             representation, from an imported labels file ONLY (never
             from a raw completed-review file directly). Refuses to
             report a segment's precision if that segment has zero
             reviewed rows -- absence of data is reported as absence,
             never silently treated as 0/0 = undefined precision.

Idempotent import: re-running `import` with the same completed file
against the same manifest produces byte-identical output; running it
again after a genuinely new completed file (same sample_version, new
rows or corrected labels) upserts by (sample_version, candidate_key) --
the same unique key convention as the validation_review_samples schema
this project already uses (scripts/schema.sql), even though this tool
writes to a CSV, not the live database.

Never writes to any database. Never modifies review_calibration_v1_
blinded.csv, review_priority_v1_blinded.csv, or any reviewer's returned
file -- only reads them.

Usage:
    python scripts/12_import_review_labels.py import \
        --completed <reviewer's returned CSV> \
        --original-blinded <the untouched blinded CSV originally sent out> \
        --manifest <the audit manifest for the same sample> \
        --out <path to the imported-labels CSV, upserted>

    python scripts/12_import_review_labels.py analyze \
        --labels <one or more imported-labels CSVs> \
        --out <path to write the segment analysis report>
"""

import argparse
import sys
from pathlib import Path

import pandas as pd
from scipy.stats import beta

ALLOWED_LABELS = {"TRUE_MATCH", "FALSE_MATCH", "AMBIGUOUS", "UNREVIEWABLE"}
REVIEWABLE_LABELS = {"TRUE_MATCH", "FALSE_MATCH"}  # AMBIGUOUS/UNREVIEWABLE excluded from the reviewable denominator

# Structured diagnostic fields (scripts/review_sample_blinding.py's
# DIAGNOSTIC_COLUMNS, docs/MODULE5_REVIEW_GUIDE.md) -- human-observable
# judgement, YES/NO/UNKNOWN, filled by the reviewer alongside
# reviewer_label. Diagnostic ONLY -- never the ground-truth label;
# ALLOWED_LABELS/REVIEWABLE_LABELS above are unchanged.
DIAGNOSTIC_COLUMNS = [
    "watch_part_check", "brand_match_check", "calibre_match_check", "part_number_match_check",
]
ALLOWED_DIAGNOSTIC_VALUES = {"YES", "NO", "UNKNOWN"}

# Explains WHY a FALSE_MATCH happened -- required (one of these, exactly)
# for FALSE_MATCH rows, NOT_APPLICABLE required for every other label.
# Never a free-text field, never a replacement for reviewer_reason.
ALLOWED_FAILURE_CATEGORIES = {
    "NOT_A_WATCH_PART", "WRONG_BRAND", "WRONG_CALIBRE", "WRONG_PART_NUMBER",
    "MODEL_REFERENCE_NOT_PART", "COMPATIBILITY_ONLY", "BUNDLE_OR_LOT",
    "INSUFFICIENT_INFORMATION", "OTHER", "NOT_APPLICABLE",
}

# Provenance: how the label was produced. No tool/assistant/model name is
# ever a valid value -- these three enum members are the only allowed
# review_method values in any reviewer-facing artefact.
ALLOWED_REVIEW_METHODS = {"MANUAL_REVIEW", "ADJUDICATION", "SECONDARY_REVIEW"}

BLINDED_CONTENT_COLUMNS = [
    "review_row_id", "inventory_brand", "inventory_calibre", "inventory_part_number",
    "evidence_source", "evidence_title",
]

LINEAGE_COLUMNS = [
    "review_row_id", "candidate_key", "source_table", "source_id", "inventory_uid",
    "matching_rule", "evidence_tier", "collection_relationship", "risk_flags",
    "contradiction_flags", "match_status", "review_stratum", "sample_version", "seed",
    "all_candidate_keys", "all_matching_rules",
]

# Bumped alongside scripts/review_sample_blinding.py's MANIFEST_COLUMNS
# (bd03ce7 added all_candidate_keys/all_matching_rules for multi-rule
# lineage preservation). A manifest missing either column predates that
# change -- rejected with a clear schema-mismatch error rather than a bare
# pandas KeyError; no compatibility shim for the older format is built,
# since no such manifest is in actual use by this project.
MANIFEST_SCHEMA_VERSION = "manifest_schema_v2_multi_rule_lineage"

IMPORTED_LABEL_COLUMNS = LINEAGE_COLUMNS + DIAGNOSTIC_COLUMNS + [
    "failure_category",
    "reviewer_label", "reviewer_reason",
    "reviewer_name", "reviewer_timestamp", "review_method",
]


class ImportValidationError(Exception):
    def __init__(self, errors: list):
        self.errors = errors
        super().__init__("\n".join(errors))


def validate_completed_review(
    completed: pd.DataFrame, original_blinded: pd.DataFrame, manifest: pd.DataFrame,
) -> list:
    """Returns a list of error strings. Empty list = valid. Never raises;
    the caller decides whether to treat a non-empty list as fatal (import
    always does)."""
    errors: list = []

    dupes = completed["review_row_id"][completed["review_row_id"].duplicated()].unique().tolist()
    if dupes:
        errors.append(f"Duplicate review_row_id values in completed file: {sorted(dupes)}")

    known_ids = set(manifest["review_row_id"])
    unknown = sorted(set(completed["review_row_id"]) - known_ids)
    if unknown:
        errors.append(f"review_row_id values not present in the manifest (unknown rows): {unknown}")

    missing_from_completed = sorted(known_ids - set(completed["review_row_id"]))
    if missing_from_completed:
        errors.append(f"review_row_id values present in the manifest but absent from the completed file: {missing_from_completed}")

    # Content-tamper check: every non-reviewer column must exactly match the
    # original blinded file that was sent out, keyed by review_row_id.
    orig_indexed = original_blinded.set_index("review_row_id")
    for _, row in completed.iterrows():
        rid = row["review_row_id"]
        if rid not in orig_indexed.index:
            continue  # already reported as unknown above
        orig_row = orig_indexed.loc[rid]
        for col in BLINDED_CONTENT_COLUMNS:
            if col == "review_row_id":
                continue
            completed_val = row.get(col)
            orig_val = orig_row.get(col)
            completed_norm = "" if pd.isna(completed_val) else str(completed_val)
            orig_norm = "" if pd.isna(orig_val) else str(orig_val)
            if completed_norm != orig_norm:
                errors.append(
                    f"{rid}: evidence content column '{col}' was altered "
                    f"(original={orig_norm!r}, completed={completed_norm!r})"
                )

    for _, row in completed.iterrows():
        rid = row["review_row_id"]
        label = row.get("reviewer_label")
        label_norm = None if pd.isna(label) else str(label).strip()
        if not label_norm:
            errors.append(f"{rid}: reviewer_label is missing/empty")
        elif label_norm not in ALLOWED_LABELS:
            errors.append(f"{rid}: reviewer_label {label_norm!r} is not one of {sorted(ALLOWED_LABELS)}")

        name = row.get("reviewer_name")
        ts = row.get("reviewer_timestamp")
        name_norm = None if pd.isna(name) else str(name).strip()
        ts_norm = None if pd.isna(ts) else str(ts).strip()
        if label_norm and not name_norm:
            errors.append(f"{rid}: reviewer_label is set but reviewer_name is missing (provenance required)")
        if label_norm and not ts_norm:
            errors.append(f"{rid}: reviewer_label is set but reviewer_timestamp is missing (provenance required)")

        if label_norm and "review_method" in completed.columns:
            method = row.get("review_method")
            method_norm = None if pd.isna(method) else str(method).strip().upper()
            if not method_norm:
                errors.append(f"{rid}: reviewer_label is set but review_method is missing (provenance required)")
            elif method_norm not in ALLOWED_REVIEW_METHODS:
                errors.append(f"{rid}: review_method {method_norm!r} is not one of {sorted(ALLOWED_REVIEW_METHODS)}")

        if label_norm and "failure_category" in completed.columns:
            category = row.get("failure_category")
            category_norm = None if pd.isna(category) else str(category).strip().upper()
            if not category_norm:
                errors.append(f"{rid}: reviewer_label is set but failure_category is missing")
            elif category_norm not in ALLOWED_FAILURE_CATEGORIES:
                errors.append(f"{rid}: failure_category {category_norm!r} is not one of {sorted(ALLOWED_FAILURE_CATEGORIES)}")
            elif label_norm == "FALSE_MATCH" and category_norm == "NOT_APPLICABLE":
                errors.append(f"{rid}: reviewer_label is FALSE_MATCH but failure_category is NOT_APPLICABLE -- a real category is required")
            elif label_norm != "FALSE_MATCH" and category_norm != "NOT_APPLICABLE":
                errors.append(f"{rid}: reviewer_label is {label_norm} but failure_category is {category_norm!r} -- must be NOT_APPLICABLE for a non-FALSE_MATCH row")

        if label_norm:
            for col in DIAGNOSTIC_COLUMNS:
                if col not in completed.columns:
                    # Legacy completed file (e.g. any v1-style file
                    # returned against a pre-diagnostic-fields blinded
                    # export) genuinely has no such column -- not an
                    # error, nothing to validate. A file that HAS the
                    # column but leaves a cell blank is different (below).
                    continue
                val = row.get(col)
                val_norm = None if pd.isna(val) else str(val).strip().upper()
                if not val_norm:
                    errors.append(f"{rid}: reviewer_label is set but {col} is missing")
                elif val_norm not in ALLOWED_DIAGNOSTIC_VALUES:
                    errors.append(
                        f"{rid}: {col} {val_norm!r} is not one of {sorted(ALLOWED_DIAGNOSTIC_VALUES)}"
                    )

    return errors


def import_labels(completed_path: str, original_blinded_path: str, manifest_path: str, out_path: str) -> pd.DataFrame:
    completed = pd.read_csv(completed_path)
    original_blinded = pd.read_csv(original_blinded_path)
    manifest = pd.read_csv(manifest_path)

    # Backward compatibility: a completed file returned against a
    # pre-diagnostic-fields blinded export (e.g. any v1-style file) won't
    # have these columns at all. validate_completed_review (below)
    # treats that as "nothing to validate," not an error, so add them as
    # blank ONLY after validation, purely so the merge/import step has
    # something to select -- imported rows from a legacy file will
    # correctly carry blank diagnostic values, not fabricated ones.

    missing_manifest_cols = [c for c in LINEAGE_COLUMNS if c not in manifest.columns]
    if missing_manifest_cols:
        raise ImportValidationError([
            f"Manifest is missing column(s) {missing_manifest_cols} required by "
            f"{MANIFEST_SCHEMA_VERSION!r} -- this manifest predates the multi-rule "
            "lineage fix (commit bd03ce7) and cannot be imported as-is. "
            "Regenerate it with the current scripts/10_build_calibration_sample.py "
            "or scripts/11_build_priority_sample.py."
        ])

    errors = validate_completed_review(completed, original_blinded, manifest)
    if errors:
        raise ImportValidationError(errors)

    for col in DIAGNOSTIC_COLUMNS + ["failure_category", "review_method"]:
        if col not in completed.columns:
            completed[col] = pd.NA

    merged = manifest.merge(
        completed[[
            "review_row_id", *DIAGNOSTIC_COLUMNS, "failure_category",
            "reviewer_label", "reviewer_reason",
            "reviewer_name", "reviewer_timestamp", "review_method",
        ]],
        on="review_row_id", how="inner",
    )
    merged["reviewer_label"] = merged["reviewer_label"].str.strip()
    for col in DIAGNOSTIC_COLUMNS + ["failure_category", "review_method"]:
        merged[col] = merged[col].apply(
            lambda v: "" if pd.isna(v) else str(v).strip().upper()
        )
    new_rows = merged[IMPORTED_LABEL_COLUMNS].copy()

    out_file = Path(out_path)
    if out_file.exists():
        existing = pd.read_csv(out_file)
        combined = pd.concat([existing, new_rows], ignore_index=True)
        # Idempotent upsert by (sample_version, candidate_key) -- last write wins,
        # matching the validation_review_samples UNIQUE(sample_version, candidate_key)
        # convention already used elsewhere in this project's schema.
        combined = combined.drop_duplicates(subset=["sample_version", "candidate_key"], keep="last")
    else:
        combined = new_rows

    combined = combined.sort_values(["sample_version", "candidate_key"]).reset_index(drop=True)
    combined.to_csv(out_file, index=False)
    return combined


def _clopper_pearson(successes: int, n: int, alpha: float = 0.05) -> tuple:
    if n == 0:
        return None, None
    lo = 0.0 if successes == 0 else beta.ppf(alpha / 2, successes, n - successes + 1)
    hi = 1.0 if successes == n else beta.ppf(1 - alpha / 2, successes + 1, n - successes)
    return lo, hi


def analyze_segments(labels_df: pd.DataFrame) -> pd.DataFrame:
    """Refuses (returns an empty-with-note row) for any segment with zero
    reviewed rows -- never fabricates a 0/0 precision. Reports BOTH a
    candidate-relationship-level analysis and an evidence-cluster
    sensitivity analysis (one row per unique (source_table, source_id),
    majority label within the cluster; ties reported as-is, not
    silently broken) wherever a segment has any evidence identity shared
    by more than one candidate relationship."""
    if labels_df.empty:
        return pd.DataFrame(columns=[
            "segment", "reviewed_candidate_relationships", "unique_evidence_identities",
            "true_match", "false_match", "ambiguous", "unreviewable", "reviewable_denominator",
            "observed_precision", "ci_lower", "ci_upper", "note",
        ])

    df = labels_df.copy()
    df["segment"] = df["matching_rule"] + "|" + df["source_table"] + "|" + df["collection_relationship"]

    rows = []
    for segment, g in df.groupby("segment"):
        n_relationships = len(g)
        n_evidence = g.groupby(["source_table", "source_id"]).ngroups
        counts = g["reviewer_label"].value_counts().to_dict()
        true_n = counts.get("TRUE_MATCH", 0)
        false_n = counts.get("FALSE_MATCH", 0)
        amb_n = counts.get("AMBIGUOUS", 0)
        unrev_n = counts.get("UNREVIEWABLE", 0)
        denom = true_n + false_n
        precision = (true_n / denom) if denom > 0 else None
        lo, hi = _clopper_pearson(true_n, denom) if denom > 0 else (None, None)
        rows.append({
            "segment": segment,
            "reviewed_candidate_relationships": n_relationships,
            "unique_evidence_identities": n_evidence,
            "true_match": true_n, "false_match": false_n,
            "ambiguous": amb_n, "unreviewable": unrev_n,
            "reviewable_denominator": denom,
            "observed_precision": precision,
            "ci_lower": lo, "ci_upper": hi,
            "note": "OK" if denom > 0 else "REFUSED: zero reviewable (TRUE/FALSE) labels in this segment",
        })
    return pd.DataFrame(rows).sort_values("segment").reset_index(drop=True)


def analyze_evidence_cluster_sensitivity(labels_df: pd.DataFrame) -> pd.DataFrame:
    """For evidence identities shared by >1 candidate relationship, checks
    whether collapsing to one label per evidence identity (majority vote,
    ties reported not resolved) would change the segment's precision
    materially from the candidate-relationship-level figure."""
    if labels_df.empty:
        return pd.DataFrame(columns=["segment", "shared_evidence_identities", "note"])

    df = labels_df.copy()
    df["segment"] = df["matching_rule"] + "|" + df["source_table"] + "|" + df["collection_relationship"]
    rows = []
    for segment, g in df.groupby("segment"):
        cluster_sizes = g.groupby(["source_table", "source_id"]).size()
        shared = cluster_sizes[cluster_sizes > 1]
        if shared.empty:
            rows.append({"segment": segment, "shared_evidence_identities": 0, "note": "no shared evidence -- candidate-relationship and evidence-cluster analyses are identical for this segment"})
            continue
        # one label per evidence identity: majority label among its candidate relationships
        cluster_labels = []
        for (st, sid), _ in shared.items():
            sub = g[(g["source_table"] == st) & (g["source_id"] == sid)]
            counts = sub["reviewer_label"].value_counts()
            top = counts.index[0]
            is_tie = (counts == counts.max()).sum() > 1
            cluster_labels.append({"label": top, "tie": is_tie})
        true_n = sum(1 for c in cluster_labels if c["label"] == "TRUE_MATCH")
        false_n = sum(1 for c in cluster_labels if c["label"] == "FALSE_MATCH")
        ties = sum(1 for c in cluster_labels if c["tie"])
        denom = true_n + false_n
        cluster_precision = (true_n / denom) if denom > 0 else None
        rows.append({
            "segment": segment,
            "shared_evidence_identities": len(shared),
            "cluster_level_precision": cluster_precision,
            "unresolved_ties": ties,
            "note": "cluster-level precision computed by majority label per shared evidence identity" if not ties else f"{ties} cluster(s) had a tied majority -- reported, not arbitrarily broken",
        })
    return pd.DataFrame(rows).sort_values("segment").reset_index(drop=True)


def analyze_false_match_diagnostics(labels_df: pd.DataFrame) -> pd.DataFrame:
    """Breaks FALSE_MATCH rows down by WHY, per segment -- e.g. "60% not
    a watch part, 30% wrong part number despite matching brand/caliber."
    Diagnostic/failure-category fields are supporting annotations, never
    the ground-truth label (ALLOWED_LABELS/reviewer_label are untouched
    by this function).

    Prefers the reviewer's own explicit failure_category (structured,
    reviewer-supplied, docs/MODULE5_REVIEW_GUIDE.md) when present and not
    NOT_APPLICABLE/blank -- this is the authoritative classification.
    Falls back to deriving a reason from the four watch_part_check/
    brand_match_check/calibre_match_check/part_number_match_check fields
    (priority order: not-a-watch-part first as the coarsest failure, down
    to wrong-part-number) only when failure_category itself is
    unavailable -- e.g. rows imported from a completed file that predates
    the failure_category field. Rows with neither are reported in their
    own "no_diagnostic_data" bucket, never silently dropped."""
    if labels_df.empty:
        return pd.DataFrame(columns=["segment", "false_match_count", "reason", "count", "pct_of_segment_false_matches"])

    df = labels_df[labels_df["reviewer_label"] == "FALSE_MATCH"].copy()
    if df.empty:
        return pd.DataFrame(columns=["segment", "false_match_count", "reason", "count", "pct_of_segment_false_matches"])

    df["segment"] = df["matching_rule"] + "|" + df["source_table"] + "|" + df["collection_relationship"]

    def _classify(row) -> str:
        category = str(row.get("failure_category", "")).strip().upper()
        if category and category not in ("", "NAN", "NOT_APPLICABLE"):
            return category.lower()
        vals = {col: str(row.get(col, "")).strip().upper() for col in DIAGNOSTIC_COLUMNS}
        if all(v == "" for v in vals.values()):
            return "no_diagnostic_data"
        if vals.get("watch_part_check") == "NO":
            return "not_a_watch_part"
        if vals.get("brand_match_check") == "NO":
            return "wrong_brand"
        if vals.get("calibre_match_check") == "NO":
            return "wrong_calibre"
        if vals.get("part_number_match_check") == "NO":
            return "wrong_part_number"
        return "other_unclassified"

    df["reason"] = df.apply(_classify, axis=1)

    rows = []
    for segment, g in df.groupby("segment"):
        seg_total = len(g)
        for reason, count in g["reason"].value_counts().items():
            rows.append({
                "segment": segment,
                "false_match_count": seg_total,
                "reason": reason,
                "count": count,
                "pct_of_segment_false_matches": round(100 * count / seg_total, 1),
            })
    return pd.DataFrame(rows).sort_values(["segment", "count"], ascending=[True, False]).reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)

    p_import = sub.add_parser("import")
    p_import.add_argument("--completed", required=True)
    p_import.add_argument("--original-blinded", required=True)
    p_import.add_argument("--manifest", required=True)
    p_import.add_argument("--out", required=True)

    p_analyze = sub.add_parser("analyze")
    p_analyze.add_argument("--labels", required=True, nargs="+")
    p_analyze.add_argument("--out", required=True)

    args = parser.parse_args()

    if args.command == "import":
        try:
            result = import_labels(args.completed, args.original_blinded, args.manifest, args.out)
        except ImportValidationError as e:
            print("IMPORT REFUSED -- the following issues must be fixed before import:", file=sys.stderr)
            for err in e.errors:
                print(f"  - {err}", file=sys.stderr)
            sys.exit(1)
        print(f"Imported {len(result)} labeled rows to {args.out}")

    elif args.command == "analyze":
        frames = [pd.read_csv(p) for p in args.labels]
        labels_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        if labels_df.empty or labels_df["reviewer_label"].isna().any() or (labels_df["reviewer_label"].astype(str).str.strip() == "").any():
            print("ANALYSIS REFUSED -- no policy recommendation can be made from incomplete labels.", file=sys.stderr)
            sys.exit(1)
        segment_report = analyze_segments(labels_df)
        cluster_report = analyze_evidence_cluster_sensitivity(labels_df)
        diagnostic_report = analyze_false_match_diagnostics(labels_df)
        out_path = Path(args.out)
        segment_report.to_csv(out_path, index=False)
        cluster_path = out_path.with_name(out_path.stem + "_evidence_cluster_sensitivity.csv")
        cluster_report.to_csv(cluster_path, index=False)
        diagnostic_path = out_path.with_name(out_path.stem + "_false_match_diagnostics.csv")
        diagnostic_report.to_csv(diagnostic_path, index=False)
        print(f"Wrote segment analysis to {out_path}")
        print(f"Wrote evidence-cluster sensitivity analysis to {cluster_path}")
        print(f"Wrote false-match diagnostic breakdown to {diagnostic_path}")


if __name__ == "__main__":
    main()
