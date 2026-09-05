"""
09_export_review_sample.py
=============================
Module 5: reproducible, stratified human-review sample export.

Pure read + export -- reads match_decisions/staging_inventory read-only,
writes only a CSV with an EMPTY reviewer_label column for every row. This
script NEVER assigns a label. A human reviewer fills reviewer_label,
reviewer_reason, reviewed_by, reviewed_at afterward, off-line, in the
exported CSV or a copy of it -- see docs/MODULE5_REVIEW_GUIDE.md for the
review process this file feeds.

Stratification (docs/MODULE5_REVIEW_GUIDE.md):
  A. Tier A by rule: PART_NUMBER_EXACT, BRAND_PART_NUMBER, CALIBER_PART_NUMBER
  B. Active-targeted: SELF_SOURCED, CROSS_REFERENCED
  C. Historical sources separately: VCP aggregate, eBay sold
  D. Every named critical risk pattern present in the data
  E. Clean technically-strong candidates (deterministic_checks_passed=TRUE,
     no risk flags at all)

Sampling is deterministic: every draw is made from an explicitly
ORDER BY candidate_key-sorted pool before random.sample() -- the same
fixed-seed discipline as scripts/08_pilot_cohort_manifest.py, chosen
specifically to avoid the DuckDB row-order/random.sample() reproducibility
gap disclosed in docs/MODULE5_VALIDATION_POLICY_ANALYSIS.md Task 3.

Usage:
    python scripts/09_export_review_sample.py --db <path> --out <path>
"""

import argparse
import random
from pathlib import Path

import duckdb
import pandas as pd

SAMPLE_VERSION = "pilot_post_collection_review_v1"
SEED = 20260726

RISK_PATTERNS = [
    "multiple_reference_list_risk",
    "multiple_calibre_list_risk",
    "measurement_collision",
    "short_identifier_risk",
    "model_name_number_collision",
    "proper_noun_or_event_collision",
    "multiple_inventory_collision",
    "unverified_calibre_compatibility",
]

EXPORT_COLUMNS = [
    "validation_sample_version", "candidate_key", "match_run_id", "inventory_uid",
    "inventory_brand", "inventory_calibre", "inventory_part_number",
    "source_table", "source_id", "evidence_uid", "evidence_title", "matching_rule", "evidence_tier",
    "collection_relationship", "deterministic_checks_passed", "contradiction_flags",
    "risk_flags", "match_reason_code", "validation_segment", "match_status",
    "reviewer_label", "reviewer_reason", "reviewed_by", "reviewed_at",
]


def _base_query(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    table_map = {
        "match_candidates_active": "stg_active_targeted",
        "match_candidates_vcp": "stg_historical_vcp_aggregate",
        "match_candidates_ebay_sold": "stg_historical_ebay_sold",
    }
    frames = []
    for source_table, evidence_table in table_map.items():
        # evidence_uid join (preferred, Module 5 stable identity) with a
        # legacy source_id fallback — docs/MODULE5_POST_PHASE1_ARCHITECTURE_AUDIT.md
        # Phase 2: the previous plain `ON d.source_id = e.id` join was the
        # export-time contamination point the original lineage audit
        # flagged, and would also re-surface duplicate-evidence-disguised-
        # as-different-evidence (this file's own Phase-2 finding) if
        # joined naively on evidence_uid without first deduplicating the
        # evidence table down to one canonical row per stable_evidence_uid
        # (one real listing can span multiple staging rows — confirmed up
        # to 8 in the pilot data). canonical_evidence's ROW_NUMBER/rn=1
        # picks the smallest id deterministically, same tie-break
        # discipline as 06_decide_matches.py's evidence_by_uid.
        q = f"""
        WITH canonical_evidence AS (
            SELECT stable_evidence_uid, title,
                   ROW_NUMBER() OVER (PARTITION BY stable_evidence_uid ORDER BY id) AS rn
            FROM {evidence_table}
            WHERE stable_evidence_uid IS NOT NULL
        )
        SELECT d.candidate_key, d.match_run_id, d.inventory_uid, si.brand AS inventory_brand,
               si.caliber AS inventory_calibre, si.part_number AS inventory_part_number,
               d.source_table, d.source_id, d.evidence_uid,
               COALESCE(ce.title, e.title) AS evidence_title, d.matching_rule,
               d.evidence_tier, d.collection_relationship, d.deterministic_checks_passed,
               d.contradiction_flags, d.risk_flags, d.match_reason_code, d.match_status,
               d.confirmation_policy_reason, d.confirmation_policy_version
        FROM match_decisions d
        JOIN staging_inventory si ON d.inventory_uid = si.inventory_uid
        LEFT JOIN canonical_evidence ce ON d.evidence_uid = ce.stable_evidence_uid AND ce.rn = 1
        LEFT JOIN {evidence_table} e ON d.evidence_uid IS NULL AND d.source_id = e.id
        WHERE d.source_table = '{source_table}' AND COALESCE(ce.title, e.title) IS NOT NULL
        ORDER BY d.candidate_key
        """
        frames.append(conn.execute(q).df())
    df = pd.concat(frames, ignore_index=True)
    df["validation_segment"] = (
        df["matching_rule"] + "|" + df["source_table"] + "|" + df["collection_relationship"]
    )
    return df.sort_values("candidate_key").reset_index(drop=True)


def _sample(df: pd.DataFrame, n: int, rng: random.Random) -> pd.DataFrame:
    n = min(n, len(df))
    if n == 0:
        return df.iloc[0:0]
    idx = sorted(rng.sample(range(len(df)), n))
    return df.iloc[idx]


def build_review_sample(conn: duckdb.DuckDBPyConnection, *, per_stratum_target: int = 50) -> pd.DataFrame:
    df = _base_query(conn)
    rng = random.Random(SEED)
    picked: set = set()
    rows = []

    def _take(pool: pd.DataFrame, n: int, stratum_label: str):
        pool = pool[~pool["candidate_key"].isin(picked)].sort_values("candidate_key").reset_index(drop=True)
        chosen = _sample(pool, n, rng)
        for _, r in chosen.iterrows():
            picked.add(r["candidate_key"])
            rows.append({**r.to_dict(), "review_stratum": stratum_label})

    # A. Tier A by rule
    for rule in ["PART_NUMBER_EXACT", "BRAND_PART_NUMBER", "CALIBER_PART_NUMBER"]:
        pool = df[(df["matching_rule"] == rule) & (df["evidence_tier"] == "A")]
        _take(pool, per_stratum_target, f"A_TIER_A_{rule}")

    # B. SELF_SOURCED / CROSS_REFERENCED (active-targeted only)
    for rel in ["SELF_SOURCED", "CROSS_REFERENCED"]:
        pool = df[(df["source_table"] == "match_candidates_active") & (df["collection_relationship"] == rel)]
        _take(pool, per_stratum_target, f"B_{rel}")

    # C. Historical sources separately
    for source_table, label in [("match_candidates_vcp", "C_VCP_AGGREGATE"), ("match_candidates_ebay_sold", "C_EBAY_SOLD")]:
        pool = df[df["source_table"] == source_table]
        _take(pool, per_stratum_target, label)

    # D. Every named critical risk pattern
    for rp in RISK_PATTERNS:
        pool = df[df["risk_flags"].fillna("").str.contains(rp)]
        _take(pool, min(per_stratum_target, len(pool)), f"D_RISK_{rp}")

    # E. Clean technically-strong candidates, no risk flags at all
    pool = df[(df["deterministic_checks_passed"] == True) & (df["risk_flags"].isna())]  # noqa: E712
    _take(pool, per_stratum_target, "E_CLEAN_TECHNICALLY_STRONG")

    out = pd.DataFrame(rows)
    out["validation_sample_version"] = SAMPLE_VERSION
    out["reviewer_label"] = ""
    out["reviewer_reason"] = ""
    out["reviewed_by"] = ""
    out["reviewed_at"] = ""
    ordered_cols = ["review_stratum"] + EXPORT_COLUMNS
    return out[ordered_cols].sort_values(["review_stratum", "candidate_key"]).reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--per-stratum-target", type=int, default=50)
    args = parser.parse_args()

    conn = duckdb.connect(args.db, read_only=True)
    try:
        sample = build_review_sample(conn, per_stratum_target=args.per_stratum_target)
    finally:
        conn.close()

    out_path = Path(args.out)
    sample.to_csv(out_path, index=False)
    print(f"Wrote {len(sample)} rows to {out_path}")
    print(sample["review_stratum"].value_counts().sort_index())


if __name__ == "__main__":
    main()
