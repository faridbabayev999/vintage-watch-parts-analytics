"""
08_pilot_cohort_manifest.py
=============================
Module 5: bounded active-targeted collection PILOT cohort selection.

Pure selection logic — reads staging_inventory + match_decisions
read-only, writes only a versioned manifest file. Never calls the eBay
API, never triggers collection, never writes to raw_active_targeted or
any staging table. Selecting which 728-39=689 not-yet-self-sourced
inventory items would most usefully expand SELF_SOURCED evidence is a
separate concern from actually collecting them
(scripts/04_collect_targeted_active.py --inventory-uid <uid>, run
per-item once this manifest is reviewed/authorized).

Stratification (docs/MODULE5_COLLECTION_PILOT_PLAN.md):
  A. Tier A evidence exists but ONLY via CROSS_REFERENCED (no SELF_SOURCED
     at all yet) -- highest-value cohort: directly tests whether
     self-sourced collection changes the evidentiary picture for an item
     that already has SOME Tier A signal.
  B. Only Tier B evidence (no Tier A at all).
  C. Only Tier C evidence (no Tier A/B). Reported as empty if the
     population has none -- never fabricated to hit a target count.
  D. NO_CANDIDATES (zero evidence of any kind).
  E. Deliberate risk-pattern top-up (reference-list, multi-calibre-list,
     measurement collision, short identifier, multi-inventory collision,
     unverified calibre compatibility) -- items carrying these flags are
     already partially represented in A/B/D by chance; this stratum tops
     up any pattern that random draw alone would under-represent,
     including exhausting the ENTIRE population for the rarest pattern
     (unverified_calibre_compatibility, currently only 2 items total).
  F. Brand/caliber/part-number-structure/stock diversity is applied within
     each stratum's draw (not a separate exclusive pool).

Sampling is deterministic: every draw is made from an explicitly
ORDER BY inventory_uid-sorted DataFrame before random.sample() -- fixing
the exact reproducibility gap disclosed in
docs/MODULE5_VALIDATION_POLICY_ANALYSIS.md Task 3 (DuckDB does not
guarantee row order without an explicit ORDER BY).

Usage:
    python scripts/08_pilot_cohort_manifest.py --db <path> --out <path>
"""

import argparse
import json
import random
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

BASE_DIR = Path(__file__).parent.parent
MANIFEST_VERSION = "pilot_cohort_v1"
SEED = 20260726  # fixed, documented -- today's date at design time, never re-rolled silently

RISK_FLAGS_OF_INTEREST = [
    "short_identifier_risk",
    "multiple_reference_list_risk",
    "multiple_calibre_list_risk",
    "measurement_collision",
    "multiple_inventory_collision",
    "unverified_calibre_compatibility",
]


def _load_item_profile(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    q = """
    WITH item_tiers AS (
        SELECT inventory_uid,
               MAX(CASE WHEN evidence_tier='A' THEN 1 ELSE 0 END) AS has_a,
               MAX(CASE WHEN evidence_tier='B' THEN 1 ELSE 0 END) AS has_b,
               MAX(CASE WHEN evidence_tier='C' THEN 1 ELSE 0 END) AS has_c,
               MAX(CASE WHEN source_table='match_candidates_active'
                         AND collection_relationship='SELF_SOURCED' THEN 1 ELSE 0 END) AS has_self
        FROM match_decisions GROUP BY inventory_uid
    )
    SELECT si.inventory_uid, si.brand, si.caliber, si.part_number, si.stock,
           COALESCE(it.has_a,0) AS has_a, COALESCE(it.has_b,0) AS has_b,
           COALESCE(it.has_c,0) AS has_c, COALESCE(it.has_self,0) AS has_self
    FROM staging_inventory si LEFT JOIN item_tiers it ON si.inventory_uid = it.inventory_uid
    WHERE si.validation_status <> 'FAIL'
    ORDER BY si.inventory_uid
    """
    df = conn.execute(q).df()
    for rf in RISK_FLAGS_OF_INTEREST:
        risky_uids = set(r[0] for r in conn.execute(
            f"SELECT DISTINCT inventory_uid FROM match_decisions WHERE risk_flags LIKE '%{rf}%'"
        ).fetchall())
        df[f"risk_{rf}"] = df["inventory_uid"].isin(risky_uids)
    return df


def _sample(df: pd.DataFrame, n: int, rng: random.Random) -> pd.DataFrame:
    """df MUST already be ORDER BY-sorted before this is called."""
    n = min(n, len(df))
    idx = sorted(rng.sample(range(len(df)), n))
    return df.iloc[idx]


def build_manifest(conn: duckdb.DuckDBPyConnection, *, pilot_size: int = 100) -> pd.DataFrame:
    df = _load_item_profile(conn)
    rng = random.Random(SEED)

    not_self_sourced = df[df["has_self"] == 0].sort_values("inventory_uid").reset_index(drop=True)

    cat_a = not_self_sourced[(not_self_sourced["has_a"] == 1)]
    cat_b = not_self_sourced[(not_self_sourced["has_a"] == 0) & (not_self_sourced["has_b"] == 1)]
    cat_c = not_self_sourced[
        (not_self_sourced["has_a"] == 0) & (not_self_sourced["has_b"] == 0) & (not_self_sourced["has_c"] == 1)
    ]
    cat_d = not_self_sourced[
        (not_self_sourced["has_a"] == 0) & (not_self_sourced["has_b"] == 0) & (not_self_sourced["has_c"] == 0)
    ]

    picked_uids: set = set()
    rows = []

    def _take(pool: pd.DataFrame, n: int, stratum: str, reason: str):
        pool = pool[~pool["inventory_uid"].isin(picked_uids)].sort_values("inventory_uid").reset_index(drop=True)
        chosen = _sample(pool, n, rng)
        for _, r in chosen.iterrows():
            picked_uids.add(r["inventory_uid"])
            rows.append({**r.to_dict(), "stratum": stratum, "selection_reason": reason})

    _take(cat_d, 20, "D_NO_CANDIDATES", "Zero evidence of any kind -- tests whether targeted collection can establish first evidence at all")
    _take(cat_b, 30, "B_TIER_B_ONLY", "Only Tier B (caliber-level) evidence -- tests whether self-sourced collection can surface Tier A-strength evidence")
    _take(cat_a, 30, "A_TIER_A_CROSS_REFERENCED_ONLY", "Tier A evidence exists but only via CROSS_REFERENCED -- tests whether the same item, self-queried, produces corroborating or contradicting SELF_SOURCED evidence")
    # Category C is reported honestly as empty if no pure Tier-C-only items exist.
    if not cat_c.empty:
        _take(cat_c, min(10, len(cat_c)), "C_TIER_C_ONLY", "Only Tier C (component-level) evidence")

    # E: deliberate risk-pattern top-up, including exhausting the rarest pattern entirely
    rarest = not_self_sourced[not_self_sourced["risk_unverified_calibre_compatibility"]]
    _take(rarest, len(rarest), "E_RISK_UNVERIFIED_CALIBRE_COMPATIBILITY", "Entire remaining population carrying this rare, high-priority risk pattern (only 2 items project-wide)")
    for rf, n in [
        ("risk_short_identifier_risk", 4),
        ("risk_multiple_reference_list_risk", 4),
        ("risk_multiple_calibre_list_risk", 4),
        ("risk_measurement_collision", 4),
        ("risk_multiple_inventory_collision", 4),
    ]:
        pool = not_self_sourced[not_self_sourced[rf]]
        _take(pool, n, "E_RISK_TOPUP", f"Deliberate top-up for {rf.replace('risk_', '')} -- ensures known edge-case representation is not left to chance")

    # F: brand diversity top-up -- ensure Tudor is represented even though rare (35/728 overall)
    tudor_pool = not_self_sourced[not_self_sourced["brand"] == "Tudor"]
    _take(tudor_pool, 6, "F_BRAND_DIVERSITY", "Tudor items are a small minority (35/728) -- explicit top-up so the pilot is not Rolex-only")

    # Fill remaining slots to reach pilot_size with additional stratified A/B draws
    remaining = pilot_size - len(rows)
    if remaining > 0:
        half = remaining // 2
        _take(cat_a, half, "A_TOPUP", "Additional Tier A CROSS_REFERENCED-only items to reach target pilot size")
        _take(cat_b, remaining - half, "B_TOPUP", "Additional Tier B-only items to reach target pilot size")

    out = pd.DataFrame(rows)
    out["manifest_version"] = MANIFEST_VERSION
    out["pilot_batch_assignment"] = [f"pilot_batch_{i // 25 + 1}" for i in range(len(out))]  # 25 items/batch, resumable
    out["current_evidence_category"] = out["stratum"]
    out["current_collection_status"] = "NOT_YET_SELF_SOURCED"
    out["expected_validation_contribution"] = out["stratum"].map({
        "D_NO_CANDIDATES": "establishes first evidence; contributes to NO_CANDIDATES->candidate-evidence coverage metric",
        "B_TIER_B_ONLY": "tests Tier B->Tier A promotion via self-sourced collection",
        "A_TIER_A_CROSS_REFERENCED_ONLY": "tests CROSS_REFERENCED-only->SELF_SOURCED coverage improvement",
        "A_TOPUP": "tests CROSS_REFERENCED-only->SELF_SOURCED coverage improvement",
        "B_TOPUP": "tests Tier B->Tier A promotion via self-sourced collection",
        "C_TIER_C_ONLY": "tests Tier C->Tier B/A promotion via self-sourced collection",
        "E_RISK_UNVERIFIED_CALIBRE_COMPATIBILITY": "expands the smallest known edge-case population for future segment review",
        "E_RISK_TOPUP": "ensures deliberate representation of a named risk pattern in the future review sample",
        "F_BRAND_DIVERSITY": "ensures Tudor representation in the future review sample",
    })
    return out.sort_values(["stratum", "inventory_uid"]).reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--pilot-size", type=int, default=100)
    args = parser.parse_args()

    conn = duckdb.connect(args.db, read_only=True)
    try:
        manifest = build_manifest(conn, pilot_size=args.pilot_size)
    finally:
        conn.close()

    out_path = Path(args.out)
    manifest.to_csv(out_path, index=False)

    summary = {
        "manifest_version": MANIFEST_VERSION,
        "seed": SEED,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "pilot_size": len(manifest),
        "by_stratum": manifest["stratum"].value_counts().to_dict(),
        "distinct_brands": sorted(manifest["brand"].unique().tolist()),
    }
    with open(out_path.with_suffix(".summary.json"), "w") as f:
        json.dump(summary, f, indent=2)

    print(f"Wrote {len(manifest)} rows to {out_path}")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
