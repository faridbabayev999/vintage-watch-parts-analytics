"""
11_build_priority_sample.py
==============================
Module 5: deterministic PRIMARY (priority) review sample, ~120-150 rows,
BLINDED. Prepared but NOT to be released for review until the 40-row
calibration set (scripts/10_build_calibration_sample.py) has been
reviewed and the guide corrected if needed
(docs/MODULE5_REVIEW_CALIBRATION_GUIDE_V1.md).

Composition, deliberately weighted toward what could actually move a
policy-approval decision:
  - 90 rows: clean Tier A candidates (deterministic_checks_passed=TRUE,
    zero risk/contradiction flags), 30 each from the three Tier A rules
    (PART_NUMBER_EXACT / BRAND_PART_NUMBER / CALIBER_PART_NUMBER) --
    these are the only rows that could ever become part of an APPROVED
    segment's evidence base (docs/MODULE5_VALIDATION_SEGMENTS.md).
  - ~45 rows: a bounded risk-validation subset, spread across the 7
    populated named risk patterns in the pool (~6-7 each) -- confirms
    whether the detectors' REVIEW_REQUIRED routing is correct, which is
    the OTHER thing (besides raw Tier A precision) a policy decision
    needs evidence for.

Total: ~135 rows, within the 120-150 target range.

Reads reports/module5_pilot/review_sample_v1.csv, EXCLUDES every
candidate_key already used in the calibration sample (loaded from
review_calibration_v1_manifest.csv, never re-included), deduplicates by
candidate relationship exactly as the calibration sample does. Writes
review_priority_v1_blinded.csv and review_priority_v1_manifest.csv. Never
populates reviewer_label.

Usage:
    python scripts/11_build_priority_sample.py --pool <csv> \
        --calibration-manifest <csv> --out-dir <dir>
"""

import argparse
import random
from pathlib import Path

import pandas as pd

import review_sample_blinding as blinding

SAMPLE_VERSION = "review_priority_v1"
SEED = 20260728
ROW_ID_PREFIX = "PRI"
TIER_A_PER_RULE_TARGET = 30
RISK_PER_PATTERN_TARGET = 7

TIER_A_RULES = ["PART_NUMBER_EXACT", "BRAND_PART_NUMBER", "CALIBER_PART_NUMBER"]
RISK_PATTERNS = [
    "multiple_reference_list_risk",
    "multiple_calibre_list_risk",
    "measurement_collision",
    "short_identifier_risk",
    "model_name_number_collision",
    "multiple_inventory_collision",
    "unverified_calibre_compatibility",
]


def _sample(df: pd.DataFrame, n: int, rng: random.Random) -> pd.DataFrame:
    n = min(n, len(df))
    if n == 0:
        return df.iloc[0:0]
    idx = sorted(rng.sample(range(len(df)), n))
    return df.iloc[idx]


def build_priority_sample(pool_df: pd.DataFrame, excluded_candidate_keys: set) -> pd.DataFrame:
    rng = random.Random(SEED)
    df = pool_df[~pool_df["candidate_key"].isin(excluded_candidate_keys)].sort_values(
        "candidate_key"
    ).reset_index(drop=True)

    is_clean_tier_a = (
        (df["evidence_tier"] == "A")
        & (df["deterministic_checks_passed"] == True)  # noqa: E712
        & df["risk_flags"].isna()
        & df["contradiction_flags"].isna()
    )

    picked: set = set()
    rows = []

    for rule in TIER_A_RULES:
        pool = df[is_clean_tier_a & (df["matching_rule"] == rule) & ~df["candidate_key"].isin(picked)]
        pool = pool.sort_values("candidate_key").reset_index(drop=True)
        chosen = _sample(pool, TIER_A_PER_RULE_TARGET, rng)
        for _, r in chosen.iterrows():
            picked.add(r["candidate_key"])
            rows.append(r.to_dict())

    for pattern in RISK_PATTERNS:
        pool = df[
            df["risk_flags"].fillna("").str.contains(pattern) & ~df["candidate_key"].isin(picked)
        ].sort_values("candidate_key").reset_index(drop=True)
        chosen = _sample(pool, RISK_PER_PATTERN_TARGET, rng)
        for _, r in chosen.iterrows():
            picked.add(r["candidate_key"])
            rows.append(r.to_dict())

    return pd.DataFrame(rows).sort_values("candidate_key").reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pool", required=True)
    parser.add_argument("--calibration-manifest", required=True)
    parser.add_argument("--out-dir", required=True)
    args = parser.parse_args()

    dedup_pool = blinding.load_deduplicated_pool(args.pool)
    calibration_manifest = pd.read_csv(args.calibration_manifest)
    excluded = set(calibration_manifest["candidate_key"])

    priority = build_priority_sample(dedup_pool, excluded)

    blinded, manifest = blinding.to_blinded_and_manifest(
        priority, row_id_prefix=ROW_ID_PREFIX, sample_version=SAMPLE_VERSION, seed=SEED,
    )

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    blinded_path = out_dir / "review_priority_v1_blinded.csv"
    manifest_path = out_dir / "review_priority_v1_manifest.csv"
    blinded.to_csv(blinded_path, index=False)
    manifest.to_csv(manifest_path, index=False)

    print(f"Wrote {len(blinded)} rows to {blinded_path}")
    print(f"Wrote {len(manifest)} rows to {manifest_path}")

    assert len(set(manifest["candidate_key"]) & excluded) == 0, "priority sample must not include calibration rows"


if __name__ == "__main__":
    main()
