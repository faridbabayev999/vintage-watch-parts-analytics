"""
10_build_calibration_sample.py
=================================
Module 5: deterministic 40-row BLINDED calibration sample.

Purpose: validate the review guide and label definitions on a small,
deliberately varied set BEFORE committing the larger 50-per-segment
review effort (docs/MODULE5_SAMPLE_SIZE_PLANNING.md). Not intended to
produce a segment-level precision estimate on its own.

Reads reports/module5_pilot/review_sample_v1.csv only. Writes two files:
review_calibration_v1_blinded.csv (no system predictions -- see
scripts/review_sample_blinding.py's blinding contract) and
review_calibration_v1_manifest.csv (full lineage, audit-only). No
reviewer_label is ever populated by this script.

Internal stratification (system fields used only to SELECT rows, never
exposed in the blinded file): a coarse 5-way split so the calibration set
exercises every label definition in the guide at least a few times --
Tier A clean, Tier A risk-flagged, Tier B, Tier C, and NO_MATCH
(contradiction-flagged) -- 8 rows each. This is deliberately coarser
than the 15-stratum review pool; the calibration set's job is breadth
across DECISION TYPES the guide must cover, not segment-level precision.

Sampling is deterministic: draws are made from an explicitly
candidate_key-sorted pool before random.sample() (same discipline as
scripts/08 and scripts/09).

Usage:
    python scripts/10_build_calibration_sample.py --pool <csv> --out-dir <dir>
"""

import argparse
import random
from pathlib import Path

import pandas as pd

import review_sample_blinding as blinding

SAMPLE_VERSION = "review_calibration_v1"
SEED = 20260727
ROW_ID_PREFIX = "CAL"
PER_GROUP_TARGET = 8


def _sample(df: pd.DataFrame, n: int, rng: random.Random) -> pd.DataFrame:
    n = min(n, len(df))
    if n == 0:
        return df.iloc[0:0]
    idx = sorted(rng.sample(range(len(df)), n))
    return df.iloc[idx]


def build_calibration_sample(pool_df: pd.DataFrame) -> pd.DataFrame:
    """pool_df must already be the deduplicated (one row per candidate
    relationship) pool from review_sample_blinding.load_deduplicated_pool.
    """
    rng = random.Random(SEED)
    df = pool_df.sort_values("candidate_key").reset_index(drop=True)

    is_no_match = df["match_status"] == "NO_MATCH"
    is_tier_a_clean = (
        (df["evidence_tier"] == "A")
        & (df["deterministic_checks_passed"] == True)  # noqa: E712
        & df["risk_flags"].isna()
        & ~is_no_match
    )
    is_tier_a_risk = (df["evidence_tier"] == "A") & ~is_tier_a_clean & ~is_no_match
    is_tier_b = (df["evidence_tier"] == "B") & ~is_no_match
    is_tier_c = (df["evidence_tier"] == "C") & ~is_no_match

    groups = {
        "tier_a_clean": df[is_tier_a_clean],
        "tier_a_risk": df[is_tier_a_risk],
        "tier_b": df[is_tier_b],
        "tier_c": df[is_tier_c],
        "no_match": df[is_no_match],
    }

    picked: set = set()
    rows = []
    for _, pool in groups.items():
        pool = pool[~pool["candidate_key"].isin(picked)].sort_values("candidate_key").reset_index(drop=True)
        chosen = _sample(pool, PER_GROUP_TARGET, rng)
        for _, r in chosen.iterrows():
            picked.add(r["candidate_key"])
            rows.append(r.to_dict())

    return pd.DataFrame(rows).sort_values("candidate_key").reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pool", required=True)
    parser.add_argument("--out-dir", required=True)
    args = parser.parse_args()

    dedup_pool = blinding.load_deduplicated_pool(args.pool)
    calibration = build_calibration_sample(dedup_pool)

    blinded, manifest = blinding.to_blinded_and_manifest(
        calibration, row_id_prefix=ROW_ID_PREFIX, sample_version=SAMPLE_VERSION, seed=SEED,
    )

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    blinded_path = out_dir / "review_calibration_v1_blinded.csv"
    manifest_path = out_dir / "review_calibration_v1_manifest.csv"
    blinded.to_csv(blinded_path, index=False)
    manifest.to_csv(manifest_path, index=False)

    print(f"Wrote {len(blinded)} rows to {blinded_path}")
    print(f"Wrote {len(manifest)} rows to {manifest_path}")


if __name__ == "__main__":
    main()
