"""
21_evidence_confidence_engine.py
=================================
Autonomous evidence-confidence classification (owner spec, 2026-07-31,
docs/AUTONOMOUS_PRODUCTION_READINESS_REPORT.md).

GOVERNANCE BOUNDARY (never crossed by this module):
  - Never writes to validation_policy.
  - Never writes to match_decisions / MATCH_CONFIRMED.
  - Never claims a candidate was human-reviewed. Every classification this
    module produces is ALGORITHMIC and is labeled as such everywhere it is
    displayed (dashboard, reports).
  - This is an ADDITIVE, parallel classification -- the existing
    validation_policy-gated MATCH_CONFIRMED path in 06_decide_matches.py is
    completely unchanged and untouched by this module.

Replaces binary "MATCH_CONFIRMED or nothing" with 5 tiers, each defined by
DETERMINISTIC, EXPLAINABLE rules -- never a bare score threshold in
isolation for the top tier:

  AUTO_CONFIRMED    -- ALL of: part_number_exactness==1.0 (exact token-
                       boundary match), brand_match==1.0 (exact whole-word),
                       component_type_match==1.0 (G2 classifies WATCH_PART),
                       zero negative keywords, AND zero v1 contradiction/
                       risk flags. A strict conjunction of deterministic
                       checks, not a weighted-score threshold -- one weak
                       feature can never be "averaged away" by strong ones.
  HIGH_CONFIDENCE   -- v2_score >= 0.80, not AUTO_CONFIRMED (e.g. caliber
                       absent from title, or exactly one negative keyword,
                       but part number + brand + component type all solid).
  MEDIUM_CONFIDENCE -- 0.50 <= v2_score < 0.80.
  LOW_CONFIDENCE    -- v2_score < 0.50 and no hard rejection.
  REJECTED          -- brand_match==0.0 (wrong brand present, or absent
                       where required) OR any v1 contradiction flag fired
                       (brand conflict, calibre conflict, product-type
                       conflict) -- these are hard, not score-averaged.

TMV consumption policy (owner spec):
  AUTO_CONFIRMED    -> full TMV, same formula as human-validated evidence.
  HIGH_CONFIDENCE   -> TMV computed, explicitly flagged lower-confidence.
  MEDIUM_CONFIDENCE -> dashboard-visible (evidence exists), NO price.
  LOW_CONFIDENCE / REJECTED -> no TMV, no dashboard price claim.
Implemented in scripts/22_build_confidence_tmv.py, not here -- this module
only classifies.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import time
from pathlib import Path

import duckdb
import pandas as pd

BASE_DIR = Path(__file__).parent.parent
SCHEMA_PATH = BASE_DIR / "scripts" / "schema.sql"
DEFAULT_DB_PATH = BASE_DIR / "database" / "watchparts.duckdb"


def connect_write_retry(db_path: str, retries: int = 30, delay_seconds: float = 0.5):
    last_exc = None
    for attempt in range(retries + 1):
        try:
            return duckdb.connect(db_path)
        except duckdb.IOException as exc:
            last_exc = exc
            if "lock" not in str(exc).lower() or attempt == retries:
                raise
            time.sleep(delay_seconds)
    raise last_exc

_spec = importlib.util.spec_from_file_location("matching_v2", Path(__file__).parent / "20_matching_v2_verification.py")
matching_v2 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(matching_v2)

HIGH_CONFIDENCE_FLOOR = 0.80
MEDIUM_CONFIDENCE_FLOOR = 0.50

TIER_ORDER = ["REJECTED", "LOW_CONFIDENCE", "MEDIUM_CONFIDENCE", "HIGH_CONFIDENCE", "AUTO_CONFIRMED"]


def classify(v2_result: dict, has_contradiction: bool) -> tuple[str, str]:
    """Returns (confidence_tier, tier_reason). Pure function of the v2
    scoring result + whether any v1 contradiction fired -- never touches
    the database."""
    c = v2_result["components"]
    if has_contradiction or c["brand_match"] == 0.0:
        reason = (
            "explicit contradiction flag (brand/calibre/product-type conflict)"
            if has_contradiction else "brand_match failed -- wrong brand or brand absent"
        )
        return "REJECTED", reason

    if (
        c["part_number_exactness"] == 1.0 and c["brand_match"] == 1.0
        and c["component_type_match"] == 1.0 and c["listing_quality"] == 1.0
    ):
        return "AUTO_CONFIRMED", (
            "exact part-number token match, exact brand, G2-confirmed WATCH_PART, "
            "zero negative keywords, zero contradiction/risk flags"
        )

    if v2_result["score"] >= HIGH_CONFIDENCE_FLOOR:
        return "HIGH_CONFIDENCE", f"v2 score {v2_result['score']:.2f} >= {HIGH_CONFIDENCE_FLOOR} but not a perfect conjunction"

    if v2_result["score"] >= MEDIUM_CONFIDENCE_FLOOR:
        return "MEDIUM_CONFIDENCE", f"v2 score {v2_result['score']:.2f} in [{MEDIUM_CONFIDENCE_FLOOR}, {HIGH_CONFIDENCE_FLOOR})"

    return "LOW_CONFIDENCE", f"v2 score {v2_result['score']:.2f} < {MEDIUM_CONFIDENCE_FLOOR}"


def classify_dataframe(rows: pd.DataFrame) -> pd.DataFrame:
    """rows must have: candidate_key, inventory_uid, matching_rule,
    source_table, source_id, evidence_uid, brand, part_number, caliber,
    title, has_contradiction (bool). Returns rows + v2_score/confidence_tier/
    tier_reason/positive_features/negative_features columns."""
    out = []
    for r in rows.itertuples(index=False):
        res = matching_v2.score_candidate(r.brand, r.part_number, r.caliber, r.title)
        tier, reason = classify(res, bool(r.has_contradiction))
        out.append({
            "v2_score": res["score"], "confidence_tier": tier, "tier_reason": reason,
            "positive_features": json.dumps(res["positive_features"]),
            "negative_features": json.dumps(res["negative_features"]),
        })
    return pd.concat([rows.reset_index(drop=True), pd.DataFrame(out)], axis=1)


def build(conn: duckdb.DuckDBPyConnection, *, inventory_uid: str | None = None) -> pd.DataFrame:
    """Classifies every Tier-A match_decisions row (PART_NUMBER_EXACT,
    BRAND_PART_NUMBER, CALIBER_PART_NUMBER) that isn't already NO_MATCH from
    an explicit contradiction -- NO_MATCH rows are still classified
    (REJECTED, for completeness/audit trail) but their contradiction is
    passed through so REJECTED is guaranteed, never re-derived incorrectly."""
    where = """
        WHERE md.matching_rule IN ('PART_NUMBER_EXACT', 'BRAND_PART_NUMBER', 'CALIBER_PART_NUMBER')
    """
    params: list[str] = []
    if inventory_uid:
        where += " AND md.inventory_uid = ?"
        params.append(inventory_uid)
    rows = conn.execute(f"""
        SELECT
            md.candidate_key, md.inventory_uid, md.matching_rule, md.source_table,
            md.source_id, md.evidence_uid,
            si.brand, si.part_number, si.caliber,
            (md.contradiction_flags IS NOT NULL AND md.contradiction_flags <> '') AS has_contradiction,
            CASE md.source_table
                WHEN 'match_candidates_active' THEN a.normalized_title
                WHEN 'match_candidates_ebay_sold' THEN e.normalized_title
                WHEN 'match_candidates_vcp' THEN v.normalized_title
            END AS title
        FROM match_decisions md
        JOIN staging_inventory si ON si.inventory_uid = md.inventory_uid
        LEFT JOIN stg_active_targeted a
          ON md.source_table='match_candidates_active'
         AND a.id = md.source_id
        LEFT JOIN stg_historical_ebay_sold e
          ON md.source_table='match_candidates_ebay_sold'
         AND e.id = md.source_id
        LEFT JOIN stg_historical_vcp_aggregate v
          ON md.source_table='match_candidates_vcp'
         AND v.id = md.source_id
        {where}
    """, params).df()
    rows = rows.dropna(subset=["title"])
    if rows.empty:
        return rows
    return classify_dataframe(rows)


def write(
    conn: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
    classification_run_id: str,
    *,
    inventory_uid: str | None = None,
) -> None:
    conn.execute(SCHEMA_PATH.read_text())
    if inventory_uid:
        conn.execute("DELETE FROM evidence_confidence_classification WHERE inventory_uid = ?", [inventory_uid])
    else:
        conn.execute("DELETE FROM evidence_confidence_classification WHERE classification_run_id = ?", [classification_run_id])
    if df.empty:
        return
    out = df.copy()
    out["classification_run_id"] = classification_run_id
    next_id = conn.execute("SELECT COALESCE(MAX(classification_id), 0) + 1 FROM evidence_confidence_classification").fetchone()[0]
    out.insert(0, "classification_id", range(next_id, next_id + len(out)))
    cols = [
        "classification_id", "classification_run_id", "candidate_key", "inventory_uid", "matching_rule",
        "source_table", "source_id", "evidence_uid", "v2_score", "confidence_tier", "tier_reason",
        "positive_features", "negative_features",
    ]
    conn.register("t_conf", out[cols])
    conn.execute(f"INSERT INTO evidence_confidence_classification ({','.join(cols)}) SELECT {','.join(cols)} FROM t_conf")
    conn.unregister("t_conf")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(os.environ.get("WATCHPARTS_DB", DEFAULT_DB_PATH)))
    ap.add_argument("--run-id", default="confidence_run_v1")
    ap.add_argument("--inventory-uid", default=None)
    args = ap.parse_args()

    conn = connect_write_retry(args.db)
    print(f"Database target: {args.db}")
    df = build(conn, inventory_uid=args.inventory_uid)
    write(conn, df, args.run_id, inventory_uid=args.inventory_uid)
    conn.close()

    if df.empty:
        print("No Tier-A candidates found to classify.")
        return
    print(f"Classified {len(df):,} candidate relationships.")
    for tier, count in df["confidence_tier"].value_counts().items():
        print(f"  {tier}: {count:,}")


if __name__ == "__main__":
    main()
