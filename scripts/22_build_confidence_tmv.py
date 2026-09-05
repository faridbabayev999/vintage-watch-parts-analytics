"""
22_build_confidence_tmv.py
============================
Autonomous, ALGORITHMIC TMV path (owner spec, 2026-07-31,
docs/AUTONOMOUS_PRODUCTION_READINESS_REPORT.md). Consumes evidence tiered
AUTO_CONFIRMED or HIGH_CONFIDENCE by
scripts/21_evidence_confidence_engine.py -- reuses scripts/13_build_tmv.py's
exact H/C/D/S/P math verbatim (via evidence_source='ALGORITHMIC_AUTO_HIGH'),
not a reimplementation, so the formula is identical to the human-governed
path; only the EVIDENCE SOURCE differs.

GOVERNANCE BOUNDARY:
  - Writes to NEW tables (tmv_results_algorithmic, feat_pricing_algorithmic,
    turnover_survival_algorithmic) -- NEVER touches tmv_results/
    feat_pricing/turnover_survival, which remain reserved for the
    human-governed MATCH_CONFIRMED path and are completely unaffected by
    this script.
  - Every row is tagged evidence_basis_type='ALGORITHMIC' and
    confidence_tier (AUTO_CONFIRMED or HIGH_CONFIDENCE, item-level =
    the WEAKEST tier among the evidence actually contributing to that
    item's TMV -- never the best-case label).
  - Per owner's TMV consumption policy: AUTO_CONFIRMED items get a price
    with no additional caveat beyond the tier label; HIGH_CONFIDENCE items
    get the same TMV math but the dashboard must show an explicit
    lower-confidence flag (enforced in scripts/16_dashboard.py).
"""
from __future__ import annotations

import argparse
import importlib.util
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

_spec13 = importlib.util.spec_from_file_location("tmv13", Path(__file__).parent / "13_build_tmv.py")
tmv13 = importlib.util.module_from_spec(_spec13)
_spec13.loader.exec_module(tmv13)

def _item_level_tiers(conn) -> dict:
    """Weakest confidence tier per inventory_uid among AUTO_CONFIRMED/
    HIGH_CONFIDENCE evidence -- an item with mixed evidence quality is
    labeled by its WEAKEST contributing evidence, never overstated."""
    rows = conn.execute("""
        SELECT inventory_uid, confidence_tier FROM evidence_confidence_classification
        WHERE confidence_tier IN ('AUTO_CONFIRMED', 'HIGH_CONFIDENCE')
    """).fetchall()
    out: dict[str, str] = {}
    for uid, tier in rows:
        if uid not in out:
            out[uid] = tier
        elif out[uid] == "AUTO_CONFIRMED" and tier == "HIGH_CONFIDENCE":
            out[uid] = "HIGH_CONFIDENCE"  # weaker tier wins
    return out


def build_and_write(conn: duckdb.DuckDBPyConnection) -> dict:
    conn.execute(SCHEMA_PATH.read_text())
    conn.execute("DELETE FROM tmv_results_algorithmic")
    conn.execute("DELETE FROM turnover_survival_algorithmic")

    res = tmv13.build(conn, evidence_source="ALGORITHMIC_AUTO_HIGH")
    df = res["df"]
    if df.empty:
        return {"items": 0}

    item_tiers = _item_level_tiers(conn)
    # inventory_uid -> canonical_inventory_id mapping already on df
    df = df.copy()
    df["item_confidence_tier"] = df["inventory_uid"].map(item_tiers)
    # Any item whose TMV math ran but has no AUTO/HIGH classification row
    # (shouldn't happen given the evidence source filter, but never assume)
    df = df.dropna(subset=["item_confidence_tier"])
    if df.empty:
        return {"items": 0}

    out = pd.DataFrame({
        "canonical_inventory_id": df["canonical_inventory_id"],
        "tmv_eur": df["tmv"], "tmv_low_eur": df["tmv_low"], "tmv_high_eur": df["tmv_high"],
        "confidence_tier": df["item_confidence_tier"],
        "valuation_basis": df["valuation_basis"],
        "historical_value_eur": pd.to_numeric(df["H"], errors="coerce").round(2),
        "current_value_eur": pd.to_numeric(df["C"], errors="coerce").round(2),
        # S/P/D: already computed by build() and already baked into tmv_eur
        # via the formula -- persisted here (2026-08-01 fix) purely so a
        # client view can show the real number instead of a blank. Never
        # recomputed, never defaulted to 0 -- read verbatim from the same
        # df the price itself came from.
        "scarcity_score": pd.to_numeric(df["S"], errors="coerce").round(4),
        "price_trend": pd.to_numeric(df["P"], errors="coerce").round(4),
        "demand_index": pd.to_numeric(df["D"], errors="coerce").round(4),
        "recommendation_reason": df["recommendation_reason"],
    })
    conn.register("t_algo_tmv", out)
    conn.execute("""
        INSERT INTO tmv_results_algorithmic
        (canonical_inventory_id, tmv_eur, tmv_low_eur, tmv_high_eur, confidence_tier,
         valuation_basis, historical_value_eur, current_value_eur,
         scarcity_score, price_trend, demand_index, recommendation_reason)
        SELECT canonical_inventory_id, tmv_eur, tmv_low_eur, tmv_high_eur, confidence_tier,
               valuation_basis, historical_value_eur, current_value_eur,
               scarcity_score, price_trend, demand_index, recommendation_reason
        FROM t_algo_tmv
    """)
    conn.unregister("t_algo_tmv")

    turn = pd.DataFrame({
        "canonical_inventory_id": df["canonical_inventory_id"],
        "median_days_to_sell": df["median_days_to_sell"],
        "probability_sell_30d": df["prob_30"], "probability_sell_90d": df["prob_90"],
        "turnover_bucket_forecast": df["turnover_bucket_forecast"],
    })
    conn.register("t_algo_turn", turn)
    conn.execute("""
        INSERT INTO turnover_survival_algorithmic
        (canonical_inventory_id, median_days_to_sell, probability_sell_30d, probability_sell_90d, turnover_bucket_forecast)
        SELECT canonical_inventory_id, median_days_to_sell, probability_sell_30d, probability_sell_90d, turnover_bucket_forecast
        FROM t_algo_turn
    """)
    conn.unregister("t_algo_turn")

    return {
        "items": len(out),
        "auto_confirmed": int((out["confidence_tier"] == "AUTO_CONFIRMED").sum()),
        "high_confidence": int((out["confidence_tier"] == "HIGH_CONFIDENCE").sum()),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(os.environ.get("WATCHPARTS_DB", DEFAULT_DB_PATH)))
    args = ap.parse_args()

    conn = connect_write_retry(args.db)
    print(f"Database target: {args.db}")
    result = build_and_write(conn)
    conn.close()
    print(f"Algorithmic TMV items: {result['items']:,}")
    if result["items"]:
        print(f"  AUTO_CONFIRMED: {result['auto_confirmed']:,}")
        print(f"  HIGH_CONFIDENCE: {result['high_confidence']:,}")


if __name__ == "__main__":
    main()
