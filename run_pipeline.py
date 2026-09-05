#!/usr/bin/env python3
"""Professor-submission pipeline wrapper.

This is an execution wrapper only. It does not change analytical logic.

Default mode is intentionally professor-safe: it refreshes reference tables and
rebuilds the final dashboard contract from the included DuckDB snapshot without
re-ingesting raw files or regenerating large candidate tables. That makes the
main review command fast, repeatable, and offline-friendly.

Use --full-rebuild to rerun the heavier development pipeline from raw/staging
through matching and TMV. Use --collect-live-ebay with --full-rebuild to refresh
live eBay data, which requires .env credentials.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DEFAULT_DB = ROOT / "database" / "watchparts.duckdb"

REVIEW_STEPS = [
    ["scripts/00b_load_scenario_rates.py"],
    ["scripts/00c_load_tmv_parameters.py"],
    ["scripts/23_build_dashboard_contract.py"],
]

FULL_REBUILD_STEPS = [
    ["scripts/00_load_fx_rates.py"],
    ["scripts/00b_load_scenario_rates.py"],
    ["scripts/00c_load_tmv_parameters.py"],
    ["scripts/01_ingest.py"],
    ["scripts/02_clean.py"],
    ["scripts/03_generate_queries.py"],
    ["scripts/05_generate_match_candidates.py"],
    ["scripts/06_decide_matches.py"],
    ["scripts/21_evidence_confidence_engine.py"],
    ["scripts/22_build_confidence_tmv.py"],
    ["scripts/24_reconcile_status_and_identity.py"],
    ["scripts/23_build_dashboard_contract.py"],
]
LIVE_COLLECTION_STEP = ["scripts/04_collect_targeted_active.py", "--resume"]

def run_step(step: list[str], env: dict[str, str], dry_run: bool) -> None:
    cmd = [sys.executable, *step]
    print("\n$ " + " ".join(cmd), flush=True)
    if not dry_run:
        subprocess.run(cmd, cwd=ROOT, env=env, check=True)

def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Vintage Watch Parts analytics pipeline.")
    parser.add_argument("--db", default=str(DEFAULT_DB), help="DuckDB path. Default: database/watchparts.duckdb")
    parser.add_argument("--full-rebuild", action="store_true", help="Run the full raw-to-dashboard rebuild. Slower and may append audit/bookkeeping rows.")
    parser.add_argument("--collect-live-ebay", action="store_true", help="Run live eBay targeted collection before matching. Requires .env credentials.")
    parser.add_argument("--dry-run", action="store_true", help="Print commands without executing them.")
    args = parser.parse_args()
    if args.collect_live_ebay and not args.full_rebuild:
        parser.error("--collect-live-ebay requires --full-rebuild")
    env = os.environ.copy()
    db_path = Path(args.db)
    env["WATCHPARTS_DB"] = str((ROOT / db_path).resolve() if not db_path.is_absolute() else db_path)
    print("Vintage Watch Parts Analytics pipeline")
    print(f"Project root: {ROOT}")
    print(f"Database: {env['WATCHPARTS_DB']}")

    if not args.full_rebuild:
        print("Mode: review refresh (fast, offline-safe, repeatable)")
        for step in REVIEW_STEPS:
            run_step(step, env, args.dry_run)
        print("\nReview refresh finished. Final client table: dashboard_inventory_pricing")
        return

    print("Mode: full rebuild (slow; may append audit/bookkeeping rows)")
    for step in FULL_REBUILD_STEPS[:6]:
        run_step(step, env, args.dry_run)
    if args.collect_live_ebay:
        run_step(LIVE_COLLECTION_STEP, env, args.dry_run)
    else:
        print("\nSkipping live eBay collection. Use --collect-live-ebay to refresh active listings with API credentials.")
    for step in FULL_REBUILD_STEPS[6:]:
        run_step(step, env, args.dry_run)
    print("\nPipeline finished. Final client table: dashboard_inventory_pricing")

if __name__ == "__main__":
    main()
