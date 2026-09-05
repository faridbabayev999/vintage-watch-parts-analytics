"""
15_historical_drop_ingest.py
============================
Drop-folder automation for historical exports (Module 1 — historical
ingestion). A user drops a Terapeak / Product Research CSV export into
data/raw/historical_drop/; everything after that is automatic:

    discover new files
      -> sha256 hash each
      -> detect source type (eBay sold vs VCP aggregate) by columns
      -> schema-validate required columns
      -> idempotency guard (skip a file whose (source_type, filename, hash)
         is already ingested — see 01_ingest.ingestion_log)
      -> route into the proven 01_ingest.py per-source ingestion
      -> archive the processed file to historical_drop/processed/

This module does NOT scrape Terapeak/Product Research (out of scope, ToS/UI
risk). Only the manual export download stays manual; the entire ETL after the
drop is automated and idempotent. Reuses 01_ingest.py's primitives rather than
reimplementing ingestion. WATCHPARTS_DB env-aware (inherited from 01_ingest).

Existing broad historical pool is untouched — new drops are additive.

Usage:
    python scripts/15_historical_drop_ingest.py
    python scripts/15_historical_drop_ingest.py --run-clean   # also run 02_clean after
    python scripts/15_historical_drop_ingest.py --db /tmp/copy.duckdb
"""
from __future__ import annotations

import argparse
import importlib.util
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).parent.parent
SCRIPTS_DIR = BASE_DIR / "scripts"
DROP_DIR = BASE_DIR / "data" / "raw" / "historical_drop"
PROCESSED_DIR = DROP_DIR / "processed"
REJECTED_DIR = DROP_DIR / "rejected"


def _load_ingest01():
    spec = importlib.util.spec_from_file_location("ingest01", SCRIPTS_DIR / "01_ingest.py")
    m = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(SCRIPTS_DIR))
    spec.loader.exec_module(m)
    return m


# Column signatures that identify each historical source (subset match).
SOURCE_SIGNATURES = {
    "ebay_sold": {"item_number", "sold_date_iso", "is_sold"},
    "vcp": {"avg_price_eur", "total_sold"},
}
# Minimum columns required for a file to be accepted for a detected source.
REQUIRED_COLUMNS = {
    "ebay_sold": {"item_number", "title", "price_eur", "sold_date_iso"},
    "vcp": {"avg_price_eur", "total_sold"},
}
# ingestion_log.source_type value written by 01_ingest for each source — must
# match exactly for the idempotency guard to recognize an already-ingested file.
# (01 logs eBay-sold as 'historical_ebay_sold' but VCP as plain 'historical'.)
SOURCE_LOG_TYPE = {
    "ebay_sold": "historical_ebay_sold",
    "vcp": "historical",
}


def detect_source_type(columns) -> str | None:
    """Return 'ebay_sold' | 'vcp' | None from a file's column set."""
    cols = {str(c).strip().lower() for c in columns}
    for source, sig in SOURCE_SIGNATURES.items():
        if sig.issubset(cols):
            return source
    return None


def validate_columns(columns, source_type: str) -> list[str]:
    """Return the list of MISSING required columns (empty == valid)."""
    cols = {str(c).strip().lower() for c in columns}
    return sorted(REQUIRED_COLUMNS[source_type] - cols)


def plan_drop_folder(drop_dir: Path) -> list[dict]:
    """Pure discovery+classification (no DB, no side effects). One dict per
    CSV: {path, source_type, missing_columns, error}."""
    plan = []
    for path in sorted(Path(drop_dir).glob("*.csv")):
        try:
            header = pd.read_csv(path, nrows=0).columns
        except Exception as exc:  # unreadable file
            plan.append({"path": path, "source_type": None, "missing_columns": [],
                         "error": f"unreadable: {exc}"})
            continue
        stype = detect_source_type(header)
        if stype is None:
            plan.append({"path": path, "source_type": None, "missing_columns": [],
                         "error": "unrecognized schema (not eBay-sold or VCP)"})
            continue
        missing = validate_columns(header, stype)
        plan.append({"path": path, "source_type": stype, "missing_columns": missing,
                     "error": (f"missing columns: {missing}" if missing else None)})
    return plan


def process_drop_folder(conn, ingest01, *, drop_dir: Path = DROP_DIR,
                        ebay_sold_dir: Path | None = None,
                        vcp_dir: Path | None = None) -> dict:
    """Route validated new drops into 01_ingest's per-source export dirs and
    run its proven ingestion (which hashes, row-dedups, and records
    ingestion_log). Idempotent: a file already ingested (by hash) is skipped.
    Returns a summary. Existing pools are never cleared."""
    ebay_sold_dir = ebay_sold_dir or ingest01.EBAY_SOLD_EXPORTS_DIR
    vcp_dir = vcp_dir or ingest01.HISTORICAL_EXPORTS_DIR
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    REJECTED_DIR.mkdir(parents=True, exist_ok=True)

    plan = plan_drop_folder(drop_dir)
    summary = {"discovered": len(plan), "ingested": [], "skipped": [], "rejected": []}
    routed = {"ebay_sold": False, "vcp": False}

    for item in plan:
        path, stype, err = item["path"], item["source_type"], item["error"]
        if err:
            shutil.copy2(path, REJECTED_DIR / path.name)
            summary["rejected"].append({"file": path.name, "reason": err})
            continue
        file_hash = ingest01.file_sha256(path)
        if ingest01.successful_file_ingested(conn, source_type=SOURCE_LOG_TYPE[stype],
                                             source_filename=path.name, file_hash=file_hash):
            summary["skipped"].append({"file": path.name, "reason": "already ingested (hash match)"})
            continue
        target_dir = ebay_sold_dir if stype == "ebay_sold" else vcp_dir
        target_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target_dir / path.name)
        routed[stype] = True
        summary["ingested"].append({"file": path.name, "source_type": stype, "file_hash": file_hash})

    # Delegate to the proven per-source ingestion for any source that got a new file.
    if routed["ebay_sold"]:
        ingest01.insert_historical_ebay_sold_exports(conn)
    if routed["vcp"]:
        ingest01.insert_historical_exports(conn)

    # Archive processed drop files (only those we ingested).
    for entry in summary["ingested"]:
        src = Path(drop_dir) / entry["file"]
        if src.exists():
            shutil.move(str(src), str(PROCESSED_DIR / entry["file"]))
    return summary


def main() -> None:
    ap = argparse.ArgumentParser(description="Historical drop-folder ingestion.")
    ap.add_argument("--db", default=None, help="Target DuckDB (overrides WATCHPARTS_DB / default).")
    ap.add_argument("--run-clean", action="store_true", help="Run 02_clean.py after ingestion.")
    args = ap.parse_args()
    if args.db:
        os.environ["WATCHPARTS_DB"] = args.db

    ingest01 = _load_ingest01()
    DROP_DIR.mkdir(parents=True, exist_ok=True)
    conn = ingest01.get_connection()
    try:
        print("=" * 60)
        print(f"WATCHPARTS — HISTORICAL DROP INGEST  ({datetime.now(timezone.utc).isoformat()})")
        print(f"Drop folder: {DROP_DIR}")
        print("=" * 60)
        summary = process_drop_folder(conn, ingest01)
    finally:
        conn.close()

    print(f"Discovered: {summary['discovered']}")
    print(f"Ingested:   {len(summary['ingested'])}  {[e['file'] for e in summary['ingested']]}")
    print(f"Skipped:    {len(summary['skipped'])}  {[e['file'] for e in summary['skipped']]}")
    print(f"Rejected:   {len(summary['rejected'])}  {summary['rejected']}")

    if args.run_clean and summary["ingested"]:
        print("Running 02_clean.py ...")
        subprocess.run([sys.executable, str(SCRIPTS_DIR / "02_clean.py")],
                       env={**os.environ}, check=True)


if __name__ == "__main__":
    main()
