"""
25_dashboard_pipeline_jobs.py
=============================

Processes jobs created by the dashboard inventory form.

This is deliberately a worker, not a forever database watcher. DuckDB is a
single-file analytical database; explicit queued jobs are easier to audit,
retry, and test than polling the database continuously from Streamlit.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import duckdb

BASE_DIR = Path(__file__).parent.parent
DEFAULT_DB_PATH = BASE_DIR / "database" / "watchparts.duckdb"
SCHEMA_PATH = BASE_DIR / "scripts" / "schema.sql"


def connect(db_path: Path, *, retries: int = 20, delay_seconds: float = 0.5) -> duckdb.DuckDBPyConnection:
    """Open DuckDB with a short retry window.

    Streamlit may still hold a read-only connection for a moment after the
    user clicks Add Item. DuckDB is single-writer, so the worker should wait
    briefly instead of failing a client job with a transient lock error.
    """
    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        try:
            conn = duckdb.connect(str(db_path))
            conn.execute(SCHEMA_PATH.read_text())
            return conn
        except duckdb.IOException as exc:
            last_exc = exc
            if "lock" not in str(exc).lower() or attempt == retries:
                raise
            time.sleep(delay_seconds)
    raise last_exc or RuntimeError(f"Could not open database {db_path}")


def _next_event_id(conn: duckdb.DuckDBPyConnection) -> int:
    return conn.execute(
        "SELECT COALESCE(MAX(event_id), 0) + 1 FROM dashboard_pipeline_job_events"
    ).fetchone()[0]


def add_event(conn: duckdb.DuckDBPyConnection, job_id: str, event_type: str, message: str) -> None:
    conn.execute(
        """
        INSERT INTO dashboard_pipeline_job_events (event_id, job_id, event_type, message)
        VALUES (?, ?, ?, ?)
        """,
        [_next_event_id(conn), job_id, event_type, message],
    )


def record_event(db_path: Path, job_id: str, event_type: str, message: str) -> None:
    conn = connect(db_path)
    try:
        add_event(conn, job_id, event_type, message)
    finally:
        conn.close()


def claim_next_job(conn: duckdb.DuckDBPyConnection, job_id: str | None = None) -> dict | None:
    if job_id:
        row = conn.execute(
            """
            SELECT * FROM dashboard_pipeline_jobs
            WHERE job_id = ? AND status IN ('QUEUED', 'FAILED')
            """,
            [job_id],
        ).fetchone()
    else:
        row = conn.execute(
            """
            SELECT * FROM dashboard_pipeline_jobs
            WHERE status = 'QUEUED'
            ORDER BY requested_at
            LIMIT 1
            """
        ).fetchone()
    if row is None:
        return None
    cols = [d[0] for d in conn.description]
    job = dict(zip(cols, row))
    conn.execute(
        """
        UPDATE dashboard_pipeline_jobs
        SET status='RUNNING', started_at=current_timestamp, error_message=NULL
        WHERE job_id=?
        """,
        [job["job_id"]],
    )
    add_event(conn, job["job_id"], "RUNNING", "Job started.")
    return job


def _run_step(name: str, cmd: list[str], db_path: Path) -> dict:
    env = os.environ.copy()
    env["WATCHPARTS_DB"] = str(db_path)
    t0 = time.perf_counter()
    proc = subprocess.run(
        cmd,
        cwd=BASE_DIR,
        env=env,
        text=True,
        capture_output=True,
    )
    seconds = round(time.perf_counter() - t0, 2)
    return {
        "step": name,
        "seconds": seconds,
        "returncode": proc.returncode,
        "stdout_tail": "\n".join(proc.stdout.splitlines()[-30:]),
        "stderr_tail": "\n".join(proc.stderr.splitlines()[-20:]),
    }


def _run_job_step(job_id: str, name: str, cmd: list[str], db_path: Path) -> dict:
    record_event(db_path, job_id, f"STEP_START:{name}", "Started.")
    result = _run_step(name, cmd, db_path)
    event_type = f"STEP_OK:{name}" if result["returncode"] == 0 else f"STEP_FAILED:{name}"
    record_event(db_path, job_id, event_type, f"{result['seconds']}s rc={result['returncode']}")
    return result


def _inventory_uid(conn: duckdb.DuckDBPyConnection, canonical_inventory_id: str) -> str | None:
    row = conn.execute(
        """
        SELECT inventory_uid
        FROM staging_inventory
        WHERE canonical_inventory_id = ? AND validation_status <> 'FAIL'
        ORDER BY inventory_uid
        LIMIT 1
        """,
        [canonical_inventory_id],
    ).fetchone()
    return row[0] if row else None


def process_job(
    db_path: Path,
    *,
    job_id: str | None = None,
    dry_run_collection: bool = False,
    skip_collection: bool = False,
) -> dict | None:
    conn = connect(db_path)
    try:
        job = claim_next_job(conn, job_id=job_id)
    finally:
        conn.close()
    if job is None:
        return None

    steps: list[tuple[str, list[str]]] = [
        ("01_ingest", [sys.executable, "scripts/01_ingest.py", "--db", str(db_path), "--inventory-only"]),
        ("02_clean", [sys.executable, "scripts/02_clean.py", "--db", str(db_path)]),
        ("03_generate_queries", [sys.executable, "scripts/03_generate_queries.py", "--db", str(db_path)]),
    ]

    results = []
    try:
        for name, cmd in steps:
            result = _run_job_step(job["job_id"], name, cmd, db_path)
            results.append(result)
            if result["returncode"] != 0:
                raise RuntimeError(f"{name} failed: {result['stderr_tail'] or result['stdout_tail']}")

        conn = connect(db_path)
        try:
            uid = _inventory_uid(conn, job["canonical_inventory_id"])
            conn.execute(
                "UPDATE dashboard_pipeline_jobs SET inventory_uid=? WHERE job_id=?",
                [uid, job["job_id"]],
            )
            add_event(conn, job["job_id"], "INVENTORY_UID_RESOLVED", uid or "No eligible inventory UID found.")
        finally:
            conn.close()

        if job["job_type"] == "NEW_ITEM" and uid and not skip_collection:
            collect_cmd = [
                sys.executable, "scripts/04_collect_targeted_active.py",
                "--db", str(db_path), "--inventory-uid", uid,
            ]
            if dry_run_collection:
                collect_cmd.append("--dry-run")
            result = _run_job_step(job["job_id"], "04_collect_targeted_active", collect_cmd, db_path)
            results.append(result)
            if result["returncode"] != 0:
                raise RuntimeError(f"04_collect_targeted_active failed: {result['stderr_tail'] or result['stdout_tail']}")

            # 04_collect_targeted_active writes CSV and triggers raw
            # ingestion. Matching reads stg_active_targeted, so clean again
            # after collection; otherwise a dashboard job can fetch eBay rows
            # successfully but still match against zero staged active evidence.
            result = _run_job_step(
                job["job_id"],
                "02_clean_after_collection",
                [sys.executable, "scripts/02_clean.py", "--db", str(db_path)],
                db_path,
            )
            results.append(result)
            if result["returncode"] != 0:
                raise RuntimeError(f"02_clean_after_collection failed: {result['stderr_tail'] or result['stdout_tail']}")

        if job["job_type"] == "NEW_ITEM":
            for name, cmd in [
                ("05_generate_match_candidates", [
                    sys.executable, "scripts/05_generate_match_candidates.py",
                    "--db", str(db_path), "--inventory-uid", uid,
                ]),
                ("06_decide_matches", [
                    sys.executable, "scripts/06_decide_matches.py",
                    "--db", str(db_path), "--inventory-uid", uid,
                ]),
                ("21_evidence_confidence", [
                    sys.executable, "scripts/21_evidence_confidence_engine.py",
                    "--db", str(db_path), "--run-id", job["job_id"], "--inventory-uid", uid,
                ]),
                ("22_build_confidence_tmv", [sys.executable, "scripts/22_build_confidence_tmv.py", "--db", str(db_path)]),
            ]:
                result = _run_job_step(job["job_id"], name, cmd, db_path)
                results.append(result)
                if result["returncode"] != 0:
                    raise RuntimeError(f"{name} failed: {result['stderr_tail'] or result['stdout_tail']}")

        result = _run_job_step(
            job["job_id"],
            "23_build_dashboard_contract",
            [sys.executable, "scripts/23_build_dashboard_contract.py", "--db", str(db_path)],
            db_path,
        )
        results.append(result)
        if result["returncode"] != 0:
            raise RuntimeError(f"23_build_dashboard_contract failed: {result['stderr_tail'] or result['stdout_tail']}")

        conn = connect(db_path)
        try:
            price_row = conn.execute(
                """
                SELECT pricing_status, pricing_confidence, recommended_price_eur,
                       turnover_confidence, sell_time_display, no_recommendation_reason
                FROM dashboard_inventory_pricing
                WHERE canonical_inventory_id = ?
                """,
                [job["canonical_inventory_id"]],
            ).fetchone()
            summary = {
                "canonical_inventory_id": job["canonical_inventory_id"],
                "dashboard_row": list(price_row) if price_row else None,
            }
            conn.execute(
                """
                UPDATE dashboard_pipeline_jobs
                SET status='SUCCEEDED', finished_at=current_timestamp,
                    step_timings_json=?, result_summary=?
                WHERE job_id=?
                """,
                [json.dumps(results), json.dumps(summary), job["job_id"]],
            )
            add_event(conn, job["job_id"], "SUCCEEDED", json.dumps(summary))
        finally:
            conn.close()

        return {"job_id": job["job_id"], "status": "SUCCEEDED", "steps": results}
    except Exception as exc:
        conn = connect(db_path)
        try:
            conn.execute(
                """
                UPDATE dashboard_pipeline_jobs
                SET status='FAILED', finished_at=current_timestamp,
                    step_timings_json=?, error_message=?
                WHERE job_id=?
                """,
                [json.dumps(results), str(exc), job["job_id"]],
            )
            add_event(conn, job["job_id"], "FAILED", str(exc))
        finally:
            conn.close()
        raise


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=str(os.environ.get("WATCHPARTS_DB", DEFAULT_DB_PATH)))
    parser.add_argument("--job-id", default=None)
    parser.add_argument("--all", action="store_true", help="Process queued jobs until none remain.")
    parser.add_argument("--dry-run-collection", action="store_true")
    parser.add_argument("--skip-collection", action="store_true")
    args = parser.parse_args()

    processed = 0
    while True:
        result = process_job(
            Path(args.db),
            job_id=args.job_id,
            dry_run_collection=args.dry_run_collection,
            skip_collection=args.skip_collection,
        )
        if result is None:
            if processed == 0:
                print("No queued dashboard pipeline jobs.")
            return
        processed += 1
        print(f"Processed job {result['job_id']}: {result['status']}")
        for step in result["steps"]:
            print(f"  {step['step']}: {step['seconds']}s rc={step['returncode']}")
        if args.job_id or not args.all:
            return


if __name__ == "__main__":
    main()
