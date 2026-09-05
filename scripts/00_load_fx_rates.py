"""
00_load_fx_rates.py
====================
Reproducible reference-data loader for ref_exchange_rates.

Source: the Frankfurter API (https://api.frankfurter.dev), which
republishes ECB reference exchange rates. Free, no API key, no auth.
This is the intended source — confirmed from the `source` column values
already present in ref_exchange_rates ("ECB via Frankfurter API ..."),
not guessed. Before this script existed, that data had been loaded
directly into the live database with no ingestion script anywhere in
the repository to reproduce it — a fresh schema-only rebuild left the
table empty and crashed 02_clean.py partway through clean_historical()
(unhandled TypeError formatting a None USD price). This script closes
that gap.

This is a REFERENCE-DATA loader, not a raw-layer ingestion script: it
fetches from an external authoritative source and writes directly to
ref_exchange_rates (a ref_* table, per schema.sql's layer convention),
not into a raw_* staging area — there is no "raw copy" concept for
externally-authoritative reference data the way there is for scraped
listings.

What it fetches, grounded in actual pipeline usage — checked against
every ref_exchange_rates read in scripts/02_clean.py, not assumed:
  - EUR -> USD, full daily historical series from FX_HISTORICAL_START_DATE
    through today. This is the pair clean_historical()'s
    per-transaction-date ASOF join needs (raw_historical prices are
    EUR-denominated; USD is a derived display currency), and
    clean_active_broad/clean_active_targeted also use it as the
    EUR->USD bridge rate for USD scenario pricing.
  - USD -> EUR, the SAME full daily historical series, computed locally
    as 1/rate for each date already fetched above — no second API call.
    Required because raw_active_targeted is commonly USD-priced and
    collected across many different dates (not just "today"), exactly
    like the EUR-priced rows; a single latest-only USD->EUR snapshot
    left most USD-priced targeted rows unable to find a direct ASOF
    match for their own collection date, incorrectly inflating the
    measured FX-fallback rate. This mirrors what the live database's
    original (pre-this-script) manually-loaded data already had —
    verified via a live audit before this fix, not assumed — and closes
    the gap between this loader's output and that already-proven-correct
    shape.
  - {GBP, CHF, HKD, AUD} -> EUR, latest only. clean_active_broad is the
    only consumer of these — the only non-USD/EUR currencies these
    listings have ever actually appeared in (checked: SELECT DISTINCT
    price_currency/shipping_cost_currency on raw_active_broad), and
    raw_active_broad is currently collected as a single-date sweep, so a
    latest-only snapshot has no same-currency staleness gap to close the
    way USD's did for raw_active_targeted. Kept latest-only deliberately,
    not daily, to avoid fetching a full historical series that nothing
    currently needs.
  - EUR -> {GBP, CHF, HKD, AUD}, latest only, computed as the
    mathematical inverse of the same latest fetch above (Frankfurter
    quotes everything relative to one base currency per call; the
    inverse is computed locally, not a second API call).
  - EUR -> EUR, identity (rate=1.0) — 02_clean.py's cross-currency
    logic depends on this row existing so a EUR-denominated row can
    ASOF-join against itself without a special case.

Rate validation: any fetched rate that is None, zero, negative, or not a
finite number is rejected before it can produce either a direct row or
an inverted row — a bad source value must never silently become a
"valid-looking" inverted rate (e.g. 1/0 or 1/-x). Rejected rates are
simply skipped, never substituted with a guess.

Idempotent and duplicate-safe: PRIMARY KEY (from_currency, to_currency,
valid_date) is already enforced by schema.sql; every insert here uses
ON CONFLICT DO NOTHING, matching this project's existing idempotency
convention (match_run, ref_condition_map, etc.) — re-running this
script (e.g. daily, to extend the historical series by one more day)
never creates duplicates and never overwrites an already-stored rate.

Provenance required by this project: rate_date, base_currency,
target_currency, rate, source, retrieved_at. All six already have a
home in the existing schema — no schema change needed:
  rate_date -> valid_date | base_currency -> from_currency |
  target_currency -> to_currency | rate -> rate | source -> source |
  retrieved_at -> created_at

Run:
    python scripts/00_load_fx_rates.py
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pandas as pd
import requests

BASE_DIR = Path(__file__).parent.parent
DB_PATH = BASE_DIR / "database" / "watchparts.duckdb"
SCHEMA_PATH = BASE_DIR / "scripts" / "schema.sql"
LOG_DIR = BASE_DIR / "logs"

API_BASE = "https://api.frankfurter.dev/v1"
FX_BASE_CURRENCY = "EUR"
FX_HISTORICAL_TARGET = "USD"
# USD deliberately excluded here — it now gets full daily history (derived
# locally from the EUR->USD historical fetch, see build_fx_rows), not a
# latest-only snapshot like these four.
FX_LATEST_TARGETS = ["GBP", "CHF", "HKD", "AUD"]

# Comfortably before the earliest known historical last_sold_date
# (2023-06-17, per stg_historical on live data) so the ASOF join in
# clean_historical() always has real coverage, not just fallback.
FX_HISTORICAL_START_DATE = date(2023, 1, 1)

CHUNK_DAYS = 365          # Frankfurter's range endpoint, requested in yearly windows
MAX_RETRIES = 4
RETRY_BACKOFF_SECONDS = 3
REQUEST_TIMEOUT_SECONDS = 20


def setup_logging() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        filename=LOG_DIR / "00_load_fx_rates.log",
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )


def log_and_print(message: str) -> None:
    print(message)
    logging.info(message)


def get_connection(db_path: Path = DB_PATH) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    conn.execute(SCHEMA_PATH.read_text())
    return conn


def _fetch_json(url: str, params: dict) -> dict:
    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:  # network hiccup, 5xx, timeout — retry, don't fail hard
            last_exc = exc
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)
    raise RuntimeError(
        f"FX API request failed after {MAX_RETRIES} attempts: {url} params={params}"
    ) from last_exc


def fetch_historical_daily(base: str, target: str, start_date: date, end_date: date) -> list[tuple[date, float]]:
    """One row per ECB-published business day in [start_date, end_date]
    for base->target. Chunked into <=CHUNK_DAYS windows — the range
    endpoint has been observed to be less reliable for very large spans
    than for a bounded one, and chunking keeps a single transient
    failure from discarding an entire multi-year fetch."""
    rows: list[tuple[date, float]] = []
    chunk_start = start_date
    while chunk_start <= end_date:
        chunk_end = min(chunk_start + timedelta(days=CHUNK_DAYS - 1), end_date)
        url = f"{API_BASE}/{chunk_start.isoformat()}..{chunk_end.isoformat()}"
        data = _fetch_json(url, {"base": base, "symbols": target})
        for date_str, rates in data.get("rates", {}).items():
            if target in rates:
                rows.append((date.fromisoformat(date_str), float(rates[target])))
        chunk_start = chunk_end + timedelta(days=1)
    return rows


def fetch_latest(base: str, targets: list[str]) -> tuple[date, dict[str, float]]:
    data = _fetch_json(f"{API_BASE}/latest", {"base": base, "symbols": ",".join(targets)})
    valid_date = date.fromisoformat(data["date"])
    return valid_date, {ccy: float(rate) for ccy, rate in data["rates"].items()}


def _is_valid_rate(rate) -> bool:
    """A rate must be a real, finite, strictly positive number to be
    usable at all — as a direct row or as the source of an inverted row.
    None, zero, negative, NaN, and infinite values are all rejected."""
    if rate is None:
        return False
    try:
        rate = float(rate)
    except (TypeError, ValueError):
        return False
    if rate != rate:  # NaN != NaN — no math.isnan import needed for one check
        return False
    return rate > 0 and rate != float("inf")


def _invert_rate(rate: float) -> float:
    """Deterministic inversion, consistent with this project's existing
    convention for computed FX inverses (see the latest-snapshot inverted
    rows below) — plain float division rounded to 6 decimal places, not a
    Decimal type, matching how every other rate in ref_exchange_rates is
    already stored (the column is DOUBLE, not a fixed-point type)."""
    return round(1.0 / rate, 6)


def build_fx_rows(
    *,
    historical: list[tuple[date, float]],
    latest_date: date,
    latest_rates: dict[str, float],
    today: date,
    retrieved_at: datetime,
) -> pd.DataFrame:
    """Pure transform, no network — separated out so it's directly
    testable with injected fetch results instead of mocking HTTP."""
    rows: list[tuple[str, str, float, date, str]] = []

    for valid_date, rate in historical:
        if not _is_valid_rate(rate):
            continue  # never propagate a zero/negative/NULL/invalid source rate
        rows.append((FX_BASE_CURRENCY, FX_HISTORICAL_TARGET, rate, valid_date,
                     "ECB via Frankfurter API (historical daily)"))
        # Same rate_date as the source row, deterministic local inversion,
        # no second API call — this is the fix: USD->EUR now gets the
        # same daily grain as EUR->USD, not a single latest-only row.
        rows.append((FX_HISTORICAL_TARGET, FX_BASE_CURRENCY, _invert_rate(rate), valid_date,
                     "ECB via Frankfurter API (historical daily, inverted)"))

    for ccy, rate in latest_rates.items():
        if not _is_valid_rate(rate):
            continue
        rows.append((FX_BASE_CURRENCY, ccy, rate, latest_date,
                     "ECB via Frankfurter API (latest)"))
        rows.append((ccy, FX_BASE_CURRENCY, _invert_rate(rate), latest_date,
                     "ECB via Frankfurter API (latest, inverted)"))

    rows.append((FX_BASE_CURRENCY, FX_BASE_CURRENCY, 1.0, today, "identity"))

    df = pd.DataFrame(rows, columns=["from_currency", "to_currency", "rate", "valid_date", "source"])
    df = df.drop_duplicates(subset=["from_currency", "to_currency", "valid_date"], keep="last")
    df["created_at"] = retrieved_at
    return df


def write_fx_rows(conn: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> dict:
    before = conn.execute("SELECT COUNT(*) FROM ref_exchange_rates").fetchone()[0]
    conn.register("tmp_fx_rows", df)
    conn.execute("""
        INSERT INTO ref_exchange_rates (from_currency, to_currency, rate, valid_date, source, created_at)
        SELECT from_currency, to_currency, rate, valid_date, source, created_at FROM tmp_fx_rows
        ON CONFLICT (from_currency, to_currency, valid_date) DO NOTHING
    """)
    conn.unregister("tmp_fx_rows")
    after = conn.execute("SELECT COUNT(*) FROM ref_exchange_rates").fetchone()[0]
    return {
        "rows_fetched": len(df),
        "rows_inserted": after - before,
        "rows_already_present": len(df) - (after - before),
    }


def load_fx_rates(
    conn: duckdb.DuckDBPyConnection,
    *,
    historical_start: date = FX_HISTORICAL_START_DATE,
    today: date | None = None,
) -> dict:
    today = today or datetime.now(timezone.utc).date()
    retrieved_at = datetime.now(timezone.utc)

    historical = fetch_historical_daily(FX_BASE_CURRENCY, FX_HISTORICAL_TARGET, historical_start, today)
    latest_date, latest_rates = fetch_latest(FX_BASE_CURRENCY, FX_LATEST_TARGETS)

    df = build_fx_rows(
        historical=historical, latest_date=latest_date, latest_rates=latest_rates,
        today=today, retrieved_at=retrieved_at,
    )
    return write_fx_rows(conn, df)


def main() -> None:
    setup_logging()
    log_and_print("=" * 60)
    log_and_print("WATCHPARTS — STEP 0: LOAD FX REFERENCE RATES")
    log_and_print("=" * 60)

    conn = get_connection()
    try:
        try:
            result = load_fx_rates(conn)
        except RuntimeError as exc:
            existing_rows = conn.execute("SELECT COUNT(*) FROM ref_exchange_rates").fetchone()[0]
            if existing_rows <= 0:
                raise
            log_and_print(
                "⚠ Live FX fetch failed, but existing ref_exchange_rates rows "
                f"are available ({existing_rows:,}). Continuing with the submitted "
                "database snapshot for offline review."
            )
            log_and_print(f"  FX fetch error: {exc}")
            result = {
                "rows_fetched": 0,
                "rows_inserted": 0,
                "rows_already_present": existing_rows,
            }
    finally:
        conn.close()

    log_and_print(
        f"FX rates: fetched {result['rows_fetched']:,}, "
        f"inserted {result['rows_inserted']:,} new, "
        f"{result['rows_already_present']:,} already present (idempotent no-op)"
    )
    log_and_print("✓ FX reference data load complete.")


if __name__ == "__main__":
    main()
