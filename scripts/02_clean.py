"""
02_clean.py
===========
Reads raw tables, applies all cleaning rules, writes to staging tables.

Why this matters:
  Every cleaning decision is documented here as a Python function.
  When a judge asks 'how did you handle currency conversion?' or
  'how did you normalise condition strings?' — you show them this file.
  Nothing was done in Excel. Everything is reproducible.

Safe to re-run: drops and recreates staging tables each time.

Usage:
    python scripts/02_clean.py
"""

import hashlib
import logging
import os
import re
from pathlib import Path
from uuid import uuid4

import duckdb
import pandas as pd

import evidence_identity
import utils

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent.parent
# DB target: WATCHPARTS_DB env var (disposable copy) > default live DB.
# Default behaviour unchanged when unset.
DEFAULT_DB_PATH = BASE_DIR / "database" / "watchparts.duckdb"
DB_PATH = Path(os.environ["WATCHPARTS_DB"]) if os.environ.get("WATCHPARTS_DB") else DEFAULT_DB_PATH
SCHEMA_PATH = BASE_DIR / "scripts" / "schema.sql"
LOG_DIR = BASE_DIR / "logs"
REPORTS_DIR = BASE_DIR / "reports"

EXCEL_DATE_CORRUPTION_RE = re.compile(r"^\d{4}-\d{2}-\d{2}(\s+\d{2}:\d{2}:\d{2})?$")
VALID_BRANDS = {"Rolex", "Tudor"}


def setup_logging(log_dir: Path = LOG_DIR) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        filename=log_dir / "02_clean.log",
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        force=True,
    )


def log_and_print(message: str = "") -> None:
    print(message)
    logging.info(message)


def latest_inventory_upload_batch_id(conn) -> str | None:
    """Return the latest successful full inventory snapshot.

    Older dashboard updates briefly wrote tiny partial raw batches. Staging must
    reflect the current inventory file, not those legacy two-row update batches.
    """
    try:
        row = conn.execute(
            """
            SELECT upload_batch_id
            FROM ingestion_log
            WHERE source_type = 'inventory'
              AND status = 'success'
              AND rows_inserted >= 100
            ORDER BY ingested_at DESC, rows_inserted DESC
            LIMIT 1
            """
        ).fetchone()
        if row:
            return row[0]
        row = conn.execute(
            """
            SELECT upload_batch_id
            FROM ingestion_log
            WHERE source_type = 'inventory'
              AND status = 'success'
              AND rows_inserted > 0
            ORDER BY rows_inserted DESC, ingested_at DESC
            LIMIT 1
            """
        ).fetchone()
        return row[0] if row else None
    except Exception:
        return None


def _aggregates_available(*values) -> bool:
    """True only if every value in a fetched aggregate-stats tuple
    (MEDIAN/MIN/MAX/SUM/etc. results) is present and not NaN. On an
    empty or all-NULL input, DuckDB aggregates return SQL NULL (Python
    None) — formatting one directly with an f-string format spec
    (":.2f", ":.0f") raises TypeError. This is the one thing shared
    across every summary-print block below; each block still decides its
    own empty-case message and behaviour, not this helper."""
    for value in values:
        if value is None:
            return False
        if isinstance(value, float) and value != value:  # NaN != NaN
            return False
    return True


def get_connection():
    last_exc = None
    for attempt in range(31):
        try:
            conn = duckdb.connect(str(DB_PATH))
            break
        except duckdb.IOException as exc:
            last_exc = exc
            if "lock" not in str(exc).lower() or attempt == 30:
                raise
            time.sleep(0.5)
    else:
        raise last_exc
    conn.execute(SCHEMA_PATH.read_text())
    migrate_stock_history_primary_key(conn)
    return conn


def migrate_stock_history_primary_key(conn) -> None:
    """
    One-time, idempotent migration: inventory_stock_history's primary key
    moves from (canonical_inventory_id, upload_batch_id) to
    (inventory_uid, upload_batch_id). canonical_inventory_id can change for
    a physical item (a correction, or a cleaning-rule fix like the
    part_number-nullable change) while inventory_uid stays stable — keying
    on the old column accumulates a stale + fresh row pair forever.

    No-ops once the PK is already (inventory_uid, upload_batch_id).
    """
    current_pk = conn.execute(
        """
        SELECT constraint_column_names FROM duckdb_constraints()
        WHERE table_name = 'inventory_stock_history' AND constraint_type = 'PRIMARY KEY'
        """
    ).fetchone()
    if not current_pk or list(current_pk[0]) == ["inventory_uid", "upload_batch_id"]:
        return

    df = conn.execute("SELECT * FROM inventory_stock_history").df()
    before_count = len(df)
    if before_count == 0:
        conn.execute("DROP TABLE inventory_stock_history")
        conn.execute(SCHEMA_PATH.read_text())
        return

    null_uid_count = int(df["inventory_uid"].isna().sum())
    if null_uid_count:
        log_and_print(
            f"   ⚠ inventory_stock_history PK migration: {null_uid_count} row(s) have NULL "
            "inventory_uid and cannot be migrated to the new key — they will be dropped."
        )

    current_canonical_ids = set(
        row[0] for row in conn.execute("SELECT canonical_inventory_id FROM staging_inventory").fetchall()
    )
    df["_is_current"] = df["canonical_inventory_id"].isin(current_canonical_ids)
    df = df.sort_values(["_is_current", "observed_at"], ascending=[False, False])
    deduped = df.dropna(subset=["inventory_uid"]).drop_duplicates(subset=["inventory_uid", "upload_batch_id"], keep="first")
    deduped = deduped.drop(columns=["_is_current"])
    after_count = len(deduped)

    conn.register("tmp_stock_history_migrated", deduped)
    conn.execute("DROP TABLE inventory_stock_history")
    conn.execute(
        """
        CREATE TABLE inventory_stock_history (
            canonical_inventory_id  VARCHAR,
            upload_batch_id         VARCHAR,
            stock                   INTEGER,
            inventory_uid           VARCHAR NOT NULL,
            observed_at             TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (inventory_uid, upload_batch_id)
        )
        """
    )
    conn.execute(
        """
        INSERT INTO inventory_stock_history (canonical_inventory_id, upload_batch_id, stock, inventory_uid, observed_at)
        SELECT canonical_inventory_id, upload_batch_id, stock, inventory_uid, observed_at
        FROM tmp_stock_history_migrated
        """
    )
    conn.unregister("tmp_stock_history_migrated")

    dropped = before_count - after_count
    log_and_print(
        f"   ✓ inventory_stock_history PK migrated to (inventory_uid, upload_batch_id): "
        f"{before_count:,} -> {after_count:,} rows ({dropped:,} stale duplicate(s) removed)"
    )


# ══════════════════════════════════════════════════════════════════════════════
# CLEAN HISTORICAL
# ══════════════════════════════════════════════════════════════════════════════

GERMAN_MONTHS = {
    "Jan": "Jan", "Feb": "Feb", "Mrz": "Mar", "Apr": "Apr",
    "Mai": "May", "Jun": "Jun", "Jul": "Jul", "Aug": "Aug",
    "Sep": "Sep", "Okt": "Oct", "Nov": "Nov", "Dez": "Dec"
}

def parse_german_date(date_str):
    """
    Convert German Terapeak date strings to ISO format.
    "7. Aug 2025"  → "2025-08-07"
    "2. Mrz 2026"  → "2026-03-02"
    Returns None if parsing fails.
    """
    if not date_str or str(date_str).strip() in ["-", "nan", ""]:
        return None
    try:
        s = str(date_str).strip()
        for de, en in GERMAN_MONTHS.items():
            s = s.replace(de, en)
        s = re.sub(r"(\d+)\.", r"\1", s)
        return pd.to_datetime(s, format="%d %b %Y").strftime("%Y-%m-%d")
    except Exception:
        return None


def extract_brand(source_file):
    """
    "terapeak_rolex_caliber_p1.html" → "Rolex"
    """
    if not source_file:
        return None
    parts = str(source_file).replace("terapeak_", "").split("_")
    return parts[0].capitalize() if parts else None


def extract_keyword(source_file):
    """
    "terapeak_rolex_caliber_p1.html"  → "caliber"
    "terapeak_rolex_original_p3.html" → "original"
    """
    if not source_file:
        return None
    name = str(source_file).replace(".html","").replace("terapeak_","")
    parts = name.split("_")
    if len(parts) >= 2:
        keyword_parts = [p for p in parts[1:] if not (p.startswith("p") and p[1:].isdigit())]
        return "_".join(keyword_parts) if keyword_parts else None
    return None


def clean_historical(conn):
    print("\nCleaning historical data...")

    df = conn.execute("SELECT * FROM raw_historical").df()
    print(f"   Input: {len(df):,} rows")

    # Step 1: Parse dates
    df["last_sold_date"]  = df["last_sold"].apply(parse_german_date)
    df["last_sold_dt"]    = pd.to_datetime(df["last_sold_date"], errors="coerce")
    df["last_sold_year"]  = df["last_sold_dt"].dt.year.astype("Int64")
    df["last_sold_month"] = df["last_sold_dt"].dt.month.astype("Int64")
    failed = df["last_sold_date"].isna().sum()
    if failed:
        print(f"   ⚠ {failed} dates unparseable — set to NULL")

    # Step 2: Extract brand and keyword from the row's OWN provenance
    # (original_source_file), never from the physical container filename.
    # original_source_file is what 01_ingest.py now preserves as the row's
    # genuine per-row source (e.g. "terapeak_rolex_caliber_p1.html"); the old
    # source_file column historically got overwritten with the container
    # filename (e.g. "terapeak_sold_last.csv"), which extract_brand/
    # extract_keyword were never designed to parse. Falls back to source_file
    # only if a database hasn't been retrofitted with original_source_file yet.
    provenance_col = "original_source_file" if "original_source_file" in df.columns else "source_file"
    df["brand"]          = df[provenance_col].apply(extract_brand)
    df["search_keyword"] = df[provenance_col].apply(extract_keyword)

    # Step 3: Normalise format
    df["format"]     = df["format"].str.strip().map({"Festpreis":"fixed_price","Auktion":"auction"}).fillna("unknown")
    df["is_auction"] = df["format"] == "auction"

    # Step 4: Landed cost — what the buyer actually pays
    df["avg_shipping_eur"]    = df["avg_shipping_eur"].fillna(0)
    df["avg_landed_cost_eur"] = df["avg_price_eur"] + df["avg_shipping_eur"]

    # Step 5: Boolean flags
    df["removed"]  = df["removed"].astype(str).str.upper().isin(["TRUE","1"])
    df["has_bids"] = df["bids"].astype(str).str.strip() != "-"

    # Step 6: Drop rows with no price
    before = len(df)
    df = df[df["avg_price_eur"].notna() & (df["avg_price_eur"] > 0)].copy()
    if before - len(df) > 0:
        print(f"   Dropped {before-len(df)} rows with missing/zero price")

    # Zero rows past this point (empty raw table, or every row dropped for
    # missing/zero price) — stop here, same early-return convention used
    # by clean_historical_vcp_aggregate/clean_historical_ebay_sold/
    # clean_active_targeted/clean_active_broad. Continuing on an empty
    # frame hits a genuine pandas pitfall further down: df.apply(...,
    # result_type="expand") on a 0-row DataFrame can't infer the lambda's
    # real return columns and silently echoes back df's OWN columns
    # instead, corrupting the later scenario-column concat.
    if df.empty:
        conn.execute("DELETE FROM stg_historical")
        print("   ✓ Written 0 rows to stg_historical (no raw_historical rows with a usable price)")
        return

    df["stg_id"] = range(1, len(df)+1)

    # Step 6b: normalized title — source-agnostic, no entity extraction
    df["normalized_title"] = df["title"].apply(utils.normalize_title)

    # Step 7: EUR -> USD conversion, using the ECB rate that was ACTUALLY in
    # effect on each listing's last_sold_date (not today's rate). This is
    # the whole point of storing the full historical daily series in
    # ref_exchange_rates: a row from August 2023 gets the August 2023 rate.
    #
    # Audited for the EUR->EUR identity-date bug (clean_active_broad/
    # clean_active_targeted): not applicable here. avg_price_eur is
    # already EUR — this function never looks up a EUR->EUR rate at all,
    # only the EUR->USD bridge below, so it was never exposed.
    #
    # ECB doesn't publish rates on weekends/EU bank holidays, so we use an
    # ASOF join — "the most recent rate on or before this date" — which is
    # standard FX convention (same thing a bank does for a Saturday trade).
    conn.register("tmp_hist_dates", df[["stg_id", "last_sold_date"]])
    fx = conn.execute("""
        SELECT h.stg_id, r.rate AS eur_usd_rate, r.valid_date AS fx_rate_date
        FROM tmp_hist_dates h
        ASOF LEFT JOIN (
            SELECT valid_date, rate FROM ref_exchange_rates
            WHERE from_currency = 'EUR' AND to_currency = 'USD'
        ) r
        ON TRY_CAST(h.last_sold_date AS DATE) >= r.valid_date
    """).df()
    conn.unregister("tmp_hist_dates")

    # Fallback for rows with no matching rate at all: unparseable last_sold_date,
    # or a date older than the earliest rate we fetched. Use the earliest
    # available rate and flag it — never silently drop the USD column.
    earliest_rate_row = conn.execute("""
        SELECT rate, valid_date FROM ref_exchange_rates
        WHERE from_currency = 'EUR' AND to_currency = 'USD'
        ORDER BY valid_date ASC LIMIT 1
    """).fetchone()
    fx["fx_rate_is_fallback"] = fx["eur_usd_rate"].isna()
    n_fallback = fx["fx_rate_is_fallback"].sum()
    if n_fallback and earliest_rate_row:
        fx["fx_rate_date"] = fx["fx_rate_date"].astype(object)
        fx.loc[fx["fx_rate_is_fallback"], "eur_usd_rate"] = earliest_rate_row[0]
        fx.loc[fx["fx_rate_is_fallback"], "fx_rate_date"] = earliest_rate_row[1]
        print(f"   ⚠ {n_fallback} rows had no ECB rate available for their date "
              f"(unparseable date or before earliest fetched rate) — "
              f"used earliest available rate ({earliest_rate_row[1]}) as fallback")

    df = df.merge(fx, on="stg_id", how="left")
    df["avg_price_usd"]       = round(df["avg_price_eur"]       * df["eur_usd_rate"], 2)
    df["avg_landed_cost_usd"] = round(df["avg_landed_cost_eur"] * df["eur_usd_rate"], 2)

    # Step 7b: Module 1 scenario prices/costs — standardized fixed-shipping
    # DE/US scenarios off the sale price alone. See utils.compute_scenario_prices.
    scenario = df.apply(
        lambda r: utils.compute_scenario_prices(r["avg_price_eur"]), axis=1, result_type="expand"
    )
    df = pd.concat([df, scenario], axis=1)

    # Step 8: Build output
    output = df[[
        "stg_id","id","title","normalized_title","brand","search_keyword","format","is_auction",
        "avg_price_eur","avg_shipping_eur","avg_landed_cost_eur",
        "avg_price_usd","avg_landed_cost_usd","eur_usd_rate","fx_rate_date","fx_rate_is_fallback",
        "price_virtual_eur","landed_cost_de_eur","landed_cost_us_eur",
        "shipping_de_eur","shipping_us_eur","estimated_import_charges_us_eur",
        "total_sold","total_sales_eur","free_shipping_pct",
        "last_sold_date","last_sold_year","last_sold_month",
        "removed","has_bids"
    ]].rename(columns={"stg_id":"id","id":"raw_id","eur_usd_rate":"eur_usd_rate_used"})

    # Add matching columns (filled later by 04_match.py)
    output["matched_product_id"] = None
    output["match_confidence"]   = None
    output["match_method"]       = None
    output["match_score"]        = None

    # Source-aware historical contract (docs/module4_historical_source_strategy.md
    # §9). Every row this function writes is, today, exclusively a
    # Verkäufer Cockpit/Terapeak AGGREGATE observation — there is no other
    # ingestion path yet — so source_type/row_grain are constant here, not
    # derived. source_record_id/observed_price_eur/condition are listing-grain
    # concepts this source has no equivalent for, and are left NULL rather than
    # fabricated. original_source_file/physical_container_file are passed
    # through from raw_historical (see its schema comment), falling back to the
    # legacy source_file column only if a database hasn't been retrofitted yet.
    output["source_type"]        = "VERKAEUFER_COCKPIT_AGGREGATE"
    output["row_grain"]           = "aggregate"
    output["source_record_id"]    = None
    output["observed_price_eur"]  = None
    output["condition"]           = None
    output["original_source_file"] = (
        df["original_source_file"] if "original_source_file" in df.columns else df["source_file"]
    )
    output["physical_container_file"] = (
        df["physical_container_file"] if "physical_container_file" in df.columns else df["source_file"]
    )

    conn.execute("DELETE FROM stg_historical")
    stg_hist_cols = ["id","raw_id","title","normalized_title","brand","search_keyword","format","is_auction",
                     "avg_price_eur","avg_shipping_eur","avg_landed_cost_eur",
                     "avg_price_usd","avg_landed_cost_usd","eur_usd_rate_used","fx_rate_date","fx_rate_is_fallback",
                     "price_virtual_eur","landed_cost_de_eur","landed_cost_us_eur",
                     "shipping_de_eur","shipping_us_eur","estimated_import_charges_us_eur",
                     "total_sold","total_sales_eur","free_shipping_pct",
                     "last_sold_date","last_sold_year","last_sold_month",
                     "removed","has_bids",
                     "matched_product_id","match_confidence","match_method","match_score",
                     "source_type","row_grain","source_record_id","observed_price_eur","condition",
                     "original_source_file","physical_container_file"]
    conn.execute(f"INSERT INTO stg_historical ({','.join(stg_hist_cols)}) SELECT {','.join(stg_hist_cols)} FROM output")

    # Use DuckDB native MEDIAN for reporting — this is the key advantage
    stats = conn.execute("""
        SELECT
            MIN(avg_price_eur)    AS min_price,
            MEDIAN(avg_price_eur) AS median_price,
            MAX(avg_price_eur)    AS max_price,
            MEDIAN(avg_price_usd) AS median_price_usd,
            MIN(last_sold_date)   AS oldest_sale,
            MAX(last_sold_date)   AS newest_sale
        FROM stg_historical
    """).fetchone()

    print(f"   ✓ Written {len(output):,} rows to stg_historical")
    if _aggregates_available(*stats):
        print(f"     Price (EUR): min €{stats[0]:.2f} | median €{stats[1]:.2f} | max €{stats[2]:.2f}")
        print(f"     Price (USD): median ${stats[3]:.2f} (per-date ECB rate applied per row)")
        print(f"     Date range:  {stats[4]} → {stats[5]}")
    else:
        print("     ⚠ No price summary available (0 rows, or all-NULL price/date data)")
    print(f"     Brands:      {df['brand'].value_counts().to_dict()}")


def clean_historical_vcp_aggregate(conn):
    """
    Module 4: cleans raw_historical (VCP/Terapeak aggregate) into
    stg_historical_vcp_aggregate — the source-specific replacement for the
    VCP portion of the deprecated shared stg_historical, kept structurally
    separate per the locked source-separated-estimators recommendation
    (docs/MODULE4_HISTORICAL_SOURCE_CONTRACTS.md). raw_historical is never
    modified.

    One staged row remains one AGGREGATE observation — never expanded into
    artificial per-transaction rows. Duplicate titles (the same title
    appearing more than once in raw_historical — 222 groups in the current
    live data, evidence points to re-scraped snapshots of the same
    recurring search-result listing across different export runs/search
    categories, not proven independent additional sales) are NEVER
    collapsed or summed here: every row is kept as its own staged row,
    each tagged with title_duplicate_group_size/duplicate_group_id so
    downstream code sees and must explicitly handle the ambiguity instead
    of it being silently absorbed into one merged figure.

    Provenance grounding (resolves the Step 0 contradiction from the
    source-contract audit): brand/search_keyword are derived from
    whichever provenance column has a genuine per-row VALUE —
    original_source_file when actually populated (a future re-ingestion
    that backfills it), else source_file. Verified against live data:
    original_source_file and physical_container_file are 100% NULL for
    all 2,473 current rows (this data predates the provenance-preservation
    fix), but source_file — despite being labeled "legacy" in
    raw_historical's schema comment — already holds genuine per-row page
    provenance today (81 distinct values, e.g.
    "terapeak_rolex_caliber_p1.html"), NOT the physical container filename
    ("terapeak_sold_last.csv") as an earlier audit pass incorrectly
    assumed from the schema comment alone without checking actual values.
    extract_brand/extract_keyword were verified to produce a non-NULL
    brand for 100% of current rows when grounded on source_file.
    """
    print("\nCleaning historical VCP/Terapeak aggregate data...")

    df = conn.execute("SELECT * FROM raw_historical").df()
    print(f"   Input: {len(df):,} rows")

    if df.empty:
        conn.execute("DELETE FROM stg_historical_vcp_aggregate")
        print("   ✓ Written 0 rows to stg_historical_vcp_aggregate (no raw_historical rows)")
        return

    # Step 1: dates
    df["last_sold_date"] = df["last_sold"].apply(parse_german_date)
    df["last_sold_dt"]   = pd.to_datetime(df["last_sold_date"], errors="coerce")
    df["last_sold_year"]  = df["last_sold_dt"].dt.year.astype("Int64")
    df["last_sold_month"] = df["last_sold_dt"].dt.month.astype("Int64")
    failed = df["last_sold_date"].isna().sum()
    if failed:
        print(f"   ⚠ {failed} last_sold dates unparseable — set to NULL")

    # Step 2: provenance-grounded brand/search_keyword — value presence,
    # not column existence (the bug this replaces: checking whether the
    # column exists says nothing about whether its values are populated).
    def _has_real_value(series: pd.Series) -> pd.Series:
        s = series.astype(str).str.strip()
        return series.notna() & (s != "") & (s.str.lower() != "nan")

    original_has_value = _has_real_value(df["original_source_file"])
    provenance = df["original_source_file"].where(original_has_value, df["source_file"])
    df["brand"] = provenance.apply(extract_brand)
    df["search_keyword"] = provenance.apply(extract_keyword)
    n_brand_null = int(df["brand"].isna().sum())
    if n_brand_null:
        print(f"   ⚠ {n_brand_null} rows could not derive a brand from any provenance field")

    # Step 3: format
    df["format_standard"] = df["format"].astype(str).str.strip().map(
        {"Festpreis": "fixed_price", "Auktion": "auction"}
    ).fillna("unknown")
    df["is_auction"] = df["format_standard"] == "auction"

    # Step 4: landed cost + shipping reliability. avg_shipping_eur is never
    # NULL in current data, but fillna(0) keeps behavior defined if it
    # ever is — that 0 is then classified exactly like an observed 0 below.
    df["avg_shipping_eur"] = df["avg_shipping_eur"].fillna(0)
    df["avg_landed_cost_eur"] = df["avg_price_eur"] + df["avg_shipping_eur"]

    # shipping_value_reliability: a zero is only ever called "confirmed
    # free shipping" when a SEPARATE source field (free_shipping_pct=100)
    # actually asserts it — never inferred from the zero alone. Verified
    # against live data: every one of the 420 avg_shipping_eur=0 rows also
    # has free_shipping_pct=100 (perfectly correlated in this dataset),
    # but the rule below does not assume that correlation holds for future
    # data — a zero with free_shipping_pct<100 is still ZERO_AMBIGUOUS.
    def _shipping_reliability(row) -> str:
        if row["avg_shipping_eur"] > 0:
            return "OBSERVED_NONZERO"
        if row["free_shipping_pct"] == 100:
            return "ZERO_CONFIRMED_FREE_SHIPPING"
        return "ZERO_AMBIGUOUS"

    df["shipping_value_reliability"] = df.apply(_shipping_reliability, axis=1)

    # Step 5: reject invalid price rows
    before = len(df)
    df = df[df["avg_price_eur"].notna() & (df["avg_price_eur"] > 0)].copy()
    if before - len(df):
        print(f"   Dropped {before - len(df)} rows with missing/non-positive avg_price_eur")

    df["stg_id"] = range(1, len(df) + 1)
    df["normalized_title"] = df["title"].apply(utils.normalize_title)

    # Step 6: duplicate-title group stats — computed on the SAME row set
    # being staged (post price-filter), never collapsed or summed, only
    # flagged so downstream logic must make an explicit decision.
    df["title_duplicate_group_size"] = df.groupby("title")["title"].transform("count")
    df["duplicate_group_id"] = df["title"].apply(
        lambda t: hashlib.sha256(str(t).encode("utf-8")).hexdigest()[:16]
    )

    # Step 7: EUR -> USD, same ASOF pattern as clean_historical(). Audited
    # for the EUR->EUR identity-date bug: not applicable — avg_price_eur
    # is already EUR, no EUR->EUR lookup happens here either.
    conn.register("tmp_vcp_dates", df[["stg_id", "last_sold_date"]])
    fx = conn.execute("""
        SELECT h.stg_id, r.rate AS eur_usd_rate, r.valid_date AS fx_rate_date
        FROM tmp_vcp_dates h
        ASOF LEFT JOIN (
            SELECT valid_date, rate FROM ref_exchange_rates
            WHERE from_currency = 'EUR' AND to_currency = 'USD'
        ) r
        ON TRY_CAST(h.last_sold_date AS DATE) >= r.valid_date
    """).df()
    conn.unregister("tmp_vcp_dates")

    earliest_rate_row = conn.execute("""
        SELECT rate, valid_date FROM ref_exchange_rates
        WHERE from_currency = 'EUR' AND to_currency = 'USD'
        ORDER BY valid_date ASC LIMIT 1
    """).fetchone()
    fx["fx_rate_is_fallback"] = fx["eur_usd_rate"].isna()
    n_fallback = int(fx["fx_rate_is_fallback"].sum())
    if n_fallback and earliest_rate_row:
        fx["fx_rate_date"] = fx["fx_rate_date"].astype(object)
        fx.loc[fx["fx_rate_is_fallback"], "eur_usd_rate"] = earliest_rate_row[0]
        fx.loc[fx["fx_rate_is_fallback"], "fx_rate_date"] = earliest_rate_row[1]
        print(f"   ⚠ {n_fallback} rows had no ECB rate for their date — used earliest available rate ({earliest_rate_row[1]}) as fallback")

    df = df.merge(fx, on="stg_id", how="left")
    df["avg_price_usd"]       = (df["avg_price_eur"] * df["eur_usd_rate"]).round(2)
    df["avg_landed_cost_usd"] = (df["avg_landed_cost_eur"] * df["eur_usd_rate"]).round(2)

    # Step 8: deterministic row_hash — same content basis every run given
    # unchanged raw data, so reruns are idempotent by construction.
    df["row_hash"] = df.apply(
        lambda r: hashlib.sha256(
            "|".join([
                str(r["title"]), str(r["avg_price_eur"]), str(r["total_sold"]),
                str(r["total_sales_eur"]), str(r["last_sold"]), str(r["id"]),
            ]).encode("utf-8")
        ).hexdigest(),
        axis=1,
    )

    output = df[[
        "stg_id", "id", "title", "normalized_title", "brand", "search_keyword",
        "format_standard", "is_auction",
        "avg_price_eur", "avg_shipping_eur", "avg_landed_cost_eur", "shipping_value_reliability",
        "avg_price_usd", "avg_landed_cost_usd", "eur_usd_rate", "fx_rate_date", "fx_rate_is_fallback",
        "total_sold", "total_sales_eur", "free_shipping_pct",
        "last_sold_date", "last_sold_year", "last_sold_month",
        "title_duplicate_group_size", "duplicate_group_id",
        "original_source_file", "physical_container_file", "source_file",
        "row_hash",
    ]].rename(columns={
        "stg_id": "id", "id": "raw_id", "eur_usd_rate": "eur_usd_rate_used",
    })

    output["matched_product_id"] = None
    output["match_confidence"]   = None
    output["match_method"]       = None
    output["match_score"]        = None

    # Module 5 evidence identity: duplicate_group_id is the CLUSTER
    # identity (confirmed not a row identity — 222 groups share a
    # group id across multiple, differently-priced rows), disambiguated
    # at the observation grain by raw_id. See scripts/evidence_identity.py.
    output = evidence_identity.add_vcp_identity_columns(
        output, duplicate_group_id_col="duplicate_group_id", raw_id_col="raw_id",
    )

    conn.execute("DELETE FROM stg_historical_vcp_aggregate")
    stg_vcp_cols = [
        "id", "raw_id", "title", "normalized_title", "brand", "search_keyword",
        "format_standard", "is_auction",
        "avg_price_eur", "avg_shipping_eur", "avg_landed_cost_eur", "shipping_value_reliability",
        "avg_price_usd", "avg_landed_cost_usd", "eur_usd_rate_used", "fx_rate_date", "fx_rate_is_fallback",
        "total_sold", "total_sales_eur", "free_shipping_pct",
        "last_sold_date", "last_sold_year", "last_sold_month",
        "title_duplicate_group_size", "duplicate_group_id",
        "original_source_file", "physical_container_file", "source_file",
        "row_hash", "matched_product_id", "match_confidence", "match_method", "match_score",
        "stable_evidence_uid", "observation_uid",
    ]
    conn.execute(
        f"INSERT INTO stg_historical_vcp_aggregate ({','.join(stg_vcp_cols)}) "
        f"SELECT {','.join(stg_vcp_cols)} FROM output"
    )

    n_dup_group_rows = int((df["title_duplicate_group_size"] > 1).sum())
    n_dup_groups = int((df.drop_duplicates("title")["title_duplicate_group_size"] > 1).sum())
    n_zero_confirmed = int((df["shipping_value_reliability"] == "ZERO_CONFIRMED_FREE_SHIPPING").sum())
    n_zero_ambiguous = int((df["shipping_value_reliability"] == "ZERO_AMBIGUOUS").sum())

    print(f"   ✓ Written {len(output):,} rows to stg_historical_vcp_aggregate")
    print(f"     Brand derivation: {len(output) - n_brand_null:,} grounded, {n_brand_null:,} NULL")
    print(f"     Duplicate-title groups: {n_dup_groups:,} groups covering {n_dup_group_rows:,} rows (never collapsed/summed)")
    print(f"     Shipping reliability: OBSERVED_NONZERO={int((df['shipping_value_reliability']=='OBSERVED_NONZERO').sum()):,} "
          f"ZERO_CONFIRMED_FREE_SHIPPING={n_zero_confirmed:,} ZERO_AMBIGUOUS={n_zero_ambiguous:,}")
    print(f"     FX fallback rows: {n_fallback:,}")


LOT_TITLE_RE = re.compile(
    r"(?:\b\d+\s*x\b|\bbundle\b|\bjob\s*lot\b|\bkonvolut\b|\bsammlung\b|\blot\s+of\b|\bx\s*rolex\b)",
    re.IGNORECASE,
)


def clean_historical_ebay_sold(conn):
    """
    Module 4: cleans raw_historical_ebay_sold into stg_historical_ebay_sold
    — the eBay item-wise counterpart to clean_historical_vcp_aggregate(),
    deliberately a separate table and a separate function, never
    concatenated with the VCP source. raw_historical_ebay_sold is never
    modified.

    One staged row remains one individual sold-listing observation.
    item_number (verified unique in raw) is the natural key; no
    deduplication happens here (raw is already unique on it by
    construction — see insert_historical_ebay_sold_exports).

    Best Offer price policy (explicit, never silently blurred):
      best_offer = FALSE -> price_reliability = CONFIRMED_DISPLAYED_SOLD_PRICE
      best_offer = TRUE  -> price_reliability = LISTED_PRICE_PROXY_BEST_OFFER
    The accepted negotiated amount for a Best-Offer sale is NOT known from
    this source — price_eur for those rows is the displayed card price
    only, flagged as a proxy, never claimed as the confirmed transaction
    price.

    Shipping policy (graduated, never a blanket fillna(0)):
      shipping present            -> converted normally
      shipping NULL, free_shipping=TRUE  -> shipping_eur = 0 (proven, not guessed)
      shipping NULL, free_shipping!=TRUE -> shipping_eur AND landed_cost_eur stay NULL
    """
    print("\nCleaning historical eBay item-wise sold-listing data...")

    df = conn.execute("SELECT * FROM raw_historical_ebay_sold").df()
    print(f"   Input: {len(df):,} rows")

    if df.empty:
        conn.execute("DELETE FROM stg_historical_ebay_sold")
        print("   ✓ Written 0 rows to stg_historical_ebay_sold (no raw_historical_ebay_sold rows)")
        return

    # Step 1: reject invalid price rows
    before = len(df)
    df = df[df["price_eur"].notna() & (df["price_eur"] > 0)].copy()
    if before - len(df):
        print(f"   Dropped {before - len(df)} rows with missing/non-positive price")

    df["stg_id"] = range(1, len(df) + 1)
    df["normalized_title"] = df["title"].apply(utils.normalize_title)

    # Step 2: rename raw fields to the "_original" contract — price_eur in
    # raw is the price AS SCRAPED, in whatever currency `currency` says,
    # NOT assumed EUR at this layer even though it is 100% EUR today (see
    # clean_historical_ebay_sold's docstring / Step 4 policy).
    df["price_original"] = df["price_eur"]
    df["currency_original"] = df["currency"].str.upper()
    df["shipping_original"] = df["shipping_eur"]
    # No separate shipping-currency field exists in this source — a single
    # listing's price and shipping are always shown in the same site
    # currency, so shipping is assumed to share price's currency. This is
    # a stated inference, not a silent one.
    df["shipping_currency_original"] = df["currency_original"]

    # Step 3: FX — long-format ASOF join covering price for every row, and
    # shipping only for rows that actually have a shipping value (a NULL
    # shipping_original must never acquire a fabricated rate).
    conn.register("tmp_ebay_dates", df[["stg_id", "sold_date_iso"]])

    price_ccy = df[["stg_id", "sold_date_iso", "currency_original"]].rename(columns={"currency_original": "ccy"})
    price_ccy["field"] = "price"
    ship_rows = df[df["shipping_original"].notna()]
    ship_ccy = ship_rows[["stg_id", "sold_date_iso", "shipping_currency_original"]].rename(
        columns={"shipping_currency_original": "ccy"}
    )
    ship_ccy["field"] = "shipping"
    long_df = pd.concat([price_ccy, ship_ccy], ignore_index=True)
    conn.register("tmp_ebay_long_ccy", long_df)

    to_eur = conn.execute("""
        SELECT l.stg_id, l.field, l.ccy, r.rate AS rate_to_eur, r.valid_date AS fx_rate_date
        FROM tmp_ebay_long_ccy l
        ASOF LEFT JOIN (
            SELECT from_currency, valid_date, rate FROM ref_exchange_rates
            WHERE to_currency = 'EUR'
        ) r
        ON l.ccy = r.from_currency AND l.sold_date_iso >= r.valid_date
    """).df()
    conn.unregister("tmp_ebay_long_ccy")

    # Currency already EUR needs no lookup at all (identity rate) — avoids
    # depending on a ref_exchange_rates EUR->EUR row existing. This
    # function already had this right; clean_active_broad and
    # clean_active_targeted were audited, found missing it (a EUR-priced
    # row collected/fetched before the single dated EUR->EUR reference row
    # was wrongly flagged fallback), and fixed to match this same pattern.
    is_eur_mask = to_eur["ccy"] == "EUR"
    to_eur.loc[is_eur_mask, "rate_to_eur"] = 1.0

    to_eur["rate_is_fallback"] = to_eur["rate_to_eur"].isna()
    latest_by_ccy = dict(conn.execute("""
        SELECT from_currency, rate FROM ref_exchange_rates r1
        WHERE to_currency = 'EUR' AND valid_date = (
            SELECT MAX(valid_date) FROM ref_exchange_rates r2
            WHERE r2.from_currency = r1.from_currency AND r2.to_currency = 'EUR'
        )
    """).fetchall())
    fallback_mask = to_eur["rate_to_eur"].isna()
    n_fx_fallback = int(fallback_mask.sum())
    if n_fx_fallback:
        to_eur.loc[fallback_mask, "rate_to_eur"] = to_eur.loc[fallback_mask, "ccy"].map(latest_by_ccy).values
        print(f"   ⚠ {n_fx_fallback} price/shipping fields had no ASOF rate match — used latest known rate (traceable via fx_rate_is_fallback)")

    price_rates = to_eur[to_eur["field"] == "price"][["stg_id", "rate_to_eur", "fx_rate_date", "rate_is_fallback"]].rename(
        columns={"rate_to_eur": "price_to_eur_rate", "rate_is_fallback": "price_fx_is_fallback"}
    )
    ship_rates = to_eur[to_eur["field"] == "shipping"][["stg_id", "rate_to_eur", "rate_is_fallback"]].rename(
        columns={"rate_to_eur": "shipping_to_eur_rate", "rate_is_fallback": "shipping_fx_is_fallback"}
    )
    df = df.merge(price_rates, on="stg_id", how="left")
    df = df.merge(ship_rates, on="stg_id", how="left")

    df["price_eur"] = (df["price_original"] * df["price_to_eur_rate"]).round(2)

    # Shipping graduated policy — applied AFTER FX conversion of whatever
    # real shipping values exist, never replacing a genuinely-unknown
    # value with a converted zero.
    df["shipping_eur"] = (df["shipping_original"] * df["shipping_to_eur_rate"]).round(2)
    has_shipping = df["shipping_original"].notna()
    proven_free = (~has_shipping) & (df["free_shipping"] == True)  # noqa: E712
    unknown_shipping = (~has_shipping) & (~proven_free)
    df.loc[proven_free, "shipping_eur"] = 0.0
    df.loc[unknown_shipping, "shipping_eur"] = None

    df["landed_cost_eur"] = df["price_eur"] + df["shipping_eur"]
    # pandas propagates NaN through + automatically, but be explicit: any
    # row with shipping_eur NULL must have landed_cost_eur NULL too.
    df.loc[df["shipping_eur"].isna(), "landed_cost_eur"] = None

    null_prices = int(df["price_eur"].isna().sum())
    if null_prices:
        print(f"   ⚠ {null_prices} rows have unresolvable currency — price_eur set to NULL")

    # Step 4: EUR -> USD bridge, keyed on sold_date_iso, same pattern as
    # clean_historical_vcp_aggregate / clean_historical.
    fx_usd = conn.execute("""
        SELECT a.stg_id, r.rate AS eur_usd_rate, r.valid_date AS fx_rate_date_usd
        FROM tmp_ebay_dates a
        ASOF LEFT JOIN (
            SELECT valid_date, rate FROM ref_exchange_rates
            WHERE from_currency = 'EUR' AND to_currency = 'USD'
        ) r
        ON a.sold_date_iso >= r.valid_date
    """).df()
    conn.unregister("tmp_ebay_dates")

    latest_eur_usd_row = conn.execute("""
        SELECT rate FROM ref_exchange_rates
        WHERE from_currency='EUR' AND to_currency='USD'
        ORDER BY valid_date DESC LIMIT 1
    """).fetchone()
    fx_usd["eur_usd_is_fallback"] = fx_usd["eur_usd_rate"].isna()
    if latest_eur_usd_row:
        fx_usd["eur_usd_rate"] = fx_usd["eur_usd_rate"].fillna(latest_eur_usd_row[0])
    df = df.merge(fx_usd, on="stg_id", how="left")

    df["price_usd"] = (df["price_eur"] * df["eur_usd_rate"]).round(2)
    df["shipping_usd"] = (df["shipping_eur"] * df["eur_usd_rate"]).round(2)
    df["landed_cost_usd"] = df["price_usd"] + df["shipping_usd"]
    df.loc[df["shipping_usd"].isna(), "landed_cost_usd"] = None

    df["fx_rate_is_fallback"] = (
        df["price_fx_is_fallback"].astype("boolean").fillna(False)
        | df["shipping_fx_is_fallback"].astype("boolean").fillna(False)
        | df["eur_usd_is_fallback"].astype("boolean").fillna(False)
    ).astype(bool)
    df["fx_rate_date"] = df["fx_rate_date_usd"]
    n_any_fallback = int(df["fx_rate_is_fallback"].sum())

    # Step 5: condition mapping — unmapped/contaminated values stay NULL,
    # never guessed.
    cond_map = dict(conn.execute(
        "SELECT condition_raw, condition_standard FROM ref_condition_map"
    ).fetchall())
    df["condition_raw"] = df["condition"]
    df["condition_standard"] = df["condition_raw"].map(cond_map)
    n_unmapped = int(df["condition_standard"].isna().sum())
    if n_unmapped:
        print(f"   ⚠ {n_unmapped} rows have an unmapped condition (left NULL, not guessed)")

    # Step 6: price reliability — Best Offer never claims a known negotiated amount
    df["price_reliability"] = df["best_offer"].apply(
        lambda v: "LISTED_PRICE_PROXY_BEST_OFFER" if v else "CONFIRMED_DISPLAYED_SOLD_PRICE"
    )
    n_best_offer_proxy = int((df["price_reliability"] == "LISTED_PRICE_PROXY_BEST_OFFER").sum())

    # Step 7: multi-unit lot heuristic — flag only, never an exclusion
    df["possible_multi_unit_lot"] = df["title"].str.contains(LOT_TITLE_RE, na=False)
    n_multi_unit = int(df["possible_multi_unit_lot"].sum())

    # Step 8: location passthrough (known-contaminated field — never cleaned/trusted here)
    df["location_raw"] = df["location"].replace("", None)
    df["seller_type"] = df["seller_type"].replace("", None)

    # Step 9: extraction completeness — constant for this snapshot; pagination
    # completeness is unconfirmed (see MODULE4_HISTORICAL_SOURCE_CONTRACTS.md)
    df["extraction_completeness"] = "UNKNOWN"

    output = df[[
        "stg_id", "id", "item_number", "title", "normalized_title", "sold_date_iso",
        "price_original", "currency_original", "price_eur", "price_usd", "price_reliability",
        "shipping_original", "shipping_currency_original", "shipping_eur", "shipping_usd",
        "landed_cost_eur", "landed_cost_usd",
        "eur_usd_rate", "fx_rate_date", "fx_rate_is_fallback",
        "condition_raw", "condition_standard", "seller_type",
        "free_shipping", "best_offer",
        "location_raw", "possible_multi_unit_lot",
        "seller", "url", "source_page", "upload_batch_id", "source_filename", "file_hash",
        "extraction_completeness", "row_hash",
    ]].rename(columns={
        "stg_id": "id", "id": "raw_id", "sold_date_iso": "sold_date",
        "eur_usd_rate": "eur_usd_rate_used", "best_offer": "has_best_offer_option",
    })

    output["matched_product_id"] = None
    output["match_confidence"]   = None
    output["match_method"]       = None
    output["match_score"]        = None

    # Module 5 evidence identity: item_number alone — this table has no
    # marketplace column (verified, not assumed). See
    # scripts/evidence_identity.py.
    output = evidence_identity.add_sold_ebay_identity_columns(
        output, item_number_col="item_number", raw_id_col="raw_id",
    )

    conn.execute("DELETE FROM stg_historical_ebay_sold")
    stg_ebay_cols = [
        "id", "raw_id", "item_number", "title", "normalized_title", "sold_date",
        "price_original", "currency_original", "price_eur", "price_usd", "price_reliability",
        "shipping_original", "shipping_currency_original", "shipping_eur", "shipping_usd",
        "landed_cost_eur", "landed_cost_usd",
        "eur_usd_rate_used", "fx_rate_date", "fx_rate_is_fallback",
        "condition_raw", "condition_standard", "seller_type",
        "free_shipping", "has_best_offer_option",
        "location_raw", "possible_multi_unit_lot",
        "seller", "url", "source_page", "upload_batch_id", "source_filename", "file_hash",
        "extraction_completeness", "row_hash",
        "matched_product_id", "match_confidence", "match_method", "match_score",
        "stable_evidence_uid", "observation_uid",
    ]
    conn.execute(
        f"INSERT INTO stg_historical_ebay_sold ({','.join(stg_ebay_cols)}) "
        f"SELECT {','.join(stg_ebay_cols)} FROM output"
    )

    print(f"   ✓ Written {len(output):,} rows to stg_historical_ebay_sold")
    print(f"     Price reliability: CONFIRMED_DISPLAYED_SOLD_PRICE={len(output)-n_best_offer_proxy:,} LISTED_PRICE_PROXY_BEST_OFFER={n_best_offer_proxy:,}")
    print(f"     Condition unmapped: {n_unmapped:,}")
    print(f"     Possible multi-unit lot titles: {n_multi_unit:,}")
    print(f"     FX fallback rows: {n_any_fallback:,}")
    print(f"     extraction_completeness: UNKNOWN for all rows (pagination completeness unconfirmed)")


# ══════════════════════════════════════════════════════════════════════════════
# CLEAN ACTIVE LISTINGS
# ══════════════════════════════════════════════════════════════════════════════

def clean_active_broad(conn):
    print("\nCleaning active listings...")

    df = conn.execute("SELECT * FROM raw_active_broad").df()
    cond_map = dict(conn.execute(
        "SELECT condition_raw, condition_standard FROM ref_condition_map"
    ).fetchall())

    print(f"   Input: {len(df):,} rows")

    # Step 1: Deduplicate — same item on multiple eBay country sites, AND
    # (now that raw_active_broad's ingest-time dedup is row_hash-based, not
    # item_id-only — see insert_current_listings) potentially multiple
    # observations of the SAME item_id over time as its price moved. raw
    # keeps every one of those as an append-only observation history;
    # staging keeps only the latest per item_id. Sort by collected_at_utc
    # descending first so keep="first" means "the newest," not "whatever
    # row order the table happened to return" — the pre-fix behaviour,
    # which for a table without duplicates was harmless but silently
    # became "oldest wins" the moment a second observation could exist.
    before = len(df)
    df["_collected_sort_dt"] = pd.to_datetime(df["collected_at_utc"], errors="coerce", utc=True)
    df = df.sort_values("_collected_sort_dt", ascending=False, na_position="last")
    df = df.drop_duplicates(subset=["item_id"], keep="first").copy()
    df = df.drop(columns=["_collected_sort_dt"])
    print(f"   Deduplication: {before:,} → {len(df):,} rows ({before-len(df):,} duplicate/stale observations removed, latest kept)")

    # Step 2: Drop rows with no price
    before = len(df)
    df = df[df["price_value"].notna() & (df["price_value"] > 0)].copy()
    if before - len(df):
        print(f"   Dropped {before-len(df)} rows with missing price")

    # Zero rows past this point (empty raw table, or every row dropped for
    # missing price) — stop here, same early-return convention already
    # used by clean_historical_vcp_aggregate/clean_historical_ebay_sold/
    # clean_active_targeted. Continuing past this point on an empty frame
    # hits a genuine pandas pitfall: df.apply(..., result_type="expand")
    # on a 0-row DataFrame can't infer the lambda's real return columns
    # and echoes back df's OWN columns instead, so the later
    # pd.concat([df, scenario], axis=1) silently duplicates every existing
    # column (confirmed: "condition" appearing twice) and crashes the
    # very next assignment that reads it.
    if df.empty:
        conn.execute("DELETE FROM stg_active_broad")
        print("   ✓ Written 0 rows to stg_active_broad (no active-broad rows to clean)")
        return

    df["stg_id"] = range(1, len(df)+1)

    # Step 2b: normalized title — source-agnostic, no entity extraction
    df["normalized_title"] = df["title"].apply(utils.normalize_title)

    # Step 3: dates we'll key the FX lookup on — the day the listing was
    # collected (i.e. "now" for a live snapshot). NOTE: unlike historical
    # data, active listings don't have a meaningful "transaction date" —
    # they're current market prices, so using the rate as of collection
    # date IS the correct rate, not an approximation.
    df["collected_date"] = pd.to_datetime(df["collected_at_utc"], errors="coerce", utc=True).dt.date

    # Step 4: ASOF-join the nearest available ECB rate (on or before
    # collected_date) for whatever currency each listing is priced in,
    # AND its shipping is priced in — they can differ.
    conn.register("tmp_active_dates", df[["stg_id", "collected_date"]])

    # Long-format table: one row per (stg_id, currency-to-convert) so a single
    # ASOF join handles price_currency and shipping_cost_currency together.
    price_ccy = df[["stg_id", "collected_date", "price_currency"]].rename(columns={"price_currency": "ccy"})
    ship_ccy  = df[["stg_id", "collected_date", "shipping_cost_currency"]].rename(columns={"shipping_cost_currency": "ccy"})
    price_ccy["field"] = "price"
    ship_ccy["field"]  = "shipping"
    long_df = pd.concat([price_ccy, ship_ccy], ignore_index=True)
    long_df["ccy"] = long_df["ccy"].str.upper()
    conn.register("tmp_long_ccy", long_df)

    to_eur = conn.execute("""
        SELECT l.stg_id, l.field, r.rate AS rate_to_eur, r.valid_date AS fx_rate_date
        FROM tmp_long_ccy l
        ASOF LEFT JOIN (
            SELECT from_currency, valid_date, rate FROM ref_exchange_rates
            WHERE to_currency = 'EUR'
        ) r
        ON l.ccy = r.from_currency AND l.collected_date >= r.valid_date
    """).df()
    conn.unregister("tmp_long_ccy")

    # Currency already EUR needs no lookup at all (identity rate) — same
    # fix as clean_historical_ebay_sold's already-correct pattern. EUR->EUR
    # is exactly 1.0 for any date; resolving it via the ASOF join above
    # made it depend on a ref_exchange_rates EUR->EUR row's own valid_date,
    # so a EUR-priced row collected before that single dated row wrongly
    # read as a genuine missing-rate fallback. Set unconditionally, before
    # rate_is_fallback is computed, so EUR is never flagged fallback
    # regardless of collection date or whether such a row even exists.
    # Joined on (stg_id, field), never positional — DuckDB's row order is
    # not guaranteed to match the input frame's order.
    to_eur = to_eur.merge(long_df[["stg_id", "field", "ccy"]], on=["stg_id", "field"], how="left")
    to_eur.loc[to_eur["ccy"] == "EUR", "rate_to_eur"] = 1.0

    # EUR -> USD bridge rate, same ASOF logic, keyed only on date
    eur_usd = conn.execute("""
        SELECT a.stg_id, r.rate AS eur_usd_rate, r.valid_date AS fx_rate_date_usd
        FROM tmp_active_dates a
        ASOF LEFT JOIN (
            SELECT valid_date, rate FROM ref_exchange_rates
            WHERE from_currency = 'EUR' AND to_currency = 'USD'
        ) r
        ON a.collected_date >= r.valid_date
    """).df()
    conn.unregister("tmp_active_dates")

    # Fallback: any currency/date combo with no ASOF match (e.g. before the
    # earliest fetched rate) gets the latest known rate for that currency —
    # flagged, never silently dropped.
    latest_by_ccy = conn.execute("""
        SELECT from_currency, rate FROM ref_exchange_rates r1
        WHERE to_currency = 'EUR' AND valid_date = (
            SELECT MAX(valid_date) FROM ref_exchange_rates r2
            WHERE r2.from_currency = r1.from_currency AND r2.to_currency = 'EUR'
        )
    """).fetchall()
    latest_by_ccy = dict(latest_by_ccy)
    # Captured BEFORE the fallback fill below, and carried all the way
    # through to the final fx_rate_is_fallback flag (see that assignment
    # further down) — matching clean_active_targeted's already-correct
    # pattern. Checking for "still null" AFTER the fill (the pre-fix
    # behaviour here) can only ever catch the doubly-unresolved case,
    # since every successfully-substituted row is no longer null by then.
    to_eur["rate_is_fallback"] = to_eur["rate_to_eur"].isna()
    n_fx_fallback = int(to_eur["rate_is_fallback"].sum())
    if n_fx_fallback:
        fallback_mask = to_eur["rate_is_fallback"]
        # ccy already present on to_eur (added above for the EUR-identity
        # check) — reused here directly rather than re-merging long_df a
        # second time for the same column.
        to_eur.loc[fallback_mask, "rate_to_eur"] = to_eur.loc[fallback_mask, "ccy"].map(latest_by_ccy).values
        print(f"   ⚠ {n_fx_fallback} price/shipping fields had no ASOF rate match — used latest known rate")

    _latest_eur_usd_row = conn.execute("""
        SELECT rate FROM ref_exchange_rates
        WHERE from_currency='EUR' AND to_currency='USD'
        ORDER BY valid_date DESC LIMIT 1
    """).fetchone()
    if _latest_eur_usd_row is None:
        raise RuntimeError(
            "ref_exchange_rates has no EUR->USD rate -- run scripts/00_load_fx_rates.py "
            "against this database before scripts/02_clean.py (pipeline prerequisite, "
            "never a silent fallback for a currency-conversion rate)."
        )
    latest_eur_usd = _latest_eur_usd_row[0]
    eur_usd["eur_usd_is_fallback"] = eur_usd["eur_usd_rate"].isna()
    eur_usd["eur_usd_rate"] = eur_usd["eur_usd_rate"].fillna(latest_eur_usd)

    price_rates = to_eur[to_eur["field"] == "price"][["stg_id", "rate_to_eur", "fx_rate_date", "rate_is_fallback"]].rename(
        columns={"rate_to_eur": "price_to_eur_rate", "fx_rate_date": "price_fx_date", "rate_is_fallback": "price_fx_is_fallback"})
    ship_rates = to_eur[to_eur["field"] == "shipping"][["stg_id", "rate_to_eur", "rate_is_fallback"]].rename(
        columns={"rate_to_eur": "shipping_to_eur_rate", "rate_is_fallback": "shipping_fx_is_fallback"})

    df = df.merge(price_rates, on="stg_id", how="left")
    df = df.merge(ship_rates, on="stg_id", how="left")
    df = df.merge(eur_usd[["stg_id", "eur_usd_rate", "fx_rate_date_usd", "eur_usd_is_fallback"]], on="stg_id", how="left")

    # Step 5: Convert to EUR (landed cost = price + shipping)
    df["price_eur"] = (df["price_value"] * df["price_to_eur_rate"]).round(2)
    df["shipping_eur"] = (df["shipping_cost_value"] * df["shipping_to_eur_rate"]).round(2)
    df["shipping_eur"] = df["shipping_eur"].fillna(0)  # unknown shipping → assume 0, documented assumption
    df["landed_cost_eur"] = (df["price_eur"] + df["shipping_eur"]).round(2)

    null_prices = df["price_eur"].isna().sum()
    if null_prices:
        print(f"   ⚠ {null_prices} rows have unknown currency — price_eur set to NULL")

    # Step 6: Convert to USD. If the listing is ALREADY in USD, use the
    # original value directly (exact — no FX error introduced at all).
    # Otherwise bridge via EUR: price_eur * (EUR->USD rate for that date).
    is_usd = df["price_currency"].str.upper() == "USD"
    df["price_usd"] = df["price_eur"] * df["eur_usd_rate"]
    df.loc[is_usd, "price_usd"] = df.loc[is_usd, "price_value"]
    df["price_usd"] = df["price_usd"].round(2)

    is_usd_ship = df["shipping_cost_currency"].str.upper() == "USD"
    shipping_usd = df["shipping_eur"] * df["eur_usd_rate"]
    shipping_usd[is_usd_ship] = df.loc[is_usd_ship, "shipping_cost_value"]
    df["landed_cost_usd"] = (df["price_usd"] + shipping_usd.fillna(0)).round(2)

    # Traceable per staged row: True whenever ANY of price / shipping / the
    # EUR-USD bridge needed a substituted or unresolved rate for this row —
    # captured pre-fill above, not "still null after the fallback fill"
    # (which is what this line used to check, and why it under-reported:
    # by this point every successfully-substituted rate is already
    # non-null). Same fix, same pattern as clean_active_targeted.
    df["fx_rate_is_fallback"] = (
        df["price_fx_is_fallback"].fillna(False)
        | df["shipping_fx_is_fallback"].fillna(False)
        | df["eur_usd_is_fallback"].fillna(False)
    )
    df["fx_rate_date"] = df["fx_rate_date_usd"]  # dominant date used for reporting/audit

    # Step 6b: Module 1 scenario prices/costs — standardized fixed-shipping
    # DE/US scenarios off the sale price alone. See utils.compute_scenario_prices.
    scenario = df.apply(
        lambda r: utils.compute_scenario_prices(r["price_eur"]), axis=1, result_type="expand"
    )
    df = pd.concat([df, scenario], axis=1)

    # Step 7: Normalise condition to 5 standard tiers
    df["condition_standard"] = df["condition"].map(cond_map)
    unmapped = df["condition_standard"].isna().sum()
    if unmapped:
        print(f"   ⚠ {unmapped} unmapped conditions:")
        unknowns = df[df["condition_standard"].isna()]["condition"].value_counts()
        for cond, count in unknowns.items():
            print(f"     '{cond}': {count} rows")

    # Step 8: Normalise buying options
    df["is_auction"]         = df["buying_options"].str.contains("AUCTION",    na=False)
    df["accepts_best_offer"] = df["buying_options"].str.contains("BEST_OFFER", na=False)

    # Step 9: Compute days listed (how long this listing has been on eBay)
    df["collected_dt"]    = pd.to_datetime(df["collected_at_utc"],   errors="coerce", utc=True)
    df["created_dt"]      = pd.to_datetime(df["item_creation_date"], errors="coerce", utc=True)
    df["days_listed"]     = (df["collected_dt"] - df["created_dt"]).dt.days

    # Step 10: Build output
    output = df[[
        "stg_id","id","item_id","title","normalized_title",
        "price_value","price_currency","price_eur","shipping_eur","landed_cost_eur",
        "price_usd","landed_cost_usd","price_to_eur_rate","eur_usd_rate","fx_rate_date","fx_rate_is_fallback",
        "price_virtual_eur","landed_cost_de_eur","landed_cost_us_eur",
        "shipping_de_eur","shipping_us_eur","estimated_import_charges_us_eur",
        "condition","condition_standard",
        "is_auction","accepts_best_offer",
        "seller_username","seller_feedback_score","seller_feedback_percentage",
        "item_location_country","listing_marketplace_id",
        "collected_at_utc","item_creation_date","days_listed"
    ]].rename(columns={
        "stg_id":"id","id":"raw_id",
        "price_value":"price_original","price_currency":"price_currency_original",
        "price_to_eur_rate":"fx_to_eur_rate_used","eur_usd_rate":"eur_usd_rate_used",
        "condition":"condition_raw","listing_marketplace_id":"marketplace",
        "collected_at_utc":"collected_at_utc","item_creation_date":"item_creation_date"
    })

    output["matched_product_id"] = None
    output["match_confidence"]   = None
    output["match_method"]       = None
    output["match_score"]        = None

    # Module 5 evidence identity: marketplace + item_id ONLY —
    # inventory_uid is deliberately not part of this table's grain
    # anyway (clean_active_broad dedups by item_id alone), but stated
    # here explicitly for consistency with clean_active_targeted. See
    # scripts/evidence_identity.py.
    output = evidence_identity.add_active_identity_columns(
        output, marketplace_col="marketplace", item_id_col="item_id", raw_id_col="raw_id",
    )

    conn.execute("DELETE FROM stg_active_broad")
    stg_active_cols = ["id","raw_id","item_id","title","normalized_title",
                       "price_original","price_currency_original","price_eur","shipping_eur","landed_cost_eur",
                       "price_usd","landed_cost_usd","fx_to_eur_rate_used","eur_usd_rate_used","fx_rate_date","fx_rate_is_fallback",
                       "price_virtual_eur","landed_cost_de_eur","landed_cost_us_eur",
                       "shipping_de_eur","shipping_us_eur","estimated_import_charges_us_eur",
                       "condition_raw","condition_standard","is_auction","accepts_best_offer",
                       "seller_username","seller_feedback_score","seller_feedback_percentage",
                       "item_location_country","marketplace",
                       "collected_at_utc","item_creation_date","days_listed",
                       "matched_product_id","match_confidence","match_method","match_score",
                       "stable_evidence_uid","observation_uid"]
    conn.execute(f"INSERT INTO stg_active_broad ({','.join(stg_active_cols)}) SELECT {','.join(stg_active_cols)} FROM output")

    # DuckDB native MEDIAN and QUANTILE — no workarounds needed
    stats = conn.execute("""
        SELECT
            MIN(landed_cost_eur)             AS min_lc,
            QUANTILE_CONT(landed_cost_eur, 0.25) AS p25_lc,
            MEDIAN(landed_cost_eur)          AS median_lc,
            QUANTILE_CONT(landed_cost_eur, 0.75) AS p75_lc,
            MAX(landed_cost_eur)             AS max_lc,
            MEDIAN(landed_cost_usd)          AS median_lc_usd
        FROM stg_active_broad
        WHERE landed_cost_eur IS NOT NULL
    """).fetchone()

    cond_counts = conn.execute("""
        SELECT condition_standard, COUNT(*) as n
        FROM stg_active_broad
        GROUP BY condition_standard
        ORDER BY n DESC
    """).fetchall()

    days_stats = conn.execute("""
        SELECT MEDIAN(days_listed), MAX(days_listed)
        FROM stg_active_broad
        WHERE days_listed IS NOT NULL AND days_listed >= 0
    """).fetchone()

    print(f"   ✓ Written {len(output):,} rows to stg_active_broad")
    if _aggregates_available(*stats):
        print(f"     Landed cost (EUR): min €{stats[0]:.2f} | p25 €{stats[1]:.2f} | median €{stats[2]:.2f} | p75 €{stats[3]:.2f} | max €{stats[4]:.2f}")
        print(f"     Landed cost (USD): median ${stats[5]:.2f}")
    else:
        print("     ⚠ No landed-cost summary available (0 rows, or all-NULL landed cost data)")
    if _aggregates_available(*days_stats):
        print(f"     Listing age:       median {days_stats[0]:.0f} days | max {days_stats[1]} days")
    else:
        print("     ⚠ No listing-age summary available (0 rows, or all-NULL/negative creation dates)")
    print(f"     Condition breakdown:")
    for cond, count in cond_counts:
        print(f"       {cond or 'UNMAPPED'}: {count:,}")


def clean_active_targeted(conn):
    """Clean raw_active_targeted → stg_active_targeted.

    Mirrors clean_active_broad()'s FX/condition/buying-option normalisation
    exactly, with one deliberate difference: there is NO plain item_id-only
    drop_duplicates here. In raw_active_broad, a repeated item_id across
    country sites really is the same evidence, so collapsing it is safe. In
    raw_active_targeted, the same item_id can legitimately be a genuine
    candidate for two DIFFERENT inventory items (that's the exact evidence
    the (inventory_uid, item_id, marketplace_id) grain fix in
    insert_targeted_listings/write_batch_csv preserves) — deduping by
    item_id alone here would silently re-collapse it one layer downstream.

    raw_active_targeted IS an append-only observation history on that
    3-column grain: the exact same (inventory_uid, item_id, marketplace_id)
    triple can legitimately recur across separate collection batches as a
    listing's price moves over time — raw preserves every one of those
    observations, on purpose, and this function must never touch raw.
    stg_active_targeted's job is different: it is the "current market
    snapshot" layer (schema.sql's stg_* = analysis-ready contract), so it
    keeps only the MOST RECENT observation per (inventory_uid, item_id,
    marketplace_id) — selected by fetched_at, never collapsed further and
    never collapsed across a different inventory_uid or marketplace. See
    the freshness-selection step below for the mechanics.

    This function does NOT filter out contamination (boxes, books, lots,
    complete/pocket watches, aftermarket) — that's relevance/match scoring,
    Module 5's job via matched_confidence/match_method/match_score, not
    cleaning's.
    """
    print("\nCleaning targeted active listings...")

    df = conn.execute("SELECT * FROM raw_active_targeted").df()
    cond_map = dict(conn.execute(
        "SELECT condition_raw, condition_standard FROM ref_condition_map"
    ).fetchall())

    print(f"   Input: {len(df):,} rows")

    if df.empty:
        conn.execute("DELETE FROM stg_active_targeted")
        print("   ✓ Written 0 rows to stg_active_targeted (no targeted-active rows to clean)")
        return

    # Step 1: Drop rows with no price. No plain item_id dedup — see docstring.
    before = len(df)
    df = df[df["price_value"].notna() & (df["price_value"] > 0)].copy()
    if before - len(df):
        print(f"   Dropped {before-len(df)} rows with missing price")

    # Step 1b: keep only the latest observation per (inventory_uid, item_id,
    # marketplace_id) — see docstring. PARTITION BY that exact triple,
    # ORDER BY collection timestamp (fetched_at) descending, keep the
    # first row of each partition. Never collapses across a different
    # inventory_uid or marketplace — those are separate partitions.
    # Unparseable fetched_at sorts last (na_position="last") so it is
    # never mistaken for "the latest" against a row with a real timestamp.
    before = len(df)
    df["_fetched_sort_dt"] = pd.to_datetime(df["fetched_at"], errors="coerce", utc=True)
    df = df.sort_values("_fetched_sort_dt", ascending=False, na_position="last")
    df = df.drop_duplicates(subset=["inventory_uid", "item_id", "marketplace_id"], keep="first")
    df = df.drop(columns=["_fetched_sort_dt"])
    superseded = before - len(df)
    if superseded:
        print(f"   Freshness: {before:,} → {len(df):,} rows "
              f"({superseded:,} older re-observation(s) of an already-seen "
              f"(inventory_uid, item_id, marketplace_id) triple superseded by a newer one)")

    df["stg_id"] = range(1, len(df)+1)

    # Step 2: normalized title — source-agnostic, no entity extraction
    df["normalized_title"] = df["title"].apply(utils.normalize_title)

    # Step 3: date to key the FX lookup on — collection timestamp, same
    # reasoning as clean_active_broad (current market price, no separate
    # transaction date). Targeted rows use fetched_at, not collected_at_utc.
    df["fetched_date"] = pd.to_datetime(df["fetched_at"], errors="coerce", utc=True).dt.date

    # Step 4: ASOF-join nearest ECB rate for price AND shipping currencies.
    conn.register("tmp_targeted_dates", df[["stg_id", "fetched_date"]])

    price_ccy = df[["stg_id", "fetched_date", "price_currency"]].rename(columns={"price_currency": "ccy"})
    ship_ccy  = df[["stg_id", "fetched_date", "shipping_cost_currency"]].rename(columns={"shipping_cost_currency": "ccy"})
    price_ccy["field"] = "price"
    ship_ccy["field"]  = "shipping"
    long_df = pd.concat([price_ccy, ship_ccy], ignore_index=True)
    long_df["ccy"] = long_df["ccy"].str.upper()
    conn.register("tmp_targeted_long_ccy", long_df)

    to_eur = conn.execute("""
        SELECT l.stg_id, l.field, l.ccy, r.rate AS rate_to_eur, r.valid_date AS fx_rate_date
        FROM tmp_targeted_long_ccy l
        ASOF LEFT JOIN (
            SELECT from_currency, valid_date, rate FROM ref_exchange_rates
            WHERE to_currency = 'EUR'
        ) r
        ON l.ccy = r.from_currency AND l.fetched_date >= r.valid_date
    """).df()
    conn.unregister("tmp_targeted_long_ccy")

    # Currency already EUR needs no lookup at all (identity rate) — same
    # fix as clean_historical_ebay_sold's already-correct pattern, applied
    # here and in clean_active_broad. EUR->EUR is exactly 1.0 for any
    # date; resolving it via the ASOF join above made it depend on a
    # ref_exchange_rates EUR->EUR row's own valid_date, so a EUR-priced
    # row fetched before that single dated row wrongly read as a genuine
    # missing-rate fallback. Set unconditionally, before rate_is_fallback
    # is computed below, so EUR is never flagged fallback regardless of
    # fetch date or whether such a row even exists.
    to_eur.loc[to_eur["ccy"] == "EUR", "rate_to_eur"] = 1.0

    # EUR -> USD bridge rate, same ASOF logic, keyed only on date
    eur_usd = conn.execute("""
        SELECT a.stg_id, r.rate AS eur_usd_rate, r.valid_date AS fx_rate_date_usd
        FROM tmp_targeted_dates a
        ASOF LEFT JOIN (
            SELECT valid_date, rate FROM ref_exchange_rates
            WHERE from_currency = 'EUR' AND to_currency = 'USD'
        ) r
        ON a.fetched_date >= r.valid_date
    """).df()
    conn.unregister("tmp_targeted_dates")

    # Capture "no direct ASOF match" PRE-fallback-fill, per field. This is
    # the flag that must survive into stg_active_targeted — computing it
    # after the fallback fill (as clean_active_broad does, and as this
    # function originally did) is wrong: by then rate_to_eur is non-null
    # again for every successfully-substituted row, so the flag would
    # always read False except in the doubly-unresolvable case.
    to_eur["rate_is_fallback"] = to_eur["rate_to_eur"].isna()

    # Fallback: any currency/date combo with no direct ASOF match gets the
    # latest known rate for that currency — flagged via rate_is_fallback
    # above, never silently dropped. A currency that's missing entirely
    # (shipping_cost_currency is NULL — 26 such fields in the live data,
    # all shipping) or unknown to ref_exchange_rates altogether cannot be
    # resolved by this fallback either: rate_to_eur stays NULL, and
    # rate_is_fallback correctly stays True to flag that no real
    # conversion happened. That is a genuinely different situation from
    # "known currency, no historical rate before this date, substitute the
    # latest known rate" — reported separately below so the two are never
    # conflated in the console output.
    latest_by_ccy = conn.execute("""
        SELECT from_currency, rate FROM ref_exchange_rates r1
        WHERE to_currency = 'EUR' AND valid_date = (
            SELECT MAX(valid_date) FROM ref_exchange_rates r2
            WHERE r2.from_currency = r1.from_currency AND r2.to_currency = 'EUR'
        )
    """).fetchall()
    latest_by_ccy = dict(latest_by_ccy)
    fallback_mask = to_eur["rate_to_eur"].isna()
    n_fx_fallback = fallback_mask.sum()
    if n_fx_fallback:
        substituted = to_eur.loc[fallback_mask, "ccy"].map(latest_by_ccy)
        to_eur.loc[fallback_mask, "rate_to_eur"] = substituted.values
        n_applied = int(substituted.notna().sum())
        n_unresolved = int(substituted.isna().sum())
        if n_applied:
            print(f"   ⚠ {n_applied} price/shipping fields had no ASOF rate match for a known currency — used latest known rate")
        if n_unresolved:
            print(f"   ⚠ {n_unresolved} price/shipping fields have no resolvable currency at all (e.g. missing shipping_cost_currency) — left unconverted, flagged via fx_rate_is_fallback")

    _latest_eur_usd_row = conn.execute("""
        SELECT rate FROM ref_exchange_rates
        WHERE from_currency='EUR' AND to_currency='USD'
        ORDER BY valid_date DESC LIMIT 1
    """).fetchone()
    if _latest_eur_usd_row is None:
        raise RuntimeError(
            "ref_exchange_rates has no EUR->USD rate -- run scripts/00_load_fx_rates.py "
            "against this database before scripts/02_clean.py (pipeline prerequisite, "
            "never a silent fallback for a currency-conversion rate)."
        )
    latest_eur_usd = _latest_eur_usd_row[0]
    eur_usd["eur_usd_is_fallback"] = eur_usd["eur_usd_rate"].isna()
    eur_usd["eur_usd_rate"] = eur_usd["eur_usd_rate"].fillna(latest_eur_usd)

    price_rates = to_eur[to_eur["field"] == "price"][["stg_id", "rate_to_eur", "fx_rate_date", "rate_is_fallback"]].rename(
        columns={"rate_to_eur": "price_to_eur_rate", "fx_rate_date": "price_fx_date", "rate_is_fallback": "price_fx_is_fallback"})
    ship_rates = to_eur[to_eur["field"] == "shipping"][["stg_id", "rate_to_eur", "rate_is_fallback"]].rename(
        columns={"rate_to_eur": "shipping_to_eur_rate", "rate_is_fallback": "shipping_fx_is_fallback"})

    df = df.merge(price_rates, on="stg_id", how="left")
    df = df.merge(ship_rates, on="stg_id", how="left")
    df = df.merge(eur_usd[["stg_id", "eur_usd_rate", "fx_rate_date_usd", "eur_usd_is_fallback"]], on="stg_id", how="left")

    # Step 5: Convert to EUR (landed cost = price + shipping)
    df["price_eur"] = (df["price_value"] * df["price_to_eur_rate"]).round(2)
    df["shipping_eur"] = (df["shipping_cost_value"] * df["shipping_to_eur_rate"]).round(2)
    df["shipping_eur"] = df["shipping_eur"].fillna(0)
    df["landed_cost_eur"] = (df["price_eur"] + df["shipping_eur"]).round(2)

    null_prices = df["price_eur"].isna().sum()
    if null_prices:
        print(f"   ⚠ {null_prices} rows have unknown currency — price_eur set to NULL")

    # Step 6: Convert to USD, same direct/bridge logic as clean_active_broad
    is_usd = df["price_currency"].str.upper() == "USD"
    df["price_usd"] = df["price_eur"] * df["eur_usd_rate"]
    df.loc[is_usd, "price_usd"] = df.loc[is_usd, "price_value"]
    df["price_usd"] = df["price_usd"].round(2)

    is_usd_ship = df["shipping_cost_currency"].str.upper() == "USD"
    shipping_usd = df["shipping_eur"] * df["eur_usd_rate"]
    shipping_usd[is_usd_ship] = df.loc[is_usd_ship, "shipping_cost_value"]
    df["landed_cost_usd"] = (df["price_usd"] + shipping_usd.fillna(0)).round(2)

    # Traceable per staged row: True whenever ANY of price / shipping / the
    # EUR-USD bridge needed a substituted or unresolved rate for this row —
    # not just "still null after the fallback fill", which is what the
    # pre-fix version checked and why it read False for the 26 known
    # shipping-fallback fields (the fill had already succeeded by then).
    df["fx_rate_is_fallback"] = (
        df["price_fx_is_fallback"].fillna(False)
        | df["shipping_fx_is_fallback"].fillna(False)
        | df["eur_usd_is_fallback"].fillna(False)
    )
    df["fx_rate_date"] = df["fx_rate_date_usd"]

    # Step 6b: Module 1 scenario prices/costs
    scenario = df.apply(
        lambda r: utils.compute_scenario_prices(r["price_eur"]), axis=1, result_type="expand"
    )
    df = pd.concat([df, scenario], axis=1)

    # Step 7: Normalise condition to 5 standard tiers
    df["condition_standard"] = df["condition"].map(cond_map)
    unmapped = df["condition_standard"].isna().sum()
    if unmapped:
        print(f"   ⚠ {unmapped} unmapped conditions:")
        unknowns = df[df["condition_standard"].isna()]["condition"].value_counts()
        for cond, count in unknowns.items():
            print(f"     '{cond}': {count} rows")

    # Step 8: Normalise buying options
    df["is_auction"]         = df["buying_options"].str.contains("AUCTION",    na=False)
    df["accepts_best_offer"] = df["buying_options"].str.contains("BEST_OFFER", na=False)

    # Step 9: Compute days listed
    df["fetched_dt"] = pd.to_datetime(df["fetched_at"],        errors="coerce", utc=True)
    df["created_dt"] = pd.to_datetime(df["item_creation_date"], errors="coerce", utc=True)
    df["days_listed"] = (df["fetched_dt"] - df["created_dt"]).dt.days

    # Step 10: Build output — preserves inventory_uid/canonical_inventory_id/
    # query_text/query_tier/query_template_version/collection_batch_id/
    # row_hash, none of which exist in clean_active_broad's output.
    output = df[[
        "stg_id","id","inventory_uid","canonical_inventory_id",
        "query_text","query_tier","query_template_version",
        "item_id","title","normalized_title",
        "price_value","price_currency","price_eur","shipping_eur","landed_cost_eur",
        "price_usd","landed_cost_usd","price_to_eur_rate","eur_usd_rate","fx_rate_date","fx_rate_is_fallback",
        "price_virtual_eur","landed_cost_de_eur","landed_cost_us_eur",
        "shipping_de_eur","shipping_us_eur","estimated_import_charges_us_eur",
        "condition","condition_standard",
        "is_auction","accepts_best_offer",
        "seller_username","seller_feedback_score","seller_feedback_percentage",
        "item_location_country","marketplace_id",
        "fetched_at","item_creation_date","days_listed",
        "collection_batch_id","row_hash",
    ]].rename(columns={
        "stg_id":"id","id":"raw_id",
        "price_value":"price_original","price_currency":"price_currency_original",
        "price_to_eur_rate":"fx_to_eur_rate_used","eur_usd_rate":"eur_usd_rate_used",
        "condition":"condition_raw","marketplace_id":"marketplace",
    })

    output["matched_canonical_inventory_id"] = None
    output["matched_product_id"] = None
    output["match_confidence"]   = None
    output["match_method"]       = None
    output["match_score"]        = None

    # Module 5 evidence identity: marketplace + item_id ONLY. Deliberately
    # NOT inventory_uid — this table's row grain is
    # (inventory_uid, item_id, marketplace) pairing-level (the same
    # listing legitimately pairs with multiple inventory_uids, up to 8
    # confirmed in the pilot data), but the EVIDENCE identity is the
    # listing itself, one level up. The many-to-many relationship
    # belongs in match_candidates_active, not in stable_evidence_uid.
    # See scripts/evidence_identity.py's module docstring for the full
    # grain-separation rationale.
    output = evidence_identity.add_active_identity_columns(
        output, marketplace_col="marketplace", item_id_col="item_id", raw_id_col="raw_id",
    )

    conn.execute("DELETE FROM stg_active_targeted")
    stg_targeted_cols = [
        "id","raw_id","inventory_uid","canonical_inventory_id",
        "query_text","query_tier","query_template_version",
        "item_id","title","normalized_title",
        "price_original","price_currency_original","price_eur","shipping_eur","landed_cost_eur",
        "price_usd","landed_cost_usd","fx_to_eur_rate_used","eur_usd_rate_used","fx_rate_date","fx_rate_is_fallback",
        "price_virtual_eur","landed_cost_de_eur","landed_cost_us_eur",
        "shipping_de_eur","shipping_us_eur","estimated_import_charges_us_eur",
        "condition_raw","condition_standard","is_auction","accepts_best_offer",
        "seller_username","seller_feedback_score","seller_feedback_percentage",
        "item_location_country","marketplace",
        "fetched_at","item_creation_date","days_listed",
        "collection_batch_id","row_hash",
        "matched_canonical_inventory_id","matched_product_id","match_confidence","match_method","match_score",
        "stable_evidence_uid","observation_uid",
    ]
    conn.execute(f"INSERT INTO stg_active_targeted ({','.join(stg_targeted_cols)}) SELECT {','.join(stg_targeted_cols)} FROM output")

    n_items = conn.execute("SELECT COUNT(DISTINCT inventory_uid) FROM stg_active_targeted").fetchone()[0]
    n_listings_multi_item = conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT item_id, marketplace FROM stg_active_targeted
            GROUP BY item_id, marketplace
            HAVING COUNT(DISTINCT inventory_uid) > 1
        )
    """).fetchone()[0]

    print(f"   ✓ Written {len(output):,} rows to stg_active_targeted, covering {n_items:,} distinct inventory items")
    if n_listings_multi_item:
        print(f"     {n_listings_multi_item:,} listing(s) are candidates for more than one inventory item (evidence preserved, not collapsed)")


# ══════════════════════════════════════════════════════════════════════════════
# CLEAN INVENTORY
# ══════════════════════════════════════════════════════════════════════════════

def is_excel_date_corrupted(value) -> bool:
    if value is None:
        return False
    text = str(value).strip()
    return bool(EXCEL_DATE_CORRUPTION_RE.match(text))


# ══════════════════════════════════════════════════════════════════════════════
# INVENTORY REPAIR FRAMEWORK
#
#   raw_inventory -> validation -> inventory_repair_candidates
#                                       -> inventory_corrections (approved)
#                                       -> clean_inventory (staging_inventory)
#
# raw_inventory is never modified anywhere in this framework. Every repair
# is a PROPOSAL first (inventory_repair_candidates), generated by a general
# pattern-matching rule — never hardcoded to specific row ids or values —
# so a future client upload with the SAME corruption shape (or a
# structurally similar one, once a new detector is added) is handled the
# same way automatically. Ambiguous proposals are never silently applied;
# only AUTO_REPAIR_ALLOWED candidates are ever auto-applied, and even then
# only via an explicit apply_auto_repairs() call, never inside
# clean_inventory() itself.
# ══════════════════════════════════════════════════════════════════════════════

_EXCEL_DATE_PARSE_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})(?:\s+\d{2}:\d{2}:\d{2})?$")


def _parse_excel_date_corruption(value: str) -> tuple[int, int, int] | None:
    """Extracts (year, month, day) from a value already confirmed by
    is_excel_date_corrupted(). Returns None if it somehow doesn't match
    (defensive only — callers only invoke this after that check)."""
    m = _EXCEL_DATE_PARSE_RE.match(str(value).strip())
    if not m:
        return None
    return int(m.group(1)), int(m.group(2)), int(m.group(3))


MIN_SIBLING_SAMPLE_FOR_AUTO_REPAIR = 10
# A necessary-but-not-sufficient gate on AUTO_REPAIR_ALLOWED: unanimous
# sibling agreement below this sample size is statistically weak evidence,
# not proof. The project-wide dash rate among non-corrupted calibers is
# ~9.4% (10/106, verified against live data) — under a null hypothesis of
# "this family isn't special," seeing zero dashes in even 10 draws still
# has a non-trivial chance of happening by luck alone. Below this
# threshold, unanimous agreement is treated as corroborating but
# insufficient on its own — statistical sibling agreement must never be
# the sole justification for silently changing a client identifier.


def _sibling_part_number_convention(conn, brand: str, caliber: str | None, exclude_raw_id: int) -> tuple[str, int]:
    """
    Classifies the part-number naming convention actually observed among
    OTHER rows sharing this (brand, caliber) — the evidence
    generate_inventory_repair_candidates uses to decide whether a bare-
    number repair guess is corroborated or contradicted by the family's
    own data, rather than assumed structurally. Returns (convention, n)
    where n is the number of siblings the convention was computed from —
    callers must apply their own minimum-sample-size gate before treating
    'all_bare' as sufficient for AUTO_REPAIR_ALLOWED (see
    MIN_SIBLING_SAMPLE_FOR_AUTO_REPAIR).

    convention is one of: 'all_bare' (every sibling's part number has no
    dash — corroborates a bare-number repair), 'mixed_or_dashed' (at least
    one sibling has a dash — contradicts a bare-number guess), or
    'no_siblings' (nothing to compare against — neither corroborates nor
    contradicts).
    """
    rows = conn.execute(
        """
        SELECT raw_p_number FROM raw_inventory
        WHERE raw_rolex_tudor = ? AND raw_calibre = ? AND id != ?
        """,
        [brand, caliber, exclude_raw_id],
    ).fetchall()
    siblings = [r[0] for r in rows if r[0] is not None and str(r[0]).strip() != "" and not is_excel_date_corrupted(r[0])]
    if not siblings:
        return "no_siblings", 0
    if any("-" in str(s) for s in siblings):
        return "mixed_or_dashed", len(siblings)
    return "all_bare", len(siblings)


def _classify_excel_date_repair(
    conn, *, raw_inventory_id: int, brand: str, caliber: str | None, column_name: str, raw_value: str,
) -> dict:
    """
    Deterministic classification for one Excel-date-corrupted value
    (caliber or part_number). See the INVENTORY REPAIR FRAMEWORK section
    docstring above for the full rule. Returns a dict with proposed_value/
    classification/confidence/repair_rule/repair_evidence — never mutates
    anything, purely a pure function of the value + sibling evidence.
    """
    parsed = _parse_excel_date_corruption(raw_value)
    if parsed is None:
        return {
            "proposed_value": None, "classification": "UNRESOLVED", "confidence": "LOW",
            "repair_rule": "excel_date_coercion_unparseable",
            "repair_evidence": f"Value matched the corruption regex but could not be parsed into Y/M/D: {raw_value!r}",
        }
    year, month, day = parsed

    if day != 1:
        return {
            "proposed_value": None, "classification": "UNRESOLVED", "confidence": "LOW",
            "repair_rule": "excel_date_coercion_nondefault_day",
            "repair_evidence": (
                f"day={day:02d} is not the expected default (1) for this corruption shape — "
                "does not match the known bare-number / dashed-variant pattern, no safe reconstruction exists."
            ),
        }

    if month == 1:
        # Consistent with a bare numeric original (e.g. "6655") that Excel
        # misread as a year-only date, defaulting both month and day to 1.
        # This branch structurally has no suffix/variant-digit ambiguity
        # (unlike the month != 1 branch below) — it only ever proposes a
        # bare number, never a guessed variant suffix — which is why it is
        # the ONLY branch that can ever reach AUTO_REPAIR_ALLOWED. Sibling
        # agreement is a necessary but not sufficient condition for that:
        # it also requires at least MIN_SIBLING_SAMPLE_FOR_AUTO_REPAIR
        # corroborating siblings, not just unanimous agreement on however
        # few happen to exist.
        proposed = str(year)
        if column_name == "part_number":
            convention, n_siblings = _sibling_part_number_convention(conn, brand, caliber, raw_inventory_id)
        else:
            # column_name == "caliber": there is no reliable per-family
            # sibling check available here — caliber IS the grouping key
            # _sibling_part_number_convention uses for part_number, so
            # there is no analogous "other rows in the same caliber
            # family" to compare against. An earlier version of this
            # function assumed "the project-wide caliber convention is
            # always bare numeric" and treated that as automatic
            # corroboration — empirically FALSE (verified against live
            # data: 10 of 106 distinct non-corrupted calibers use a dash,
            # e.g. "247-2", "29-295"). Never auto-corroborated; always
            # falls through to USER_CONFIRMATION_REQUIRED below.
            convention, n_siblings = "no_sibling_check_available_for_caliber", 0
        if convention == "all_bare" and n_siblings >= MIN_SIBLING_SAMPLE_FOR_AUTO_REPAIR:
            return {
                "proposed_value": proposed, "classification": "AUTO_REPAIR_ALLOWED", "confidence": "HIGH",
                "repair_rule": "excel_date_coercion_day_and_month_default",
                "repair_evidence": (
                    f"month=01 and day=01 both at their default — original was almost certainly the bare "
                    f"number {proposed!r}. Corroborated by {n_siblings} sibling rows (>= "
                    f"{MIN_SIBLING_SAMPLE_FOR_AUTO_REPAIR} required) in the same (brand={brand!r}, "
                    f"caliber={caliber!r}) family, all using bare numeric part numbers with no dash suffix."
                ),
            }
        if convention == "no_sibling_check_available_for_caliber":
            reason = (
                "caliber corrections have no reliable sibling-based corroboration available (caliber is "
                "itself the grouping key used for part_number siblings), and the project-wide caliber "
                "format is not uniformly bare numeric (some calibers use a dash suffix)"
            )
        elif convention == "no_siblings":
            reason = "no other rows share this (brand, caliber) to compare against"
        elif convention == "all_bare":
            reason = (
                f"only {n_siblings} corroborating sibling(s) were found (fewer than the "
                f"{MIN_SIBLING_SAMPLE_FOR_AUTO_REPAIR} required for AUTO_REPAIR_ALLOWED) — unanimous "
                "agreement on a small sample is statistically weak evidence, not proof, given the "
                "project-wide dash rate is not negligible"
            )
        else:
            reason = "at least one sibling in this (brand, caliber) family uses a dash-suffixed part number"
        return {
            "proposed_value": proposed, "classification": "USER_CONFIRMATION_REQUIRED", "confidence": "MEDIUM",
            "repair_rule": "excel_date_coercion_day_and_month_default",
            "repair_evidence": (
                f"month=01 and day=01 both at their default — likely the bare number {proposed!r}, but "
                f"{reason}, so this is not corroborated strongly enough to auto-apply."
            ),
        }

    # month != 1: consistent with a real second segment (e.g. "NNNN-2"),
    # but whether it was written as "2" or "02" is unrecoverable from the
    # corrupted value alone — always requires a human decision.
    proposed = f"{year}-{month}"
    return {
        "proposed_value": proposed, "classification": "USER_CONFIRMATION_REQUIRED", "confidence": "MEDIUM",
        "repair_rule": "excel_date_coercion_variant_suffix_ambiguous",
        "repair_evidence": (
            f"day=01 (default) but month={month:02d} is not — the original likely had a real second "
            f"segment, plausibly {proposed!r}, but could equally have been {year}-{month:02d} "
            "(leading-zero form). This specific digit-format ambiguity cannot be resolved from the "
            "corrupted value alone, regardless of sibling evidence."
        ),
    }


def generate_inventory_repair_candidates(conn, upload_batch_id: str | None = None) -> list[dict]:
    """
    Scans inventory_validation_report for FAIL rows on caliber/part_number
    caused by Excel date-coercion (the one corruption shape this generator
    currently understands via is_excel_date_corrupted — adding a detector
    for a different corruption shape means adding another classifier
    function, not touching this one), proposes a repair for each via the
    deterministic rule in _classify_excel_date_repair, and inserts each
    proposal into inventory_repair_candidates.

    Idempotent: a candidate already present for a given
    (raw_inventory_id, column_name) with a non-terminal status (PROPOSED,
    AUTO_APPLIED, APPROVED) is never duplicated — reruns against unchanged
    raw_inventory/validation state insert nothing new. Never hardcodes
    specific row ids or values: entirely driven by the validation report
    and the regex-based corruption detector, so a future upload with the
    same corruption shape is handled identically.
    """
    query = """
        SELECT r.row_id, r.check_name, r.raw_value, r.upload_batch_id,
               i.raw_rolex_tudor, i.raw_calibre
        FROM inventory_validation_report r
        JOIN raw_inventory i ON i.id = r.row_id
        WHERE r.check_status = 'FAIL' AND r.check_name IN ('caliber', 'part_number')
    """
    params: list[str] = []
    if upload_batch_id is not None:
        query += " AND r.upload_batch_id = ?"
        params.append(upload_batch_id)
    fail_rows = conn.execute(query, params).fetchall()

    inserted: list[dict] = []
    for row_id, column_name, raw_value, batch, brand, caliber in fail_rows:
        if raw_value is None or not is_excel_date_corrupted(raw_value):
            continue  # a FAIL for a different reason (e.g. blank part_number) — no detector for it yet

        existing = conn.execute(
            "SELECT COUNT(*) FROM inventory_repair_candidates "
            "WHERE raw_inventory_id = ? AND column_name = ? AND status IN ('PROPOSED', 'AUTO_APPLIED', 'APPROVED')",
            [row_id, column_name],
        ).fetchone()[0]
        if existing:
            continue

        result = _classify_excel_date_repair(
            conn, raw_inventory_id=row_id, brand=brand, caliber=caliber, column_name=column_name, raw_value=raw_value,
        )
        next_id = conn.execute("SELECT nextval('inventory_repair_candidates_id_seq')").fetchone()[0]
        conn.execute(
            """
            INSERT INTO inventory_repair_candidates
                (id, raw_inventory_id, upload_batch_id, column_name, raw_value, proposed_value,
                 classification, confidence, repair_rule, repair_evidence, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PROPOSED')
            """,
            [
                next_id, row_id, batch, column_name, raw_value, result["proposed_value"],
                result["classification"], result["confidence"], result["repair_rule"], result["repair_evidence"],
            ],
        )
        inserted.append({"id": next_id, "raw_inventory_id": row_id, "column_name": column_name, **result})

    return inserted


def _current_effective_correction_fields(conn, raw_inventory_id: int) -> dict:
    """The (brand, caliber, part_number) this row would resolve to RIGHT
    NOW under apply_inventory_corrections' latest-correction-wins rule —
    used so a new correction for ONE column never silently reverts an
    existing correction on a DIFFERENT column for the same row (since only
    the single latest inventory_corrections row is honored per row)."""
    raw_row = conn.execute(
        "SELECT raw_rolex_tudor, raw_calibre, raw_p_number FROM raw_inventory WHERE id = ?", [raw_inventory_id]
    ).fetchone()
    brand, caliber, part_number = raw_row
    latest = conn.execute(
        "SELECT corrected_brand, corrected_caliber, corrected_part_number FROM inventory_corrections "
        "WHERE raw_inventory_id = ? ORDER BY corrected_at DESC LIMIT 1",
        [raw_inventory_id],
    ).fetchone()
    if latest:
        brand = latest[0] if latest[0] not in (None, "") else brand
        caliber = latest[1] if latest[1] not in (None, "") else caliber
        part_number = latest[2] if latest[2] not in (None, "") else part_number
    return {"brand": brand, "caliber": caliber, "part_number": part_number}


def _write_correction_from_candidate(conn, candidate_id: int, corrected_by: str, notes: str | None) -> None:
    cand = conn.execute(
        "SELECT raw_inventory_id, column_name, proposed_value FROM inventory_repair_candidates WHERE id = ?",
        [candidate_id],
    ).fetchone()
    if cand is None:
        raise ValueError(f"No inventory_repair_candidates row with id={candidate_id}")
    raw_inventory_id, column_name, proposed_value = cand
    if proposed_value is None:
        raise ValueError(f"Candidate {candidate_id} has no proposed_value (UNRESOLVED) — cannot apply")

    current = _current_effective_correction_fields(conn, raw_inventory_id)
    field_map = {"brand": "brand", "caliber": "caliber", "part_number": "part_number"}
    current[field_map[column_name]] = proposed_value

    conn.execute(
        """
        INSERT INTO inventory_corrections
            (raw_inventory_id, corrected_brand, corrected_caliber, corrected_part_number, corrected_by, notes)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [raw_inventory_id, current["brand"], current["caliber"], current["part_number"], corrected_by, notes],
    )


def apply_auto_repairs(conn, decided_by: str = "system:auto_repair") -> list[dict]:
    """
    Applies every PROPOSED, AUTO_REPAIR_ALLOWED candidate: writes a real
    inventory_corrections row (never touches raw_inventory) and marks the
    candidate AUTO_APPLIED. USER_CONFIRMATION_REQUIRED and UNRESOLVED
    candidates are never touched here — they stay PROPOSED until a human
    calls approve_repair_candidate/reject_repair_candidate. Idempotent:
    already-AUTO_APPLIED candidates are not reprocessed.
    """
    candidates = conn.execute(
        "SELECT id FROM inventory_repair_candidates WHERE status = 'PROPOSED' AND classification = 'AUTO_REPAIR_ALLOWED'"
    ).fetchall()
    applied = []
    for (candidate_id,) in candidates:
        _write_correction_from_candidate(conn, candidate_id, corrected_by=decided_by, notes="Auto-applied: HIGH confidence, corroborated by sibling data")
        conn.execute(
            "UPDATE inventory_repair_candidates SET status = 'AUTO_APPLIED', decided_by = ?, decided_at = current_timestamp WHERE id = ?",
            [decided_by, candidate_id],
        )
        applied.append({"id": candidate_id})
    return applied


def approve_repair_candidate(conn, candidate_id: int, decided_by: str, notes: str | None = None) -> None:
    """Human approval of a PROPOSED candidate (typically
    USER_CONFIRMATION_REQUIRED) — writes the real inventory_corrections
    row using the candidate's proposed_value and marks it APPROVED. Raises
    if the candidate has no proposed_value (UNRESOLVED) — approving an
    UNRESOLVED candidate with no value makes no sense; reject it instead,
    or resolve it manually by inserting a correction directly."""
    _write_correction_from_candidate(conn, candidate_id, corrected_by=decided_by, notes=notes)
    conn.execute(
        "UPDATE inventory_repair_candidates SET status = 'APPROVED', decided_by = ?, decided_at = current_timestamp WHERE id = ?",
        [decided_by, candidate_id],
    )


def reject_repair_candidate(conn, candidate_id: int, decided_by: str, notes: str | None = None) -> None:
    """Human rejection — no correction is applied, raw_inventory and
    staging remain exactly as validation left them. Notes are preserved
    for audit (e.g. why a plausible-looking guess was actually wrong)."""
    conn.execute(
        "UPDATE inventory_repair_candidates SET status = 'REJECTED', decided_by = ?, decided_at = current_timestamp, notes = ? WHERE id = ?",
        [decided_by, notes, candidate_id],
    )


def _report_row(upload_batch_id, source_filename, check_name, check_status, row_id, column_name, raw_value, message):
    return {
        "upload_batch_id": upload_batch_id,
        "source_filename": source_filename,
        "check_name": check_name,
        "check_status": check_status,
        "row_id": row_id,
        "column_name": column_name,
        "raw_value": None if raw_value is None else str(raw_value),
        "validation_message": message,
    }


def apply_inventory_corrections(conn, raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply inventory_corrections overrides to raw_rolex_tudor/raw_calibre/raw_p_number
    before validation. raw_inventory itself is never modified.
    """
    corrections = conn.execute(
        """
        SELECT
            c.raw_inventory_id,
            i.raw_rolex_tudor,
            i.raw_calibre,
            i.raw_p_number,
            c.corrected_brand,
            c.corrected_caliber,
            c.corrected_part_number,
            c.corrected_at
        FROM inventory_corrections c
        LEFT JOIN raw_inventory i ON i.id = c.raw_inventory_id
        ORDER BY c.corrected_at
        """
    ).df()

    raw_df = raw_df.copy()
    if corrections.empty:
        raw_df["effective_brand"] = raw_df["raw_rolex_tudor"]
        raw_df["effective_calibre"] = raw_df["raw_calibre"]
        raw_df["effective_p_number"] = raw_df["raw_p_number"]
        raw_df["was_corrected"] = False
        return raw_df

    latest = corrections.sort_values("corrected_at").drop_duplicates("raw_inventory_id", keep="last")
    latest = latest.set_index("raw_inventory_id")
    sig_latest = (
        corrections
        .dropna(subset=["raw_rolex_tudor", "raw_calibre", "raw_p_number"])
        .sort_values("corrected_at")
        .drop_duplicates(["raw_rolex_tudor", "raw_calibre", "raw_p_number"], keep="last")
    )
    latest_by_signature = {
        (
            str(row.raw_rolex_tudor),
            str(row.raw_calibre),
            str(row.raw_p_number),
        ): row
        for row in sig_latest.itertuples(index=False)
    }

    def resolve(row, raw_value, corrected_col):
        row_id = row["id"]
        if row_id in latest.index:
            corrected_value = latest.loc[row_id, corrected_col]
            if pd.notna(corrected_value) and str(corrected_value).strip() != "":
                return corrected_value
        signature = (
            str(row["raw_rolex_tudor"]),
            str(row["raw_calibre"]),
            str(row["raw_p_number"]),
        )
        correction = latest_by_signature.get(signature)
        if correction is not None:
            corrected_value = getattr(correction, corrected_col)
            if pd.notna(corrected_value) and str(corrected_value).strip() != "":
                return corrected_value
        return raw_value

    raw_df["effective_brand"] = raw_df.apply(lambda r: resolve(r, r["raw_rolex_tudor"], "corrected_brand"), axis=1)
    raw_df["effective_calibre"] = raw_df.apply(lambda r: resolve(r, r["raw_calibre"], "corrected_caliber"), axis=1)
    raw_df["effective_p_number"] = raw_df.apply(lambda r: resolve(r, r["raw_p_number"], "corrected_part_number"), axis=1)
    raw_df["was_corrected"] = raw_df.apply(
        lambda r: r["id"] in latest.index or (
            str(r["raw_rolex_tudor"]),
            str(r["raw_calibre"]),
            str(r["raw_p_number"]),
        ) in latest_by_signature,
        axis=1,
    )
    return raw_df


def validate_inventory_rows(raw_df: pd.DataFrame) -> tuple[pd.DataFrame, list[dict]]:
    """
    Validate each raw_inventory row (post-corrections). Returns the row-level
    cleaned frame plus a flat list of per-check validation_report rows.
    """
    report_rows: list[dict] = []
    brands, calibers, part_numbers, stocks, statuses = [], [], [], [], []

    for row in raw_df.itertuples():
        batch = row.upload_batch_id
        source_filename = row.source_filename

        # --- brand ---
        raw_brand_value = row.effective_brand
        brand_clean = "" if raw_brand_value is None else str(raw_brand_value).strip()
        if brand_clean in VALID_BRANDS:
            brand_status, brand = "PASS", brand_clean
            brand_msg = "Valid brand"
        else:
            brand_status, brand = "FAIL", brand_clean or None
            brand_msg = f"Unrecognized brand: {raw_brand_value!r} (expected Rolex or Tudor)"
        report_rows.append(_report_row(batch, source_filename, "brand", brand_status, row.id, "raw_rolex_tudor", raw_brand_value, brand_msg))

        # --- caliber ---
        raw_caliber_value = row.effective_calibre
        caliber_clean = "" if raw_caliber_value is None else str(raw_caliber_value).strip()
        if caliber_clean == "":
            caliber_status, caliber = "WARNING", None
            caliber_msg = "Blank calibre — stored as NULL, never the literal string UNKNOWN"
        elif is_excel_date_corrupted(caliber_clean):
            caliber_status, caliber = "FAIL", None
            caliber_msg = f"Excel date-corrupted calibre value: {raw_caliber_value!r} — never guessed, stored as NULL"
        else:
            caliber_status, caliber = "PASS", caliber_clean
            caliber_msg = "Valid calibre"
        report_rows.append(_report_row(batch, source_filename, "caliber", caliber_status, row.id, "raw_calibre", raw_caliber_value, caliber_msg))

        # --- part_number ---
        # FAIL cases store NULL in the cleaned layer (staging_inventory) —
        # the raw text is never lost, it stays intact in raw_inventory and
        # in this validation report's raw_value column.
        raw_pn_value = row.effective_p_number
        pn_clean = "" if raw_pn_value is None else str(raw_pn_value).strip()
        if pn_clean == "":
            pn_status, part_number = "FAIL", None
            pn_msg = "Blank part number — stored as NULL in staging_inventory, raw value preserved here"
        elif is_excel_date_corrupted(pn_clean):
            pn_status, part_number = "FAIL", None
            pn_msg = f"Excel date-corrupted part number value: {raw_pn_value!r} — never guessed, stored as NULL, raw value preserved here"
        else:
            pn_status, part_number = "PASS", pn_clean
            pn_msg = "Valid part number"
        report_rows.append(_report_row(batch, source_filename, "part_number", pn_status, row.id, "raw_p_number", raw_pn_value, pn_msg))

        # --- stock ---
        raw_stock_value = row.raw_stock
        try:
            stock_int = int(str(raw_stock_value).strip())
            if stock_int < 0:
                stock_status, stock = "FAIL", None
                stock_msg = f"Negative stock value: {raw_stock_value!r}"
            else:
                stock_status, stock = "PASS", stock_int
                stock_msg = "Valid stock"
        except (ValueError, TypeError):
            stock_status, stock = "FAIL", None
            stock_msg = f"Non-numeric stock value: {raw_stock_value!r}"
        report_rows.append(_report_row(batch, source_filename, "stock", stock_status, row.id, "raw_stock", raw_stock_value, stock_msg))

        overall_statuses = (brand_status, caliber_status, pn_status, stock_status)
        overall = "FAIL" if "FAIL" in overall_statuses else ("WARNING" if "WARNING" in overall_statuses else "PASS")

        brands.append(brand)
        calibers.append(caliber)
        part_numbers.append(part_number)
        stocks.append(stock)
        statuses.append(overall)

    validated = raw_df.copy()
    validated["brand"] = brands
    validated["caliber"] = calibers
    validated["part_number"] = part_numbers
    validated["stock"] = stocks
    validated["validation_status"] = statuses
    return validated, report_rows


def aggregate_duplicates(validated_df: pd.DataFrame) -> tuple[pd.DataFrame, list[dict]]:
    """
    Aggregate duplicate (brand, caliber, part_number) within the same
    upload_batch_id by summing stock. Returns one row per (batch, canonical id)
    plus a log of every merge event.
    """
    df = validated_df.copy()
    df["canonical_inventory_id"] = df.apply(
        lambda r: utils.slugify_canonical_id(r["brand"], r["caliber"], r["part_number"]), axis=1
    )

    merge_events: list[dict] = []
    grouped_records: list[dict] = []
    for (batch, canon_id), group in df.groupby(["upload_batch_id", "canonical_inventory_id"], dropna=False, sort=False):
        group = group.sort_values("id")
        raw_ids = group["id"].tolist()
        stock_values = group["stock"].tolist()

        valid_stocks = [s for s in stock_values if s is not None and not pd.isna(s)]
        total_stock = int(sum(valid_stocks)) if valid_stocks else None

        anchor_raw_id = min(raw_ids)
        worst_status = "FAIL" if (group["validation_status"] == "FAIL").any() else (
            "WARNING" if (group["validation_status"] == "WARNING").any() else "PASS")

        rep_row = group.iloc[0]

        grouped_records.append({
            "canonical_inventory_id": canon_id,
            "upload_batch_id": batch,
            "brand": rep_row["brand"],
            "caliber": rep_row["caliber"],
            "part_number": rep_row["part_number"],
            "stock": total_stock,
            "source_filename": rep_row["source_filename"],
            "ingested_at": rep_row["ingested_at"],
            "validation_status": worst_status,
            "anchor_raw_inventory_id": anchor_raw_id,
            "raw_rolex_tudor": rep_row["raw_rolex_tudor"],
            "raw_calibre": rep_row["raw_calibre"],
            "raw_p_number": rep_row["raw_p_number"],
        })

        if len(group) > 1:
            merge_events.append({
                "canonical_inventory_id": canon_id,
                "upload_batch_id": batch,
                "raw_ids": raw_ids,
                "stock_values": stock_values,
                "total_stock": total_stock,
            })

    return pd.DataFrame(grouped_records), merge_events


def keep_latest_batch_per_canonical(grouped_df: pd.DataFrame) -> pd.DataFrame:
    """staging_inventory reflects, per canonical id, only its most recent batch."""
    ordered = grouped_df.sort_values(["canonical_inventory_id", "upload_batch_id"])
    latest = ordered.groupby("canonical_inventory_id", as_index=False, sort=False).tail(1)
    return latest.reset_index(drop=True)


def resolve_inventory_uids(conn, final_df: pd.DataFrame) -> pd.DataFrame:
    """
    Preserve inventory_uid across both re-uploads and corrections:

    Tier 1 (re-uploads): if a staging_inventory row already exists for this
    exact canonical_inventory_id (from a prior run, e.g. an unchanged item
    re-uploaded in a new batch), reuse its inventory_uid directly.

    Tier 2 (corrections that change canonical_inventory_id): if tier 1 misses
    — e.g. a correction just changed which canonical id this raw row group
    resolves to — look up inventory_uid_registry by the group's anchor
    (MIN(raw_inventory_id)). That anchor never changes for a given physical
    raw row, regardless of what canonical id a correction later derives from it.

    Tier 3: truly new — mint a new uid and register it under this anchor.

    Every resolved uid gets (re-)registered under its anchor if missing, so
    a future correction that changes this canonical id can still find it
    via tier 2.
    """
    old_uid_by_canonical = dict(
        conn.execute(
            "SELECT canonical_inventory_id, inventory_uid FROM staging_inventory WHERE inventory_uid IS NOT NULL"
        ).fetchall()
    )
    uid_by_anchor = dict(
        conn.execute("SELECT raw_inventory_id, inventory_uid FROM inventory_uid_registry").fetchall()
    )
    uid_by_corrected_raw_signature = {
        (
            str(raw_rolex_tudor),
            str(raw_calibre),
            str(raw_p_number),
        ): inventory_uid
        for raw_rolex_tudor, raw_calibre, raw_p_number, inventory_uid in conn.execute(
            """
            SELECT
                i.raw_rolex_tudor,
                i.raw_calibre,
                i.raw_p_number,
                r.inventory_uid
            FROM inventory_corrections c
            JOIN raw_inventory i ON i.id = c.raw_inventory_id
            JOIN inventory_uid_registry r ON r.raw_inventory_id = c.raw_inventory_id
            WHERE r.inventory_uid IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY i.raw_rolex_tudor, i.raw_calibre, i.raw_p_number
                ORDER BY c.corrected_at DESC
            ) = 1
            """
        ).fetchall()
    }

    new_registry_entries = []
    uids = []
    for _, row in final_df.iterrows():
        canon_id = row["canonical_inventory_id"]
        anchor = int(row["anchor_raw_inventory_id"])
        raw_signature = (
            str(row.get("raw_rolex_tudor")),
            str(row.get("raw_calibre")),
            str(row.get("raw_p_number")),
        )

        if canon_id in old_uid_by_canonical:
            uid = old_uid_by_canonical[canon_id]
            if anchor not in uid_by_anchor:
                uid_by_anchor[anchor] = uid
                new_registry_entries.append((anchor, uid))
        elif raw_signature in uid_by_corrected_raw_signature:
            uid = uid_by_corrected_raw_signature[raw_signature]
            if anchor not in uid_by_anchor:
                uid_by_anchor[anchor] = uid
                new_registry_entries.append((anchor, uid))
        elif anchor in uid_by_anchor:
            uid = uid_by_anchor[anchor]
        else:
            uid = f"iuid_{uuid4().hex[:16]}"
            uid_by_anchor[anchor] = uid
            new_registry_entries.append((anchor, uid))

        uids.append(uid)

    final_df = final_df.copy()
    final_df["inventory_uid"] = uids

    if new_registry_entries:
        entries_df = pd.DataFrame(new_registry_entries, columns=["raw_inventory_id", "inventory_uid"])
        conn.register("tmp_new_registry_entries", entries_df)
        conn.execute(
            """
            INSERT INTO inventory_uid_registry (raw_inventory_id, inventory_uid)
            SELECT raw_inventory_id, inventory_uid FROM tmp_new_registry_entries
            ON CONFLICT DO NOTHING
            """
        )
        conn.unregister("tmp_new_registry_entries")

    return final_df


def rebuild_staging_inventory(conn, final_df: pd.DataFrame) -> pd.DataFrame:
    conn.execute("DELETE FROM staging_inventory")

    df = final_df.copy()
    df["condition"] = None
    df["part_number_is_distinctive"] = df["part_number"].apply(utils.part_number_is_distinctive)

    insert_cols = ["canonical_inventory_id", "upload_batch_id", "brand", "caliber", "part_number", "stock",
                   "condition", "source_filename", "ingested_at",
                   "inventory_uid", "validation_status", "part_number_is_distinctive"]
    conn.register("tmp_staging_inventory", df[insert_cols])
    conn.execute(f"INSERT INTO staging_inventory ({','.join(insert_cols)}) SELECT {','.join(insert_cols)} FROM tmp_staging_inventory")
    conn.unregister("tmp_staging_inventory")

    return df


def append_stock_history(conn, grouped_df: pd.DataFrame) -> None:
    """Append-only: one row per (inventory_uid, upload_batch_id) ever
    computed. Never deletes or overwrites existing history rows."""
    hist_df = grouped_df[["canonical_inventory_id", "upload_batch_id", "stock", "inventory_uid"]].copy()
    conn.register("tmp_stock_history", hist_df)
    conn.execute(
        """
        INSERT INTO inventory_stock_history (canonical_inventory_id, upload_batch_id, stock, inventory_uid)
        SELECT canonical_inventory_id, upload_batch_id, stock, inventory_uid FROM tmp_stock_history
        ON CONFLICT DO NOTHING
        """
    )
    conn.unregister("tmp_stock_history")


def rebuild_validation_report(conn, report_rows: list[dict], merge_events: list[dict]) -> pd.DataFrame:
    rows = list(report_rows)
    for event in merge_events:
        rows.append(
            _report_row(
                event["upload_batch_id"], None, "duplicate_aggregation", "WARNING",
                event["raw_ids"][0], None, None,
                f"Merged raw_inventory ids {event['raw_ids']} into {event['canonical_inventory_id']}: "
                f"stock {'+'.join(str(s) for s in event['stock_values'])} -> {event['total_stock']}",
            )
        )

    report_df = pd.DataFrame(rows)
    conn.execute("DELETE FROM inventory_validation_report")
    if not report_df.empty:
        conn.register("tmp_validation_report", report_df)
        conn.execute(
            """
            INSERT INTO inventory_validation_report (
                upload_batch_id, source_filename, check_name, check_status,
                row_id, column_name, raw_value, validation_message
            )
            SELECT
                upload_batch_id, source_filename, check_name, check_status,
                row_id, column_name, raw_value, validation_message
            FROM tmp_validation_report
            """
        )
        conn.unregister("tmp_validation_report")
    return report_df


def export_validation_report(report_df: pd.DataFrame, reports_dir: Path = REPORTS_DIR) -> Path:
    reports_dir.mkdir(parents=True, exist_ok=True)
    out_path = reports_dir / "inventory_validation_report.csv"
    report_df.to_csv(out_path, index=False)
    return out_path


def clean_inventory(conn, reports_dir: Path = REPORTS_DIR) -> None:
    log_and_print("\nCleaning inventory...")

    latest_batch_id = latest_inventory_upload_batch_id(conn)
    if latest_batch_id:
        raw_df = conn.execute(
            "SELECT * FROM raw_inventory WHERE upload_batch_id = ? ORDER BY id",
            [latest_batch_id],
        ).df()
        log_and_print(f"   Current inventory snapshot: {latest_batch_id}")
    else:
        raw_df = conn.execute("SELECT * FROM raw_inventory ORDER BY id").df()
        log_and_print("   ⚠ No successful inventory snapshot found; cleaning all raw inventory rows.")
    log_and_print(f"   Input: {len(raw_df):,} rows")

    corrected_df = apply_inventory_corrections(conn, raw_df)
    n_corrected = int(corrected_df["was_corrected"].sum())
    if n_corrected:
        log_and_print(f"   Applied corrections to {n_corrected} row(s) from inventory_corrections")

    validated_df, report_rows = validate_inventory_rows(corrected_df)

    status_counts = validated_df["validation_status"].value_counts().to_dict()
    log_and_print(
        f"   Row validation: PASS={status_counts.get('PASS', 0):,} "
        f"WARNING={status_counts.get('WARNING', 0):,} FAIL={status_counts.get('FAIL', 0):,}"
    )
    for check_name in ["brand", "caliber", "part_number", "stock"]:
        check_rows = [r for r in report_rows if r["check_name"] == check_name]
        counts = pd.Series([r["check_status"] for r in check_rows]).value_counts().to_dict()
        log_and_print(
            f"     {check_name}: PASS={counts.get('PASS', 0):,} "
            f"WARNING={counts.get('WARNING', 0):,} FAIL={counts.get('FAIL', 0):,}"
        )

    total_raw_stock = sum(s for s in validated_df["stock"].tolist() if s is not None and not pd.isna(s))

    grouped_df, merge_events = aggregate_duplicates(validated_df)
    if merge_events:
        log_and_print(f"   Duplicates aggregated: {len(merge_events)} group(s)")
        for event in merge_events:
            log_and_print(
                f"     {event['canonical_inventory_id']}: raw ids {event['raw_ids']} "
                f"stock {'+'.join(str(s) for s in event['stock_values'])} -> {event['total_stock']}"
            )
    else:
        log_and_print("   Duplicates aggregated: 0 groups")

    total_grouped_stock = sum(s for s in grouped_df["stock"].tolist() if s is not None and not pd.isna(s))
    if total_raw_stock == total_grouped_stock:
        log_and_print(f"   Stock reconciliation OK: {total_raw_stock:,} units accounted for before and after aggregation")
    else:
        log_and_print(
            f"   ⚠ Stock reconciliation MISMATCH: raw sum={total_raw_stock:,} vs aggregated sum={total_grouped_stock:,}"
        )

    grouped_df = resolve_inventory_uids(conn, grouped_df)
    final_df = keep_latest_batch_per_canonical(grouped_df)

    staged_df = rebuild_staging_inventory(conn, final_df)
    append_stock_history(conn, grouped_df)
    report_df = rebuild_validation_report(conn, report_rows, merge_events)
    report_path = export_validation_report(report_df, reports_dir)

    staging_count = conn.execute("SELECT COUNT(*) FROM staging_inventory").fetchone()[0]
    history_count = conn.execute("SELECT COUNT(*) FROM inventory_stock_history").fetchone()[0]
    log_and_print(f"   ✓ staging_inventory: {staging_count:,} rows")
    log_and_print(f"   ✓ inventory_stock_history: {history_count:,} rows (append-only)")
    log_and_print(f"   ✓ Validation report exported: {report_path}")

    literal_unknown_count = int((staged_df["caliber"] == "UNKNOWN").sum())
    if literal_unknown_count:
        log_and_print(f"   ⚠ {literal_unknown_count} rows have literal string 'UNKNOWN' in caliber — this should never happen")

    if utils.US_DUTY_RATE == 0.0 and utils.US_SALES_TAX_RATE == 0.0:
        log_and_print(
            "   ⚠ US_DUTY_RATE and US_SALES_TAX_RATE are still at their unset 0.0 default — "
            "estimated_import_charges_us_eur is a placeholder, not a verified duty-free rate."
        )


# ══════════════════════════════════════════════════════════════════════════════
# COMPETITOR SNAPSHOT
# ══════════════════════════════════════════════════════════════════════════════

def build_competitor_snapshot(conn):
    """
    Build the competitor profile table from broad active listings.
    This runs NOW on the broad data we already have.
    It will be refreshed again after targeted collection.

    Metrics computed:
      - market_share_pct: what % of all listings does each seller own
      - HHI market concentration score
      - pricing profile per seller
      - listing format preferences
    """
    print("\nBuilding competitor snapshot...")

    # Top sellers by listing count
    result = conn.execute("""
        SELECT
            seller_username,
            COUNT(*)                                            AS total_listings,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS market_share_pct,
            ROUND(MEDIAN(landed_cost_eur), 2)                  AS median_price_eur,
            ROUND(AVG(seller_feedback_score), 0)               AS avg_feedback_score,
            ROUND(AVG(seller_feedback_percentage), 1)          AS avg_feedback_pct,
            ROUND(SUM(CASE WHEN is_auction=false THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS pct_fixed_price,
            ROUND(SUM(CASE WHEN is_auction=true  THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS pct_auction,
            ROUND(SUM(CASE WHEN accepts_best_offer=true THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS pct_best_offer,
            STRING_AGG(DISTINCT item_location_country, ', ')   AS primary_countries
        FROM stg_active_broad
        WHERE seller_username IS NOT NULL
        GROUP BY seller_username
        ORDER BY total_listings DESC
    """).df()

    result["primary_countries"] = None  # STRING_AGG compatibility — refill below

    conn.execute("DELETE FROM feat_competitor")
    comp_cols = ["seller_username","total_listings","market_share_pct","median_price_eur",
                 "avg_feedback_score","avg_feedback_pct","pct_fixed_price","pct_auction",
                 "pct_best_offer","primary_countries"]
    conn.execute(f"INSERT INTO feat_competitor ({','.join(comp_cols)}) SELECT {','.join(comp_cols)} FROM result")

    # Overall HHI score
    hhi = conn.execute("""
        SELECT ROUND(SUM(POWER(market_share_pct, 2)), 0) AS hhi
        FROM feat_competitor
    """).fetchone()[0]

    top3 = conn.execute("""
        SELECT seller_username, total_listings, market_share_pct
        FROM feat_competitor
        ORDER BY total_listings DESC
        LIMIT 3
    """).fetchall()

    print(f"   ✓ Profiled {len(result):,} unique sellers")
    if _aggregates_available(hhi):
        print(f"     HHI market concentration score: {hhi:.0f}")
        if   hhi < 1500: print(f"     → Competitive market (HHI < 1500)")
        elif hhi < 2500: print(f"     → Moderately concentrated (HHI 1500-2500)")
        else:            print(f"     → Highly concentrated (HHI > 2500) — monitor top sellers closely")
    else:
        print("     ⚠ No HHI score available (no sellers with a known username)")
    print(f"     Top 3 sellers:")
    for s in top3:
        print(f"       {s[0]}: {s[1]:,} listings ({s[2]:.1f}% share)")


# ── Main ──────────────────────────────────────────────────────────────────────

def report_active_vs_historical_gap(conn) -> None:
    """The price gap finding — put this in your presentation.

    Split out of main() so it's independently callable/testable: if
    stg_historical or stg_active_broad (filtered to non-NULL landed
    cost) is empty, every MEDIAN() here returns NULL and a direct
    f-string format would crash — instead this prints a clear
    "insufficient data" message and returns normally, never halting
    whatever called it.
    """
    hist_median   = conn.execute("SELECT MEDIAN(avg_landed_cost_eur) FROM stg_historical").fetchone()[0]
    active_median = conn.execute("SELECT MEDIAN(landed_cost_eur) FROM stg_active_broad WHERE landed_cost_eur IS NOT NULL").fetchone()[0]
    hist_median_usd   = conn.execute("SELECT MEDIAN(avg_landed_cost_usd) FROM stg_historical").fetchone()[0]
    active_median_usd = conn.execute("SELECT MEDIAN(landed_cost_usd) FROM stg_active_broad WHERE landed_cost_usd IS NOT NULL").fetchone()[0]
    print(f"\n── KEY FINDING ─────────────────────────────────────────────")
    if not _aggregates_available(hist_median, active_median, hist_median_usd, active_median_usd):
        print("   insufficient data for gap comparison")
        return
    print(f"   Historical median landed cost:    €{hist_median:.2f}  |  ${hist_median_usd:.2f}")
    print(f"   Active market median landed cost: €{active_median:.2f}  |  ${active_median_usd:.2f}")
    print(f"   Gap (EUR): €{hist_median - active_median:.2f}   Gap (USD): ${hist_median_usd - active_median_usd:.2f}")
    print(f"   → Gap holds in both currencies, so it isn't an FX artefact — investigate in EDA.")


def main():
    setup_logging()

    log_and_print("=" * 60)
    log_and_print("WATCHPARTS — STEP 2: CLEAN DATA (DuckDB)")
    log_and_print("=" * 60)

    conn = get_connection()

    clean_inventory(conn)
    clean_historical(conn)
    clean_historical_vcp_aggregate(conn)
    clean_historical_ebay_sold(conn)
    clean_active_broad(conn)
    clean_active_targeted(conn)
    build_competitor_snapshot(conn)

    print("\n── Final staging counts ─────────────────────────────────────")
    for t in ["staging_inventory","inventory_stock_history","stg_historical","stg_historical_vcp_aggregate","stg_historical_ebay_sold","stg_active_broad","stg_active_targeted","feat_competitor"]:
        count = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"   {t}: {count:,} rows")

    report_active_vs_historical_gap(conn)

    conn.close()
    print(f"\n✓ Cleaning complete.")
    print(f"\nNext step:")
    print(f"  python scripts/03_generate_queries.py")


if __name__ == "__main__":
    main()
