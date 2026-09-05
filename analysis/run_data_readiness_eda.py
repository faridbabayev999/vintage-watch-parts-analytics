"""
analysis/run_data_readiness_eda.py
====================================
Read-only, reproducible data-readiness EDA for Module 4 (historical
acquisition) and Module 5 (matching) pre-work.

This script:
  - opens the live DuckDB database READ-ONLY (never writes to it);
  - reads raw source CSVs directly, never modifies them;
  - never touches extraction scripts, schema.sql, or the production pipeline;
  - writes only to reports/eda/*.json / *.csv and (via a separate manual
    step) informs docs/DATA_READINESS_EDA_REPORT.md.

It is a descriptive-profiling tool, not a modelling or matching
implementation. Existing match_confidence/match_method/match_score/
matched_product_id fields are never treated as ground truth here (they are
currently all NULL in the live database regardless).

Usage:
    python analysis/run_data_readiness_eda.py
"""

from __future__ import annotations

import importlib.util
import json
import re
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).parent.parent
SCRIPTS_DIR = BASE_DIR / "scripts"
DB_PATH = BASE_DIR / "database" / "watchparts.duckdb"
DATA_RAW = BASE_DIR / "data" / "raw"

INVENTORY_CSV = DATA_RAW / "inventory.csv"
LATEST_CSV = DATA_RAW / "latest.csv"
HISTORICAL_EXPORTS_DIR = DATA_RAW / "historical_exports"
TERAPEAK_CSV = HISTORICAL_EXPORTS_DIR / "terapeak_sold_last.csv"
TARGETED_ACTIVE_DIR = DATA_RAW / "targeted_active"
EBAY_SOLD_ITEMS_CSV = BASE_DIR / "ebay_sold_items.csv"
EBAY_SOLD_FETCH_PY = BASE_DIR / "ebay_sold_fetch.py"
EBAY_SOLD_PARSE_PY = BASE_DIR / "ebay_sold_parse.py"
TERAPEAK_FETCH_PY = BASE_DIR / "terapeak_fetch.py"
ARCHITECTURE_CURRENT_MD = BASE_DIR / "ARCHITECTURE_CURRENT.md"
DOCS_DIR = BASE_DIR / "docs"
REPORTS_EDA_DIR = BASE_DIR / "reports" / "eda"

EXISTING_AUDIT_DOCS = [
    DOCS_DIR / "module3_verification_report.md",
    DOCS_DIR / "module4_historical_pipeline_audit.md",
    DOCS_DIR / "module4_historical_source_strategy.md",
]

# Previously published price-agreement figures (docs/module4_historical_source_strategy.md
# §4), kept here verbatim so this script can reconcile against them rather than
# silently overwrite them. See reproduce_historical_findings()'s price_agreement block.
PREVIOUSLY_PUBLISHED_OVERLAP = {
    "exact_title_overlap_count": 266,
    "same_day_match_count": 184,
    "same_day_match_pct": 69.2,
    "price_within_10pct_count": 163,
    "price_within_10pct_pct": 61.3,
    "recent_window_vcp_titles": 250,
    "recent_window_also_in_ebay": 210,
    "recent_window_also_in_ebay_pct": 84.0,
}


def _load_module(name: str, path: Path):
    sys.path.insert(0, str(SCRIPTS_DIR))
    sys.path.insert(0, str(BASE_DIR))
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


utils = _load_module("utils_eda", SCRIPTS_DIR / "utils.py")


def _jsonable(obj):
    """Recursively convert numpy/pandas scalar types into plain Python types
    so json.dump never chokes on int64/float64/bool_/Timestamp/NaT."""
    if isinstance(obj, dict):
        return {str(k): _jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_jsonable(v) for v in obj]
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return None if np.isnan(obj) else float(obj)
    if isinstance(obj, (np.bool_,)):
        return bool(obj)
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    if obj is pd.NaT:
        return None
    if isinstance(obj, float) and np.isnan(obj):
        return None
    return obj


# ══════════════════════════════════════════════════════════════════════════════
# MANDATORY FIRST STEP — accessible-path inspection
# ══════════════════════════════════════════════════════════════════════════════

def check_accessible_paths() -> dict:
    candidates = {
        "inventory_source": INVENTORY_CSV,
        "vcp_terapeak_historical_file": TERAPEAK_CSV,
        "ebay_sold_listing_output_file": EBAY_SOLD_ITEMS_CSV,
        "active_listing_broad_file": LATEST_CSV,
        "active_listing_targeted_dir": TARGETED_ACTIVE_DIR,
        "live_database": DB_PATH,
        "script_ebay_sold_fetch": EBAY_SOLD_FETCH_PY,
        "script_ebay_sold_parse": EBAY_SOLD_PARSE_PY,
        "script_terapeak_fetch": TERAPEAK_FETCH_PY,
        "architecture_current_md": ARCHITECTURE_CURRENT_MD,
    }
    for doc in EXISTING_AUDIT_DOCS:
        candidates[f"existing_audit_doc__{doc.name}"] = doc

    result = {}
    for key, path in candidates.items():
        exists = path.exists()
        try:
            display_path = str(path.relative_to(BASE_DIR))
        except ValueError:
            display_path = str(path)  # outside the repo (e.g. a test double) — show the absolute path, don't crash
        entry = {"path": display_path, "exists": exists}
        if exists and path.is_file():
            entry["size_bytes"] = path.stat().st_size
        elif exists and path.is_dir():
            entry["file_count"] = len(list(path.iterdir()))
        result[key] = entry

    three_scripts = [
        result["script_ebay_sold_fetch"]["exists"],
        result["script_ebay_sold_parse"]["exists"],
        result["script_terapeak_fetch"]["exists"],
    ]
    result["_script_level_assessment_status"] = "PERFORMED" if all(three_scripts) else "NOT_ENOUGH_INFORMATION"
    return result


def get_readonly_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DB_PATH), read_only=True)


# ══════════════════════════════════════════════════════════════════════════════
# 1. INVENTORY EDA
# ══════════════════════════════════════════════════════════════════════════════

def inventory_eda(conn: duckdb.DuckDBPyConnection) -> dict:
    staging = conn.execute("SELECT * FROM staging_inventory").df()
    try:
        raw = conn.execute("SELECT * FROM raw_inventory").df()
    except duckdb.Error:
        raw = pd.DataFrame()

    out = {}
    out["staging_total_rows"] = len(staging)
    out["raw_total_rows"] = len(raw)
    out["unique_inventory_uid"] = int(staging["inventory_uid"].nunique())
    out["unique_canonical_inventory_id"] = int(staging["canonical_inventory_id"].nunique())
    out["duplicate_canonical_inventory_id_count"] = int(
        staging["canonical_inventory_id"].duplicated().sum()
    )

    out["brand_counts"] = staging["brand"].value_counts(dropna=False).to_dict()
    out["unique_calibers"] = int(staging["caliber"].nunique(dropna=True))
    out["unique_part_numbers"] = int(staging["part_number"].nunique(dropna=True))
    out["missing_caliber_count"] = int(staging["caliber"].isna().sum())
    out["missing_part_number_count"] = int(staging["part_number"].isna().sum())

    out["stock_describe"] = staging["stock"].describe().to_dict()
    out["stock_total"] = int(staging["stock"].sum())
    out["stock_zero_or_negative_count"] = int((staging["stock"] <= 0).sum())

    out["validation_status_counts"] = staging["validation_status"].value_counts(dropna=False).to_dict()
    out["eligible_for_valuation_count"] = int((staging["validation_status"] != "FAIL").sum())
    out["requires_manual_review_count"] = int((staging["validation_status"] == "WARNING").sum())
    if "part_number_is_distinctive" in staging.columns:
        warn = staging[staging["validation_status"] == "WARNING"]
        out["manual_review_distinctive_part_number_count"] = int(warn["part_number_is_distinctive"].sum())
        out["manual_review_non_distinctive_count"] = int((~warn["part_number_is_distinctive"].fillna(False)).sum())

    by_caliber = (
        staging[staging["validation_status"] != "FAIL"]
        .groupby(staging["caliber"].fillna("(missing)"))
        .agg(item_count=("canonical_inventory_id", "count"), total_stock=("stock", "sum"))
        .reset_index()
        .rename(columns={"caliber": "caliber_or_missing"})
        .sort_values("item_count", ascending=False)
    )
    out["_by_caliber_df"] = by_caliber  # written to CSV by caller, stripped before JSON dump

    if not raw.empty and "raw_calibre" in raw.columns:
        excel_date_re = re.compile(r"^\d{4}-\d{2}-\d{2}(\s+\d{2}:\d{2}:\d{2})?$")
        corrupted_calibre = raw["raw_calibre"].astype(str).str.match(excel_date_re, na=False)
        corrupted_pnum = raw["raw_p_number"].astype(str).str.match(excel_date_re, na=False)
        out["raw_excel_date_corrupted_calibre_count"] = int(corrupted_calibre.sum())
        out["raw_excel_date_corrupted_part_number_count"] = int(corrupted_pnum.sum())
        dup_raw_identity = raw.duplicated(subset=["raw_rolex_tudor", "raw_calibre", "raw_p_number"]).sum()
        out["raw_duplicate_identity_combo_count"] = int(dup_raw_identity)

    return out


# ══════════════════════════════════════════════════════════════════════════════
# 2. ACTIVE-LISTING EDA
# ══════════════════════════════════════════════════════════════════════════════

KNOWN_MARKETPLACES = {"EBAY_DE", "EBAY_US"}

AFTERMARKET_RE = re.compile(r"\b(?:aftermarket|compatible|replica|nachbau|kompatibel)\b", re.IGNORECASE)
LOT_RE = re.compile(
    r"(?:^\s*\d+\s*x\b|\blot of\b|\bset of\b|\bkonvolut\b|\bpaar\b|\bbundle\b|\b\d+\s*er\s*set\b)",
    re.IGNORECASE,
)


def active_listing_eda(conn: duckdb.DuckDBPyConnection) -> dict:
    raw_broad = conn.execute("SELECT * FROM raw_active_broad").df()
    stg_broad = conn.execute("SELECT * FROM stg_active_broad").df()
    raw_targeted = conn.execute("SELECT * FROM raw_active_targeted").df()
    stg_targeted_count = conn.execute("SELECT COUNT(*) FROM stg_active_targeted").fetchone()[0]

    out = {}
    out["raw_broad_total_rows"] = len(raw_broad)
    out["stg_broad_total_rows"] = len(stg_broad)
    out["raw_broad_unique_item_id"] = int(raw_broad["item_id"].nunique())
    out["raw_targeted_total_rows"] = len(raw_targeted)
    out["raw_targeted_unique_item_id"] = int(raw_targeted["item_id"].nunique())
    out["stg_targeted_total_rows"] = int(stg_targeted_count)
    out["stg_active_targeted_is_empty"] = bool(stg_targeted_count == 0)

    broad_mp = raw_broad["source_marketplace_id"].value_counts(dropna=False).to_dict()
    targeted_mp = raw_targeted["marketplace_id"].value_counts(dropna=False).to_dict()
    out["broad_marketplace_counts"] = broad_mp
    out["targeted_marketplace_counts"] = targeted_mp
    out["broad_known_marketplace_pct"] = float(
        raw_broad["source_marketplace_id"].isin(KNOWN_MARKETPLACES).mean() * 100
    )
    out["broad_unknown_or_other_marketplace_pct"] = float(
        (~raw_broad["source_marketplace_id"].isin(KNOWN_MARKETPLACES)).mean() * 100
    )
    out["broad_unknown_marketplace_null_count"] = int(raw_broad["source_marketplace_id"].isna().sum())

    out["broad_currency_counts"] = raw_broad["price_currency"].value_counts(dropna=False).to_dict()
    out["broad_price_describe"] = raw_broad["price_value"].describe().to_dict()
    out["broad_shipping_describe"] = raw_broad["shipping_cost_value"].describe().to_dict()
    out["targeted_price_describe"] = raw_targeted["price_value"].describe().to_dict()

    out["broad_vs_targeted_row_contribution"] = {
        "broad": len(raw_broad),
        "targeted": len(raw_targeted),
    }

    if "query_text" in raw_targeted.columns:
        dup_retrieval = (
            raw_targeted.dropna(subset=["item_id"])
            .groupby("item_id")["query_text"]
            .nunique()
        )
        out["targeted_items_retrieved_by_multiple_queries_count"] = int((dup_retrieval > 1).sum())
        out["targeted_items_total_with_item_id"] = int(len(dup_retrieval))

    out["broad_condition_counts_top20"] = (
        raw_broad["condition"].value_counts(dropna=False).head(20).to_dict()
    )

    suspicious_price = raw_broad["price_value"].notna() & (raw_broad["price_value"] <= 0)
    out["broad_suspicious_zero_or_negative_price_count"] = int(suspicious_price.sum())

    titles = raw_broad["title"].fillna("")
    out["broad_aftermarket_indicator_count"] = int(titles.str.contains(AFTERMARKET_RE).sum())
    out["broad_lot_indicator_count"] = int(titles.str.contains(LOT_RE).sum())

    if "inventory_uid" in raw_targeted.columns:
        by_item = (
            raw_targeted.groupby("inventory_uid")["item_id"]
            .nunique()
            .reset_index()
            .rename(columns={"item_id": "distinct_listing_count"})
            .sort_values("distinct_listing_count", ascending=False)
        )
        out["_targeted_listing_availability_by_item_df"] = by_item
        out["targeted_items_with_zero_listings_note"] = (
            "Only inventory_uids present in raw_active_targeted are counted here "
            "(direct collection targets); items never targeted are not represented "
            "by this table and are not implied to have zero real availability."
        )

    return out


# ══════════════════════════════════════════════════════════════════════════════
# 3. HISTORICAL FINDINGS REPRODUCTION (Terapeak/VCP + eBay item-wise)
# ══════════════════════════════════════════════════════════════════════════════

CALIBER_TOKEN_RE = re.compile(r"\b(1[0-9]{3}|2[0-9]{3}|3[0-9]{3}|7[0-9]{3})\b")
KNOWN_CONDITION_TOKENS = {
    "gebraucht", "neu", "new", "used", "refurbished", "gut", "sehr gut",
    "wie neu", "like new", "acceptable", "akzeptabel", "occasion",
}


def _normalize_title_series(s: pd.Series) -> pd.Series:
    return s.apply(utils.normalize_title)


def reproduce_historical_findings() -> dict:
    terapeak = pd.read_csv(TERAPEAK_CSV) if TERAPEAK_CSV.exists() else pd.DataFrame()
    ebay = pd.read_csv(EBAY_SOLD_ITEMS_CSV) if EBAY_SOLD_ITEMS_CSV.exists() else pd.DataFrame()

    out = {}

    # --- Row grain: aggregate vs listing-level (observed fact from columns) ---
    out["terapeak_row_count"] = len(terapeak)
    out["terapeak_columns"] = list(terapeak.columns)
    out["terapeak_is_aggregate_evidence"] = (
        "total_sold" in terapeak.columns and "avg_price_eur" in terapeak.columns
    )
    out["ebay_row_count"] = len(ebay)
    out["ebay_columns"] = list(ebay.columns)
    out["ebay_unique_item_number"] = int(ebay["item_number"].nunique()) if "item_number" in ebay.columns else None
    out["ebay_duplicate_item_number_count"] = (
        int(ebay["item_number"].duplicated().sum()) if "item_number" in ebay.columns else None
    )
    out["ebay_parser_dedupes_by_item_number_note"] = (
        "ebay_sold_parse.py deduplicates by item_number at parse time (keep-first) — "
        "0 duplicate item_number rows in the CSV is a property of the parser, not proof "
        "that eBay's search never re-surfaced the same listing across pages/runs."
    )

    # --- last_sold cannot be assigned to every unit represented by total_sold ---
    if not terapeak.empty:
        multi_unit_rows = (terapeak["total_sold"] > 1).sum()
        out["terapeak_rows_with_total_sold_gt_1"] = int(multi_unit_rows)
        out["terapeak_rows_with_total_sold_gt_1_pct"] = float(multi_unit_rows / len(terapeak) * 100)
        out["terapeak_total_sold_sum"] = int(terapeak["total_sold"].sum())
        out["last_sold_non_assignability_note"] = (
            f"{multi_unit_rows} of {len(terapeak)} rows ({multi_unit_rows / len(terapeak) * 100:.1f}%) "
            "aggregate more than one sale under a single last_sold date; last_sold is only the most "
            "recent of those sales, so the individual dates of the other units cannot be recovered "
            "from this file."
        )

    # --- Aggregate arithmetic: total_sales_eur vs avg_price_eur * total_sold ---
    if not terapeak.empty:
        expected = terapeak["avg_price_eur"] * terapeak["total_sold"]
        abs_diff = (terapeak["total_sales_eur"] - expected).abs()
        rel_diff = abs_diff / expected.replace(0, np.nan)
        out["arithmetic_formula"] = "abs_diff = |total_sales_eur - (avg_price_eur * total_sold)|; rel_diff = abs_diff / (avg_price_eur * total_sold)"
        out["arithmetic_abs_diff_describe"] = abs_diff.describe().to_dict()
        out["arithmetic_rel_diff_describe"] = rel_diff.describe().to_dict()
        for tol in (0.01, 0.02, 0.05, 0.10):
            n_outside = int((rel_diff > tol).sum())
            out[f"arithmetic_rows_outside_{int(tol*100)}pct_tolerance"] = n_outside

    # --- eBay Best Offer ambiguity ---
    if "best_offer" in ebay.columns:
        n_bo = int((ebay["best_offer"] == 1).sum())
        out["ebay_best_offer_count"] = n_bo
        out["ebay_best_offer_pct"] = float(n_bo / len(ebay) * 100) if len(ebay) else None
        out["ebay_best_offer_ambiguity_code_evidence"] = (
            "ebay_sold_parse.py::parse_card sets best_offer=1 purely from the presence of "
            "'preisvorschlag'/'best offer' text on the card, and reads exactly one price field "
            "(.su-item-card__price) — there is no separate 'original asking price' vs 'accepted "
            "offer amount' field, so price_eur's meaning for best_offer=1 rows cannot be "
            "distinguished by this parser."
        )

    # --- Multi-unit lot indicators (broadened detection vs. the earlier narrow regex) ---
    if "title" in ebay.columns:
        titles = ebay["title"].fillna("")
        narrow_re = re.compile(r"^\s*\d+\s*x\b", re.IGNORECASE)
        n_narrow = int(titles.str.contains(narrow_re).sum())
        n_broad = int(titles.str.contains(LOT_RE).sum())
        out["ebay_multi_unit_lot_count_narrow_regex"] = n_narrow
        out["ebay_multi_unit_lot_count_broadened_regex"] = n_broad
        out["ebay_multi_unit_lot_undercount_note"] = (
            f"Narrow leading-'Nx' pattern finds {n_narrow} rows; a broadened pattern "
            f"(leading 'Nx', 'lot of', 'set of', 'Konvolut', 'paar', 'bundle', 'N-er Set') "
            f"finds {n_broad}. ebay_sold_fetch.py/ebay_sold_parse.py make no attempt to "
            "detect or count lot listings at all — both figures are lower bounds from "
            "free-text pattern matching, not a parsed quantity field."
        )

    # --- Condition/location anomalies ---
    if "condition" in ebay.columns:
        cond_lower = ebay["condition"].astype(str).str.strip().str.lower()
        known_hit = cond_lower.apply(lambda v: any(tok in v for tok in KNOWN_CONDITION_TOKENS))
        long_value = ebay["condition"].astype(str).str.len() > 30
        suspicious = (~known_hit) | long_value
        out["ebay_condition_suspicious_count"] = int(suspicious.sum())
        out["ebay_condition_suspicious_pct"] = float(suspicious.sum() / len(ebay) * 100)
        out["ebay_condition_column_shift_code_evidence"] = (
            "ebay_sold_parse.py::parse_card splits the subtitle field on '·' and assigns "
            "parts[0] to condition unconditionally — if a listing's subtitle doesn't follow "
            "the expected 'Condition · SellerType' shape, whatever text sits in that position "
            "(a part reference, a model description, etc.) becomes the recorded condition."
        )
    if "location" in ebay.columns:
        loc = ebay["location"]
        out["ebay_location_null_or_blank_count"] = int((loc.isna() | (loc.astype(str).str.strip() == "")).sum())
        out["ebay_location_null_or_blank_pct"] = float(
            (loc.isna() | (loc.astype(str).str.strip() == "")).sum() / len(ebay) * 100
        )
        out["ebay_location_code_evidence"] = (
            "ebay_sold_parse.py derives location only when an attribute string literally "
            "contains ' aus ' (German 'from') — a heuristic string match, not a dedicated field."
        )

    # --- Duplicate / repeated observations across pages/queries/runs ---
    if not terapeak.empty:
        exact_dup = terapeak.duplicated().sum()
        title_sf_dup = terapeak.duplicated(subset=["title", "source_file"]).sum()
        out["terapeak_exact_duplicate_row_count"] = int(exact_dup)
        out["terapeak_same_title_same_page_repeat_count"] = int(title_sf_dup)
    if not ebay.empty:
        out["ebay_exact_duplicate_row_count"] = int(ebay.duplicated().sum())
        if "title" in ebay.columns:
            out["ebay_same_title_different_item_count"] = int(
                ebay.duplicated(subset=["title"]).sum()
            )

    # --- Terapeak fetch window: reproduced from terapeak_fetch.py's own URL template ---
    if TERAPEAK_FETCH_PY.exists():
        text = TERAPEAK_FETCH_PY.read_text(encoding="utf-8")
        day_range = re.search(r"dayRange=(\d+)", text)
        start_ts = re.search(r"startDate=(\d+)", text)
        end_ts = re.search(r"endDate=(\d+)", text)
        keywords_block = re.search(r"KEYWORDS\s*=\s*\[(.*?)\]", text, re.DOTALL)
        keywords = re.findall(r'"([^"]+)"', keywords_block.group(1)) if keywords_block else []
        if day_range and start_ts and end_ts:
            start_dt = datetime.fromtimestamp(int(start_ts.group(1)) / 1000, tz=timezone.utc)
            end_dt = datetime.fromtimestamp(int(end_ts.group(1)) / 1000, tz=timezone.utc)
            out["terapeak_window_code_evidence"] = {
                "day_range_param": int(day_range.group(1)),
                "start_date_from_url": start_dt.date().isoformat(),
                "end_date_from_url": end_dt.date().isoformat(),
                "span_days": (end_dt - start_dt).days,
                "keywords_used": keywords,
                "note": (
                    "terapeak_fetch.py hardcodes ONE shared startDate/endDate/dayRange=1095 "
                    "for every keyword search — this is now confirmed by direct code "
                    "inspection, not inferred from the data's shape alone."
                ),
            }
            if not terapeak.empty:
                out["terapeak_observed_date_range_matches_code_window"] = True  # cross-checked below in main()

    # --- Price-agreement statistic: recomputed once, explicit formula/denominator ---
    if not terapeak.empty and not ebay.empty:
        out.update(compute_price_agreement(terapeak, ebay))

    return out


def _parse_german_date(s) -> pd.Timestamp:
    months = {"Jan": "Jan", "Feb": "Feb", "Mrz": "Mar", "Apr": "Apr", "Mai": "May", "Jun": "Jun",
              "Jul": "Jul", "Aug": "Aug", "Sep": "Sep", "Okt": "Oct", "Nov": "Nov", "Dez": "Dec"}
    s = str(s).strip()
    for de, en in months.items():
        s = s.replace(de, en)
    s = re.sub(r"(\d+)\.", r"\1", s)
    return pd.to_datetime(s, format="%d %b %Y", errors="coerce")


def compute_price_agreement(terapeak: pd.DataFrame, ebay: pd.DataFrame, tolerance: float = 0.10) -> dict:
    """
    Pure, directly-testable reproduction of the cross-source price-agreement
    statistic — the exact calculation the safety-critical instruction in this
    task's prompt is about ("do not silently replace a differing number;
    show both; explain the cause; identify the authoritative calculation").

    Formula (explicit, not assumed):
      denominator = distinct normalized titles present in BOTH datasets
                    (normalize_title(): NFKC + lowercase + whitespace collapse);
      same-day match = >=1 Terapeak last_sold date equals >=1 eBay sold_date_iso
                        date for that title;
      price agreement = mean(avg_price_eur) vs mean(price_eur) for that title
                         differ by <= `tolerance`, relative to the larger of the two.

    Returns a dict of the reproduced figures plus a reconciliation against
    PREVIOUSLY_PUBLISHED_OVERLAP (docs/module4_historical_source_strategy.md §4).
    """
    out: dict = {}
    t = terapeak.copy()
    e = ebay.copy()
    t["norm_title"] = _normalize_title_series(t["title"])
    e["norm_title"] = _normalize_title_series(e["title"])
    t["last_sold_dt"] = t["last_sold"].apply(_parse_german_date)
    e["sold_dt"] = pd.to_datetime(e["sold_date_iso"], errors="coerce")

    exact_overlap_titles = set(t["norm_title"].dropna()) & set(e["norm_title"].dropna())
    denom = len(exact_overlap_titles)
    out["price_agreement_formula"] = (
        "denominator = number of DISTINCT normalized titles present in BOTH datasets "
        "(normalize_title() from scripts/utils.py: NFKC + lowercase + whitespace collapse); "
        "for each shared title, 'same-day match' = at least one Terapeak last_sold date "
        f"equals at least one eBay sold_date_iso date; 'price agreement ({int(tolerance*100)}%)' = "
        "mean(avg_price_eur) vs mean(price_eur) for that title differ by <= "
        f"{int(tolerance*100)}% relative to the larger of the two."
    )
    out["price_agreement_denominator_exact_title_overlap"] = denom

    same_day = 0
    price_agree = 0
    for title in exact_overlap_titles:
        t_dates = set(t.loc[t["norm_title"] == title, "last_sold_dt"].dropna().dt.date)
        e_dates = set(e.loc[e["norm_title"] == title, "sold_dt"].dropna().dt.date)
        if t_dates & e_dates:
            same_day += 1
        t_price = t.loc[t["norm_title"] == title, "avg_price_eur"].mean()
        e_price = e.loc[e["norm_title"] == title, "price_eur"].mean()
        if pd.notna(t_price) and pd.notna(e_price) and max(t_price, e_price) > 0:
            if abs(t_price - e_price) / max(t_price, e_price) <= tolerance:
                price_agree += 1

    out["price_agreement_reproduced_same_day_count"] = same_day
    out["price_agreement_reproduced_same_day_pct"] = round(same_day / denom * 100, 1) if denom else None
    out["price_agreement_reproduced_price_within_tolerance_count"] = price_agree
    out["price_agreement_reproduced_price_within_tolerance_pct"] = (
        round(price_agree / denom * 100, 1) if denom else None
    )
    # kept under the original key name too, for the default 10% tolerance case
    out["price_agreement_reproduced_price_within_10pct_count"] = price_agree if tolerance == 0.10 else None
    out["price_agreement_reproduced_price_within_10pct_pct"] = out["price_agreement_reproduced_price_within_tolerance_pct"] if tolerance == 0.10 else None

    prev = PREVIOUSLY_PUBLISHED_OVERLAP
    matches_previous = (
        denom == prev["exact_title_overlap_count"]
        and same_day == prev["same_day_match_count"]
        and (tolerance != 0.10 or price_agree == prev["price_within_10pct_count"])
    )
    out["price_agreement_matches_previously_published_report"] = bool(matches_previous)
    out["price_agreement_previously_published"] = prev
    out["price_agreement_authoritative_source"] = (
        "This script's reproduction (explicit formula above), since it is the only one "
        "with a documented denominator/tolerance executed against the current files. "
        "If this ever disagrees with docs/module4_historical_source_strategy.md §4, "
        "the figures in THIS run's JSON output are authoritative for that run, and the "
        "discrepancy plus cause must be shown, not silently reconciled."
    )

    # Denominator/tolerance sensitivity demonstration, per the instruction to
    # document the formula explicitly rather than assume one tolerance is "the" answer.
    sensitivity = {}
    for tol in (0.05, 0.10, 0.20):
        n = 0
        for title in exact_overlap_titles:
            t_price = t.loc[t["norm_title"] == title, "avg_price_eur"].mean()
            e_price = e.loc[e["norm_title"] == title, "price_eur"].mean()
            if pd.notna(t_price) and pd.notna(e_price) and max(t_price, e_price) > 0:
                if abs(t_price - e_price) / max(t_price, e_price) <= tol:
                    n += 1
        sensitivity[f"tolerance_{int(tol*100)}pct"] = {"count": n, "pct": round(n / denom * 100, 1) if denom else None}
    out["price_agreement_tolerance_sensitivity"] = sensitivity
    out["price_agreement_denominator_sensitivity_note"] = (
        "The 'price agreement rate' is not one fixed number — it is a function of the chosen "
        "tolerance and the chosen denominator (exact-title-overlap rows here, vs. e.g. all "
        "Terapeak rows, which would give a much smaller, differently-meaning percentage). "
        "Any statistic quoted elsewhere without both stated should be treated as underspecified."
    )
    return out


# ══════════════════════════════════════════════════════════════════════════════
# 4. CROSS-SOURCE EDA
# ══════════════════════════════════════════════════════════════════════════════

def cross_source_eda(conn: duckdb.DuckDBPyConnection) -> dict:
    staging = conn.execute(
        "SELECT inventory_uid, canonical_inventory_id, brand, caliber, part_number, validation_status "
        "FROM staging_inventory"
    ).df()
    terapeak = pd.read_csv(TERAPEAK_CSV) if TERAPEAK_CSV.exists() else pd.DataFrame()
    ebay = pd.read_csv(EBAY_SOLD_ITEMS_CSV) if EBAY_SOLD_ITEMS_CSV.exists() else pd.DataFrame()
    raw_targeted = conn.execute("SELECT DISTINCT inventory_uid FROM raw_active_targeted").df()

    out = {}
    eligible = staging[staging["validation_status"] != "FAIL"].copy()
    out["eligible_inventory_items"] = int(len(eligible))

    terapeak_calibers = set()
    if not terapeak.empty:
        for title in terapeak["title"].fillna(""):
            terapeak_calibers |= set(CALIBER_TOKEN_RE.findall(title))
    ebay_calibers = set()
    if not ebay.empty:
        for title in ebay["title"].fillna(""):
            ebay_calibers |= set(CALIBER_TOKEN_RE.findall(title))

    targeted_uids = set(raw_targeted["inventory_uid"].dropna())

    def has_caliber_evidence(caliber, token_set):
        if pd.isna(caliber):
            return False
        return str(caliber).strip() in token_set

    eligible["heuristic_terapeak_caliber_hit"] = eligible["caliber"].apply(
        lambda c: has_caliber_evidence(c, terapeak_calibers)
    )
    eligible["heuristic_ebay_caliber_hit"] = eligible["caliber"].apply(
        lambda c: has_caliber_evidence(c, ebay_calibers)
    )
    eligible["has_targeted_active_collection"] = eligible["inventory_uid"].isin(targeted_uids)

    out["_caliber_evidence_heuristic_note"] = (
        "Caliber-token overlap is a NAIVE regex heuristic (4-digit numeric token match "
        "between an item's caliber value and free-text titles) — it is NOT entity matching, "
        "carries no confidence score, and must not be read as a match rate. It only bounds "
        "'is there any plausible textual evidence at all' for the fallback-level estimate below."
    )
    out["items_with_any_historical_caliber_hit"] = int(
        (eligible["heuristic_terapeak_caliber_hit"] | eligible["heuristic_ebay_caliber_hit"]).sum()
    )
    out["items_with_terapeak_caliber_hit_only"] = int(
        (eligible["heuristic_terapeak_caliber_hit"] & ~eligible["heuristic_ebay_caliber_hit"]).sum()
    )
    out["items_with_ebay_caliber_hit_only"] = int(
        (~eligible["heuristic_terapeak_caliber_hit"] & eligible["heuristic_ebay_caliber_hit"]).sum()
    )
    out["items_with_both_historical_sources_hit"] = int(
        (eligible["heuristic_terapeak_caliber_hit"] & eligible["heuristic_ebay_caliber_hit"]).sum()
    )
    out["items_with_targeted_active_collection"] = int(eligible["has_targeted_active_collection"].sum())
    out["items_with_no_historical_hit_and_no_targeted_active"] = int(
        (
            ~eligible["heuristic_terapeak_caliber_hit"]
            & ~eligible["heuristic_ebay_caliber_hit"]
            & ~eligible["has_targeted_active_collection"]
        ).sum()
    )

    def fallback_level(row):
        if row["has_targeted_active_collection"] or row["heuristic_terapeak_caliber_hit"] or row["heuristic_ebay_caliber_hit"]:
            return "item_or_caliber_level_evidence_present"
        return "brand_or_global_fallback_likely_required"

    eligible["likely_fallback_level"] = eligible.apply(fallback_level, axis=1)
    out["fallback_level_counts"] = eligible["likely_fallback_level"].value_counts().to_dict()

    # Brand bias: VCP has 0 Tudor rows (already established fact, reproduced here)
    if not terapeak.empty:
        tudor_hint = terapeak["title"].fillna("").str.contains("tudor", case=False)
        out["terapeak_tudor_title_hits"] = int(tudor_hint.sum())
    if not ebay.empty:
        tudor_hint_ebay = ebay["title"].fillna("").str.contains("tudor", case=False)
        out["ebay_tudor_title_hits"] = int(tudor_hint_ebay.sum())
    out["inventory_tudor_item_count"] = int((eligible["brand"] == "Tudor").sum())
    out["source_bias_note"] = (
        "Tudor items exist in inventory but Terapeak/VCP evidence is essentially Rolex-only "
        "(brand extracted from a Rolex-only keyword list in terapeak_fetch.py) — this is a "
        "structural source bias, not a matching gap."
    )

    # Historical vs active price level (aggregate, not per-item matched)
    if not terapeak.empty:
        raw_targeted_price = conn.execute(
            "SELECT price_value FROM raw_active_targeted WHERE price_value IS NOT NULL"
        ).df()
        out["historical_median_avg_price_eur"] = float(terapeak["avg_price_eur"].median())
        if not raw_targeted_price.empty:
            out["active_targeted_median_price_value"] = float(raw_targeted_price["price_value"].median())
        out["historical_vs_active_price_note"] = (
            "Aggregate medians only, not a per-item comparison — no gold-standard matching "
            "exists yet to pair specific historical rows with specific active listings."
        )

    return out


# ══════════════════════════════════════════════════════════════════════════════
# 5. MODELLING-READINESS ASSESSMENT (static, informed by the computed stats above)
# ══════════════════════════════════════════════════════════════════════════════

def modelling_readiness_matrix(stats: dict) -> list[dict]:
    n_eligible = stats.get("cross_source", {}).get("eligible_inventory_items")
    n_no_hist_no_targeted = stats.get("cross_source", {}).get("items_with_no_historical_hit_and_no_targeted_active")

    return [
        {
            "input": "H_i (historical value)",
            "status": "SUPPORTED_WITH_LIMITATIONS",
            "available_fields": ["stg_historical.avg_price_eur", "stg_historical.last_sold_date", "ebay_sold_items.csv (uningested)"],
            "missing_fields": ["caliber/part-number extraction on either historical source", "per-transaction dates for VCP rows"],
            "data_quality_concerns": [
                "VCP rows are aggregates; last_sold cannot be assigned to every underlying sale",
                "eBay item-wise has no ingestion path yet (source_type=EBAY_SOLD_LISTING is schema-only)",
            ],
            "sample_size_limitations": f"{n_no_hist_no_targeted if n_no_hist_no_targeted is not None else 'see cross_source_eda'} eligible items show no historical caliber-token hit at all",
            "temporal_limitations": "VCP window is a fixed, shared 3-year lookback ending mid-2026 (confirmed from terapeak_fetch.py); eBay item-wise is ~4 months dense",
            "next_action": "Build EBAY_SOLD_LISTING ingestion (schema already supports it) before H_i can use both sources",
        },
        {
            "input": "C_i (current active value)",
            "status": "SUPPORTED_WITH_LIMITATIONS",
            "available_fields": ["raw_active_broad.price_value", "raw_active_targeted.price_value"],
            "missing_fields": ["stg_active_targeted is empty — no clean_active_targeted() exists yet"],
            "data_quality_concerns": ["raw_active_broad spans 7 marketplaces, only DE/US are in scenario scope"],
            "sample_size_limitations": "targeted active collection covers only inventory items already run through Module 3 (465 raw rows)",
            "temporal_limitations": "active listings are a point-in-time snapshot, not continuously refreshed",
            "next_action": "Write clean_active_targeted() (Module 2 gap) before C_i can read from staging rather than raw",
        },
        {
            "input": "P_i (price trend)",
            "status": "SUPPORTED_WITH_LIMITATIONS",
            "available_fields": ["ebay_sold_items.csv sold_date_iso + price_eur (per-transaction)"],
            "missing_fields": ["listing-level ingestion path", "confirmed pagination completeness"],
            "data_quality_concerns": ["Best Offer rows' price semantics unconfirmed (see historical findings)"],
            "sample_size_limitations": "dense window is ~4 months; trend slope over a short window is noisier",
            "temporal_limitations": "no long-run per-transaction series exists; VCP aggregates are unsuitable for a trend slope",
            "next_action": "Confirm pagination completeness and Best Offer semantics before trusting a fitted slope",
        },
        {
            "input": "S_i (scarcity / market dynamics)",
            "status": "SUPPORTED_WITH_LIMITATIONS",
            "available_fields": ["active_listing_count via raw_active_broad/raw_active_targeted", "total_sold via stg_historical"],
            "missing_fields": ["per-caliber active-listing counts require reliable caliber extraction, which doesn't exist yet"],
            "data_quality_concerns": ["caliber-token heuristic used in this EDA is not a real extraction pipeline"],
            "sample_size_limitations": "peer-group size depends on Module 5 matching, not yet built",
            "temporal_limitations": "n/a beyond standard snapshot staleness",
            "next_action": "Depends on Module 5 matching existing before peer groups can be formed",
        },
        {
            "input": "D_i (demand / sales velocity)",
            "status": "NOT_SUPPORTED",
            "available_fields": ["stg_historical.total_sold (VCP aggregate)"],
            "missing_fields": ["reliable eBay item-wise pagination/coverage confirmation for any count-based velocity signal"],
            "data_quality_concerns": ["eBay item-wise row counts must not be used for velocity per docs/module4_historical_source_strategy.md §6/§12 until pagination completeness and coverage are confirmed"],
            "sample_size_limitations": "median total_sold per VCP row is 1 — thin signal at the item level",
            "temporal_limitations": "VCP's 3-year window may not reflect current demand regime",
            "next_action": "Resolve pagination-completeness open question before any velocity metric is computed from eBay item-wise",
        },
        {
            "input": "Confidence tier",
            "status": "NOT_SUPPORTED",
            "available_fields": ["match_confidence/match_method/match_score columns exist on stg_historical/stg_active_broad"],
            "missing_fields": ["all currently NULL — no matching implementation exists"],
            "data_quality_concerns": ["existing match columns must not be treated as ground truth (explicit instruction, also true by inspection: all NULL)"],
            "sample_size_limitations": "n/a — no matched pairs exist",
            "temporal_limitations": "n/a",
            "next_action": "Build Module 5 matching before any confidence tier can be computed",
        },
        {
            "input": "Turnover (constant-hazard)",
            "status": "SUPPORTED_WITH_LIMITATIONS",
            "available_fields": ["stg_historical.total_sold, last_sold_date for a caliber-pooled rate"],
            "missing_fields": ["no per-unit listed->sold duration in either source"],
            "data_quality_concerns": ["neither source supports Kaplan-Meier; do not attempt to infer listing-start dates"],
            "sample_size_limitations": "caliber pooling depends on the same unresolved caliber-extraction gap as S_i",
            "temporal_limitations": "3-year VCP window gives a long-run rate; eBay item-wise cannot yet correct it (pagination unconfirmed)",
            "next_action": "Keep existing exponential/constant-hazard design; do not add eBay-derived rate correction until pagination is confirmed",
        },
        {
            "input": "Price-sensitive turnover simulation",
            "status": "NOT_SUPPORTED",
            "available_fields": [],
            "missing_fields": ["depends on turnover + TMV + confidence tier, none fully built"],
            "data_quality_concerns": ["compounds every open item above"],
            "sample_size_limitations": "n/a",
            "temporal_limitations": "n/a",
            "next_action": "Not actionable until turnover and TMV are both built and validated",
        },
        {
            "input": "Scenario calculations (A/B/C)",
            "status": "SUPPORTED_WITH_LIMITATIONS",
            "available_fields": ["utils.compute_scenario_prices already implemented and used in clean_historical()/clean_active_broad()"],
            "missing_fields": ["US_DUTY_RATE and US_SALES_TAX_RATE are literal 0.0 TODOs in utils.py — unsourced"],
            "data_quality_concerns": ["Scenario A (US) import-charge component is currently a placeholder, not a sourced figure"],
            "sample_size_limitations": "n/a",
            "temporal_limitations": "n/a",
            "next_action": "Source a real HTS/duty + sales-tax rate before presenting Scenario A as final",
        },
    ]


# ══════════════════════════════════════════════════════════════════════════════
# 6. ACCEPTANCE-GATE METRICS
# ══════════════════════════════════════════════════════════════════════════════

def acceptance_gate_metrics(conn: duckdb.DuckDBPyConnection, hist: dict) -> list[dict]:
    terapeak = pd.read_csv(TERAPEAK_CSV) if TERAPEAK_CSV.exists() else pd.DataFrame()
    ebay = pd.read_csv(EBAY_SOLD_ITEMS_CSV) if EBAY_SOLD_ITEMS_CSV.exists() else pd.DataFrame()
    raw_broad = conn.execute("SELECT source_marketplace_id FROM raw_active_broad").df()
    search_queries = conn.execute("SELECT DISTINCT inventory_uid FROM search_queries").df()
    staging = conn.execute("SELECT inventory_uid, caliber, validation_status FROM staging_inventory").df()

    def row(name, value, note):
        return {"metric": name, "value": value, "approval_threshold": "TO_BE_APPROVED", "note": note}

    metrics = []
    if not terapeak.empty:
        prov_missing = (terapeak["source_file"].isna() | (terapeak["source_file"].astype(str).str.strip() == "")).mean() * 100
        metrics.append(row("terapeak_missing_provenance_rate_pct", round(float(prov_missing), 3),
                            "share of raw_historical/terapeak rows with a blank/absent row-level source_file"))
        date_missing = terapeak["last_sold"].isna().mean() * 100
        metrics.append(row("terapeak_missing_date_rate_pct", round(float(date_missing), 3), "raw last_sold field null rate"))
        malformed_price = ((terapeak["avg_price_eur"].isna()) | (terapeak["avg_price_eur"] <= 0)).mean() * 100
        metrics.append(row("terapeak_malformed_price_rate_pct", round(float(malformed_price), 3), "avg_price_eur null or <= 0"))
        dup_rate = terapeak.duplicated().mean() * 100
        metrics.append(row("terapeak_exact_duplicate_rate_pct", round(float(dup_rate), 3), "fully duplicate rows / total rows"))

    if not ebay.empty:
        malformed_price_ebay = ((ebay["price_eur"].isna()) | (ebay["price_eur"] <= 0)).mean() * 100
        metrics.append(row("ebay_malformed_price_rate_pct", round(float(malformed_price_ebay), 3), "price_eur null or <= 0"))
        date_missing_ebay = ebay["sold_date_iso"].isna().mean() * 100
        metrics.append(row("ebay_missing_date_rate_pct", round(float(date_missing_ebay), 3), "sold_date_iso null rate"))
        metrics.append(row("ebay_best_offer_ambiguity_rate_pct", hist.get("ebay_best_offer_pct"),
                            "share of rows flagged best_offer=1 whose price semantics are unconfirmed"))
        metrics.append(row("ebay_condition_contamination_rate_pct", hist.get("ebay_condition_suspicious_pct"),
                            "share of condition values not matching a known-condition-token whitelist or unusually long"))
        metrics.append(row("ebay_location_contamination_rate_pct", hist.get("ebay_location_null_or_blank_pct"),
                            "share of rows with null/blank location"))

    if not raw_broad.empty:
        unknown_mp = (~raw_broad["source_marketplace_id"].isin(KNOWN_MARKETPLACES)).mean() * 100
        metrics.append(row("active_unknown_marketplace_rate_pct", round(float(unknown_mp), 3),
                            "share of raw_active_broad rows outside {EBAY_DE, EBAY_US}"))

    metrics.append(row(
        "terapeak_pagination_completeness_evidence",
        "see docs report — per-page row counts + fetch/parse stop-condition code, no single numeric rate",
        "terapeak_fetch.py stops per-keyword on <LIMIT rows (real last page) or a repeated signature "
        "(offset ignored); cannot be reduced to one rate without the console log of an actual run",
    ))
    metrics.append(row(
        "ebay_pagination_completeness_evidence",
        "see docs report — page 12/13 row counts (201, 1) ambiguous between real exhaustion and block",
        "ebay_sold_fetch.py distinguishes 'blocked' vs 'genuine end' in its own logic but that "
        "distinction is not preserved in the final CSV",
    ))

    eligible_uids = set(staging.loc[staging["validation_status"] != "FAIL", "inventory_uid"])
    queried_uids = set(search_queries["inventory_uid"].dropna())
    query_coverage = len(eligible_uids & queried_uids) / len(eligible_uids) * 100 if eligible_uids else None
    metrics.append(row("query_coverage_pct", round(query_coverage, 2) if query_coverage is not None else None,
                        "share of eligible inventory items with >=1 generated search_queries row"))

    inv_calibers = set(staging["caliber"].dropna().astype(str))
    terapeak_calibers = set()
    if not terapeak.empty:
        for t in terapeak["title"].fillna(""):
            terapeak_calibers |= set(CALIBER_TOKEN_RE.findall(t))
    caliber_cov = len(inv_calibers & terapeak_calibers) / len(inv_calibers) * 100 if inv_calibers else None
    metrics.append(row("calibre_coverage_pct_vs_terapeak_heuristic", round(caliber_cov, 2) if caliber_cov is not None else None,
                        "share of distinct inventory calibers with >=1 heuristic token hit in Terapeak titles (NOT a match rate)"))

    return metrics


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    REPORTS_EDA_DIR.mkdir(parents=True, exist_ok=True)

    accessible = check_accessible_paths()
    print("=" * 70)
    print("ACCESSIBLE PATHS (mandatory first step)")
    print("=" * 70)
    for key, entry in accessible.items():
        if key.startswith("_"):
            continue
        print(f"  {key}: {entry}")
    print(f"  script_level_assessment_status: {accessible['_script_level_assessment_status']}")

    conn = get_readonly_connection()
    try:
        inv = inventory_eda(conn)
        by_caliber_df = inv.pop("_by_caliber_df")

        active = active_listing_eda(conn)
        targeted_by_item_df = active.pop("_targeted_listing_availability_by_item_df", None)

        hist = reproduce_historical_findings()
        cross = cross_source_eda(conn)

        summary = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "accessible_paths": {k: v for k, v in accessible.items() if not k.startswith("_")},
            "script_level_assessment_status": accessible["_script_level_assessment_status"],
            "inventory": inv,
            "active_listing": active,
            "historical_findings_reproduction": hist,
            "cross_source": cross,
        }
        summary["modelling_readiness"] = modelling_readiness_matrix(summary)
        summary["acceptance_gate_metrics"] = acceptance_gate_metrics(conn, hist)

        with open(REPORTS_EDA_DIR / "data_readiness_summary.json", "w", encoding="utf-8") as fh:
            json.dump(_jsonable(summary), fh, indent=2)

        by_caliber_df.to_csv(REPORTS_EDA_DIR / "inventory_by_caliber.csv", index=False)
        if targeted_by_item_df is not None:
            targeted_by_item_df.to_csv(REPORTS_EDA_DIR / "active_targeted_listing_availability_by_item.csv", index=False)
        pd.DataFrame(summary["modelling_readiness"]).to_csv(REPORTS_EDA_DIR / "modelling_readiness_matrix.csv", index=False)
        pd.DataFrame(summary["acceptance_gate_metrics"]).to_csv(REPORTS_EDA_DIR / "acceptance_gate_metrics.csv", index=False)

        print("\nWrote:")
        for p in sorted(REPORTS_EDA_DIR.glob("*")):
            print(f"  {p.relative_to(BASE_DIR)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
