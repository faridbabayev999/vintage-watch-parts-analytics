"""
16_dashboard_data.py
====================
Module 5 — dashboard DATA-ACCESS layer (skeleton). Pure query layer that binds
the dashboard UI to Implementation A's real pipeline tables. This module holds
NO business logic: it never recomputes TMV, confidence, scarcity, or any price
math — it only READS backend-computed values (tmv_results / turnover_survival /
feat_pricing / match_decisions) and shapes them for display. All valuation math
lives in scripts/13_build_tmv.py; porting any of B's in-dashboard recomputation
(wH·H+wM·C, ×0.79 ask→sold, ×(1+0.10·(S−0.5)), embedded guidance rules) is
explicitly out of scope.

Read-only. WATCHPARTS_DB env-aware. Every consumer/test MUST call
assert_db_target() before querying — a dashboard that silently reads the wrong
database instance produces a false "no data" (or worse, stale) result that a
table/column lineage check alone would not catch.
"""
from __future__ import annotations

import os
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

BASE_DIR = Path(__file__).parent.parent
SCRIPTS_DIR = Path(__file__).parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
DEFAULT_DB_PATH = BASE_DIR / "database" / "watchparts.duckdb"

# Enforced verbatim in UI, schema, and here (Module 4 disclaimer).
TURNOVER_DISCLAIMER = (
    "Estimated selling velocity based on historical sales behaviour. "
    "It is not a price elasticity model and does not estimate price response."
)

# Shown instead of any number when no validated evidence exists.
AWAITING_MESSAGE = "Awaiting validated evidence"


def number_or_zero(value):
    """Return zero for None/NaN/pandas NA without evaluating pd.NA as bool."""
    if value is None:
        return 0
    try:
        if pd.isna(value):
            return 0
    except Exception:
        pass
    return value


def resolve_db_path(db_path: str | os.PathLike | None = None) -> Path:
    if db_path:
        return Path(db_path)
    if os.environ.get("WATCHPARTS_DB"):
        return Path(os.environ["WATCHPARTS_DB"])
    return DEFAULT_DB_PATH


def connect_readonly(db_path: str | os.PathLike | None = None) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(resolve_db_path(db_path)), read_only=True)


def connect_write_retry(
    db_path: str | os.PathLike | None = None,
    *,
    retries: int = 20,
    delay_seconds: float = 0.5,
) -> duckdb.DuckDBPyConnection:
    """Open a write connection, waiting briefly for transient DuckDB locks."""
    db = resolve_db_path(db_path)
    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        try:
            return duckdb.connect(str(db))
        except duckdb.IOException as exc:
            last_exc = exc
            if "lock" not in str(exc).lower() or attempt == retries:
                raise
            time.sleep(delay_seconds)
    raise last_exc or RuntimeError(f"Could not open database {db}")


def assert_db_target(conn: duckdb.DuckDBPyConnection, expected_path: str | os.PathLike) -> str:
    """Assert the connection's main database file is `expected_path`. Returns the
    actual path. Raises AssertionError on mismatch. Standing rule (post live-DB
    incident): every dashboard/lineage query must confirm its DB target first."""
    # rows: (seq, name, file); name is the db alias (file stem), not 'main'.
    rows = conn.execute("PRAGMA database_list").fetchall()
    files = {str(Path(r[2]).resolve()) for r in rows if r[2]}
    exp = str(Path(expected_path).resolve())
    assert exp in files, f"DB target mismatch: connected to {files}, expected {exp!r}"
    return exp


# Client-facing pricing-state labels (2026-07-31 revision). Internal engine
# names (AUTO_CONFIRMED/HIGH_CONFIDENCE/MATCH_CONFIRMED) stay as-is in the
# database/code -- this is a DISPLAY-ONLY mapping, translated at the
# dashboard boundary, never renamed in the governed schema itself.
PRICING_STATE_LABELS = {
    "GOVERNED": "Pricing Ready — Validated",
    "AUTO_CONFIRMED": "Pricing Ready",
    "HIGH_CONFIDENCE": "Pricing Estimate",
    "HIGH_CONFIDENCE_CLIENT": "Pricing Ready",
    "MEDIUM_CONFIDENCE": "Pricing Estimate",
    "HIGH": "High confidence",
    "MEDIUM": "Medium confidence",
    "LOW": "Low confidence",
}


def _contract_ready(conn) -> bool:
    try:
        row = conn.execute("SELECT COUNT(*) FROM dashboard_inventory_pricing").fetchone()
        return bool(row and row[0] > 0)
    except Exception:
        return False


def contract_ready(conn) -> bool:
    """Public launch-time guard for the client dashboard.

    The data module keeps legacy fallbacks for tests and technical inspection,
    but the client dashboard must not open against those joins by accident.
    """
    return _contract_ready(conn)


def dashboard_state(conn) -> dict:
    """READY when EITHER the governed (human-validated) path or the
    algorithmic confidence-tier path has produced priced items -- a client
    dashboard should show a recommendation the moment either path can
    honestly support one; never fabricated when both are empty."""
    def count(sql):
        try:
            return conn.execute(sql).fetchone()[0]
        except Exception:
            return 0
    n_confirmed = count("SELECT COUNT(*) FROM match_decisions WHERE match_status='MATCH_CONFIRMED'")
    n_tmv = count("SELECT COUNT(*) FROM tmv_results")
    n_tmv_algo = count("SELECT COUNT(*) FROM tmv_results_algorithmic")
    n_turn = count("SELECT COUNT(*) FROM turnover_survival")
    ready = (n_tmv + n_tmv_algo) > 0
    return {
        "state": "READY" if ready else "AWAITING_EVIDENCE",
        "message": None if ready else AWAITING_MESSAGE,
        "n_confirmed": n_confirmed, "n_tmv": n_tmv, "n_tmv_algorithmic": n_tmv_algo, "n_turnover": n_turn,
    }


def latest_usd_eur_rate(conn):
    """FX for display. FIX: reads ref_exchange_rates (B queried a non-existent
    ref_fx_rates with a wrong rate_date column). Returns rate or None."""
    try:
        row = conn.execute(
            """SELECT rate FROM ref_exchange_rates
               WHERE from_currency='USD' AND to_currency='EUR'
               ORDER BY valid_date DESC LIMIT 1"""
        ).fetchone()
        return float(row[0]) if row else None
    except Exception:
        return None


def market_summary(conn) -> dict:
    """Data-availability by market (always-honest context, not a valuation).
    Derived from the raw evidence pool, so it is informative even in the
    AWAITING state."""
    def one(sql):
        try:
            return conn.execute(sql).fetchone()[0] or 0
        except Exception:
            return 0
    return {
        "active_eu": one("SELECT COUNT(*) FROM stg_active_targeted WHERE marketplace='EBAY_DE'"),
        "active_us": one("SELECT COUNT(*) FROM stg_active_targeted WHERE marketplace='EBAY_US'"),
        "sold_eur": one("SELECT COUNT(*) FROM stg_historical_ebay_sold WHERE currency_original='EUR'"),
        "sold_usd": one("SELECT COUNT(*) FROM stg_historical_ebay_sold WHERE currency_original='USD'"),
    }


def data_freshness(conn) -> dict:
    def one(sql):
        try:
            return conn.execute(sql).fetchone()[0]
        except Exception:
            return None
    return {
        "tmv_computed_at": one("SELECT MAX(computed_at) FROM tmv_results"),
        "latest_sold_date": one("SELECT MAX(sold_date) FROM stg_historical_ebay_sold"),
        "latest_active_fetch": one("SELECT MAX(fetched_at) FROM stg_active_targeted"),
    }


def overview_summary(conn) -> dict:
    """Phase 9 overview cards: total inventory, evidence coverage %, TMV
    coverage %, average recommended price and turnover -- ONLY averaged over
    items that actually have a TMV row (never padded with fabricated zeros
    for items awaiting evidence). Every ratio is computed against the real
    collection-eligible inventory count (validation_status <> 'FAIL'), the
    same denominator used throughout this project's own reporting -- never
    a stricter PASS-only subset presented as if it were the full scope."""
    def one(sql, default=0):
        try:
            row = conn.execute(sql).fetchone()
            return row[0] if row and row[0] is not None else default
        except Exception:
            return default

    if _contract_ready(conn):
        active_scope = "COALESCE(stock_quantity, 0) > 0"
        total = one(f"SELECT COUNT(*) FROM dashboard_inventory_pricing WHERE {active_scope}")
        total_physical_stock = one(
            f"SELECT SUM(stock_quantity) FROM dashboard_inventory_pricing WHERE {active_scope}"
        )
        active_covered = one(
            f"SELECT COUNT(*) FROM dashboard_inventory_pricing WHERE {active_scope} AND active_evidence_count > 0"
        )
        hist_covered = one(
            f"SELECT COUNT(*) FROM dashboard_inventory_pricing WHERE {active_scope} AND historical_evidence_count > 0"
        )
        tmv_count = one(
            f"SELECT COUNT(*) FROM dashboard_inventory_pricing WHERE {active_scope} AND pricing_status='PRICED'"
        )
        avg_price = one(
            f"SELECT AVG(recommended_price_eur) FROM dashboard_inventory_pricing WHERE {active_scope} AND pricing_status='PRICED'",
            default=None,
        )
        avg_turnover = one(
            "SELECT AVG(median_days_to_sell) FROM dashboard_inventory_pricing "
            f"WHERE {active_scope} AND turnover_evidence_status='SUPPORTED'",
            default=None,
        )

        def pct(n, d):
            return round(100.0 * n / d, 1) if d else 0.0

        return {
            "total_inventory": total,
            "total_physical_stock": total_physical_stock,
            "active_coverage_n": active_covered, "active_coverage_pct": pct(active_covered, total),
            "historical_coverage_n": hist_covered, "historical_coverage_pct": pct(hist_covered, total),
            "tmv_available_n": tmv_count, "tmv_available_pct": pct(tmv_count, total),
            "avg_recommended_price_eur": round(avg_price, 2) if avg_price is not None else None,
            "avg_turnover_days": round(avg_turnover, 1) if avg_turnover is not None else None,
        }

    total = one("SELECT COUNT(*) FROM staging_inventory WHERE validation_status <> 'FAIL'")
    total_physical_stock = one(
        "SELECT SUM(stock) FROM staging_inventory WHERE validation_status <> 'FAIL'"
    )
    active_covered = one(
        "SELECT COUNT(DISTINCT si.inventory_uid) FROM staging_inventory si "
        "JOIN stg_active_targeted a ON a.inventory_uid = si.inventory_uid "
        "WHERE si.validation_status <> 'FAIL'"
    )
    hist_covered = one(
        "SELECT COUNT(DISTINCT inventory_uid) FROM ("
        "  SELECT c.inventory_uid FROM match_candidates_ebay_sold c "
        "  JOIN staging_inventory si ON si.inventory_uid = c.inventory_uid WHERE si.validation_status <> 'FAIL'"
        "  UNION"
        "  SELECT c.inventory_uid FROM match_candidates_vcp c "
        "  JOIN staging_inventory si ON si.inventory_uid = c.inventory_uid WHERE si.validation_status <> 'FAIL'"
        ")"
    )
    # Priced items = governed (tmv_results) UNION algorithmic (tmv_results_algorithmic),
    # governed taking precedence for any item in both -- same rule as load_items().
    # A bare "SELECT COUNT(*) FROM tmv_results" here undercounted real coverage
    # (showed 0/728 while the item cards below listed 575 priced items) --
    # found during dashboard client-review, 2026-07-31.
    tmv_count = one("""
        SELECT COUNT(*) FROM (
            SELECT canonical_inventory_id FROM tmv_results
            UNION
            SELECT canonical_inventory_id FROM tmv_results_algorithmic
        )
    """)
    avg_price = one("""
        SELECT AVG(tmv_eur) FROM (
            SELECT canonical_inventory_id, tmv_eur FROM tmv_results
            UNION
            SELECT canonical_inventory_id, tmv_eur FROM tmv_results_algorithmic
            WHERE canonical_inventory_id NOT IN (SELECT canonical_inventory_id FROM tmv_results)
        )
    """, default=None)
    avg_turnover = one("""
        SELECT AVG(median_days_to_sell) FROM (
            SELECT canonical_inventory_id, median_days_to_sell FROM turnover_survival
            UNION
            SELECT canonical_inventory_id, median_days_to_sell FROM turnover_survival_algorithmic
            WHERE canonical_inventory_id NOT IN (SELECT canonical_inventory_id FROM turnover_survival)
        )
    """, default=None)

    def pct(n, d):
        return round(100.0 * n / d, 1) if d else 0.0

    return {
        "total_inventory": total,
        "total_physical_stock": total_physical_stock,
        "active_coverage_n": active_covered, "active_coverage_pct": pct(active_covered, total),
        "historical_coverage_n": hist_covered, "historical_coverage_pct": pct(hist_covered, total),
        "tmv_available_n": tmv_count, "tmv_available_pct": pct(tmv_count, total),
        "avg_recommended_price_eur": round(avg_price, 2) if avg_price is not None else None,
        "avg_turnover_days": round(avg_turnover, 1) if avg_turnover is not None else None,
    }


def evidence_depth(conn) -> dict:
    """confirmed evidence count per canonical_inventory_id (0 until gate opens)."""
    try:
        rows = conn.execute("""
            SELECT si.canonical_inventory_id, COUNT(*) AS n
            FROM match_decisions md
            JOIN staging_inventory si ON md.inventory_uid = si.inventory_uid
            WHERE md.match_status='MATCH_CONFIRMED'
            GROUP BY 1
        """).fetchall()
        return {r[0]: r[1] for r in rows}
    except Exception:
        return {}


def _market_evidence_counts(conn) -> dict:
    """active listing count + historical sold-candidate count per
    inventory item -- the client-facing 'Market evidence' figure. Active
    listings link directly by inventory_uid; historical sold evidence has
    no such direct column (docs/MATCHING_AUTOMATION_IMPLEMENTATION_REPORT.md)
    and must go through the candidate tables instead -- the same join
    pattern already verified correct for historical_coverage_report.csv."""
    try:
        rows = conn.execute("""
            SELECT si.canonical_inventory_id,
                   COUNT(DISTINCT a.id) AS active_n,
                   COUNT(DISTINCT c.ebay_sold_raw_id) AS sold_n
            FROM staging_inventory si
            LEFT JOIN stg_active_targeted a ON a.inventory_uid = si.inventory_uid
            LEFT JOIN match_candidates_ebay_sold c ON c.inventory_uid = si.inventory_uid
            GROUP BY 1
        """).fetchall()
        return {r[0]: {"active": r[1], "sold": r[2]} for r in rows}
    except Exception:
        return {}


def load_items(conn) -> list[dict]:
    """One display row per priced item -- backend values only, no
    recomputation. Merges TWO sources, governed taking precedence per item:
      1. tmv_results (human-validated MATCH_CONFIRMED evidence) -- labeled
         'Pricing Ready — Validated'.
      2. tmv_results_algorithmic (AUTO_CONFIRMED/HIGH_CONFIDENCE evidence,
         scripts/21_evidence_confidence_engine.py) -- labeled 'Pricing
         Ready' / 'Pricing Estimate'.
    An item present in BOTH is shown once, from the governed source only
    (the stronger evidence basis). Empty list when neither source has any
    rows (AWAITING state)."""
    state = dashboard_state(conn)
    if state["state"] != "READY":
        return []
    if _contract_ready(conn):
        rows = conn.execute("""
            SELECT *
            FROM dashboard_inventory_pricing
            WHERE pricing_status='PRICED'
              AND COALESCE(stock_quantity, 0) > 0
            ORDER BY recommended_price_eur DESC NULLS LAST
        """).fetchall()
        cols = [d[0] for d in conn.description]
        out = []
        for row_tuple in rows:
            r = dict(zip(cols, row_tuple))
            confidence = r.get("pricing_confidence")
            pricing_state = confidence
            out.append({
                "canonical_inventory_id": r["canonical_inventory_id"],
                "brand": r.get("brand"), "caliber": r.get("caliber"),
                "part_number": r.get("part_number"), "stock": r.get("stock_quantity"),
                "tmv_eur": r.get("recommended_price_eur"),
                "tmv_low_eur": None, "tmv_high_eur": None,
                "confidence_tier": confidence,
                "pricing_state": pricing_state,
                "pricing_state_label": r.get("confidence_label") or PRICING_STATE_LABELS.get(pricing_state, confidence),
                "evidence_depth": r.get("total_unique_evidence_count") or 0,
                "market_evidence_active": r.get("active_evidence_count") or 0,
                "market_evidence_sold": r.get("historical_evidence_count") or 0,
                "median_days_to_sell": r.get("median_days_to_sell"),
                "sell_time_display": r.get("sell_time_display"),
                "turnover_evidence_status": r.get("turnover_evidence_status"),
                "turnover_confidence": r.get("turnover_confidence"),
                "turnover_method": r.get("turnover_method"),
                "prob_sell_30d": r.get("probability_sell_30d"),
                "prob_sell_90d": r.get("probability_sell_90d"),
                "turnover_note": TURNOVER_DISCLAIMER,
                "turnover_bucket_forecast": _contract_bucket_forecast(r),
                "historical_value_eur": r.get("historical_value_h"),
                "current_value_eur": r.get("current_value_c"),
                "demand_index": r.get("demand_index_d"),
                "market_dynamics": r.get("scarcity_score_s"),
                "price_trend": r.get("price_trend_p"),
                "demand_adjustment_eur": r.get("demand_adjustment_eur"),
                "recommendation_reason": r.get("recommendation_reason"),
                "virtual_price_eur": r.get("virtual_price_eur"),
                "germany_price_eur": r.get("germany_price_eur"),
                "us_price_eur": r.get("us_price_eur"),
            })
        return out
    depth = evidence_depth(conn)
    evidence_counts = _market_evidence_counts(conn)

    governed = conn.execute("""
        SELECT t.canonical_inventory_id, f.brand, f.caliber, f.part_number, f.stock,
               t.tmv_eur, t.tmv_low_eur, t.tmv_high_eur, t.confidence_tier,
               s.median_days_to_sell, s.probability_sell_30d, s.probability_sell_90d,
               s.turnover_bucket_forecast,
               f.historical_value_eur, f.current_value_eur, f.scarcity_score,
               f.recommendation_reason,
               d.recency_score AS demand_index, d.price_trend_slope AS price_trend
        FROM tmv_results t
        LEFT JOIN feat_pricing f USING (canonical_inventory_id)
        LEFT JOIN feat_demand d USING (canonical_inventory_id)
        LEFT JOIN turnover_survival s USING (canonical_inventory_id)
        ORDER BY t.tmv_eur DESC NULLS LAST
    """).df()

    governed_ids = set(governed["canonical_inventory_id"]) if not governed.empty else set()

    try:
        algo = conn.execute("""
            SELECT t.canonical_inventory_id, si.brand, si.caliber, si.part_number, si.stock,
                   t.tmv_eur, t.tmv_low_eur, t.tmv_high_eur, t.confidence_tier,
                   s.median_days_to_sell, s.probability_sell_30d, s.probability_sell_90d,
                   s.turnover_bucket_forecast,
                   t.historical_value_eur, t.current_value_eur,
                   t.scarcity_score, t.price_trend, t.demand_index,
                   t.recommendation_reason
            FROM tmv_results_algorithmic t
            JOIN staging_inventory si ON si.canonical_inventory_id = t.canonical_inventory_id
            LEFT JOIN turnover_survival_algorithmic s ON s.canonical_inventory_id = t.canonical_inventory_id
            ORDER BY t.tmv_eur DESC NULLS LAST
        """).df()
    except Exception:
        import pandas as _pd
        algo = _pd.DataFrame()

    out = []
    for r in governed.itertuples(index=False):
        cid = r.canonical_inventory_id
        ev = evidence_counts.get(cid, {"active": 0, "sold": 0})
        out.append({
            "canonical_inventory_id": cid,
            "brand": getattr(r, "brand", None), "caliber": getattr(r, "caliber", None),
            "part_number": getattr(r, "part_number", None), "stock": getattr(r, "stock", None),
            "tmv_eur": r.tmv_eur, "tmv_low_eur": r.tmv_low_eur, "tmv_high_eur": r.tmv_high_eur,
            "confidence_tier": r.confidence_tier,
            "pricing_state": "GOVERNED", "pricing_state_label": PRICING_STATE_LABELS["GOVERNED"],
            "evidence_depth": depth.get(cid, 0),
            "market_evidence_active": ev["active"], "market_evidence_sold": ev["sold"],
            "median_days_to_sell": r.median_days_to_sell,
            "prob_sell_30d": r.probability_sell_30d, "prob_sell_90d": r.probability_sell_90d,
            "turnover_note": TURNOVER_DISCLAIMER,
            "turnover_bucket_forecast": getattr(r, "turnover_bucket_forecast", None),
            "historical_value_eur": r.historical_value_eur,
            "current_value_eur": r.current_value_eur,
            "demand_index": r.demand_index,
            "market_dynamics": r.scarcity_score,
            "price_trend": r.price_trend,
            "recommendation_reason": getattr(r, "recommendation_reason", None),
        })

    if not algo.empty:
        for r in algo.itertuples(index=False):
            cid = r.canonical_inventory_id
            if cid in governed_ids:
                continue  # governed evidence wins; never show the same item twice
            ev = evidence_counts.get(cid, {"active": 0, "sold": 0})
            tier = r.confidence_tier
            out.append({
                "canonical_inventory_id": cid,
                "brand": getattr(r, "brand", None), "caliber": getattr(r, "caliber", None),
                "part_number": getattr(r, "part_number", None), "stock": getattr(r, "stock", None),
                "tmv_eur": r.tmv_eur, "tmv_low_eur": r.tmv_low_eur, "tmv_high_eur": r.tmv_high_eur,
                "confidence_tier": tier,
                "pricing_state": tier, "pricing_state_label": PRICING_STATE_LABELS.get(tier, tier),
                "evidence_depth": 0,
                "market_evidence_active": ev["active"], "market_evidence_sold": ev["sold"],
                "median_days_to_sell": r.median_days_to_sell,
                "prob_sell_30d": r.probability_sell_30d, "prob_sell_90d": r.probability_sell_90d,
                "turnover_note": TURNOVER_DISCLAIMER,
                "turnover_bucket_forecast": getattr(r, "turnover_bucket_forecast", None),
                "historical_value_eur": r.historical_value_eur,
                "current_value_eur": r.current_value_eur,
                # Real values, read verbatim -- already computed by
                # 13_build_tmv.py and already baked into tmv_eur; not
                # fabricated, not defaulted to 0 (2026-08-01 fix).
                "demand_index": getattr(r, "demand_index", None),
                "market_dynamics": getattr(r, "scarcity_score", None),
                "price_trend": getattr(r, "price_trend", None),
                "recommendation_reason": getattr(r, "recommendation_reason", None),
            })

    out.sort(key=lambda d: (d["tmv_eur"] is None, -(d["tmv_eur"] or 0)))
    return out


def load_unpriced_items(conn) -> list[dict]:
    """Every eligible inventory item that received NO price -- with a
    specific, never-blank reason. Previously these items simply did not
    appear anywhere in the dashboard (found during client-review,
    2026-07-31): a client had no way to tell 'no evidence yet' apart from
    'this item doesn't exist in my inventory'. Computed live from the same
    tables load_items() reads, not a cached report file."""
    try:
        rows = conn.execute("""
            WITH priced AS (
                SELECT canonical_inventory_id FROM tmv_results
                UNION SELECT canonical_inventory_id FROM tmv_results_algorithmic
            ),
            classified AS (
                SELECT inventory_uid,
                       MAX(CASE WHEN confidence_tier='MEDIUM_CONFIDENCE' THEN 1 ELSE 0 END) AS has_medium,
                       MAX(CASE WHEN confidence_tier='LOW_CONFIDENCE' THEN 1 ELSE 0 END) AS has_low,
                       MAX(CASE WHEN confidence_tier='REJECTED' THEN 1 ELSE 0 END) AS has_rejected,
                       COUNT(*) AS n_classified
                FROM evidence_confidence_classification GROUP BY 1
            )
            SELECT si.canonical_inventory_id, si.brand, si.caliber, si.part_number,
                   si.stock,
                   COALESCE(c.n_classified, 0) AS n_classified,
                   COALESCE(c.has_medium, 0) AS has_medium,
                   COALESCE(c.has_low, 0) AS has_low,
                   COALESCE(c.has_rejected, 0) AS has_rejected
            FROM staging_inventory si
            LEFT JOIN classified c ON c.inventory_uid = si.inventory_uid
            WHERE si.validation_status <> 'FAIL'
              AND si.canonical_inventory_id NOT IN (SELECT canonical_inventory_id FROM priced)
            ORDER BY si.canonical_inventory_id
        """).fetchall()
    except Exception:
        return []

    out = []
    for cid, brand, caliber, part_number, stock, n_classified, has_medium, has_low, has_rejected in rows:
        if n_classified == 0:
            reason = "No matching market listings found for this part yet."
        elif has_medium:
            reason = "Some market evidence found, but not specific enough to confirm a price."
        elif has_low:
            reason = "Weak market evidence found — not enough to recommend a price."
        elif has_rejected:
            reason = "Matched listings did not pass brand/identity checks."
        else:
            reason = "Insufficient evidence to recommend a price."
        out.append({
            "canonical_inventory_id": cid, "brand": brand, "caliber": caliber,
            "part_number": part_number, "stock": stock, "reason": reason,
        })
    return out


def item_scenarios(conn, tmv_eur: float, as_of=None) -> dict:
    """Thin wrapper around 17_scenario_engine.compute_scenarios(). Never
    recomputes the scenario math here -- delegates entirely. On missing
    reference-data configuration, returns a visible error dict rather than
    letting the exception propagate and blank the whole dashboard, and never
    silently substitutes a 0/fabricated rate."""
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "scenario_engine_dd", Path(__file__).parent / "17_scenario_engine.py")
    engine = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(engine)
    try:
        return {"ok": True, "scenarios": engine.compute_scenarios(conn, tmv_eur, as_of=as_of)}
    except engine.ConfigurationError as exc:
        return {"ok": False, "error": str(exc)}


def price_time_table(conn, tmv_eur: float, base_days: float, pct_points=(-10, 0, 10)) -> list[dict]:
    """Thin wrapper around 17_scenario_engine.simulate_price_time() -- never
    recomputes the elasticity math here, just formats a small comparison
    table for the dashboard (e.g. -10%/0%/+10% price points)."""
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "scenario_engine_pt", Path(__file__).parent / "17_scenario_engine.py")
    engine = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(engine)
    rows = []
    for pct in pct_points:
        price = round(tmv_eur * (1 + pct / 100), 2)
        r = engine.simulate_price_time(conn, tmv_eur=tmv_eur, base_days=base_days, scenario_price_eur=price)
        rows.append({"pct": pct, "price_eur": price, **r})
    return rows


# ═══════════════════════════════════════════════════════════════════════════
# CLIENT PRODUCT LAYER (2026-07-31) — Portfolio Overview / Pricing Workspace /
# Inventory Management / Portfolio Analytics. Every function here is a READ
# query or a pure aggregation/bucketing of values load_items() already
# returns -- NONE of them compute a new price, confidence, or turnover
# figure. All business math still lives exclusively in scripts/13_build_tmv.py
# and scripts/17_scenario_engine.py.
# ═══════════════════════════════════════════════════════════════════════════

SELL_TIME_BUCKETS = [("Fast", 0, 30), ("Medium", 31, 183), ("Slow", 184, float("inf"))]


def portfolio_overview(conn) -> dict:
    """Landing-page cards: total inventory, priced coverage, portfolio
    value (sum of already-computed recommended prices, never a new
    valuation), average price, dominant brands among priced items, and a
    plain-language sell-time overview. Reuses overview_summary() + load_items()
    -- no separate SQL business logic."""
    ov = overview_summary(conn)
    items = load_items(conn)
    portfolio_value = round(
        sum(number_or_zero(it.get("tmv_eur")) * number_or_zero(it.get("stock")) for it in items),
        2,
    ) if items else None

    brand_counts: dict[str, int] = {}
    for it in items:
        b = it.get("brand") or "Unknown"
        brand_counts[b] = brand_counts.get(b, 0) + 1
    top_brands_list = sorted(brand_counts.items(), key=lambda kv: -kv[1])[:3]

    sell_days = [it["median_days_to_sell"] for it in items if it["median_days_to_sell"] is not None]
    sell_time_label = "—"
    if sell_days:
        sell_days_sorted = sorted(sell_days)
        median_days = sell_days_sorted[len(sell_days_sorted) // 2]
        for label, lo, hi in SELL_TIME_BUCKETS:
            if lo <= median_days <= hi:
                sell_time_label = label
                break

    return {
        "total_inventory": ov["total_inventory"],
        "total_physical_stock": ov.get("total_physical_stock"),
        "priced_n": ov["tmv_available_n"], "priced_pct": ov["tmv_available_pct"],
        "portfolio_value_eur": portfolio_value,
        "avg_recommended_price_eur": ov["avg_recommended_price_eur"],
        "top_brands": top_brands_list,
        "sell_time_overview": sell_time_label,
    }


def search_priced_items(conn, query: str = "") -> list[dict]:
    """Case-insensitive substring match on brand/caliber/part_number/
    canonical_inventory_id across the already-loaded priced items. No SQL
    recomputation -- filters load_items()'s output in Python (575 rows,
    trivially fast)."""
    items = load_items(conn)
    if not query or not query.strip():
        return items
    q = query.strip().lower()
    def matches(it):
        fields = [it.get("canonical_inventory_id"), it.get("brand"), it.get("caliber"), it.get("part_number")]
        return any(q in str(f).lower() for f in fields if f is not None)
    return [it for it in items if matches(it)]


def get_item_by_id(conn, canonical_inventory_id: str) -> dict | None:
    """Single item lookup for the detail page -- delegates to load_items(),
    never re-queries tmv_results*/turnover_survival* independently (keeps
    exactly one code path for 'what does this item's price/tier look like')."""
    for it in load_items(conn):
        if it["canonical_inventory_id"] == canonical_inventory_id:
            return it
    return None


def price_distribution_bins(items: list[dict], bin_width: int = 50, max_bins: int = 12) -> list[dict]:
    """Pure bucketing of already-computed tmv_eur values into fixed-width
    bins for a histogram. No price is computed here."""
    prices = [it["tmv_eur"] for it in items if it.get("tmv_eur") is not None]
    if not prices:
        return []
    bins: dict[int, int] = {}
    for p in prices:
        idx = min(int(p // bin_width), max_bins - 1)
        bins[idx] = bins.get(idx, 0) + 1
    out = []
    for idx in sorted(bins):
        lo = idx * bin_width
        label = f"€{lo}+" if idx == max_bins - 1 else f"€{lo}-{lo + bin_width}"
        out.append({"bucket": label, "count": bins[idx]})
    return out


def sell_time_distribution(items: list[dict]) -> list[dict]:
    """Pure bucketing of already-computed median_days_to_sell into
    Fast/Medium/Slow. No turnover figure is computed here."""
    counts = {label: 0 for label, _, _ in SELL_TIME_BUCKETS}
    for it in items:
        days = it.get("median_days_to_sell")
        if days is None:
            continue
        for label, lo, hi in SELL_TIME_BUCKETS:
            if lo <= days <= hi:
                counts[label] += 1
                break
    return [{"bucket": label, "count": counts[label]} for label, _, _ in SELL_TIME_BUCKETS]


def top_brands(items: list[dict], n: int = 10) -> list[dict]:
    """Pure count of priced items per brand, with % share. No new pricing
    or demand computation -- just a count of the already-priced list."""
    counts: dict[str, int] = {}
    for it in items:
        b = it.get("brand") or "Unknown"
        counts[b] = counts.get(b, 0) + 1
    total = len(items) or 1
    ranked = sorted(counts.items(), key=lambda kv: -kv[1])[:n]
    return [{"brand": b, "count": c, "pct": round(100 * c / total, 1)} for b, c in ranked]


# ── Inventory management: writes ONLY to data/raw/inventory.csv (the real
# pipeline source of truth), never directly to staging_inventory. Rationale:
# canonical_inventory_id/inventory_uid generation, deduplication, and
# validation are non-trivial logic that already lives in
# scripts/02_clean.py (resolve_inventory_uids/rebuild_staging_inventory) --
# reimplementing it here would risk producing an ID that diverges from what
# a future pipeline run would assign to the same row, corrupting identity.
# New rows are visible in the dashboard's raw-count only until the next
# `01_ingest.py` + `02_clean.py` run brings them into staging_inventory with
# real evidence/pricing -- disclosed on-screen, never silently instant.

# Must exactly match scripts/01_ingest.py's EXPECTED_INVENTORY_COLUMNS.
INVENTORY_CSV_COLUMNS = ["Rolex/Tudor", "Calibre", "P-number", "Stock"]


def inventory_csv_path() -> Path:
    return BASE_DIR / "data" / "raw" / "inventory.csv"


def _inventory_key(brand: str, caliber: str, part_number: str) -> tuple[str, str, str]:
    return (
        str(brand or "").strip().casefold(),
        str(caliber or "").strip().casefold(),
        str(part_number or "").strip().casefold(),
    )


def _canonical_inventory_id(brand: str, caliber: str, part_number: str) -> str:
    import utils
    return utils.slugify_canonical_id(brand, caliber, part_number)


def _read_inventory_csv(path: Path) -> list[dict]:
    import csv as _csv
    if not path.exists():
        return []
    with open(path, newline="") as f:
        return [dict(r) for r in _csv.DictReader(f)]


def _write_inventory_csv(path: Path, rows: list[dict]) -> None:
    import csv as _csv
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", newline="") as f:
        writer = _csv.DictWriter(f, fieldnames=INVENTORY_CSV_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({col: row.get(col, "") for col in INVENTORY_CSV_COLUMNS})
    tmp.replace(path)


def enqueue_pipeline_job(
    *,
    job_type: str,
    brand: str,
    caliber: str,
    part_number: str,
    stock: int,
    trigger_source: str = "dashboard",
    db_path: str | os.PathLike | None = None,
) -> str | None:
    db = resolve_db_path(db_path)
    if not db.exists():
        return None
    job_id = f"job_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    conn = connect_write_retry(db, retries=30, delay_seconds=0.5)
    try:
        conn.execute((BASE_DIR / "scripts" / "schema.sql").read_text())
        conn.execute(
            """
            INSERT INTO dashboard_pipeline_jobs
            (job_id, trigger_source, job_type, status, brand, caliber, part_number,
             stock, canonical_inventory_id)
            VALUES (?, ?, ?, 'QUEUED', ?, ?, ?, ?, ?)
            """,
            [
                job_id, trigger_source, job_type, brand, caliber, part_number,
                int(stock), _canonical_inventory_id(brand, caliber, part_number),
            ],
        )
    finally:
        conn.close()
    return job_id


def latest_pipeline_jobs(conn, limit: int = 10) -> list[dict]:
    try:
        rows = conn.execute(
            """
            SELECT job_id, job_type, status, brand, caliber, part_number,
                   canonical_inventory_id, inventory_uid, requested_at, started_at,
                   finished_at, step_timings_json, result_summary, error_message
            FROM dashboard_pipeline_jobs
            ORDER BY requested_at DESC
            LIMIT ?
            """,
            [limit],
        ).fetchall()
        cols = [d[0] for d in conn.description]
        return [dict(zip(cols, row)) for row in rows]
    except Exception:
        return []


def pipeline_job_events(conn, job_id: str, limit: int = 30) -> list[dict]:
    try:
        rows = conn.execute(
            """
            SELECT event_at, event_type, message
            FROM dashboard_pipeline_job_events
            WHERE job_id = ?
            ORDER BY event_at DESC, event_id DESC
            LIMIT ?
            """,
            [job_id, limit],
        ).fetchall()
        cols = [d[0] for d in conn.description]
        return [dict(zip(cols, row)) for row in rows]
    except Exception:
        return []


def dashboard_contract_row(conn, canonical_inventory_id: str) -> dict | None:
    try:
        row = conn.execute(
            """
            SELECT canonical_inventory_id, brand, caliber, part_number, pricing_status,
                   pricing_confidence, recommended_price_eur, sell_time_display,
                   turnover_confidence, no_recommendation_reason,
                   active_evidence_count, historical_evidence_count,
                   total_unique_evidence_count
            FROM dashboard_inventory_pricing
            WHERE canonical_inventory_id = ?
            """,
            [canonical_inventory_id],
        ).fetchone()
        if row is None:
            return None
        return dict(zip([d[0] for d in conn.description], row))
    except Exception:
        return None


def start_pipeline_job(
    job_id: str,
    *,
    db_path: str | os.PathLike | None = None,
    dry_run_collection: bool = False,
) -> dict:
    """Start one queued dashboard job in a background process.

    Streamlit should not block while the pipeline runs. The worker records
    durable job status in DuckDB and writes stdout/stderr to a log file.
    """
    import subprocess
    import sys

    db = resolve_db_path(db_path)
    log_dir = BASE_DIR / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"dashboard_pipeline_{job_id}.log"
    cmd = [
        sys.executable,
        str(BASE_DIR / "scripts" / "25_dashboard_pipeline_jobs.py"),
        "--db",
        str(db),
        "--job-id",
        job_id,
    ]
    if dry_run_collection:
        cmd.append("--dry-run-collection")
    with open(log_path, "a") as log:
        proc = subprocess.Popen(cmd, cwd=BASE_DIR, stdout=log, stderr=subprocess.STDOUT)
    return {"pid": proc.pid, "log_path": str(log_path)}


def retry_pipeline_job(job_id: str, *, db_path: str | os.PathLike | None = None) -> dict:
    """Retry a failed/queued job via the same worker path."""
    return start_pipeline_job(job_id, db_path=db_path)


def validate_inventory_upload_rows(rows: list[dict], path: Path | None = None) -> dict:
    """Dry-run validation for the bulk upload preview. Does not write files
    or enqueue jobs."""
    path = path or inventory_csv_path()
    existing_rows = _read_inventory_csv(path)
    existing_keys = {
        _inventory_key(row.get("Rolex/Tudor"), row.get("Calibre"), row.get("P-number"))
        for row in existing_rows
    }
    seen = set()
    errors = []
    new_rows = 0
    stock_updates = 0
    for i, r in enumerate(rows):
        row_no = i + 1
        brand = str(r.get("brand", "")).strip()
        caliber = str(r.get("caliber", "")).strip()
        part_number = str(r.get("part_number", "")).strip()
        if not brand or not part_number:
            errors.append(f"Row {row_no}: brand and part number are required.")
            continue
        try:
            stock_val = int(r.get("stock", 0))
        except (TypeError, ValueError):
            errors.append(f"Row {row_no}: stock must be a whole number.")
            continue
        if stock_val < 0:
            errors.append(f"Row {row_no}: stock must be non-negative.")
            continue
        key = _inventory_key(brand, caliber, part_number)
        if key in seen:
            errors.append(
                f"Row {row_no}: duplicate item in upload ({brand} / {caliber} / {part_number})."
            )
            continue
        seen.add(key)
        if key in existing_keys:
            stock_updates += 1
        else:
            new_rows += 1
    return {
        "ok": not errors,
        "errors": errors,
        "new_rows": new_rows,
        "stock_updates": stock_updates,
        "total_rows": len(rows),
    }


def append_inventory_item(
    brand: str,
    caliber: str,
    part_number: str,
    stock: int,
    path: Path | None = None,
    *,
    stock_mode: str = "set",
    enqueue_job: bool = True,
    db_path: str | os.PathLike | None = None,
) -> dict:
    """Adds a new inventory row, or updates stock when the item already
    exists. Existing items may only modify stock from the dashboard; identity
    fields stay unchanged."""

    brand = (brand or "").strip()
    caliber = (caliber or "").strip()
    part_number = (part_number or "").strip()
    if not brand or not part_number:
        raise ValueError("Brand and part number are required.")
    if stock is None or stock < 0:
        raise ValueError("Stock must be a non-negative number.")
    stock_mode = (stock_mode or "set").strip().lower()
    if stock_mode not in {"set", "add"}:
        raise ValueError("Stock update mode must be 'set' or 'add'.")

    path = path or inventory_csv_path()
    rows = _read_inventory_csv(path)
    key = _inventory_key(brand, caliber, part_number)
    action = "created"
    old_stock = None
    new_stock = int(stock)
    for row in rows:
        if _inventory_key(row.get("Rolex/Tudor"), row.get("Calibre"), row.get("P-number")) == key:
            old_stock = int(row.get("Stock") or 0)
            new_stock = old_stock + int(stock) if stock_mode == "add" else int(stock)
            row["Stock"] = str(new_stock)
            action = "stock_updated"
            break
    else:
        rows.append({"Rolex/Tudor": brand, "Calibre": caliber, "P-number": part_number, "Stock": str(new_stock)})

    _write_inventory_csv(path, rows)

    job_id = None
    if enqueue_job and path == inventory_csv_path():
        job_id = enqueue_pipeline_job(
            job_type="STOCK_UPDATE" if action == "stock_updated" else "NEW_ITEM",
            brand=brand, caliber=caliber, part_number=part_number, stock=new_stock, db_path=db_path,
        )

    if action == "stock_updated" and stock_mode == "add":
        note_success = (
            f"Existing item found: added {int(stock)} unit(s). "
            f"Stock changed from {old_stock} to {new_stock}, and a refresh job was queued."
        )
        note_no_job = (
            f"Existing item found: added {int(stock)} unit(s). "
            f"Stock changed from {old_stock} to {new_stock}."
        )
    else:
        note_success = (
            "Existing item found: stock total was updated and a refresh job was queued."
            if action == "stock_updated"
            else "New item added and a pricing job was queued."
        )
        note_no_job = (
            "Existing item found: stock total was updated."
            if action == "stock_updated"
            else "New item added to inventory.csv."
        )

    return {
        "brand": brand, "caliber": caliber, "part_number": part_number, "stock": new_stock,
        "stock_mode": stock_mode,
        "previous_stock": old_stock,
        "action": action,
        "job_id": job_id,
        "note": note_success if job_id else note_no_job,
    }


def append_inventory_rows(
    rows: list[dict],
    path: Path | None = None,
    *,
    enqueue_job: bool = True,
    db_path: str | os.PathLike | None = None,
) -> dict:
    """Bulk version of append_inventory_item for CSV import. Validates
    every row before writing any -- refuses the whole batch on the first
    invalid row rather than a partial import."""
    for i, r in enumerate(rows):
        if not str(r.get("brand", "")).strip() or not str(r.get("part_number", "")).strip():
            raise ValueError(f"Row {i + 1}: brand and part number are required.")
        try:
            stock_val = int(r.get("stock", 0))
        except (TypeError, ValueError):
            raise ValueError(f"Row {i + 1}: stock must be a whole number.")
        if stock_val < 0:
            raise ValueError(f"Row {i + 1}: stock must be non-negative.")

    seen = set()
    for i, r in enumerate(rows):
        key = _inventory_key(r["brand"], r.get("caliber", ""), r["part_number"])
        if key in seen:
            raise ValueError(
                f"Row {i + 1}: duplicate item appears in this upload "
                f"({r['brand']} / {r.get('caliber', '')} / {r['part_number']}). "
                "No rows were written. Remove the duplicate or combine the stock first."
            )
        seen.add(key)

    created = 0
    stock_updated = 0
    job_ids = []
    for r in rows:
        result = append_inventory_item(
            r["brand"], r.get("caliber", ""), r["part_number"], int(r["stock"]),
            path=path, enqueue_job=enqueue_job, db_path=db_path,
        )
        created += 1 if result["action"] == "created" else 0
        stock_updated += 1 if result["action"] == "stock_updated" else 0
        if result.get("job_id"):
            job_ids.append(result["job_id"])
    return {"rows_added": created, "stock_updated": stock_updated, "job_ids": job_ids}


def export_recommendations_rows(items: list[dict]) -> list[dict]:
    """Pure formatting of already-loaded priced items into the client
    export column shape. No computation -- every value is read verbatim
    from the item dict load_items() already produced."""
    out = []
    for it in items:
        out.append({
            "Part Number": it.get("part_number"),
            "Brand": it.get("brand"),
            "Caliber": it.get("caliber"),
            "Stock": it.get("stock"),
            "Recommended Price (EUR)": it.get("tmv_eur"),
            "Confidence": it.get("pricing_state_label"),
            "Expected Selling Time": it.get("sell_time_display"),
            "Potential Revenue (EUR)": (
                round(number_or_zero(it.get("tmv_eur")) * number_or_zero(it.get("stock")), 2)
                if it.get("tmv_eur") is not None and not pd.isna(it.get("tmv_eur")) else None
            ),
        })
    return out


def _contract_bucket_forecast(row: dict) -> str | None:
    bucket_cols = [
        ("0-7", "units_sold_0_7"),
        ("8-30", "units_sold_8_30"),
        ("31-90", "units_sold_31_90"),
        ("91-183", "units_sold_91_183"),
        ("184-365", "units_sold_184_365"),
        ("366-730", "units_sold_366_730"),
        ("731-1065", "units_sold_731_1065"),
        ("1066+", "units_sold_1066_plus"),
    ]
    if not any(row.get(col) for _, col in bucket_cols):
        return None
    import json
    return json.dumps([
        {"bucket": label, "expected_units": float(row.get(col) or 0.0)}
        for label, col in bucket_cols
    ])
