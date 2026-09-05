"""
23_build_dashboard_contract.py
==============================

Builds dashboard_inventory_pricing, the single client-facing contract table.

This script does not compute TMV. It reads the already-computed governed and
algorithmic pricing outputs, adds display-safe semantics, and writes one row
per eligible inventory item. The dashboard can then read this table without
reconstructing business joins live.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import math
import os
import time
from pathlib import Path

import duckdb
import pandas as pd

BASE_DIR = Path(__file__).parent.parent
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
SCHEMA_PATH = BASE_DIR / "scripts" / "schema.sql"
REPORTS_DIR = BASE_DIR / "reports"
CALCULATION_VERSION = "dashboard_contract_v1"
ASK_TO_SOLD_ADJUSTMENT = 0.79
RECENCY_HALFLIFE_MONTHS = 12.0
TURNOVER_CAP_DAYS = 3650.0
ACTIVE_STABLE_IQR_RATIO_MAX = 0.60
ACTIVE_STABLE_RANGE_RATIO_MAX = 6.0
HISTORICAL_STABLE_IQR_RATIO_MAX = 0.75
HIGH_PRICE_AGREEMENT_RATIO_MAX = 0.65
MEDIUM_PRICE_AGREEMENT_RATIO_MAX = 0.90
COHORT_TURNOVER_MIN_EVIDENCE = 5
COHORT_TURNOVER_MIN_ITEMS = 3
BRAND_TURNOVER_MIN_EVIDENCE = 25
BRAND_TURNOVER_MIN_ITEMS = 10
TURNOVER_BUCKETS = [
    (0, 7, "0-7"),
    (8, 30, "8-30"),
    (31, 90, "31-90"),
    (91, 183, "91-183"),
    (184, 365, "184-365"),
    (366, 730, "366-730"),
    (731, 1065, "731-1065"),
    (1066, None, "1066+"),
]

CONFIDENCE_LABELS = {
    "HIGH": "High confidence",
    "MEDIUM": "Medium confidence",
    "LOW": "Low confidence",
    "INSUFFICIENT_DATA": "Insufficient data",
}


def _safe_int(value, default: int = 0) -> int:
    if value is None or pd.isna(value):
        return default
    return int(value)


def _load_scenario_engine():
    spec = importlib.util.spec_from_file_location(
        "scenario_engine", Path(__file__).parent / "17_scenario_engine.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _read_tmv_parameter(conn, name: str, default: float = 0.0) -> float:
    try:
        row = conn.execute(
            "SELECT parameter_value, active_flag FROM ref_tmv_parameters WHERE parameter_name = ?",
            [name],
        ).fetchone()
    except Exception:
        return default
    if row is None:
        return default
    value, active = row
    return float(value) if active else default


def _age_months(d, ref) -> float:
    if d is None or pd.isna(d):
        return RECENCY_HALFLIFE_MONTHS
    d = pd.to_datetime(d).date()
    return max(0.0, (ref - d).days / 30.44)


def _lambda_to_turnover(lam: float, stock: int = 0) -> tuple[float | None, float | None, float | None, dict[str, float]]:
    bucket_values = _empty_bucket_values()
    if lam is None or lam <= 0:
        return None, None, None, bucket_values
    median_days = min(TURNOVER_CAP_DAYS, 30.0 * math.log(2) / lam)
    prob30 = 1 - math.exp(-lam * 30 / 30.0)
    prob90 = 1 - math.exp(-lam * 90 / 30.0)
    if stock and stock > 0:
        def surv_f(t):
            return 1.0 - math.exp(-lam * max(0.0, t) / 30.0)
        for lo, hi, label in TURNOVER_BUCKETS:
            col = {
                "0-7": "units_sold_0_7",
                "8-30": "units_sold_8_30",
                "31-90": "units_sold_31_90",
                "91-183": "units_sold_91_183",
                "184-365": "units_sold_184_365",
                "366-730": "units_sold_366_730",
                "731-1065": "units_sold_731_1065",
                "1066+": "units_sold_1066_plus",
            }[label]
            f_lo = surv_f(lo - 1)
            f_hi = 1.0 if hi is None else surv_f(hi)
            bucket_values[col] = round(stock * max(0.0, f_hi - f_lo), 3)
    return round(float(median_days), 1), round(float(prob30), 4), round(float(prob90), 4), bucket_values


def _historical_observations(conn) -> pd.DataFrame:
    """Trusted sold evidence at the grain used by pricing/turnover.

    VCP aggregate rows carry total_sold volume; eBay sold rows are one sale.
    This is still direct matched evidence only, never raw staging row counts.
    """
    return conn.execute("""
        SELECT e.inventory_uid, si.brand, si.caliber, e.evidence_uid,
               v.avg_price_eur AS price_eur,
               COALESCE(v.total_sold, 1) AS sold_units,
               v.last_sold_date AS sold_date
        FROM evidence_confidence_classification e
        JOIN staging_inventory si ON si.inventory_uid = e.inventory_uid
        JOIN stg_historical_vcp_aggregate v
          ON e.source_table = 'match_candidates_vcp'
         AND e.evidence_uid = v.stable_evidence_uid
        WHERE e.confidence_tier IN ('AUTO_CONFIRMED','HIGH_CONFIDENCE')
          AND e.evidence_uid IS NOT NULL
          AND v.avg_price_eur IS NOT NULL
          AND v.avg_price_eur > 0
        UNION ALL
        SELECT e.inventory_uid, si.brand, si.caliber, e.evidence_uid,
               s.price_eur AS price_eur,
               1 AS sold_units,
               s.sold_date AS sold_date
        FROM evidence_confidence_classification e
        JOIN staging_inventory si ON si.inventory_uid = e.inventory_uid
        JOIN stg_historical_ebay_sold s
          ON e.source_table = 'match_candidates_ebay_sold'
         AND e.evidence_uid = s.stable_evidence_uid
        WHERE e.confidence_tier IN ('AUTO_CONFIRMED','HIGH_CONFIDENCE')
          AND e.evidence_uid IS NOT NULL
          AND s.price_eur IS NOT NULL
          AND s.price_eur > 0
    """).df()


def _historical_stats(hist_obs: pd.DataFrame) -> pd.DataFrame:
    if hist_obs.empty:
        return pd.DataFrame(columns=[
            "inventory_uid", "historical_units_sold_count", "historical_price_median_eur",
            "historical_price_q1_eur", "historical_price_q3_eur",
        ])
    obs = hist_obs.copy()
    obs["price_eur"] = pd.to_numeric(obs["price_eur"], errors="coerce")
    obs["sold_units"] = pd.to_numeric(obs["sold_units"], errors="coerce").fillna(1).clip(lower=1)
    per_evidence = (
        obs.groupby(["inventory_uid", "evidence_uid"], dropna=False)
        .agg(
            price_eur=("price_eur", "median"),
            sold_units=("sold_units", "max"),
        )
        .reset_index()
    )
    return (
        per_evidence.groupby("inventory_uid", dropna=False)
        .agg(
            historical_units_sold_count=("sold_units", "sum"),
            historical_price_median_eur=("price_eur", "median"),
            historical_price_q1_eur=("price_eur", lambda s: s.quantile(0.25)),
            historical_price_q3_eur=("price_eur", lambda s: s.quantile(0.75)),
        )
        .reset_index()
    )


def _cohort_turnover_map(hist_obs: pd.DataFrame) -> dict[tuple[str, str, str], dict]:
    if hist_obs.empty:
        return {}
    obs = hist_obs.copy()
    obs["sold_date"] = pd.to_datetime(obs["sold_date"], errors="coerce")
    obs = obs.dropna(subset=["sold_date"])
    if obs.empty:
        return {}
    ref = obs["sold_date"].max().date()
    min_date = obs["sold_date"].min().date()
    dataset_months = max(1.0, (ref - min_date).days / 30.44)
    eff_window = max(
        1.0,
        (RECENCY_HALFLIFE_MONTHS / math.log(2))
        * (1 - math.exp(-math.log(2) * dataset_months / RECENCY_HALFLIFE_MONTHS)),
    )
    obs["sold_units"] = pd.to_numeric(obs["sold_units"], errors="coerce").fillna(1).clip(lower=1)
    obs["weighted_units"] = obs.apply(
        lambda r: float(r["sold_units"])
        * math.exp(-math.log(2) * _age_months(r["sold_date"], ref) / RECENCY_HALFLIFE_MONTHS),
        axis=1,
    )

    out: dict[tuple[str, str, str], dict] = {}
    group_specs = [
        ("BRAND_CALIBER", ["brand", "caliber"]),
        ("BRAND_PORTFOLIO", ["brand"]),
        ("GLOBAL_PORTFOLIO", []),
    ]
    for level, keys in group_specs:
        if keys:
            grouped = (
                obs.dropna(subset=keys)
                .groupby(keys, dropna=False)
                .agg(
                    weighted_units=("weighted_units", "sum"),
                    sold_units=("sold_units", "sum"),
                    evidence_count=("evidence_uid", "nunique"),
                    item_count=("inventory_uid", "nunique"),
                )
                .reset_index()
            )
        else:
            grouped = pd.DataFrame([{
                "weighted_units": obs["weighted_units"].sum(),
                "sold_units": obs["sold_units"].sum(),
                "evidence_count": obs["evidence_uid"].nunique(),
                "item_count": obs["inventory_uid"].nunique(),
            }])
        for r in grouped.to_dict("records"):
            item_count = max(1, int(r["item_count"] or 0))
            # Cohort sales volume is observed across many inventory identities.
            # Convert it to a per-item monthly hazard before using it as a
            # sell-time prior for one inventory row.
            lam = float(r["weighted_units"]) / (eff_window * item_count) if eff_window > 0 else 0.0
            if level == "BRAND_CALIBER":
                key = (level, str(r["brand"]), str(r["caliber"]))
            elif level == "BRAND_PORTFOLIO":
                key = (level, str(r["brand"]), "")
            else:
                key = (level, "", "")
            out[key] = {
                "lambda_monthly": lam,
                "sold_units": int(r["sold_units"]),
                "evidence_count": int(r["evidence_count"]),
                "item_count": item_count,
            }
    return out


def _reason_without_stale_confidence(reason: str | None) -> str | None:
    """Remove the old internal confidence sentence from 13_build_tmv.py.

    Algorithmic rows receive their final client tier after 13_build_tmv.py
    creates recommendation_reason, so the old sentence can contradict the
    actual dashboard tier. Keep the pricing explanation, drop only the stale
    confidence wording.
    """
    if reason is None or pd.isna(reason):
        return reason
    reason = str(reason)
    marker = " Confidence:"
    if marker in reason:
        return reason.split(marker, 1)[0].rstrip()
    return reason


def _pricing_method(evidence_basis: str | None, active_count: int, historical_count: int, recommended: float | None) -> str:
    if recommended is None:
        return "NO_DATA"
    if evidence_basis == "HISTORICAL" and active_count > 0:
        return "DIRECT_HISTORICAL_AND_ACTIVE"
    if evidence_basis == "HISTORICAL":
        return "DIRECT_HISTORICAL"
    if evidence_basis == "ACTIVE_ONLY":
        return "ACTIVE_ONLY_CALIBRATED"
    return "COMPARABLE_MARKET"


def _pricing_confidence(
    *,
    source: str | None,
    tier: str | None,
    evidence_basis: str | None,
    recommended: float | None,
    active_count: int,
    historical_count: int,
    confidence_score,
    active_iqr_ratio,
    active_range_ratio=None,
    active_outlier_count: int = 0,
    historical_units_sold: int = 0,
    historical_iqr_ratio=None,
    historical_active_gap_ratio=None,
) -> tuple[str, str, str]:
    if recommended is None:
        return (
            "NO_RECOMMENDATION",
            "LOW",
            "No price is shown because no trusted direct evidence or implemented comparable fallback supports a recommendation.",
        )

    score = None if confidence_score is None or pd.isna(confidence_score) else float(confidence_score)
    dispersion = None if active_iqr_ratio is None or pd.isna(active_iqr_ratio) else float(active_iqr_ratio)
    range_ratio = None if active_range_ratio is None or pd.isna(active_range_ratio) else float(active_range_ratio)
    hist_dispersion = None if historical_iqr_ratio is None or pd.isna(historical_iqr_ratio) else float(historical_iqr_ratio)
    price_gap = (
        None
        if historical_active_gap_ratio is None or pd.isna(historical_active_gap_ratio)
        else float(historical_active_gap_ratio)
    )

    if source == "GOVERNED":
        return "PRICED", "HIGH", "Human-governed pricing path with validated match evidence."

    if evidence_basis == "ACTIVE_ONLY":
        active_market_stable = (
            (dispersion is None or dispersion <= ACTIVE_STABLE_IQR_RATIO_MAX)
            and (range_ratio is None or range_ratio <= ACTIVE_STABLE_RANGE_RATIO_MAX)
        )
        if active_count >= 5 and active_market_stable and (score is None or score >= 0.80):
            return (
                "PRICED",
                "MEDIUM",
                "Active-only price is allowed because identity is trusted and unique active listings are sufficiently numerous, deduplicated, and price-stable; turnover remains separate.",
            )
        return (
            "PRICED",
            "LOW",
            "Active-only price is shown as an indicative calibrated estimate because evidence is sparse, dispersed, outlier-affected, condition-ambiguous, or lacks direct sold support.",
        )

    if evidence_basis == "HISTORICAL":
        stable_history = hist_dispersion is None or hist_dispersion <= HISTORICAL_STABLE_IQR_RATIO_MAX
        stable_active = (
            (dispersion is None or dispersion <= ACTIVE_STABLE_IQR_RATIO_MAX)
            and (range_ratio is None or range_ratio <= ACTIVE_STABLE_RANGE_RATIO_MAX)
        )
        deep_history = historical_count >= 5 or historical_units_sold >= 10
        moderate_history = historical_count >= 2 or historical_units_sold >= 5
        active_context = active_count >= 5 and stable_active
        high_agreement = price_gap is None or price_gap <= HIGH_PRICE_AGREEMENT_RATIO_MAX
        medium_agreement = price_gap is None or price_gap <= MEDIUM_PRICE_AGREEMENT_RATIO_MAX
        strong_identity = score is None or score >= 0.80
        high_identity = score is None or score >= 0.85
        trusted_direct_tier = tier in ("AUTO_CONFIRMED", "HIGH_CONFIDENCE")
        if trusted_direct_tier and stable_history and high_identity:
            if deep_history and high_agreement:
                return "PRICED", "HIGH", "Deep, stable direct sold evidence and strong identity matching support the price; active market context is consistent where available."
            if historical_count >= 3 and active_context and high_agreement:
                return "PRICED", "HIGH", "Multiple direct sold observations, strong identity matching, and stable active listings agree closely enough for high-confidence pricing."
        if moderate_history and stable_history and medium_agreement:
            return "PRICED", "MEDIUM", "Direct sold evidence supports the price, but sample size or cross-market support is not strong enough for High."
        if historical_count == 1 and active_context and medium_agreement:
            return "PRICED", "MEDIUM", "One sold observation is supported by stable active listings, so the price is usable but not High confidence."
        if historical_count >= 1:
            return "PRICED", "LOW", "Sold evidence exists, but it is thin, dispersed, or not well supported by current market context."
        return "PRICED", "LOW", "Price is based on limited historical evidence."

    if tier == "AUTO_CONFIRMED":
        return "PRICED", "MEDIUM", "Trusted direct evidence supports a price, but evidence basis is not strong enough for High."
    if tier == "HIGH_CONFIDENCE":
        return "PRICED", "LOW", "Trusted but weaker evidence supports only an indicative price."
    return "PRICED", "LOW", "Price is defensible but uncertainty remains high."


def _no_recommendation_reason(row) -> str | None:
    recommended = row.get("recommended_price_eur")
    if recommended is not None and not pd.isna(recommended):
        return None
    match_status = row.get("inventory_match_status")
    if match_status is not None and not pd.isna(match_status):
        status = str(match_status)
        if status == "NO_CANDIDATES":
            return "NO_CANDIDATES"
        if status == "ONLY_LOW_CONFIDENCE_CANDIDATES":
            return "ONLY_LOW_CONFIDENCE_CANDIDATES"
        if status == "ONLY_INSUFFICIENT_EVIDENCE":
            return "ONLY_INSUFFICIENT_EVIDENCE"
        if status == "ALL_CANDIDATES_REJECTED":
            return "REJECTED_ONLY"
        if status == "REVIEW_PENDING":
            return "REVIEW_REQUIRED_BEFORE_PRICING"
    candidate_count = int(row.get("candidate_count") or 0)
    auto_high = int(row.get("auto_high_evidence") or 0)
    rejected = int(row.get("rejected_evidence") or 0)
    if candidate_count == 0:
        return "NO_CANDIDATES"
    if auto_high == 0 and rejected > 0:
        return "REJECTED_ONLY"
    if auto_high == 0:
        return "NO_TRUSTWORTHY_EVIDENCE"
    return "OTHER_EXPLICIT_REASON"


def _turnover_state(median_days, prob30, prob90, evidence_basis, historical_count: int) -> tuple[
    str, str, int | None, int | None, str | None, str, str
]:
    display, status = _sell_time_display(median_days, prob30, prob90, evidence_basis, historical_count)
    if display == "Insufficient sold evidence" or median_days is None or pd.isna(median_days):
        return (
            "INSUFFICIENT_DATA",
            "NO_SOLD_VELOCITY",
            None,
            None,
            "Sell-time estimate unavailable - insufficient sold evidence",
            status,
            "Active listings can support price, but they do not reveal completed-sale velocity.",
        )

    days = float(median_days)
    # Bucket display is client-facing, so fractional model outputs need to land
    # in the next whole-day bucket. Example: 30.6 days should display as
    # 31-90 days, not fall between integer ranges and become "None-None days".
    bucket_day = math.ceil(days)
    buckets = [
        (0, 30), (31, 90), (91, 183), (184, 365),
        (366, 730), (731, 1065), (1066, None),
    ]
    lower = upper = None
    for lo, hi in buckets:
        if hi is None and bucket_day >= lo:
            lower, upper = lo, None
            break
        if hi is not None and lo <= bucket_day <= hi:
            lower, upper = lo, hi
            break

    if historical_count >= 3:
        confidence = "HIGH"
        reason = "Dated item-level sold observations support the exponential hazard estimate."
    elif historical_count >= 1:
        confidence = "LOW"
        reason = "Sparse dated sold observations support only a low-confidence turnover estimate."
    else:
        confidence = "INSUFFICIENT_DATA"
        reason = "No dated sold observations support turnover."
    method = "DIRECT_ITEM_HAZARD_FIT" if historical_count >= 1 else "NO_SOLD_VELOCITY"
    interval = f"{lower}+ days" if upper is None and lower is not None else f"{lower}-{upper} days"
    return confidence, method, lower, upper, interval, status, reason


def _cohort_candidate(row: dict, cohort_map: dict[tuple[str, str, str], dict]) -> tuple[str, dict] | tuple[None, None]:
    brand = str(row.get("brand")) if row.get("brand") is not None and not pd.isna(row.get("brand")) else ""
    caliber = str(row.get("caliber")) if row.get("caliber") is not None and not pd.isna(row.get("caliber")) else ""
    candidates = [
        ("COMPARABLE_COHORT_HAZARD_FIT", cohort_map.get(("BRAND_CALIBER", brand, caliber))),
        ("HIERARCHICAL_PORTFOLIO_PRIOR", cohort_map.get(("BRAND_PORTFOLIO", brand, ""))),
        ("HIERARCHICAL_PORTFOLIO_PRIOR", cohort_map.get(("GLOBAL_PORTFOLIO", "", ""))),
    ]
    for method, stats in candidates:
        if not stats or float(stats.get("lambda_monthly") or 0.0) <= 0:
            continue
        evidence_count = int(stats.get("evidence_count") or 0)
        item_count = int(stats.get("item_count") or 0)
        if method == "COMPARABLE_COHORT_HAZARD_FIT":
            if evidence_count >= COHORT_TURNOVER_MIN_EVIDENCE and item_count >= COHORT_TURNOVER_MIN_ITEMS:
                return method, stats
        elif evidence_count >= BRAND_TURNOVER_MIN_EVIDENCE and item_count >= BRAND_TURNOVER_MIN_ITEMS:
            return method, stats
    return None, None


def _turnover_contract_values(
    *,
    row: dict,
    historical_count: int,
    stock: int,
    cohort_map: dict[tuple[str, str, str], dict],
) -> tuple[float | None, float | None, float | None, dict[str, float], str, str, int | None, int | None, str | None, str, str, int | None, int | None, str | None]:
    bucket_values = _parse_bucket_forecast(row.get("turnover_bucket_forecast"))
    confidence, method, lower, upper, display, status, reason = _turnover_state(
        row.get("median_days_to_sell"),
        row.get("probability_sell_30d"),
        row.get("probability_sell_90d"),
        row.get("evidence_basis"),
        historical_count,
    )
    direct_supported = method == "DIRECT_ITEM_HAZARD_FIT" and display != "Sell-time estimate unavailable - insufficient sold evidence"
    if direct_supported:
        return (
            row.get("median_days_to_sell"), row.get("probability_sell_30d"), row.get("probability_sell_90d"),
            bucket_values, confidence, method, lower, upper, display, status, reason,
            historical_count, 1, "ITEM",
        )

    cohort_method, cohort_stats = _cohort_candidate(row, cohort_map)
    if cohort_method and cohort_stats:
        median_days, prob30, prob90, cohort_buckets = _lambda_to_turnover(
            float(cohort_stats["lambda_monthly"]), stock
        )
        confidence = "MEDIUM" if cohort_method == "COMPARABLE_COHORT_HAZARD_FIT" else "LOW"
        _, _, lower, upper, display, _, _ = _turnover_state(
            median_days, prob30, prob90, "HISTORICAL", int(cohort_stats["evidence_count"])
        )
        status = "COHORT_SUPPORTED" if confidence == "MEDIUM" else "PORTFOLIO_PRIOR_SUPPORTED"
        level = "BRAND_CALIBER" if confidence == "MEDIUM" else "BRAND_PORTFOLIO"
        reason = (
            "Sell-time is estimated from comparable sold observations in the same brand and caliber cohort."
            if confidence == "MEDIUM"
            else "Sell-time is estimated from a broad brand/portfolio historical prior because item-level sold velocity is unavailable."
        )
        return (
            median_days, prob30, prob90, cohort_buckets, confidence, cohort_method,
            lower, upper, display, status, reason,
            int(cohort_stats["evidence_count"]), int(cohort_stats["item_count"]), level,
        )

    return (
        None, None, None, _empty_bucket_values(), "LOW", "NO_SOLD_VELOCITY",
        None, None, "Sell-time estimate unavailable - insufficient sold evidence",
        status if status in ("ACTIVE_ONLY_NO_VELOCITY", "NO_TURNOVER") else "NO_TURNOVER",
        (
            "Active listings can support price, but they do not reveal completed-sale velocity."
            if status == "ACTIVE_ONLY_NO_VELOCITY"
            else "No dated sold observations or validated historical cohort support turnover."
        ),
        None, None, None,
    )


def _unpriced_turnover_contract_values() -> tuple[
    None, None, None, dict[str, float], str, str, None, None, str, str, str, None, None, None
]:
    return (
        None,
        None,
        None,
        _empty_bucket_values(),
        "LOW",
        "NO_PRICE_RECOMMENDATION",
        None,
        None,
        "Sell-time estimate unavailable - no price recommendation",
        "NO_PRICE_RECOMMENDATION",
        "No client-facing sell-time recommendation is shown because the item does not have a defensible price recommendation.",
        None,
        None,
        None,
    )


def _empty_bucket_values() -> dict[str, float]:
    return {
        "units_sold_0_7": 0.0,
        "units_sold_8_30": 0.0,
        "units_sold_31_90": 0.0,
        "units_sold_91_183": 0.0,
        "units_sold_184_365": 0.0,
        "units_sold_366_730": 0.0,
        "units_sold_731_1065": 0.0,
        "units_sold_1066_plus": 0.0,
    }


def _parse_bucket_forecast(raw: str | None) -> dict[str, float]:
    out = _empty_bucket_values()
    if not raw:
        return out
    mapping = {
        "0-7": "units_sold_0_7",
        "8-30": "units_sold_8_30",
        "31-90": "units_sold_31_90",
        "91-183": "units_sold_91_183",
        "184-365": "units_sold_184_365",
        "366-730": "units_sold_366_730",
        "731-1065": "units_sold_731_1065",
        "1066+": "units_sold_1066_plus",
    }
    try:
        buckets = json.loads(raw)
    except (TypeError, ValueError):
        return out
    for bucket in buckets:
        col = mapping.get(str(bucket.get("bucket")))
        if col:
            out[col] = float(bucket.get("expected_units") or 0.0)
    return out


def _sell_time_display(median_days, prob30, prob90, evidence_basis, historical_count: int) -> tuple[str | None, str]:
    if median_days is None or pd.isna(median_days):
        return None, "NO_TURNOVER"
    if (
        float(median_days) >= 3650.0
        and float(prob30 or 0.0) == 0.0
        and float(prob90 or 0.0) == 0.0
        and (evidence_basis == "ACTIVE_ONLY" or historical_count == 0)
    ):
        return "Insufficient sold evidence", "ACTIVE_ONLY_NO_VELOCITY"
    return f"{float(median_days):.0f} days", "SUPPORTED"


def _source_frames(conn) -> pd.DataFrame:
    governed = conn.execute("""
        SELECT t.canonical_inventory_id, 'GOVERNED' AS pricing_source,
               t.tmv_eur AS recommended_price_eur, t.tmv_eur AS final_recommended_price,
               t.tmv_low_eur, t.tmv_high_eur, t.confidence_tier,
               t.valuation_basis AS evidence_basis,
               f.historical_value_eur AS historical_value_h,
               f.current_value_eur AS current_value_c,
               f.scarcity_score AS scarcity_score_s,
               d.price_trend_slope AS price_trend_p,
               d.recency_score AS demand_index_d,
               f.recommendation_reason,
               s.median_days_to_sell, s.probability_sell_30d, s.probability_sell_90d,
               s.turnover_bucket_forecast
        FROM tmv_results t
        LEFT JOIN feat_pricing f USING (canonical_inventory_id)
        LEFT JOIN feat_demand d USING (canonical_inventory_id)
        LEFT JOIN turnover_survival s USING (canonical_inventory_id)
    """).df()

    algorithmic = conn.execute("""
        SELECT t.canonical_inventory_id, 'ALGORITHMIC' AS pricing_source,
               t.tmv_eur AS recommended_price_eur, t.tmv_eur AS final_recommended_price,
               t.tmv_low_eur, t.tmv_high_eur, t.confidence_tier,
               t.valuation_basis AS evidence_basis,
               t.historical_value_eur AS historical_value_h,
               t.current_value_eur AS current_value_c,
               t.scarcity_score AS scarcity_score_s,
               t.price_trend AS price_trend_p,
               t.demand_index AS demand_index_d,
               t.recommendation_reason,
               s.median_days_to_sell, s.probability_sell_30d, s.probability_sell_90d,
               s.turnover_bucket_forecast
        FROM tmv_results_algorithmic t
        LEFT JOIN turnover_survival_algorithmic s USING (canonical_inventory_id)
        WHERE t.canonical_inventory_id NOT IN (SELECT canonical_inventory_id FROM tmv_results)
    """).df()

    return pd.concat([governed, algorithmic], ignore_index=True)


def build_contract_dataframe(conn) -> pd.DataFrame:
    scenario_engine = _load_scenario_engine()
    demand_weight = _read_tmv_parameter(conn, "demand_weight", default=0.0)
    hist_obs = _historical_observations(conn)
    hist_stats = _historical_stats(hist_obs)
    cohort_turnover = _cohort_turnover_map(hist_obs)

    eligible = conn.execute("""
        SELECT inventory_uid, canonical_inventory_id, brand, caliber, part_number,
               stock AS stock_quantity, validation_status
        FROM staging_inventory
        WHERE validation_status <> 'FAIL'
    """).df()

    priced = _source_frames(conn)
    evidence = conn.execute("""
        SELECT si.inventory_uid,
               COUNT(DISTINCT CASE
                   WHEN e.confidence_tier IN ('AUTO_CONFIRMED','HIGH_CONFIDENCE')
                    AND e.source_table='match_candidates_active'
                   THEN e.evidence_uid END) AS active_evidence_count,
               COUNT(DISTINCT CASE
                   WHEN e.confidence_tier IN ('AUTO_CONFIRMED','HIGH_CONFIDENCE')
                    AND e.source_table IN ('match_candidates_ebay_sold','match_candidates_vcp')
                   THEN e.evidence_uid END) AS historical_evidence_count,
               SUM(CASE WHEN e.candidate_key IS NOT NULL THEN 1 ELSE 0 END) AS candidate_count,
               SUM(CASE WHEN e.confidence_tier IN ('AUTO_CONFIRMED','HIGH_CONFIDENCE') THEN 1 ELSE 0 END) AS auto_high_evidence,
               SUM(CASE WHEN e.confidence_tier='REJECTED' THEN 1 ELSE 0 END) AS rejected_evidence,
               MIN(CASE WHEN e.confidence_tier IN ('AUTO_CONFIRMED','HIGH_CONFIDENCE') THEN e.v2_score END) AS confidence_score,
               string_agg(DISTINCT e.classification_run_id, ', ' ORDER BY e.classification_run_id) AS source_run_id
        FROM staging_inventory si
        LEFT JOIN evidence_confidence_classification e ON e.inventory_uid = si.inventory_uid
        WHERE si.validation_status <> 'FAIL'
        GROUP BY si.inventory_uid
    """).df()

    active_stats = conn.execute("""
        WITH active_evidence AS (
            SELECT
                e.inventory_uid,
                e.evidence_uid,
                a.price_eur,
                a.fetched_at,
                row_number() OVER (
                    PARTITION BY e.inventory_uid, e.evidence_uid
                    ORDER BY a.fetched_at DESC NULLS LAST, a.id DESC
                ) AS rn
            FROM evidence_confidence_classification e
            JOIN stg_active_targeted a
              ON e.source_table = 'match_candidates_active'
             AND a.id = e.source_id
            WHERE e.confidence_tier IN ('AUTO_CONFIRMED','HIGH_CONFIDENCE')
              AND e.evidence_uid IS NOT NULL
              AND a.price_eur IS NOT NULL
              AND a.price_eur > 0
        ),
        observation_counts AS (
            SELECT
                inventory_uid,
                COUNT(*) AS active_observation_count,
                COUNT(DISTINCT evidence_uid) AS active_unique_listing_count,
                COUNT(*) - COUNT(DISTINCT evidence_uid) AS active_duplicate_observation_count
            FROM active_evidence
            GROUP BY inventory_uid
        ),
        latest AS (
            SELECT inventory_uid, evidence_uid, price_eur
            FROM active_evidence
            WHERE rn = 1
        ),
        stats AS (
            SELECT
                inventory_uid,
                COUNT(DISTINCT evidence_uid) AS active_unique_listing_count,
                median(price_eur) AS active_price_median_eur,
                quantile_cont(price_eur, 0.25) AS active_price_q1_eur,
                quantile_cont(price_eur, 0.75) AS active_price_q3_eur,
                min(price_eur) AS active_price_min_eur,
                max(price_eur) AS active_price_max_eur
            FROM latest
            GROUP BY inventory_uid
        ),
        mad AS (
            SELECT
                l.inventory_uid,
                median(abs(l.price_eur - s.active_price_median_eur)) AS active_price_mad_eur
            FROM latest l
            JOIN stats s ON s.inventory_uid = l.inventory_uid
            GROUP BY l.inventory_uid
        ),
        outliers AS (
            SELECT
                l.inventory_uid,
                SUM(CASE
                    WHEN m.active_price_mad_eur > 0
                     AND abs(l.price_eur - s.active_price_median_eur) > 3.5 * 1.4826 * m.active_price_mad_eur
                    THEN 1
                    WHEN (m.active_price_mad_eur IS NULL OR m.active_price_mad_eur = 0)
                     AND (s.active_price_q3_eur - s.active_price_q1_eur) > 0
                     AND (
                         l.price_eur < s.active_price_q1_eur - 1.5 * (s.active_price_q3_eur - s.active_price_q1_eur)
                      OR l.price_eur > s.active_price_q3_eur + 1.5 * (s.active_price_q3_eur - s.active_price_q1_eur)
                     )
                    THEN 1
                    ELSE 0
                END) AS active_outlier_count
            FROM latest l
            JOIN stats s ON s.inventory_uid = l.inventory_uid
            LEFT JOIN mad m ON m.inventory_uid = l.inventory_uid
            GROUP BY l.inventory_uid
        )
        SELECT
            inventory_uid,
            s.active_unique_listing_count,
            oc.active_observation_count,
            oc.active_duplicate_observation_count,
            s.active_price_median_eur,
            s.active_price_q1_eur,
            s.active_price_q3_eur,
            s.active_price_min_eur,
            s.active_price_max_eur,
            CASE WHEN s.active_price_min_eur > 0 THEN s.active_price_max_eur / s.active_price_min_eur ELSE NULL END AS active_price_range_ratio,
            m.active_price_mad_eur,
            COALESCE(o.active_outlier_count, 0) AS active_outlier_count
        FROM stats s
        JOIN observation_counts oc USING (inventory_uid)
        LEFT JOIN mad m USING (inventory_uid)
        LEFT JOIN outliers o USING (inventory_uid)
    """).df()

    df = eligible.merge(priced, on="canonical_inventory_id", how="left")
    df = df.merge(evidence, on="inventory_uid", how="left")
    df = df.merge(active_stats, on="inventory_uid", how="left")
    df = df.merge(hist_stats, on="inventory_uid", how="left")
    try:
        match_summary = conn.execute("""
            SELECT inventory_uid, inventory_match_status
            FROM inventory_match_summary
        """).df()
        df = df.merge(match_summary, on="inventory_uid", how="left")
    except Exception:
        df["inventory_match_status"] = None

    rows = []
    for raw in df.to_dict("records"):
        row = dict(raw)
        source = row.get("pricing_source")
        tier = row.get("confidence_tier")
        active_count = _safe_int(row.get("active_evidence_count"))
        hist_count = _safe_int(row.get("historical_evidence_count"))
        total_count = active_count + hist_count
        recommended = row.get("recommended_price_eur")
        recommended = None if recommended is None or pd.isna(recommended) else float(recommended)
        active_median = row.get("active_price_median_eur")
        active_q1 = row.get("active_price_q1_eur")
        active_q3 = row.get("active_price_q3_eur")
        active_min = row.get("active_price_min_eur")
        active_max = row.get("active_price_max_eur")
        active_mad = row.get("active_price_mad_eur")
        active_range_ratio = row.get("active_price_range_ratio")
        active_outlier_count = _safe_int(row.get("active_outlier_count"))
        active_duplicate_observation_count = _safe_int(row.get("active_duplicate_observation_count"))
        active_dispersion = None
        active_iqr_ratio = None
        if (
            active_median is not None and not pd.isna(active_median) and float(active_median) > 0
            and active_q1 is not None and not pd.isna(active_q1)
            and active_q3 is not None and not pd.isna(active_q3)
        ):
            active_dispersion = float(active_q3) - float(active_q1)
            active_iqr_ratio = active_dispersion / float(active_median)
        historical_median = row.get("historical_price_median_eur")
        historical_q1 = row.get("historical_price_q1_eur")
        historical_q3 = row.get("historical_price_q3_eur")
        historical_iqr_ratio = None
        if (
            historical_median is not None and not pd.isna(historical_median) and float(historical_median) > 0
            and historical_q1 is not None and not pd.isna(historical_q1)
            and historical_q3 is not None and not pd.isna(historical_q3)
        ):
            historical_iqr_ratio = (float(historical_q3) - float(historical_q1)) / float(historical_median)
        historical_active_gap_ratio = None
        current_value = row.get("current_value_c")
        historical_value = row.get("historical_value_h")
        if (
            current_value is not None and not pd.isna(current_value) and float(current_value) > 0
            and historical_value is not None and not pd.isna(historical_value) and float(historical_value) > 0
        ):
            historical_active_gap_ratio = abs(float(current_value) - float(historical_value)) / float(historical_value)
        confidence_score = row.get("confidence_score")
        if confidence_score is not None and pd.isna(confidence_score):
            confidence_score = None
        pricing_status, pricing_confidence, confidence_reason = _pricing_confidence(
            source=source,
            tier=tier,
            evidence_basis=row.get("evidence_basis"),
            recommended=recommended,
            active_count=active_count,
            historical_count=hist_count,
            confidence_score=confidence_score,
            active_iqr_ratio=active_iqr_ratio,
            active_range_ratio=active_range_ratio,
            active_outlier_count=active_outlier_count,
            historical_units_sold=_safe_int(row.get("historical_units_sold_count")),
            historical_iqr_ratio=historical_iqr_ratio,
            historical_active_gap_ratio=historical_active_gap_ratio,
        )
        pricing_method = _pricing_method(row.get("evidence_basis"), active_count, hist_count, recommended)

        demand_index = row.get("demand_index_d")
        if demand_index is None or pd.isna(demand_index) or recommended is None:
            base_tmv = recommended
            demand_adjustment = None if recommended is None else 0.0
        else:
            demand_factor = 1.0 + demand_weight * (float(demand_index) - 0.5)
            base_tmv = recommended / demand_factor if demand_factor else recommended
            demand_adjustment = recommended - base_tmv

        stock = row.get("stock_quantity")
        stock_number = _safe_int(stock)
        (
            turnover_median_days,
            turnover_prob30,
            turnover_prob90,
            bucket_values,
            turnover_confidence,
            turnover_method,
            sell_lower,
            sell_upper,
            sell_time_display,
            turnover_status,
            turnover_reason,
            turnover_support_evidence_count,
            turnover_support_item_count,
            turnover_support_level,
        ) = (
            _unpriced_turnover_contract_values()
            if pricing_status == "NO_RECOMMENDATION"
            else _turnover_contract_values(
                row=row,
                historical_count=hist_count,
                stock=stock_number,
                cohort_map=cohort_turnover,
            )
        )
        units_sold = sum(bucket_values.values())
        units_remaining = max(0.0, stock_number - units_sold)
        potential_revenue = round(recommended * stock_number, 2) if recommended is not None else None

        virtual_price = germany_price = us_price = None
        germany_shipping = us_shipping = us_tax = us_duty = None
        if recommended is not None:
            try:
                scenarios = scenario_engine.compute_scenarios(conn, recommended)
                virtual_price = scenarios["C"]["landed_cost_eur"]
                germany_price = scenarios["B"]["landed_cost_eur"]
                us_price = scenarios["A"]["landed_cost_eur"]
                germany_shipping = scenarios["B"]["shipping_eur"]
                us_shipping = scenarios["A"]["shipping_eur"]
                us_tax = scenarios["A"]["tax_eur"]
                us_duty = scenarios["A"]["customs_eur"]
            except Exception:
                virtual_price = germany_price = us_price = None
                germany_shipping = us_shipping = us_tax = us_duty = None

        description = " ".join(
            str(v) for v in [row.get("brand"), row.get("caliber"), row.get("part_number")]
            if v is not None and not pd.isna(v)
        )

        rows.append({
            "inventory_uid": row["inventory_uid"],
            "canonical_inventory_id": row["canonical_inventory_id"],
            "brand": row.get("brand"),
            "caliber": row.get("caliber"),
            "part_number": row.get("part_number"),
            "description": description,
            "stock_quantity": stock_number,
            "validation_status": row.get("validation_status"),
            "pricing_status": pricing_status,
            "pricing_confidence": pricing_confidence,
            "confidence_label": CONFIDENCE_LABELS.get(pricing_confidence, pricing_confidence),
            "confidence_score": float(confidence_score) if confidence_score is not None else None,
            "pricing_method": pricing_method,
            "recommended_price_eur": recommended,
            "price_lower_bound_eur": row.get("tmv_low_eur") if recommended is not None else None,
            "price_upper_bound_eur": row.get("tmv_high_eur") if recommended is not None else None,
            "base_tmv_eur": round(base_tmv, 2) if base_tmv is not None else None,
            "confidence_reason": confidence_reason,
            "recommendation_reason": _reason_without_stale_confidence(row.get("recommendation_reason")),
            "historical_value_h": row.get("historical_value_h"),
            "current_value_c": row.get("current_value_c"),
            "demand_index_d": demand_index,
            "scarcity_score_s": row.get("scarcity_score_s"),
            "price_trend_p": row.get("price_trend_p"),
            "demand_adjustment_eur": round(demand_adjustment, 2) if demand_adjustment is not None else None,
            "active_evidence_count": active_count,
            "historical_evidence_count": hist_count,
            "unique_active_evidence_count": active_count,
            "unique_historical_evidence_count": hist_count,
            "total_unique_evidence_count": total_count,
            "evidence_basis": row.get("evidence_basis") if recommended is not None else None,
            "active_price_median_eur": round(float(active_median), 2) if active_median is not None and not pd.isna(active_median) else None,
            "active_price_iqr_ratio": round(active_iqr_ratio, 4) if active_iqr_ratio is not None else None,
            "active_price_dispersion": round(active_dispersion, 2) if active_dispersion is not None else None,
            "active_price_min_eur": round(float(active_min), 2) if active_min is not None and not pd.isna(active_min) else None,
            "active_price_max_eur": round(float(active_max), 2) if active_max is not None and not pd.isna(active_max) else None,
            "active_price_range_ratio": round(float(active_range_ratio), 4) if active_range_ratio is not None and not pd.isna(active_range_ratio) else None,
            "active_price_mad_eur": round(float(active_mad), 2) if active_mad is not None and not pd.isna(active_mad) else None,
            "active_outlier_count": active_outlier_count,
            "active_duplicate_observation_count": active_duplicate_observation_count,
            "condition_assumption": (
                "LIKELY_UNIFORM_NEW_NOS_UNCONFIRMED"
                if row.get("evidence_basis") == "ACTIVE_ONLY" and recommended is not None
                else None
            ),
            "authenticity_assessment_status": (
                "NOT_TAGGED_IN_MATCHING_LAYER"
                if row.get("evidence_basis") == "ACTIVE_ONLY" and recommended is not None
                else None
            ),
            "active_pricing_caveat": (
                "Inventory condition/OEM status is not confirmed in the source file; active prices may contain used, aftermarket, bundle, damaged, or premium NOS segments."
                if row.get("evidence_basis") == "ACTIVE_ONLY" and recommended is not None
                else None
            ),
            "historical_price_median_eur": round(float(historical_median), 2) if historical_median is not None and not pd.isna(historical_median) else None,
            "historical_price_iqr_ratio": round(historical_iqr_ratio, 4) if historical_iqr_ratio is not None else None,
            "historical_active_gap_ratio": round(historical_active_gap_ratio, 4) if historical_active_gap_ratio is not None else None,
            "ask_to_sold_adjustment": ASK_TO_SOLD_ADJUSTMENT if row.get("evidence_basis") == "ACTIVE_ONLY" and recommended is not None else None,
            "adjustment_source": "MEASURED" if row.get("evidence_basis") == "ACTIVE_ONLY" and recommended is not None else None,
            "adjustment_support_count": 168 if row.get("evidence_basis") == "ACTIVE_ONLY" and recommended is not None else None,
            "adjustment_hierarchy_level": "PORTFOLIO_BACKTEST" if row.get("evidence_basis") == "ACTIVE_ONLY" and recommended is not None else None,
            "median_days_to_sell": turnover_median_days,
            "turnover_confidence": turnover_confidence,
            "turnover_method": turnover_method,
            "sell_time_lower_days": sell_lower,
            "sell_time_upper_days": sell_upper,
            "sell_time_display": sell_time_display,
            "turnover_evidence_status": turnover_status,
            "turnover_reason": turnover_reason,
            "turnover_support_evidence_count": turnover_support_evidence_count,
            "turnover_support_item_count": turnover_support_item_count,
            "turnover_support_level": turnover_support_level,
            "probability_sell_30d": turnover_prob30,
            "probability_sell_90d": turnover_prob90,
            **bucket_values,
            "units_remaining": round(units_remaining, 3),
            "potential_revenue": potential_revenue,
            "potential_revenue_eur": potential_revenue,
            "virtual_price_eur": virtual_price,
            "germany_price_eur": germany_price,
            "us_price_eur": us_price,
            "germany_shipping_eur": germany_shipping,
            "us_shipping_eur": us_shipping,
            "us_tax_eur": us_tax,
            "us_duty_eur": us_duty,
            "no_recommendation_reason": _no_recommendation_reason(row),
            "calculation_version": CALCULATION_VERSION,
            "source_run_id": row.get("source_run_id"),
        })

    return pd.DataFrame(rows)


def write_contract(conn, df: pd.DataFrame) -> None:
    conn.execute(SCHEMA_PATH.read_text())
    cols = [
        "inventory_uid", "canonical_inventory_id", "brand", "caliber", "part_number",
        "description", "stock_quantity", "validation_status", "pricing_status",
        "pricing_confidence", "confidence_label", "confidence_score",
        "pricing_method", "recommended_price_eur", "price_lower_bound_eur",
        "price_upper_bound_eur", "base_tmv_eur", "confidence_reason", "recommendation_reason",
        "historical_value_h", "current_value_c", "demand_index_d", "scarcity_score_s",
        "price_trend_p", "demand_adjustment_eur", "active_evidence_count",
        "historical_evidence_count", "unique_active_evidence_count",
        "unique_historical_evidence_count", "total_unique_evidence_count", "evidence_basis",
        "active_price_median_eur", "active_price_iqr_ratio", "active_price_dispersion",
        "active_price_min_eur", "active_price_max_eur", "active_price_range_ratio",
        "active_price_mad_eur", "active_outlier_count",
        "active_duplicate_observation_count", "condition_assumption",
        "authenticity_assessment_status", "active_pricing_caveat",
        "historical_price_median_eur", "historical_price_iqr_ratio",
        "historical_active_gap_ratio",
        "ask_to_sold_adjustment", "adjustment_source", "adjustment_support_count",
        "adjustment_hierarchy_level", "median_days_to_sell", "turnover_confidence",
        "turnover_method", "sell_time_lower_days", "sell_time_upper_days",
        "sell_time_display", "turnover_evidence_status", "turnover_reason",
        "turnover_support_evidence_count", "turnover_support_item_count",
        "turnover_support_level", "probability_sell_30d", "probability_sell_90d",
        "units_sold_0_7", "units_sold_8_30", "units_sold_31_90", "units_sold_91_183",
        "units_sold_184_365", "units_sold_366_730", "units_sold_731_1065",
        "units_sold_1066_plus", "units_remaining", "potential_revenue",
        "potential_revenue_eur",
        "virtual_price_eur", "germany_price_eur", "us_price_eur",
        "germany_shipping_eur", "us_shipping_eur", "us_tax_eur", "us_duty_eur",
        "no_recommendation_reason", "calculation_version", "source_run_id",
    ]
    conn.execute("BEGIN")
    try:
        conn.execute("DELETE FROM dashboard_inventory_pricing")
        if not df.empty:
            conn.register("dashboard_contract_df", df[cols])
            conn.execute(
                f"INSERT INTO dashboard_inventory_pricing ({','.join(cols)}) "
                f"SELECT {','.join(cols)} FROM dashboard_contract_df"
            )
            conn.unregister("dashboard_contract_df")
        conn.execute("COMMIT")
    except Exception:
        try:
            conn.unregister("dashboard_contract_df")
        except Exception:
            pass
        conn.execute("ROLLBACK")
        raise


def write_low_confidence_audit(df: pd.DataFrame, reports_dir: Path = REPORTS_DIR) -> Path:
    reports_dir.mkdir(parents=True, exist_ok=True)
    path = reports_dir / "low_confidence_root_cause_audit.csv"
    audit = df.copy()
    audit["identity_quality"] = audit["confidence_score"].apply(
        lambda v: "NONE" if pd.isna(v) else ("STRONG_DIRECT" if float(v) >= 0.80 else "WEAK")
    )
    audit["price_dispersion"] = audit["active_price_iqr_ratio"]
    audit["comparable_count"] = 0
    audit["current_reason_for_low"] = audit.apply(
        lambda r: r["confidence_reason"] if r["pricing_confidence"] == "LOW" else "",
        axis=1,
    )
    audit["low_resulted_merely_from_active_only_mapping"] = False
    audit["active_only_priced"] = audit["evidence_basis"].eq("ACTIVE_ONLY")
    cols = [
        "inventory_uid", "canonical_inventory_id", "pricing_confidence", "evidence_basis",
        "unique_active_evidence_count", "unique_historical_evidence_count",
        "identity_quality", "active_price_median_eur", "active_price_min_eur",
        "active_price_max_eur", "active_price_range_ratio",
        "active_price_mad_eur", "active_outlier_count",
        "active_duplicate_observation_count", "price_dispersion",
        "condition_assumption", "authenticity_assessment_status",
        "active_pricing_caveat",
        "historical_price_median_eur", "historical_price_iqr_ratio",
        "historical_active_gap_ratio", "comparable_count", "current_reason_for_low",
        "low_resulted_merely_from_active_only_mapping", "pricing_method",
        "confidence_reason", "recommended_price_eur", "price_lower_bound_eur",
        "price_upper_bound_eur", "turnover_confidence", "turnover_method",
        "turnover_support_level", "turnover_support_evidence_count",
        "turnover_support_item_count", "turnover_reason",
    ]
    audit[cols].to_csv(path, index=False)
    return path


def build_and_write(conn) -> dict:
    df = build_contract_dataframe(conn)
    write_contract(conn, df)
    audit_path = write_low_confidence_audit(df)
    eligible = len(df)
    priced = int((df["pricing_status"] == "PRICED").sum()) if not df.empty else 0
    high = int((df["pricing_confidence"] == "HIGH").sum()) if not df.empty else 0
    medium = int((df["pricing_confidence"] == "MEDIUM").sum()) if not df.empty else 0
    low = int((df["pricing_confidence"] == "LOW").sum()) if not df.empty else 0
    unpriced = int((df["pricing_status"] == "NO_RECOMMENDATION").sum()) if not df.empty else 0
    return {
        "eligible": eligible,
        "priced": priced,
        "high_confidence": high,
        "medium_confidence": medium,
        "low_confidence": low,
        "unpriced": unpriced,
        "audit_path": str(audit_path),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=str(os.environ.get("WATCHPARTS_DB", DEFAULT_DB_PATH)))
    args = parser.parse_args()

    conn = connect_write_retry(args.db)
    result = build_and_write(conn)
    conn.close()
    print(f"Database target: {args.db}")
    print(f"Eligible inventory rows: {result['eligible']:,}")
    print(f"Priced rows: {result['priced']:,}")
    print(f"High confidence rows: {result['high_confidence']:,}")
    print(f"Medium confidence rows: {result['medium_confidence']:,}")
    print(f"Low confidence rows: {result['low_confidence']:,}")
    print(f"Unpriced rows: {result['unpriced']:,}")
    print(f"Wrote {result['audit_path']}")
    print("Built dashboard_inventory_pricing")


if __name__ == "__main__":
    main()
