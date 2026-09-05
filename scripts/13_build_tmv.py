"""
13_build_tmv.py
===============
Module 6-7: TMV (True Market Value) + supporting features, computed ONLY from
MATCH_CONFIRMED evidence (scripts/06_decide_matches.py). Never uses
REVIEW_REQUIRED / INSUFFICIENT_EVIDENCE / NO_MATCH rows.

Formula (ARCHITECTURE_CURRENT.md §4), applied per inventory item i:

  H_i  historical value   = Σⱼ wⱼ·priceⱼ·volⱼ / Σⱼ wⱼ·volⱼ ,  wⱼ = exp(−ln2·age_months/12)
                            (recency half-life 12 months; VCP rows use avg_price_eur/total_sold,
                             eBay-sold rows use observed price / vol=1)
  C_i  current value      = median(matched active listing price_eur) × δ ,  δ = 0.90 ask→sold
  D_i  demand index       = percentile_rank(sales_velocity within caliber peer group)
  S_i  scarcity/dynamics  = 1 − percentile_rank(active_count/(total_sold+1) within caliber peer group)
  P_i  price trend        = clip( OLS slope(price vs month, eBay-sold) / H_i × 100 , −10%, +10% )

  w_H = n_hist/(n_hist+n_active),  w_C = 1−w_H
  TMV_i = (w_H·H_i + w_C·C_i) × (1 + α·P_i) × (1 + β·(S_i−0.5))    α=1.0, β=0.10

TMV_i is a SELLING PRICE in EUR (the "zero baseline" the teacher asked for — no
strategic preference applied; the Time/Price sliders live in the dashboard, on
top of this). The three shipping/tax scenarios (A US / B DE / C virtual) are fee
STACKS on top of the same TMV for display, never separately-computed TMVs
(ARCHITECTURE_CURRENT.md §6; teacher: "comparison based exclusively on the
selling price").

Confidence tier reflects the evidence actually available for each item:
  HIGH   both historical AND active evidence, historical n≥3
  MEDIUM historical only (n≥1) OR active only (n≥2)
  LOW    thin (single-observation) evidence

Writes: tmv_results, feat_pricing, feat_demand, feat_market_supply.
Full rebuild each run (same idempotency discipline as clean_*/06).

Usage:
    python scripts/13_build_tmv.py
    python scripts/13_build_tmv.py --db /tmp/copy.duckdb
"""

from __future__ import annotations

import argparse
import json
import math
import os
from datetime import date
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).parent.parent
# DB target: WATCHPARTS_DB env var (disposable copy) > default live DB.
DEFAULT_DB_PATH = BASE_DIR / "database" / "watchparts.duckdb"
DB_PATH = Path(os.environ["WATCHPARTS_DB"]) if os.environ.get("WATCHPARTS_DB") else DEFAULT_DB_PATH
SCHEMA_PATH = BASE_DIR / "scripts" / "schema.sql"

# δ — active listings are ASKING prices, discounted to an expected SOLD price.
# CHANGED 0.90 → 0.79 after the holdout backtest (reports/tmv_backtest_report.md):
# the real ask→sold ratio measured on 168 items / 3,654 active listings has
# median 0.794, not the placeholder 0.90 the design doc always flagged as
# 'assumed_default'. See docs/TMV_BACKTEST_AND_MODEL_REVISION.md.
ASK_TO_SOLD_DISCOUNT = 0.79
ALPHA_TREND = 1.0               # weight on price trend P_i
BETA_SCARCITY = 0.10            # weight on scarcity S_i
RECENCY_HALFLIFE_MONTHS = 12.0

BUCKETS = [(0, 7), (8, 30), (31, 90), (91, 183), (184, 365), (366, 730), (731, 1065), (1066, 100000)]
BUCKET_LABELS = ["0-7", "8-30", "31-90", "91-183", "184-365", "366-730", "731-1065", "1066+"]


def log(msg=""):
    print(msg)


def _now_ref(dates: pd.Series) -> date:
    """Reference 'today' = the most recent observation in the data (keeps ages
    stable/reproducible regardless of the wall clock at run time)."""
    d = pd.to_datetime(dates, errors="coerce").max()
    return d.date() if pd.notna(d) else date.today()


def _age_months(d, ref: date) -> float:
    if pd.isna(d):
        return RECENCY_HALFLIFE_MONTHS  # unknown date → one half-life of decay, never dropped
    d = pd.to_datetime(d).date()
    return max(0.0, (ref - d).days / 30.44)


def load_confirmed_evidence(conn, evidence_source: str = "MATCH_CONFIRMED") -> dict:
    """Return per inventory_uid the confirmed historical + active observations.

    evidence_source:
      'MATCH_CONFIRMED' (default) -- the human-governed path, UNCHANGED
        behavior: consumes ONLY match_decisions rows with
        match_status='MATCH_CONFIRMED' (validation_policy-approved).
      'ALGORITHMIC_AUTO_HIGH' -- owner-directed autonomous path
        (docs/AUTONOMOUS_PRODUCTION_READINESS_REPORT.md): consumes
        evidence_confidence_classification rows tiered AUTO_CONFIRMED or
        HIGH_CONFIDENCE by scripts/21_evidence_confidence_engine.py.
        NEVER reads match_status or validation_policy -- entirely separate
        from the governed gate, never conflated with it.
    """
    inv = conn.execute("""
        SELECT inventory_uid, canonical_inventory_id, brand, caliber, part_number, stock
        FROM staging_inventory WHERE validation_status <> 'FAIL'
    """).df().set_index("inventory_uid")

    if evidence_source == "MATCH_CONFIRMED":
        # PORT ADAPTATION (A): join on A's stable evidence identity `evidence_uid`
        # (match_decisions.evidence_uid = <stg>.stable_evidence_uid), NOT B's
        # positional `source_id`. Consumes ONLY MATCH_CONFIRMED — never
        # LOW_CONFIDENCE_CANDIDATE / REVIEW_REQUIRED / raw candidate rows.
        evidence_cte = "match_decisions"
        join_col = "evidence_uid"
        status_filter = "match_status='MATCH_CONFIRMED'"
    elif evidence_source == "ALGORITHMIC_AUTO_HIGH":
        evidence_cte = "evidence_confidence_classification"
        join_col = "evidence_uid"
        status_filter = "confidence_tier IN ('AUTO_CONFIRMED', 'HIGH_CONFIDENCE')"
    else:
        raise ValueError(f"Unknown evidence_source: {evidence_source!r}")

    vcp = conn.execute(f"""
        SELECT md.inventory_uid, v.avg_price_eur AS price, v.total_sold AS vol, v.last_sold_date AS dt
        FROM {evidence_cte} md JOIN stg_historical_vcp_aggregate v ON md.{join_col} = v.stable_evidence_uid
        WHERE {status_filter} AND md.source_table='match_candidates_vcp'
          AND v.avg_price_eur IS NOT NULL AND v.avg_price_eur > 0
    """).df()

    # best_offer carried through for price-reliability classification: an
    # accepted-offer sale's displayed price is a proxy, not the transacted price.
    ebay = conn.execute(f"""
        SELECT md.inventory_uid, e.price_eur AS price, e.sold_date AS dt,
               COALESCE(e.has_best_offer_option, FALSE) AS best_offer
        FROM {evidence_cte} md JOIN stg_historical_ebay_sold e ON md.{join_col} = e.stable_evidence_uid
        WHERE {status_filter} AND md.source_table='match_candidates_ebay_sold'
          AND e.price_eur IS NOT NULL AND e.price_eur > 0
    """).df()

    # DEDUP FIX (found during pricing quality audit, 2026-07-31): the same
    # physical listing is often collected multiple times across repeated
    # query fetches, all sharing one stable_evidence_uid but with distinct
    # raw row ids (docs/PRICING_QUALITY_AUDIT_FINDINGS.md) -- e.g. one €20
    # listing observed 12 times. Joining directly on stable_evidence_uid
    # without deduplication let that single listing count as 12 independent
    # market observations, skewing the active-price median. Fixed by taking
    # exactly one (the most recently fetched) row per distinct evidence_uid
    # -- one physical listing = one price observation, always.
    active = conn.execute(f"""
        SELECT md.inventory_uid, a.price_eur AS price, a.marketplace
        FROM {evidence_cte} md
        JOIN (
            SELECT stable_evidence_uid, price_eur, marketplace,
                   ROW_NUMBER() OVER (PARTITION BY stable_evidence_uid ORDER BY fetched_at DESC) AS rn
            FROM stg_active_targeted
        ) a ON md.{join_col} = a.stable_evidence_uid AND a.rn = 1
        WHERE {status_filter} AND md.source_table='match_candidates_active'
          AND a.price_eur IS NOT NULL AND a.price_eur > 0
    """).df()

    return {"inv": inv, "vcp": vcp, "ebay": ebay, "active": active}


def _pct_rank_within_group(df: pd.DataFrame, value_col: str, group_col: str) -> pd.Series:
    """Percentile rank of value within its caliber group (>=3 members); otherwise
    fall back to the global percentile rank. Robust to tiny peer groups."""
    global_rank = df[value_col].rank(pct=True)
    out = global_rank.copy()
    for g, idx in df.groupby(group_col).groups.items():
        if len(idx) >= 3:
            out.loc[idx] = df.loc[idx, value_col].rank(pct=True)
    return out.fillna(0.5)


def _get_tmv_parameter(conn, name: str, default: float = 0.0) -> float:
    """Read a configurable TMV parameter from ref_tmv_parameters
    (docs/TMV_DEMAND_PARAMETER_DESIGN.md). Returns `default` if the table
    doesn't exist yet, the row is absent, or active_flag is FALSE -- inactive
    means "read the value but apply it as a no-op", not "value missing"."""
    try:
        row = conn.execute(
            "SELECT parameter_value, active_flag FROM ref_tmv_parameters WHERE parameter_name = ?", [name]
        ).fetchone()
    except Exception:
        return default
    if row is None:
        return default
    value, active = row
    return float(value) if active else default


def build(conn, evidence_source: str = "MATCH_CONFIRMED") -> dict:
    demand_weight = _get_tmv_parameter(conn, "demand_weight", default=0.0)
    ev = load_confirmed_evidence(conn, evidence_source=evidence_source)
    inv, vcp, ebay, active = ev["inv"], ev["vcp"], ev["ebay"], ev["active"]

    all_dates = pd.concat([vcp["dt"], ebay["dt"]], ignore_index=True) if len(vcp) or len(ebay) else pd.Series([], dtype="object")
    ref = _now_ref(all_dates)
    hist_min = pd.to_datetime(all_dates, errors="coerce").min()
    dataset_months = max(1.0, ((ref - hist_min.date()).days / 30.44)) if pd.notna(hist_min) else 12.0

    # ── TURNOVER λ INPUTS (backtest revision — docs/TMV_BACKTEST_AND_MODEL_REVISION.md §7) ──
    # The original λ = total_sold / dataset_months divided by the FULL ~3-year
    # span, producing a LIFETIME-AVERAGE rate. The holdout calibration showed
    # that under-predicts current velocity ~5× because the market is
    # ACCELERATING (26/mo long-run vs 80/mo recent). Two fixes, together:
    #
    #  (A) DILUTION FIX — replace the 3-year denominator with a recency-weighted
    #      EFFECTIVE window (same 12-mo half-life the price side already uses),
    #      so λ reflects current pace, not lifetime average. Internally
    #      consistent with H.
    #  (B) TRENDING HAZARD — fit a global monthly growth factor e^g from the
    #      monthly sale-count series (ln(count) ~ a + g·t) and project λ forward
    #      along that trend, instead of assuming a flat (constant-hazard) rate.
    #      Falls back to constant-hazard where the trend can't be fit.
    #
    # DISCLOSED LIMITATION (do not overclaim): even with both, calibration
    # improved only ~5.0×→4.0×. The residual gap is STRUCTURAL — the steep part
    # of the acceleration postdates the training window (unforecastable), and
    # the ~90-day eBay export compresses recent sales (inflates the observed
    # "actual"). Turnover remains a DIRECTIONAL estimate, not an exact sell-date.
    _mseries = (
        pd.concat([vcp[["dt", "vol"]].assign(vol=vcp["vol"].fillna(1).clip(lower=1)),
                   ebay[["dt"]].assign(vol=1)], ignore_index=True)
        .dropna(subset=["dt"]).set_index("dt")["vol"].resample("MS").sum()
        if (len(vcp) or len(ebay)) else pd.Series([], dtype="float")
    )
    growth_g = 0.0  # monthly log-growth of market sale rate; 0 ⇒ constant-hazard
    if len(_mseries) >= 4 and (_mseries > 0).sum() >= 3:
        _t = np.arange(len(_mseries))
        _pos = _mseries.values > 0
        try:
            _coef = np.polyfit(_t[_pos], np.log(_mseries.values[_pos]), 1)
            # clip to a sane band: never let a noisy fit imply >±8%/mo drift
            growth_g = float(np.clip(_coef[0], -0.08, 0.08))
        except Exception:
            growth_g = 0.0
    # Effective recency window (months): ∫ e^{−ln2·t/HL} dt over the span.
    _hl = RECENCY_HALFLIFE_MONTHS
    eff_window_months = max(1.0, (_hl / math.log(2)) * (1 - math.exp(-math.log(2) * dataset_months / _hl)))

    confirmed_uids = sorted(set(vcp["inventory_uid"]) | set(ebay["inventory_uid"]) | set(active["inventory_uid"]))
    rows = []
    for uid in confirmed_uids:
        if uid not in inv.index:
            continue
        meta = inv.loc[uid]
        v = vcp[vcp["inventory_uid"] == uid]
        e = ebay[ebay["inventory_uid"] == uid]
        a = active[active["inventory_uid"] == uid]

        # ---- historical observations (price, vol, date) ----
        hobs = []
        for _, r in v.iterrows():
            hobs.append((float(r["price"]), max(1, int(r["vol"]) if pd.notna(r["vol"]) else 1), r["dt"]))
        for _, r in e.iterrows():
            hobs.append((float(r["price"]), 1, r["dt"]))
        n_hist = len(hobs)
        total_sold = int(sum(o[1] for o in hobs))
        # recency-weighted sales (same 12-mo half-life as H) — feeds the
        # dilution-fixed / trending λ below. Recent sales count near-full,
        # old ones fade, so λ tracks current pace not lifetime average.
        weighted_sold = sum(
            math.exp(-math.log(2) * _age_months(dt, ref) / RECENCY_HALFLIFE_MONTHS) * vol
            for _, vol, dt in hobs
        )

        # ---- H_i: recency (half-life) × volume weighted ----
        H = None
        if hobs:
            num = den = 0.0
            for price, vol, dt in hobs:
                w = math.exp(-math.log(2) * _age_months(dt, ref) / RECENCY_HALFLIFE_MONTHS)
                num += w * price * vol
                den += w * vol
            H = num / den if den > 0 else None

        # ---- C_i: median active ask × δ ----
        n_active = len(a)
        C = float(np.median(a["price"])) * ASK_TO_SOLD_DISCOUNT if n_active else None
        active_count = n_active

        # ---- P_i: OLS slope of eBay-sold price vs month ----
        P = 0.0
        if len(e) >= 2:
            months = pd.to_datetime(e["dt"], errors="coerce")
            if months.notna().sum() >= 2:
                x = (months - months.min()).dt.days.values / 30.44
                y = e["price"].values.astype(float)
                mask = ~np.isnan(x)
                if mask.sum() >= 2 and np.ptp(x[mask]) > 0:
                    slope = np.polyfit(x[mask], y[mask], 1)[0]  # EUR per month
                    base = H if H else float(np.median(y))
                    P = float(np.clip((slope / base) if base else 0.0, -0.10, 0.10))

        # velocity / scarcity raw (percentile-ranked after the loop)
        velocity = total_sold / dataset_months if dataset_months else 0.0
        scarcity_raw = active_count / (total_sold + 1)

        last_dt = pd.to_datetime(pd.concat([v["dt"], e["dt"]]), errors="coerce").max() if (len(v) or len(e)) else pd.NaT

        rows.append({
            "inventory_uid": uid,
            "canonical_inventory_id": meta["canonical_inventory_id"],
            "brand": meta["brand"], "caliber": meta["caliber"], "part_number": meta["part_number"],
            "stock": int(meta["stock"]) if pd.notna(meta["stock"]) else None,
            "H": H, "C": C, "P": P,
            "n_hist": n_hist, "n_active": n_active, "total_sold": total_sold,
            "weighted_sold": weighted_sold,
            "active_count": active_count, "velocity": velocity, "scarcity_raw": scarcity_raw,
            "last_sold_date": last_dt.date() if pd.notna(last_dt) else None,
        })

    df = pd.DataFrame(rows)
    if df.empty:
        log("No confirmed evidence found — nothing to compute.")
        return {"df": df}

    # ---- D_i and S_i: caliber-peer-group percentile ranks ----
    df["caliber_key"] = df["caliber"].fillna("__none__")
    df["D"] = _pct_rank_within_group(df, "velocity", "caliber_key")
    df["S"] = 1.0 - _pct_rank_within_group(df, "scarcity_raw", "caliber_key")

    # ---- HYBRID base value + composite TMV ----
    # BACKTEST-DRIVEN REVISION (reports/tmv_backtest_report.md,
    # docs/TMV_BACKTEST_AND_MODEL_REVISION.md):
    #
    # The original design BLENDED historical-sold (H) and active-ask (C) on
    # every item (w_H = nH/(nH+nA)). A leave-one-out holdout backtest against
    # the confirmed real sale prices showed that blend LOSES to a
    # historical-only estimate at EVERY tested C-weight and EVERY tested δ:
    # active-ask prices for vintage parts are a high-variance signal (measured
    # ask→sold ratio ranges 0.10–1.53 per item), so mixing them INTO the point
    # estimate drags accuracy down (median APE 27% blended vs 21% H-only;
    # ±20% hit-rate 39% vs 49%).
    #
    # So the estimate is now a HYBRID, not a blend:
    #   • historical evidence exists  → TMV is HISTORICAL-ONLY (H), the
    #     accurate signal. w_H=1, w_C=0. (Active data is still surfaced as
    #     market context downstream — it is not discarded, just kept OUT of
    #     the point estimate where it hurt.)
    #   • NO historical evidence      → fall back to C = median_active_ask × δ,
    #     flagged ACTIVE_ONLY / LOW confidence with a wide band, so the ~66%
    #     of confirmed items that are active-only still get an honest,
    #     clearly-labelled value rather than none. w_H=0, w_C=1.
    # This is the ONLY change to the value math; scarcity/trend nudges,
    # confidence bands, and turnover are untouched in mechanism.
    #
    # Demand term (docs/TMV_DEMAND_PARAMETER_DESIGN.md, owner decision
    # 2026-07-30): (1 + demand_weight*(D-0.5)) -- IMPLEMENTED FRAMEWORK, NOT
    # ACTIVE. demand_weight is read from ref_tmv_parameters and defaults to
    # 0.0, making this term mathematically a no-op ((1+0*(D-0.5))==1 for any
    # D) until a backtest validates a real weight, the same discipline
    # ALPHA_TREND/BETA_SCARCITY were held to. D itself is still computed and
    # persisted (feat_demand.recency_score) -- this only controls whether it
    # moves the price.
    def _tmv(r):
        # normalize pandas NaN -> None (a None placed in a DataFrame column
        # becomes NaN, and `NaN is None` is False).
        H = None if pd.isna(r["H"]) else r["H"]
        C = None if pd.isna(r["C"]) else r["C"]
        if H is None and C is None:
            return None, None, None, "NONE"
        if H is not None:
            base = H; wH, wC = 1.0, 0.0; basis = "HISTORICAL"
        else:
            base = C; wH, wC = 0.0, 1.0; basis = "ACTIVE_ONLY"
        tmv = (base * (1 + ALPHA_TREND * r["P"]) * (1 + BETA_SCARCITY * (r["S"] - 0.5))
               * (1 + demand_weight * (r["D"] - 0.5)))
        return round(tmv, 2), round(wH, 3), round(wC, 3), basis

    tmv_vals = df.apply(_tmv, axis=1, result_type="expand")
    df["tmv"], df["w_H"], df["w_C"], df["valuation_basis"] = (
        tmv_vals[0], tmv_vals[1], tmv_vals[2], tmv_vals[3]
    )

    # ---- confidence tier ----
    # REVISED (backtest): the OLD tiering keyed HIGH on "has BOTH historical
    # AND active" — but the backtest proved active data is the WEAK signal, so
    # that made the highest-confidence tier the LEAST accurate (HIGH MAPE 86%
    # vs MEDIUM 69% — an inverted, meaningless tier). Confidence is now a
    # function of HISTORICAL (sold) evidence depth only, since that is what
    # actually drives the estimate now:
    #   HIGH   ≥3 historical observations   (the accurate, well-evidenced core)
    #   MEDIUM 1–2 historical observations
    #   LOW    no historical evidence at all → ACTIVE_ONLY fallback (the ~66%
    #          of items resting on the high-variance active-ask signal; honest
    #          low confidence, matched by a wider band below).
    def _tier(r):
        has_h = not pd.isna(r["H"])
        if not has_h:
            return "LOW"           # ACTIVE_ONLY — weakest signal, honestly flagged
        if r["n_hist"] >= 3:
            return "HIGH"
        return "MEDIUM"            # 1–2 historical observations
    df["confidence_tier"] = df.apply(_tier, axis=1)

    # ---- band: ± spread driven by confidence ----
    band = {"HIGH": 0.15, "MEDIUM": 0.25, "LOW": 0.40}
    df["tmv_low"] = (df["tmv"] * (1 - df["confidence_tier"].map(band))).round(2)
    df["tmv_high"] = (df["tmv"] * (1 + df["confidence_tier"].map(band))).round(2)

    # ---- scarcity flag ----
    def _flag(s):
        if s >= 0.66: return "SCARCE"
        if s <= 0.33: return "OVERSUPPLIED"
        return "BALANCED"
    df["scarcity_flag"] = df["S"].apply(_flag)

    # ---- Task 6: price-recommendation transparency text ----
    # recommended_price_eur stays numerically == tmv (no invented multiplier,
    # owner instruction 2026-07-30); this only states, in plain text, which
    # inputs produced that number so the dashboard can show WHY, not just WHAT.
    def _reason(r):
        trend_word = "up" if r["P"] > 0.001 else ("down" if r["P"] < -0.001 else "flat")
        if r["valuation_basis"] == "HISTORICAL":
            base_txt = f"historical sold evidence (n={r['n_hist']} sale(s))"
        else:
            base_txt = f"active asking prices only (n={r['n_active']} listing(s)), discounted {int(round((1 - ASK_TO_SOLD_DISCOUNT) * 100))}% ask→sold"
        return (
            f"Based on {base_txt}. Price trend {trend_word} ({r['P']:+.1%}), "
            f"scarcity {r['scarcity_flag'].lower()} (S={r['S']:.2f}). "
            f"Confidence: {r['confidence_tier']}."
        )
    df["recommendation_reason"] = df.apply(
        lambda r: None if r["tmv"] is None else _reason(r), axis=1
    )

    # ---- turnover: recency-weighted λ + trending hazard (revised, ARCHITECTURE §5) ----
    # DISCLAIMER (enforced — docs/MODULE4_TURNOVER.md, schema turnover_survival):
    #   "Turnover estimates selling velocity based on historical sales behavior.
    #    It is not a price elasticity model and does not estimate price response."
    # No price/TMV variable enters λ or median_days_to_sell by construction — the
    # hazard rate is a function of sold COUNT and dates only.
    # λ_now = recency-weighted sales / effective recency window  (dilution fix A).
    # Trending hazard: the market grows at monthly factor e^g, so over the next
    # t months the EXPECTED sales are the integral of λ_now·e^{g·τ}, giving an
    # effective near-term rate λ_eff = λ_now · (e^{g·H_MED} − 1)/(g·H_MED) across
    # the median horizon — i.e. λ is nudged up when the market is accelerating
    # (fix B). g=0 recovers the plain constant-hazard rate exactly (fallback).
    # P(sell within t days) and median days then use λ_eff. Still exponential in
    # form — an honest, directional constant-/trending-hazard estimate, never an
    # exact sell-date (see the limitation note above and the backtest doc).
    CAP_DAYS = 3650.0

    def _lambda_eff(lam_now: float) -> float:
        """Apply the global growth trend to a base monthly rate. Uses a 6-month
        near-term projection horizon (the calibration window) to define the
        effective forward rate; g=0 → returns lam_now unchanged."""
        if lam_now <= 0:
            return 0.0
        if abs(growth_g) < 1e-9:
            return lam_now
        horizon = 6.0
        factor = (math.exp(growth_g * horizon) - 1.0) / (growth_g * horizon)
        return lam_now * factor

    def _turn(r):
        # dilution fix: recency-weighted sales over the effective recency window,
        # not raw total over the full ~3-year span.
        lam_now = (r["weighted_sold"] / eff_window_months) if eff_window_months > 0 else 0.0
        lam = _lambda_eff(lam_now)  # trending-hazard adjusted forward rate
        if lam <= 0:
            return CAP_DAYS, 0.0, 0.0, lam
        median_days = min(CAP_DAYS, 30.0 * math.log(2) / lam)
        p30 = 1 - math.exp(-lam * 30 / 30.0)
        p90 = 1 - math.exp(-lam * 90 / 30.0)
        return round(median_days, 1), round(p30, 4), round(p90, 4), round(lam, 4)
    tt = df.apply(_turn, axis=1, result_type="expand")
    df["median_days_to_sell"], df["prob_30"], df["prob_90"], df["lambda_monthly"] = tt[0], tt[1], tt[2], tt[3]

    def _bucket(days):
        for (lo, hi), lab in zip(BUCKETS, BUCKET_LABELS):
            if lo <= days <= hi:
                return lab
        return BUCKET_LABELS[-1]
    df["turnover_bucket"] = df["median_days_to_sell"].apply(_bucket)

    # ---- expected units sold per time bucket, for stock quantity Q ----
    # Same exponential-hazard survival model as median_days_to_sell above
    # (lambda_monthly, already trending/recency-adjusted) -- no new methodology,
    # just integrated over each bucket instead of solved for the median.
    # F(t) = 1 - exp(-lam*t/30) is the probability a single unit has sold by
    # day t; expected units in bucket [lo,hi] = Q * (F(hi) - F(lo-1)), with the
    # last (open-ended) bucket taking the remaining probability mass to 1.0.
    # lam<=0 (no confirmed sales velocity) or missing/zero stock -> all-zero
    # forecast (never a fabricated distribution).
    def _surv_F(lam, t):
        t = max(0.0, t)
        return 1.0 - math.exp(-lam * t / 30.0)

    def _bucket_forecast(r):
        lam, stock = r["lambda_monthly"], r["stock"]
        if not lam or lam <= 0 or not stock or stock <= 0:
            return [{"bucket": lab, "expected_units": 0.0} for lab in BUCKET_LABELS]
        out = []
        for (lo, hi), lab in zip(BUCKETS, BUCKET_LABELS):
            f_lo = _surv_F(lam, lo - 1)
            f_hi = 1.0 if hi >= 100000 else _surv_F(lam, hi)
            out.append({"bucket": lab, "expected_units": round(stock * max(0.0, f_hi - f_lo), 3)})
        return out
    df["turnover_bucket_forecast"] = df.apply(lambda r: json.dumps(_bucket_forecast(r)), axis=1)
    df["dataset_months"] = round(dataset_months, 1)

    # PORT ADAPTATION (A): NO caliber fallback. Spec — TMV consumes ONLY
    # MATCH_CONFIRMED; items without confirmed evidence get NO value (never an
    # inferred fallback). _add_caliber_fallback() is intentionally NOT called.
    # When MATCH_CONFIRMED = 0, df is already empty and TMV writes 0 rows.

    # ---- price reliability classification (per item) ----
    #   CONFIRMED_SALE_PRICE : historical sold evidence, transacted price
    #   BEST_OFFER_PROXY     : historical sold evidence dominated by accepted-offer
    #                          listings (displayed price is a proxy, not transacted)
    #   LISTED_PRICE_ONLY    : active-only basis (asking price, discounted by delta)
    bo = (ebay.groupby("inventory_uid")["best_offer"].mean() if len(ebay) else pd.Series(dtype="float"))
    def _reliability(r):
        if r["valuation_basis"] == "ACTIVE_ONLY":
            return "LISTED_PRICE_ONLY"
        return "BEST_OFFER_PROXY" if bo.get(r["inventory_uid"], 0.0) >= 0.5 else "CONFIRMED_SALE_PRICE"
    df["price_reliability"] = df.apply(_reliability, axis=1)

    return {"df": df, "dataset_months": dataset_months, "ref": ref,
            "growth_g": growth_g, "eff_window_months": eff_window_months}


def _add_caliber_fallback(conn, df: pd.DataFrame) -> pd.DataFrame:
    """
    Give EVERY eligible inventory item a value, so the deliverable covers all
    728 items rather than only the ~563 with confirmed match evidence.

    For each eligible item that has NO evidence-based TMV row in `df`, estimate
    its value by borrowing from its peer group, in strict order (the sparse-data
    hierarchy named in ARCHITECTURE_CURRENT.md §4: item → caliber → brand →
    global): median evidence-based TMV of confirmed items in the SAME
    brand+caliber; else same brand; else global. These rows are flagged
    valuation_basis='CALIBER_ESTIMATED', confidence_tier='LOW', with the widest
    band — an INFERRED value ("parts like this typically sell for ~€X"),
    explicitly NOT an evidence-based one, and never mistaken for it. Turnover is
    left null/uninformative for these (no sold history of their own).

    Does nothing if there are no confirmed rows to borrow from (returns df
    unchanged) — never fabricates a value out of thin air.
    """
    if df.empty:
        return df
    evidence = df[df["valuation_basis"].isin(["HISTORICAL", "ACTIVE_ONLY"])].copy()
    if evidence.empty:
        return df

    global_med = float(evidence["tmv"].median())
    brand_med = evidence.groupby("brand")["tmv"].median().to_dict()
    bc_med = evidence.groupby(["brand", "caliber"])["tmv"].median().to_dict()

    inv = conn.execute("""
        SELECT canonical_inventory_id, inventory_uid, brand, caliber, part_number, stock
        FROM staging_inventory WHERE validation_status <> 'FAIL'
    """).df()
    have = set(df["canonical_inventory_id"])
    missing = inv[~inv["canonical_inventory_id"].isin(have)]
    if missing.empty:
        return df

    LOW_BAND = 0.40  # same as the LOW tier band elsewhere; wide by design
    rows = []
    for r in missing.itertuples():
        b, cal = r.brand, r.caliber
        if (b, cal) in bc_med and pd.notna(bc_med[(b, cal)]):
            est, basis_level = bc_med[(b, cal)], "caliber"
        elif b in brand_med and pd.notna(brand_med[b]):
            est, basis_level = brand_med[b], "brand"
        else:
            est, basis_level = global_med, "global"
        est = round(float(est), 2)
        rows.append({
            "inventory_uid": r.inventory_uid,
            "canonical_inventory_id": r.canonical_inventory_id,
            "brand": b, "caliber": cal, "part_number": r.part_number,
            "stock": int(r.stock) if pd.notna(r.stock) else None,
            "H": None, "C": None, "P": 0.0,
            "n_hist": 0, "n_active": 0, "total_sold": 0, "weighted_sold": 0.0,
            "active_count": 0, "velocity": 0.0, "scarcity_raw": 0.0,
            "last_sold_date": None, "caliber_key": cal if pd.notna(cal) else "__none__",
            "D": 0.5, "S": 0.5,
            "tmv": est, "w_H": 0.0, "w_C": 0.0,
            "valuation_basis": "CALIBER_ESTIMATED",
            "confidence_tier": "LOW",
            "tmv_low": round(est * (1 - LOW_BAND), 2),
            "tmv_high": round(est * (1 + LOW_BAND), 2),
            "scarcity_flag": "BALANCED",
            "median_days_to_sell": None, "prob_30": None, "prob_90": None,
            "lambda_monthly": None, "turnover_bucket": None,
            "dataset_months": df["dataset_months"].iloc[0] if "dataset_months" in df else None,
            "fallback_level": basis_level,
        })
    fb = pd.DataFrame(rows)
    log(f"  caliber fallback: estimated {len(fb)} item(s) with no confirmed evidence "
        f"(levels: {fb['fallback_level'].value_counts().to_dict()})")
    # align columns before concat (df has no fallback_level; fb has the same
    # value columns) so pandas doesn't warn about mismatched/all-NA dtypes.
    for col in df.columns:
        if col not in fb.columns:
            fb[col] = None
    fb = fb.reindex(columns=list(df.columns) + [c for c in fb.columns if c not in df.columns])
    # pandas emits a harmless FutureWarning about all-NA columns in concat
    # (e.g. last_sold_date is NaT for many rows); the result is correct, so
    # silence just this call rather than reshape every column's dtype.
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        return pd.concat([df, fb], ignore_index=True)


def _ensure_feat_schema(conn):
    """The feat_* output tables in some databases predate the
    canonical_inventory_id migration (they were created keyed on the old
    'product_id'/'quantity_on_hand'). They are empty, purely-derived output
    tables, so we rebuild them here on the current key. Non-destructive to any
    real data — nothing but computed features ever lives here."""
    cols_ok = "canonical_inventory_id" in [r[0] for r in conn.execute("DESCRIBE feat_pricing").fetchall()] \
        if conn.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name='feat_pricing'").fetchone()[0] else False
    if cols_ok:
        return
    conn.execute("DROP TABLE IF EXISTS feat_pricing")
    conn.execute("DROP TABLE IF EXISTS feat_demand")
    conn.execute("DROP TABLE IF EXISTS feat_market_supply")
    conn.execute("""
        CREATE TABLE feat_pricing (
            canonical_inventory_id VARCHAR PRIMARY KEY, brand VARCHAR, caliber VARCHAR, part_number VARCHAR,
            condition VARCHAR, stock INTEGER, recommended_price_eur DOUBLE, weight_historical DOUBLE,
            weight_market DOUBLE, confidence_tier VARCHAR, scarcity_score DOUBLE, scarcity_flag VARCHAR,
            competitive_position DOUBLE, median_days_to_sell DOUBLE, prob_sold_30_days DOUBLE,
            prob_sold_90_days DOUBLE, expected_revenue_base DOUBLE, expected_revenue_low DOUBLE,
            expected_revenue_high DOUBLE, action VARCHAR, action_reason VARCHAR,
            historical_value_eur DOUBLE, current_value_eur DOUBLE, recommendation_reason VARCHAR,
            computed_at TIMESTAMP DEFAULT current_timestamp)
    """)
    conn.execute("""
        CREATE TABLE feat_demand (
            canonical_inventory_id VARCHAR PRIMARY KEY, total_sold INTEGER, dataset_months DOUBLE,
            sales_velocity_monthly DOUBLE, last_sold_date DATE, days_since_last_sale INTEGER,
            recency_score DOUBLE, price_trend_slope DOUBLE, computed_at TIMESTAMP DEFAULT current_timestamp)
    """)
    conn.execute("""
        CREATE TABLE feat_market_supply (
            canonical_inventory_id VARCHAR PRIMARY KEY, active_listing_count INTEGER, unique_seller_count INTEGER,
            min_landed_cost_eur DOUBLE, max_landed_cost_eur DOUBLE, median_landed_cost_eur DOUBLE,
            p25_landed_cost_eur DOUBLE, p75_landed_cost_eur DOUBLE, price_spread_eur DOUBLE, hhi_score DOUBLE,
            dominant_seller VARCHAR, dominant_seller_share DOUBLE, computed_at TIMESTAMP DEFAULT current_timestamp)
    """)


def write(conn, df: pd.DataFrame):
    _ensure_feat_schema(conn)
    # Additive, idempotent: records whether each TMV rests on HISTORICAL sold
    # evidence or the ACTIVE_ONLY fallback (backtest revision). Safe to run
    # every time; never drops or rewrites existing columns.
    conn.execute("ALTER TABLE tmv_results ADD COLUMN IF NOT EXISTS valuation_basis VARCHAR")
    # PORT ADAPTATION (A): additive price-reliability column.
    conn.execute("ALTER TABLE tmv_results ADD COLUMN IF NOT EXISTS price_reliability VARCHAR")
    # Phase 4 (dashboard integration): additive-only, persists the H/C dollar
    # values build() already computes in memory but previously discarded (only
    # the binary weight_historical/weight_market basis flags were kept). No
    # formula, weight, or D/S/P change -- these two columns are pure output
    # persistence so the dashboard can display H/C without recomputing them.
    conn.execute("ALTER TABLE feat_pricing ADD COLUMN IF NOT EXISTS historical_value_eur DOUBLE")
    conn.execute("ALTER TABLE feat_pricing ADD COLUMN IF NOT EXISTS current_value_eur DOUBLE")
    # Task 6 (price recommendation transparency): additive-only text disclosure;
    # recommended_price_eur numeric value is unchanged (still == tmv).
    conn.execute("ALTER TABLE feat_pricing ADD COLUMN IF NOT EXISTS recommendation_reason VARCHAR")
    # Task 7 (turnover bucket forecast): additive-only, persists the
    # expected-units-sold-per-bucket JSON build() already computes in memory.
    # No change to median_days_to_sell/probability_sell_30d/90d.
    conn.execute("ALTER TABLE turnover_survival ADD COLUMN IF NOT EXISTS turnover_bucket_forecast VARCHAR")
    conn.execute("DELETE FROM tmv_results")
    conn.execute("DELETE FROM feat_pricing")
    conn.execute("DELETE FROM feat_demand")
    conn.execute("DELETE FROM feat_market_supply")
    # PORT ADAPTATION (A): clear turnover_survival BEFORE the empty guard so a
    # rebuild with 0 MATCH_CONFIRMED leaves 0 turnover rows (idempotent), not
    # stale rows from a prior run. Turnover shares TMV's confirmed-only evidence
    # scope (both derive from load_confirmed_evidence → MATCH_CONFIRMED only).
    conn.execute("DELETE FROM turnover_survival")
    if df.empty:
        return

    tmv = df[["canonical_inventory_id", "tmv", "tmv_low", "tmv_high", "confidence_tier", "valuation_basis", "price_reliability"]].rename(
        columns={"tmv": "tmv_eur", "tmv_low": "tmv_low_eur", "tmv_high": "tmv_high_eur"})
    conn.register("t_tmv", tmv)
    conn.execute("INSERT INTO tmv_results (canonical_inventory_id, tmv_eur, tmv_low_eur, tmv_high_eur, confidence_tier, valuation_basis, price_reliability) "
                 "SELECT canonical_inventory_id, tmv_eur, tmv_low_eur, tmv_high_eur, confidence_tier, valuation_basis, price_reliability FROM t_tmv")
    conn.unregister("t_tmv")

    fp = df.copy()
    fp["condition"] = None
    fp["recommended_price_eur"] = fp["tmv"]
    fp["weight_historical"] = fp["w_H"]; fp["weight_market"] = fp["w_C"]
    fp["scarcity_score"] = fp["S"].round(3)
    fp["competitive_position"] = None
    fp["median_days_to_sell"] = df["median_days_to_sell"]
    fp["prob_sold_30_days"] = df["prob_30"]
    fp["prob_sold_90_days"] = df["prob_90"]
    fp["expected_revenue_base"] = (fp["tmv"] * fp["stock"].fillna(0)).round(2)
    fp["expected_revenue_low"] = (fp["tmv_low"] * fp["stock"].fillna(0)).round(2)
    fp["expected_revenue_high"] = (fp["tmv_high"] * fp["stock"].fillna(0)).round(2)
    fp["action"] = None; fp["action_reason"] = None
    # Phase 4: persist the already-computed H/C dollar values (no recomputation
    # here -- df["H"]/df["C"] were computed in build(), unchanged by this write).
    fp["historical_value_eur"] = pd.to_numeric(df["H"], errors="coerce").round(2)
    fp["current_value_eur"] = pd.to_numeric(df["C"], errors="coerce").round(2)
    fp["recommendation_reason"] = df["recommendation_reason"]
    cols = ["canonical_inventory_id", "brand", "caliber", "part_number", "condition", "stock",
            "recommended_price_eur", "weight_historical", "weight_market", "confidence_tier",
            "scarcity_score", "scarcity_flag", "competitive_position",
            "median_days_to_sell", "prob_sold_30_days", "prob_sold_90_days",
            "expected_revenue_base", "expected_revenue_low", "expected_revenue_high",
            "action", "action_reason", "historical_value_eur", "current_value_eur", "recommendation_reason"]
    conn.register("t_fp", fp[cols])
    conn.execute(f"INSERT INTO feat_pricing ({','.join(cols)}) SELECT {','.join(cols)} FROM t_fp")
    conn.unregister("t_fp")

    # turnover_survival (canonical_inventory_id keyed) — evidence-based items ONLY.
    # CALIBER_ESTIMATED fallback items have no sold history of their own, so they
    # get no turnover estimate (excluded here rather than written as a misleading
    # null/zero sell-time).
    conn.execute("DELETE FROM turnover_survival")
    ts_src = df[df["valuation_basis"] != "CALIBER_ESTIMATED"]
    ts = ts_src[["canonical_inventory_id", "median_days_to_sell", "prob_30", "prob_90", "turnover_bucket_forecast"]].rename(
        columns={"prob_30": "probability_sell_30d", "prob_90": "probability_sell_90d"})
    conn.register("t_ts", ts)
    conn.execute("INSERT INTO turnover_survival (canonical_inventory_id, median_days_to_sell, "
                 "probability_sell_30d, probability_sell_90d, turnover_bucket_forecast) SELECT canonical_inventory_id, "
                 "median_days_to_sell, probability_sell_30d, probability_sell_90d, turnover_bucket_forecast FROM t_ts")
    conn.unregister("t_ts")

    fd = df.copy()
    fd["dataset_months"] = df["dataset_months"]
    fd["days_since_last_sale"] = None
    fd["recency_score"] = df["D"].round(3)
    fd["price_trend_slope"] = df["P"].round(4)
    dcols = ["canonical_inventory_id", "total_sold", "dataset_months", "last_sold_date",
             "days_since_last_sale", "recency_score", "price_trend_slope"]
    fd2 = fd[dcols].copy(); fd2["sales_velocity_monthly"] = df["velocity"].round(4)
    conn.register("t_fd", fd2)
    conn.execute("INSERT INTO feat_demand (canonical_inventory_id, total_sold, dataset_months, sales_velocity_monthly, "
                 "last_sold_date, days_since_last_sale, recency_score, price_trend_slope) "
                 "SELECT canonical_inventory_id, total_sold, dataset_months, sales_velocity_monthly, "
                 "last_sold_date, days_since_last_sale, recency_score, price_trend_slope FROM t_fd")
    conn.unregister("t_fd")

    ms = df[df["active_count"] > 0].copy()
    if not ms.empty:
        ms["active_listing_count"] = ms["active_count"]
        for c in ["unique_seller_count", "min_landed_cost_eur", "max_landed_cost_eur", "median_landed_cost_eur",
                  "p25_landed_cost_eur", "p75_landed_cost_eur", "price_spread_eur", "hhi_score",
                  "dominant_seller", "dominant_seller_share"]:
            ms[c] = None
        mcols = ["canonical_inventory_id", "active_listing_count", "unique_seller_count", "min_landed_cost_eur",
                 "max_landed_cost_eur", "median_landed_cost_eur", "p25_landed_cost_eur", "p75_landed_cost_eur",
                 "price_spread_eur", "hhi_score", "dominant_seller", "dominant_seller_share"]
        conn.register("t_ms", ms[mcols])
        conn.execute(f"INSERT INTO feat_market_supply ({','.join(mcols)}) SELECT {','.join(mcols)} FROM t_ms")
        conn.unregister("t_ms")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(DB_PATH))
    args = ap.parse_args()
    conn = duckdb.connect(args.db)
    conn.execute(SCHEMA_PATH.read_text())
    log("=" * 60); log("WATCHPARTS — STEP 13: BUILD TMV (from confirmed matches)"); log("=" * 60)
    res = build(conn)
    df = res["df"]
    write(conn, df)
    if not df.empty:
        log(f"TMV computed for {len(df):,} confirmed items.")
        log(f"  confidence tiers: {df['confidence_tier'].value_counts().to_dict()}")
        log(f"  scarcity flags:   {df['scarcity_flag'].value_counts().to_dict()}")
        log(f"  TMV EUR: min {df['tmv'].min():.2f} | median {df['tmv'].median():.2f} | max {df['tmv'].max():.2f}")
        log(f"  total portfolio value (TMV × stock): €{(df['tmv'] * df['stock'].fillna(0)).sum():,.0f}")
        log(f"  turnover: market growth e^g = {math.exp(res['growth_g']):.3f}/mo "
            f"({(math.exp(res['growth_g'])-1)*100:+.1f}%/mo), "
            f"effective recency window = {res['eff_window_months']:.1f} mo "
            f"(was {res['dataset_months']:.1f} mo lifetime span)")
        log(f"  median days-to-sell: min {df['median_days_to_sell'].min():.0f} | "
            f"median {df['median_days_to_sell'].median():.0f} | max {df['median_days_to_sell'].max():.0f}")
    conn.close()
    log("✓ TMV build complete. Next: turnover + dashboard.")


if __name__ == "__main__":
    main()
