"""
14_backtest_tmv.py
==================
Validation / backtest for the TMV + turnover models (ARCHITECTURE_CURRENT.md
§36, R16 — "mandatory holdout backtest before TMV/turnover presented as
reliable"). Read-only against the DB; writes only a report file.

WHY THIS DESIGN (not the doc's literal temporal-only holdout):
  The confirmed eBay-sold evidence (2,832 dated sales) is time-COMPRESSED —
  a ~90-day eBay Sold-items export window puts ~96% of sales in Apr–Jul 2026,
  so a train-before-T / test-after-T split leaves only 3–4 backtestable items
  (feasibility probe, this session). The VCP aggregate source, by contrast,
  carries last_sold_date spread evenly Jun-2023 → Jun-2026 (3 years).

  So we run THREE complementary checks (Option C):
    1. LEAVE-ONE-OUT accuracy backtest on all confirmed dated sales — the
       primary result. Does NOT depend on temporal spread. Predict each held-
       out real sale price from the TMV model built on the REST of that item's
       evidence; report MAPE + hit-rate ±10/±20% by confidence tier, AND vs
       two dumb baselines (historical-median-only, active-median-only). If the
       blend can't beat the simplest baseline, the doc says simplify — we
       surface exactly that.
    2. TEMPORAL HOLDOUT where feasible — the doc's literal method, on the few
       items/calibers with sales both sides of a cutoff. Presented as
       directional / feasibility-limited, with a coverage table.
    3. TURNOVER CALIBRATION — λ-implied expected sales in a window vs actual.

  Every result is reported with its n and its limitation. Nothing is
  presented as universal proof.

Reuses the SAME TMV feature math as scripts/13_build_tmv.py (imported where
possible, re-expressed identically here where 13's functions are entangled
with its DB writes) so the backtest validates the real model, not a proxy.

Usage:
    python 14_backtest_tmv.py --db /path/to/watchparts.duckdb --out report.md
"""
from __future__ import annotations

import os
import argparse
import math
from datetime import date, timedelta
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

ASK_TO_SOLD_DISCOUNT = 0.90
ALPHA_TREND = 1.0
BETA_SCARCITY = 0.10
RECENCY_HALFLIFE_MONTHS = 12.0


# ── TMV feature math (identical to 13_build_tmv.py) ─────────────────────────
def _age_months(d, ref: date) -> float:
    if pd.isna(d):
        return RECENCY_HALFLIFE_MONTHS
    d = pd.to_datetime(d).date()
    return max(0.0, (ref - d).days / 30.44)


def _H(hobs, ref):
    """Recency(half-life 12mo) × volume weighted historical value.
    hobs = list of (price, vol, date)."""
    if not hobs:
        return None
    num = den = 0.0
    for price, vol, dt in hobs:
        w = math.exp(-math.log(2) * _age_months(dt, ref) / RECENCY_HALFLIFE_MONTHS)
        num += w * price * vol
        den += w * vol
    return num / den if den > 0 else None


def _C(active_prices):
    if not len(active_prices):
        return None
    return float(np.median(active_prices)) * ASK_TO_SOLD_DISCOUNT


def _P(ebay_prices_dates, H):
    """OLS slope of eBay-sold price vs month, expressed as fraction of H, clipped ±10%."""
    if len(ebay_prices_dates) < 2:
        return 0.0
    dts = pd.to_datetime([d for _, d in ebay_prices_dates], errors="coerce")
    if dts.notna().sum() < 2:
        return 0.0
    x = (dts - dts.min()).days.values / 30.44
    y = np.array([p for p, _ in ebay_prices_dates], dtype=float)
    mask = ~np.isnan(x)
    if mask.sum() < 2 or np.ptp(x[mask]) == 0:
        return 0.0
    slope = np.polyfit(x[mask], y[mask], 1)[0]
    base = H if H else float(np.median(y))
    return float(np.clip((slope / base) if base else 0.0, -0.10, 0.10))


def _tmv(H, C, nH, nA, P, S):
    if H is None and C is None:
        return None
    if H is None:
        base = C
    elif C is None:
        base = H
    else:
        wH = nH / (nH + nA)
        base = wH * H + (1 - wH) * C
    return base * (1 + ALPHA_TREND * P) * (1 + BETA_SCARCITY * (S - 0.5))


def _tier(has_h, has_c, nH, nA):
    if has_h and has_c and nH >= 3:
        return "HIGH"
    if (has_h and nH >= 1) or (has_c and nA >= 2):
        return "MEDIUM"
    return "LOW"


# ── data loading (same confirmed-evidence joins as 13_build_tmv.py) ─────────
def load(conn):
    inv = conn.execute("""
        SELECT inventory_uid, canonical_inventory_id, caliber, stock
        FROM staging_inventory WHERE validation_status <> 'FAIL'
    """).df().set_index("inventory_uid")
    vcp = conn.execute("""
        SELECT md.inventory_uid, v.avg_price_eur AS price, v.total_sold AS vol, v.last_sold_date AS obsdate
        FROM match_decisions md JOIN stg_historical_vcp_aggregate v ON md.evidence_uid=v.stable_evidence_uid
        WHERE md.match_status='MATCH_CONFIRMED' AND md.source_table='match_candidates_vcp'
          AND v.avg_price_eur IS NOT NULL AND v.avg_price_eur>0
    """).df()
    ebay = conn.execute("""
        SELECT md.inventory_uid, e.price_eur AS price, e.sold_date AS obsdate
        FROM match_decisions md JOIN stg_historical_ebay_sold e ON md.evidence_uid=e.stable_evidence_uid
        WHERE md.match_status='MATCH_CONFIRMED' AND md.source_table='match_candidates_ebay_sold'
          AND e.price_eur IS NOT NULL AND e.price_eur>0
    """).df()
    active = conn.execute("""
        SELECT md.inventory_uid, a.price_eur AS price
        FROM match_decisions md JOIN stg_active_targeted a ON md.evidence_uid=a.stable_evidence_uid
        WHERE md.match_status='MATCH_CONFIRMED' AND md.source_table='match_candidates_active'
          AND a.price_eur IS NOT NULL AND a.price_eur>0
    """).df()
    for d in (vcp, ebay):
        d["obsdate"] = pd.to_datetime(d["obsdate"], errors="coerce")
    return inv, vcp, ebay, active


def _hobs_for(uid, vcp, ebay, exclude_idx=None, source=None):
    """Build historical observations list for an item, optionally excluding
    one held-out row (by df index) from a given source."""
    hobs = []
    for i, r in vcp[vcp.inventory_uid == uid].iterrows():
        if source == "vcp" and i == exclude_idx:
            continue
        hobs.append((float(r.price), max(1, int(r.vol) if pd.notna(r.vol) else 1), r.obsdate))
    for i, r in ebay[ebay.inventory_uid == uid].iterrows():
        if source == "ebay" and i == exclude_idx:
            continue
        hobs.append((float(r.price), 1, r.obsdate))
    return hobs


# ── metrics ─────────────────────────────────────────────────────────────────
def _metrics(preds, actuals):
    preds, actuals = np.array(preds, float), np.array(actuals, float)
    ok = (~np.isnan(preds)) & (~np.isnan(actuals)) & (actuals > 0)
    preds, actuals = preds[ok], actuals[ok]
    if not len(preds):
        return dict(n=0, mape=None, hit10=None, hit20=None, medape=None)
    ape = np.abs(preds - actuals) / actuals
    return dict(n=int(len(preds)),
                mape=round(float(ape.mean()) * 100, 1),
                medape=round(float(np.median(ape)) * 100, 1),
                hit10=round(float((ape <= 0.10).mean()) * 100, 1),
                hit20=round(float((ape <= 0.20).mean()) * 100, 1))


# ── 1) LEAVE-ONE-OUT accuracy backtest ──────────────────────────────────────
def leave_one_out(inv, vcp, ebay, active):
    """For every confirmed dated sale (eBay-sold row, and each VCP aggregate
    row treated as one observation at its avg price), hold it out, rebuild the
    TMV model for that item from the remaining evidence, and compare the model
    price against that sale's real price. Report full-blend vs two baselines."""
    ref_all = pd.concat([vcp.obsdate, ebay.obsdate]).max()
    ref = ref_all.date() if pd.notna(ref_all) else date.today()

    active_med = active.groupby("inventory_uid").price.median().to_dict()
    active_n = active.groupby("inventory_uid").price.count().to_dict()

    rows = []
    # hold out each eBay-sold row (true per-sale price)
    for src, srcdf in (("ebay", ebay), ("vcp", vcp)):
        for idx, r in srcdf.iterrows():
            uid = r.inventory_uid
            if uid not in inv.index:
                continue
            actual = float(r.price)
            hobs = _hobs_for(uid, vcp, ebay, exclude_idx=idx, source=src)
            H = _H(hobs, ref)
            nH = len(hobs)
            ap = active[active.inventory_uid == uid].price.values
            C = _C(ap)
            nA = len(ap)
            if H is None and C is None:
                continue
            # trend from that item's remaining ebay sales
            eb = [(float(x.price), x.obsdate) for j, x in ebay[ebay.inventory_uid == uid].iterrows()
                  if not (src == "ebay" and j == idx)]
            P = _P(eb, H)
            tier = _tier(H is not None, C is not None, nH, nA)
            # full blended prediction (S=0.5 -> neutral scarcity; scarcity is a
            # market-position nudge, not a per-sale price driver, and holding it
            # neutral isolates the core H/C/P accuracy being validated)
            pred_blend = _tmv(H, C, nH, nA, P, 0.5)
            pred_hist = H                      # baseline 1: historical only
            pred_active = C                    # baseline 2: active-median × δ only
            rows.append(dict(uid=uid, src=src, actual=actual, tier=tier,
                             pred_blend=pred_blend, pred_hist=pred_hist, pred_active=pred_active))
    bt = pd.DataFrame(rows)
    return bt, ref


# ── 2) TEMPORAL HOLDOUT (feasible subset) ────────────────────────────────────
def temporal_holdout(inv, vcp, ebay, months_back=6):
    """Doc's literal method. Uses VCP last_sold_date (3-yr spread) as the
    timeline. Train on sales before cutoff T, test on sales after. Report
    coverage honestly."""
    alld = pd.concat([vcp.obsdate, ebay.obsdate]).dropna()
    if alld.empty:
        return None, None
    T = (alld.max() - pd.DateOffset(months=months_back))
    # combine both sources as dated observations
    obs = pd.concat([
        vcp.assign(vol=vcp.vol.fillna(1).clip(lower=1), source="vcp")[["inventory_uid", "price", "vol", "obsdate", "source"]],
        ebay.assign(vol=1, source="ebay")[["inventory_uid", "price", "vol", "obsdate", "source"]],
    ], ignore_index=True).dropna(subset=["obsdate"])
    before = obs[obs.obsdate < T]
    after = obs[obs.obsdate >= T]
    both_items = sorted(set(before.inventory_uid) & set(after.inventory_uid))
    ref = T.date()
    rows = []
    for uid in both_items:
        if uid not in inv.index:
            continue
        train = before[before.inventory_uid == uid]
        hobs = [(float(r.price), int(r.vol), r.obsdate) for _, r in train.iterrows()]
        H = _H(hobs, ref)
        if H is None:
            continue
        for _, r in after[after.inventory_uid == uid].iterrows():
            rows.append(dict(uid=uid, pred=H, actual=float(r.price)))
    return pd.DataFrame(rows), dict(T=T.date(), n_before=len(before), n_after=len(after),
                                    items_both=len(both_items))


# ── 3) TURNOVER CALIBRATION ──────────────────────────────────────────────────
def turnover_calibration(inv, vcp, ebay, months_back=6):
    """Compare λ-implied expected sale count in a holdout window vs actual.
    λ estimated from sales before T; actual counted after T. VCP timeline."""
    obs = pd.concat([
        vcp.assign(vol=vcp.vol.fillna(1).clip(lower=1))[["inventory_uid", "vol", "obsdate"]],
        ebay.assign(vol=1)[["inventory_uid", "vol", "obsdate"]],
    ], ignore_index=True).dropna(subset=["obsdate"])
    if obs.empty:
        return None
    T = obs.obsdate.max() - pd.DateOffset(months=months_back)
    hmin = obs.obsdate.min()
    train_months = max(1.0, (T - hmin).days / 30.44)
    window_months = months_back
    before = obs[obs.obsdate < T]
    after = obs[obs.obsdate >= T]
    rows = []
    for uid in sorted(set(before.inventory_uid)):
        sold_before = before[before.inventory_uid == uid].vol.sum()
        lam = sold_before / train_months
        expected = lam * window_months
        actual = after[after.inventory_uid == uid].vol.sum()
        rows.append(dict(uid=uid, lam_monthly=round(lam, 3), expected=round(expected, 2), actual=int(actual)))
    cal = pd.DataFrame(rows)
    return dict(T=T.date(), train_months=round(train_months, 1), window_months=window_months, table=cal)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--out", default="tmv_backtest_report.md")
    args = ap.parse_args()
    conn = duckdb.connect(args.db, read_only=True)
    inv, vcp, ebay, active = load(conn)
    conn.close()

    print(f"Loaded: vcp={len(vcp)} ebay={len(ebay)} active={len(active)} items={inv.shape[0]}")

    bt, ref = leave_one_out(inv, vcp, ebay, active)
    print(f"LOO rows: {len(bt)}")

    # overall + per-tier + per-source
    def block(df, label):
        out = [f"### {label}  (n={len(df)})", ""]
        out.append("| model | n | MAPE | median APE | hit ±10% | hit ±20% |")
        out.append("|---|---|---|---|---|---|")
        for name, col in (("Blended TMV", "pred_blend"), ("Historical-median only", "pred_hist"), ("Active-median×δ only", "pred_active")):
            m = _metrics(df[col], df["actual"])
            out.append(f"| {name} | {m['n']} | {m['mape']}% | {m['medape']}% | {m['hit10']}% | {m['hit20']}% |")
        out.append("")
        return "\n".join(out)

    report = ["# TMV / Turnover Backtest Report", "",
              f"_Reference date (max observation): {ref}_", "",
              "## 1. Leave-one-out accuracy backtest (primary)", "",
              "Each confirmed real sale is held out; TMV is rebuilt for that item from the "
              "remaining evidence and compared to the true sale price. Scarcity held neutral "
              "(S=0.5) to isolate core H/C/P accuracy. Baselines: historical-median-only and "
              "active-median×δ-only — if the blend can't beat the simplest baseline, the model "
              "should be simplified (ARCHITECTURE_CURRENT.md §36).", ""]
    report.append(block(bt, "Overall"))
    for tier in ("HIGH", "MEDIUM", "LOW"):
        sub = bt[bt.tier == tier]
        if len(sub):
            report.append(block(sub, f"Confidence tier = {tier}"))
    for src, lbl in (("ebay", "eBay-sold (per-sale price)"), ("vcp", "VCP (aggregate avg price)")):
        sub = bt[bt.src == src]
        if len(sub):
            report.append(block(sub, f"Source = {lbl}"))

    th, cov = temporal_holdout(inv, vcp, ebay)
    report += ["## 2. Temporal holdout (directional — feasibility-limited)", ""]
    if cov:
        report.append(f"Cutoff T = {cov['T']}. Observations before T: {cov['n_before']}, "
                      f"after T: {cov['n_after']}. Items with sales BOTH sides: **{cov['items_both']}**.")
        report.append("")
        if th is not None and len(th):
            m = _metrics(th["pred"], th["actual"])
            report.append(f"Train-before-T H vs actual after-T sales — n={m['n']}, MAPE={m['mape']}%, "
                          f"hit ±20%={m['hit20']}%.")
            report.append("")
            report.append("_Small n by construction: the eBay export's ~90-day window compresses "
                          "per-sale dates into Apr–Jul 2026, so temporal coverage rests on the few "
                          "items whose VCP/eBay evidence straddles T. Presented as directional "
                          "evidence, not universal proof._")
        else:
            report.append("_No item had a rebuildable train-side H and a test-side sale — temporal "
                          "holdout not feasible on current data; see leave-one-out result instead._")
    report.append("")

    cal = turnover_calibration(inv, vcp, ebay)
    report += ["## 3. Turnover calibration (directional)", ""]
    if cal:
        t = cal["table"]
        t_nonzero = t[(t.expected > 0) | (t.actual > 0)]
        tot_exp = round(t.expected.sum(), 1)
        tot_act = int(t.actual.sum())
        report.append(f"Cutoff T = {cal['T']}, train window {cal['train_months']} mo, "
                      f"holdout window {cal['window_months']} mo. λ estimated per item before T; "
                      f"expected = λ×window vs actual sales after T.")
        report.append("")
        report.append(f"Aggregate over {len(t)} items with pre-T sales: "
                      f"**expected {tot_exp} sales vs actual {tot_act}**.")
        report.append("")
        report.append("| item (uid tail) | λ/mo | expected | actual |")
        report.append("|---|---|---|---|")
        for r in t_nonzero.sort_values("expected", ascending=False).head(15).itertuples():
            report.append(f"| …{r.uid[-8:]} | {r.lam_monthly} | {r.expected} | {r.actual} |")
        report.append("")
        report.append("_Constant-hazard (exponential) assumption; directional calibration only, "
                      "limited by the same temporal sparsity._")
    report.append("")

    Path(args.out).write_text("\n".join(report), encoding="utf-8")
    print(f"Report written: {args.out}")
    # also echo the headline overall block to stdout
    print("\n" + block(bt, "Overall"))


if __name__ == "__main__":
    main()
