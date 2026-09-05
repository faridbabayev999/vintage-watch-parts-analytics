"""
16_dashboard.py
===============
Module 5 — dashboard RENDERER (skeleton). Thin presentation layer over
scripts/16_dashboard_data.py. Reuses a card/banner layout (structure only) and
binds strictly to backend-computed values — it NEVER recomputes TMV,
confidence, or price math (that stayed out of the port, unlike B's in-dashboard
JS). Turnover is always labelled as selling velocity, never price sensitivity,
and there is no control that makes turnover respond to price.

`build_dashboard_html(conn)` is a pure function (testable). `main()` is the
Streamlit entrypoint. Read-only; asserts its DB target before any query.

Run:  streamlit run scripts/16_dashboard.py
      WATCHPARTS_DB=/tmp/copy.duckdb streamlit run scripts/16_dashboard.py
"""
from __future__ import annotations

import html
import importlib.util
import json
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent


def _data_module():
    spec = importlib.util.spec_from_file_location("dash16_data", SCRIPTS_DIR / "16_dashboard_data.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


_CSS = """
<style>
.wp-wrap{font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;color:#1c2430}
.wp-banner{padding:14px 18px;border-radius:10px;margin:10px 0;font-weight:600}
.wp-awaiting{background:#fff7ed;border:1px solid #fdba74;color:#9a3412}
.wp-meta{display:flex;gap:22px;flex-wrap:wrap;font-size:13px;color:#475569;margin:8px 0 16px}
.wp-card{border:1px solid #e2e8f0;border-radius:12px;padding:16px;margin:10px 0;background:#fff}
.wp-tmv{font-size:26px;font-weight:700}
.wp-tier{display:inline-block;padding:2px 8px;border-radius:999px;font-size:12px;background:#eef2ff;color:#3730a3}
.wp-note{font-size:12px;color:#64748b;margin-top:8px}
.wp-row{display:flex;gap:18px;flex-wrap:wrap;font-size:13px;color:#334155;margin-top:6px}
.wp-overview{display:flex;gap:14px;flex-wrap:wrap;margin:12px 0}
.wp-ocard{border:1px solid #e2e8f0;border-radius:10px;padding:10px 14px;background:#f8fafc;min-width:140px}
.wp-ocard b{display:block;font-size:20px}
.wp-ocard span{font-size:12px;color:#64748b}
.wp-primary{display:flex;gap:22px;flex-wrap:wrap;align-items:baseline;margin:8px 0 4px}
.wp-primary .wp-tmv{margin-right:4px}
.wp-badge{display:inline-block;padding:3px 10px;border-radius:999px;font-size:12px;font-weight:600}
.wp-badge-ready{background:#dcfce7;color:#166534}
.wp-badge-estimate{background:#fef9c3;color:#854d0e}
.wp-details{margin-top:10px;border-top:1px solid #eef0f3;padding-top:10px}
.wp-unpriced-wrap{margin-top:24px}
.wp-unpriced{border:1px solid #e2e8f0;border-radius:10px;padding:10px 14px;margin:6px 0;background:#f8fafc;display:flex;justify-content:space-between;gap:16px;flex-wrap:wrap}
.wp-unpriced b{font-size:14px}
.wp-unpriced span{font-size:13px;color:#64748b}
</style>
"""


def _overview_html(d, conn) -> str:
    ov = d.overview_summary(conn)
    def card(label, value, sub=""):
        v = _fmt(value) if isinstance(value, float) else (value if value is not None else "—")
        return f'<div class="wp-ocard"><b>{v}</b><span>{label}{" — " + sub if sub else ""}</span></div>'
    return (
        '<div class="wp-overview">'
        + card("Total inventory", ov["total_inventory"])
        + card("Active evidence coverage", f'{ov["active_coverage_pct"]}%', f'{ov["active_coverage_n"]}/{ov["total_inventory"]}')
        + card("Historical evidence coverage", f'{ov["historical_coverage_pct"]}%', f'{ov["historical_coverage_n"]}/{ov["total_inventory"]}')
        + card("TMV available", f'{ov["tmv_available_pct"]}%', f'{ov["tmv_available_n"]}/{ov["total_inventory"]}')
        + card("Avg. recommended price", f'€{_fmt(ov["avg_recommended_price_eur"])}' if ov["avg_recommended_price_eur"] is not None else "—")
        + card("Avg. turnover", f'{_fmt(ov["avg_turnover_days"])} days' if ov["avg_turnover_days"] is not None else "—")
        + '</div>'
    )


def _fmt(v, suffix=""):
    # isinstance(nan, float) is True, so a bare isinstance check alone
    # rendered literal "nan" for items with no historical evidence
    # (historical_value_eur is NaN, not None, coming out of pandas) --
    # found during dashboard client-review, 2026-07-31.
    if isinstance(v, (int, float)) and v == v:  # v==v is False only for NaN
        return f"{v:.2f}{suffix}"
    return "—"


def build_dashboard_html(conn, data=None) -> str:
    """Pure render. Empty state → 'Awaiting validated evidence' (no numbers).
    Ready → item cards from backend values only. `data` may be injected for
    tests; otherwise pulled from the data layer."""
    d = data or _data_module()
    state = d.dashboard_state(conn)
    market = d.market_summary(conn)
    fresh = d.data_freshness(conn)
    fx = d.latest_usd_eur_rate(conn)

    meta = (
        f'<div class="wp-meta">'
        f'<span>Market data — EU active: {market["active_eu"]}, US active: {market["active_us"]}, '
        f'EUR sold: {market["sold_eur"]}, USD sold: {market["sold_usd"]}</span>'
        f'<span>FX USD→EUR: {_fmt(fx) if fx is not None else "n/a"}</span>'
        f'<span>TMV computed: {html.escape(str(fresh["tmv_computed_at"] or "—"))}</span>'
        f'</div>'
    )
    overview = _overview_html(d, conn)

    if state["state"] != "READY":
        body = (
            f'<div class="wp-banner wp-awaiting">{html.escape(d.AWAITING_MESSAGE)} — '
            f'{state["n_confirmed"]} confirmed matches, {state["n_tmv"]} TMV rows. '
            f'No prices, recommendations, or fallback values are shown until validated '
            f'evidence exists.</div>'
        )
        return f'{_CSS}<div class="wp-wrap"><h2>Vintage Watch Parts — Valuation</h2>{meta}{overview}{body}</div>'

    cards = []
    for it in d.load_items(conn):
        name = html.escape(str(it.get("canonical_inventory_id", "")))
        components = (
            f'<div class="wp-row">'
            f'<span>Historical value (H): €{_fmt(it["historical_value_eur"])}</span>'
            f'<span>Current value (C): €{_fmt(it["current_value_eur"])}</span>'
            f'<span>Demand index (D): {_fmt(it["demand_index"])}</span>'
            f'<span>Market dynamics (S): {_fmt(it["market_dynamics"])}</span>'
            f'<span>Price trend (P): {_fmt(it["price_trend"])}</span>'
            f'</div>'
        )
        reco_html = ""
        if it.get("recommendation_reason"):
            reco_html = f'<div class="wp-note wp-reco"><b>Why this price:</b> {html.escape(it["recommendation_reason"])}</div>'
        scen_html = ""
        sc = d.item_scenarios(conn, it["tmv_eur"])
        if not sc["ok"]:
            scen_html = f'<div class="wp-note wp-scen-error">Scenario comparison unavailable — {html.escape(sc["error"])}</div>'
        else:
            labels = {"A": "US customer", "B": "Germany customer", "C": "Virtual (price only)"}
            cells = []
            for key in ("A", "B", "C"):
                s = sc["scenarios"][key]
                src_bits = [f"{k}: {html.escape(str(v))}" for k, v in s["sources"].items() if v]
                cells.append(
                    f'<div class="wp-scen"><div><b>{labels[key]}</b></div>'
                    f'<div>Price: €{_fmt(s["price_eur"])}</div>'
                    f'<div>Shipping: €{_fmt(s["shipping_eur"])}</div>'
                    f'<div>Customs: €{_fmt(s["customs_eur"])}</div>'
                    f'<div>Tax: €{_fmt(s["tax_eur"])}</div>'
                    f'<div><b>Landed: €{_fmt(s["landed_cost_eur"])}</b></div>'
                    f'<div class="wp-note">{" · ".join(src_bits) if src_bits else "no import charges (domestic)"}</div>'
                    f'</div>'
                )
            scen_html = f'<div class="wp-scenarios">{"".join(cells)}</div>'

        price_note = (
            "Price/time: this dashboard reports the fixed backend turnover estimate "
            "(from confirmed sold-evidence velocity). The current turnover model does "
            "not respond to price — it has no price-elasticity term — so no 'expected "
            "selling period impact' from a price change can be shown without inventing "
            "an unfitted assumption, which this project does not do."
        )

        sim_html = ""
        if it.get("turnover_evidence_status", "SUPPORTED") == "SUPPORTED" and it["median_days_to_sell"] is not None:
            sim_rows = d.price_time_table(conn, it["tmv_eur"], it["median_days_to_sell"])
            cells = []
            for r in sim_rows:
                if r["simulated_days"] is None:
                    continue
                cells.append(
                    f'<div class="wp-sim"><div>{r["pct"]:+d}% (€{_fmt(r["price_eur"])})</div>'
                    f'<div><b>{_fmt(r["simulated_days"])} days</b></div></div>'
                )
            if cells:
                eps = sim_rows[0].get("epsilon")
                eps_src = sim_rows[0].get("epsilon_source")
                disclaimer = sim_rows[0].get("disclaimer", "")
                sim_html = (
                    f'<div class="wp-simulator"><div class="wp-note"><b>Price/time simulation</b> '
                    f'(ε={_fmt(eps)}{" — configured" if eps_src else " — default, not configured"}): '
                    f'{html.escape(disclaimer)}</div>'
                    f'<div class="wp-row">{"".join(cells)}</div></div>'
                )

        bucket_html = ""
        if it.get("turnover_bucket_forecast"):
            try:
                buckets = json.loads(it["turnover_bucket_forecast"])
            except (TypeError, ValueError):
                buckets = []
            cells = [
                f'<div class="wp-bucket"><div>{html.escape(str(b["bucket"]))}d</div>'
                f'<div><b>{_fmt(b["expected_units"])}</b> units</div></div>'
                for b in buckets
            ]
            if cells:
                bucket_html = (
                    f'<div class="wp-note"><b>Expected units sold by time bucket</b> '
                    f'(stock {it.get("stock", "—")}):</div>'
                    f'<div class="wp-row">{"".join(cells)}</div>'
                )

        # Client-facing primary block: Part / Recommended price / Confidence /
        # Market evidence / Expected selling time / Scenario price / Demand.
        pricing_label = html.escape(str(it.get("pricing_state_label") or it.get("confidence_tier") or ""))
        badge_class = "wp-badge-ready" if it.get("pricing_state") in ("GOVERNED", "AUTO_CONFIRMED", "HIGH") else "wp-badge-estimate"
        market_ev = f'{it.get("market_evidence_active", 0)} active listing(s), {it.get("market_evidence_sold", 0)} sold record(s)'
        expected_sell = it.get("sell_time_display") or (
            f'{_fmt(it["median_days_to_sell"])} days' if it["median_days_to_sell"] is not None else "—"
        )
        demand_display = _fmt(it["demand_index"]) if it.get("demand_index") is not None else "—"
        scenario_price_display = "—"
        sc = d.item_scenarios(conn, it["tmv_eur"])
        if sc["ok"]:
            scenario_price_display = f'€{_fmt(sc["scenarios"]["A"]["landed_cost_eur"])} (US) / €{_fmt(sc["scenarios"]["B"]["landed_cost_eur"])} (DE)'

        primary = (
            f'<div class="wp-primary">'
            f'<span class="wp-tmv">€{_fmt(it["tmv_eur"])}</span>'
            f'<span class="wp-badge {badge_class}">{pricing_label}</span>'
            f'</div>'
            f'<div class="wp-row">'
            f'<span>Market evidence: {market_ev}</span>'
            f'<span>Expected selling time: {expected_sell}</span>'
            f'<span>Scenario price: {scenario_price_display}</span>'
            f'<span>Demand indicator: {demand_display}</span>'
            f'</div>'
        )

        details = (
            f'<div class="wp-details">'
            f'<div class="wp-row">'
            f'<span>Price range: €{_fmt(it["tmv_low_eur"])}–€{_fmt(it["tmv_high_eur"])}</span>'
            f'<span>P(sell within 30d): {_fmt(it["prob_sell_30d"])} · P(90d): {_fmt(it["prob_sell_90d"])}</span>'
            f'<span>Internal evidence tier: {html.escape(str(it["confidence_tier"]))}</span>'
            f'</div>'
            f'{components}'
            f'{reco_html}'
            f'{scen_html}'
            f'<div class="wp-note">Turnover: {html.escape(it["turnover_note"])}</div>'
            f'{bucket_html}'
            f'<div class="wp-note">{html.escape(price_note)}</div>'
            f'{sim_html}</div>'
        )

        cards.append(
            f'<div class="wp-card"><div><b>{name}</b></div>'
            f'{primary}'
            f'{details}</div>'
        )

    unpriced_html = ""
    unpriced = d.load_unpriced_items(conn)
    if unpriced:
        rows_html = "".join(
            f'<div class="wp-unpriced"><b>{html.escape(str(u["canonical_inventory_id"]))}</b>'
            f'<span>Recommendation unavailable — {html.escape(u["reason"])}</span></div>'
            for u in unpriced
        )
        unpriced_html = (
            f'<div class="wp-unpriced-wrap"><h3>Items without a price recommendation '
            f'({len(unpriced)})</h3>{rows_html}</div>'
        )

    return f'{_CSS}<div class="wp-wrap"><h2>Vintage Watch Parts — Valuation</h2>{meta}{overview}{"".join(cards)}{unpriced_html}</div>'


# ═══════════════════════════════════════════════════════════════════════════
# CLIENT PRODUCT PAGES (2026-07-31) — luxury pricing workspace, not a
# technical dashboard. Every number rendered below is read verbatim from
# scripts/16_dashboard_data.py (governed tmv_results / algorithmic
# tmv_results_algorithmic / turnover_survival* / evidence_confidence_
# classification) or the scenario engine -- nothing here recomputes a
# price, confidence, or turnover figure. The old technical view
# (build_dashboard_html, above) is preserved unchanged and moved behind
# the "Admin — Technical View" nav entry, not deleted.
# ═══════════════════════════════════════════════════════════════════════════

_CLIENT_CSS = """
<style>
.cp-wrap{font-family:'Georgia',system-ui,serif;color:#1c2430}
.cp-card{border:1px solid #e5e0d8;border-radius:14px;padding:18px 22px;background:#fffdf9;box-shadow:0 1px 3px rgba(0,0,0,.04)}
.cp-value{font-size:28px;font-weight:700;color:#1c2430}
.cp-label{font-size:12px;color:#8a7f6d;text-transform:uppercase;letter-spacing:.06em;margin-top:4px}
.cp-big-price{font-size:48px;font-weight:700;color:#8a6d3b}
.cp-badge{display:inline-block;padding:4px 14px;border-radius:999px;font-size:13px;font-weight:600}
.cp-badge-ready{background:#e8f3ea;color:#2f6b3a}
.cp-badge-estimate{background:#fbf1de;color:#9a6a1c}
.cp-trace-row{display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid #f0ece3;font-size:14px}
.cp-trace-row.total{font-weight:700;border-top:2px solid #1c2430;border-bottom:none;margin-top:4px;padding-top:10px}
</style>
"""


def _connect(d):
    db_path = d.resolve_db_path()
    conn = d.connect_readonly(db_path)
    d.assert_db_target(conn, db_path)
    return conn, db_path


def _page_portfolio_overview(st, d, conn) -> None:
    st.html(_CLIENT_CSS)
    st.title("Vintage Watch Parts — Pricing Intelligence")
    st.caption("Portfolio Overview")
    ov = d.portfolio_overview(conn)

    cols = st.columns(5)
    cards = [
        ("Inventory", f'{ov["total_inventory"]} parts'),
        ("Pricing Coverage", f'{ov["priced_n"]} / {ov["total_inventory"]}  ({ov["priced_pct"]}%)'),
        ("Portfolio Value", f'€{ov["portfolio_value_eur"]:,.2f}' if ov["portfolio_value_eur"] is not None else "—"),
        ("Avg. Recommended Price", f'€{ov["avg_recommended_price_eur"]:.2f}' if ov["avg_recommended_price_eur"] is not None else "—"),
        ("Expected Sell Time", ov["sell_time_overview"]),
    ]
    for col, (label, value) in zip(cols, cards):
        col.html(f'<div class="cp-card"><div class="cp-value">{html.escape(str(value))}</div><div class="cp-label">{html.escape(label)}</div></div>')

    if ov["top_brands"]:
        st.subheader("High-coverage brands")
        st.html(
            '<div style="display:flex;gap:10px;flex-wrap:wrap">'
            + "".join(
                f'<div class="cp-card" style="min-width:120px"><div class="cp-value" style="font-size:18px">{html.escape(b)}</div>'
                f'<div class="cp-label">{c} parts priced</div></div>'
                for b, c in ov["top_brands"]
            )
            + "</div>"
        )


def _render_item_row_table(st, items) -> None:
    import pandas as pd
    if not items:
        st.info("No items match your search.")
        return
    df = pd.DataFrame([{
        "Part": it["canonical_inventory_id"], "Brand": it.get("brand"),
        "Recommended Price": f'€{it["tmv_eur"]:.2f}' if it["tmv_eur"] is not None else "—",
        "Confidence": it.get("pricing_state_label"),
        "Sell Time": it.get("sell_time_display") or (
            f'{it["median_days_to_sell"]:.0f}d' if it.get("median_days_to_sell") is not None else "—"
        ),
    } for it in items])
    st.dataframe(df, width='stretch', hide_index=True)


def _page_pricing_workspace(st, d, conn) -> None:
    st.html(_CLIENT_CSS)
    st.title("Pricing Workspace")
    query = st.text_input("Search inventory (part number, brand, or caliber)", key="workspace_search_query")
    items = d.search_priced_items(conn, query)
    st.caption(f"{len(items)} priced item(s) match.")
    _render_item_row_table(st, items)

    if items:
        ids = [it["canonical_inventory_id"] for it in items]
        selected = st.selectbox("Open item detail", options=ids)
        if selected:
            _page_item_detail(st, d, conn, selected)


def _page_item_detail(st, d, conn, canonical_inventory_id: str) -> None:
    it = d.get_item_by_id(conn, canonical_inventory_id)
    if it is None:
        st.warning("Item not found.")
        return
    st.divider()
    st.subheader(f'{it.get("brand") or ""} {it.get("caliber") or ""} — Part {it.get("part_number") or it["canonical_inventory_id"]}')

    badge_class = "cp-badge-ready" if it["pricing_state"] in ("GOVERNED", "AUTO_CONFIRMED", "HIGH") else "cp-badge-estimate"
    st.html(
        f'<div class="cp-big-price">€{_fmt(it["tmv_eur"])}</div>'
        f'<span class="cp-badge {badge_class}">{html.escape(str(it["pricing_state_label"]))}</span>'
    )

    st.markdown("**Why this price?**")
    h = it.get("historical_value_eur")
    c = it.get("current_value_eur")
    trace_rows = []
    if h is not None and h == h:
        trace_rows.append(("Historical Market Value", h))
    if c is not None and c == c:
        trace_rows.append(("Current Market Value", c))
    trace_rows.append(("Recommended Price", it["tmv_eur"]))
    st.html(
        "".join(
            f'<div class="cp-trace-row{" total" if label == "Recommended Price" else ""}">'
            f'<span>{html.escape(label)}</span><span>€{_fmt(v)}</span></div>'
            for label, v in trace_rows
        )
    )
    if it.get("recommendation_reason"):
        st.caption(it["recommendation_reason"])

    ev_col, sell_col = st.columns(2)
    ev_col.metric("Market Evidence", f'{it.get("market_evidence_active", 0)} active · {it.get("market_evidence_sold", 0)} sold')
    sell_col.metric(
        "Expected Selling Time",
        it.get("sell_time_display") or (
            f'{_fmt(it["median_days_to_sell"])} days' if it["median_days_to_sell"] is not None else "—"
        ),
    )

    # ── Price / Time simulation slider (reuses 17_scenario_engine.py's
    # simulate_price_time -- no new math, same disclosed epsilon assumption
    # already used elsewhere in this project). Reset returns to 0%.
    st.markdown("---")
    st.markdown("**Selling price simulation**")
    slider_key = f"price_pct_{canonical_inventory_id}"
    if st.button("Reset to recommended price", key=f"reset_{canonical_inventory_id}"):
        st.session_state[slider_key] = 0
    pct = st.slider("Price adjustment", min_value=-20, max_value=20, value=st.session_state.get(slider_key, 0), step=5, key=slider_key)
    if (
        it.get("turnover_evidence_status", "SUPPORTED") == "SUPPORTED"
        and it["tmv_eur"] is not None
        and it["median_days_to_sell"] is not None
    ):
        sim = d.price_time_table(conn, it["tmv_eur"], it["median_days_to_sell"], pct_points=(pct,))[0]
        sim_col1, sim_col2, sim_col3 = st.columns(3)
        sim_col1.metric("Price", f'€{_fmt(sim["price_eur"])}')
        sim_col2.metric("Expected Selling Time", f'{_fmt(sim["simulated_days"])} days' if sim.get("simulated_days") is not None else "—")
        revenue = round(sim["price_eur"] * (it.get("stock") or 0), 2)
        sim_col3.metric("Potential Revenue (full stock)", f'€{revenue:,.2f}')
        st.caption(sim.get("disclaimer", ""))

    # ── Stock sell-through forecast (existing turnover_bucket_forecast,
    # unchanged math -- just adding revenue = units × current price).
    if it.get("turnover_bucket_forecast"):
        st.markdown("**Stock sell-through forecast** (stock: " + str(it.get("stock", "—")) + ")")
        import json as _json
        import pandas as pd
        buckets = _json.loads(it["turnover_bucket_forecast"])
        price = it["tmv_eur"] or 0
        rows = [{"Time bucket": f'{b["bucket"]}d', "Expected units sold": round(b["expected_units"], 2),
                 "Revenue": round(b["expected_units"] * price, 2)} for b in buckets]
        total_units = sum(b["expected_units"] for b in buckets)
        remaining = max(0, (it.get("stock") or 0) - total_units)
        st.dataframe(pd.DataFrame(rows), width='stretch', hide_index=True)
        st.caption(f'Remaining (unsold in forecast horizon): {remaining:.2f} units · Potential revenue: €{remaining * price:,.2f}')

    # ── Scenario comparison (reuses item_scenarios(), unchanged math).
    st.markdown("**Scenario comparison**")
    sc = d.item_scenarios(conn, it["tmv_eur"])
    if not sc["ok"]:
        st.warning(f'Scenario comparison unavailable — {sc["error"]}')
    else:
        labels = {"A": "US customer", "B": "Germany customer", "C": "Virtual (price only)"}
        sc_cols = st.columns(3)
        for col, key in zip(sc_cols, ("A", "B", "C")):
            s = sc["scenarios"][key]
            col.markdown(f'**{labels[key]}**')
            col.write(f'Base price: €{_fmt(s["price_eur"])}')
            col.write(f'Shipping: €{_fmt(s["shipping_eur"])}')
            col.write(f'Customs: €{_fmt(s["customs_eur"])}')
            col.write(f'Tax: €{_fmt(s["tax_eur"])}')
            col.markdown(f'**Landed: €{_fmt(s["landed_cost_eur"])}**')


def _render_pipeline_jobs(st, d, conn) -> None:
    jobs = d.latest_pipeline_jobs(conn)
    if not jobs:
        return

    st.subheader("Pipeline Jobs")
    running = [j for j in jobs if j.get("status") in ("QUEUED", "RUNNING")]
    if running:
        st.info("Pricing jobs are running in the background. Refresh this section to see the latest status.")
    if st.button("Refresh job status", key="refresh_pipeline_jobs"):
        st.rerun()

    for job in jobs:
        title = (
            f'{job.get("brand") or ""} {job.get("caliber") or ""} '
            f'{job.get("part_number") or ""} — {job.get("status")}'
        ).strip()
        with st.expander(title, expanded=job.get("status") in ("RUNNING", "FAILED")):
            meta_cols = st.columns(4)
            meta_cols[0].metric("Job type", job.get("job_type") or "—")
            meta_cols[1].metric("Status", job.get("status") or "—")
            meta_cols[2].metric("Requested", str(job.get("requested_at") or "—"))
            meta_cols[3].metric("Finished", str(job.get("finished_at") or "—"))

            if job.get("step_timings_json"):
                import pandas as pd
                try:
                    steps = json.loads(job["step_timings_json"])
                except (TypeError, ValueError):
                    steps = []
                if steps:
                    st.dataframe(
                        pd.DataFrame([
                            {
                                "Step": step.get("step"),
                                "Seconds": step.get("seconds"),
                                "Status": "OK" if step.get("returncode") == 0 else "Failed",
                            }
                            for step in steps
                        ]),
                        width="stretch",
                        hide_index=True,
                    )

            events = d.pipeline_job_events(conn, job["job_id"], limit=12)
            if events:
                import pandas as pd
                st.caption("Recent events")
                st.dataframe(pd.DataFrame(events), width="stretch", hide_index=True)

            if job.get("status") == "SUCCEEDED":
                row = d.dashboard_contract_row(conn, job.get("canonical_inventory_id"))
                if row:
                    st.success(
                        f'Result: {row["pricing_status"]} · {row["pricing_confidence"]} · '
                        f'€{_fmt(row["recommended_price_eur"])} · {row["sell_time_display"] or "—"}'
                    )
                    st.caption(
                        f'Evidence: {row["active_evidence_count"]} active · '
                        f'{row["historical_evidence_count"]} sold · '
                        f'{row["total_unique_evidence_count"]} total'
                    )
                    if st.button("Show this item in Pricing Workspace", key=f'open_{job["job_id"]}'):
                        st.session_state["workspace_search_query"] = job.get("canonical_inventory_id") or ""
                        st.success("Open Pricing Workspace from the sidebar; the search is pre-filled.")
            elif job.get("status") == "FAILED":
                st.error(job.get("error_message") or "Pipeline job failed.")
                if st.button("Retry job", key=f'retry_{job["job_id"]}'):
                    worker = d.retry_pipeline_job(job["job_id"])
                    st.success(f'Retry started: PID {worker["pid"]}. Log: {worker["log_path"]}')
                    st.rerun()


def _page_inventory_management(st, d, conn) -> None:
    st.html(_CLIENT_CSS)
    st.title("Inventory Management")

    with st.expander("+ Add Inventory Item", expanded=False):
        with st.form("add_item_form"):
            brand = st.selectbox("Brand", ["Rolex", "Tudor"])
            caliber = st.text_input("Caliber")
            part_number = st.text_input("Part Number")
            stock = st.number_input("Stock Quantity", min_value=0, step=1, value=1)
            submitted = st.form_submit_button("Add Item")
            if submitted:
                try:
                    result = d.append_inventory_item(brand, caliber, part_number, int(stock))
                    st.success(f'{result["brand"]} {result["part_number"]}: {result["note"]}')
                    if result.get("job_id"):
                        st.caption(f'Pipeline job: {result["job_id"]}')
                        worker = d.start_pipeline_job(result["job_id"])
                        st.caption(f'Worker started: PID {worker["pid"]}. Log: {worker["log_path"]}')
                except ValueError as e:
                    st.error(str(e))

    with st.expander("Import Inventory CSV", expanded=False):
        st.caption("Expected columns: Rolex/Tudor, Calibre, P-number, Stock")
        uploaded = st.file_uploader("Upload CSV", type=["csv"])
        if uploaded is not None:
            import pandas as pd
            preview = pd.read_csv(uploaded)
            st.dataframe(preview, width='stretch', hide_index=True)
            rows = [
                {"brand": r.get("Rolex/Tudor"), "caliber": r.get("Calibre"),
                 "part_number": r.get("P-number"), "stock": r.get("Stock")}
                for _, r in preview.iterrows()
            ]
            validation = d.validate_inventory_upload_rows(rows)
            if validation["ok"]:
                st.success(
                    f'Upload check passed: {validation["new_rows"]} new row(s), '
                    f'{validation["stock_updates"]} stock update(s).'
                )
            else:
                st.error("Upload check failed. No rows will be written until these issues are fixed.")
                for msg in validation["errors"][:10]:
                    st.write(f"- {msg}")
                if len(validation["errors"]) > 10:
                    st.caption(f'{len(validation["errors"]) - 10} more issue(s) not shown.')
            if st.button("Confirm import", disabled=not validation["ok"]):
                try:
                    result = d.append_inventory_rows(rows)
                    st.success(
                        f'Imported {result["rows_added"]} new row(s), updated stock for '
                        f'{result["stock_updated"]} existing row(s), queued {len(result["job_ids"])} job(s).'
                    )
                    if result["job_ids"]:
                        st.caption("Bulk jobs are queued. Process them with: python scripts/25_dashboard_pipeline_jobs.py --all")
                except ValueError as e:
                    st.error(str(e))

    _render_pipeline_jobs(st, d, conn)

    st.subheader("Export Inventory Recommendations")
    items = d.load_items(conn)
    if items:
        import pandas as pd
        export_df = pd.DataFrame(d.export_recommendations_rows(items))
        st.download_button(
            "Export Inventory Recommendations (CSV)",
            data=export_df.to_csv(index=False).encode("utf-8"),
            file_name="inventory_recommendations.csv", mime="text/csv",
        )
    else:
        st.info("No priced items to export yet.")


def _page_portfolio_analytics(st, d, conn) -> None:
    st.html(_CLIENT_CSS)
    st.title("Portfolio Analytics")
    items = d.load_items(conn)
    if not items:
        st.info("No priced items yet.")
        return

    ov = d.portfolio_overview(conn)
    st.metric("Total Inventory Value", f'€{ov["portfolio_value_eur"]:,.2f}' if ov["portfolio_value_eur"] is not None else "—")

    import pandas as pd
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Price distribution")
        bins = d.price_distribution_bins(items)
        if bins:
            st.bar_chart(pd.DataFrame(bins).set_index("bucket"))
        else:
            st.info("No data.")
    with col2:
        st.subheader("Sell-time distribution")
        dist = d.sell_time_distribution(items)
        st.bar_chart(pd.DataFrame(dist).set_index("bucket"))

    st.subheader("Top brands")
    tb = d.top_brands(items)
    st.dataframe(pd.DataFrame(tb), width='stretch', hide_index=True)


def main() -> None:  # pragma: no cover (Streamlit entrypoint)
    import streamlit as st
    d = _data_module()
    st.set_page_config(page_title="Vintage Watch Parts — Pricing Intelligence", layout="wide")
    conn, db_path = _connect(d)
    if not d.contract_ready(conn):
        st.error("Dashboard contract is not built for this database yet.")
        st.caption(f"Data source: {db_path}")
        st.write(
            "Run the contract builder first so the dashboard reads one authoritative "
            "row per inventory item instead of legacy backend joins."
        )
        st.code("python scripts/23_build_dashboard_contract.py\nstreamlit run scripts/16_dashboard.py", language="bash")
        conn.close()
        return

    page = st.sidebar.radio(
        "Navigate",
        ["Portfolio Overview", "Pricing Workspace", "Inventory Management", "Portfolio Analytics", "Admin — Technical View"],
    )
    if page == "Portfolio Overview":
        _page_portfolio_overview(st, d, conn)
    elif page == "Pricing Workspace":
        _page_pricing_workspace(st, d, conn)
    elif page == "Inventory Management":
        _page_inventory_management(st, d, conn)
    elif page == "Portfolio Analytics":
        _page_portfolio_analytics(st, d, conn)
    else:
        st.caption(f"Data source: {db_path}")
        st.html(build_dashboard_html(conn))
    conn.close()


if __name__ == "__main__":  # pragma: no cover
    main()
