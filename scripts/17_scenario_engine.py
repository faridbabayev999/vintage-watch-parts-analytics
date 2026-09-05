"""
17_scenario_engine.py
======================
Module 6 — TMV-level scenario engine. Computes the professor-required
Germany/US/Virtual landed-cost comparison FROM THE FINAL TMV PRICE, not from
individual evidence listings (that was Module 1's utils.compute_scenario_prices,
now deprecated -- see its docstring and docs/PHASE3_SCENARIO_ENGINE_TRACE_AND_DESIGN.md).

Design lineage: this mirrors Implementation B's scenario_landed() formula
(customs on price+shipping, tax on the taxable subtotal) -- identity-safe pure
math, already audited as reusable (docs/TMV_B_AUDIT_AND_A_NATIVE_DESIGN.md).
The only change from B: every rate is now READ from a dated, sourced reference
table (ref_shipping_rates/ref_customs_rates/ref_tax_rates) via an ASOF lookup,
never a bare Python constant.

No hidden constants: a missing required rate raises ConfigurationError rather
than silently defaulting to 0 -- the caller always knows whether a number is
"looked up" or "not configured", never conflates the two.

Scenarios:
  A (US)      -- shipping + customs (HS 9114.90) + sales tax
  B (DE)      -- shipping only, no import charges (both rates seeded at 0.0,
                 sourced as "intra-EU / domestic", not a code-level special case)
  C (Virtual) -- selling price only, no lookup performed at all
"""
from __future__ import annotations

from datetime import date

DEFAULT_HS_CODE = "9114.90"


class ConfigurationError(Exception):
    """Raised when a required reference rate has no row for the given
    country/date -- never silently treated as 0."""


def _latest_rate(conn, table: str, key_cols: dict, rate_col: str, as_of: date):
    where = " AND ".join(f"{c} = ?" for c in key_cols)
    params = list(key_cols.values()) + [as_of]
    row = conn.execute(
        f"""SELECT {rate_col}, source, valid_from FROM {table}
            WHERE {where} AND valid_from <= ?
            ORDER BY valid_from DESC LIMIT 1""",
        params,
    ).fetchone()
    return row  # (rate, source, valid_from) or None


def lookup_shipping(conn, country: str, as_of: date = None):
    as_of = as_of or date.today()
    row = _latest_rate(conn, "ref_shipping_rates", {"country": country}, "shipping_cost", as_of)
    if row is None:
        raise ConfigurationError(f"No ref_shipping_rates row for country={country!r} as of {as_of}")
    cost, source, valid_from = row
    return {"amount_eur": float(cost), "source": source, "valid_from": valid_from}


def lookup_customs(conn, hs_code: str, country: str, as_of: date = None):
    as_of = as_of or date.today()
    row = _latest_rate(conn, "ref_customs_rates", {"hs_code": hs_code, "country": country}, "duty_rate", as_of)
    if row is None:
        raise ConfigurationError(f"No ref_customs_rates row for hs_code={hs_code!r} country={country!r} as of {as_of}")
    rate, source, valid_from = row
    return {"rate": float(rate), "source": source, "valid_from": valid_from}


def lookup_tax(conn, country: str, tax_type: str, as_of: date = None):
    as_of = as_of or date.today()
    row = _latest_rate(conn, "ref_tax_rates", {"country": country, "tax_type": tax_type}, "rate", as_of)
    if row is None:
        raise ConfigurationError(f"No ref_tax_rates row for country={country!r} tax_type={tax_type!r} as of {as_of}")
    rate, source, valid_from = row
    return {"rate": float(rate), "source": source, "valid_from": valid_from}


def _landed(tmv_eur: float, shipping: dict, customs: dict | None, tax: dict | None) -> dict:
    """Same structure as B's scenario_landed(): customs on (price+ship);
    tax on the taxable subtotal (price+ship+customs)."""
    price = round(tmv_eur, 2)
    ship = round(shipping["amount_eur"], 2)
    customs_amt = round((price + ship) * customs["rate"], 2) if customs else 0.0
    taxable = price + ship + customs_amt
    tax_amt = round(taxable * tax["rate"], 2) if tax else 0.0
    landed = round(price + ship + customs_amt + tax_amt, 2)
    return {
        "price_eur": price, "shipping_eur": ship,
        "customs_eur": customs_amt, "tax_eur": tax_amt,
        "landed_cost_eur": landed,
        "sources": {
            "shipping": shipping["source"],
            "customs": customs["source"] if customs else None,
            "tax": tax["source"] if tax else None,
        },
    }


def compute_scenarios(conn, tmv_eur: float, as_of: date = None, hs_code: str = DEFAULT_HS_CODE) -> dict:
    """Return {'A': {...US...}, 'B': {...DE...}, 'C': {...virtual...}}.
    Raises ConfigurationError if a required US/DE rate is missing -- never
    silently substitutes 0 for an unconfigured (as opposed to genuinely-zero)
    rate."""
    as_of = as_of or date.today()

    scenario_c = {
        "price_eur": round(tmv_eur, 2), "shipping_eur": 0.0, "customs_eur": 0.0, "tax_eur": 0.0,
        "landed_cost_eur": round(tmv_eur, 2),
        "sources": {"shipping": None, "customs": None, "tax": None},
        "note": "Virtual customer: selling price only, no lookup performed.",
    }

    ship_de = lookup_shipping(conn, "DE", as_of)
    customs_de = lookup_customs(conn, hs_code, "DE", as_of)
    tax_de = lookup_tax(conn, "DE", "import_tax", as_of)
    scenario_b = _landed(tmv_eur, ship_de, customs_de, tax_de)

    ship_us = lookup_shipping(conn, "US", as_of)
    customs_us = lookup_customs(conn, hs_code, "US", as_of)
    tax_us = lookup_tax(conn, "US", "sales_tax", as_of)
    scenario_a = _landed(tmv_eur, ship_us, customs_us, tax_us)

    return {"A": scenario_a, "B": scenario_b, "C": scenario_c}


# ══════════════════════════════════════════════════════════════════════════
# PRICE / TURNOVER-TIME SIMULATOR (owner decision 2026-07-30,
# docs/TMV_DEMAND_PARAMETER_DESIGN.md)
# ══════════════════════════════════════════════════════════════════════════
#
# Scenario-simulator ONLY. Does NOT modify 13_build_tmv.py's TMV formula or
# the backend turnover_survival calculation -- both stay price-independent,
# exactly as backtest-verified (docs/MODULE4_TURNOVER.md). This function
# answers a *what-if* question about a hypothetical listing price, using an
# explicitly disclosed, NOT-fitted assumption (epsilon), carried from
# Implementation B's own disclosed EPS=1.5 -- never presented as a learned
# elasticity.

PRICE_ELASTICITY_DISCLAIMER = (
    "Scenario simulation uses a disclosed price-elasticity assumption "
    "(epsilon). This is not a statistically fitted market elasticity."
)


def _lookup_tmv_parameter(conn, name: str, default: float):
    row = conn.execute(
        "SELECT parameter_value, active_flag FROM ref_tmv_parameters WHERE parameter_name = ?", [name]
    ).fetchone()
    if row is None:
        return default, None
    value, active = row
    return (float(value), name) if active else (default, None)


def simulate_price_time(conn, tmv_eur: float, base_days: float, scenario_price_eur: float) -> dict:
    """days = base_days * (scenario_price/tmv_eur)^epsilon. Higher price ->
    longer simulated days; lower price -> shorter. epsilon is read from
    ref_tmv_parameters (default 1.5 if unconfigured -- the same value carried
    from B, but callers should treat a missing row as a configuration gap,
    surfaced via `epsilon_source=None`)."""
    epsilon, source = _lookup_tmv_parameter(conn, "price_elasticity_epsilon", default=1.5)
    if tmv_eur is None or tmv_eur <= 0 or base_days is None:
        return {"simulated_days": None, "epsilon": epsilon, "epsilon_source": source,
                "disclaimer": PRICE_ELASTICITY_DISCLAIMER,
                "note": "No TMV baseline or base turnover estimate available -- cannot simulate."}
    price_change_pct = round((scenario_price_eur / tmv_eur - 1) * 100, 2)
    factor = (scenario_price_eur / tmv_eur) ** epsilon
    simulated_days = round(base_days * factor, 1)
    return {
        "simulated_days": simulated_days, "price_change_pct": price_change_pct,
        "epsilon": epsilon, "epsilon_source": source,
        "disclaimer": PRICE_ELASTICITY_DISCLAIMER,
    }
