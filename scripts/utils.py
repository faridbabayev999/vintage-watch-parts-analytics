"""
utils.py
========
Source-agnostic cleaning helpers shared across staging tables
(inventory, active listings, historical). No brand/caliber/part-number
extraction lives here — that's matching-module territory.
"""

from __future__ import annotations

import re
import unicodedata

import pandas as pd

SHIPPING_DE_EUR = 5
SHIPPING_US_EUR = 25

# TODO: source this — currently unset. No verified US import duty rate
# for watch parts (HTS classification dependent) has been confirmed yet.
US_DUTY_RATE = 0.0
# TODO: source this — currently unset. US sales tax varies by destination
# state and isn't collected at import; no verified single rate exists.
US_SALES_TAX_RATE = 0.0


def _is_missing(value) -> bool:
    return value is None or (isinstance(value, float) and pd.isna(value))


def normalize_title(title):
    """Lowercase, unicode-normalize, and collapse whitespace. No entity extraction."""
    if _is_missing(title):
        return None
    text = unicodedata.normalize("NFKC", str(title))
    text = text.lower()
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def _slug_part(value, default: str) -> str:
    if _is_missing(value):
        return default
    text = str(value).strip().lower()
    if not text:
        return default
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text or default


def slugify_canonical_id(brand, caliber, part_number) -> str:
    """
    Deterministic id from brand + caliber + part_number.
    caliber=None (or blank) renders as the literal "unknown" inside the id
    string only — it is never written back as the real caliber value.
    """
    brand_part = _slug_part(brand, default="unknown")
    caliber_part = _slug_part(caliber, default="unknown")
    part_number_part = _slug_part(part_number, default="unknown")
    return f"{brand_part}_{caliber_part}_{part_number_part}"


def part_number_is_distinctive(part_number) -> bool:
    """True if the part number has >= 5 alphanumeric characters. Inventory-only."""
    if _is_missing(part_number):
        return False
    alnum_chars = [c for c in str(part_number) if c.isalnum()]
    return len(alnum_chars) >= 5


def compute_scenario_prices(price_eur):
    """
    DEPRECATED (2026-07-30, Phase 2/3 scenario-engine audit): this is
    listing-level scenario calculation (runs during cleaning, on each
    individual listing's raw price). It has NO downstream consumers — no
    dashboard, no TMV file reads price_virtual_eur/landed_cost_de_eur/
    landed_cost_us_eur — and it is the wrong grain for the professor's
    requirement, which needs scenarios computed on the final TMV price, not
    on individual evidence listings. Replaced by the TMV-level scenario
    engine (scripts/17_scenario_engine.py), which reads sourced/dated rates
    from ref_shipping_rates/ref_customs_rates/ref_tax_rates instead of the
    bare constants below.

    Left running (not removed) pending confirmation that no other consumer
    depends on the staging columns it writes — see
    docs/PHASE3_SCENARIO_ENGINE_TRACE_AND_DESIGN.md. Remove once that is
    confirmed after this port's tests pass.

    Three standardized, fixed-shipping comparison scenarios computed off the
    sale price alone. A listing's actual shipping cost is never used here —
    these scenarios are deliberately standardized so listings with wildly
    different real shipping fees stay comparable on the same basis:

      Scenario C (virtual): price_virtual_eur      = price_eur
      Scenario B (Germany):  landed_cost_de_eur     = price_eur + SHIPPING_DE_EUR
      Scenario A (USA):      landed_cost_us_eur     = price_eur + SHIPPING_US_EUR
                                                       + estimated_import_charges_us_eur

    Import charges use US_DUTY_RATE / US_SALES_TAX_RATE, both currently
    unset (0.0) pending a verified source — see TODOs above. Callers must
    treat estimated_import_charges_us_eur as a placeholder, not a real
    duty-free confirmation, until those constants are sourced.
    """
    if _is_missing(price_eur):
        return {
            "price_virtual_eur": None,
            "landed_cost_de_eur": None,
            "landed_cost_us_eur": None,
            "shipping_de_eur": SHIPPING_DE_EUR,
            "shipping_us_eur": SHIPPING_US_EUR,
            "estimated_import_charges_us_eur": None,
        }

    price_virtual_eur = float(price_eur)
    estimated_import_charges_us_eur = price_virtual_eur * (US_DUTY_RATE + US_SALES_TAX_RATE)
    landed_cost_de_eur = price_virtual_eur + SHIPPING_DE_EUR
    landed_cost_us_eur = price_virtual_eur + SHIPPING_US_EUR + estimated_import_charges_us_eur

    return {
        "price_virtual_eur": round(price_virtual_eur, 2),
        "landed_cost_de_eur": round(landed_cost_de_eur, 2),
        "landed_cost_us_eur": round(landed_cost_us_eur, 2),
        "shipping_de_eur": SHIPPING_DE_EUR,
        "shipping_us_eur": SHIPPING_US_EUR,
        "estimated_import_charges_us_eur": round(estimated_import_charges_us_eur, 2),
    }
