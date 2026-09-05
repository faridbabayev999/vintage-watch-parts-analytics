"""
evidence_identity.py
=====================
Module 5: stable evidence identity — the shared module the pipeline's
staging cleaners call into, replacing the positional `stg_id =
range(1, len(df)+1)` pattern (docs/MODULE5_LINEAGE_INTEGRITY_AUDIT.md)
with a deterministic, content-derived identity.

Grain separation (docs/MODULE5_EVIDENCE_IDENTITY_IMPLEMENTATION_CHECKLIST.md
§0) — enforced here, not just documented:

1. Raw ingestion identity   — owned by 01_ingest.py's next_id(), untouched
2. Stable evidence identity — this module's job: one UID per real-world
   evidence object (a listing, or a VCP cluster). NEVER includes
   inventory_uid.
3. Evidence observation identity — this module's job: one UID per
   distinct snapshot of an evidence object (price/condition/title at a
   point in time).
4. Candidate relationship identity — NOT this module's job. That's
   (inventory_uid, stable_evidence_uid, matching_rule) in
   match_candidates_* — a many-to-many relationship, computed downstream.

Nothing in this module reads inventory_uid. If a caller ever needs to
pass inventory_uid to a function here, that is a grain violation and
the function is wrong.
"""

from __future__ import annotations

import hashlib

import pandas as pd


def _is_missing(value) -> bool:
    return value is None or (isinstance(value, float) and pd.isna(value)) or str(value).strip() == ""


def _sha256(*parts: str) -> str:
    """Deterministic, order-independent within a fixed part order: same
    parts in, same digest out, regardless of row order, DataFrame index,
    or when it's computed. No timestamp, no random, no execution-order
    dependency of any kind."""
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------
# Stable evidence identity (grain 2) — one per source type
# ---------------------------------------------------------------------

def active_evidence_uid(marketplace, item_id) -> str | None:
    """Active-listing evidence identity (stg_active_targeted, stg_active_broad).

    marketplace + item_id ONLY. inventory_uid is deliberately not a
    parameter here — the same listing legitimately pairs with multiple
    inventory items (confirmed empirically: up to 8 distinct
    inventory_uids for one item_id in the pilot data), and that
    many-to-many relationship belongs in match_candidates_*, not in the
    evidence identity itself. See module docstring.
    """
    if _is_missing(marketplace) or _is_missing(item_id):
        return None
    return "EV-ACTIVE-" + _sha256("active", str(marketplace).strip(), str(item_id).strip())


def sold_ebay_evidence_uid(item_number) -> str | None:
    """Sold-eBay evidence identity (stg_historical_ebay_sold).

    item_number ONLY — this table has no marketplace column (verified
    directly against the schema, not assumed; see
    docs/MODULE5_EVIDENCE_IDENTITY_IMPLEMENTATION_CHECKLIST.md §5). Do
    not invent a marketplace composite.
    """
    if _is_missing(item_number):
        return None
    return "EV-SOLD-" + _sha256("sold_ebay", str(item_number).strip())


def vcp_cluster_evidence_uid(duplicate_group_id) -> str | None:
    """VCP aggregate evidence identity — CLUSTER grain
    (stg_historical_vcp_aggregate).

    duplicate_group_id is confirmed NOT a row identity (222 groups in
    the pilot data share a group id across multiple, differently-priced
    rows) — it is the cluster/product identity. Individual rows within
    a cluster are disambiguated at the observation grain
    (observation_uid, below), never collapsed here.
    """
    if _is_missing(duplicate_group_id):
        return None
    return "EV-VCPCLUSTER-" + _sha256("vcp_cluster", str(duplicate_group_id).strip())


def fallback_content_evidence_uid(normalized_title, condition, price_eur) -> str | None:
    """Fallback identity when no natural key is available. Deterministic
    over normalized content, not row position — but explicitly a LOWER
    confidence tier (identity_confidence='LOW') since two distinct
    listings could collide if their normalized content is identical.
    Callers must tag identity_source='fallback_hash' when using this."""
    if _is_missing(normalized_title):
        return None
    price_part = "" if _is_missing(price_eur) else f"{float(price_eur):.2f}"
    return "EV-FALLBACK-" + _sha256(
        "fallback", str(normalized_title).strip(), str(condition or "").strip(), price_part
    )


# ---------------------------------------------------------------------
# Evidence observation identity (grain 3)
# ---------------------------------------------------------------------

def observation_uid(stable_evidence_uid, raw_id) -> str | None:
    """One observation per (stable_evidence_uid, raw_id) — raw_id is
    already a stable, append-only identifier (01_ingest.py's next_id()),
    so this is deterministic and never collides across genuinely
    different raw rows. Two raw rows for the same stable_evidence_uid
    (e.g. a VCP cluster's 222 known multi-row groups, or a listing
    re-collected on a later date) get two distinct observation_uids,
    never merged."""
    if _is_missing(stable_evidence_uid) or _is_missing(raw_id):
        return None
    return "OBS-" + _sha256(str(stable_evidence_uid), str(raw_id))


# ---------------------------------------------------------------------
# Vectorized helpers — applied once per staging cleaner, not per-row
# Python loops. Order-independent: computed from column values only.
# ---------------------------------------------------------------------

def add_active_identity_columns(df: pd.DataFrame, *, marketplace_col: str, item_id_col: str,
                                 raw_id_col: str = "raw_id") -> pd.DataFrame:
    """Adds stable_evidence_uid, identity_source, identity_confidence,
    identity_type, natural_key_type, observation_uid to df. Does not
    mutate df in place; returns a new frame with the columns appended.
    Row order of df is irrelevant to the values produced (verified by
    the shuffled-input identity test)."""
    df = df.copy()
    df["stable_evidence_uid"] = [
        active_evidence_uid(m, i) for m, i in zip(df[marketplace_col], df[item_id_col])
    ]
    df["identity_type"] = "INDIVIDUAL_LISTING"
    df["identity_source"] = "natural_key"
    df["identity_confidence"] = "HIGH"
    df["natural_key_type"] = "ITEM_ID"
    df["observation_uid"] = [
        observation_uid(u, r) for u, r in zip(df["stable_evidence_uid"], df[raw_id_col])
    ]
    return df


def add_sold_ebay_identity_columns(df: pd.DataFrame, *, item_number_col: str,
                                    raw_id_col: str = "raw_id") -> pd.DataFrame:
    df = df.copy()
    df["stable_evidence_uid"] = [sold_ebay_evidence_uid(v) for v in df[item_number_col]]
    df["identity_type"] = "INDIVIDUAL_LISTING"
    df["identity_source"] = "natural_key"
    df["identity_confidence"] = "HIGH"
    df["natural_key_type"] = "ITEM_NUMBER"
    df["observation_uid"] = [
        observation_uid(u, r) for u, r in zip(df["stable_evidence_uid"], df[raw_id_col])
    ]
    return df


def add_vcp_identity_columns(df: pd.DataFrame, *, duplicate_group_id_col: str,
                              raw_id_col: str = "raw_id") -> pd.DataFrame:
    df = df.copy()
    df["stable_evidence_uid"] = [vcp_cluster_evidence_uid(v) for v in df[duplicate_group_id_col]]
    df["identity_type"] = "AGGREGATE_CLUSTER"
    df["identity_source"] = "natural_key"
    df["identity_confidence"] = "HIGH"
    df["natural_key_type"] = "DUPLICATE_GROUP_ID"
    df["observation_uid"] = [
        observation_uid(u, r) for u, r in zip(df["stable_evidence_uid"], df[raw_id_col])
    ]
    return df


def add_fallback_identity_columns(df: pd.DataFrame, *, normalized_title_col: str,
                                   condition_col: str | None, price_eur_col: str | None,
                                   raw_id_col: str = "raw_id") -> pd.DataFrame:
    """Used only when no source-specific natural key is available (e.g.
    stg_historical today — source_record_id confirmed unpopulated, see
    docs/MODULE5_EVIDENCE_IDENTITY_IMPLEMENTATION_CHECKLIST.md §5).
    Always LOW confidence."""
    df = df.copy()
    cond = df[condition_col] if condition_col else [None] * len(df)
    price = df[price_eur_col] if price_eur_col else [None] * len(df)
    df["stable_evidence_uid"] = [
        fallback_content_evidence_uid(t, c, p)
        for t, c, p in zip(df[normalized_title_col], cond, price)
    ]
    df["identity_type"] = "INDIVIDUAL_LISTING"
    df["identity_source"] = "fallback_hash"
    df["identity_confidence"] = "LOW"
    df["natural_key_type"] = "FALLBACK_CONTENT_HASH"
    df["observation_uid"] = [
        observation_uid(u, r) for u, r in zip(df["stable_evidence_uid"], df[raw_id_col])
    ]
    return df
