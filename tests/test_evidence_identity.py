"""
tests/test_evidence_identity.py
=================================
Identity tests for scripts/evidence_identity.py, per
docs/MODULE5_EVIDENCE_IDENTITY_IMPLEMENTATION_CHECKLIST.md §6:
deterministic, order-independent, incrementally stable, duplicate-
collection-safe, multi-inventory-relationship-preserving (by
construction — this module never takes inventory_uid), and
different-listings-different-UID (including the marketplace-collision
case).

No database, no file I/O — pure function tests only.
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
import evidence_identity as ei  # noqa: E402


# ---------------------------------------------------------------------
# 1. Deterministic
# ---------------------------------------------------------------------

def test_active_uid_deterministic_same_input_same_output():
    a = ei.active_evidence_uid("EBAY_DE", "117314526585")
    b = ei.active_evidence_uid("EBAY_DE", "117314526585")
    assert a == b
    assert a is not None


def test_sold_ebay_uid_deterministic():
    a = ei.sold_ebay_evidence_uid("267714807206")
    b = ei.sold_ebay_evidence_uid("267714807206")
    assert a == b


def test_vcp_cluster_uid_deterministic():
    a = ei.vcp_cluster_evidence_uid("04447aa96926269f")
    b = ei.vcp_cluster_evidence_uid("04447aa96926269f")
    assert a == b


def test_uid_does_not_depend_on_execution_order_or_wall_clock():
    # Calling twice, with unrelated calls interleaved, must not change
    # the result -- no hidden global state, no timestamp component.
    first = ei.active_evidence_uid("EBAY_US", "1")
    _ = ei.active_evidence_uid("EBAY_US", "2")
    _ = ei.sold_ebay_evidence_uid("999")
    second = ei.active_evidence_uid("EBAY_US", "1")
    assert first == second


# ---------------------------------------------------------------------
# 2. Order independence (shuffled input -> same UID assignment)
# ---------------------------------------------------------------------

def test_shuffled_dataframe_rows_get_identical_uids_regardless_of_order():
    df = pd.DataFrame({
        "marketplace": ["EBAY_DE", "EBAY_US", "EBAY_DE", "EBAY_US"],
        "item_id": ["111", "222", "333", "444"],
        "raw_id": [1, 2, 3, 4],
    })
    forward = ei.add_active_identity_columns(df, marketplace_col="marketplace", item_id_col="item_id")

    shuffled = df.iloc[[3, 1, 0, 2]].reset_index(drop=True)
    reversed_order = ei.add_active_identity_columns(shuffled, marketplace_col="marketplace", item_id_col="item_id")

    forward_map = dict(zip(forward["item_id"], forward["stable_evidence_uid"]))
    shuffled_map = dict(zip(reversed_order["item_id"], reversed_order["stable_evidence_uid"]))
    assert forward_map == shuffled_map


# ---------------------------------------------------------------------
# 3. Incremental stability (new records added -> existing UIDs unchanged)
# ---------------------------------------------------------------------

def test_appending_new_rows_does_not_change_existing_uids():
    day1 = pd.DataFrame({
        "marketplace": ["EBAY_DE", "EBAY_US"],
        "item_id": ["111", "222"],
        "raw_id": [1, 2],
    })
    day1_result = ei.add_active_identity_columns(day1, marketplace_col="marketplace", item_id_col="item_id")

    day2 = pd.DataFrame({
        "marketplace": ["EBAY_DE", "EBAY_US", "EBAY_DE"],
        "item_id": ["111", "222", "333"],  # 333 is new
        "raw_id": [1, 2, 3],
    })
    day2_result = ei.add_active_identity_columns(day2, marketplace_col="marketplace", item_id_col="item_id")

    day1_map = dict(zip(day1_result["item_id"], day1_result["stable_evidence_uid"]))
    day2_map = dict(zip(day2_result["item_id"], day2_result["stable_evidence_uid"]))
    assert day1_map["111"] == day2_map["111"]
    assert day1_map["222"] == day2_map["222"]
    assert "333" in day2_map
    assert day2_map["333"] not in (day1_map["111"], day1_map["222"])


# ---------------------------------------------------------------------
# 4. Duplicate collection -> same evidence UID, distinct observation UID
# ---------------------------------------------------------------------

def test_same_listing_collected_twice_same_evidence_uid_different_observation_uid():
    first_collection = ei.active_evidence_uid("EBAY_DE", "555")
    second_collection = ei.active_evidence_uid("EBAY_DE", "555")
    assert first_collection == second_collection

    obs1 = ei.observation_uid(first_collection, raw_id=10)
    obs2 = ei.observation_uid(second_collection, raw_id=11)  # different raw_id = different observation
    assert obs1 != obs2


def test_vcp_multi_row_cluster_keeps_one_evidence_uid_but_distinct_observations():
    # Mirrors the 222 confirmed real multi-row duplicate_group_id groups.
    df = pd.DataFrame({
        "duplicate_group_id": ["04447aa96926269f"] * 4,
        "raw_id": [754, 755, 756, 757],
        "avg_price_eur": [129.48, 130.37, 115.77, 133.87],
    })
    result = ei.add_vcp_identity_columns(df, duplicate_group_id_col="duplicate_group_id")
    assert result["stable_evidence_uid"].nunique() == 1
    assert result["observation_uid"].nunique() == 4


# ---------------------------------------------------------------------
# 5. Multi-inventory relationship is not representable here at all
#    (by construction -- this module has no inventory_uid parameter)
# ---------------------------------------------------------------------

def test_active_evidence_uid_signature_has_no_inventory_parameter():
    import inspect
    params = inspect.signature(ei.active_evidence_uid).parameters
    assert "inventory_uid" not in params


def test_vcp_evidence_uid_signature_has_no_inventory_parameter():
    import inspect
    params = inspect.signature(ei.vcp_cluster_evidence_uid).parameters
    assert "inventory_uid" not in params


# ---------------------------------------------------------------------
# 6. Different listings -> different UIDs, including the marketplace-
#    collision case (same item_id, different marketplace)
# ---------------------------------------------------------------------

def test_different_item_ids_different_uids():
    a = ei.active_evidence_uid("EBAY_DE", "111")
    b = ei.active_evidence_uid("EBAY_DE", "222")
    assert a != b


def test_marketplace_collision_same_item_id_different_marketplace_different_uid():
    de = ei.active_evidence_uid("EBAY_DE", "12345")
    us = ei.active_evidence_uid("EBAY_US", "12345")
    assert de != us


def test_sold_and_active_uids_never_collide_across_source_types():
    active = ei.active_evidence_uid("EBAY_DE", "999")
    sold = ei.sold_ebay_evidence_uid("999")
    cluster = ei.vcp_cluster_evidence_uid("999")
    assert len({active, sold, cluster}) == 3


# ---------------------------------------------------------------------
# Fallback path
# ---------------------------------------------------------------------

def test_fallback_hash_deterministic_and_tagged_low_confidence():
    df = pd.DataFrame({
        "normalized_title": ["rolex crown 3135"],
        "condition": ["USED"],
        "price_eur": [50.0],
        "raw_id": [1],
    })
    result = ei.add_fallback_identity_columns(
        df, normalized_title_col="normalized_title", condition_col="condition",
        price_eur_col="price_eur",
    )
    assert result["identity_confidence"].iloc[0] == "LOW"
    assert result["identity_source"].iloc[0] == "fallback_hash"
    again = ei.fallback_content_evidence_uid("rolex crown 3135", "USED", 50.0)
    assert result["stable_evidence_uid"].iloc[0] == again


def test_missing_natural_key_returns_none_not_a_fabricated_uid():
    assert ei.active_evidence_uid(None, "123") is None
    assert ei.active_evidence_uid("EBAY_DE", None) is None
    assert ei.sold_ebay_evidence_uid(None) is None
    assert ei.vcp_cluster_evidence_uid("") is None
