"""
Regression tests for ebay_watch_parts_collector.py after its OAuth/pagination
internals were extracted into ebay_api_common.py (shared with
scripts/04_collect_targeted_active.py).

Goal: prove the refactor did not change this module's behavior — imports
cleanly, CLI defaults are identical, authentication (token fetch + cache)
still works, pagination still walks every configured marketplace and tags
items correctly, retry/backoff still applies, and the output CSV schema is
byte-identical to the pre-refactor column set. None of this touches the real
network, real credentials, or the real data/latest.csv — deliberately not
relying on live output being byte-identical, since real marketplace listings
change between runs.
"""

from __future__ import annotations

import csv
import io
import json
import sys
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError

import pytest

BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR))

import ebay_api_common  # noqa: E402
import ebay_watch_parts_collector as collector  # noqa: E402


# ── Guard: never touch the real token cache or real data output ──────────────

@pytest.fixture(autouse=True)
def guard_production_untouched(tmp_path, monkeypatch):
    fake_cache = tmp_path / ".ebay_token_cache.json"
    fake_output_dir = tmp_path / "data"
    monkeypatch.setattr(ebay_api_common, "TOKEN_CACHE", fake_cache)
    monkeypatch.setattr(collector, "OUTPUT_DIR", fake_output_dir)

    real_cache = BASE_DIR / ".ebay_token_cache.json"
    real_data_dir = BASE_DIR / "data"
    before_cache = real_cache.read_bytes() if real_cache.exists() else None
    before_latest = (real_data_dir / "latest.csv").stat().st_mtime if (real_data_dir / "latest.csv").exists() else None

    yield

    after_cache = real_cache.read_bytes() if real_cache.exists() else None
    after_latest = (real_data_dir / "latest.csv").stat().st_mtime if (real_data_dir / "latest.csv").exists() else None
    assert before_cache == after_cache, "test must not touch the real token cache"
    assert before_latest == after_latest, "test must not touch the real data/latest.csv"


@pytest.fixture(autouse=True)
def no_real_network(monkeypatch):
    def _forbidden(*args, **kwargs):
        raise AssertionError("Test attempted a real network call — must be mocked.")
    monkeypatch.setattr("ebay_api_common.urlopen", _forbidden)


# ── Public API / import surface unchanged ─────────────────────────────────────

def test_module_imports_cleanly_and_exposes_expected_public_api():
    for name in ("flatten_item", "search_items", "write_csv", "inspect_csv", "parse_args", "main"):
        assert hasattr(collector, name), f"expected public function {name} missing after refactor"
    assert collector.MARKETPLACES == ebay_api_common.MARKETPLACES
    assert collector.DEFAULT_SCOPE == ebay_api_common.DEFAULT_SCOPE


def test_cli_defaults_unchanged(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["ebay_watch_parts_collector.py"])
    args = collector.parse_args()
    assert args.keyword == "vintage watch spare parts"
    assert args.limit == 200
    assert args.max_items is None
    assert args.sort == "newlyListed"
    assert args.filter == ["buyingOptions:{FIXED_PRICE|AUCTION}"]
    assert args.inspect is False


def test_cli_repeatable_filter_and_inspect_flag(monkeypatch):
    monkeypatch.setattr(
        sys, "argv",
        ["ebay_watch_parts_collector.py", "--filter", "conditionIds:{3000}", "--filter", "conditionIds:{7000}", "--inspect"],
    )
    args = collector.parse_args()
    # argparse's action="append" with a non-empty default list appends onto
    # that default rather than replacing it — pre-existing behavior, not
    # touched by the refactor; asserting it explicitly here so a future
    # argparse/config change can't silently alter it.
    assert args.filter == [
        "buyingOptions:{FIXED_PRICE|AUCTION}", "conditionIds:{3000}", "conditionIds:{7000}",
    ]
    assert args.inspect is True


# ── Authentication (token fetch + cache), through the shared common module ───

def test_get_access_token_fetches_and_caches(monkeypatch, tmp_path):
    monkeypatch.setenv("EBAY_CLIENT_ID", "fake-id")
    monkeypatch.setenv("EBAY_CLIENT_SECRET", "fake-secret")
    fake_cache = tmp_path / ".ebay_token_cache.json"
    monkeypatch.setattr(ebay_api_common, "TOKEN_CACHE", fake_cache)

    call_count = {"n": 0}

    def fake_request(url, *, headers, data=None, **kwargs):
        call_count["n"] += 1
        assert url == ebay_api_common.TOKEN_URL
        assert headers["Authorization"].startswith("Basic ")
        return {"access_token": "fake-token-abc", "expires_in": 7200}

    with patch.object(ebay_api_common, "request_json_with_retry", side_effect=fake_request):
        token1 = collector.get_access_token(collector.DEFAULT_SCOPE)
        token2 = collector.get_access_token(collector.DEFAULT_SCOPE)  # should hit cache, not refetch

    assert token1 == "fake-token-abc"
    assert token2 == "fake-token-abc"
    assert call_count["n"] == 1, "second call within expiry window must use the cache, not refetch"
    assert fake_cache.exists()


def test_get_access_token_missing_credentials_raises(monkeypatch):
    monkeypatch.delenv("EBAY_CLIENT_ID", raising=False)
    monkeypatch.delenv("EBAY_CLIENT_SECRET", raising=False)
    with pytest.raises(SystemExit):
        collector.get_access_token(collector.DEFAULT_SCOPE)


# ── Pagination across all configured marketplaces, through search_items ──────

def test_search_items_walks_every_configured_marketplace_and_tags_items():
    """collector.search_items delegates to ebay_api_common.search_items,
    which loops MARKETPLACES with max_pages=None (unbounded-until-exhausted,
    same as the pre-refactor inline loop). Confirm every marketplace is
    queried exactly once and items carry source_country/source_marketplace_id."""
    seen_marketplaces = []

    def fake_request(url, *, headers, data=None, **kwargs):
        marketplace_id = headers["X-EBAY-C-MARKETPLACE-ID"]
        seen_marketplaces.append(marketplace_id)
        return {
            "itemSummaries": [{
                "itemId": f"item-{marketplace_id}",
                "title": "Test listing",
                "price": {"value": "10.00", "currency": "USD"},
            }],
            # no "next" key: single page per marketplace
        }

    with patch.object(ebay_api_common, "request_json_with_retry", side_effect=fake_request):
        items = collector.search_items(
            token="fake-token", keyword="rolex part", limit=50, max_items=None,
            sort="newlyListed", filters=["buyingOptions:{FIXED_PRICE|AUCTION}"],
        )

    assert seen_marketplaces == list(collector.MARKETPLACES.values())
    assert len(items) == len(collector.MARKETPLACES)
    for item, (country, marketplace_id) in zip(items, collector.MARKETPLACES.items()):
        assert item["source_marketplace_id"] == marketplace_id
        assert item["source_country"] == country


def test_search_items_respects_max_items_across_marketplaces():
    from urllib.parse import urlparse, parse_qs

    def fake_request(url, *, headers, data=None, **kwargs):
        marketplace_id = headers["X-EBAY-C-MARKETPLACE-ID"]
        requested_limit = int(parse_qs(urlparse(url).query)["limit"][0])
        return {"itemSummaries": [
            {"itemId": f"item-{marketplace_id}-{i}", "title": "x", "price": {"value": "1", "currency": "USD"}}
            for i in range(min(5, requested_limit))
        ]}

    with patch.object(ebay_api_common, "request_json_with_retry", side_effect=fake_request):
        items = collector.search_items(
            token="fake-token", keyword="rolex part", limit=50, max_items=3,
            sort="newlyListed", filters=[],
        )

    assert len(items) == 3, "max_items must cap total items across all marketplaces, not per marketplace"


def test_search_items_paginates_within_a_marketplace_until_no_next():
    """A single marketplace returning a 'next' link must be paginated (offset
    advances) until eBay stops returning one — same behavior as the original
    inline while-loop, now inside search_items_single_marketplace."""
    pages_served = {"US": 0}

    def fake_request(url, *, headers, data=None, **kwargs):
        marketplace_id = headers["X-EBAY-C-MARKETPLACE-ID"]
        if marketplace_id != "EBAY_US":
            return {"itemSummaries": []}
        pages_served["US"] += 1
        if pages_served["US"] == 1:
            return {"itemSummaries": [{"itemId": "us-page1", "title": "x", "price": {"value": "1", "currency": "USD"}}], "next": "..."}
        return {"itemSummaries": [{"itemId": "us-page2", "title": "x", "price": {"value": "1", "currency": "USD"}}]}

    with patch.object(ebay_api_common, "request_json_with_retry", side_effect=fake_request):
        items = collector.search_items(
            token="fake-token", keyword="rolex part", limit=50, max_items=None,
            sort="newlyListed", filters=[],
        )

    us_items = [i for i in items if i.get("source_marketplace_id") == "EBAY_US"]
    assert [i["itemId"] for i in us_items] == ["us-page1", "us-page2"]
    assert pages_served["US"] == 2


# ── Retry / backoff still applies through the shared request path ────────────

def test_retry_on_throttling_then_success(monkeypatch):
    monkeypatch.setattr(ebay_api_common.time, "sleep", lambda *_: None)
    attempts = {"n": 0}

    def fake_urlopen(request, timeout=None, context=None):
        attempts["n"] += 1
        if attempts["n"] == 1:
            body = json.dumps({"errors": [{"message": "RateLimiter"}]}).encode("utf-8")
            raise HTTPError(request.full_url, 429, "Too Many Requests", {}, io.BytesIO(body))
        payload = json.dumps({"itemSummaries": []}).encode("utf-8")
        return io.BytesIO(payload)

    class _CM:
        def __init__(self, resp):
            self._resp = resp
        def __enter__(self):
            return self._resp
        def __exit__(self, *a):
            return False

    def fake_urlopen_cm(request, timeout=None, context=None):
        return _CM(fake_urlopen(request, timeout=timeout, context=context))

    with patch.object(ebay_api_common, "urlopen", side_effect=fake_urlopen_cm):
        result = ebay_api_common.request_json_with_retry(
            f"{ebay_api_common.SEARCH_URL}?q=x",
            headers={"Authorization": "Bearer fake", "X-EBAY-C-MARKETPLACE-ID": "EBAY_US"},
            retry_count=3, initial_backoff_seconds=0.01,
        )

    assert result == {"itemSummaries": []}
    assert attempts["n"] == 2, "must retry exactly once after the 429 before succeeding"


# ── Output schema unchanged ────────────────────────────────────────────────────

EXPECTED_FLATTEN_ITEM_COLUMNS = [
    "collected_at_utc", "keyword", "source_country", "source_marketplace_id",
    "item_id", "legacy_item_id", "title", "price_value", "price_currency",
    "condition", "condition_id", "buying_options", "item_web_url", "image_url",
    "seller_username", "seller_feedback_score", "seller_feedback_percentage",
    "shipping_cost_value", "shipping_cost_currency", "item_location_country",
    "item_location_city", "category_ids", "category_names",
    "listing_marketplace_id", "item_creation_date",
]


def test_flatten_item_output_schema_unchanged():
    raw_item = {
        "itemId": "v1|123|0", "legacyItemId": "123", "title": "Rolex Cal 1030",
        "price": {"value": "199.99", "currency": "EUR"},
        "condition": "Used", "conditionId": "3000",
        "buyingOptions": ["FIXED_PRICE"],
        "itemWebUrl": "https://ebay.example/item/123",
        "image": {"imageUrl": "https://ebay.example/img/123.jpg"},
        "seller": {"username": "seller1", "feedbackScore": 500, "feedbackPercentage": "99.5"},
        "shippingOptions": [{"shippingCost": {"value": "5.00", "currency": "EUR"}}],
        "itemLocation": {"country": "DE", "city": "Berlin"},
        "categories": [{"categoryId": "1", "categoryName": "Parts"}, {"categoryId": "2", "categoryName": "Watches"}],
        "listingMarketplaceId": "EBAY_DE",
        "itemCreationDate": "2026-01-01T00:00:00.000Z",
        "source_country": "Germany", "source_marketplace_id": "EBAY_DE",
    }
    row = collector.flatten_item(raw_item, "rolex 1030", "2026-07-11T00:00:00")
    assert list(row.keys()) == EXPECTED_FLATTEN_ITEM_COLUMNS
    assert row["item_id"] == "v1|123|0"
    assert row["price_value"] == "199.99"
    assert row["category_names"] == "Parts > Watches"
    assert row["category_ids"] == "1 > 2"


def test_write_csv_and_inspect_csv_roundtrip(tmp_path):
    rows = [collector.flatten_item(
        {"itemId": "abc", "title": "T", "price": {"value": "1", "currency": "USD"},
         "source_country": "US", "source_marketplace_id": "EBAY_US"},
        "kw", "2026-07-11T00:00:00",
    )]
    out_path = tmp_path / "latest.csv"
    collector.write_csv(rows, out_path)

    with out_path.open() as fh:
        reader = csv.DictReader(fh)
        read_rows = list(reader)
    assert len(read_rows) == 1
    assert read_rows[0]["item_id"] == "abc"
    assert list(reader.fieldnames) == EXPECTED_FLATTEN_ITEM_COLUMNS

    import contextlib
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        collector.inspect_csv(path=out_path, sample_size=1)
    output = buf.getvalue()
    assert "=== ACTIVE COLUMNS ===" in output
    assert "item_id: abc" in output


def test_inspect_csv_missing_file_raises_system_exit(tmp_path):
    with pytest.raises(SystemExit):
        collector.inspect_csv(path=tmp_path / "does_not_exist.csv")
