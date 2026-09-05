"""
ebay_api_common.py
==================
Shared eBay Browse API primitives: OAuth token caching, HTTP request handling
with retry/backoff, single-marketplace bounded pagination, and secret
redaction. Used by both ebay_watch_parts_collector.py (broad collection) and
scripts/04_collect_targeted_active.py (targeted collection) so OAuth,
pagination, and retry logic exist in exactly one place.

eBay Browse API documented limits (verified, not assumed):
  - limit: 1-200 (per-page result count)
  - offset: 0-10,000 (maximum items reachable via pagination)
  - q (search keyword string): commonly documented as <=350 characters total,
    <=99 characters per whitespace-separated word. The formal OpenAPI schema
    does not enumerate a hard maxLength for `q`; this figure comes from
    eBay's own developer forum guidance, not the machine-readable spec. If
    eBay tightens or clarifies this later, update EBAY_QUERY_MAX_LENGTH /
    EBAY_QUERY_MAX_WORD_LENGTH below — do not silently assume a new number.
"""

from __future__ import annotations

import base64
import json
import os
import random
import re
import ssl
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

TOKEN_URL = "https://api.ebay.com/identity/v1/oauth2/token"
SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"
RATE_LIMITS_URL = "https://api.ebay.com/developer/analytics/v1_beta/rate_limit/"
DEFAULT_SCOPE = "https://api.ebay.com/oauth/api_scope"
BASE_DIR = Path(__file__).parent
TOKEN_CACHE = BASE_DIR / ".ebay_token_cache.json"

MARKETPLACES = {
    "US": "EBAY_US",
    "UK": "EBAY_GB",
    "France": "EBAY_FR",
    "Italy": "EBAY_IT",
    "Germany": "EBAY_DE",
    "Switzerland": "EBAY_CH",
    "Hong Kong": "EBAY_HK",
}
MARKETPLACE_ID_TO_COUNTRY = {v: k for k, v in MARKETPLACES.items()}

EBAY_LIMIT_MIN = 1
EBAY_LIMIT_MAX = 200
EBAY_OFFSET_MAX = 10_000
EBAY_QUERY_MAX_LENGTH = 350
EBAY_QUERY_MAX_WORD_LENGTH = 99

_SECRET_PATTERNS = [
    re.compile(r"Bearer\s+[A-Za-z0-9\-._~+/]+=*", re.IGNORECASE),
    re.compile(r"Basic\s+[A-Za-z0-9+/]+=*", re.IGNORECASE),
    re.compile(r'"access_token"\s*:\s*"[^"]*"'),
    re.compile(r'"client_secret"\s*:\s*"[^"]*"'),
]


def redact_sensitive(text: str) -> str:
    """Scrub anything resembling an OAuth token, Basic auth header, or
    secret from a string before it is ever logged or printed."""
    if not text:
        return text
    redacted = text
    for pattern in _SECRET_PATTERNS:
        redacted = pattern.sub("[REDACTED]", redacted)
    return redacted


def ssl_context() -> ssl.SSLContext:
    ca_file = Path(os.environ.get("SSL_CERT_FILE", "/etc/ssl/cert.pem"))
    if ca_file.exists():
        return ssl.create_default_context(cafile=str(ca_file))
    return ssl.create_default_context()


def load_dotenv(path: Path = BASE_DIR / ".env") -> None:
    """Load KEY=VALUE pairs from .env without requiring python-dotenv."""
    if not path.exists():
        return

    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def request_json(url: str, *, headers: dict[str, str], data: bytes | None = None) -> dict[str, Any]:
    request = Request(url, data=data, headers=headers, method="POST" if data else "GET")
    try:
        with urlopen(request, timeout=45, context=ssl_context()) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"eBay request failed with HTTP {exc.code}: {redact_sensitive(body)}") from exc
    except URLError as exc:
        raise RuntimeError(
            "Could not connect to eBay. Check your internet connection, VPN/proxy, "
            "and that the client is allowed to access the network."
        ) from exc


class ThrottledError(RuntimeError):
    """Raised when eBay signals rate limiting (HTTP 429) — always retriable."""


def _is_throttling_error(exc: Exception) -> bool:
    message = str(exc)
    return "HTTP 429" in message or "Too Many Requests" in message or "RateLimiter" in message


def request_json_with_retry(
    url: str,
    *,
    headers: dict[str, str],
    data: bytes | None = None,
    retry_count: int = 3,
    initial_backoff_seconds: float = 1.0,
    backoff_multiplier: float = 2.0,
    on_retry: Callable[[int, float, str], None] | None = None,
) -> dict[str, Any]:
    """request_json wrapped with exponential backoff + jitter. Retries on
    throttling (HTTP 429) and transient 5xx/connection errors; does not
    retry on other 4xx errors (those are permanent — a malformed query
    won't succeed on retry)."""
    attempt = 0
    backoff = initial_backoff_seconds
    last_exc: Exception | None = None

    while attempt <= retry_count:
        try:
            return request_json(url, headers=headers, data=data)
        except RuntimeError as exc:
            last_exc = exc
            message = str(exc)
            is_5xx = any(f"HTTP {code}" in message for code in (500, 502, 503, 504))
            is_throttled = _is_throttling_error(exc)
            is_connection_error = "Could not connect to eBay" in message
            retriable = is_throttled or is_5xx or is_connection_error
            if not retriable or attempt == retry_count:
                raise
            sleep_seconds = backoff + random.uniform(0, backoff)
            if on_retry:
                on_retry(attempt + 1, sleep_seconds, redact_sensitive(message))
            time.sleep(sleep_seconds)
            backoff *= backoff_multiplier
            attempt += 1

    assert last_exc is not None
    raise last_exc


def get_access_token(scope: str) -> str:
    client_id = os.environ.get("EBAY_CLIENT_ID")
    client_secret = os.environ.get("EBAY_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise SystemExit(
            "Missing EBAY_CLIENT_ID or EBAY_CLIENT_SECRET. Create a .env file from .env.example."
        )

    cache_key = f"production:{client_id}:{scope}"
    now = int(time.time())
    if TOKEN_CACHE.exists():
        cache = json.loads(TOKEN_CACHE.read_text())
        cached = cache.get(cache_key)
        if cached and cached.get("expires_at", 0) > now + 300:
            return cached["access_token"]
    else:
        cache = {}

    credentials = f"{client_id}:{client_secret}".encode("utf-8")
    basic_token = base64.b64encode(credentials).decode("ascii")
    form = urlencode({"grant_type": "client_credentials", "scope": scope}).encode("utf-8")
    token_response = request_json_with_retry(
        TOKEN_URL,
        headers={
            "Authorization": f"Basic {basic_token}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data=form,
    )

    access_token = token_response["access_token"]
    expires_in = int(token_response.get("expires_in", 7200))
    cache[cache_key] = {
        "access_token": access_token,
        "expires_at": now + expires_in,
        "expires_at_utc": datetime.fromtimestamp(now + expires_in, timezone.utc).isoformat(),
    }
    TOKEN_CACHE.write_text(json.dumps(cache, indent=2))
    return access_token


def first_value(item: dict[str, Any], path: list[str], default: str = "") -> str:
    current: Any = item
    for key in path:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return "" if current is None else str(current)


def validate_query_length(query_text: str) -> tuple[bool, str]:
    """Check a search string against eBay's documented practical limits
    for the `q` parameter. Returns (is_valid, reason_if_invalid)."""
    if len(query_text) > EBAY_QUERY_MAX_LENGTH:
        return False, f"query is {len(query_text)} chars, exceeds documented limit of {EBAY_QUERY_MAX_LENGTH}"
    for word in query_text.split():
        if len(word) > EBAY_QUERY_MAX_WORD_LENGTH:
            return False, f"word {word!r} is {len(word)} chars, exceeds documented per-word limit of {EBAY_QUERY_MAX_WORD_LENGTH}"
    return True, ""


def search_items_single_marketplace(
    *,
    token: str,
    marketplace_id: str,
    country_name: str,
    keyword: str,
    limit: int,
    max_items: int | None,
    sort: str,
    filters: list[str],
    max_pages: int | None = None,
    on_call: Callable[[], None] | None = None,
    retry_count: int = 3,
    initial_backoff_seconds: float = 1.0,
    backoff_multiplier: float = 2.0,
    on_retry: Callable[[int, float, str], None] | None = None,
) -> list[dict[str, Any]]:
    """Bounded pagination against a single eBay marketplace. max_pages caps
    how many pages are fetched even if eBay keeps returning a 'next' link
    and offset stays under EBAY_OFFSET_MAX — never unbounded traversal."""
    items: list[dict[str, Any]] = []
    offset = 0
    pages_fetched = 0

    while True:
        if max_pages is not None and pages_fetched >= max_pages:
            break
        if offset >= EBAY_OFFSET_MAX:
            break

        page_limit = min(EBAY_LIMIT_MAX, max(EBAY_LIMIT_MIN, limit))
        if max_items is not None:
            remaining = max_items - len(items)
            if remaining <= 0:
                break
            page_limit = min(page_limit, remaining)

        params = {
            "q": keyword,
            "limit": str(page_limit),
            "offset": str(offset),
            "fieldgroups": "EXTENDED",
            "sort": sort,
        }
        if filters:
            params["filter"] = ",".join(filters)

        url = f"{SEARCH_URL}?{urlencode(params)}"
        if on_call:
            on_call()
        response = request_json_with_retry(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "X-EBAY-C-MARKETPLACE-ID": marketplace_id,
                "Accept": "application/json",
            },
            retry_count=retry_count,
            initial_backoff_seconds=initial_backoff_seconds,
            backoff_multiplier=backoff_multiplier,
            on_retry=on_retry,
        )
        pages_fetched += 1

        page_items = response.get("itemSummaries") or []
        for item in page_items:
            item["source_marketplace_id"] = marketplace_id
            item["source_country"] = country_name

        items.extend(page_items)

        if not page_items or "next" not in response:
            break
        offset += page_limit

    return items


def search_items(
    *,
    token: str,
    keyword: str,
    limit: int,
    max_items: int | None,
    sort: str,
    filters: list[str],
) -> list[dict[str, Any]]:
    """Broad, all-marketplaces search — unchanged behavior for
    ebay_watch_parts_collector.py. Delegates per-marketplace pagination to
    search_items_single_marketplace (max_pages=None preserves the original
    unbounded-until-exhausted traversal this function always had)."""
    items: list[dict[str, Any]] = []

    for country, marketplace_id in MARKETPLACES.items():
        remaining = None if max_items is None else max_items - len(items)
        if remaining is not None and remaining <= 0:
            break
        page_items = search_items_single_marketplace(
            token=token,
            marketplace_id=marketplace_id,
            country_name=country,
            keyword=keyword,
            limit=limit,
            max_items=remaining,
            sort=sort,
            filters=filters,
        )
        items.extend(page_items)

        if max_items is not None and len(items) >= max_items:
            break

    return items


def get_rate_limits(token: str, api_name: str = "Browse") -> dict[str, Any]:
    """
    Read-only call to eBay's Developer Analytics getRateLimits endpoint,
    scoped to the Browse API by default. Uses the same client-credentials
    token/scope already used for search — no additional grant needed.
    Raises RuntimeError (via request_json) with eBay's exact error body if
    the current credentials/access don't permit this call; callers should
    surface that error rather than guessing a quota number.
    """
    url = f"{RATE_LIMITS_URL}?api_context=buy&api_name={api_name}"
    return request_json_with_retry(
        url,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        retry_count=1,
    )


def extract_rate_limit_headers(headers: dict[str, str]) -> dict[str, str]:
    """
    Preserve non-secret rate-limit-related response headers if the Browse
    API returns any (it does not document any as of this writing, but this
    is defensive: never assume, always check what's actually there).
    Authorization/token headers are never included by construction — this
    only looks for header names containing 'rate' or 'quota' or 'limit'.
    """
    return {
        key: value
        for key, value in headers.items()
        if any(token in key.lower() for token in ("rate", "quota", "limit", "remaining"))
    }
