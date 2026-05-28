"""
GET /admin/logs-query
=====================
Admin-only CloudWatch log tail (admin-portal F5). Wraps
``boto3.client('logs').filter_log_events(...)`` against a hard-coded
allowlist of 10 lambda log groups (every admin / AI-review / notif
lambda touched by the admin-portal epic), redacts PII server-side
before returning, caches identical first-page queries for 60 seconds
to dampen rapid polling, and paginates via ``next_token``.

Query params:
- ``log_group``   (required)  — slug key from ``ADMIN_LOG_GROUP_ALLOWLIST``.
                                 Validated against the allowlist; unknown
                                 slugs yield a 400. The lambda's IAM
                                 role also enforces this (only the 10
                                 allowlisted ARNs are reachable), so
                                 this validation is defense in depth.
- ``level``       (optional)  — one of ``info`` / ``warn`` / ``error``.
                                 Applied as a post-fetch filter on the
                                 heuristically-derived per-event level.
- ``search``      (optional)  — literal substring; passed through to
                                 CloudWatch ``filterPattern`` as a
                                 quoted string. No regex.
- ``limit``       (optional)  — 1-200, default 50. Out-of-range values
                                 are clamped to the nearest bound;
                                 unparseable values fall back to 50.
- ``next_token``  (optional)  — opaque CloudWatch cursor. When present,
                                 the module-level cache is bypassed
                                 (paginated calls always re-fetch).

Response:
    {
      "Success":    true,
      "log_group":  "ai-review-weekly",
      "events": [
        {
          "id":        "<eventId>",
          "timestamp": "<ISO-8601 UTC>",
          "level":     "ERROR" | "WARN" | "INFO" | null,
          "message":   "<redacted>"
        },
        ...
      ],
      "next_token": "<cursor>" | null
    }

Errors:
- 403 — caller is not admin.
- 400 — ``log_group`` missing or not in the allowlist.

Cache:
- Module-level dict, 60s TTL, keyed by
  ``(log_group, level, search, limit)``. ``next_token`` is excluded
  from the key so paginating doesn't blow up the cache. Cold starts
  naturally evict.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Optional

import boto3

from lambdas.common.admin_gate import NotAdmin, require_admin
from lambdas.common.constants import ADMIN_LOG_GROUP_ALLOWLIST, AWS_DEFAULT_REGION
from lambdas.common.errors import handle_errors
from lambdas.common.log_redact import redact
from lambdas.common.logger import get_logger
from lambdas.common.utility_helpers import get_query_params, success_response

log = get_logger(__file__)
HANDLER = "api_admin_logs_query"

DEFAULT_LIMIT = 50
MAX_LIMIT = 200
MIN_LIMIT = 1
CACHE_TTL_SECONDS = 60

ALLOWED_LEVELS = {"info", "warn", "error"}

# Module-level cache. Keyed by ``(log_group, level, search, limit)``
# (next_token deliberately excluded). Value is ``(inserted_at, body)``.
# Cleared on cold start; per-warm-container only.
_CACHE: dict[tuple[str, Optional[str], Optional[str], int], tuple[float, dict[str, Any]]] = {}

# Lazily-initialised CloudWatch Logs client. Module-level so warm
# containers reuse the connection pool.
_LOGS_CLIENT: Any = None


def _logs_client() -> Any:
    """Lazy boto3 client init so unit tests can monkeypatch the
    module attribute before the first handler call."""
    global _LOGS_CLIENT
    if _LOGS_CLIENT is None:
        _LOGS_CLIENT = boto3.client("logs", region_name=AWS_DEFAULT_REGION)
    return _LOGS_CLIENT


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    log.info("Admin logs-query request")

    try:
        require_admin(event)
    except NotAdmin:
        return success_response(
            {"Success": False, "Message": "Not authorized"},
            status_code=403,
        )

    qs = get_query_params(event)

    log_group_slug = (qs.get("log_group") or "").strip()
    if not log_group_slug:
        return success_response(
            {
                "Success": False,
                "Message": "log_group is required",
                "allowed": sorted(ADMIN_LOG_GROUP_ALLOWLIST.keys()),
            },
            status_code=400,
        )
    if log_group_slug not in ADMIN_LOG_GROUP_ALLOWLIST:
        log.warning(
            f"logs-query: rejecting non-allowlisted log_group='{log_group_slug}'"
        )
        return success_response(
            {
                "Success": False,
                "Message": (
                    f"log_group '{log_group_slug}' is not allowlisted. "
                    "Defense in depth: IAM also denies."
                ),
                "allowed": sorted(ADMIN_LOG_GROUP_ALLOWLIST.keys()),
            },
            status_code=400,
        )

    level = _parse_level(qs.get("level"))
    search = (qs.get("search") or "").strip() or None
    limit = _parse_limit(qs.get("limit"))
    next_token = (qs.get("next_token") or "").strip() or None

    # Cache check. Pagination (``next_token`` set) bypasses entirely
    # so "Load older" never returns the cached first page.
    cache_key: tuple[str, Optional[str], Optional[str], int] = (
        log_group_slug,
        level,
        search,
        limit,
    )
    if next_token is None:
        cached = _cache_get(cache_key)
        if cached is not None:
            log.info(f"logs-query: cache hit for {cache_key}")
            return success_response(cached)

    log_group_name = ADMIN_LOG_GROUP_ALLOWLIST[log_group_slug]
    boto_kwargs: dict[str, Any] = {
        "logGroupName": log_group_name,
        "limit": limit,
    }
    filter_pattern = _build_filter_pattern(search)
    if filter_pattern:
        boto_kwargs["filterPattern"] = filter_pattern
    if next_token is not None:
        boto_kwargs["nextToken"] = next_token

    log.info(
        f"logs-query: calling filter_log_events log_group='{log_group_slug}' "
        f"limit={limit} level={level} has_search={search is not None} "
        f"has_token={next_token is not None}"
    )

    response = _logs_client().filter_log_events(**boto_kwargs)
    raw_events = response.get("events") or []
    cw_next_token = response.get("nextToken")

    events: list[dict[str, Any]] = []
    for raw in raw_events:
        ev = _format_event(raw)
        # Post-fetch level filter — CloudWatch's filterPattern can't
        # see our heuristic, so we apply the cut here.
        if level is not None and (ev["level"] or "").lower() != level:
            continue
        events.append(ev)

    body = {
        "Success": True,
        "log_group": log_group_slug,
        "events": events,
        "next_token": cw_next_token,
    }

    # Only cache first-page results. Paginated calls (next_token set)
    # are inherently transient cursors — caching them would just bloat
    # the dict for no real hit rate.
    if next_token is None:
        _cache_put(cache_key, body)

    return success_response(body)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_level(raw: Any) -> Optional[str]:
    """Normalise level to one of ``info`` / ``warn`` / ``error``.
    Unknown / empty values return None (no filter applied)."""
    if not raw:
        return None
    value = str(raw).strip().lower()
    if value == "warning":
        value = "warn"
    if value not in ALLOWED_LEVELS:
        return None
    return value


def _parse_limit(raw: Any) -> int:
    """Parse the ``limit`` query param. Defaults to ``DEFAULT_LIMIT``;
    clamps to ``[MIN_LIMIT, MAX_LIMIT]``. Unparseable falls back to
    the default."""
    if raw is None or raw == "":
        return DEFAULT_LIMIT
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return DEFAULT_LIMIT
    if value < MIN_LIMIT:
        return MIN_LIMIT
    if value > MAX_LIMIT:
        return MAX_LIMIT
    return value


def _build_filter_pattern(search: Optional[str]) -> Optional[str]:
    """Build a CloudWatch ``filterPattern`` from the search string.

    CloudWatch quoted-string filters do literal substring matching,
    which is what F5 ships (regex was rejected as out-of-scope). We
    return None when there's nothing to filter on so boto3 omits the
    parameter and CloudWatch returns the raw tail.
    """
    if not search:
        return None
    # Escape embedded double quotes so the search string can't break
    # out of the quoted filter pattern.
    sanitized = search.replace('"', '\\"')
    return f'"{sanitized}"'


def _detect_level(message: str) -> Optional[str]:
    """Best-effort substring scan over the event message.

    Looks for ``ERROR`` / ``WARN`` / ``WARNING`` / ``INFO`` markers in
    typical Python / xomper log formats. Returns the uppercase level
    string (so callers can lowercase if needed) or None when no
    marker is present.

    Documented as best-effort — the heuristic only annotates rows for
    the UI chip; it does NOT gate what's returned (the optional
    ``level`` query param applies the cut on top of this).
    """
    if not isinstance(message, str) or not message:
        return None
    # Order matters: WARNING contains WARN as a substring; ERROR is
    # checked first so a stray "WARNING about ERROR" still resolves
    # to ERROR (the more severe).
    if "ERROR" in message:
        return "ERROR"
    if "WARNING" in message or "WARN" in message:
        return "WARN"
    if "INFO" in message:
        return "INFO"
    return None


def _format_event(raw: dict[str, Any]) -> dict[str, Any]:
    """Project a CloudWatch ``filter_log_events`` event onto the wire
    shape. Applies PII redaction to ``message`` and ISO-8601 formats
    the millisecond-epoch timestamp."""
    message = raw.get("message") or ""
    redacted = redact(message)
    level = _detect_level(message)

    ts_ms = raw.get("timestamp")
    if isinstance(ts_ms, (int, float)) and ts_ms > 0:
        try:
            iso = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()
        except (OverflowError, OSError, ValueError):
            iso = None
    else:
        iso = None

    return {
        "id": raw.get("eventId"),
        "timestamp": iso,
        "level": level,
        "message": redacted,
    }


def _cache_get(
    key: tuple[str, Optional[str], Optional[str], int],
) -> Optional[dict[str, Any]]:
    """Return the cached body for ``key`` if still fresh, else None.
    Stale entries are evicted on read to keep the dict bounded."""
    entry = _CACHE.get(key)
    if entry is None:
        return None
    inserted_at, body = entry
    if (time.time() - inserted_at) > CACHE_TTL_SECONDS:
        _CACHE.pop(key, None)
        return None
    return body


def _cache_put(
    key: tuple[str, Optional[str], Optional[str], int],
    body: dict[str, Any],
) -> None:
    """Stamp ``body`` into the cache for ``key`` at the current
    wall-clock time."""
    _CACHE[key] = (time.time(), body)


def _cache_clear() -> None:
    """Test hook — clears the module-level cache. Tests that exercise
    cache hit/miss must call this in setup to isolate runs."""
    _CACHE.clear()
