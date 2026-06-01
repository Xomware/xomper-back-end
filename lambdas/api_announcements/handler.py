"""
GET /announcements/list
=======================
Public-read endpoint (JWT-gated by the API Gateway authorizer, but NOT
admin-gated) that returns the active+unexpired league announcements
shown on the iOS Landing page. Replaces the hardcoded
`LeagueAnnouncements.current` array shipped in Season Refocus F2.

Path note: the API Gateway module requires 2 path segments, so the
public read path is flattened from `/announcements` to
`/announcements/list`.

Response:
{
    "Success": true,
    "count":   N,
    "rows":    [ ... league_announcements rows ... ]
}

Module-level 5-minute cache:
    Reduces Supabase load + improves cold-start latency for the
    once-per-launch Landing fetch. Cache is per-lambda-container, so
    AWS's natural container churn keeps it from getting too stale.
    Admin writes don't bust the cache — iOS sees up to 5 min of lag
    after a commissioner edit, which is the documented trade-off.

GRACEFUL FALLBACK:
    `announcements_store.list_active` is best-effort and returns `[]` on
    any Supabase failure. The handler still returns 200 + empty rows in
    that case; the iOS store transparently falls back to the hardcoded
    `LeagueAnnouncements.current` array when it sees an empty payload
    OR a network error. Either way the Landing page never blanks.
"""
from __future__ import annotations

import time
from typing import Any

from lambdas.common.announcements_store import list_active
from lambdas.common.errors import handle_errors
from lambdas.common.logger import get_logger
from lambdas.common.utility_helpers import success_response

log = get_logger(__file__)
HANDLER = "api_announcements"

# 5-minute TTL for the in-container cache. Tuned to match the iOS
# AnnouncementsStore freshness gate so the two layers don't fight.
_CACHE_TTL_SECONDS = 300

# Module-level cache. `_cache_at` is the monotonic timestamp of the last
# successful fetch; `_cache_rows` is the most-recently-served payload.
# Per-lambda-container, NOT shared across cold starts.
_cache_at: float = 0.0
_cache_rows: list[dict[str, Any]] = []


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    log.info("Public announcements/list request")

    rows = _get_rows_cached()

    return success_response(
        {
            "Success": True,
            "count": len(rows),
            "rows": rows,
        }
    )


def _get_rows_cached() -> list[dict[str, Any]]:
    """Return cached rows if the cache is fresh; otherwise refresh from
    Supabase via `list_active` (best-effort). Cache update is atomic
    (single assignment to two module globals — no concurrency concerns
    because Lambda containers handle one request at a time)."""
    global _cache_at, _cache_rows

    now = time.monotonic()
    age = now - _cache_at
    if _cache_rows and age < _CACHE_TTL_SECONDS:
        log.info(
            f"announcements cache HIT age={age:.1f}s "
            f"count={len(_cache_rows)}"
        )
        return _cache_rows

    fresh = list_active()
    _cache_rows = fresh
    _cache_at = now
    log.info(
        f"announcements cache MISS — refreshed count={len(fresh)} "
        f"ttl={_CACHE_TTL_SECONDS}s"
    )
    return fresh


def _reset_cache_for_tests() -> None:
    """Test-only helper to reset the module-level cache between cases.
    Not exposed publicly — tests reach into the module via name."""
    global _cache_at, _cache_rows
    _cache_at = 0.0
    _cache_rows = []
