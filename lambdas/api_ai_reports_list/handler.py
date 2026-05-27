"""
GET /api/ai-reports/list
========================
Paginated archive of AI Review reports for the active whitelisted
league.

Query params:
- `type` (optional) — when set, filters rows to a single report
  type (`postDraft` | `preseason` | `weekly`). When missing,
  returns all types interleaved newest-first.
- `limit` (optional, default 20, max 50) — page size.
- `cursor` (optional) — opaque pagination cursor returned from a
  previous call.

Auth:
- JWT-gated by the existing API Gateway authorizer. No admin
  requirement.

Response shape:
- 200 with `{"rows": [...], "next_cursor": "..." | null}`.
- 400 on an invalid `type`.
- 404 when no active whitelisted league is configured.
"""
from __future__ import annotations

from typing import Any

from lambdas.common.admin_gate import is_admin
from lambdas.common.ai_reports_store import REPORT_TYPES, list_recent
from lambdas.common.errors import handle_errors
from lambdas.common.logger import get_logger
from lambdas.common.supabase_helper import get_active_whitelisted_league
from lambdas.common.utility_helpers import success_response

log = get_logger(__file__)
HANDLER = "api_ai_reports_list"

_DEFAULT_LIMIT = 20
_MAX_LIMIT = 50


def _coerce_limit(raw: str | None) -> int:
    if raw is None:
        return _DEFAULT_LIMIT
    try:
        parsed = int(raw)
    except (TypeError, ValueError):
        return _DEFAULT_LIMIT
    return max(1, min(parsed, _MAX_LIMIT))


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    log.info("AI reports list request")

    qs = event.get("queryStringParameters") or {}
    report_type = (qs.get("type") or "").strip() or None
    cursor = qs.get("cursor") or None
    limit = _coerce_limit(qs.get("limit"))

    if report_type is not None and report_type not in REPORT_TYPES:
        return success_response(
            {
                "Success": False,
                "Message": (
                    f"Invalid type '{report_type}'. Must be one of {list(REPORT_TYPES)}."
                ),
            },
            status_code=400,
        )

    league = get_active_whitelisted_league()
    if not league:
        return success_response(
            {"Success": False, "Message": "No active whitelisted league configured"},
            status_code=404,
        )

    league_id = league.get("sleeper_league_id") or league.get("league_id")
    if not league_id:
        return success_response(
            {"Success": False, "Message": "Active league row is missing sleeper_league_id"},
            status_code=500,
        )

    # Admin-portal F3: non-admin callers never see redacted rows.
    # Resolved once per request and threaded through the post-query
    # filter below.
    caller_is_admin = is_admin(event)

    def _is_redacted(row: dict[str, Any]) -> bool:
        meta = row.get("metadata") or {}
        return str(meta.get("is_redacted", "")).lower() == "true"

    # When filtering by type we may need to walk a few pages to fill
    # one page of `limit` rows since Dynamo applies `Limit` before
    # the in-process filter. Cap the walk so a sparse type doesn't
    # turn into an unbounded scan.
    MAX_PAGES = 5
    collected: list[dict[str, Any]] = []
    next_cursor: str | None = cursor
    pages = 0

    while pages < MAX_PAGES:
        items, next_cursor = list_recent(
            league_id=league_id,
            limit=limit,
            cursor=next_cursor,
        )
        pages += 1

        for row in items:
            # Type filter (when set) AND redact filter (when caller
            # is not admin).
            if report_type is not None and (row.get("report_type") or "") != report_type:
                continue
            if not caller_is_admin and _is_redacted(row):
                continue
            collected.append(row)

        if len(collected) >= limit or next_cursor is None:
            break

    # Trim down to requested page size and surface the next cursor
    # only if we actually have more to read.
    if len(collected) > limit:
        # If we over-filled while filtering, drop overflow and keep
        # the underlying next_cursor as-is. The next call will resume
        # cleanly from the GSI position we left off at.
        collected = collected[:limit]

    return success_response({
        "rows": collected,
        "next_cursor": next_cursor,
        "filters": {
            "type": report_type,
            "limit": limit,
        },
    })
