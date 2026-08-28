"""
API — Sleeper claim audit
=========================
GET /admin/sleeper-claims

Every linked Sleeper account, grouped, with the platform users claiming it.

Linking is deliberately unverified: Sleeper has no OAuth, and the only field
that could prove ownership is the public `display_name`, which would mean
sending every new user to another app to rename themselves mid-signup. So any
account may claim any handle, and more than one may claim the same one.

That is a considered trade, not an oversight — everything Xomper shows from
Sleeper is already public, so a false claim reveals nothing a stranger could
not look up. It stops being acceptable when profiles become identities
(friends, comments), and this endpoint is how we watch for that day arriving.

Users are never told about a collision. Admins can see them here.
"""
from __future__ import annotations

from typing import Any

import boto3

from lambdas.common.caller_identity import get_caller
from lambdas.common.constants import PLATFORM_USERS_TABLE
from lambdas.common.errors import AuthorizationError, handle_errors
from lambdas.common.logger import get_logger
from lambdas.common.utility_helpers import success_response

HANDLER = "api_admin_sleeper_claims"
log = get_logger(HANDLER)


def _scan_all(table: Any) -> list[dict[str, Any]]:
    """Full scan, following pagination.

    A scan is right here: the endpoint reports on the whole table by design,
    and the table holds one small item per platform user. There is no
    partition key that would narrow it without inventing one purely to avoid
    scanning something we always want in full.
    """
    items: list[dict[str, Any]] = []
    kwargs: dict[str, Any] = {}
    while True:
        page = table.scan(**kwargs)
        items.extend(page.get("Items", []))
        last = page.get("LastEvaluatedKey")
        if not last:
            return items
        kwargs["ExclusiveStartKey"] = last


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    caller = get_caller(event)
    if not caller.is_admin:
        # Same message whether the caller is unknown or merely not an admin —
        # confirming an account exists is itself information.
        raise AuthorizationError("Not authorized")

    items = _scan_all(boto3.resource("dynamodb").Table(PLATFORM_USERS_TABLE))

    claims: dict[str, list[dict[str, str]]] = {}
    unlinked = 0

    for item in items:
        sleeper_user_id = str(item.get("sleeperUserId") or "")
        if not sleeper_user_id:
            unlinked += 1
            continue
        claims.setdefault(sleeper_user_id, []).append(
            {
                "userId": str(item.get("userId") or ""),
                "email": str(item.get("email") or ""),
                "linkedAt": str(item.get("updatedAt") or ""),
            }
        )

    grouped = [
        {
            "sleeperUserId": sleeper_user_id,
            "sleeperUsername": next(
                (
                    str(i.get("sleeperUsername") or "")
                    for i in items
                    if str(i.get("sleeperUserId") or "") == sleeper_user_id
                ),
                "",
            ),
            "claimCount": len(claimants),
            "isContested": len(claimants) > 1,
            "claimants": sorted(claimants, key=lambda c: c["linkedAt"]),
        }
        for sleeper_user_id, claimants in claims.items()
    ]

    # Contested first, then most-claimed, so the thing worth looking at is at
    # the top rather than sorted into the middle of a long list.
    grouped.sort(key=lambda g: (not g["isContested"], -g["claimCount"]))

    contested = sum(1 for g in grouped if g["isContested"])
    log.info(f"sleeper-claims: {len(grouped)} accounts, {contested} contested")

    return success_response(
        {
            "totalUsers": len(items),
            "unlinkedUsers": unlinked,
            "linkedAccounts": len(grouped),
            "contestedAccounts": contested,
            "accounts": grouped,
        }
    )
