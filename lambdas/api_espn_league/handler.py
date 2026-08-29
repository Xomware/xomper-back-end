"""
API — ESPN league read proxy
============================
GET    /espn/league      -> proxy one ESPN league read
PUT    /espn/connect     -> store the caller's espn_s2 + SWID
DELETE /espn/disconnect  -> revoke them

Flat paths with a part per method: api-gateway-service keys one API Gateway
resource on `path_part`, so PUT and DELETE on a shared "credentials" part
collide at apply time. Same reason /me exposes sleeper-link and sleeper-unlink
rather than two methods on one part.

Why a proxy exists at all: a public ESPN league is readable straight from the
browser, but a private one needs the member's `espn_s2` and `SWID` cookies, and
browsers will not send those cross-site. So the read has to happen server-side
with cookies this user has handed over.

That makes this endpoint an authenticated request forwarder holding a full ESPN
session, which is exactly the shape that turns into SSRF if it takes a URL. It
does not. The host is fixed, the path is built from a template, `leagueId` and
`season` must be digits, and `view` is checked against an allowlist. A caller
cannot reach anything but an ESPN fantasy league read.
"""
from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any

from lambdas.common.caller_identity import get_caller
from lambdas.common.errors import ValidationError, handle_errors
from lambdas.common.espn_credentials import clear_espn, get_espn, store_espn
from lambdas.common.logger import get_logger
from lambdas.common.utility_helpers import parse_body, success_response

HANDLER = "api_espn_league"
log = get_logger(HANDLER)

ESPN_HOST = "https://lm-api-reads.fantasy.espn.com"
LEAGUE_PATH = "/apis/v3/games/ffl/seasons/{season}/segments/0/leagues/{league_id}"
USER_AGENT = "xomper-espn-proxy/1.0"

# Only the views the app reads. An open `view` parameter would let a caller aim
# this proxy, with someone's ESPN session attached, at anything ESPN serves.
ALLOWED_VIEWS = frozenset({
    "mDraftDetail",
    "mSettings",
    "mTeam",
    "mRoster",
    "kona_player_info",
})

TIMEOUT_SECONDS = 20


def _require_digits(value: Any, field: str) -> str:
    text = str(value or "").strip()
    if not text.isdigit():
        raise ValidationError(f"{field} must be numeric")
    return text


def _views(raw: Any) -> list[str]:
    if isinstance(raw, str):
        requested = [v for v in raw.split(",") if v]
    elif isinstance(raw, list):
        requested = [str(v) for v in raw]
    else:
        requested = []

    if not requested:
        raise ValidationError("view is required")

    rejected = [v for v in requested if v not in ALLOWED_VIEWS]
    if rejected:
        raise ValidationError(f"unsupported view(s): {', '.join(sorted(rejected))}")
    return requested


def _fetch(league_id: str, season: str, views: list[str], cookies: dict[str, str] | None) -> Any:
    url = ESPN_HOST + LEAGUE_PATH.format(season=season, league_id=league_id)
    url += "?" + "&".join(f"view={v}" for v in views)

    headers = {"User-Agent": USER_AGENT}
    if cookies:
        headers["Cookie"] = "; ".join(f"{k}={v}" for k, v in cookies.items())

    request = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
        return json.load(response)


def _read_league(event: dict[str, Any]) -> dict[str, Any]:
    caller = get_caller(event)
    params = event.get("queryStringParameters") or {}

    league_id = _require_digits(params.get("leagueId"), "leagueId")
    season = _require_digits(params.get("season"), "season")
    views = _views(params.get("view"))

    cookies = get_espn(caller.user_id)

    try:
        data = _fetch(league_id, season, views, cookies)
    except urllib.error.HTTPError as err:
        # 401 here means the league is private and this user either has no
        # cookies stored or they have expired. That is actionable by the user,
        # so it is worth distinguishing from a generic upstream failure.
        if err.code in (401, 403):
            return success_response(
                {
                    "error": "espn_auth_required",
                    "hasStoredCredentials": cookies is not None,
                },
                status_code=403,
            )
        if err.code == 404:
            return success_response({"error": "league not found"}, status_code=404)
        raise

    return success_response({"leagueId": league_id, "season": season, "data": data})


def _put_credentials(event: dict[str, Any]) -> dict[str, Any]:
    caller = get_caller(event)
    body = parse_body(event) or {}

    espn_s2 = str(body.get("espn_s2") or "").strip()
    swid = str(body.get("swid") or body.get("SWID") or "").strip()
    if not espn_s2 or not swid:
        raise ValidationError("espn_s2 and swid are both required")

    store_espn(caller.user_id, espn_s2, swid)
    return success_response({"connected": True})


def _delete_credentials(event: dict[str, Any]) -> dict[str, Any]:
    caller = get_caller(event)
    clear_espn(caller.user_id)
    return success_response({"connected": False})


_ROUTES = {
    ("league", "GET"): _read_league,
    ("connect", "PUT"): _put_credentials,
    ("disconnect", "DELETE"): _delete_credentials,
}


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    path = (event.get("path") or "").rstrip("/").rsplit("/", 1)[-1]
    method = (event.get("httpMethod") or "").upper()

    route = _ROUTES.get((path, method))
    if not route:
        raise ValidationError(f"unsupported route {method} {path}")
    return route(event)
