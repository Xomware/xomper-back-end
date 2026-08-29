"""
API — Compute league values
===========================
POST /values/compute

Body, one of:
  { "leagueId": "<sleeper league id>" }             Sleeper, settings resolved
  { "scoring", "rosterPositions", "numTeams", ... } settings supplied directly
  { "espnLeagueId", "season", ... }                 ESPN, pre-scored upstream

Returns every projected player valued under THAT league's scoring and roster
shape, read from the nightly projections Parquet in the warehouse.

There is no stored values grid to look up. Values are a function of
scoring_settings and roster_positions, both arbitrary, so a grid could never
enumerate them. Computing one league measured ~36 ms once the Parquet is
resident, which is cheaper than maintaining a cross product would be.

Parity: the values this returns are identical to the TypeScript engine the
frontend ships today — all 3,227 scored players match on points and value,
flex allocation included. That was verified against
xomper-frontend/src/app/models/{projections,vor}.model.ts compiled and run on
the same inputs, not against a second implementation of the same idea.
"""
from typing import Any

import duckdb

from lambdas.common.constants import WAREHOUSE_BUCKET_NAME
from urllib.error import HTTPError

import boto3

from lambdas.common.constants import PLAYERS_TABLE_NAME
from lambdas.common.errors import handle_errors
from lambdas.common.espn_credentials import get_espn
from lambdas.common.espn_crosswalk import crosswalk_from_players_table
from lambdas.common.espn_projections import (
    fetch_league_players,
    fetch_league_settings,
    scored_rows,
)
from lambdas.common.logger import get_logger
from lambdas.common.sleeper_helper import get_nfl_state, get_sleeper_league
from lambdas.common.utility_helpers import parse_body, success_response
from lambdas.common.warehouse_values import (
    load_scored,
    score_players,
    starters_by_position,
    values_for,
)

HANDLER = "api_values_compute"
log = get_logger(HANDLER)

USER_AGENT = "xomper-values/1.0"

# The only writable path in a Lambda execution environment. Without a home
# directory DuckDB fails on connect before running a single query.
EPHEMERAL_DIR = "/tmp"


def _parquet_uri(season: str) -> str:
    return (
        f"s3://{WAREHOUSE_BUCKET_NAME}"
        f"/projections/current/season={season}/stats.parquet"
    )


def _connect() -> duckdb.DuckDBPyConnection:
    # custom_user_agent is connect-time only; SET after the database is running
    # raises. Same for the directory settings.
    con = duckdb.connect(config={
        "custom_user_agent": USER_AGENT,
        "home_directory": EPHEMERAL_DIR,
        "extension_directory": f"{EPHEMERAL_DIR}/duckdb_extensions",
    })
    con.execute("INSTALL httpfs; LOAD httpfs;")
    # Picks up the Lambda execution role — no keys in config.
    con.execute("CREATE SECRET (TYPE S3, PROVIDER CREDENTIAL_CHAIN);")
    return con


def resolve_sleeper_settings(league_id: str) -> dict[str, Any] | None:
    """The value engine's inputs, read off a Sleeper league. None if unknown.

    Split out from the handler because `warehouse_values` never needed Sleeper —
    only this resolution step does. A second resolver can feed the same shape
    from somewhere else without touching the computation.
    """
    league = get_sleeper_league(league_id)
    if not league:
        return None

    scoring = league.get("scoring_settings") or {}
    return {
        "scoring": scoring,
        "rosterPositions": league.get("roster_positions") or [],
        "numTeams": league.get("total_rosters") or 12,
        "ppr": scoring.get("rec", 0) or 0,
        # A league object carries the season it belongs to, which is what its
        # projections should be read from — not necessarily the current one.
        "season": str(league.get("season") or get_nfl_state().get("season") or ""),
    }


# Everything the value engine needs when the caller is not a Sleeper league.
REQUIRED_SETTINGS = ("scoring", "rosterPositions", "numTeams", "season")


def explicit_settings(body: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    """Settings handed over directly instead of read from a Sleeper league.

    Returns (settings, error). This is how an ESPN league reaches the same
    engine: its scoring is translated to Sleeper stat keys upstream, so by the
    time it arrives here nothing platform-specific is left.
    """
    missing = [k for k in REQUIRED_SETTINGS if body.get(k) is None]
    if missing:
        return None, f"missing required field(s): {', '.join(missing)}"

    scoring = body["scoring"]
    roster_positions = body["rosterPositions"]
    if not isinstance(scoring, dict):
        return None, "scoring must be an object"
    if not isinstance(roster_positions, list):
        return None, "rosterPositions must be an array"
    try:
        num_teams = int(body["numTeams"])
    except (TypeError, ValueError):
        return None, "numTeams must be a number"
    if num_teams < 1:
        return None, "numTeams must be at least 1"

    return {
        "scoring": scoring,
        "rosterPositions": roster_positions,
        "numTeams": num_teams,
        # Derived the same way as the Sleeper path, so the two cannot drift.
        "ppr": scoring.get("rec", 0) or 0,
        "season": str(body["season"]),
    }, None


# ESPN roster slot ids -> the position names warehouse_values expects.
ESPN_SLOTS = {0: "QB", 2: "RB", 4: "WR", 6: "TE", 16: "DEF", 17: "K", 23: "FLEX"}


def _caller_espn_cookies(event: dict[str, Any]) -> dict[str, str] | None:
    """The caller's ESPN cookies, or None.

    Not an auth check. `/values/compute` serves unauthenticated callers for
    Sleeper and explicit settings, so a missing authorizer context here is an
    ordinary case meaning "public ESPN leagues only", not a failure.

    The cookies are read, used for one upstream call and discarded. They are
    never logged and never reach the response — see espn_credentials.
    """
    context = (event.get("requestContext") or {}).get("authorizer") or {}
    user_id = context.get("sub")
    if not user_id:
        return None
    return get_espn(str(user_id))


def _stored_crosswalk() -> dict[str, dict[str, str]]:
    """The ingest's espn_id map, read straight off the players table."""
    table = boto3.resource("dynamodb").Table(PLAYERS_TABLE_NAME)
    players: dict[str, dict[str, Any]] = {}
    kwargs: dict[str, Any] = {}
    while True:
        page = table.scan(**kwargs)
        for item in page.get("Items", []):
            players[str(item.get("playerId"))] = item
        key = page.get("LastEvaluatedKey")
        if not key:
            break
        kwargs["ExclusiveStartKey"] = key
    return crosswalk_from_players_table(players)


def resolve_espn_settings(
    league_id: str,
    season: str,
    cookies: dict[str, str] | None = None,
) -> dict[str, Any] | None:
    """Roster shape and team count from ESPN's own settings.

    No scoring: an ESPN league never supplies one. Its projections arrive
    already scored under its own rules (see espn_projections), which is the
    only safe way to read ESPN scoring at all — #112.
    """
    league = fetch_league_settings(league_id, season, cookies)
    if not league:
        return None

    settings = league.get("settings") or {}
    slot_counts = (settings.get("rosterSettings") or {}).get("lineupSlotCounts") or {}

    roster_positions: list[str] = []
    for slot_id, count in slot_counts.items():
        position = ESPN_SLOTS.get(int(slot_id))
        if not position:
            continue
        roster_positions.extend([position] * int(count))

    return {
        "rosterPositions": roster_positions,
        "numTeams": int(settings.get("size") or 12),
        "season": str(season),
        "espnLeagueId": str(league_id),
    }


def compute_espn_values(
    settings: dict[str, Any],
    crosswalk: dict[str, Any],
    cookies: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Values for an ESPN league, from points ESPN already applied.

    `score_players` is skipped entirely — there is nothing to score. `load_scored`
    fills the same table `values_for` reads, so everything downstream is the
    Sleeper path unchanged.
    """
    players = fetch_league_players(settings["espnLeagueId"], settings["season"], cookies=cookies)
    scored = scored_rows(players, crosswalk)

    con = _connect()
    load_scored(con, scored["rows"])
    starters = starters_by_position(
        settings["rosterPositions"], settings["numTeams"], scored["rows"]
    )
    values = values_for(con, starters)

    return {
        "starters": starters,
        "count": len(values),
        "values": values,
        "unresolved": scored["unresolved"],
    }


def compute_values(settings: dict[str, Any]) -> dict[str, Any]:
    """Values for a resolved settings blob. Knows nothing about any platform."""
    con = _connect()
    scored = score_players(
        con, _parquet_uri(settings["season"]), settings["scoring"], settings["ppr"]
    )
    starters = starters_by_position(
        settings["rosterPositions"], settings["numTeams"], scored
    )
    values = values_for(con, starters)
    return {"starters": starters, "count": len(values), "values": values}


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    body = parse_body(event) or {}
    league_id = body.get("leagueId")
    espn_league_id = body.get("espnLeagueId")

    if espn_league_id:
        season = str(body.get("season") or "")
        if not season:
            return success_response(
                {"error": "season is required with espnLeagueId"}, status_code=400
            )

        cookies = _caller_espn_cookies(event)

        try:
            settings = resolve_espn_settings(espn_league_id, season, cookies)
        except HTTPError as err:
            if err.code not in (401, 403):
                raise
            # The league is private and this caller either never connected
            # ESPN or their cookies expired. Those need different things from
            # the user, so say which.
            return success_response(
                {
                    "error": "espn_auth_required",
                    "hasStoredCredentials": cookies is not None,
                },
                status_code=403,
            )

        if settings is None:
            return success_response({"error": "espn league not found"}, status_code=404)
        if not settings["rosterPositions"]:
            return success_response(
                {"error": "could not read roster shape from espn"}, status_code=502
            )

        log.info(f"Valuing espn league {espn_league_id} ({season})")
        computed = compute_espn_values(settings, _stored_crosswalk(), cookies)

        return success_response({
            "espnLeagueId": settings["espnLeagueId"],
            "season": settings["season"],
            "numTeams": settings["numTeams"],
            # ESPN leagues are valued off ESPN's own projections while Sleeper
            # leagues use the warehouse, so values do not compare across
            # platforms. Say which produced these rather than let a caller
            # assume they are the same currency.
            "projectionSource": "espn",
            "starters": computed["starters"],
            "count": computed["count"],
            "values": computed["values"],
            "unresolved": computed["unresolved"],
        })

    if league_id:
        settings = resolve_sleeper_settings(league_id)
        if settings is None:
            return success_response({"error": "league not found"}, status_code=404)
        if not settings["season"]:
            return success_response({"error": "could not determine season"}, status_code=500)
    elif any(body.get(k) is not None for k in REQUIRED_SETTINGS):
        settings, error = explicit_settings(body)
        if error:
            return success_response({"error": error}, status_code=400)
    else:
        return success_response(
            {"error": "leagueId, espnLeagueId or explicit league settings are required"},
            status_code=400,
        )

    log.info(
        f"Valuing {league_id or 'explicit settings'} ({settings['season']}), "
        f"{settings['numTeams']} teams"
    )

    computed = compute_values(settings)

    return success_response({
        "leagueId": league_id,
        "season": settings["season"],
        "numTeams": settings["numTeams"],
        "projectionSource": "warehouse",
        # Returned so a client can show what the values were built from rather
        # than presenting them as absolute truth.
        "starters": computed["starters"],
        "count": computed["count"],
        "values": computed["values"],
    })
