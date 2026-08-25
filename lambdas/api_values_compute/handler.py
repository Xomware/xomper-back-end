"""
API — Compute league values
===========================
POST /values/compute

Body: { "leagueId": "<sleeper league id>" }

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
from lambdas.common.errors import handle_errors
from lambdas.common.logger import get_logger
from lambdas.common.sleeper_helper import get_nfl_state, get_sleeper_league
from lambdas.common.utility_helpers import parse_body, success_response
from lambdas.common.warehouse_values import (
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


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    body = parse_body(event)
    league_id = (body or {}).get("leagueId")
    if not league_id:
        return success_response({"error": "leagueId is required"}, status_code=400)

    league = get_sleeper_league(league_id)
    if not league:
        return success_response({"error": "league not found"}, status_code=404)

    scoring = league.get("scoring_settings") or {}
    roster_positions = league.get("roster_positions") or []
    num_teams = league.get("total_rosters") or 12
    ppr = scoring.get("rec", 0) or 0

    # Season from the league, falling back to NFL state. A league object
    # carries the season it belongs to, which is what its projections should
    # be read from — not necessarily the current one.
    season = str(league.get("season") or get_nfl_state().get("season") or "")
    if not season:
        return success_response({"error": "could not determine season"}, status_code=500)

    log.info(f"Valuing league {league_id} ({season}), {num_teams} teams")

    con = _connect()
    scored = score_players(con, _parquet_uri(season), scoring, ppr)
    starters = starters_by_position(roster_positions, num_teams, scored)
    values = values_for(con, starters)

    return success_response({
        "leagueId": league_id,
        "season": season,
        "numTeams": num_teams,
        # Returned so a client can show what the values were built from rather
        # than presenting them as absolute truth.
        "starters": starters,
        "count": len(values),
        "values": values,
    })
