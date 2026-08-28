"""
Warehouse — Projections Ingest (scheduled)
==========================================
Runs nightly. Reads Sleeper's season projections and writes them to the
warehouse bucket as Parquet, then refreshes the slimmed player metadata
table that the frontend reads instead of downloading the ~5 MB
/players/nfl dump on every session.

Triggered by EventBridge cron at 08:00 UTC — after the US night, before
anyone opens the app. No API Gateway integration.

Why DuckDB rather than plain Python
-----------------------------------
The valuation is a dot product of a league's scoring_settings against
projected stats, then a per-position ranking. That is a window function,
not a hand-rolled sort. A spike (xomper-frontend/tools/duckdb-spike/)
established two things worth knowing here:

- The SQL port is byte-identical to the TypeScript engine the app ships:
  all 3,227 scored players match on points and value.
- DuckDB reads the Sleeper endpoint directly. `read_json_auto()` over the
  URL returned 3,302 rows in 597 ms, so there is no download-then-parse
  step in this handler at all.

Deliberately NOT done here: computing and storing a values grid. Values
are a function of each league's own scoring_settings and roster_positions,
which are arbitrary, and computing one league on demand measured ~10 ms.
Storing a cross product would be precomputing something cheaper to derive.
This job stores the *inputs*; the API computes per request.

Idempotency: the nightly write is a full replace of `projections/current`,
plus a dated snapshot. Re-invoking on the same day overwrites both with
identical content.
"""
from typing import Any

import boto3
import duckdb

from lambdas.common.constants import (
    PLAYERS_TABLE_NAME,
    WAREHOUSE_BUCKET_NAME,
)
from lambdas.common.errors import handle_errors
from lambdas.common.logger import get_logger
from lambdas.common.sleeper_helper import fetch_nfl_players, get_nfl_state
from lambdas.common.utility_helpers import success_response

HANDLER = "warehouse_ingest"
log = get_logger(HANDLER)

PROJECTIONS_URL = (
    "https://api.sleeper.com/projections/nfl/{season}"
    "?season_type=regular"
    "&position[]=QB&position[]=RB&position[]=WR&position[]=TE"
    "&position[]=K&position[]=DEF"
)

# Attribution, not a workaround. Sleeper rejects Python's DEFAULT urllib
# agent and returns nothing -- that bit the Phase 3 coverage measurement,
# where every redraft league silently scored 0% until a header was added.
# DuckDB's HTTP client sends its own agent and Sleeper accepts it, verified
# before this handler was written. This just identifies the caller.
#
# Note `custom_user_agent` APPENDS to DuckDB's agent rather than replacing
# it. The setting is not `http_useragent`, which does not exist.
USER_AGENT = "xomper-warehouse-ingest/1.0"

# The only writable path in a Lambda execution environment.
EPHEMERAL_DIR = "/tmp"

VALUED_POSITIONS = ("QB", "RB", "WR", "TE", "K", "DEF")

# Fields the frontend actually reads.
#
# Sleeper's /players/nfl dump is 14.6 MB and the browser downloads all of it
# every session. This is the subset the app touches, counted from usage across
# the Angular source rather than guessed: position (68 references), status
# (53), team (49), first/last name (12 each), number, years_exp,
# injury_status, age. Plus the espn/yahoo ids, which nothing renders but which
# are the cross-platform crosswalk.
#
# Adding a field here is cheap; omitting one the UI reads is not. An earlier
# version of this list left out status, injury_status, age, years_exp and
# number, which would have blanked those in every player view.
PLAYER_FIELDS = (
    "first_name",
    "last_name",
    "position",
    "team",
    "status",
    "injury_status",
    "age",
    "years_exp",
    "number",
    "espn_id",
    "yahoo_id",
    "search_rank",
    # Added after a field-by-field diff of the Angular source against this
    # list found these read in the UI but never stored. The Sleeper fallback
    # in PlayerService only fires on a network error, so a missing field is
    # not a failure it can detect -- it just serves blanks.
    "fantasy_positions",
    "height",
    "weight",
    "college",
    "depth_chart_order",
    "search_full_name",
)


def _connect() -> duckdb.DuckDBPyConnection:
    # custom_user_agent is connect-time only. `SET` after the database is
    # running raises "Cannot change custom_user_agent setting while database
    # is running", so it has to go in the config dict.
    con = duckdb.connect(config={
        "custom_user_agent": USER_AGENT,
        # Lambda has no writable HOME. Without these DuckDB fails on connect
        # with "IO Error: Can't find the home directory at ''" before a single
        # query runs. /tmp is the only writable path in the execution
        # environment, and it survives for the life of the container so a warm
        # invoke reuses the extensions rather than downloading them again.
        "home_directory": EPHEMERAL_DIR,
        "extension_directory": f"{EPHEMERAL_DIR}/duckdb_extensions",
    })
    con.execute("INSTALL json; LOAD json;")
    con.execute("INSTALL httpfs; LOAD httpfs;")

    # Reads the standard AWS chain, so in Lambda this is the execution role
    # with no keys anywhere in config. Verified writing and reading Parquet
    # against the real bucket before this handler was written.
    con.execute("CREATE SECRET (TYPE S3, PROVIDER CREDENTIAL_CHAIN);")
    return con


def _ingest_projections(con: duckdb.DuckDBPyConnection, season: str) -> int:
    """Read projections straight from the API and land them as Parquet."""
    url = PROJECTIONS_URL.format(season=season)
    bucket = WAREHOUSE_BUCKET_NAME

    positions = ", ".join(f"'{p}'" for p in VALUED_POSITIONS)
    con.execute(f"""
        CREATE OR REPLACE TABLE proj AS
        SELECT player_id,
               upper(coalesce(player.position, '')) AS position,
               stats
        FROM read_json_auto('{url}')
        WHERE upper(coalesce(player.position, '')) IN ({positions})
    """)

    total = con.execute("SELECT count(*) FROM proj").fetchone()[0]
    if total == 0:
        # Treated as a hard failure, not an empty day. A silent empty write
        # would leave every league unpriceable until someone noticed.
        raise RuntimeError(
            f"projections for {season} came back empty — refusing to write "
            "an empty warehouse"
        )

    # Long form: one row per (player, stat). This is the shape the scoring
    # dot product wants and what a columnar engine is fastest at.
    con.execute("""
        CREATE OR REPLACE TABLE stat_long AS
        SELECT p.player_id,
               p.position,
               k.key AS stat_key,
               TRY_CAST(json_extract_string(to_json(p.stats),
                        '$."' || k.key || '"') AS DOUBLE) AS stat_value
        FROM proj p,
             LATERAL unnest(json_keys(to_json(p.stats))) AS k(key)
    """)

    rows = con.execute("SELECT count(*) FROM stat_long").fetchone()[0]

    # Current, plus a dated snapshot so a Sleeper outage degrades to
    # stale-but-present rather than broken. The snapshot prefix is what the
    # bucket's 90-day lifecycle rule expires.
    today = con.execute("SELECT strftime(current_date, '%Y-%m-%d')").fetchone()[0]

    for key in (
        f"projections/current/season={season}/stats.parquet",
        f"snapshots/season={season}/dt={today}/stats.parquet",
    ):
        con.execute(f"""
            COPY (SELECT * FROM stat_long)
            TO 's3://{bucket}/{key}'
            (FORMAT PARQUET, OVERWRITE_OR_IGNORE true)
        """)

    log.info(f"projections: {total} players -> {rows} stat rows for {season}")
    return rows


def _refresh_players() -> int:
    """Slim the /players/nfl dump into DynamoDB for the frontend."""
    players = fetch_nfl_players()
    table = boto3.resource("dynamodb").Table(PLAYERS_TABLE_NAME)

    written = 0
    with table.batch_writer(overwrite_by_pkeys=["playerId"]) as batch:
        for player_id, player in players.items():
            position = (player.get("position") or "").upper()
            # Everything Sleeper knows about, including retired and practice
            # squad, is in this dump. Only positions we value are worth the
            # write cost.
            if position not in VALUED_POSITIONS:
                continue

            item = {"playerId": str(player_id)}
            for field in PLAYER_FIELDS:
                value = player.get(field)
                if value is None or value == "":
                    continue
                if isinstance(value, list):
                    # fantasy_positions. str() would store "['WR', 'FLEX']".
                    item[field] = [str(v) for v in value]
                elif isinstance(value, (int, float)):
                    item[field] = value
                else:
                    item[field] = str(value)
            batch.put_item(Item=item)
            written += 1

    log.info(f"players: wrote {written} of {len(players)} to {PLAYERS_TABLE_NAME}")
    return written


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    log.info("Starting warehouse projections ingest...")

    # Season comes from Sleeper rather than the calendar. The two disagree
    # every January, and a wrong season silently ingests last year.
    state = get_nfl_state()
    season = str(state.get("season") or "")
    if not season:
        raise RuntimeError("could not determine current season from NFL state")

    con = _connect()
    stat_rows = _ingest_projections(con, season)
    players_written = _refresh_players()

    log.info("Warehouse ingest complete.")
    return success_response(
        {
            "season": season,
            "statRows": stat_rows,
            "playersWritten": players_written,
        },
        is_api=False,
    )
