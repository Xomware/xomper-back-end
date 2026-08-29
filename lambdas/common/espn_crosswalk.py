"""
ESPN player id -> Sleeper player id.

Sleeper publishes an `espn_id` per player, but it is set for only 6,727 of
12,225 players and resolved 70 of 180 picks (39%) of a real ESPN draft. So the
crosswalk is layered, most authoritative first:

  1. Sleeper's own `espn_id`
  2. FantasyCalc, which publishes `espnId` and `sleeperId` on the same record
  3. Normalized name + position
  4. Team defenses, matched on nickname, since ESPN gives D/ST a negative id
     that appears in no player list

Measured against ESPN's full 2025 player list, 1,196 entries at valued
positions: 99.67% resolve, 4 do not. Name matching carries 45% of the total —
more than either id source — so this is more fragile than "id mapping" sounds,
and `espn_id_source` is stored per player to make that visible.
"""
import json
import re
import urllib.request
from collections import defaultdict
from typing import Any

ESPN_PLAYERS_URL = (
    "https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl/seasons/{season}"
    "/players?view=players_wl"
)
FANTASYCALC_URL = (
    "https://api.fantasycalc.com/values/current"
    "?isDynasty=false&numQbs=1&numTeams=12&ppr=1"
)
USER_AGENT = "xomper-warehouse-ingest/1.0"

# ESPN's defaultPositionId. Only the ones we value.
ESPN_POSITIONS = {1: "QB", 2: "RB", 3: "WR", 4: "TE", 5: "K", 16: "DEF"}

# Below this, something upstream changed and the ingest should not quietly
# publish a half-mapped table. The measured figure is 99.67%; the gap is fringe
# players Sleeper does not carry, which is expected drift, not breakage.
COVERAGE_FLOOR = 0.99

_SUFFIX = re.compile(r"\b(jr|sr|ii|iii|iv|v)\b")


def normalize_name(name: str | None) -> str:
    """Lowercase, strip punctuation and generational suffixes."""
    cleaned = (name or "").lower().replace(".", "").replace("'", "").replace("-", " ")
    return " ".join(_SUFFIX.sub("", cleaned).split())


def _get(url: str) -> Any:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.load(response)


def fetch_espn_players(season: str) -> list[dict[str, Any]]:
    """ESPN's player list. The filter header is required to lift the default page size."""
    request = urllib.request.Request(
        ESPN_PLAYERS_URL.format(season=season),
        headers={
            "User-Agent": USER_AGENT,
            "x-fantasy-filter": json.dumps({"players": {"limit": 8000}}),
        },
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.load(response)


def fetch_fantasycalc() -> list[dict[str, Any]]:
    return _get(FANTASYCALC_URL)


def build_crosswalk(
    sleeper_players: dict[str, dict[str, Any]],
    espn_players: list[dict[str, Any]],
    fantasycalc_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """Map every ESPN player at a valued position onto a Sleeper player id.

    Returns `mapping` (espn id -> {sleeperId, source}), `sources` (counts per
    layer), `misses`, and `coverage`.
    """
    by_espn_id = {
        str(p["espn_id"]): pid for pid, p in sleeper_players.items() if p.get("espn_id")
    }
    by_fantasycalc = {
        str(r["player"]["espnId"]): str(r["player"]["sleeperId"])
        for r in fantasycalc_rows
        if r.get("player", {}).get("espnId") and r.get("player", {}).get("sleeperId")
    }

    by_name: dict[str, list[tuple[str, str]]] = defaultdict(list)
    defenses: dict[str, str] = {}
    for pid, player in sleeper_players.items():
        position = (player.get("position") or "").upper()
        if player.get("full_name"):
            by_name[normalize_name(player["full_name"])].append((pid, position))
        if position == "DEF":
            defenses[normalize_name(player.get("last_name"))] = pid

    mapping: dict[str, dict[str, str]] = {}
    sources: dict[str, int] = defaultdict(int)
    misses: list[dict[str, str]] = []

    for player in espn_players:
        position = ESPN_POSITIONS.get(player.get("defaultPositionId"))
        if not position:
            continue
        espn_id = str(player.get("id"))
        name = player.get("fullName", "")

        resolved = by_espn_id.get(espn_id)
        source = "sleeper_espn_id"

        if not resolved and espn_id in by_fantasycalc:
            resolved, source = by_fantasycalc[espn_id], "fantasycalc"

        if not resolved and position == "DEF":
            # "Texans D/ST" -> "texans". ESPN gives defenses a negative id that
            # is in no id source, so nickname is the only join available.
            resolved = defenses.get(normalize_name(name.replace("D/ST", "")))
            source = "def_nickname"

        if not resolved:
            candidates = by_name.get(normalize_name(name), [])
            if len(candidates) > 1:
                candidates = [c for c in candidates if c[1] == position] or candidates
            if candidates:
                resolved, source = candidates[0][0], "name_position"

        if resolved:
            mapping[espn_id] = {"sleeperId": resolved, "source": source}
            sources[source] += 1
        else:
            misses.append({"espnId": espn_id, "name": name, "position": position})

    total = len(mapping) + len(misses)
    return {
        "mapping": mapping,
        "sources": dict(sources),
        "misses": misses,
        "coverage": len(mapping) / total if total else 0.0,
    }


def crosswalk_from_players_table(players: dict[str, dict[str, Any]]) -> dict[str, dict[str, str]]:
    """ESPN id -> Sleeper id, read from what the nightly ingest already stored.

    The layered resolution in `build_crosswalk` runs once a night against three
    upstreams. Re-running it per request would mean fetching Sleeper's 14.6 MB
    dump plus two APIs to answer one league. The ingest writes `espn_id` and
    `espn_id_source` onto each player row precisely so this side is a lookup.
    """
    mapping: dict[str, dict[str, str]] = {}
    for player_id, player in players.items():
        espn_id = player.get("espn_id")
        if not espn_id:
            continue
        mapping[str(espn_id)] = {
            "sleeperId": str(player_id),
            "source": str(player.get("espn_id_source") or "stored"),
        }
    return mapping
