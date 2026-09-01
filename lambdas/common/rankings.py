"""
Consensus rankings from several public sources.

One list is one opinion. The useful signal from several is not a better average
but the **disagreement**: a player the sources rank 12th, 14th and 41st is a
decision, and no single list would have told you that.

Sources, all public and key-free:

- FantasyFootballCalculator ADP, already ingested per format by `ffc_adp`
- FantasyCalc trade values, which carry an overall rank
- ESPN's own draft ranks from the public `leaguedefaults` endpoint

Deliberately absent: FantasyPros, PFF and DraftSharks. All three are paid
products whose terms forbid scraping. There is no key-free endpoint to use, so
they are not here and cannot be without a subscription and an agreement.
"""
from __future__ import annotations

import json
import re
import urllib.request
from statistics import mean, pstdev
from typing import Any

USER_AGENT = "Mozilla/5.0 (compatible; xomper/1.0)"

FANTASYCALC_URL = (
    "https://api.fantasycalc.com/values/current"
    "?isDynasty={dynasty}&numQbs={qbs}&numTeams={teams}&ppr={ppr}"
)
ESPN_RANKS_URL = (
    "https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl/seasons/{season}"
    "/segments/0/leaguedefaults/3?view=kona_player_info"
)

# ESPN lineup slot -> position, for the name fallback when espn_id is absent.
SLOT_POS = {0: "QB", 2: "RB", 4: "WR", 6: "TE", 16: "DEF", 17: "K"}

SUFFIX = re.compile(r"\b(jr|sr|ii|iii|iv|v)\b")


def norm_name(name: str | None) -> str:
    """Lowercase, strip punctuation and generational suffixes.

    Sources disagree on "Marvin Harrison Jr." vs "Marvin Harrison", and on
    apostrophes in "Ja'Marr". Neither difference is real.
    """
    cleaned = re.sub(r"[^a-z ]", "", (name or "").lower())
    return re.sub(r"\s+", " ", SUFFIX.sub("", cleaned)).strip()


def _get(url: str, headers: dict[str, str] | None = None) -> Any:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, **(headers or {})})
    with urllib.request.urlopen(request, timeout=45) as response:
        return json.load(response)


def fetch_fantasycalc(dynasty: bool, qbs: int, teams: int, ppr: float) -> list[dict[str, Any]]:
    return _get(
        FANTASYCALC_URL.format(
            dynasty=str(bool(dynasty)).lower(), qbs=qbs, teams=teams, ppr=ppr
        )
    )


def fetch_espn_ranks(season: str, rank_type: str = "PPR", limit: int = 300) -> list[dict[str, Any]]:
    """ESPN's published draft ranks. Public, no cookies — this is the default
    league view every logged-out visitor gets."""
    filt = {
        "players": {
            "limit": limit,
            "sortDraftRanks": {"sortPriority": 1, "sortAsc": True, "value": rank_type},
        }
    }
    payload = _get(ESPN_RANKS_URL.format(season=season), {"x-fantasy-filter": json.dumps(filt)})
    return payload.get("players", []) or []


def _resolve(name: str, position: str | None, by_name: dict[tuple[str, str], str]) -> str | None:
    key = (norm_name(name), position or "")
    if key in by_name:
        return by_name[key]
    # ESPN writes defenses as "Steelers D/ST"; Sleeper keys them by team
    # abbreviation. Nothing else about the player differs.
    if position == "DEF":
        nickname = norm_name(name).replace(" dst", "").strip()
        return by_name.get((nickname, "DEF"))
    return None


def fantasycalc_ranks(
    rows: list[dict[str, Any]], by_name: dict[tuple[str, str], str]
) -> dict[str, int]:
    """Sleeper id -> FantasyCalc overall rank."""
    out: dict[str, int] = {}
    for row in rows:
        player = row.get("player") or {}
        pid = _resolve(player.get("name", ""), player.get("position"), by_name)
        rank = row.get("overallRank")
        if pid and isinstance(rank, int):
            out[pid] = rank
    return out


def espn_ranks(
    rows: list[dict[str, Any]],
    by_espn_id: dict[str, str],
    by_name: dict[tuple[str, str], str],
    rank_type: str = "PPR",
) -> dict[str, int]:
    """Sleeper id -> ESPN draft rank, resolved by espn_id then by name.

    espn_id alone resolves about a third of the list, because Sleeper carries it
    for only some players. The name fallback takes it past 90%, which is the
    same layered approach the player crosswalk uses.
    """
    out: dict[str, int] = {}
    for row in rows:
        player = row.get("player") or {}
        pid = by_espn_id.get(str(row.get("id")))
        if not pid:
            position = next(
                (SLOT_POS[s] for s in (player.get("eligibleSlots") or []) if s in SLOT_POS),
                None,
            )
            pid = _resolve(player.get("fullName", ""), position, by_name)
        rank = ((player.get("draftRanksByRankType") or {}).get(rank_type) or {}).get("rank")
        if pid and isinstance(rank, int):
            out[pid] = rank
    return out


def adp_ranks(adp_players: list[dict[str, Any]], by_name: dict[tuple[str, str], str]) -> dict[str, int]:
    """FFC ADP converted to a rank, so it is comparable with the others.

    ADP is a pick number and the other sources are ordinals. Averaging them
    directly would let a 12-team ADP outvote a 300-long rank list purely
    through scale.
    """
    ordered = sorted(
        (p for p in adp_players if isinstance(p.get("adp"), (int, float))),
        key=lambda p: p["adp"],
    )
    out: dict[str, int] = {}
    for i, player in enumerate(ordered, start=1):
        pid = _resolve(player.get("name", ""), player.get("position"), by_name)
        if pid:
            out[pid] = i
    return out


def consensus(sources: dict[str, dict[str, int]]) -> dict[str, dict[str, Any]]:
    """Merge per-source ranks into a consensus plus a disagreement measure.

    `spread` is the population standard deviation of the ranks a player has.
    It is the point of pulling several lists: a player at 12, 14 and 41 is worth
    a second look, and an average alone would bury that.

    A player ranked by one source keeps that rank as consensus with spread 0 and
    `sourceCount` 1, so a caller can tell "everyone agrees" from "only one list
    has an opinion" — they are not the same thing.
    """
    players: dict[str, dict[str, Any]] = {}
    for source, ranks in sources.items():
        for pid, rank in ranks.items():
            players.setdefault(pid, {"ranks": {}})["ranks"][source] = rank

    for pid, row in players.items():
        values = list(row["ranks"].values())
        row["consensus"] = round(mean(values), 1)
        row["spread"] = round(pstdev(values), 1) if len(values) > 1 else 0.0
        row["sourceCount"] = len(values)
    return players
