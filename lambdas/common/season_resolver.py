"""
Season resolver
===============
Shared helpers for walking Sleeper's `previous_league_id` chain
backwards from the active league.

Used by AI Review F1 (post-draft) + F3 (weekly) to support backfill
runs against historical seasons. Both lambdas need to:

- Walk back N seasons (`seasons_back=1` -> the prior league),
- Or walk back until a league with `season == "<year>"` is found
  (target-year mode).

Returns the resolved Sleeper league dict, or raises `NotFoundError`
if we ran out of `previous_league_id` links before satisfying the
request.

Lives in `lambdas/common/` so both F1's orchestrator and F3's
weekly_orchestrator can import it. The deploy script zips each
lambda dir in isolation; cross-package imports only survive when
they originate in the common layer.
"""
from __future__ import annotations

from typing import Any, Callable

from lambdas.common.errors import NotFoundError
from lambdas.common.logger import get_logger
from lambdas.common.sleeper_helper import get_sleeper_league as _default_get_league

log = get_logger(__file__)

# Bounded to keep accidental misuse from walking forever. The CLT
# Dynasty league started in 2022 so 10 hops is generous.
_MAX_HOPS = 12

# Type alias: caller-injectable league fetcher. Always returns the
# Sleeper league dict for the given id. Tests / orchestrators pass
# their already-patched reference so monkeypatch reaches the
# resolver without needing to also patch this module.
LeagueFetcher = Callable[[str], dict[str, Any]]


def resolve_historical_league(
    league_id: str,
    seasons_back: int,
    *,
    league_fetcher: LeagueFetcher | None = None,
) -> dict[str, Any]:
    """Walk `previous_league_id` exactly `seasons_back` times from
    the given league, returning the Sleeper league dict at that
    depth.

    Args:
        league_id: Active league id to start from.
        seasons_back: Number of times to follow `previous_league_id`.
            0 -> return the active league unchanged. 1 -> immediate
            prior season. Etc. Must be >= 0.
        league_fetcher: Optional injection point — the function used
            to load a league by id. Defaults to
            `sleeper_helper.get_sleeper_league`. Orchestrators pass
            their already-imported reference so monkeypatched
            tests resolve correctly.

    Returns:
        The Sleeper league dict at the requested depth.

    Raises:
        NotFoundError: When the chain is exhausted before we hit the
            requested depth (i.e. the league has no further prior
            seasons linked).
        ValidationError: Never directly; callers should pre-validate
            `seasons_back >= 0`.
    """
    fetch = league_fetcher or _default_get_league

    if seasons_back < 0:
        raise NotFoundError(
            message=(
                f"seasons_back must be >= 0, got {seasons_back}"
            ),
            handler="season_resolver",
            function="resolve_historical_league",
            resource="seasons_back",
        )

    current_id: str | None = league_id
    current_league: dict[str, Any] = fetch(current_id)
    hops = 0
    while hops < seasons_back:
        if hops >= _MAX_HOPS:
            raise NotFoundError(
                message=(
                    f"Exceeded max hops ({_MAX_HOPS}) while walking "
                    f"previous_league_id from {league_id}"
                ),
                handler="season_resolver",
                function="resolve_historical_league",
                resource="previous_league_id",
            )
        prev_id = current_league.get("previous_league_id")
        if prev_id in (None, "", "0"):
            raise NotFoundError(
                message=(
                    f"League {current_league.get('league_id') or current_id} "
                    f"has no previous_league_id — cannot walk "
                    f"{seasons_back} season(s) back from {league_id} "
                    f"(stopped after {hops} hop(s))"
                ),
                handler="season_resolver",
                function="resolve_historical_league",
                resource="previous_league_id",
            )
        current_id = str(prev_id)
        current_league = fetch(current_id)
        hops += 1

    return current_league


def resolve_league_by_year(
    league_id: str,
    target_year: int,
    *,
    league_fetcher: LeagueFetcher | None = None,
) -> dict[str, Any]:
    """Walk `previous_league_id` from the active league until we hit
    a league whose `season == str(target_year)`. Returns that league
    dict.

    Note: this walks BACKWARDS (only). If the active league's
    `season` is older than `target_year`, no forward chain exists
    on Sleeper, so we raise `NotFoundError`.

    Args:
        league_id: Active league id to start from.
        target_year: Calendar year (e.g. 2024). Matched against
            Sleeper's `season` field as a string.
        league_fetcher: Optional injection point — the function used
            to load a league by id. Defaults to
            `sleeper_helper.get_sleeper_league`. Orchestrators pass
            their already-imported reference so monkeypatched
            tests resolve correctly.

    Returns:
        The Sleeper league dict for the matching season.

    Raises:
        NotFoundError: When the chain runs out (no `previous_league_id`)
            before finding a league with `season == str(target_year)`.
    """
    fetch = league_fetcher or _default_get_league
    target = str(target_year)
    current_id: str | None = league_id
    hops = 0
    while True:
        if hops >= _MAX_HOPS:
            raise NotFoundError(
                message=(
                    f"Exceeded max hops ({_MAX_HOPS}) while searching "
                    f"for season={target!r} from {league_id}"
                ),
                handler="season_resolver",
                function="resolve_league_by_year",
                resource="previous_league_id",
            )
        league = fetch(current_id)
        season = str(league.get("season") or "").strip()
        if season == target:
            return league
        prev_id = league.get("previous_league_id")
        if prev_id in (None, "", "0"):
            raise NotFoundError(
                message=(
                    f"No league with season={target!r} reachable from "
                    f"{league_id} via previous_league_id "
                    f"(stopped at season={season!r} after {hops} hop(s))"
                ),
                handler="season_resolver",
                function="resolve_league_by_year",
                resource="target_year",
            )
        current_id = str(prev_id)
        hops += 1
