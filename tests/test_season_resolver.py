"""
Tests for `lambdas.common.season_resolver`.

The resolver walks Sleeper's `previous_league_id` chain backwards
from an active league. Two modes:
  - `resolve_historical_league(league_id, seasons_back=N)` — walk
    exactly N hops.
  - `resolve_league_by_year(league_id, target_year=Y)` — walk
    until `season == str(Y)`.

Both helpers accept an injectable `league_fetcher` so tests don't
need to monkeypatch the underlying HTTP call.
"""
from __future__ import annotations

import pytest

from lambdas.common.errors import NotFoundError
from lambdas.common.season_resolver import (
    resolve_historical_league,
    resolve_league_by_year,
)


# Three-season chain shared by every test below:
#   ACTIVE (2026) -> P1 (2025) -> P2 (2024) -> P3 (2023) -> None.
CHAIN = {
    "ACTIVE": {
        "league_id": "ACTIVE",
        "season": "2026",
        "previous_league_id": "P1",
    },
    "P1": {
        "league_id": "P1",
        "season": "2025",
        "previous_league_id": "P2",
    },
    "P2": {
        "league_id": "P2",
        "season": "2024",
        "previous_league_id": "P3",
    },
    "P3": {
        "league_id": "P3",
        "season": "2023",
        "previous_league_id": None,
    },
}


def _fetch(league_id: str) -> dict:
    return CHAIN[league_id]


class TestResolveHistoricalLeague:
    def test_zero_returns_active(self) -> None:
        result = resolve_historical_league(
            "ACTIVE", 0, league_fetcher=_fetch
        )
        assert result["league_id"] == "ACTIVE"
        assert result["season"] == "2026"

    def test_one_hop(self) -> None:
        result = resolve_historical_league(
            "ACTIVE", 1, league_fetcher=_fetch
        )
        assert result["league_id"] == "P1"
        assert result["season"] == "2025"

    def test_two_hops(self) -> None:
        result = resolve_historical_league(
            "ACTIVE", 2, league_fetcher=_fetch
        )
        assert result["league_id"] == "P2"
        assert result["season"] == "2024"

    def test_three_hops_terminal(self) -> None:
        result = resolve_historical_league(
            "ACTIVE", 3, league_fetcher=_fetch
        )
        assert result["league_id"] == "P3"

    def test_too_many_hops_raises(self) -> None:
        with pytest.raises(NotFoundError):
            resolve_historical_league(
                "ACTIVE", 4, league_fetcher=_fetch
            )

    def test_negative_seasons_back_raises(self) -> None:
        with pytest.raises(NotFoundError):
            resolve_historical_league(
                "ACTIVE", -1, league_fetcher=_fetch
            )

    def test_empty_string_previous_league_id_terminates(self) -> None:
        chain = {
            "A": {
                "league_id": "A",
                "season": "2026",
                "previous_league_id": "",
            }
        }
        with pytest.raises(NotFoundError):
            resolve_historical_league(
                "A", 1, league_fetcher=lambda lid: chain[lid]
            )

    def test_zero_string_previous_league_id_terminates(self) -> None:
        chain = {
            "A": {
                "league_id": "A",
                "season": "2026",
                "previous_league_id": "0",
            }
        }
        with pytest.raises(NotFoundError):
            resolve_historical_league(
                "A", 1, league_fetcher=lambda lid: chain[lid]
            )


class TestResolveLeagueByYear:
    def test_target_active_year(self) -> None:
        result = resolve_league_by_year(
            "ACTIVE", 2026, league_fetcher=_fetch
        )
        assert result["league_id"] == "ACTIVE"

    def test_target_one_back(self) -> None:
        result = resolve_league_by_year(
            "ACTIVE", 2025, league_fetcher=_fetch
        )
        assert result["league_id"] == "P1"

    def test_target_two_back(self) -> None:
        result = resolve_league_by_year(
            "ACTIVE", 2024, league_fetcher=_fetch
        )
        assert result["league_id"] == "P2"

    def test_target_three_back(self) -> None:
        result = resolve_league_by_year(
            "ACTIVE", 2023, league_fetcher=_fetch
        )
        assert result["league_id"] == "P3"

    def test_target_year_unreachable_raises(self) -> None:
        with pytest.raises(NotFoundError):
            resolve_league_by_year(
                "ACTIVE", 1999, league_fetcher=_fetch
            )

    def test_target_year_forward_raises(self) -> None:
        """The chain only walks BACKWARDS — 2027 is unreachable
        from a 2026 active league."""
        with pytest.raises(NotFoundError):
            resolve_league_by_year(
                "ACTIVE", 2027, league_fetcher=_fetch
            )

    def test_target_year_as_string_coerce_safe(self) -> None:
        """The resolver compares str(target_year) — passing 2024
        works even if Sleeper's season field varies in case."""
        chain = {
            "A": {
                "league_id": "A",
                "season": "2024",
                "previous_league_id": None,
            }
        }
        result = resolve_league_by_year(
            "A", 2024, league_fetcher=lambda lid: chain[lid]
        )
        assert result["league_id"] == "A"
