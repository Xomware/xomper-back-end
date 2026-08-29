"""
Tests for `lambdas.api_values_compute.handler`.

This endpoint was one function that fetched a Sleeper league and valued it in
the same breath. It is now a resolver plus a computation so a non-Sleeper
caller can supply the same settings shape (#110). These tests pin the response
contract so that follow-up cannot quietly change it — the refactor's whole
claim is that the body is identical.
"""
from __future__ import annotations

import importlib
import json
import sys
import types
from typing import Any
from unittest.mock import MagicMock

import pytest

# duckdb reaches the Lambda through a layer, not requirements.txt, so it is not
# importable here. Same shim approach as tests/test_claude_helper.py takes for
# the anthropic SDK. Nothing in these tests exercises real DuckDB — _connect
# and the three warehouse_values functions are all patched out.
if "duckdb" not in sys.modules:
    _duckdb = types.ModuleType("duckdb")
    _duckdb.connect = MagicMock(name="duckdb.connect")
    _duckdb.DuckDBPyConnection = object
    sys.modules["duckdb"] = _duckdb

LEAGUE = {
    "league_id": "123",
    "season": "2025",
    "total_rosters": 12,
    "scoring_settings": {"rec": 1.0, "pass_td": 4.0},
    "roster_positions": ["QB", "RB", "RB", "WR", "WR", "TE", "FLEX", "BN"],
}

SCORED = [("p1", "RB", 250.0), ("p2", "WR", 240.0)]
STARTERS = {"RB": 24, "WR": 24, "QB": 12, "TE": 12}
VALUES = [{"playerId": "p1", "value": 40.0}, {"playerId": "p2", "value": 30.0}]


@pytest.fixture
def mod(monkeypatch):
    m = importlib.import_module("lambdas.api_values_compute.handler")
    monkeypatch.setattr(m, "get_sleeper_league", lambda lid: LEAGUE if lid == "123" else None)
    monkeypatch.setattr(m, "get_nfl_state", lambda: {"season": "2026"})
    monkeypatch.setattr(m, "_connect", lambda: object())
    monkeypatch.setattr(m, "score_players", lambda con, uri, scoring, ppr: SCORED)
    monkeypatch.setattr(m, "starters_by_position", lambda rp, teams, scored: STARTERS)
    monkeypatch.setattr(m, "values_for", lambda con, starters: VALUES)
    return m


def _call(mod: Any, body: dict | None) -> dict:
    event = {"body": json.dumps(body)} if body is not None else {"body": None}
    return mod.handler(event, None)


class TestResolveSleeperSettings:
    def test_maps_league_onto_engine_inputs(self, mod):
        s = mod.resolve_sleeper_settings("123")
        assert s == {
            "scoring": {"rec": 1.0, "pass_td": 4.0},
            "rosterPositions": LEAGUE["roster_positions"],
            "numTeams": 12,
            "ppr": 1.0,
            "season": "2025",
        }

    def test_unknown_league_is_none(self, mod):
        assert mod.resolve_sleeper_settings("nope") is None

    def test_league_season_wins_over_nfl_state(self, mod):
        assert mod.resolve_sleeper_settings("123")["season"] == "2025"

    def test_falls_back_to_nfl_state_without_a_league_season(self, mod, monkeypatch):
        monkeypatch.setattr(mod, "get_sleeper_league", lambda lid: {**LEAGUE, "season": None})
        assert mod.resolve_sleeper_settings("123")["season"] == "2026"

    def test_defaults_when_the_league_omits_fields(self, mod, monkeypatch):
        monkeypatch.setattr(mod, "get_sleeper_league", lambda lid: {"season": "2025"})
        s = mod.resolve_sleeper_settings("123")
        assert s["numTeams"] == 12
        assert s["ppr"] == 0
        assert s["scoring"] == {}
        assert s["rosterPositions"] == []


class TestComputeValues:
    def test_returns_starters_count_and_values(self, mod):
        out = mod.compute_values(mod.resolve_sleeper_settings("123"))
        assert out == {"starters": STARTERS, "count": 2, "values": VALUES}

    def test_takes_a_settings_dict_with_no_league_id(self, mod):
        # The point of the split: nothing Sleeper-shaped reaches this function.
        out = mod.compute_values({
            "scoring": {"rec": 0.5},
            "rosterPositions": ["QB", "RB"],
            "numTeams": 10,
            "ppr": 0.5,
            "season": "2025",
        })
        assert out["count"] == 2


class TestHandlerContract:
    def test_response_body_is_unchanged(self, mod):
        res = _call(mod, {"leagueId": "123"})
        assert res["statusCode"] == 200
        assert json.loads(res["body"]) == {
            "leagueId": "123",
            "season": "2025",
            "numTeams": 12,
            "starters": STARTERS,
            "count": 2,
            "values": VALUES,
        }

    def test_missing_league_id_is_400(self, mod):
        res = _call(mod, {})
        assert res["statusCode"] == 400
        assert "leagueId" in json.loads(res["body"])["error"]

    def test_unknown_league_is_404(self, mod):
        res = _call(mod, {"leagueId": "nope"})
        assert res["statusCode"] == 404

    def test_unresolvable_season_is_500(self, mod, monkeypatch):
        monkeypatch.setattr(mod, "get_sleeper_league", lambda lid: {**LEAGUE, "season": None})
        monkeypatch.setattr(mod, "get_nfl_state", lambda: {})
        res = _call(mod, {"leagueId": "123"})
        assert res["statusCode"] == 500
