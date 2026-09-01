"""
Tests for `lambdas.api_rankings_current.handler`.

Named to match the lambda directory so the deploy matrix actually runs it.
"""
import json
import sys
import types
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

if "duckdb" not in sys.modules:
    _d = types.ModuleType("duckdb"); _d.connect = MagicMock(); _d.DuckDBPyConnection = object
    sys.modules["duckdb"] = _d

from lambdas.api_rankings_current import handler as mod  # noqa: E402

SNAPSHOT = {
    "capturedAt": "2026-08-30T08:00:00Z",
    "season": "2026",
    "sources": ["espn", "ffc"],
    "failed": {"fantasycalc": "timeout"},
    "players": {"p1": {"ranks": {"ffc": 12, "espn": 14}, "consensus": 13.0,
                       "spread": 1.0, "sourceCount": 2}},
}


def test_returns_the_snapshot(monkeypatch):
    monkeypatch.setattr(mod, "_load", lambda: SNAPSHOT)
    res = mod.handler({}, None)
    body = json.loads(res["body"])
    assert res["statusCode"] == 200
    assert body["count"] == 1
    assert body["players"]["p1"]["spread"] == 1.0


def test_names_the_sources_that_failed(monkeypatch):
    # Otherwise two lists get presented as three and nobody can tell.
    monkeypatch.setattr(mod, "_load", lambda: SNAPSHOT)
    body = json.loads(mod.handler({}, None)["body"])
    assert body["failed"] == {"fantasycalc": "timeout"}
    assert body["sources"] == ["espn", "ffc"]


def test_missing_snapshot_is_a_named_404(monkeypatch):
    monkeypatch.setattr(mod, "_load", lambda: None)
    res = mod.handler({}, None)
    assert res["statusCode"] == 404
    assert json.loads(res["body"])["error"] == "no_rankings_snapshot"


def test_a_real_s3_failure_still_raises(monkeypatch):
    def denied():
        raise ClientError({"Error": {"Code": "AccessDenied"}}, "GetObject")

    monkeypatch.setattr(mod, "_load", denied)
    # A permissions problem is not "no snapshot yet".
    assert mod.handler({}, None)["statusCode"] != 404
