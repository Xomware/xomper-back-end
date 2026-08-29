"""
Tests for `lambdas.api_adp_current.handler`.

Named to match the lambda directory: the deploy workflow runs
`pytest tests/test_<lambda>.py` and prints "No tests found, skipping" for
anything else, so tests in a differently-named file never run in CI and the
job still goes green.
"""
from __future__ import annotations

import json

import pytest


class TestEndpoint:
    SNAPSHOT = {
        "season": "2025",
        "capturedAt": "2026-08-29T08:00:00+00:00",
        "formats": {"ppr": {"type": "PPR", "players": [], "sampleEnd": "2025-09-01"}},
        "failed": {},
    }

    @pytest.fixture
    def mod(self, monkeypatch):
        m = __import__(
            "lambdas.api_adp_current.handler", fromlist=["handler"]
        )
        monkeypatch.setattr(m, "_load", lambda: self.SNAPSHOT)
        return m

    def test_returns_the_whole_snapshot_by_default(self, mod):
        res = mod.handler({}, None)
        assert res["statusCode"] == 200
        assert json.loads(res["body"])["season"] == "2025"

    def test_returns_one_format(self, mod):
        res = mod.handler({"queryStringParameters": {"format": "ppr"}}, None)
        body = json.loads(res["body"])
        assert body["format"] == "ppr"
        assert body["capturedAt"] == self.SNAPSHOT["capturedAt"]

    def test_unsupported_format_is_refused_by_name(self, mod):
        res = mod.handler({"queryStringParameters": {"format": "te_premium"}}, None)
        assert res["statusCode"] == 400
        message = json.loads(res["body"])["error"]["message"]
        assert "te_premium" in message and "ppr" in message

    def test_missing_snapshot_for_a_supported_format_is_404(self, mod):
        res = mod.handler({"queryStringParameters": {"format": "rookie"}}, None)
        assert res["statusCode"] == 404

    def test_no_snapshot_at_all_is_a_named_404(self, mod, monkeypatch):
        # Between deploying the endpoint and the 08:00 UTC ingest there is
        # genuinely no snapshot. That must not read as a broken warehouse.
        monkeypatch.setattr(mod, "_load", lambda: None)
        res = mod.handler({}, None)
        assert res["statusCode"] == 404
        body = json.loads(res["body"])
        assert body["error"] == "no_adp_snapshot"
        assert "ingest" in body["detail"]

    def test_a_real_s3_failure_still_raises(self, mod, monkeypatch):
        from botocore.exceptions import ClientError

        def denied():
            raise ClientError({"Error": {"Code": "AccessDenied"}}, "GetObject")

        monkeypatch.setattr(mod, "_load", denied)
        res = mod.handler({}, None)
        # Not a 404 - a permissions problem is not "no snapshot yet".
        assert res["statusCode"] != 404
