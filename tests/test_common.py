"""
Tests for lambdas.common modules.
Covers utility_helpers, errors, ses_helper validation, models, and dynamo safety guards.
"""

import json
import pytest
from unittest.mock import patch, MagicMock
from datetime import timezone

from lambdas.common.utility_helpers import (
    validate_input,
    require_fields,
    parse_body,
    success_response,
    error_response,
    get_timestamp,
    get_iso_timestamp,
    XomperJSONEncoder,
    json_dumps,
)
from lambdas.common.errors import (
    XomperError,
    ValidationError,
    DynamoDBError,
    handle_errors,
    mask_sensitive_data,
    _mask_emails_in_string,
)
from lambdas.common.ses_helper import validate_email, mask_email
from lambdas.common.models import (
    RuleProposalRequest,
    RuleVoteRequest,
    TaxiStealRequest,
    Proposal,
)


# ============================================
# utility_helpers
# ============================================

class TestValidateInput:
    def test_valid_input(self) -> None:
        data = {"name": "test", "email": "a@b.com"}
        valid, err = validate_input(data, required_fields={"name", "email"})
        assert valid is True
        assert err is None

    def test_missing_required_field(self) -> None:
        data = {"name": "test"}
        valid, err = validate_input(data, required_fields={"name", "email"})
        assert valid is False
        assert "email" in err

    def test_none_data_no_required(self) -> None:
        valid, err = validate_input(None)
        assert valid is True

    def test_none_data_with_required(self) -> None:
        valid, err = validate_input(None, required_fields={"name"})
        assert valid is False


class TestRequireFields:
    def test_happy_path(self) -> None:
        data = {"email": "a@b.com", "name": "test"}
        require_fields(data, "email", "name")

    def test_missing_field_raises(self) -> None:
        data = {"name": "test"}
        with pytest.raises(ValidationError) as exc_info:
            require_fields(data, "email", "name")
        assert "email" in str(exc_info.value)


class TestParseBody:
    def test_json_string(self) -> None:
        event = {"body": '{"key": "val"}'}
        assert parse_body(event) == {"key": "val"}

    def test_dict_body(self) -> None:
        event = {"body": {"key": "val"}}
        assert parse_body(event) == {"key": "val"}

    def test_none_body(self) -> None:
        assert parse_body({}) == {}

    def test_invalid_json(self) -> None:
        event = {"body": "not json"}
        assert parse_body(event) == {}


class TestResponses:
    def test_success_response_shape(self) -> None:
        resp = success_response({"ok": True})
        assert resp["statusCode"] == 200
        assert "xomper.xomware.com" in resp["headers"]["Access-Control-Allow-Origin"]
        body = json.loads(resp["body"])
        assert body["ok"] is True

    def test_error_response_shape(self) -> None:
        resp = error_response("bad", status_code=400)
        assert resp["statusCode"] == 400
        body = json.loads(resp["body"])
        assert body["error"]["message"] == "bad"


class TestTimestamps:
    def test_get_timestamp_format(self) -> None:
        ts = get_timestamp()
        assert len(ts) == 19  # YYYY-MM-DD HH:MM:SS

    def test_get_iso_timestamp(self) -> None:
        ts = get_iso_timestamp()
        assert "+" in ts or "Z" in ts or ts.endswith("+00:00")


# ============================================
# errors
# ============================================

class TestXomperError:
    def test_to_response(self) -> None:
        err = XomperError("fail", handler="test", function="fn", status=400)
        resp = err.to_response()
        assert resp["statusCode"] == 400
        assert "xomper.xomware.com" in resp["headers"]["Access-Control-Allow-Origin"]

    def test_to_dict(self) -> None:
        err = XomperError("fail", status=500)
        d = err.to_dict()
        assert d["error"]["message"] == "fail"


class TestHandleErrors:
    def test_wraps_preserves_name(self) -> None:
        @handle_errors("test")
        def my_handler(event, context):
            return {"statusCode": 200}

        assert my_handler.__name__ == "my_handler"

    def test_catches_xomper_error(self) -> None:
        @handle_errors("test", log_context=False)
        def my_handler(event, context):
            raise ValidationError("bad input", field="name")

        resp = my_handler({}, None)
        assert resp["statusCode"] == 400

    def test_catches_unexpected_error(self) -> None:
        @handle_errors("test", log_context=False)
        def my_handler(event, context):
            raise RuntimeError("boom")

        resp = my_handler({}, None)
        assert resp["statusCode"] == 500


class TestMaskSensitiveData:
    def test_masks_token_field(self) -> None:
        data = {"accessToken": "secret123", "name": "test"}
        masked = mask_sensitive_data(data)
        assert masked["accessToken"] == "***MASKED***"
        assert masked["name"] == "test"

    def test_masks_emails_in_strings(self) -> None:
        data = {"message": "sent to user@example.com"}
        masked = mask_sensitive_data(data)
        assert "user@example.com" not in masked["message"]
        assert "u***@example.com" in masked["message"]


class TestMaskEmailsInString:
    def test_masks_email(self) -> None:
        result = _mask_emails_in_string("contact john@example.com please")
        assert "john@example.com" not in result
        assert "j***@example.com" in result

    def test_no_email(self) -> None:
        result = _mask_emails_in_string("no email here")
        assert result == "no email here"


# ============================================
# ses_helper
# ============================================

class TestEmailValidation:
    def test_valid_email(self) -> None:
        assert validate_email("user@example.com") is True

    def test_invalid_email_no_at(self) -> None:
        assert validate_email("notanemail") is False

    def test_invalid_email_empty(self) -> None:
        assert validate_email("") is False

    def test_invalid_email_none(self) -> None:
        assert validate_email(None) is False

    def test_invalid_email_spaces(self) -> None:
        assert validate_email("user @example.com") is False


class TestMaskEmail:
    def test_masks_local_part(self) -> None:
        assert mask_email("john@example.com") == "j***@example.com"

    def test_empty_string(self) -> None:
        assert mask_email("") == "***"

    def test_no_at(self) -> None:
        assert mask_email("nope") == "***"


# ============================================
# models (Pydantic)
# ============================================

class TestRuleProposalRequest:
    def test_valid_request(self) -> None:
        req = RuleProposalRequest(
            proposal=Proposal(title="Test Rule"),
            recipients=["a@b.com"]
        )
        assert req.proposal.title == "Test Rule"

    def test_empty_recipients_fails(self) -> None:
        with pytest.raises(Exception):
            RuleProposalRequest(
                proposal=Proposal(title="Test"),
                recipients=[]
            )


class TestRuleVoteRequest:
    def test_valid_request(self) -> None:
        req = RuleVoteRequest(
            proposal=Proposal(title="Test"),
            approved_by=["Dom"],
            rejected_by=["Steve"],
            recipients=["a@b.com"],
        )
        assert len(req.approved_by) == 1


class TestTaxiStealRequest:
    def test_valid_request(self) -> None:
        req = TaxiStealRequest(
            stealer={"display_name": "Dom"},
            player={"first_name": "John", "last_name": "Doe"},
            owner={"display_name": "Steve"},
            recipients=["a@b.com"],
            league_name="Test League",
        )
        assert req.stealer.display_name == "Dom"


# ============================================
# dynamo_helpers safety guards
# ============================================

class TestDynamoSafetyGuards:
    def test_empty_table_requires_confirm(self) -> None:
        from lambdas.common.dynamo_helpers import empty_table
        with pytest.raises(DynamoDBError, match="confirm=True"):
            empty_table("test-table", "id", "S")

    def test_delete_table_requires_confirm(self) -> None:
        from lambdas.common.dynamo_helpers import delete_table
        with pytest.raises(DynamoDBError, match="confirm=True"):
            delete_table("test-table")

    def test_empty_table_confirm_false(self) -> None:
        from lambdas.common.dynamo_helpers import empty_table
        with pytest.raises(DynamoDBError):
            empty_table("test-table", "id", "S", confirm=False)


# ============================================
# SSM lazy loading
# ============================================

class TestSSMHelpers:
    @patch("lambdas.common.ssm_helpers.boto3")
    def test_lazy_api_secret_key(self, mock_boto3: MagicMock) -> None:
        """Verify SSM params are fetched lazily, not at import time."""
        import importlib
        import lambdas.common.ssm_helpers as ssm_mod

        # Clear any cached values
        if "API_SECRET_KEY" in ssm_mod.__dict__:
            del ssm_mod.__dict__["API_SECRET_KEY"]
        ssm_mod._cache.clear()
        ssm_mod._ssm_client = None

        mock_client = MagicMock()
        mock_client.get_parameter.return_value = {
            "Parameter": {"Value": "test-secret-123"}
        }
        mock_boto3.client.return_value = mock_client

        value = ssm_mod.API_SECRET_KEY
        assert value == "test-secret-123"
        mock_client.get_parameter.assert_called_once()


# ============================================
# vote_breakdown zero division
# ============================================

class TestVoteBreakdown:
    def test_zero_voters(self) -> None:
        """Ensure no ZeroDivisionError when both voter lists are empty."""
        from lambdas.common.email_templates.base import generate_vote_breakdown
        result = generate_vote_breakdown([], [])
        assert "No yes votes" in result
        assert "No dissenting votes" in result

    def test_normal_voters(self) -> None:
        from lambdas.common.email_templates.base import generate_vote_breakdown
        result = generate_vote_breakdown(["Dom", "Steve"], ["Jake"])
        assert "2 YES" in result
        assert "1 NO" in result
