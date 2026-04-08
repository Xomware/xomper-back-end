"""
Tests for push notification modules: sns_helper, push_templates, and device handlers.
"""

import json
import pytest
from unittest.mock import patch, MagicMock

from lambdas.common.push_templates import (
    rule_proposed_push,
    rule_accepted_push,
    rule_denied_push,
    taxi_steal_push,
)


# ============================================
# push_templates
# ============================================

class TestRuleProposedPush:
    def test_returns_correct_tuple(self) -> None:
        title, body, category, data = rule_proposed_push("Dom", "Allow IR stashing")
        assert title == "New Rule Proposal"
        assert "Dom" in body
        assert "Allow IR stashing" in body
        assert category == "RULE_PROPOSAL"
        assert data == {}

    def test_empty_name(self) -> None:
        title, body, category, data = rule_proposed_push("", "Test Rule")
        assert title == "New Rule Proposal"
        assert "Test Rule" in body


class TestRuleAcceptedPush:
    def test_returns_correct_tuple(self) -> None:
        title, body, category, data = rule_accepted_push("Allow IR stashing")
        assert title == "Rule Approved"
        assert "Allow IR stashing" in body
        assert "approved" in body
        assert category == "RULE_ACCEPTED"
        assert data == {}


class TestRuleDeniedPush:
    def test_returns_correct_tuple(self) -> None:
        title, body, category, data = rule_denied_push("Allow IR stashing")
        assert title == "Rule Denied"
        assert "Allow IR stashing" in body
        assert "denied" in body
        assert category == "RULE_DENIED"
        assert data == {}


class TestTaxiStealPush:
    def test_returns_correct_tuple(self) -> None:
        title, body, category, data = taxi_steal_push("Dom", "John Johnson")
        assert title == "Taxi Steal!"
        assert "Dom" in body
        assert "John Johnson" in body
        assert "stealing" in body
        assert category == "TAXI_STEAL"
        assert data == {}


# ============================================
# sns_helper
# ============================================

class TestCreatePlatformEndpoint:
    @patch("lambdas.common.sns_helper.sns_client")
    def test_success(self, mock_sns: MagicMock) -> None:
        from lambdas.common.sns_helper import create_platform_endpoint

        mock_sns.create_platform_endpoint.return_value = {
            "EndpointArn": "arn:aws:sns:us-east-1:123:endpoint/APNS/app/token123"
        }
        result = create_platform_endpoint("device-token-abc")
        assert result == "arn:aws:sns:us-east-1:123:endpoint/APNS/app/token123"
        mock_sns.create_platform_endpoint.assert_called_once()

    @patch("lambdas.common.sns_helper.sns_client")
    def test_client_error_returns_none(self, mock_sns: MagicMock) -> None:
        from lambdas.common.sns_helper import create_platform_endpoint
        from botocore.exceptions import ClientError

        mock_sns.create_platform_endpoint.side_effect = ClientError(
            {"Error": {"Code": "InvalidParameter", "Message": "bad token"}},
            "CreatePlatformEndpoint",
        )
        result = create_platform_endpoint("bad-token")
        assert result is None

    @patch("lambdas.common.sns_helper.sns_client")
    def test_unexpected_error_returns_none(self, mock_sns: MagicMock) -> None:
        from lambdas.common.sns_helper import create_platform_endpoint

        mock_sns.create_platform_endpoint.side_effect = RuntimeError("boom")
        result = create_platform_endpoint("token")
        assert result is None


class TestDeletePlatformEndpoint:
    @patch("lambdas.common.sns_helper.sns_client")
    def test_success(self, mock_sns: MagicMock) -> None:
        from lambdas.common.sns_helper import delete_platform_endpoint

        mock_sns.delete_endpoint.return_value = {}
        result = delete_platform_endpoint("arn:aws:sns:us-east-1:123:endpoint/test")
        assert result is True

    @patch("lambdas.common.sns_helper.sns_client")
    def test_client_error_returns_false(self, mock_sns: MagicMock) -> None:
        from lambdas.common.sns_helper import delete_platform_endpoint
        from botocore.exceptions import ClientError

        mock_sns.delete_endpoint.side_effect = ClientError(
            {"Error": {"Code": "NotFound", "Message": "not found"}},
            "DeleteEndpoint",
        )
        result = delete_platform_endpoint("arn:aws:sns:us-east-1:123:endpoint/test")
        assert result is False


class TestSendPush:
    @patch("lambdas.common.sns_helper.sns_client")
    def test_success(self, mock_sns: MagicMock) -> None:
        from lambdas.common.sns_helper import send_push

        mock_sns.publish.return_value = {"MessageId": "msg-123"}
        result = send_push(
            "arn:aws:sns:us-east-1:123:endpoint/test",
            "Test Title",
            "Test Body",
            category="TEST",
            data={"key": "val"},
        )
        assert result is True
        call_args = mock_sns.publish.call_args
        message = json.loads(call_args.kwargs["Message"])
        apns_payload = json.loads(message["APNS"])
        assert apns_payload["aps"]["alert"]["title"] == "Test Title"
        assert apns_payload["aps"]["alert"]["body"] == "Test Body"
        assert apns_payload["aps"]["category"] == "TEST"
        assert apns_payload["data"] == {"key": "val"}

    @patch("lambdas.common.sns_helper.sns_client")
    def test_no_category_or_data(self, mock_sns: MagicMock) -> None:
        from lambdas.common.sns_helper import send_push

        mock_sns.publish.return_value = {"MessageId": "msg-456"}
        result = send_push("arn:test", "Title", "Body")
        assert result is True
        call_args = mock_sns.publish.call_args
        message = json.loads(call_args.kwargs["Message"])
        apns_payload = json.loads(message["APNS"])
        assert "category" not in apns_payload["aps"]
        assert "data" not in apns_payload

    @patch("lambdas.common.sns_helper.sns_client")
    def test_failure_returns_false(self, mock_sns: MagicMock) -> None:
        from lambdas.common.sns_helper import send_push

        mock_sns.publish.side_effect = RuntimeError("network error")
        result = send_push("arn:test", "Title", "Body")
        assert result is False


class TestSendPushToUsers:
    @patch("lambdas.common.sns_helper.send_push")
    @patch("lambdas.common.sns_helper._get_endpoints_for_users")
    def test_sends_to_all_endpoints(self, mock_get: MagicMock, mock_send: MagicMock) -> None:
        from lambdas.common.sns_helper import send_push_to_users

        mock_get.return_value = ["arn:1", "arn:2"]
        mock_send.return_value = True
        successes, failures = send_push_to_users(["user1"], "Title", "Body")
        assert successes == 2
        assert failures == 0

    @patch("lambdas.common.sns_helper._get_endpoints_for_users")
    def test_empty_user_ids(self, mock_get: MagicMock) -> None:
        from lambdas.common.sns_helper import send_push_to_users

        successes, failures = send_push_to_users([], "Title", "Body")
        assert successes == 0
        assert failures == 0
        mock_get.assert_not_called()

    @patch("lambdas.common.sns_helper._get_endpoints_for_users")
    def test_no_endpoints_found(self, mock_get: MagicMock) -> None:
        from lambdas.common.sns_helper import send_push_to_users

        mock_get.return_value = []
        successes, failures = send_push_to_users(["user1"], "Title", "Body")
        assert successes == 0
        assert failures == 0

    @patch("lambdas.common.sns_helper.send_push")
    @patch("lambdas.common.sns_helper._get_endpoints_for_users")
    def test_partial_failure(self, mock_get: MagicMock, mock_send: MagicMock) -> None:
        from lambdas.common.sns_helper import send_push_to_users

        mock_get.return_value = ["arn:1", "arn:2", "arn:3"]
        mock_send.side_effect = [True, False, True]
        successes, failures = send_push_to_users(["user1"], "Title", "Body")
        assert successes == 2
        assert failures == 1

    @patch("lambdas.common.sns_helper._get_endpoints_for_users")
    def test_exception_returns_zero(self, mock_get: MagicMock) -> None:
        from lambdas.common.sns_helper import send_push_to_users

        mock_get.side_effect = RuntimeError("db error")
        successes, failures = send_push_to_users(["user1"], "Title", "Body")
        assert successes == 0
        assert failures == 0


# ============================================
# api_register_device handler
# ============================================

class TestRegisterDeviceHandler:
    @patch("lambdas.api_register_device.handler.device_tokens_table")
    @patch("lambdas.api_register_device.handler.create_platform_endpoint")
    def test_success(self, mock_create: MagicMock, mock_table: MagicMock) -> None:
        from lambdas.api_register_device.handler import handler

        mock_create.return_value = "arn:aws:sns:us-east-1:123:endpoint/test"
        event = {"body": json.dumps({
            "user_id": "user123",
            "device_token": "token-abc",
            "platform": "ios",
        })}
        resp = handler(event, None)
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["Success"] is True
        mock_table.put_item.assert_called_once()
        item = mock_table.put_item.call_args.kwargs["Item"]
        assert item["user_id"] == "user123"
        assert item["endpoint_arn"] == "arn:aws:sns:us-east-1:123:endpoint/test"

    @patch("lambdas.api_register_device.handler.create_platform_endpoint")
    def test_missing_fields(self, mock_create: MagicMock) -> None:
        from lambdas.api_register_device.handler import handler

        event = {"body": json.dumps({"user_id": "user123"})}
        resp = handler(event, None)
        assert resp["statusCode"] == 400

    @patch("lambdas.api_register_device.handler.device_tokens_table")
    @patch("lambdas.api_register_device.handler.create_platform_endpoint")
    def test_endpoint_creation_failure(self, mock_create: MagicMock, mock_table: MagicMock) -> None:
        from lambdas.api_register_device.handler import handler

        mock_create.return_value = None
        event = {"body": json.dumps({
            "user_id": "user123",
            "device_token": "bad-token",
        })}
        resp = handler(event, None)
        assert resp["statusCode"] == 500
        body = json.loads(resp["body"])
        assert body["Success"] is False


# ============================================
# api_unregister_device handler
# ============================================

class TestUnregisterDeviceHandler:
    @patch("lambdas.api_unregister_device.handler.device_tokens_table")
    @patch("lambdas.api_unregister_device.handler.delete_platform_endpoint")
    def test_success(self, mock_delete: MagicMock, mock_table: MagicMock) -> None:
        from lambdas.api_unregister_device.handler import handler

        mock_table.get_item.return_value = {
            "Item": {
                "user_id": "user123",
                "device_token": "token-abc",
                "endpoint_arn": "arn:aws:sns:us-east-1:123:endpoint/test",
            }
        }
        mock_delete.return_value = True
        event = {"body": json.dumps({
            "user_id": "user123",
            "device_token": "token-abc",
        })}
        resp = handler(event, None)
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["Success"] is True
        mock_delete.assert_called_once_with("arn:aws:sns:us-east-1:123:endpoint/test")
        mock_table.delete_item.assert_called_once()

    @patch("lambdas.api_unregister_device.handler.device_tokens_table")
    def test_item_not_found(self, mock_table: MagicMock) -> None:
        from lambdas.api_unregister_device.handler import handler

        mock_table.get_item.return_value = {}
        event = {"body": json.dumps({
            "user_id": "user123",
            "device_token": "nonexistent",
        })}
        resp = handler(event, None)
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["Success"] is True
        mock_table.delete_item.assert_not_called()

    def test_missing_fields(self) -> None:
        from lambdas.api_unregister_device.handler import handler

        event = {"body": json.dumps({"user_id": "user123"})}
        resp = handler(event, None)
        assert resp["statusCode"] == 400
