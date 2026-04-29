"""
XOMPER SNS Helper
=================
Push notification sending via AWS SNS with fire-and-forget safety.
"""

import json
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from lambdas.common.constants import (
    AWS_DEFAULT_REGION,
    SNS_PLATFORM_APP_ARN,
    DEVICE_TOKENS_TABLE,
)
from lambdas.common.logger import get_logger

log = get_logger(__file__)

sns_client = boto3.client('sns', region_name=AWS_DEFAULT_REGION)
dynamodb = boto3.resource('dynamodb', region_name=AWS_DEFAULT_REGION)
device_tokens_table = dynamodb.Table(DEVICE_TOKENS_TABLE)


def create_platform_endpoint(device_token: str) -> Optional[str]:
    """
    Create an SNS platform endpoint for a device token.

    Args:
        device_token: The APNs device token

    Returns:
        Endpoint ARN string on success, None on failure
    """
    try:
        response = sns_client.create_platform_endpoint(
            PlatformApplicationArn=SNS_PLATFORM_APP_ARN,
            Token=device_token,
        )
        endpoint_arn = response.get('EndpointArn')
        log.info(f"Created SNS endpoint: {endpoint_arn}")
        return endpoint_arn
    except ClientError as err:
        error = err.response['Error']
        log.error(f"SNS error creating endpoint: {error['Code']} - {error['Message']}")
        return None
    except Exception as err:
        log.error(f"Error creating SNS endpoint: {err}")
        return None


def delete_platform_endpoint(endpoint_arn: str) -> bool:
    """
    Delete an SNS platform endpoint.

    Args:
        endpoint_arn: The endpoint ARN to delete

    Returns:
        True on success, False on failure
    """
    try:
        sns_client.delete_endpoint(EndpointArn=endpoint_arn)
        log.info(f"Deleted SNS endpoint: {endpoint_arn}")
        return True
    except ClientError as err:
        error = err.response['Error']
        log.error(f"SNS error deleting endpoint {endpoint_arn}: {error['Code']} - {error['Message']}")
        return False
    except Exception as err:
        log.error(f"Error deleting SNS endpoint {endpoint_arn}: {err}")
        return False


def send_push(
    endpoint_arn: str,
    title: str,
    body: str,
    category: Optional[str] = None,
    data: Optional[dict] = None,
) -> bool:
    """
    Send a push notification to a single SNS endpoint.

    Args:
        endpoint_arn: Target SNS platform endpoint ARN
        title: Notification title
        body: Notification body text
        category: Optional APNs category for actionable notifications
        data: Optional custom data payload

    Returns:
        True on success, False on failure
    """
    try:
        apns_payload: dict = {
            "aps": {
                "alert": {"title": title, "body": body},
                "sound": "default",
            }
        }
        if category:
            apns_payload["aps"]["category"] = category
        if data:
            apns_payload["data"] = data

        message = json.dumps({
            "APNS": json.dumps(apns_payload),
        })

        response = sns_client.publish(
            TargetArn=endpoint_arn,
            Message=message,
            MessageStructure='json',
        )
        log.info(f"Push sent to {endpoint_arn}, MessageId: {response.get('MessageId')}")
        return True
    except ClientError as err:
        error = err.response['Error']
        log.error(f"SNS publish error for {endpoint_arn}: {error['Code']} - {error['Message']}")
        return False
    except Exception as err:
        log.error(f"Error sending push to {endpoint_arn}: {err}")
        return False


def _get_endpoints_for_users(user_ids: list[str]) -> list[str]:
    """
    Query the device_tokens DynamoDB table for all endpoint ARNs for given user IDs.

    Args:
        user_ids: List of user IDs to look up

    Returns:
        List of endpoint ARN strings
    """
    endpoint_arns: list[str] = []
    for user_id in user_ids:
        try:
            response = device_tokens_table.query(
                KeyConditionExpression=Key('user_id').eq(user_id),
            )
            for item in response.get('Items', []):
                arn = item.get('endpoint_arn')
                if arn:
                    endpoint_arns.append(arn)
        except Exception as err:
            log.error(f"Error querying device tokens for user {user_id}: {err}")
    return endpoint_arns


def send_push_to_users(
    user_ids: list[str],
    title: str,
    body: str,
    category: Optional[str] = None,
    data: Optional[dict] = None,
) -> tuple[int, int]:
    """
    Send push notifications to all devices for the given user IDs.

    Queries the device_tokens DynamoDB table for endpoint ARNs, then sends
    concurrently using a thread pool. Fire-and-forget safe -- exceptions are
    caught and logged, never raised. Every per-user attempt (including
    "no device token registered") writes a row to xomper-notification-log
    so the admin portal sees the full activity feed.

    Args:
        user_ids: List of user IDs to notify
        title: Notification title
        body: Notification body text
        category: Optional APNs category
        data: Optional custom data payload

    Returns:
        Tuple of (successes, failures)
    """
    if not user_ids:
        return 0, 0

    # Lazy import — see ses_helper for rationale.
    from lambdas.common.notification_log import log_push

    try:
        endpoint_arns = _get_endpoints_for_users(user_ids)

        # Single-user attribution covers ~all current callers (handlers
        # send per-manager). For multi-user fan-outs (rule proposal /
        # accept / deny) we attribute log rows to "_multi" — the admin
        # portal can correlate by `title` + `epoch_ms`.
        attributed_user = user_ids[0] if len(user_ids) == 1 else "_multi"

        if not endpoint_arns:
            log.info(f"No device tokens found for {len(user_ids)} user(s)")
            for uid in user_ids:
                log_push(
                    user_id=uid,
                    title=title,
                    body=body,
                    category=category,
                    success=False,
                    error="no device tokens registered",
                )
            return 0, 0

        with ThreadPoolExecutor(max_workers=min(len(endpoint_arns), 10)) as executor:
            futures = [
                executor.submit(send_push, arn, title, body, category, data)
                for arn in endpoint_arns
            ]
            results = [f.result() for f in futures]

        for ok in results:
            log_push(
                user_id=attributed_user,
                title=title,
                body=body,
                category=category,
                success=ok,
                error=None if ok else "send_push returned false",
            )

        successes = sum(1 for r in results if r)
        failures = len(results) - successes
        log.info(f"Push notifications complete: {successes} sent, {failures} failed for {len(user_ids)} user(s)")
        return successes, failures
    except Exception as err:
        log.error(f"Error in send_push_to_users: {err}")
        return 0, 0
