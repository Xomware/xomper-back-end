"""
POST /api/register-device - Register Device for Push Notifications
Stores device token and creates SNS platform endpoint.

Expected body:
{
    "user_id": "abc123",
    "device_token": "apns-device-token-string",
    "platform": "ios"
}
"""
import boto3

from lambdas.common.logger import get_logger
from lambdas.common.errors import handle_errors
from lambdas.common.utility_helpers import (
    success_response,
    parse_body,
    require_fields,
    get_iso_timestamp,
)
from lambdas.common.sns_helper import create_platform_endpoint
from lambdas.common.constants import AWS_DEFAULT_REGION, DEVICE_TOKENS_TABLE

log = get_logger(__file__)

HANDLER = 'api_register_device'

dynamodb = boto3.resource('dynamodb', region_name=AWS_DEFAULT_REGION)
device_tokens_table = dynamodb.Table(DEVICE_TOKENS_TABLE)


@handle_errors(HANDLER)
def handler(event, context):
    log.info("Starting Register Device...")
    body = parse_body(event)
    require_fields(body, 'user_id', 'device_token')

    user_id = body['user_id']
    device_token = body['device_token']
    platform = body.get('platform', 'ios')

    log.info(f"Registering device for user {user_id}, platform={platform}")

    endpoint_arn = create_platform_endpoint(device_token)
    if not endpoint_arn:
        log.error(f"Failed to create SNS endpoint for user {user_id}")
        return success_response({
            "Success": False,
            "Message": "Failed to create push endpoint",
        }, status_code=500)

    now = get_iso_timestamp()
    device_tokens_table.put_item(
        Item={
            'user_id': user_id,
            'device_token': device_token,
            'endpoint_arn': endpoint_arn,
            'platform': platform,
            'created_at': now,
            'updated_at': now,
        }
    )

    log.info(f"Device registered for user {user_id}, endpoint={endpoint_arn}")
    return success_response({
        "Success": True,
        "Message": "Device registered",
    })
