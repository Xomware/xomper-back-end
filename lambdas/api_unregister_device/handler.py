"""
POST /api/unregister-device - Unregister Device from Push Notifications
Removes device token and deletes SNS platform endpoint.

Expected body:
{
    "user_id": "abc123",
    "device_token": "apns-device-token-string"
}
"""
import boto3

from lambdas.common.logger import get_logger
from lambdas.common.errors import handle_errors
from lambdas.common.utility_helpers import success_response, parse_body, require_fields
from lambdas.common.sns_helper import delete_platform_endpoint
from lambdas.common.constants import AWS_DEFAULT_REGION, DEVICE_TOKENS_TABLE

log = get_logger(__file__)

HANDLER = 'api_unregister_device'

dynamodb = boto3.resource('dynamodb', region_name=AWS_DEFAULT_REGION)
device_tokens_table = dynamodb.Table(DEVICE_TOKENS_TABLE)


@handle_errors(HANDLER)
def handler(event, context):
    log.info("Starting Unregister Device...")
    body = parse_body(event)
    require_fields(body, 'user_id', 'device_token')

    user_id = body['user_id']
    device_token = body['device_token']

    log.info(f"Unregistering device for user {user_id}")

    # Retrieve item to get endpoint_arn before deletion
    response = device_tokens_table.get_item(
        Key={
            'user_id': user_id,
            'device_token': device_token,
        }
    )
    item = response.get('Item')

    if item:
        endpoint_arn = item.get('endpoint_arn', '')
        if endpoint_arn:
            delete_platform_endpoint(endpoint_arn)

        device_tokens_table.delete_item(
            Key={
                'user_id': user_id,
                'device_token': device_token,
            }
        )
        log.info(f"Device unregistered for user {user_id}")
    else:
        log.info(f"No device token found for user {user_id}, nothing to delete")

    return success_response({
        "Success": True,
        "Message": "Device unregistered",
    })
