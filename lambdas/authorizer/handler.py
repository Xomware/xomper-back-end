"""
Lambda Authorizer
=================
JWT-based API Gateway authorizer for Xomper.
"""

import jwt
from lambdas.common.constants import PRODUCT
from lambdas.common.ssm_helpers import API_SECRET_KEY
from lambdas.common.logger import get_logger

log = get_logger(__file__)

HANDLER = 'authorizer'


def generate_policy(effect: str, resource: str) -> dict:
    """Return a valid AWS IAM policy response for API Gateway."""
    return {
        'principalId': PRODUCT,
        'policyDocument': {
            'Version': '2012-10-17',
            'Statement': [
                {
                    'Action': 'execute-api:*',
                    'Effect': effect,
                    'Resource': resource
                }
            ]
        }
    }


def decode_auth_token(auth_token: str) -> dict | None:
    """Decode a JWT auth token. Returns claims dict or None on failure."""
    try:
        token = auth_token.replace('Bearer ', '')
        return jwt.decode(token, API_SECRET_KEY, algorithms=['HS256'])
    except jwt.ExpiredSignatureError:
        log.warning("Authorizer: token expired")
        return None
    except jwt.InvalidTokenError as err:
        log.warning(f"Authorizer: invalid token - {err}")
        return None


def handler(event: dict, context: object) -> dict:
    """Lambda authorizer entry point."""
    method_arn = event.get('methodArn', '')

    try:
        auth_token = event.get('authorizationToken', '')

        if not auth_token:
            log.warning("Authorizer: no authorization token provided")
            return generate_policy('Deny', method_arn)

        if not method_arn:
            log.error("Authorizer: no methodArn in event")
            return generate_policy('Deny', method_arn)

        user_details = decode_auth_token(auth_token)
        if user_details:
            arn_parts = method_arn.split(':')
            api_gateway_arn_tmp = arn_parts[5].split('/')
            resource_arn = (
                f"{arn_parts[0]}:{arn_parts[1]}:{arn_parts[2]}:"
                f"{arn_parts[3]}:{arn_parts[4]}:"
                f"{api_gateway_arn_tmp[0]}/{api_gateway_arn_tmp[1]}/*"
            )
            return generate_policy('Allow', resource_arn)

        log.warning("Authorizer: Deny - token decode failed")
        return generate_policy('Deny', method_arn)

    except Exception as err:
        log.error(f"Authorizer: unexpected error - {err}", exc_info=True)
        return generate_policy('Deny', method_arn)
