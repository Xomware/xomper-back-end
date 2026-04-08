"""
Shared test fixtures for xomper-back-end.
"""

import os
import pytest

# Set required env vars before any imports that touch constants
os.environ.setdefault("AWS_ACCOUNT_ID", "123456789012")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("DYNAMODB_KMS_ALIAS", "alias/test-key")
os.environ.setdefault("LOG_LEVEL", "DEBUG")
os.environ.setdefault("FROM_EMAIL", "noreply@xomper.xomware.com")
os.environ.setdefault("SNS_PLATFORM_APP_ARN", "arn:aws:sns:us-east-1:123456789012:app/APNS/xomper-test")
os.environ.setdefault("DEVICE_TOKENS_TABLE", "xomper-device-tokens-test")
