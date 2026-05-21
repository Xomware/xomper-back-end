import os

# General
AWS_DEFAULT_REGION ='us-east-1'
AWS_ACCOUNT_ID = os.environ['AWS_ACCOUNT_ID']
PRODUCT = 'xomper'

# Headers
RESPONSE_HEADERS = {
    "Access-Control-Allow-Origin": "https://xomper.xomware.com",
    "Strict-Transport-Security": "max-age=31536000; includeSubDomains; preload",
    "Content-Type": "application/json"
}

# Logging
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

# Dynamodb
DYNAMODB_KMS_ALIAS = os.environ['DYNAMODB_KMS_ALIAS']

# Email Service
FROM_EMAIL = os.environ.get('FROM_EMAIL', 'noreply@xomper.xomware.com')
XOMPER_URL = "https://xomper.xomware.com"

# Push Notifications (SNS)
SNS_PLATFORM_APP_ARN = os.environ.get("SNS_PLATFORM_APP_ARN", "")
DEVICE_TOKENS_TABLE = os.environ.get("DEVICE_TOKENS_TABLE", "xomper-device-tokens")

# AI Review (Anthropic / Claude)
AI_REPORTS_TABLE = os.environ.get("AI_REPORTS_TABLE", "xomper-ai-reports")
AI_REVIEW_PROMPT_VERSION = os.environ.get("AI_REVIEW_PROMPT_VERSION", "v1")
AI_REVIEW_POSTDRAFT_PERIOD = "2026"
AI_REVIEW_DEFAULT_MODEL = "claude-haiku-4-5"
AI_REVIEW_MAX_TOKENS = 4000

# Admin user id for dry-run delivery (Dominick).
ADMIN_DOMINICK_USER_ID = "594625531702460416"

# LOGO URL
LOGO_URL = f"{XOMPER_URL}/assets/img/xomper-logo.jpg"
BANNER_LOGO_URL = f"{XOMPER_URL}/assets/img/xomper-banner.jpg"
