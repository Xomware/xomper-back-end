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

# LOGO URL
LOGO_URL = f"{XOMPER_URL}/assets/img/xomper-logo.jpg"
BANNER_LOGO_URL = f"{XOMPER_URL}/assets/img/xomper-banner.jpg"
