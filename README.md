# Xomper Back End

Serverless Python backend for Xomper fantasy football, running on AWS Lambda behind API Gateway.

## Architecture

| Service             | Purpose                                |
| ------------------- | -------------------------------------- |
| API Gateway         | REST API with JWT authorizer           |
| Lambda              | Python 3.10 handlers                   |
| DynamoDB            | Data persistence (KMS encrypted)       |
| SES                 | Transactional email delivery           |
| SSM Parameter Store | Secrets management                     |
| Lambda Layers       | Shared code (`xomper-shared-packages`) |

**Region:** `us-east-1`

## Project Structure

```
lambdas/
├── authorizer/          # JWT token validation for API Gateway
├── email_rule_proposal/ # POST /email/rule-proposal
├── email_rule_accept/   # POST /email/rule-accept
├── email_rule_deny/     # POST /email/rule-deny
├── email_taxi/          # POST /email/taxi
└── common/              # Shared layer code
    ├── constants.py         # Config & env vars
    ├── logger.py            # XomperLogger (singleton, per-module child loggers)
    ├── errors.py            # Exception hierarchy & @handle_errors decorator
    ├── dynamo_helpers.py    # DynamoDB CRUD operations
    ├── ses_helper.py        # SES email sending (concurrent via asyncio)
    ├── sleeper_helper.py    # Sleeper.app API integration
    ├── ssm_helpers.py       # SSM Parameter Store access
    ├── utility_helpers.py   # JSON encoding, request parsing, validation
    └── email_templates/     # HTML email templates (table-based, inline CSS)
        ├── base.py              # Shared header, footer, components
        ├── rule_proposed.py
        ├── rule_accepted.py
        ├── rule_denied.py
        ├── taxi_steal_league.py
        └── taxi_steal_owner.py
```

## API Endpoints

### Email Notifications

All email endpoints send concurrently via `asyncio.to_thread` + `asyncio.gather`.

**POST /email/rule-proposal** - Notify league of new rule proposal

```json
{
  "proposal": {
    "title": "Allow IR stashing",
    "description": "Players on IR can be stashed...",
    "proposed_by_username": "Dom",
    "league_name": "The Dynasty League"
  },
  "recipients": ["email1@example.com", "email2@example.com"]
}
```

**POST /email/rule-accept** - Notify league of approved rule

```json
{
  "proposal": {
    "title": "...",
    "description": "...",
    "proposed_by_username": "...",
    "league_name": "..."
  },
  "approved_by": ["Dom", "Steve", "Mike"],
  "rejected_by": ["Jake"],
  "recipients": ["email1@example.com"]
}
```

**POST /email/rule-deny** - Notify league of denied rule (same shape as rule-accept)

**POST /email/taxi** - Notify league + owner of taxi squad steal

```json
{
  "stealer": { "display_name": "Dom" },
  "player": {
    "first_name": "John",
    "last_name": "Johnson",
    "position": "RB",
    "team": "NYG"
  },
  "owner": { "display_name": "Steve", "email": "steve@example.com" },
  "recipients": ["email1@example.com"],
  "league_name": "The Dynasty League"
}
```

All email endpoints return:

```json
{ "successfulEmails": 5, "failedEmails": 0 }
```

## Auth

JWT-based authorization via API Gateway Lambda authorizer.

1. Client sends `Authorization: Bearer <JWT_TOKEN>` header
2. Authorizer decodes token using HS256 with secret from SSM (`/xomper/api/API_SECRET_KEY`)
3. Valid token -> Allow policy, invalid/expired -> Deny policy

## Environment Variables

| Variable             | Required | Default                      | Description                       |
| -------------------- | -------- | ---------------------------- | --------------------------------- |
| `AWS_ACCOUNT_ID`     | Yes      | -                            | AWS account ID                    |
| `DYNAMODB_KMS_ALIAS` | Yes      | -                            | KMS alias for DynamoDB encryption |
| `LOG_LEVEL`          | No       | `INFO`                       | Logging level                     |
| `FROM_EMAIL`         | No       | `noreply@xomper.xomware.com` | SES sender address                |

## SSM Parameters

| Key                          | Description                    |
| ---------------------------- | ------------------------------ |
| `/xomper/api/API_SECRET_KEY` | JWT signing secret (encrypted) |

## Handler Pattern

All handlers follow the same structure:

```python
from lambdas.common.logger import get_logger
from lambdas.common.errors import handle_errors
from lambdas.common.utility_helpers import success_response, parse_body, require_fields

log = get_logger(__file__)
HANDLER = 'handler_name'

@handle_errors(HANDLER)
def handler(event, context):
    body = parse_body(event)
    require_fields(body, 'field1', 'field2')
    # ... business logic ...
    return success_response({...}, is_api=False)
```

The `@handle_errors` decorator catches all exceptions, logs with sensitive data masking, and returns formatted error responses.

## Error Handling

Custom exception hierarchy in `errors.py`:

| Exception            | HTTP Status |
| -------------------- | ----------- |
| `ValidationError`    | 400         |
| `AuthorizationError` | 401         |
| `NotFoundError`      | 404         |
| `DynamoDBError`      | 500         |
| `EmailError`         | 500         |
| `SleeperAPIError`    | 502         |

## Deployment

CI/CD via GitHub Actions (`.github/workflows/deploy-backend.yml`), triggered on push to `master`:

1. Detects changed files between commits
2. Runs `pytest` on changed lambdas
3. If `lambdas/common/` changed: publishes new shared layer, updates all lambdas
4. Deploys changed lambda code via `aws lambda update-function-code`

Lambda naming convention: `xomper-{function-name}` (underscores become hyphens)

## Dependencies

See `requirements.txt`. Key packages:

- `PyJWT` - JWT token handling
- `pydantic` - Data validation
- `requests` - HTTP client (Sleeper API)
- `jwcrypto` - Cryptographic operations
- `boto3` - AWS SDK (provided by Lambda runtime)
