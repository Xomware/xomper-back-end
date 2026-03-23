---
paths:
  - "terraform/**"
  - "infra/**"
  - ".github/workflows/**"
  - "*.tf"
---
# Infra Rules

- Check `infra-standards` skill for full IaC conventions
- Confirm workspace/state backend before writing Terraform
- Always `terraform plan` before apply -- review output
- All resources tagged: `environment`, `project`, `owner`
- IAM: least privilege, no `*` without justification, no IAM users, OIDC only
- Secrets via secrets manager (see org.md) -- not hardcoded
- Pin container image tags -- no `latest`
- GitHub Actions: pin action versions to SHA, OIDC for AWS auth
