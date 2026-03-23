# Org Conventions — Xomware

## GitHub
- **Org:** `Xomware`
- **User:** `domgiordano`

## Project Management
- **Tool:** GitHub Issues + XomBoard (GitHub Projects #2)
- **Board URL:** https://github.com/orgs/Xomware/projects/2
- **Board fields:** Status, App, Category, Priority
- All repos have `add-to-board.yml` workflow — new issues auto-add to XomBoard
- Update board item status as work progresses — don't just close issues
- Post completion comments on issues with summary of changes

## Branch Protection
- All repos: PRs required, no direct pushes to main/master
- No force pushes, no branch deletions on default branch
- Enforced for admins

## Secrets
- **Local dev:** `.env` files (gitignored)
- **Production:** environment variables via hosting platform
- Never commit `.env` files — use `.env.example` with placeholder values
- `BOARD_TOKEN` org secret for GitHub Projects API access

## Cloud / Infra
- **Hosting:** AWS (S3 + CloudFront) for frontends
- **IaC:** Terraform with remote state
- Keep infra simple — managed services over self-hosted

## CI/CD
- GitHub Actions for CI
- Deploy on merge to main/master (platform-specific)

## Deprecated
- OpenClaw multi-agent framework is deprecated — use Claude Code native workflows
- Do not create or reference OpenClaw patterns, dispatcher configs, or agent orchestrators
