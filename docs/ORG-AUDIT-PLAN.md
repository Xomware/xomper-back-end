# Xomware Org Audit & Reset Plan

> Reusable playbook for auditing each Xomware repo, cleaning up stale work, creating GitHub Issues, and connecting to the org-level project board.

**Status:** Ready
**Owner:** Dominick
**Date:** 2026-03-23

---

## Goals

1. **Single source of truth** — GitHub Issues per repo, surfaced on one org-level GitHub Projects board
2. **Kill stale work** — delete dead branches, close orphan PRs, rip out OpenClaw artifacts
3. **Extract value** — mine existing task trackers (command center, in-code TODOs) for still-valid work → GitHub Issues
4. **Clean foundation** — every repo left in a state where `main`/`master` is deployable and the issue list reflects real work

---

## Prerequisites (all completed 2026-03-23)

- [x] **GitHub token:** `gh` OAuth token has `project` scope
- [x] **Org secret:** `BOARD_TOKEN` (classic PAT with `repo` + `project`) set at org level for all repos
- [x] **Project board:** XomBoard #2 — https://github.com/orgs/Xomware/projects/2
  - Columns: `Backlog`, `Up Next`, `In Progress`, `In Review`, `Done`
  - Fields: **App** (Xomware, Xomify, Xomper, Xomcloud, XomFit, Meals, Float, Tooling), **Category** (Bug, Feature, Cleanup, Infra, Design), **Priority** (P1-Critical, P2-High, P3-Medium, P4-Low)
  - 218 stale items archived, 8 kept in Backlog
- [x] **All 25 repos made public**
- [x] **All 25 repos branch-protected** on default branch (`master`/`main`):
  - PRs required — no direct pushes
  - No force pushes, no branch deletions
  - Enforced for admins

---

## Per-Repo Audit Checklist

Run this for each repo (or app group if frontend + backend + infra exist together).

### Phase 1: Inventory

- [ ] List all branches: `gh api repos/Xomware/{repo}/branches --jq '.[].name'`
- [ ] List open PRs: `gh pr list -R Xomware/{repo} --state open`
- [ ] List open issues: `gh issue list -R Xomware/{repo} --state open`
- [ ] Check for in-code task trackers (kanban components, TODO comments, task lists)
- [ ] Check for OpenClaw artifacts (`.openclaw/`, agent configs, dispatcher scripts)
- [ ] Check for stale CI/CD (workflows referencing dead services, broken deploys)
- [ ] Note the last meaningful commit date on each non-default branch

### Phase 2: Triage

For each branch/PR/issue/in-code task, classify as:

| Decision | Action |
|----------|--------|
| **Done** | Already shipped or merged — delete branch, close issue/PR |
| **Still wanted** | Create or keep as GitHub Issue with clear description |
| **Scrapped** | No longer relevant — delete branch, close with "wontfix" label |

Rules:
- If a branch hasn't been touched in 30+ days and has no open PR, it's likely stale
- If an in-code task matches a shipped feature, it's done
- When in doubt, create the issue — it's cheap to close later

### Phase 3: Cleanup

- [ ] **Branches:** Delete all stale remote branches (`git push origin --delete {branch}`)
  - Keep only default branch + any actively in-flight feature branches
  - If branch has an open PR, check if PR is still relevant before deleting
- [ ] **PRs:** Close stale/orphan PRs with comment ("Closed during org audit — branch deleted")
- [ ] **Issues:** Triage all open issues:
  - Close OpenClaw-generated feature wishlists (label: `wontfix`)
  - Close issues for shipped/completed work
  - Keep real bugs, infra cleanup, and still-wanted features
- [ ] **Branch protection:** Verify default branch is protected (PRs required, no direct pushes, no force push, no deletions, enforced for admins). Should already be set — confirm with:
  ```bash
  gh api repos/Xomware/{repo}/branches/{branch}/protection --jq '.enforce_admins.enabled'
  ```
- [ ] Remove OpenClaw artifacts:
  - `.openclaw/` directories
  - Agent dispatcher/orchestrator configs
  - OpenClaw-specific CI workflows
  - In-app references to OpenClaw services
- [ ] Remove dead code:
  - Components/views that only existed for OpenClaw integration
  - Unused services, models, pipes that served scrapped features
- [ ] Update `.gitignore` if needed
- [ ] Verify default branch builds clean: `npm run build:prod` / equivalent

### Phase 3.5: Add Board Workflow

- [ ] Copy `.github/workflows/add-to-board.yml` into the repo (create `.github/workflows/` if needed)
- This workflow auto-adds new issues to XomBoard when they're opened
- Uses the org-level `BOARD_TOKEN` secret (already configured)

```yaml
name: Add to Project Board

on:
  issues:
    types: [opened]

jobs:
  add-to-board:
    runs-on: ubuntu-latest
    steps:
      - name: Add issue to XomBoard
        env:
          GH_TOKEN: ${{ secrets.BOARD_TOKEN }}
        run: |
          gh project item-add 2 --owner Xomware --url "${{ github.event.issue.html_url }}"
```

### Phase 4: Create Issues

For every "still wanted" item from triage:
```bash
gh issue create -R Xomware/{repo} \
  --title "{concise title}" \
  --body "## Context\n{what and why}\n\n## Acceptance\n- [ ] {criteria}" \
  --label "{type}"
```

Then add to project board:
```bash
gh project item-add 2 --owner Xomware --url {issue-url}
```

### Phase 5: Connect to Board

- [ ] Verify all new issues appear on the org project board
- [ ] Set App, Category, and Priority fields on each issue
- [ ] Move anything actively planned to "Up Next"

---

## App Groups & Audit Order

Audit related repos together. Start with the hub (this repo), then work outward.

### 1. Xomware Hub (start here)
| Repo | Notes |
|------|-------|
| `xomware-frontend` | 25 branches, 24 issues, 1 PR. Command center has task data to mine. Keep infra dashboard. Shelve pixel office. Rip out OpenClaw agent infra, Jira board views, stale command center panels. Full redesign. |
| `xomware-infrastructure` | 3 branches, 1 issue. Hosts all sites — critical, audit carefully. |

**xomware-frontend specifics:**
- Mine command center kanban/issue-board for still-valid tasks → GitHub Issues
- **Keep:** landing page, monster, infra dashboard
- **Shelve:** pixel office (fun but not priority — revisit later)
- **Remove:** command center Jira/kanban board UI, OpenClaw agent status/scene components, CI monitor, stale PR dashboard, activity log
- **Modernize:** full redesign — clean Angular 18, modern SCSS, polished UI

### 2. Xomify (Spotify app)
| Repo | Notes |
|------|-------|
| `xomify-frontend` | 21 branches, 5 issues — heavy cleanup |
| `xomify-backend` | 4 branches |
| `xomify-infrastructure` | 4 branches |
| `xomify-ios` | 6 branches |

### 3. Xomper (Sleeper API)
| Repo | Notes |
|------|-------|
| `xomper-front-end` | 2 branches — light |
| `xomper-back-end` | 3 branches |
| `xomper-infrastructure` | 4 branches |

### 4. Xomcloud (SoundCloud downloader)
| Repo | Notes |
|------|-------|
| `xomcloud-frontend` | 2 branches — light |
| `xomcloud-backend` | 1 branch — minimal |
| `xomcloud-infrastructure` | 4 branches |

### 5. XomFit (fitness tracker)
| Repo | Notes |
|------|-------|
| `xomfit-backend` | 3 branches |
| `xomfit-infrastructure` | 1 branch — clean |
| `xomfit-ios` | 2 branches, 6 issues |

### 6. Meals (meal tracking)
| Repo | Notes |
|------|-------|
| `meals-backend` | 5 branches |
| `meals-frontend` | 3 branches |
| `meals-infrastructure` | 4 branches |

### 7. Float (deals app — iOS)
| Repo | Notes |
|------|-------|
| `Float` | 30 branches, 17 issues, 5 PRs — heaviest cleanup in the org |

### 8. Claude Tooling (OpenClaw replacement)
| Repo | Notes |
|------|-------|
| `xom-claude-agents` | 8 branches, 9 issues. Evaluate what's worth keeping for Claude-native replacement. |
| `xom-claude-mcp-tools` | 4 branches |
| `xom-claude-skills` | 5 branches |
| `xom-claude-workflows` | 8 branches |
| `xom-research` | 3 branches — keep as-is, it's notes |

**Decision:** Review each repo for anything reusable with current Claude Code global config (MCP tools, useful scripts). Archive or delete the rest — the OpenClaw multi-agent dispatcher/orchestrator pattern is replaced by Claude Code native workflows.

### 9. Standalone
| Repo | Notes |
|------|-------|
| `waf` | 1 branch, 0 issues — clean Terraform module, no action needed |

---

## How to Run an Audit with Claude

For each repo (or app group), open Claude Code in that directory and run:

```
/work-issue or direct prompt:
"Audit this repo using the plan at docs/ORG-AUDIT-PLAN.md.
Run Phases 1-3 (inventory, triage, cleanup).
Present triage decisions before executing cleanup.
Then create GitHub Issues for Phase 4."
```

For app groups, clone all related repos locally and reference them together.

---

## Decisions (Resolved)

- [x] **Pixel office:** Shelved — remove from active codebase, revisit later
- [x] **Claude tooling repos:** Review for anything reusable with current Claude Code setup (global config, MCP tools), archive/trash the rest (OpenClaw dispatcher, orchestrator, multi-agent framework)
- [x] **Meals repos:** Separate app — audit independently from XomFit
- [x] **Frontend modernization scope:** Full redesign for every app frontend
