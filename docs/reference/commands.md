# Command Reference

Quick reference for all available slash commands. Run any command by typing it in Claude Code.

## Workflow Commands

| Command | What it does | When to use |
|---------|-------------|-------------|
| `/fix [description]` | Quick-fix pipeline: read → implement → test → review | Bug fixes, small changes, anything < 30 min |
| `/research [topic]` | Investigate a technology before brainstorming | Unfamiliar library, API, or architecture question |
| `/brainstorm [topic]` | Explore options, converge to 2-3 with tradeoffs | Start of a new feature when approach is unclear |
| `/plan [topic]` | Write a structured implementation plan | After brainstorm, or when approach is already clear |
| `/execute [feature]` | Act on a plan — shows preview, waits for approval | When plan status is Ready |
| `/orchestrate [epic]` | Break epic plan into sub-feature folders | Multi-feature work with dependencies |

## Code Quality Commands

| Command | What it does | When to use |
|---------|-------------|-------------|
| `/review` | Review changed code for quality, security, correctness | After implementing, before committing |
| `/test` | Detect and run test suite, diagnose failures | After changes to verify nothing broke |
| `/commit` | Stage and commit with structured message | When changes are ready to commit |
| `/pr` | Create a pull request with description | When branch is ready for review |

## Knowledge & Memory Commands

| Command | What it does | When to use |
|---------|-------------|-------------|
| `/compound [pattern]` | Capture a pattern into a reusable solution doc | After discovering a recurring issue or tricky pattern |
| `/status` | Dashboard of all features and their status | Anytime — see what's in flight |
| `/catchup` | Resume context from last session | Start of session |
| `/sync-memory` | Backfill session log from git history | When /end-session was skipped |
| `/end-session` | Log session summary, update Current Focus | End of every session |

## Setup Commands

| Command | What it does | When to use |
|---------|-------------|-------------|
| `/setup` | Interactive project setup for new devs | First time working in a project |

## Decision Tree

```
Is it a bug fix or < 30 min change?
  → /fix

Is the approach unclear?
  → /brainstorm (or /research first if tech is unfamiliar)

Do you know what to build?
  → /plan → /execute

Is it a large multi-feature effort?
  → /plan [epic] → /orchestrate → /plan each → /execute each

Done for the day?
  → /end-session
```
