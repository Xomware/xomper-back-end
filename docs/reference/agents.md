# Agent Reference

Agents are specialized subprocesses that handle specific tasks. They're invoked automatically by commands or can be called directly.

## Design-Phase Agents

| Agent | Model | What it does | Invoked by |
|-------|-------|-------------|------------|
| **brainstorm** | opus | Explores solution space, converges to 2-3 options | `/brainstorm` |
| **planner** | opus | Turns ideas into structured plan docs | `/plan` |
| **orchestrator** | opus | Breaks epics into sub-feature folders + dependency order | `/orchestrate` |
| **researcher** | opus | Technical investigation — facts before decisions | `/research` |

## Execution-Phase Agents

| Agent | Model | What it does | Invoked by |
|-------|-------|-------------|------------|
| **executor** | sonnet | Executes plan docs step by step with audit trail | `/execute` |
| **code-reviewer** | sonnet | Reviews for quality, security, correctness | `/review` or auto after `/execute` |
| **debugger** | sonnet | Root cause analysis for bugs | Direct invocation |

## Knowledge Agents

| Agent | Model | What it does | Invoked by |
|-------|-------|-------------|------------|
| **compounder** | sonnet | Captures patterns into reusable solution docs | `/compound` |
| **memory-updater** | sonnet | Writes session summaries, updates Current Focus | `/end-session` |

## Meta Agents

| Agent | Model | What it does | Invoked by |
|-------|-------|-------------|------------|
| **meta-agent** | opus | Creates new agents from descriptions | Direct invocation |

## How Agents Work

- **Opus agents** handle thinking tasks (brainstorming, planning, research, architecture)
- **Sonnet agents** handle execution tasks (coding, reviewing, logging)
- Agents read from and write to `docs/features/[topic]/` — each feature gets its own folder
- The executor always shows a delegation preview before doing any work
- Agents can be overridden per-project by placing a file with the same name in `.claude/agents/`

## Adding Project-Specific Agents

Create `.claude/agents/[name].md` in your project. If the name matches a global agent, the project version takes precedence.

```markdown
---
name: agent-name
description: What this agent does and when to use it.
tools: Read, Write, Edit, Bash, Glob, Grep
model: sonnet
---

Instructions for the agent...
```
