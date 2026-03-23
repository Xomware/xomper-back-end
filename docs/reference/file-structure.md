# File Structure Reference

Where everything lives and why.

## Project Layout

```
your-project/
├── .claude/
│   ├── CLAUDE.md              ← project context (loaded every session)
│   ├── settings.json          ← permissions and hook config
│   ├── agents/                ← project-specific agent overrides
│   ├── skills/                ← project-specific skill references
│   ├── commands/              ← project-specific commands
│   ├── rules/                 ← path-scoped rules (loaded per-file match)
│   └── memory/
│       ├── session-log.md     ← session history (gitignored)
│       └── dirty-files        ← changed files buffer (gitignored)
│
├── docs/
│   ├── features/              ← one folder per feature/initiative
│   │   ├── my-feature/
│   │   │   ├── RESEARCH.md    ← from /research (optional)
│   │   │   ├── BRAINSTORM.md  ← from /brainstorm (optional)
│   │   │   ├── PLAN.md        ← from /plan (required for /execute)
│   │   │   └── EXECUTION_LOG.md ← from /execute (auto-generated)
│   │   └── my-epic/
│   │       └── PLAN.md        ← epic plan, then /orchestrate creates sub-folders
│   │
│   ├── solutions/             ← reusable patterns from /compound
│   │   ├── auth/
│   │   ├── deployment/
│   │   └── ...
│   │
│   ├── reference/             ← system documentation (this folder)
│   │   ├── commands.md
│   │   ├── agents.md
│   │   ├── workflows.md
│   │   └── file-structure.md
│   │
│   ├── workflows/             ← how-to guides for the pipeline
│   │   ├── feature-workflow.md
│   │   ├── epic-workflow.md
│   │   └── research-workflow.md
│   │
│   └── architecture.md        ← system design decisions
│
└── [your source code]
```

## Where Does This Rule Go? (Decision Tree)

```
Is this rule universal across ALL projects?
  YES → ~/.claude/CLAUDE.md (global)
  NO  ↓

Does it only apply to certain files or directories?
  YES → .claude/rules/[name].md with paths: frontmatter
  NO  ↓

Is it a workflow you trigger on demand?
  Simple (one job)        → .claude/commands/[name].md
  Complex (multi-step)    → .claude/skills/[name]/SKILL.md
  Needs isolated context  → .claude/agents/[name].md
  ↓

Is it specific to this machine / this engineer?
  YES → CLAUDE.local.md or .claude/settings.local.json (never committed)
  NO  → .claude/CLAUDE.md (project-level, committed)
```

**Size limits**: Global CLAUDE.md ≤ 150 lines. Project CLAUDE.md ≤ 200 lines. When over, split to rules/ files or extract to skills.

---

## What Goes Where

### `docs/features/` — Work in progress and completed features

Each feature gets its own folder. The folder name is `kebab-case` and matches across all commands:
- `/brainstorm rate-limiting` → `docs/features/rate-limiting/BRAINSTORM.md`
- `/plan rate-limiting` → `docs/features/rate-limiting/PLAN.md`
- `/execute rate-limiting` → reads `docs/features/rate-limiting/PLAN.md`

Lifecycle of a feature folder:
1. Created by whichever command runs first (research, brainstorm, or plan)
2. Accumulates docs as the feature progresses
3. Stays in the repo as a record of decisions and execution

### `docs/solutions/` — Reusable patterns

Created by `/compound`. Organized by category (auth, deployment, testing, etc.). These are lessons learned that prevent repeating mistakes.

### `docs/reference/` — How the system works

Static documentation about commands, agents, and file structure. Updated when the setup changes, not during regular work.

### `docs/workflows/` — Step-by-step guides

Pipeline walkthroughs with examples. Reference these with `@docs/workflows/feature-workflow.md` when you need a refresher.

### `.claude/CLAUDE.md` — Project context

The most important file. Loaded into every Claude session. Keep it lean:
- What the project is
- Stack and versions
- Key commands
- Important file paths
- Current Focus (what's in flight)

### `.claude/rules/` — Path-scoped rules

Rules files are loaded based on which files Claude is editing. A rule with `paths:` frontmatter only loads when working on matching files — everything else loads every session.

```markdown
---
paths:
  - "src/api/**"
---
# API Conventions
- Return { data, error, meta } shape
- Validate with Pydantic/Zod at boundary
```

Use rules/ to keep CLAUDE.md under 200 lines. Move directory-specific details here.

### `.claude/memory/` — Session state (gitignored)

Personal to your machine. Tracks what happened in each session and what files were changed. Managed automatically by hooks and `/end-session`.

## What's Versioned in Git vs. What's Not

| Versioned | Not versioned (gitignored) |
|-----------|---------------------------|
| `docs/features/` | `.claude/memory/` |
| `docs/solutions/` | `.claude/.backups/` |
| `docs/reference/` | |
| `docs/workflows/` | |
| `.claude/CLAUDE.md` | |
| `.claude/settings.json` | |
