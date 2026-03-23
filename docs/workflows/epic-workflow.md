# Workflow: Epic Build (High-Level → Feature Plans)

> For large, multi-part work that needs to be broken into sub-features.
> Use when a single plan doc would have 10+ steps or multiple independent tracks.
> Reference with `@docs/workflows/epic-workflow.md` when relevant.

---

## The Pipeline

```
/brainstorm → /plan [epic] → /orchestrate [epic] → feature plans → /execute each → /end-session
```

The epic and all sub-features live under `docs/features/`.

---

## When to Use This vs. Standard Workflow

| Situation | Use |
|-----------|-----|
| Single feature, clear scope | `feature-workflow.md` |
| Multiple related features with dependencies | This workflow |
| Work that could be split across sessions or people | This workflow |
| Unsure — start with brainstorm and see | Brainstorm first, decide after |

---

## Step 1 — Brainstorm the Epic

```
/brainstorm AI-powered client reporting system
```

Same as standard — explore approaches, converge to 2-3, pick one.
At epic scale, options are usually architectural (monolith vs. modular, batch vs. streaming, etc.).

---

## Step 2 — Write the Epic Plan

```
/plan AI reporting system — modular, per-client, exportable
```

The epic plan (`docs/features/ai-reporting/PLAN.md`) is **high-level only**:
- What the system does and why
- The sub-features it comprises (named, not detailed)
- Dependencies between sub-features
- What "done" looks like for the whole epic

**Epic plan does NOT contain implementation steps** — those live in feature plans.

### Epic Plan Structure
```markdown
# Plan: AI Reporting System (Epic)

**Status**: Ready
**Created**: YYYY-MM-DD

## Summary
[What the system does. Who uses it. What success looks like.]

## Sub-Features
| ID  | Feature              | Depends On | Status |
|-----|----------------------|------------|--------|
| R-1 | Data aggregation     | —          | Draft  |
| R-2 | Report generation    | R-1        | Draft  |
| R-3 | Export (PDF/CSV)     | R-2        | Draft  |
| R-4 | Client delivery UI   | R-2        | Draft  |

## Out of Scope
- [what's not included]

## Definition of Done
- [ ] All sub-features at status Done
- [ ] End-to-end tested with real client data
- [ ] Reviewed by Spencer/Sara
```

---

## Step 3 — Orchestrate (Break Into Feature Plans)

```
/orchestrate ai-reporting
```

The orchestrator agent reads the epic plan and:
1. Creates a feature folder and plan stub for each sub-feature (`docs/features/ai-reporting-R1-data-aggregation/PLAN.md`, etc.)
2. Shows you the dependency order and proposed execution sequence
3. **Asks how you want to run it**: sequential, parallel where possible, or manual control
4. Waits for your go-ahead before anything executes

**You decide the execution mode per epic** — the orchestrator surfaces the options, you choose.

### Execution Mode Options
```
Sequential       — R-1 → R-2 → R-3 → R-4 (safe, predictable)
Parallel tracks  — R-1 first, then R-2+R-3 in parallel, then R-4
Manual           — I'll /execute each one myself when I'm ready
```

---

## Step 4 — Execute Feature Plans

Each feature plan runs exactly like the standard workflow:

```
/execute ai-reporting-R1-data-aggregation
```

The executor:
- Shows delegation preview for that feature
- Waits for go-ahead
- Ticks off steps in the feature plan doc
- Logs progress to `docs/features/ai-reporting-R1-data-aggregation/EXECUTION_LOG.md`
- Updates the sub-feature status in the epic plan when done

---

## Step 5 — Track Progress

The epic plan doc is your source of truth. Sub-feature statuses update as work completes:

```markdown
| ID  | Feature              | Depends On | Status      |
|-----|----------------------|------------|-------------|
| R-1 | Data aggregation     | —          | Done        |
| R-2 | Report generation    | R-1        | In Progress |
| R-3 | Export (PDF/CSV)     | R-2        | Draft       |
| R-4 | Client delivery UI   | R-2        | Draft       |
```

Run `/status` anytime for a dashboard view of all features.

---

## Step 6 — End Session

```
/end-session
```

Memory updater captures which sub-features are done, what's next, and any cross-cutting gotchas discovered during the session.

---

## Example: Full Run

```
You:      /brainstorm AI-powered client reporting system

Claude:   [explores: manual templates, AI-generated summaries, full pipeline]
          [converges to 3 options]
          [recommends: modular pipeline with AI generation + export]
          Saved: docs/features/ai-reporting/BRAINSTORM.md

You:      go with that. make it exportable to PDF.

Claude:   Run /plan when ready.

You:      /plan AI reporting — modular, AI-generated, PDF export

Claude:   [writes docs/features/ai-reporting/PLAN.md — epic plan]
          4 sub-features identified: data aggregation, report gen,
          PDF export, delivery UI. Set status to Ready when happy.

You:      [reviews, sets Ready]
          /orchestrate ai-reporting

Claude:   Epic: AI Reporting System
          Sub-feature plans created:
            docs/features/ai-reporting-R1-data-aggregation/PLAN.md
            docs/features/ai-reporting-R2-report-generation/PLAN.md
            docs/features/ai-reporting-R3-pdf-export/PLAN.md
            docs/features/ai-reporting-R4-delivery-ui/PLAN.md

          Dependency order:
            R-1 (no deps) → R-2 (needs R-1) → R-3 + R-4 (parallel, need R-2)

          How do you want to run this?
            [A] Sequential — R1 → R2 → R3 → R4
            [B] Parallel where possible — R1 → R2 → (R3 || R4)
            [C] Manual — I'll execute each myself

You:      B — parallel where possible

Claude:   Got it. Start with /execute ai-reporting-R1-data-aggregation

You:      /execute ai-reporting-R1-data-aggregation
          [delegation preview → go → executes → R-1 marked Done]

          /execute ai-reporting-R2-report-generation
          [executes → R-2 marked Done]

          /execute ai-reporting-R3-pdf-export &
          /execute ai-reporting-R4-delivery-ui
          [both run, both marked Done]

You:      /end-session
```
