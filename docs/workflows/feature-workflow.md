# Workflow: Standard Feature Build

> For single, well-scoped features. No epic breakdown needed.
> Reference with `@docs/workflows/feature-workflow.md` when relevant.

---

## The Pipeline

```
/brainstorm → /plan → /execute → /end-session
```

All artifacts land in `docs/features/[topic]/` — one folder per feature.

---

## Step 1 — Brainstorm

**When**: You have an idea but haven't committed to an approach.
**Skip if**: Approach is already decided (go straight to `/plan`).

```
/brainstorm add rate limiting to the AI completions endpoint
```

Claude will:
1. Explore approaches freely (token bucket, Redis sliding window, middleware, etc.)
2. Converge to 2-3 options with tradeoffs
3. Give a recommendation
4. Save to `docs/features/rate-limiting/BRAINSTORM.md`
5. Prompt you to pick one and `/plan` it

**You**: Pick an option, or push back, or say "go with your recommendation."

---

## Step 2 — Plan

**When**: You know what you're building and want a written plan before touching code.

```
/plan option 2 — Redis sliding window rate limiter
```

Claude will:
1. Ask 1-2 clarifying questions if scope is unclear
2. Write `docs/features/rate-limiting/PLAN.md` with:
   - Summary + approach
   - Affected files table
   - Implementation checklist
   - Risks / out of scope
   - Which agents/skills to use
3. Set status to `Draft`

**You**: Review the plan. When happy, change status to `Ready`. This is your intentional go-ahead gate.

---

## Step 3 — Execute

**When**: Plan status is `Ready`.

```
/execute rate-limiting
```

Claude will:
1. Read `docs/features/rate-limiting/PLAN.md`
2. Output a delegation table — what runs, in what order, which model
3. **Pause and wait for your go-ahead**
4. Execute step by step, ticking off checkboxes in the plan doc as it goes
5. Log progress to `docs/features/rate-limiting/EXECUTION_LOG.md`
6. Run code-reviewer on changed files when done

**You**: Review the delegation table. Say "go" or adjust before anything runs.

---

## Step 4 — End Session

```
/end-session
```

Claude will:
1. Summarize what was built and decisions made
2. Note any gotchas for next session
3. List next steps as checkboxes
4. Clear the dirty-files log

---

## What's in the Feature Folder

After a full run, `docs/features/rate-limiting/` contains:

```
docs/features/rate-limiting/
├── RESEARCH.md        ← if /research was run first (optional)
├── BRAINSTORM.md      ← options explored and decision made
├── PLAN.md            ← implementation plan (Draft → Ready → Done)
└── EXECUTION_LOG.md   ← step-by-step audit trail
```

Everything about one piece of work lives together.

---

## Example: Full Run

```
You:    /brainstorm add user-facing AI usage dashboard

Claude: [explores 4 approaches]
        [converges: polling API, SSE streaming, or static daily digest]
        [recommends SSE streaming]
        Saved: docs/features/usage-dashboard/BRAINSTORM.md

You:    go with SSE, but make it per-workspace not per-user

Claude: Got it. Run /plan when ready.

You:    /plan SSE streaming usage dashboard per workspace

Claude: [asks: should it show token count or cost or both?]

You:    both, with a 30-day rolling window

Claude: [writes docs/features/usage-dashboard/PLAN.md]
        Status: Draft — review and set to Ready when happy.

You:    [reviews plan, sets status to Ready]
        /execute usage-dashboard

Claude: ## Execution Plan: Usage Dashboard
        | Step | Action                        | Agent          | Model  |
        |------|-------------------------------|----------------|--------|
        | 1    | Create SSE endpoint           | direct         | sonnet |
        | 2    | Build dashboard component     | ts-component   | sonnet |
        | 3    | Add workspace aggregation     | direct         | sonnet |
        | 4    | Review all changes            | code-reviewer  | sonnet |
        Ready to execute?

You:    go

Claude: [executes, ticks off steps, runs review]
        Done. 4 files changed. /end-session to log.

You:    /end-session
```
