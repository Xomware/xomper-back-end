# Workflow: Research (Spike)

> For when you need to understand something before you can even brainstorm.
> Reference with `@docs/workflows/research-workflow.md` when relevant.

---

## When to Use

Run a spike when:
- You're evaluating a library or tool you haven't used before
- You need to compare two or more competing options with real data
- You're integrating an external API you don't know yet
- You have an architectural question that needs investigation before committing

Skip it when:
- You already know the approach — go straight to `/brainstorm` or `/plan`
- The research would take longer than just trying it

---

## The Pipeline

```
/research [topic]     → docs/features/[topic]/RESEARCH.md
/brainstorm [topic]   → reads research doc automatically, sharper options
/plan [option]        → docs/features/[topic]/PLAN.md
/execute [feature]    → build
```

All artifacts land in the same feature folder.

---

## Step by Step

### 1. Kick off the spike

```
/research rate limiting options for our AI completions endpoint
```

The researcher agent will:
- Check what's already in the codebase (existing patterns, installed deps)
- Investigate the specific options relevant to your stack
- Produce `docs/features/rate-limiting-options/RESEARCH.md` with findings and a recommendation

### 2. Review the research doc

Read `docs/features/rate-limiting-options/RESEARCH.md`. The researcher may surface gaps or open questions — resolve those before moving on if they're decision-critical.

### 3. Brainstorm with research context

```
/brainstorm rate limiting for AI completions endpoint
```

The brainstorm agent checks for a research doc automatically. Your options will be grounded in actual findings rather than generic approaches.

### 4. Continue with standard flow

```
/plan [chosen option]
/execute [feature]
/end-session
```

---

## Feature Folder After Full Pipeline

```
docs/features/rate-limiting/
├── RESEARCH.md        ← findings from the spike
├── BRAINSTORM.md      ← options explored, decision made
├── PLAN.md            ← implementation plan
└── EXECUTION_LOG.md   ← step-by-step audit trail
```

---

## Example

```
You:        /research Elixir libraries for PDF generation

Researcher: [checks mix.exs for existing deps]
            [investigates: typst, chromic_pdf, pdf_generator, puppeteer via Port]
            [writes docs/features/elixir-pdf-generation/RESEARCH.md]

            Research complete: docs/features/elixir-pdf-generation/RESEARCH.md
            Recommendation: typst — best maintained, pure Elixir, no external deps
            Next: /brainstorm PDF generation — research context will inform the options

You:        /brainstorm PDF report generation for client exports

Brainstorm: [reads docs/features/elixir-pdf-generation/RESEARCH.md automatically]
            [produces options grounded in the research findings]
            [saves docs/features/pdf-generation/BRAINSTORM.md]
```
