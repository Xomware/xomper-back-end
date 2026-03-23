---
paths:
  - "src/components/**"
  - "src/app/**"
  - "*.tsx"
  - "*.jsx"
---
# Frontend Rules

- Check `frontend-standards` skill for full frontend conventions
- Server Components by default -- only `"use client"` when state/effects/browser APIs needed
- Use shadcn/ui primitives before building custom components
- Check for `.interface-design/system.md` or scan existing components for design patterns
- All interactive states: default, hover, focus, active, disabled, loading, error, empty
- Accessibility: semantic HTML, `<label>` on inputs, visible focus rings, keyboard nav
- No `any` types -- TypeScript strict
- Run lint/test commands after changes
