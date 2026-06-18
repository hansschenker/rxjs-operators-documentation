# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
npm run docs:dev       # Start VitePress dev server
npm run docs:build     # Build static site
npm run docs:preview   # Preview built site
```

No test framework is configured. There is no lint command.

## Architecture

This is a VitePress documentation site for RxJS operators. The actual VitePress config and home page live in `.vitepress/` (not the project root), so the dev server serves from there.

**Key directories:**
- `operators-claude/` — all operator documentation, organized by category (transformation/, combination/, etc.)
- `docs/operator-trees/` — learning path decision trees and operator categorization overviews
- `.vitepress/.vitepress/config.mts` — site config (nav, sidebar, theme)
- `SKILL.md` — the complete eight-policy framework specification; read this before adding or editing any operator doc

## Eight-Policy Framework

Every operator document must cover all 8 policies in order. `SKILL.md` is the authoritative spec; the existing docs in `operators-claude/` are the canonical examples.

1. **Identity** — name, category (one of 17 groups), type, import path, TypeScript signature
2. **Functional Specification** — input/output types, transformation rules, mathematical representation, invariants
3. **Marble Diagrams** — ASCII temporal visualization (`-` = 10ms, letters = values, `|` = completion, `#` = error, `✂️` = cancellation)
4. **Behavioral Characteristics** — subscription mechanics, completion semantics, error handling, backpressure, hot/cold
5. **Type System Integration** — generics, type narrowing, compile-time guarantees
6. **Practical Examples** — minimum 3 (basic → common pattern → edge case), all runnable with expected output in comments
7. **Common Pitfalls** — anti-patterns with corrections using `❌ INCORRECT` / `✅ CORRECT` / `WHY:` format
8. **Related Operators** — same-category operators, complementary operators, alternatives by use case

## Content Conventions

**Marble diagram format:**
```
Source:   --1-----2-----|
            |     |
operator(config)
Result: ----a--b---|
```

**Anti-pattern format:**
```typescript
// ❌ INCORRECT
// problematic code

// ✅ CORRECT
// correct approach
// WHY: explanation
```

**Code examples** must include explicit imports, be complete and runnable, and demonstrate TypeScript type safety.

Each operator doc should include cognitive load (1–5), usage frequency (1–5), composability factor (1–5), and teaching sequence placement for educational metadata.

## Operator Categories (17 groups)

Creation, Transformation, Filtering, Combination, Multicasting, Error Handling, Utility, Conditional, Mathematical/Aggregate, Join Creation, Join, Rate Limiting, Higher-Order, Connectable Observable, Testing/Debugging, Interop, Subject/Notification.

New operator docs go in `operators-claude/<category>/` named `<operatorName>.md` or `<operatorName>-operator-documentation.md`.
