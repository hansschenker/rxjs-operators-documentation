# Flattening Strategy Guide

Choosing between `mergeMap`, `switchMap`, `concatMap`, and `exhaustMap` is one of the most important decisions in RxJS. Each answers a different question about what to do with the **previous inner Observable** when a **new source value** arrives.

---

## The Decision Question

> When a new source value arrives while an inner Observable is still active — what should happen to the active inner?

| Strategy | Answer | Operator |
|---|---|---|
| **Run both** | Start a new inner; keep the old one running | `mergeMap` |
| **Queue it** | Finish the current inner; then start the next | `concatMap` |
| **Switch to it** | Cancel the current inner; start the new one | `switchMap` |
| **Ignore it** | Stay with the current inner; drop the new value | `exhaustMap` |

---

## Quick Selection Guide

```
Does order of results matter?
├── Yes → Does every source emission need to be processed?
│          ├── Yes → concatMap  (sequential, queued, ordered)
│          └── No  → exhaustMap (ignore while busy)
│
└── No  → Does only the LATEST result matter?
           ├── Yes → switchMap  (cancel stale, use latest)
           └── No  → mergeMap   (all concurrent, unordered)
```

---

## `mergeMap` — Concurrent, Unordered

**Behavior**: Every source emission immediately starts a new inner Observable. All run concurrently. Output order matches arrival order, not source order.

**Use when**:
- Results are independent and order doesn't matter
- You want maximum throughput (parallelism)
- Each item needs to be processed, and they don't interfere

**Classic use cases**: Parallel HTTP requests, batch processing independent items, parallel file operations

```typescript
// Fetch all users in parallel — results arrive as HTTP responses complete
from(userIds).pipe(
  mergeMap(id => ajax.getJSON(`/api/users/${id}`))
).subscribe(user => render(user));
```

**Watch out for**: Non-deterministic output order; infinite inner Observables accumulating; unbounded concurrency (add `concurrent` parameter).

---

## `concatMap` — Sequential, Ordered, Queued

**Behavior**: Each new inner starts only after the previous one completes. Source emissions that arrive while an inner is active are queued. Output preserves source order.

**Use when**:
- Order of results must match source order
- Operations must not overlap (e.g., sequential file writes, ordered API calls)
- Every emission must be processed — nothing can be dropped

**Classic use cases**: Sequential animations, ordered API writes, upload queues, transaction logs

```typescript
// Process messages in order — each waits for the previous to confirm
messageQueue$.pipe(
  concatMap(msg => sendMessage(msg)) // next send waits for current ACK
).subscribe(ack => markDelivered(ack));
```

**Watch out for**: Queue growth if source is faster than inner completes; if any inner never completes, the entire queue stalls.

---

## `switchMap` — Latest-Wins, Cancels Previous

**Behavior**: When a new source value arrives, the current inner Observable is **cancelled** (unsubscribed). Only the most recently started inner runs. Output may skip values from stale inners.

**Use when**:
- Only the result for the LATEST value is relevant
- Stale results from previous values should be discarded
- The inner Observable represents a "current state" query

**Classic use cases**: Search-as-you-type, live navigation, autocomplete, "refresh on change" patterns

```typescript
// Each new search cancels the in-flight request for the previous query
searchQuery$.pipe(
  debounceTime(300),
  switchMap(query => ajax.getJSON(`/api/search?q=${query}`))
).subscribe(results => renderResults(results));
```

**Watch out for**: Not suitable when every result matters (use `concatMap` or `mergeMap` then). An inner that synchronously completes before the next source emission is never cancelled — only async work benefits from cancellation.

---

## `exhaustMap` — Ignore-While-Busy

**Behavior**: While an inner Observable is active, new source emissions are **silently dropped**. The running inner is never cancelled and nothing is queued.

**Use when**:
- The current operation must not be interrupted
- Duplicate/excess requests should be ignored, not queued
- "Debounce at the action level" — process one at a time, drop extras

**Classic use cases**: Submit buttons (prevent double-submit), login forms, polling that must not overlap, save operations

```typescript
// First click starts the save; subsequent clicks are ignored until save completes
saveButton$.pipe(
  exhaustMap(() => saveFormData())
).subscribe(result => showSuccessToast());
```

**Watch out for**: Silent dropping — if the user's repeated clicks represent intent (not accident), they'll be confused. Use `concatMap` if you want to queue the retries instead.

---

## Side-by-Side Marble Comparison

```
Source:   --A-----B--C------D--|
Inner:    each takes ~3 frames to complete

mergeMap:   --A1A2--B1C1A3B2C2--|   (all concurrent, mixed)
concatMap:  --A1A2-----B1B2-C1C2--D1D2--|  (sequential queue)
switchMap:  -----B1--C1C2------D1D2--|  (A cancelled by B; B cancelled by C)
exhaustMap: --A1A2----C1C2------D1D2--|  (B dropped — A was active; D processes)
```

---

## The Higher-Order Equivalents (`*All` operators)

Each `*Map` operator has a `*All` counterpart for when you already have an `Observable<Observable<T>>` (no projection needed):

| `*Map` | `*All` |
|---|---|
| `mergeMap(fn)` | `mergeAll()` |
| `concatMap(fn)` | `concatAll()` |
| `switchMap(fn)` | `switchAll()` |
| `exhaustMap(fn)` | `exhaustAll()` |

```typescript
// These are equivalent:
source$.pipe(mergeMap(v => project(v)))
source$.pipe(map(v => project(v)), mergeAll())
```

---

## Common Mistakes

| Mistake | Symptom | Fix |
|---|---|---|
| `mergeMap` for search | Old results overwrite new ones | `switchMap` |
| `switchMap` for ordered processing | Results arrive out of order or skipped | `concatMap` |
| `concatMap` for parallel requests | Slow — each waits for previous | `mergeMap` |
| `mergeMap` for submit button | Multiple submissions on double-click | `exhaustMap` |
| `exhaustMap` for keystroke handler | Keystrokes silently lost | `concatMap` |

---

## Decision Flowchart (Text)

```
Is every source emission important?
├── No (can drop some) ─────────────────────────────────────────┐
│                                                                 │
│   Does only the latest matter?                                  │
│   ├── Yes → switchMap                                           │
│   └── No  → exhaustMap (ignore while busy)                     │
│                                                                 │
└── Yes (must process all) ──────────────────────────────────────┐
                                                                  │
    Does output order need to match source order?                 │
    ├── Yes → concatMap                                           │
    └── No  → mergeMap (add concurrent= for resource control)     │
```
