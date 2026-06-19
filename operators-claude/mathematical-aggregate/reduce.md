# reduce

## Identity
- **Name**: reduce
- **Category**: Mathematical/Aggregate Operators
- **Type**: Terminal accumulator — accumulates all source values into a single result, emits once on source completion
- **Import**:
  ```typescript
  import { reduce } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  // With seed — V (source) and A (accumulator) may differ
  function reduce<V, A>(
    accumulator: (acc: A, value: V, index: number) => A,
    seed: A
  ): OperatorFunction<V, A>

  // Without seed — A = V; uses first value as initial accumulator
  function reduce<V>(
    accumulator: (acc: V, value: V, index: number) => V
  ): OperatorFunction<V, V>
  ```

## Functional Specification

**Input**: `Observable<V>` — a source that must complete for `reduce` to emit

**Output**: `Observable<A>` — emits exactly one value (the final accumulated result) when the source completes

**Transformation**: Identical to `Array.prototype.reduce` — applies the accumulator function to each source value, threading the result through. Unlike `scan`, does not emit intermediate states. Emits a single final value followed by completion.

**Relationship to `scan`**:
```
scan emits on EVERY source value  (N source values → N output values)
reduce emits ONLY on completion   (N source values → 1 output value)

of(1, 2, 3, 4).pipe(scan((acc, v) => acc + v, 0))
  // emits: 1, 3, 6, 10

of(1, 2, 3, 4).pipe(reduce((acc, v) => acc + v, 0))
  // emits: 10  (once, at completion)
```

**Mathematical representation**:
```
Let S = v₁, v₂, ..., vₙ (source must complete)
Let f = accumulator function
Let a₀ = seed

aₙ = f(f(...f(f(a₀, v₁, 0), v₂, 1)..., vₙ₋₁, n-2), vₙ, n-1)

Output: aₙ  ++ complete()   (single emission at source completion)

Without seed: v₁ is a₀; accumulator first called at index 1.
If source is empty with seed:    output = seed ++ complete()
If source is empty without seed: output = complete() (no emission)
```

**Invariants**:
- **Single emission**: Always emits at most once — the final accumulated value
- **Completion required**: If source never completes, `reduce` never emits
- **Synchronous accumulation per emission**: Accumulator called synchronously on each source value
- **Stateful**: Holds current accumulated value in memory

## Marble Diagram

```
Source:   --1--2--3--4--|
          reduce((acc, v) => acc + v, 0)
Result:   ---------------10|

Accumulation happens silently for each value.
Single emission '10' at source completion.
```

**Contrast with `scan`**:
```
Source:   --1--2--3--|
scan:     --1--3--6--|    (emits after each value)
reduce:   -----------6|  (emits only at completion)
```

**Never-completing source — reduce hangs**:
```
Source:   --1--2--3--4--...  (interval — never completes)
          reduce((acc, v) => acc + v, 0)
Result:   (nothing ever emitted)
```

**Key observation**: `reduce` is the right tool when you need a summary statistic of a completed, bounded data stream. Use `scan` when downstream consumers need to react to each accumulation step. Use `reduce` when only the final answer matters.

## Behavioral Characteristics

**Subscription**:
- Subscribes to source immediately; accumulates silently
- Holds one internal value: the current accumulated state

**Completion semantics**:
- Source completion → emit accumulated value → output completes
- Empty source with seed → emit seed → output completes
- Empty source without seed → output completes without emitting

**Error handling**:
- Source error propagates immediately; accumulated state discarded
- Accumulator function error propagates immediately

**Backpressure**:
- None — O(1) state (current accumulator value only)

**Hot vs. Cold**:
- Almost exclusively used with cold, completing sources
- With a hot source: `reduce` waits forever unless the hot source is bounded with `take`, `takeUntil`, etc.

## Type System Integration

```typescript
/**
 * Type Parameters:
 *   V - Source value type
 *   A - Accumulator (result) type — may differ from V when seed is provided
 *
 * Input Type:  Observable<V>
 * Output Type: Observable<A>
 *
 * Same type inference rules as scan:
 *   With seed:    A inferred from seed type
 *   Without seed: A = V
 *
 * reduce is often used to aggregate to a type different from the source
 * (e.g., Observable<string> → Observable<string[]>)
 */

import { from, of } from 'rxjs';
import { reduce } from 'rxjs/operators';

// V = number, A = number (same type, with seed)
from([1, 2, 3, 4, 5]).pipe(
  reduce((acc: number, v: number) => acc + v, 0)
).subscribe((total: number) => console.log(total)); // 15

// V = string, A = string[] (different types)
from(['a', 'b', 'c']).pipe(
  reduce((acc: string[], v: string) => [...acc, v.toUpperCase()], [] as string[])
).subscribe((arr: string[]) => console.log(arr)); // ['A', 'B', 'C']

// V = LogEntry, A = Map<string, number> (complex aggregation)
interface LogEntry { level: 'info' | 'warn' | 'error'; message: string; }

from(logEntries).pipe(
  reduce(
    (counts: Map<string, number>, entry: LogEntry) =>
      counts.set(entry.level, (counts.get(entry.level) ?? 0) + 1) && counts,
    new Map<string, number>()
  )
).subscribe((counts: Map<string, number>) => reportCounts(counts));
```

## Examples

### Basic Usage — Sum, Count, Min/Max
```typescript
import { from } from 'rxjs';
import { reduce } from 'rxjs/operators';

const numbers$ = from([3, 1, 4, 1, 5, 9, 2, 6]);

// Sum
numbers$.pipe(reduce((acc, v) => acc + v, 0)).subscribe(console.log);
// Output: 31

// Count
numbers$.pipe(reduce(count => count + 1, 0)).subscribe(console.log);
// Output: 8

// Max (without seed — first value is initial max)
numbers$.pipe(reduce((max, v) => Math.max(max, v))).subscribe(console.log);
// Output: 9

// Min
numbers$.pipe(reduce((min, v) => Math.min(min, v))).subscribe(console.log);
// Output: 1
```

### Common Pattern — Aggregating HTTP Response Arrays
```typescript
import { from } from 'rxjs';
import { mergeMap, reduce, toArray } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

interface Order { userId: number; total: number; items: string[]; }

// Fetch orders, compute user totals
ajax.getJSON<Order[]>('/api/orders').pipe(
  mergeMap(orders => from(orders)),
  reduce(
    (totals: Map<number, number>, order: Order) =>
      totals.set(order.userId, (totals.get(order.userId) ?? 0) + order.total) && totals,
    new Map<number, number>()
  )
).subscribe(totals => renderUserTotals(totals));
```

### Common Pattern — Building a Lookup Table
```typescript
import { from } from 'rxjs';
import { reduce } from 'rxjs/operators';

interface User { id: number; name: string; email: string; }

// Array of users → dictionary by ID
from(users).pipe(
  reduce(
    (map: Record<number, User>, user: User) => ({ ...map, [user.id]: user }),
    {} as Record<number, User>
  )
).subscribe(userMap => {
  console.log(userMap[42]); // O(1) lookup
});
```

### Common Pattern — `reduce` vs `scan` — Choosing the Right Tool
```typescript
import { Subject } from 'rxjs';
import { reduce, scan, startWith, take } from 'rxjs/operators';

// Use scan when: downstream needs to react to each accumulation step
const actions$ = new Subject<Action>();

const liveState$ = actions$.pipe(
  scan(reducer, initialState), // ← each action → new state emitted immediately
  startWith(initialState)      // ← subscribers get initial state on subscription
);
// UI components can bind to liveState$ and re-render on every action

// Use reduce when: only the final result matters (batch processing)
const batchEvents$ = from(eventLog); // finite stream of historical events

batchEvents$.pipe(
  reduce(reducer, initialState) // ← accumulate all events; emit final state once
).subscribe(finalState => saveCheckpoint(finalState));
// No intermediate states needed; result written once at end

// Equivalence: scan + last() ≡ reduce()  (functionally, not idiomatically)
batchEvents$.pipe(
  scan(reducer, initialState),
  last() // ← emits only final value, same as reduce
).subscribe(console.log);
// Output same as reduce — but scan emits all intermediates (wasted work)
```

### Edge Cases — Empty Source, Without Seed, Index Parameter
```typescript
import { EMPTY, of } from 'rxjs';
import { reduce } from 'rxjs/operators';

// Edge case 1: empty source with seed — emits seed, then completes
EMPTY.pipe(
  reduce((acc, v: number) => acc + v, 0)
).subscribe({ next: v => console.log(v), complete: () => console.log('done') });
// Output: 0, done

// Edge case 2: empty source without seed — completes without emitting
EMPTY.pipe(
  reduce((acc: number, v: number) => acc + v)
).subscribe({ next: v => console.log(v), complete: () => console.log('done') });
// Output: done  (no value!)

// Edge case 3: index parameter
of('a', 'b', 'c').pipe(
  reduce((acc, v, i) => [...acc, `${i}:${v}`], [] as string[])
).subscribe(console.log);
// Output: ['0:a', '1:b', '2:c']

// Edge case 4: single value with seed
of(42).pipe(
  reduce((acc, v) => acc + v, 10)
).subscribe(console.log);
// Output: 52
```

## Common Pitfalls

### Anti-pattern: Using `reduce` on a Never-Completing Source
```typescript
import { interval, Subject } from 'rxjs';
import { reduce, take, scan, shareReplay, startWith } from 'rxjs/operators';

// ❌ BROKEN — interval never completes; reduce never emits
interval(100).pipe(
  reduce((acc, v) => acc + v, 0)
).subscribe(console.log);
// No output ever. reduce waits for completion.

// ✅ CORRECT option 1 — make the source finite with take()
interval(100).pipe(
  take(10),             // completes after 10 values
  reduce((acc, v) => acc + v, 0)
).subscribe(console.log);
// Output: 45  (0+1+2+...+9)

// ✅ CORRECT option 2 — use scan when you want running totals from an infinite source
interval(100).pipe(
  scan((acc, v) => acc + v, 0)
).subscribe(console.log);
// Emits running sum: 0, 1, 3, 6, 10, ...

// WHY: reduce is only appropriate for bounded, completing sources.
// If you want to aggregate an infinite stream, use scan to emit running results,
// or bound the stream with take/takeUntil/takeWhile first.
```

### Anti-pattern: Accumulating Mutable State
```typescript
import { from } from 'rxjs';
import { reduce } from 'rxjs/operators';

interface Product { id: number; category: string; price: number; }

// ❌ INCORRECT — mutating the accumulator; same reference returned each time
from(products).pipe(
  reduce((groups: Record<string, Product[]>, p: Product) => {
    if (!groups[p.category]) groups[p.category] = [];
    groups[p.category].push(p); // mutates the object
    return groups;              // same reference
  }, {})
).subscribe(groups => renderGroups(groups));
// Functionally works for reduce (single final value), but
// if this were scan, downstream operators comparing by reference would break.

// ✅ CORRECT — return a new object on each accumulation step
from(products).pipe(
  reduce((groups: Record<string, Product[]>, p: Product) => ({
    ...groups,
    [p.category]: [...(groups[p.category] ?? []), p]
  }), {} as Record<string, Product[]>)
).subscribe(groups => renderGroups(groups));

// WHY: While mutation technically works for reduce (single final emission),
// using immutable updates is the correct pattern — it makes the reducer
// portable to scan and prevents accidental aliasing bugs.
```

### Anti-pattern: Using `scan + last()` When `reduce` Is Available
```typescript
import { from } from 'rxjs';
import { scan, last, reduce } from 'rxjs/operators';

// ❌ VERBOSE — scan emits all intermediates; last() discards them
from([1, 2, 3, 4, 5]).pipe(
  scan((acc, v) => acc + v, 0), // emits 1, 3, 6, 10, 15 — 4 intermediates wasted
  last()                         // takes only 15
).subscribe(console.log);

// ✅ CORRECT — reduce is the idiomatic choice when only the final value matters
from([1, 2, 3, 4, 5]).pipe(
  reduce((acc, v) => acc + v, 0) // accumulates silently; emits 15 once
).subscribe(console.log);

// WHY: scan + last() is functionally equivalent to reduce but allocates and
// emits intermediate values that are immediately discarded. reduce makes the
// intent clear ("I only want the final result") and avoids unnecessary work.
```

## Related Operators

**Same Category (Mathematical/Aggregate)**:
- **`scan`**: Running accumulator — emits after every source value; use when intermediate states are needed
- **`count`**: Counts source emissions — equivalent to `reduce(() => acc + 1, 0)` but explicit
- **`min` / `max`**: Minimum/maximum of source values on completion
- **`toArray`**: Collects all source values into an array on completion — equivalent to `reduce((arr, v) => [...arr, v], [])`

**Complementary Operators**:
- **`take(n)`**: Bound an infinite source before passing to `reduce`
- **`mergeMap` / `from`**: Flatten arrays from HTTP responses before reducing

**Alternatives by Use Case**:

| Use Case | Instead of `reduce` | Use This | Why |
|----------|---------------------|----------|-----|
| Running totals from infinite stream | `reduce` | `scan` | scan emits intermediate values; reduce waits for completion |
| Collect all into array | `reduce((a, v) => [...a, v], [])` | `toArray()` | toArray is the idiomatic shorthand |
| Count emissions | `reduce((c) => c + 1, 0)` | `count()` | Explicit intent |
| Final value without aggregation | `reduce((_, v) => v)` | `last()` | last() is the canonical "final value" operator |

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/reduce](https://rxjs.dev/api/operators/reduce)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/operators/reduce.html](http://reactivex.io/documentation/operators/reduce.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/reduce.ts](https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/reduce.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Terminal Batch Aggregator
- **Cognitive Load**: 2/5 — Identical semantics to Array.reduce; the main teaching point is the scan vs. reduce choice and the "never completes → never emits" consequence
- **Usage Frequency**: 3/5 — Less frequent than scan in reactive UIs; essential for batch processing and data pipeline aggregation
- **Composability**: 4/5 — Works cleanly with take, from, mergeMap for data pipeline composition

**Teaching Sequence**:
- **Prerequisites**: `scan` (contrast is essential), Observable completion semantics
- **Teaches**: Terminal vs. running accumulation, scan/reduce decision, the completion requirement
- **Common with**: `take`, `from`, `mergeMap`, `toArray`, `scan`
