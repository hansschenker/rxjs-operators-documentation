# generate

## Identity

- **Name**: generate
- **Category**: Creation Operators
- **Type**: Loop-based sequence generator — creates an Observable from a for-loop definition
- **Import**:
  ```typescript
  import { generate } from 'rxjs';
  ```
- **Signature**:
  ```typescript
  // Object config form (recommended):
  function generate<T, S>(options: GenerateOptions<T, S>): Observable<T>

  interface GenerateOptions<T, S> {
    initialState: S
    condition?: (state: S) => boolean  // loop while true; omit for infinite
    iterate:   (state: S) => S         // next state (like i++ in for-loop)
    resultSelector?: (state: S) => T   // transform state to emitted value
    scheduler?: SchedulerLike
  }

  // Positional form (legacy, still valid):
  function generate<T, S>(
    initialState: S,
    condition: (state: S) => boolean,
    iterate:   (state: S) => S,
    resultSelector?: (state: S) => T,
    scheduler?: SchedulerLike
  ): Observable<T>
  ```

## Functional Specification

`generate` is the Observable equivalent of a `for` loop:

```typescript
// Conceptual equivalent:
// for (let state = initialState; condition(state); state = iterate(state)) {
//   emit(resultSelector(state));
// }
```

It is **synchronous by default** (emits all values synchronously unless a scheduler is provided). With a scheduler, emissions are spaced across ticks.

**When to use `generate`**:
- Generate a mathematical sequence (Fibonacci, powers, ranges)
- Produce a range of values where `from([...])` would require pre-computing the array
- Lazy infinite sequences (with no `condition`)

**`generate` vs alternatives**:

| | `generate` | `range(start, count)` | `from([...])` |
|---|---|---|---|
| State | Arbitrary | Integer counter only | Pre-computed array |
| Condition | Arbitrary | Count-based | Array length |
| Memory | O(1) — no pre-allocation | O(1) | O(n) — full array |
| Readability | High (explicit loop) | Highest for integers | Familiar |

## Marble Diagram

```
generate({ initialState: 0, condition: s => s < 4, iterate: s => s + 1 }):
Sync:   (0)(1)(2)(3)|   (all emitted synchronously)

With asyncScheduler:
        --0--1--2--3--|  (one per event loop tick)

generate({ initialState: 1, iterate: s => s * 2 }):  (no condition → infinite)
        (1)(2)(4)(8)(16)...  (never completes without take())
```

## Type System Integration

```typescript
import { generate } from 'rxjs';

// State type S = number, emit type T = number (no resultSelector → T = S)
const squares$ = generate({
  initialState: 1,
  condition: s => s <= 5,
  iterate: s => s + 1,
  resultSelector: s => s * s
});
// Observable<number>

// State is an object, emit type is a string
const labels$ = generate({
  initialState: { n: 0, prefix: 'item' },
  condition: s => s.n < 3,
  iterate: s => ({ ...s, n: s.n + 1 }),
  resultSelector: s => `${s.prefix}-${s.n}`
});
// Observable<string> — emits 'item-0', 'item-1', 'item-2'
```

## Examples

### Basic Usage — Integer Range
```typescript
import { generate } from 'rxjs';

// Equivalent to range(1, 5) but more explicit
generate({
  initialState: 1,
  condition:    s => s <= 5,
  iterate:      s => s + 1
}).subscribe(console.log); // 1, 2, 3, 4, 5

// With transformation
generate({
  initialState:   1,
  condition:      s => s <= 10,
  iterate:        s => s + 1,
  resultSelector: s => s * s
}).subscribe(console.log); // 1, 4, 9, 16, 25, 36, 49, 64, 81, 100
```

### Common Pattern — Fibonacci Sequence
```typescript
import { generate } from 'rxjs';
import { take } from 'rxjs/operators';

// State holds [prev, curr]; emit curr
generate({
  initialState:   [0, 1] as [number, number],
  iterate:        ([prev, curr]) => [curr, prev + curr],
  resultSelector: ([, curr]) => curr
}).pipe(
  take(10)
).subscribe(console.log);
// 1, 1, 2, 3, 5, 8, 13, 21, 34, 55
```

### Common Pattern — Geometric Sequence
```typescript
import { generate } from 'rxjs';

// Powers of 2 up to 1024
generate({
  initialState: 1,
  condition:    s => s <= 1024,
  iterate:      s => s * 2
}).subscribe(console.log);
// 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024
```

### Common Pattern — Async Scheduler (Non-Blocking)
```typescript
import { generate, asyncScheduler } from 'rxjs';
import { take } from 'rxjs/operators';

// Spread a large sequence across event loop ticks to avoid blocking
generate({
  initialState: 0,
  condition:    s => s < 10_000,
  iterate:      s => s + 1,
  scheduler:    asyncScheduler  // one emission per tick
}).pipe(
  take(100)
).subscribe(v => heavyRender(v));
// UI stays responsive — each value processed in its own tick
```

## Common Pitfalls

### Anti-pattern: Infinite Loop Without `take`
```typescript
import { generate } from 'rxjs';

// ❌ HANGS — no condition and no take() → infinite synchronous loop
generate({
  initialState: 0,
  iterate: s => s + 1
}).subscribe(console.log); // blocks forever, never reaches next line

// ✅ CORRECT — always bound infinite generates
import { take } from 'rxjs/operators';
generate({
  initialState: 0,
  iterate: s => s + 1
}).pipe(take(100)).subscribe(console.log); // emits 0..99

// Or add a condition:
generate({
  initialState: 0,
  condition: s => s < 100,
  iterate: s => s + 1
}).subscribe(console.log);

// WHY: Without condition or take(), generate runs synchronously forever,
// freezing the JavaScript thread. Always constrain infinite generates.
```

### Anti-pattern: Using `generate` When `range` Is Clearer
```typescript
import { generate } from 'rxjs';

// ❌ VERBOSE — integer range with generate
generate({
  initialState: 0,
  condition: s => s < 10,
  iterate: s => s + 1
}).subscribe(console.log);

// ✅ SIMPLER — range() for integer sequences
import { range } from 'rxjs';
range(0, 10).subscribe(console.log); // 0..9

// WHY: range(start, count) is purpose-built for integer sequences and
// is immediately readable. Use generate only when the state or condition
// is more complex than a simple counter.
```

## Related Operators

- **`range(start, count)`**: Simpler integer range — prefer over `generate` for integer sequences
- **`interval(ms)`**: Time-based counter — infinite, regularly spaced
- **`expand`**: Recursive `mergeMap` — better for async recursive sequences (pagination, trees)
- **`from([...])`**: When the full array can be pre-computed without memory concern

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/index/function/generate](https://rxjs.dev/api/index/function/generate)

---

**Cognitive Load**: 3/5 | **Usage Frequency**: 1/5 | **Composability**: 3/5
**Key teaching points**:
1. Synchronous by default — add a `scheduler` or `take()` for async/bounded sequences
2. No `condition` = infinite — always pair with `take()` or `condition`
3. Prefer `range()` for simple integers; reach for `generate` only for non-integer state
