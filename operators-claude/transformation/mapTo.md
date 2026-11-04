# mapTo

## Identity
- **Name**: mapTo
- **Category**: Transformation Operators
- **Type**: Constant projection (1-to-1 transformation) — Deprecated in RxJS 7, removed in RxJS 8. Replaces every source value with the same fixed value
- **Import**:
  ```typescript
  // Deprecated — still exported in RxJS 7
  import { mapTo } from 'rxjs/operators';

  // Replacement
  import { map } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  /** @deprecated Use map(() => value) instead. */
  function mapTo<R>(value: R): OperatorFunction<unknown, R>

  // Older two-parameter overload, also deprecated:
  function mapTo<T, R>(value: R): OperatorFunction<T, R>
  ```

## Functional Specification

**Input**: `Observable<T>` — any source; its values are ignored and only their timing matters

**Output**: `Observable<R>` — emits `value` once for every source emission

**Transformation**:
`mapTo(value)` is exactly `map(() => value)`. The constant is captured once, when the pipeline is constructed, and the *same reference* is emitted on every source notification. Cardinality is preserved: N inputs produce N outputs. Completion and errors pass through untouched.

**Mathematical representation**:
```
mapTo(c)(S) = { c, c, c, … }   with |mapTo(c)(S)| = |S|

Formally: mapTo(c) ≡ map(_ => c)
          ∀ vᵢ ∈ S → c ∈ Output, at the same time as vᵢ
```

**Invariants**:
- **1-to-1 cardinality**: Every source emission yields exactly one output emission
- **Timing preservation**: Output emissions are synchronous with their source emissions
- **Referential constancy**: Every emission is the identical value (`===`) supplied at construction time
- **Notification transparency**: Complete and error notifications are forwarded unchanged

## Marble Diagram

```
Source:   --a----b----c----d--|
          mapTo('X')
Result:   --X----X----X----X--|

Each source value becomes 'X'; timing and completion are preserved.
```

**Source errors**:
```
Source:   --a----b--#
          mapTo('X')
Result:   --X----X--#
```

**Empty source**:
```
Source:   -----|
          mapTo('X')
Result:   -----|          (nothing to map — no emissions)
```

**Key observation**: `mapTo` carries no information from the source; it turns a stream of *values* into a stream of *events*. That is exactly the job `map(() => value)` does with one more character of intent, which is why the dedicated operator was retired.

## Behavioral Characteristics

**Subscription**:
- Subscribes to the source immediately; no scheduling, no inner Observables
- The constant is evaluated once at pipe construction, not per subscription and not per emission

**Completion semantics**:
- Completes when the source completes
- Never completes early and never adds emissions of its own

**Error handling**:
- Source errors propagate unchanged
- Cannot throw on its own — there is no projection function to fail

**Backpressure**:
- None — O(1), synchronous pass-through

**Hot vs. Cold**:
- No effect on temperature: cold stays cold, hot stays hot
- A shared mutable `value` is shared across *all* subscribers of a hot source as well as across emissions

## Type System Integration

```typescript
/**
 * Type Parameters:
 *   R - The constant's type; the source type is accepted as `unknown`
 *
 * Input Type:  Observable<unknown>  (any source)
 * Output Type: Observable<R>
 *
 * Type Narrowing:
 *   Literal types are preserved: mapTo('tick') yields Observable<'tick'>,
 *   mapTo(1 as const) yields Observable<1>. map(() => 'tick') infers the
 *   same literal type, so the replacement loses nothing.
 */

import { fromEvent } from 'rxjs';
import { mapTo, map } from 'rxjs/operators';

// Deprecated
const legacy$: Observable<'click'> = fromEvent(button, 'click').pipe(mapTo('click' as const));

// Replacement — identical inferred type
const modern$: Observable<'click'> = fromEvent(button, 'click').pipe(map(() => 'click' as const));

// Object constants — the SAME reference every time (see pitfalls)
const action = { type: 'INCREMENT' } as const;
const actions$ = fromEvent(button, 'click').pipe(map(() => action));
// Observable<{ readonly type: 'INCREMENT' }>
```

## Examples

### Basic Usage — Deprecated Form and Its Replacement
```typescript
import { interval } from 'rxjs';
import { mapTo, map, take } from 'rxjs/operators';

// Deprecated — shown for reading legacy code
interval(1000).pipe(
  take(3),
  mapTo('tick')
).subscribe(v => console.log(v));
// Output (once per second): tick, tick, tick

// ✅ Replacement
interval(1000).pipe(
  take(3),
  map(() => 'tick')
).subscribe(v => console.log(v));
// Output (once per second): tick, tick, tick
```

### Common Pattern — Counting Events
```typescript
import { fromEvent } from 'rxjs';
import { map, scan } from 'rxjs/operators';

const button = document.querySelector<HTMLButtonElement>('#add')!;

// Legacy: fromEvent(button, 'click').pipe(mapTo(1), scan((sum, n) => sum + n, 0))
fromEvent(button, 'click').pipe(
  map(() => 1),                      // each click is worth 1
  scan((sum, n) => sum + n, 0)
).subscribe(total => console.log('clicks:', total));
// Output per click: clicks: 1, clicks: 2, clicks: 3, …
```

### Common Pattern — Turning UI Events Into Actions
```typescript
import { fromEvent, merge } from 'rxjs';
import { map } from 'rxjs/operators';

interface Action { type: 'OPEN' | 'CLOSE' }

const openBtn  = document.querySelector('#open')!;
const closeBtn = document.querySelector('#close')!;

// Legacy: mapTo<Action>({ type: 'OPEN' }) — same object on every click
const actions$ = merge(
  fromEvent(openBtn,  'click').pipe(map((): Action => ({ type: 'OPEN' }))),
  fromEvent(closeBtn, 'click').pipe(map((): Action => ({ type: 'CLOSE' })))
);

actions$.subscribe(a => console.log('dispatch', a));
// Output: dispatch { type: 'OPEN' }, dispatch { type: 'CLOSE' }, …
```

### Edge Cases — Shared Reference, Eager Evaluation, Empty Source
```typescript
import { of, EMPTY } from 'rxjs';
import { mapTo, map } from 'rxjs/operators';

// Edge case 1: the same object reference is emitted every time
const shared = { count: 0 };
of(1, 2, 3).pipe(mapTo(shared)).subscribe(obj => { obj.count++; });
console.log(shared.count);
// Output: 3    (every emission mutated the one shared object)

// Edge case 2: the constant is evaluated ONCE, at construction
const stamped$ = of('a', 'b').pipe(mapTo(Date.now()));   // captured now, not per emission
stamped$.subscribe(t => console.log(t));
// Output: the same timestamp twice
const fresh$ = of('a', 'b').pipe(map(() => Date.now()));  // evaluated per emission
fresh$.subscribe(t => console.log(t));
// Output: two (possibly different) timestamps

// Edge case 3: empty source — nothing is emitted
EMPTY.pipe(mapTo('never')).subscribe({
  next:     v => console.log(v),
  complete: () => console.log('complete')
});
// Output: complete
```

## Common Pitfalls

### Anti-pattern: Still Using the Deprecated Form
```typescript
import { interval } from 'rxjs';
import { mapTo, map } from 'rxjs/operators';

// ❌ INCORRECT — deprecated in RxJS 7, removed in RxJS 8
interval(1000).pipe(mapTo('tick'));

// ✅ CORRECT — a zero-argument arrow in map
interval(1000).pipe(map(() => 'tick'));

// WHY: map(() => value) is equally short, needs no extra import, and makes the
// "ignore the input" intent visible in the arrow's empty parameter list. The
// single-purpose operator did not justify its API surface.
```

### Anti-pattern: Mapping to a Mutable Object
```typescript
import { fromEvent } from 'rxjs';
import { mapTo, map } from 'rxjs/operators';

// ❌ INCORRECT — one object instance shared by every emission (and every subscriber)
const state = { selected: false };
fromEvent(button, 'click').pipe(mapTo(state)).subscribe(s => {
  s.selected = !s.selected;       // mutates the shared instance
});

// ✅ CORRECT — build a fresh object per emission
fromEvent(button, 'click').pipe(
  map(() => ({ selected: true }))
).subscribe(s => consume(s));

// WHY: mapTo captures one reference and re-emits it. Any downstream mutation is
// visible to every other consumer of that reference — including earlier
// emissions still held in buffers, ReplaySubjects, or state stores.
```

### Anti-pattern: Expecting the Constant to Be Re-evaluated
```typescript
import { interval } from 'rxjs';
import { mapTo, map } from 'rxjs/operators';

// ❌ INCORRECT — Math.random() runs once; every tick carries the same number
interval(500).pipe(mapTo(Math.random())).subscribe(console.log);
// 0.4137…, 0.4137…, 0.4137…

// ✅ CORRECT — a projection function runs per emission
interval(500).pipe(map(() => Math.random())).subscribe(console.log);
// 0.4137…, 0.9021…, 0.1176…

// WHY: mapTo takes a VALUE, evaluated eagerly when the pipeline is built.
// map takes a FUNCTION, evaluated lazily for each value. Anything that should
// vary over time — timestamps, ids, random numbers — belongs in a function.
```

## Related Operators

**Same Category (Transformation)**:
- **`map`**: The replacement and the general case — `map(() => value)` is `mapTo(value)`
- **`scan`**: Frequently paired to turn `mapTo(1)`-style event streams into running counts
- **`switchMapTo`** / **`mergeMapTo`** / **`concatMapTo`**: The same "ignore the value" idea for inner Observables — all deprecated alongside `mapTo`

**Complementary Operators**:
- **`fromEvent`**: The most common source — DOM events whose payload is irrelevant
- **`ignoreElements`**: Drops values entirely instead of replacing them; use when only completion/error matter
- **`startWith`**: Seeds the constant stream with an initial value

**Alternatives by Use Case**:

| Use Case | Instead of `mapTo` | Use This | Why |
|----------|--------------------|----------|-----|
| Constant per emission | `mapTo(c)` | `map(() => c)` | Not deprecated |
| Fresh object per emission | `mapTo({ … })` | `map(() => ({ … }))` | Avoids shared mutable state |
| Value computed per emission | `mapTo(compute())` | `map(() => compute())` | Lazy, per-emission evaluation |
| Only completion matters | `mapTo(null)` | `ignoreElements()` | No junk emissions |

**Migration Notes**:
```typescript
// Deprecated API (RxJS 6 / early 7)
import { mapTo } from 'rxjs/operators';
clicks$.pipe(mapTo('clicked'));
clicks$.pipe(mapTo({ type: 'INCREMENT' }));

// Current API (RxJS 7+)
import { map } from 'rxjs/operators';
clicks$.pipe(map(() => 'clicked'));
clicks$.pipe(map(() => ({ type: 'INCREMENT' })));   // fresh object per click
```

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/mapTo](https://rxjs.dev/api/operators/mapTo)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/operators/map.html](http://reactivex.io/documentation/operators/map.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/mapTo.ts](https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/mapTo.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Constant Projection (deprecated specialization of `map`)
- **Cognitive Load**: 1/5 — Trivial; the only traps are shared references and eager evaluation
- **Usage Frequency**: 2/5 — Still common in RxJS 6 code and older tutorials; absent from new RxJS 7 code
- **Composability**: 4/5 — Pure pass-through; slots anywhere `map` does

**Teaching Sequence**:
- **Prerequisites**: `map`, the difference between a value and a function that returns a value
- **Teaches**: Eager vs. lazy evaluation in operator arguments; why small convenience operators get removed
- **Leads to**: `map`, `scan` for counting, `switchMap` for value-ignoring inner subscriptions
- **Common with**: `fromEvent`, `scan`, `merge`, `startWith`
