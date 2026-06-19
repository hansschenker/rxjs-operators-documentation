# filter

## Identity
- **Name**: filter
- **Category**: Filtering Operators
- **Type**: Synchronous predicate filter (N-to-M transformation, M ≤ N)
- **Import**:
  ```typescript
  import { filter } from 'rxjs/operators';
  // Also available as a standalone operator:
  import { filter } from 'rxjs';
  ```
- **Signature**:
  ```typescript
  // Standard boolean predicate
  function filter<T>(
    predicate: (value: T, index: number) => boolean,
    thisArg?: any
  ): MonoTypeOperatorFunction<T>

  // Type guard overload — narrows emission type from T to S
  function filter<T, S extends T>(
    predicate: (value: T, index: number) => value is S,
    thisArg?: any
  ): OperatorFunction<T, S>
  ```

## Functional Specification

**Input**: `Observable<T>` — a source Observable emitting values of type T

**Output**: `Observable<T>` (or `Observable<S>` with a type guard) — an Observable emitting only the values for which the predicate returns `true`

**Transformation**: Evaluates the predicate function synchronously for each source emission. Values where the predicate returns `true` are forwarded to the output; values where it returns `false` are silently discarded. The stream cardinality may decrease (M ≤ N), but never increases.

**Mathematical representation**:
```
Let S be the source Observable emitting values v₁, v₂, v₃, ...
Let predicate: (T, number) → boolean be the filter function

Output = { vᵢ ∈ S | predicate(vᵢ, i) = true }

Formally: filter(p) ≡ ∀ vᵢ ∈ S → if p(vᵢ, i) then emit vᵢ else discard
```

**Invariants**:
- **Non-increasing cardinality**: Output emissions ≤ source emissions; filter never adds values
- **Order preserved**: Passing emissions appear in the same order as in the source
- **Values unchanged**: Emissions that pass through are forwarded as-is — filter never transforms values
- **Synchronous evaluation**: The predicate executes synchronously; filter introduces no timing changes
- **Index reflects source position**: The `index` parameter counts all source emissions (including filtered ones), not just those that pass

## Marble Diagram

```
Source:   --1--2--3--4--5--|
          filter(x => x % 2 === 0)
Result:   -----2-----4-----|

Legend:
  - : time unit (10ms)
  1,2,3,4,5 : source values
  2,4 : values that pass the predicate
  | : completion
  1,3,5 are silently discarded
```

**With index parameter**:
```
Source:   --a--b--c--d--e--|
          filter((_, i) => i % 2 === 0)
Result:   --a-----c-----e--|

Note: index tracks all source emissions (0,1,2,3,4), not just passing ones.
Values at even indices (0,2,4) pass; values at odd indices (1,3) are dropped.
```

**All values filtered (empty result)**:
```
Source:   --1--3--5--|
          filter(x => x % 2 === 0)
Result:   ----------|

Source completes, result completes immediately — no emissions.
```

**Error propagation**:
```
Source:   --1--2--#
          filter(x => x % 2 === 0)
Result:   -----2--#

Errors bypass the predicate and propagate immediately.
```

**Key observation**: `filter` is transparent to timing — it only decides whether to forward or discard each value synchronously as it arrives. The time between emissions in the output reflects the time between *passing* emissions in the source.

## Behavioral Characteristics

**Subscription**:
- Subscribes to source lazily — only when the output Observable is subscribed to
- Maintains exactly one subscription to the source for the lifetime of the output subscription
- Unsubscribes from source immediately when the output subscriber unsubscribes

**Completion semantics**:
- Source completion propagates immediately to the output — no buffering
- If all source values are filtered out, the result completes without emitting (empty stream)
- If the source is already empty, the result completes immediately without calling the predicate

**Error handling**:
- Errors from the source propagate immediately to the output, bypassing the predicate
- If the predicate function throws, the error is caught and forwarded as an Observable error, then the subscription terminates
- No built-in recovery; use `catchError` downstream for error handling

**Backpressure**:
- None — `filter` is synchronous and discards values immediately; it cannot buffer
- Output rate ≤ source rate; filter can only slow the stream by emitting fewer values, never more
- No memory accumulation from `filter` itself

**Hot vs. Cold**:
- Transparent to hot/cold semantics — `filter` does not alter multicast behaviour
- With a hot source, filtered-out values are gone; late subscribers cannot recover discarded emissions

## Type System Integration

```typescript
/**
 * Type Parameters:
 *   T - The type emitted by the source Observable
 *   S extends T - The narrowed type (only when using the type guard overload)
 *
 * Input Type:  Observable<T>
 * Output Type: Observable<T>  (boolean predicate)
 *              Observable<S>  (type guard predicate — S extends T)
 *
 * Type Narrowing:
 *   - With a boolean predicate, the output type remains T (no narrowing)
 *   - With a type guard predicate (`value is S`), TypeScript narrows the
 *     output type to S, removing the need for type assertions downstream
 *
 * Type Safety:
 *   - The boolean overload guarantees the emitted type is still T
 *   - The type guard overload provides compile-time proof that only S values
 *     are emitted, enabling exhaustive type checking downstream
 */

import { of } from 'rxjs';
import { filter } from 'rxjs/operators';

// Boolean predicate — output type stays T (number)
const numbers$ = of(1, 2, 3, 4, 5);
const evens$ = numbers$.pipe(
  filter(n => n % 2 === 0)
);
// evens$: Observable<number>

// Type guard predicate — output type is narrowed to S
type Action =
  | { type: 'LOAD';    payload: string }
  | { type: 'SUCCESS'; data: string[]  }
  | { type: 'ERROR';   message: string };

type SuccessAction = Extract<Action, { type: 'SUCCESS' }>;

const actions$ = of<Action>(
  { type: 'LOAD',    payload: 'users' },
  { type: 'SUCCESS', data: ['Alice', 'Bob'] },
  { type: 'ERROR',   message: 'Not found' }
);

// Without type guard: result is Observable<Action> — must assert downstream
const withoutGuard$ = actions$.pipe(
  filter(a => a.type === 'SUCCESS')
);
// withoutGuard$: Observable<Action> — TypeScript doesn't know it's SuccessAction

// With type guard: result is Observable<SuccessAction>
const withGuard$ = actions$.pipe(
  filter((a): a is SuccessAction => a.type === 'SUCCESS')
);
// withGuard$: Observable<SuccessAction> — type-safe, no assertion needed

withGuard$.subscribe(a => {
  console.log(a.data); // TypeScript knows a.data: string[] — no error
});

// Combining filter + map with type guards for discriminated unions
const errorMessages$ = actions$.pipe(
  filter((a): a is Extract<Action, { type: 'ERROR' }> => a.type === 'ERROR'),
  map(a => a.message) // type-safe: a.message is string
);
// errorMessages$: Observable<string>
```

## Examples

### Basic Usage — Filtering Numeric Values
```typescript
import { of } from 'rxjs';
import { filter } from 'rxjs/operators';

const scores$ = of(42, 15, 87, 3, 95, 61, 8);

const passing$ = scores$.pipe(
  filter(score => score >= 60)
);

passing$.subscribe(score => console.log(`Pass: ${score}`));
// Output:
// Pass: 87
// Pass: 95
// Pass: 61
```

### Common Pattern — Filtering DOM Events by Key
```typescript
import { fromEvent } from 'rxjs';
import { filter, map } from 'rxjs/operators';

const keydown$ = fromEvent<KeyboardEvent>(document, 'keydown');

// Only react to Enter key
const enter$ = keydown$.pipe(
  filter(e => e.key === 'Enter'),
  map(e => (e.target as HTMLInputElement).value)
);

enter$.subscribe(value => {
  console.log('Submitted:', value);
  submitForm(value);
});

// Only react to alphanumeric characters
const alphanumeric$ = keydown$.pipe(
  filter(e => /^[a-zA-Z0-9]$/.test(e.key))
);

alphanumeric$.subscribe(e => console.log('Key typed:', e.key));
```

### Common Pattern — Type-Safe Action Filtering with Type Guards
```typescript
import { Subject } from 'rxjs';
import { filter, map } from 'rxjs/operators';

type AppAction =
  | { type: 'USER_LOADED';   user: { id: number; name: string } }
  | { type: 'USER_UPDATED';  changes: Partial<{ name: string }> }
  | { type: 'USER_DELETED';  id: number }
  | { type: 'ERROR';         message: string };

const action$ = new Subject<AppAction>();

// Type-safe stream of only USER_LOADED actions
const userLoaded$ = action$.pipe(
  filter((a): a is Extract<AppAction, { type: 'USER_LOADED' }> =>
    a.type === 'USER_LOADED'
  )
);

userLoaded$.subscribe(a => {
  // a.user is fully typed — no assertion needed
  console.log(`Loaded user: ${a.user.name} (id: ${a.user.id})`);
});

// Dispatch actions
action$.next({ type: 'ERROR', message: 'Network failure' });
action$.next({ type: 'USER_LOADED', user: { id: 1, name: 'Alice' } });
action$.next({ type: 'USER_UPDATED', changes: { name: 'Alicia' } });

// Output:
// Loaded user: Alice (id: 1)
```

### Edge Cases — Index Parameter and Empty Results
```typescript
import { of, EMPTY } from 'rxjs';
import { filter } from 'rxjs/operators';

// Edge case 1: Using the index parameter
of('a', 'b', 'c', 'd', 'e').pipe(
  filter((value, index) => index % 2 === 0) // keep values at even indices
).subscribe(v => console.log(v));
// Output: a, c, e
// Note: index counts ALL source emissions (including discarded ones)

// Edge case 2: Predicate filters everything out
of(1, 3, 5, 7).pipe(
  filter(n => n % 2 === 0) // no even numbers in source
).subscribe({
  next:     v => console.log('Value:', v),
  complete: () => console.log('Completed'),
});
// Output: Completed  (no values emitted)

// Edge case 3: Empty source
EMPTY.pipe(
  filter(n => true) // predicate never called
).subscribe({
  next:     v => console.log('Value:', v),
  complete: () => console.log('Completed'),
});
// Output: Completed

// Edge case 4: Predicate throws
of(1, 2, 3).pipe(
  filter(n => {
    if (n === 2) throw new Error('Predicate failed on 2');
    return n > 0;
  })
).subscribe({
  next:  v   => console.log('Value:', v),
  error: err => console.log('Error:', err.message),
});
// Output:
// Value: 1
// Error: Predicate failed on 2
// (3 is never evaluated)
```

### Advanced Pattern — Partitioning a Stream with Multiple Filters
```typescript
import { share } from 'rxjs/operators';
import { fromEvent } from 'rxjs';
import { filter, map } from 'rxjs/operators';

interface SensorReading {
  sensorId: string;
  value: number;
  timestamp: number;
}

// Share the source so it is only subscribed to once
const readings$ = getSensorStream().pipe(share());

// Fan-out: three independent filtered views of the same stream
const critical$ = readings$.pipe(
  filter(r => r.value > 90),
  map(r => ({ ...r, severity: 'critical' as const }))
);

const warning$ = readings$.pipe(
  filter(r => r.value > 70 && r.value <= 90),
  map(r => ({ ...r, severity: 'warning' as const }))
);

const normal$ = readings$.pipe(
  filter(r => r.value <= 70),
  map(r => ({ ...r, severity: 'normal' as const }))
);

critical$.subscribe(r => alertOps(r));
warning$.subscribe(r => logWarning(r));
normal$.subscribe(r => updateDashboard(r));

// Alternative: use partition() for a two-way split
import { partition } from 'rxjs';
const [above$, below$] = partition(readings$, r => r.value > 70);
```

## Common Pitfalls

### Anti-pattern: Using `filter` for Side Effects
```typescript
import { of } from 'rxjs';
import { filter, tap } from 'rxjs/operators';

// ❌ INCORRECT — side effect hidden inside filter predicate
of(1, 2, 3, 4).pipe(
  filter(n => {
    console.log('Checking:', n); // side effect in predicate!
    return n % 2 === 0;
  })
).subscribe(console.log);

// ✅ CORRECT — use tap for side effects, keep filter pure
of(1, 2, 3, 4).pipe(
  tap(n => console.log('Checking:', n)),
  filter(n => n % 2 === 0)
).subscribe(console.log);

// WHY: The predicate is a pure function contract — it should only inspect
// the value and return true/false. Side effects in predicates are invisible
// to readers, execute even for discarded values, and break testability.
// Use tap before or after filter for observation.
```

### Anti-pattern: Not Using Type Guards When Type Narrowing Is Needed
```typescript
import { of } from 'rxjs';
import { filter, map } from 'rxjs/operators';

type Result = { ok: true; value: number } | { ok: false; error: string };

const results$ = of<Result>(
  { ok: true, value: 42 },
  { ok: false, error: 'failed' },
  { ok: true, value: 7 }
);

// ❌ INCORRECT — boolean predicate, output is still Observable<Result>
const unsafe$ = results$.pipe(
  filter(r => r.ok),
  map(r => r.value) // TypeScript error: 'value' does not exist on type Result
                    // (because r could still be { ok: false })
);

// ✅ CORRECT — type guard narrows to { ok: true; value: number }
const safe$ = results$.pipe(
  filter((r): r is Extract<Result, { ok: true }> => r.ok),
  map(r => r.value) // no error: TypeScript knows r.value exists
);

safe$.subscribe(v => console.log(v));
// Output: 42, 7

// WHY: Without a type guard, TypeScript cannot narrow the type after filter,
// forcing you to use unsafe type assertions (as) downstream.
// A type guard makes the narrowing explicit and compiler-verified.
```

### Anti-pattern: Filtering Instead of Transforming
```typescript
import { of } from 'rxjs';
import { filter, map } from 'rxjs/operators';

interface ApiResponse {
  status: number;
  body: string | null;
}

// ❌ INCORRECT — using filter to guard, then accessing same property in map
of<ApiResponse>({ status: 200, body: 'ok' }, { status: 404, body: null }).pipe(
  filter(r => r.body !== null),
  map(r => r.body!.toUpperCase()) // non-null assertion '!' needed — code smell
).subscribe(console.log);

// ✅ CORRECT — narrow with type guard so '!' is not needed
of<ApiResponse>({ status: 200, body: 'ok' }, { status: 404, body: null }).pipe(
  filter((r): r is ApiResponse & { body: string } => r.body !== null),
  map(r => r.body.toUpperCase()) // TypeScript knows body is string
).subscribe(console.log);
// Output: OK

// WHY: Non-null assertions (!) bypass TypeScript's type system and hide
// potential bugs. A type guard is one extra word but provides actual safety.
```

### Anti-pattern: Expensive Predicates Without Rate Limiting
```typescript
import { fromEvent } from 'rxjs';
import { filter, debounceTime } from 'rxjs/operators';

// ❌ INCORRECT — expensive predicate called on every mousemove
fromEvent<MouseEvent>(document, 'mousemove').pipe(
  filter(e => isInsideExpensiveRegion(e.clientX, e.clientY)) // called ~100/s
).subscribe(handleHover);

// ✅ CORRECT — rate-limit the source before applying the expensive predicate
fromEvent<MouseEvent>(document, 'mousemove').pipe(
  debounceTime(16),
  filter(e => isInsideExpensiveRegion(e.clientX, e.clientY)) // called ≤60/s
).subscribe(handleHover);

// WHY: filter's predicate runs synchronously on every source emission.
// For high-frequency sources (mousemove, scroll, resize), an expensive
// predicate will execute hundreds of times per second.
// Rate-limit upstream first, then filter.
```

### Performance: `filter` Before vs. After Expensive Operators
**When this matters**:
When `filter` and an expensive operator (e.g., `map` with heavy computation) are both in the pipeline.

**What to do**:
```typescript
// ❌ Less efficient: map runs on all values, filter discards some after
source$.pipe(
  map(x => expensiveTransform(x)), // runs on every emission
  filter(x => x.isValid)
);

// ✅ More efficient: filter first to reduce work for map
source$.pipe(
  filter(x => x.isValid),          // cheaply discard early
  map(x => expensiveTransform(x))  // only runs on values that matter
);

// Note: this only applies when the predicate can be evaluated on the
// un-transformed value. If the predicate needs the transformed value,
// you have no choice but to map first.
```

## Related Operators

**Same Category (Filtering)**:
- **`take`**: Emits the first N values then completes — use when you want a fixed count rather than a condition
- **`takeWhile`**: Emits while predicate is true, completes on first false — use when a false result means "done"
- **`skip`**: Discards the first N values — positional filtering rather than value-based
- **`distinct`**: Filters out values already seen in the stream — use for deduplication across the entire lifetime
- **`distinctUntilChanged`**: Filters consecutive duplicates — use when you only care about changes
- **`first`**: Emits the first value matching a predicate then completes — equivalent to `filter(p).pipe(take(1))`

**Complementary Operators**:
- **`map`**: The natural follow-on to `filter` — filter to the right shape, then transform it
- **`tap`**: Use before `filter` to observe all values (including those that will be discarded)
- **`partition`**: Splits one stream into two based on a predicate — use when you need both the passing and failing values
- **`catchError`**: Use downstream to recover if the predicate or later operators throw

**Alternatives by Use Case**:

| Use Case | Instead of `filter` | Use This | Why |
|----------|---------------------|----------|-----|
| Two-way split | `filter(p)` + `filter(x => !p(x))` | `partition(source$, p)` | Single subscription, explicit pair |
| First match only | `filter(p).pipe(take(1))` | `first(p)` | Shorter, same semantics |
| Last match only | `filter(p).pipe(last())` | `last(p)` | Shorter, same semantics |
| Drop until condition | `filter(p)` | `skipWhile(x => !p(x))` | Different: `skipWhile` stops filtering after first pass |
| Emit until condition | `filter(p)` | `takeWhile(p)` | Different: `takeWhile` completes on first false |

**Migration Notes**:
```typescript
// first(predicate) is shorthand for filter + take(1)
source$.pipe(filter(p), take(1))   // equivalent (but first() throws on empty)
source$.pipe(first(p))             // cleaner — throws EmptyError if nothing matches

// Use defaultIfEmpty to avoid the EmptyError
source$.pipe(first(p), defaultIfEmpty(fallback))
```

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/filter](https://rxjs.dev/api/operators/filter)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/operators/filter.html](http://reactivex.io/documentation/operators/filter.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/filter.ts](https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/filter.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Predicate Gate Strategy (Conditional Value Forwarding)
- **Cognitive Load**: 1/5 — Identical mental model to `Array.prototype.filter`; no timing, state, or higher-order concepts
- **Usage Frequency**: 5/5 — Present in virtually every RxJS pipeline alongside `map`
- **Composability**: 5/5 — Fundamental building block; composes freely with every other operator

**Problem Domain**:
Selectively forwarding emissions that satisfy a condition, reducing stream noise and narrowing types. Used for event routing (act on specific event types), data validation (only process valid inputs), and stream branching (route to different handlers based on value).

**When to Teach**:
Second operator after `map` — shares the `Array.prototype.filter` mental model students already know.

- **Prerequisites**: Observable creation, subscribe, `map`
- **Teaches**: Selective emission, predicate functions, TypeScript type guards in reactive context
- **Leads to**: `takeWhile` (filter that terminates), `partition` (two-way filter), `distinctUntilChanged` (filter on equality)
- **Common with**: `map`, `tap`, `distinctUntilChanged` — the everyday pipeline quartet

**Common Misconceptions**:
1. **"filter's index counts only passing values"** — the index counts all source emissions, including discarded ones
2. **"filter with a boolean predicate narrows the type"** — it does not; only a type guard predicate (`value is S`) narrows the type
3. **"filter can drop all values and still complete"** — yes; a stream that filters everything still completes when the source completes
4. **"I can use filter for async conditions"** — the predicate must return `boolean` synchronously; for async conditions use `mergeMap` + `filter` or `switchMap`
