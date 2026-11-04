# defaultIfEmpty

## Identity
- **Name**: defaultIfEmpty
- **Category**: Conditional Operators
- **Type**: Empty-stream fallback — emits a single default value only when the source completes without emitting
- **Import**:
  ```typescript
  import { defaultIfEmpty } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  function defaultIfEmpty<T, R>(defaultValue: R): OperatorFunction<T, T | R>
  ```

## Functional Specification

**Input**: `Observable<T>` — any source that may or may not emit before completing

**Output**: `Observable<T | R>` — the source values unchanged, or exactly one `defaultValue` if there were none

**Transformation**:
Forwards every source value and remembers that at least one was seen. On source completion, if nothing was forwarded, emits `defaultValue` and then completes; otherwise completes directly. The default is never emitted alongside real values and never emitted on error.

**Mathematical representation**:
```
defaultIfEmpty(d)(S) =
  S                     if |S| > 0
  { d } ++ complete()   if |S| = 0   (S completed with no next)

Not applied when S errors: error is propagated as-is.
```

**Invariants**:
- **Non-empty guarantee**: the output emits at least one value whenever the source completes
- **Transparency**: when the source emits, the output is identical to the source
- **Single default**: at most one `defaultValue` is ever emitted, and only at completion
- **Error-neutral**: an erroring source bypasses the default entirely

## Marble Diagram

```
Source:   --------|
          defaultIfEmpty('d')
Result:   --------(d|)

Source completed empty; the default is emitted and the output completes.
```

**Source emits values**:
```
Source:   --a--b--|
          defaultIfEmpty('d')
Result:   --a--b--|

Default ignored — the source was not empty.
```

**Source errors before emitting**:
```
Source:   -----#
          defaultIfEmpty('d')
Result:   -----#

No default — defaultIfEmpty reacts to empty completion, not to errors.
```

**Key observation**: `defaultIfEmpty` answers a question that can only be decided at completion: "did anything come through?" It is therefore invisible on streams that emit, and a one-value substitute on streams that do not. Pair it with `filter`, `take(1)`, or `first` to make "nothing matched" a value instead of a silent completion or an `EmptyError`.

## Behavioral Characteristics

**Subscription**:
- Subscribes to the source immediately; no second source is involved
- Does not alter the source's subscription lifetime

**Completion semantics**:
- Completes when the source completes
- On empty completion, emits `defaultValue` synchronously in the completion handler, then completes
- Never emits on an infinite source that produces nothing

**Error handling**:
- Source errors propagate immediately without the default
- No errors of its own

**Backpressure**:
- O(1) state (a boolean); zero buffering
- No effect on emission timing

**Hot vs. Cold**:
- Cold sources: the "empty" decision is made per subscription
- Hot sources: only emissions after subscription count; a hot source that never completes never triggers the default

## Type System Integration

```typescript
/**
 * Type Parameters:
 *   T - Source value type
 *   R - Default value type
 *
 * Input Type:  Observable<T>
 * Output Type: Observable<T | R>
 *
 * Type Narrowing:
 *   The output is a union. When R is assignable to T the union collapses
 *   to T; otherwise consumers must handle both members.
 *
 * Type Safety:
 *   Passing a default of a different type is allowed and widens the
 *   stream — intentional for sentinels like null, accidental otherwise.
 */

import { of, EMPTY } from 'rxjs';
import { defaultIfEmpty } from 'rxjs/operators';

// Same type — union collapses to number
of(1, 2).pipe(defaultIfEmpty(0)).subscribe((v: number) => console.log(v));

// Different type — union is preserved
EMPTY.pipe(defaultIfEmpty(null)).subscribe((v: never | null) => console.log(v));

interface User { id: number; name: string }

// Typical sentinel pattern: Observable<User | null>
const firstUser$ = of<User>().pipe(
  defaultIfEmpty<User, null>(null)
);
firstUser$.subscribe((u: User | null) => console.log(u?.name ?? 'no user'));
// Output: no user
```

## Examples

### Basic Usage — Fallback for an Empty Source
```typescript
import { EMPTY, of } from 'rxjs';
import { defaultIfEmpty } from 'rxjs/operators';

EMPTY.pipe(
  defaultIfEmpty('no data')
).subscribe(console.log);
// Output: no data

of('a', 'b').pipe(
  defaultIfEmpty('no data')
).subscribe(console.log);
// Output: a, b   (default not used)
```

### Common Pattern — Fallback When a Filter Removes Everything
```typescript
import { of } from 'rxjs';
import { filter, defaultIfEmpty } from 'rxjs/operators';

// No even numbers → the stream would otherwise complete silently
of(1, 3, 5, 7).pipe(
  filter(n => n % 2 === 0),
  defaultIfEmpty(-1)
).subscribe(console.log);
// Output: -1

// At least one value survives → default ignored
of(1, 2, 3).pipe(
  filter(n => n % 2 === 0),
  defaultIfEmpty(-1)
).subscribe(console.log);
// Output: 2
```

### Common Pattern — Guarantee the UI Always Renders Something
```typescript
import { of } from 'rxjs';
import { map, defaultIfEmpty, mergeMap } from 'rxjs/operators';

interface Notification { id: number; text: string }

// Imagine this came from an API and returned an empty list
const apiResponse$ = of<Notification[]>([]);

apiResponse$.pipe(
  mergeMap(list => list),                 // flatten to one value per item (none here)
  map(n => n.text),
  defaultIfEmpty('You have no new notifications')
).subscribe(text => console.log('render:', text));
// Output: render: You have no new notifications
```

### Edge Cases — Errors, `take(1)` on Empty, Late Default
```typescript
import { EMPTY, throwError, Subject } from 'rxjs';
import { defaultIfEmpty, take } from 'rxjs/operators';

// Edge case 1: error bypasses the default
throwError(() => new Error('boom')).pipe(
  defaultIfEmpty('fallback')
).subscribe({
  next:  v   => console.log(v),
  error: err => console.log('error:', err.message),
});
// Output: error: boom

// Edge case 2: take(1) on an empty source + defaultIfEmpty — a safe "first or default"
EMPTY.pipe(
  take(1),
  defaultIfEmpty(null)
).subscribe(v => console.log('value:', v));
// Output: value: null

// Edge case 3: the default waits for completion
const s$ = new Subject<number>();
s$.pipe(defaultIfEmpty(0)).subscribe(v => console.log('got', v));
// (nothing yet)
s$.complete();
// Output: got 0
```

## Common Pitfalls

### Anti-pattern: Confusing `defaultIfEmpty` With `startWith`
```typescript
import { of } from 'rxjs';
import { startWith, defaultIfEmpty } from 'rxjs/operators';

// ❌ INCORRECT — startWith always prepends, even when real values exist
of(1, 2).pipe(startWith(0)).subscribe(console.log);
// Output: 0, 1, 2   (spurious leading 0)

// ✅ CORRECT — defaultIfEmpty only substitutes when nothing arrived
of(1, 2).pipe(defaultIfEmpty(0)).subscribe(console.log);
// Output: 1, 2
of<number>().pipe(defaultIfEmpty(0)).subscribe(console.log);
// Output: 0

// WHY: startWith is unconditional and happens at subscription time.
// defaultIfEmpty is conditional and happens at completion time.
```

### Anti-pattern: Expecting the Default to Cover `undefined` Values
```typescript
import { of } from 'rxjs';
import { map, defaultIfEmpty } from 'rxjs/operators';

interface Wrapper { nested?: string }

// ❌ INCORRECT — the stream DID emit (an undefined); defaultIfEmpty does nothing
of<Wrapper>({}).pipe(
  map(w => w.nested),
  defaultIfEmpty('fallback')
).subscribe(console.log);
// Output: undefined

// ✅ CORRECT — per-value defaults belong in map
of<Wrapper>({}).pipe(
  map(w => w.nested ?? 'fallback')
).subscribe(console.log);
// Output: fallback

// WHY: defaultIfEmpty counts emissions, not their contents. An emitted
// undefined is still an emission.
```

### Anti-pattern: Expecting the Default to Cover Errors
```typescript
import { throwError, of } from 'rxjs';
import { defaultIfEmpty, catchError } from 'rxjs/operators';

// ❌ INCORRECT — the default never appears; the stream errors instead
throwError(() => new Error('boom')).pipe(
  defaultIfEmpty('fallback')
).subscribe({
  next:  console.log,
  error: err => console.error('errored:', err.message),
});
// Output: errored: boom

// ✅ CORRECT — recover from errors explicitly, then guard emptiness
throwError(() => new Error('boom')).pipe(
  catchError(() => of<string>()),   // swallow error → empty stream
  defaultIfEmpty('fallback')
).subscribe(console.log);
// Output: fallback

// WHY: Empty completion and error are different terminal notifications.
// defaultIfEmpty handles only the former; catchError handles the latter.
```

## Related Operators

**Same Category (Emptiness Handling)**:
- **`throwIfEmpty(errorFactory?)`**: The error-oriented counterpart — raises instead of substituting
- **`isEmpty()`**: Reports emptiness as a boolean instead of substituting a value
- **`every(predicate)`**: Also decided at completion; an empty source yields `true`

**Complementary Operators**:
- **`filter`**: The most common producer of unexpectedly empty streams
- **`take(1)` / `first(null, default)`**: `first` has a built-in default parameter; `take(1) + defaultIfEmpty` is the explicit form
- **`catchError`**: Handles the error case that `defaultIfEmpty` deliberately ignores

**Alternatives by Use Case**:

| Use Case | Instead of `defaultIfEmpty` | Use This | Why |
|----------|-----------------------------|----------|-----|
| Always emit a leading value | `defaultIfEmpty(v)` | `startWith(v)` | Unconditional, immediate |
| Empty should be an error | `defaultIfEmpty(v)` | `throwIfEmpty()` | Surfaces the problem |
| Only need to know if empty | `defaultIfEmpty(v)` | `isEmpty()` | Boolean result, no substitute |
| Per-value nullish default | `defaultIfEmpty(v)` | `map(x => x ?? v)` | Acts on values, not on emptiness |

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/defaultIfEmpty](https://rxjs.dev/api/operators/defaultIfEmpty)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/operators/defaultifempty.html](http://reactivex.io/documentation/operators/defaultifempty.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/defaultIfEmpty.ts](https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/defaultIfEmpty.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Empty-Stream Substitution
- **Cognitive Load**: 1/5 — One rule; the only trap is conflating emptiness with undefined values or errors
- **Usage Frequency**: 4/5 — Appears wherever `filter`, `take(1)`, or search results may legitimately produce nothing
- **Composability**: 5/5 — Transparent on non-empty streams, so it can be added anywhere a non-empty guarantee is wanted

**Teaching Sequence**:
- **Prerequisites**: `filter`, completion semantics, the difference between empty completion and error
- **Teaches**: Completion-time decisions, non-empty guarantees, union output types
- **Leads to**: `throwIfEmpty`, `isEmpty`, `first` with a default, `catchError`
- **Common with**: `filter`, `take`, `first`, `catchError`, `EMPTY`
