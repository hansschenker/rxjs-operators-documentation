# single

## Identity
- **Name**: single
- **Category**: Filtering Operators
- **Type**: Cardinality assertion — emits the one value (optionally matching a predicate) and errors if the count is not exactly one
- **Import**:
  ```typescript
  import { single } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  function single<T>(
    predicate?: (value: T, index: number, source: Observable<T>) => boolean
  ): MonoTypeOperatorFunction<T>
  ```

## Functional Specification

**Input**: `Observable<T>` — a finite source that is expected to contain exactly one matching value

**Output**: `Observable<T>` — emits that one value when the source completes, then completes; errors in every other case

**Transformation**:
Tracks whether a matching value has been seen. The first match is stored, not forwarded. A second match raises `SequenceError` immediately and unsubscribes the source. When the source completes: the stored value is emitted and the output completes; if nothing matched, the operator errors with `NotFoundError` (a predicate was given and at least one value was seen) or `EmptyError` (the source emitted nothing at all).

**Mathematical representation**:
```
single(p)(S):
  M = { v ∈ S | p(v) }                 (M = S when p is omitted)
  |M| = 1            → emit m, complete       (delivered when S completes)
  |M| > 1            → error SequenceError    (raised the moment the 2nd match arrives)
  |M| = 0 ∧ |S| > 0  → error NotFoundError    (raised when S completes)
  |S| = 0            → error EmptyError       (raised when S completes)
```

**Invariants**:
- **At most one `next`** ever reaches the subscriber
- **Deferred emission**: the value is delivered at source completion, never at arrival — the operator must first prove no second match follows
- **Fail fast on excess**: a second match errors synchronously and unsubscribes the source
- **Never silent**: the output always either emits exactly one value or errors; it cannot complete empty

## Marble Diagram

```
Source:   --a--|
          single()
Result:   -----a|

'a' is held until completion proves it was the only value.
```

**Two values — `SequenceError`**:
```
Source:   --a--b--c--|
          single()
Result:   -----#

Errors the moment 'b' arrives; 'c' is never requested.
```

**Empty source — `EmptyError`**:
```
Source:   --|
          single()
Result:   --#
```

**Predicate with no match — `NotFoundError`**:
```
Source:   --a--b--c--|
          single(v => v === 'x')
Result:   -----------#

Values were seen but none matched; the error is raised at completion.
```

**Key observation**: `single` is the strict sibling of `first`. Where `first` stops at the first match and never looks further, `single` keeps watching so it can *reject* a second match. That is why `single` cannot emit until the source completes, and why it is the right tool when "more than one" would be a bug rather than a convenience.

## Behavioral Characteristics

**Subscription**:
- Subscribes to the source immediately on subscription
- Unsubscribes early only on a second match (`SequenceError`); otherwise stays subscribed until the source completes

**Completion semantics**:
- Requires source completion — an infinite source with exactly one match never emits
- Emits the single value and completes in the same tick as the source completion

**Error handling**:
- Source errors propagate immediately; any stored match is discarded
- Produces three distinct errors of its own: `SequenceError` (too many), `NotFoundError` (predicate, no match), `EmptyError` (no values at all)
- A throwing predicate propagates as an error notification

**Backpressure**:
- Holds at most one value; O(1) memory
- Does not slow the source — values are consumed as they arrive

**Hot vs. Cold**:
- Works naturally with cold, finite sources (HTTP responses, `from(array)`)
- On a hot source only values after subscription count; a `Subject` that never completes will hold the match forever — bound it with `take`/`takeUntil` first

## Type System Integration

```typescript
/**
 * Type Parameters:
 *   T - Value type (MonoTypeOperatorFunction<T> — type preserved)
 *
 * Input Type:  Observable<T>
 * Output Type: Observable<T>
 *
 * Type Narrowing:
 *   single does not narrow T even when the predicate is a type guard.
 *   Follow with map or filter(guard) if a narrowed type is required.
 *
 * Type Safety:
 *   The three failure modes are runtime errors, not types. Model them
 *   with catchError when a typed fallback is needed.
 */

import { from, of } from 'rxjs';
import { single, catchError } from 'rxjs/operators';

interface Route { path: string; isDefault: boolean }

const routes: Route[] = [
  { path: '/home',  isDefault: true },
  { path: '/about', isDefault: false },
];

// Output type stays Observable<Route>
const defaultRoute$ = from(routes).pipe(
  single(r => r.isDefault)
);

// Widen explicitly when recovering from the error cases
const safeDefault$ = defaultRoute$.pipe(
  catchError(() => of<Route | null>(null))
); // Observable<Route | null>
```

## Examples

### Basic Usage — Assert Exactly One Value
```typescript
import { of } from 'rxjs';
import { single } from 'rxjs/operators';

of(42).pipe(
  single()
).subscribe({
  next:  value => console.log('Single value:', value),
  error: err   => console.error('Error:', err.name),
});
// Output: Single value: 42

of(1, 2).pipe(
  single()
).subscribe({
  next:  value => console.log(value),
  error: err   => console.error('Error:', err.name),
});
// Output: Error: SequenceError
```

### Common Pattern — Exactly-One Lookup
```typescript
import { from } from 'rxjs';
import { single } from 'rxjs/operators';

interface Task { id: number; primary: boolean }

const tasks: Task[] = [
  { id: 1, primary: false },
  { id: 2, primary: true },
  { id: 3, primary: false },
];

// Business rule: there must be exactly one primary task.
// single() enforces the rule instead of silently taking the first.
from(tasks).pipe(
  single(task => task.primary)
).subscribe({
  next:  task => console.log('Primary task:', task.id),
  error: err  => console.error('Invariant violated:', err.name),
});
// Output: Primary task: 2
```

### Common Pattern — Turning Cardinality Errors Into Domain Errors
```typescript
import { from, throwError } from 'rxjs';
import { single, catchError } from 'rxjs/operators';
import { SequenceError, NotFoundError, EmptyError } from 'rxjs';

interface User { id: string; email: string }

function findUserByEmail(users: User[], email: string) {
  return from(users).pipe(
    single(u => u.email === email),
    catchError(err => {
      if (err instanceof SequenceError) {
        return throwError(() => new Error(`Duplicate account for ${email}`));
      }
      if (err instanceof NotFoundError || err instanceof EmptyError) {
        return throwError(() => new Error(`No account for ${email}`));
      }
      return throwError(() => err);
    })
  );
}

findUserByEmail(
  [{ id: 'a', email: 'x@y.z' }, { id: 'b', email: 'x@y.z' }],
  'x@y.z'
).subscribe({
  error: err => console.error(err.message),
});
// Output: Duplicate account for x@y.z
```

### Edge Cases — The Three Error Types
```typescript
import { of, EMPTY } from 'rxjs';
import { single } from 'rxjs/operators';

// Edge case 1: more than one match → SequenceError (raised at the 2nd match)
of(1, 2, 3).pipe(single(n => n > 1)).subscribe({
  error: err => console.log('1:', err.name),
});
// Output: 1: SequenceError

// Edge case 2: values seen, none match → NotFoundError (raised at completion)
of(1, 2, 3).pipe(single(n => n > 10)).subscribe({
  error: err => console.log('2:', err.name),
});
// Output: 2: NotFoundError

// Edge case 3: no values at all → EmptyError
EMPTY.pipe(single()).subscribe({
  error: err => console.log('3:', err.name),
});
// Output: 3: EmptyError

// Edge case 4: emission happens at completion, not at arrival
of('only').pipe(single()).subscribe({
  next:     v  => console.log('next:', v),
  complete: () => console.log('complete'),
});
// Output: next: only, complete   (both delivered when the source completes)
```

## Common Pitfalls

### Anti-pattern: Using `single` When Multiple Matches Are Legitimate
```typescript
import { of } from 'rxjs';
import { single, filter, first } from 'rxjs/operators';

// ❌ INCORRECT — several even numbers exist; single() errors with SequenceError
of(2, 4, 6).pipe(
  single(n => n % 2 === 0)
).subscribe({ error: err => console.error(err.name) });
// Output: SequenceError

// ✅ CORRECT — all matches: filter
of(2, 4, 6).pipe(
  filter(n => n % 2 === 0)
).subscribe(console.log);
// Output: 2, 4, 6

// ✅ CORRECT — only the first match: first
of(2, 4, 6).pipe(
  first(n => n % 2 === 0)
).subscribe(console.log);
// Output: 2

// WHY: single() is an assertion, not a selection. It exists to turn
// "more than one" into an error. Reach for it only when a second match
// would indicate corrupt data or a broken invariant.
```

### Anti-pattern: No Error Handler on a Possibly-Empty Source
```typescript
import { Subject } from 'rxjs';
import { single } from 'rxjs/operators';

const result$ = new Subject<number>();

// ❌ INCORRECT — completing without a value throws EmptyError with no handler
result$.pipe(single()).subscribe(value => console.log(value));
result$.complete(); // unhandled EmptyError

// ✅ CORRECT — handle the error, or decide empty is acceptable and use take(1)
result$.pipe(single()).subscribe({
  next:  value => console.log(value),
  error: err   => console.error('Caught:', err.name),
});
result$.complete();
// Output: Caught: EmptyError

// WHY: Unlike take(1), single() treats an empty source as a failure. If
// "no value" is a normal outcome, single() is the wrong operator; if it is
// a genuine failure, the error must still be observed somewhere.
```

### Anti-pattern: Expecting `single` to Emit on an Infinite Source
```typescript
import { interval } from 'rxjs';
import { single, take } from 'rxjs/operators';

// ❌ INCORRECT — interval never completes; the match is held forever
interval(1000).pipe(
  single(n => n === 3)
).subscribe(console.log);
// (no output, ever — and no error either)

// ✅ CORRECT — bound the source so completion can occur
interval(1000).pipe(
  take(5),
  single(n => n === 3)
).subscribe(console.log);
// Output: 3   (after the 5th tick, when take(5) completes)

// WHY: single() cannot know a value is the only one until the source
// completes. On a never-completing source it waits indefinitely.
```

## Related Operators

**Same Category (Single-Value Selection)**:
- **`first(predicate?)`**: Emits the first match immediately and completes; errors only on empty — use when extra matches are harmless
- **`last(predicate?)`**: Emits the last match at completion; never errors on duplicates
- **`find(predicate)`**: Emits the first match or `undefined`; never errors
- **`elementAt(index)`**: Selects by position rather than by predicate

**Complementary Operators**:
- **`catchError`**: Translate `SequenceError` / `NotFoundError` / `EmptyError` into domain errors or fallbacks
- **`take` / `takeUntil`**: Bound a hot source so `single` can observe completion
- **`throwIfEmpty`**: Custom error for the empty case when only emptiness, not duplicates, must be rejected

**Alternatives by Use Case**:

| Use Case | Instead of `single` | Use This | Why |
|----------|---------------------|----------|-----|
| First match is enough | `single(p)` | `first(p)` | No need to wait for completion |
| Empty is acceptable | `single()` | `take(1)` | Completes silently on empty |
| Missing match should be `undefined` | `single(p)` | `find(p)` | Returns a value instead of erroring |
| Duplicates must be rejected | `first(p)` | `single(p)` | Only `single` detects a second match |

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/single](https://rxjs.dev/api/operators/single)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/operators/first.html](http://reactivex.io/documentation/operators/first.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/single.ts](https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/single.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Cardinality Assertion
- **Cognitive Load**: 2/5 — Simple intent, but three distinct error types and completion-deferred emission surprise newcomers
- **Usage Frequency**: 2/5 — Reserved for invariant checks; `first` and `take(1)` cover the everyday cases
- **Composability**: 3/5 — Pairs naturally with `catchError`, but its strictness makes it a poor fit inside long-lived pipelines

**Teaching Sequence**:
- **Prerequisites**: `first`, `take`, the `EmptyError` concept
- **Teaches**: Operators as assertions, deferred emission until completion, error-type discrimination with `instanceof`
- **Leads to**: `throwIfEmpty`, `catchError` error-translation patterns, `defaultIfEmpty`
- **Common with**: `first`, `find`, `filter`, `catchError`, `take`
