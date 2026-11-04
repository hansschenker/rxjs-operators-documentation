# throwIfEmpty

## Identity
- **Name**: throwIfEmpty
- **Category**: Error Handling Operators
- **Type**: Empty-stream guard — forwards all values unchanged, but converts an empty completion into an error
- **Import**:
  ```typescript
  import { throwIfEmpty } from 'rxjs/operators';
  // Also available as a standalone operator:
  import { throwIfEmpty } from 'rxjs';
  ```
- **Signature**:
  ```typescript
  function throwIfEmpty<T>(
    errorFactory: () => any = () => new EmptyError()
  ): MonoTypeOperatorFunction<T>
  ```

## Functional Specification

**Input**: `Observable<T>` — a source Observable that may emit zero or more values

**Output**: `Observable<T>` — an Observable that mirrors the source exactly, except that an empty completion becomes an error

**Transformation**:
Tracks a single boolean flag, `hasValue`, which flips to `true` on the first source emission. Every value is forwarded unchanged. When the source completes, the flag decides the outcome: if at least one value was seen, the output completes normally; otherwise `errorFactory()` is invoked and its return value is emitted as the error notification. Source errors are forwarded as-is and the factory is never consulted.

**Mathematical representation**:
```
throwIfEmpty(f)(S) =
  S                      if |S| ≥ 1          (identity — values and completion pass through)
  error(f())             if |S| = 0 and S completes
  error(e)               if S errors with e  (f is never called)

where |S| is the number of next notifications emitted by S
```

**Invariants**:
- **Value transparency**: Every source value is forwarded unchanged, in order, with no buffering
- **Lazy factory**: `errorFactory` runs at most once, and only at the moment of an empty completion
- **Error precedence**: A source error always wins — the operator never replaces an existing error with its own
- **No early completion**: The operator cannot decide "empty" until the source completes; it never terminates on its own

## Marble Diagram

```
Source:   --a--b--c--|
          throwIfEmpty()
Result:   --a--b--c--|

Source emitted values; throwIfEmpty is a pass-through.
```

**Empty source**:
```
Source:   -----|
          throwIfEmpty()
Result:   -----#   (EmptyError)

Source completed without a value; the completion becomes an error.
```

**Custom error factory**:
```
Source:   -----|
          throwIfEmpty(() => new NotFoundError('user-42'))
Result:   -----#   (NotFoundError)
```

**Source errors first**:
```
Source:   --#
          throwIfEmpty()
Result:   --#   (original error — the factory is never called)
```

**Key observation**: `throwIfEmpty` is the error-raising twin of `defaultIfEmpty`. Both react to the same condition — "the source completed with nothing" — but one supplies a value and the other raises an error. Choose `throwIfEmpty` when emptiness is a bug or a business-rule violation that must surface, and `defaultIfEmpty` when emptiness is an ordinary outcome that deserves a fallback.

## Behavioral Characteristics

**Subscription**:
- Subscribes to the source exactly once, immediately on output subscription
- Adds no scheduling, no delay, and no inner subscriptions

**Completion semantics**:
- Source completes after ≥ 1 value: output completes normally
- Source completes after 0 values: output errors with `errorFactory()` instead of completing
- The output never completes before the source does

**Error handling**:
- Source errors propagate immediately and unchanged; `errorFactory` is not invoked
- If `errorFactory` itself throws, that thrown value becomes the error notification
- The default error is an `EmptyError` instance (`err.name === 'EmptyError'`, message `no elements in sequence`)

**Backpressure**:
- None — O(1) state (one boolean); values are forwarded synchronously as they arrive

**Hot vs. Cold**:
- With cold sources: "empty" means the fresh execution produced nothing
- With hot sources: "empty" means nothing was observed *after* subscription; values emitted earlier do not count
- Pairs naturally with `first()`-style one-shot reads of hot sources where a missing value is an error

## Type System Integration

```typescript
/**
 * Type Parameters:
 *   T - Value type (MonoTypeOperatorFunction<T> — the value type is preserved)
 *
 * Input Type:  Observable<T>
 * Output Type: Observable<T>
 *
 * errorFactory returns `any`, so the error channel is untyped — the same as
 * every other RxJS error. Narrow it in the subscriber or in catchError.
 *
 * Type Safety:
 *   The output cannot carry a "missing" marker (no `T | undefined`); emptiness
 *   is moved entirely to the error channel, which keeps the value type clean.
 */

import { of, EmptyError } from 'rxjs';
import { filter, throwIfEmpty, catchError } from 'rxjs/operators';

interface User { id: string; active: boolean }

const activeUser$ = of<User>({ id: 'u1', active: false }).pipe(
  filter(u => u.active),
  throwIfEmpty(() => new EmptyError())
);
// activeUser$: Observable<User>  — not Observable<User | undefined>

activeUser$.pipe(
  catchError((err: unknown) => {
    if (err instanceof EmptyError) {
      return of<User>({ id: 'anonymous', active: true });
    }
    throw err;
  })
).subscribe((u: User) => console.log(u.id));
// Output: anonymous
```

## Examples

### Basic Usage — Default `EmptyError`
```typescript
import { of } from 'rxjs';
import { filter, throwIfEmpty } from 'rxjs/operators';

of(1, 2, 3, 4, 5).pipe(
  filter(x => x > 10), // nothing passes
  throwIfEmpty()
).subscribe({
  next:  v   => console.log('value:', v),
  error: err => console.log('error:', err.name, '-', err.message)
});
// Output: error: EmptyError - no elements in sequence

of(1, 2, 3).pipe(
  filter(x => x > 1),
  throwIfEmpty()
).subscribe({
  next:     v => console.log('value:', v),
  complete: () => console.log('done')
});
// Output: value: 2, value: 3, done
```

### Common Pattern — Custom Error for a Missing Resource
```typescript
import { from } from 'rxjs';
import { filter, throwIfEmpty, map } from 'rxjs/operators';

class ResourceNotFoundError extends Error {
  constructor(public readonly resourceId: string) {
    super(`Resource "${resourceId}" not found`);
    this.name = 'ResourceNotFoundError';
  }
}

interface Row { id: string; name: string }
const rows: Row[] = [{ id: 'user-1', name: 'Ada' }, { id: 'user-2', name: 'Grace' }];

function findRow(id: string) {
  return from(rows).pipe(
    filter(row => row.id === id),
    throwIfEmpty(() => new ResourceNotFoundError(id)),
    map(row => row.name)
  );
}

findRow('user-42').subscribe({
  next:  name => console.log('found:', name),
  error: (err: ResourceNotFoundError) => console.log(`[${err.name}] ${err.message}`)
});
// Output: [ResourceNotFoundError] Resource "user-42" not found
```

### Common Pattern — Guard, Then Recover With `catchError`
```typescript
import { of } from 'rxjs';
import { filter, throwIfEmpty, catchError } from 'rxjs/operators';

const matches$ = of(10, 20, 30).pipe(
  filter(x => x > 100),
  throwIfEmpty(() => new Error('No results matched the criteria')),
  catchError(err => {
    console.warn('caught:', err.message);
    return of(-1); // safe sentinel instead of a failed stream
  })
);

matches$.subscribe({
  next:     v => console.log('result:', v),
  complete: () => console.log('done')
});
// Output:
// caught: No results matched the criteria
// result: -1
// done
```

### Edge Cases — Source Error, Hot Source, Factory That Throws
```typescript
import { Subject, throwError, of } from 'rxjs';
import { throwIfEmpty, take } from 'rxjs/operators';

// Edge case 1: source errors before emitting — original error wins
throwError(() => new Error('upstream failed')).pipe(
  throwIfEmpty(() => new Error('was empty'))
).subscribe({ error: err => console.log(err.message) });
// Output: upstream failed

// Edge case 2: hot source — only values AFTER subscription count
const events$ = new Subject<string>();
events$.next('missed');                       // emitted before anyone subscribed
events$.pipe(take(1), throwIfEmpty()).subscribe({
  next:  v   => console.log('got:', v),
  error: err => console.log('error:', err.name)
});
events$.complete();
// Output: error: EmptyError   ('missed' was never observed)

// Edge case 3: a throwing factory — the thrown value is the error
of().pipe(
  throwIfEmpty(() => { throw new RangeError('factory exploded'); })
).subscribe({ error: err => console.log(err.name) });
// Output: RangeError
```

## Common Pitfalls

### Anti-pattern: Expecting `throwIfEmpty` to React to Source Errors
```typescript
import { throwError, EMPTY } from 'rxjs';
import { throwIfEmpty, catchError } from 'rxjs/operators';

// ❌ INCORRECT — the custom error never fires; the upstream error is forwarded instead
throwError(() => new Error('network down')).pipe(
  throwIfEmpty(() => new Error('nothing found'))
).subscribe({
  error: err => console.log(err.message) // "network down"
});

// ✅ CORRECT — handle the two conditions separately
EMPTY.pipe(
  throwIfEmpty(() => new Error('nothing found')),   // empty completion → error
  catchError(err => {                               // ANY error (empty or upstream)
    console.error('failed:', err.message);
    return EMPTY;
  })
).subscribe();

// WHY: throwIfEmpty only watches the COMPLETE notification. An error notification
// bypasses it entirely and the factory is never called. If you need "empty" and
// "failed" to converge on one handler, add catchError downstream.
```

### Anti-pattern: Using `throwIfEmpty` Where a Fallback Value Is the Real Intent
```typescript
import { of } from 'rxjs';
import { filter, throwIfEmpty, catchError, defaultIfEmpty } from 'rxjs/operators';

// ❌ INCORRECT — raising an error just to catch it and substitute a value
of(1, 2, 3).pipe(
  filter(x => x > 5),
  throwIfEmpty(),
  catchError(() => of(0))
).subscribe(console.log); // 0 — but via an error round-trip

// ✅ CORRECT — defaultIfEmpty expresses "fallback on empty" directly
of(1, 2, 3).pipe(
  filter(x => x > 5),
  defaultIfEmpty(0)
).subscribe(console.log); // 0

// WHY: throwIfEmpty is for cases where emptiness is a FAILURE that callers must
// see. If the pipeline immediately recovers with a value, the error channel adds
// noise (stack traces, error logging, retry triggers) without adding meaning.
```

### Anti-pattern: Side Effects Inside the Error Factory
```typescript
import { throwIfEmpty, EmptyError } from 'rxjs';

// ❌ INCORRECT — the factory doubles as an analytics hook
source$.pipe(
  throwIfEmpty(() => {
    trackEvent('empty-stream');   // runs on every empty completion, invisible in the pipe
    return new EmptyError();
  })
);

// ✅ CORRECT — keep the factory pure; react to the error where it is observed
source$.pipe(
  throwIfEmpty()
).subscribe({
  error: err => {
    if (err instanceof EmptyError) trackEvent('empty-stream');
  }
});

// WHY: Factories should construct an error and nothing else. Hiding side effects
// there makes them easy to miss, impossible to test in isolation, and surprising
// when the operator is reused in a pipeline with retry (each retry re-runs it).
```

## Related Operators

**Same Category (Error Handling)**:
- **`catchError`**: Downstream recovery from the error that `throwIfEmpty` raises
- **`timeout`**: A different "did not happen" guard — errors when values are too slow rather than absent
- **`retry`**: Re-executes the source when `throwIfEmpty` errors — useful when "empty" is transient

**Complementary Operators**:
- **`filter`**: The most common upstream partner — `filter` + `throwIfEmpty` asserts that at least one value matches
- **`first(predicate)`**: Already errors with `EmptyError` on an empty source; `throwIfEmpty` adds a custom error to the `take`-style operators that do not
- **`single`**: Stricter — errors on zero matches *and* on more than one match

**Alternatives by Use Case**:

| Use Case | Instead of `throwIfEmpty` | Use This | Why |
|----------|---------------------------|----------|-----|
| Fallback value on empty | `throwIfEmpty` + `catchError` | `defaultIfEmpty(value)` | No error round-trip |
| Boolean "was it empty?" | `throwIfEmpty` + `catchError` | `isEmpty()` | Emits `true`/`false` as a value |
| First value, error on empty | `take(1)` + `throwIfEmpty()` | `first()` | One operator, same `EmptyError` |
| Exactly one match required | `throwIfEmpty` | `single(predicate)` | Also rejects multiple matches |

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/throwIfEmpty](https://rxjs.dev/api/operators/throwIfEmpty)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/throwIfEmpty.ts](https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/throwIfEmpty.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Empty-Stream Assertion (completion-to-error conversion)
- **Cognitive Load**: 2/5 — One flag and one decision at completion; the only subtlety is that source errors bypass it
- **Usage Frequency**: 2/5 — Occasional; most codebases reach for `first()` or `defaultIfEmpty` and only use `throwIfEmpty` when a custom error type is needed
- **Composability**: 4/5 — Transparent pass-through, slots after any filtering stage and before `catchError`/`retry`

**Teaching Sequence**:
- **Prerequisites**: `filter`, completion vs. error notifications, `EmptyError` from `first`
- **Teaches**: Treating emptiness as a first-class outcome; the value channel vs. the error channel
- **Leads to**: `defaultIfEmpty`, `isEmpty`, `single`, domain-specific error classes in `catchError`
- **Common with**: `filter`, `take`, `catchError`, `retry`, `first`
