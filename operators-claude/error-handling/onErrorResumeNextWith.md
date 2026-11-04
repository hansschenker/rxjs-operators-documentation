# onErrorResumeNextWith

## Identity
- **Name**: onErrorResumeNextWith
- **Category**: Error Handling Operators
- **Type**: Error-ignoring sequencer — the pipeable form of `onErrorResumeNext`; subscribes to the source and then to each further source in order, treating both error and completion as "move on"
- **Import**:
  ```typescript
  import { onErrorResumeNextWith } from 'rxjs';
  // Also re-exported from 'rxjs/operators'.
  // The older pipeable `onErrorResumeNext` operator is deprecated in favor of this name;
  // `onErrorResumeNext` now refers to the static creation function.
  ```
- **Signature**:
  ```typescript
  function onErrorResumeNextWith<T, A extends readonly unknown[]>(
    sources: [...ObservableInputTuple<A>]
  ): OperatorFunction<T, T | A[number]>

  function onErrorResumeNextWith<T, A extends readonly unknown[]>(
    ...sources: [...ObservableInputTuple<A>]
  ): OperatorFunction<T, T | A[number]>
  ```

## Functional Specification

**Input**: `Observable<T>` — the first source in the chain, plus any number of further `ObservableInput` values (Observables, Promises, arrays, iterables)

**Output**: `Observable<T | A[number]>` — the values of every source, in order, with all errors suppressed

**Transformation**:
Subscribes to the source. When it terminates — by **error or by completion**, indistinguishably — the operator subscribes to the next provided source, and so on. Values from each active source are forwarded unchanged. After the last source terminates, the output completes. The output never errors.

**Mathematical representation**:
```
onErrorResumeNextWith(S₁, S₂, …, Sₙ)(S₀) ≡ onErrorResumeNext(S₀, S₁, S₂, …, Sₙ)

values(Result) = values(S₀) ++ values(S₁) ++ … ++ values(Sₙ)
terminal(Result) = complete             (always — every error is swallowed)

For each k:  terminate(Sₖ) ∈ { complete, error } ⇒ subscribe(Sₖ₊₁)
```

**Invariants**:
- **Strict sequence**: At most one source is subscribed at a time; `Sₖ₊₁` is never subscribed before `Sₖ` terminates
- **Error suppression**: No error from any source reaches the subscriber, and no error information is retained
- **Order preservation**: Output values appear in source order, never interleaved
- **Guaranteed completion**: Once the last source terminates (either way), the output completes

## Marble Diagram

```
Source:    --1--2--#
S₁:                --3--4--#
S₂:                        --5--6--|
           onErrorResumeNextWith(S₁, S₂)
Result:    --1--2----3--4----5--6--|

Both errors are swallowed; each termination hands off to the next source.
```

**No errors — identical to `concatWith`**:
```
Source:    --1--|
S₁:             --2--|
S₂:                  --3--|
Result:    --1----2----3--|
```

**Last source errors — silent completion**:
```
Source:    --1--|
S₁:             --#
Result:    --1---|          (no error, nothing more to try)
```

**Key observation**: The operator does not *catch* errors so much as *forget* them. `catchError` gives you the error object and a choice; `onErrorResumeNextWith` gives you neither. That makes it right for best-effort chains where every step is optional, and wrong for anything where a failure must be visible.

## Behavioral Characteristics

**Subscription**:
- Subscribes to the source immediately; later sources are subscribed lazily, one at a time, only when their predecessor terminates
- Non-Observable inputs (Promises, arrays) are converted with `from` at the moment they are subscribed

**Completion semantics**:
- Output completes exactly once, after the last source terminates
- A source completing normally and a source erroring have the same effect on sequencing
- A source that never terminates blocks the chain — later sources are never reached

**Error handling**:
- Every error from every source is suppressed; the output's `error` callback is never invoked
- There is no hook to observe the suppressed error inside this operator — add `tap({ error })` or `catchError` on the individual source if you need visibility
- A source that errors synchronously on subscription is skipped just like any other

**Backpressure**:
- O(1) state: the list of pending sources and one active subscription
- No buffering — values are forwarded as they arrive

**Hot vs. Cold**:
- Cold sources: each is executed fresh when its turn comes
- Hot sources: a Subject that has already errored or completed terminates immediately on subscription, so the chain skips past it at once
- The source Observable is cold-subscribed like any pipeable operator's input

## Type System Integration

```typescript
/**
 * Type Parameters:
 *   T - Source value type
 *   A - Tuple of the additional sources' value types (inferred per argument)
 *
 * Input Type:  Observable<T>
 * Output Type: Observable<T | A[number]>
 *
 * Type Narrowing:
 *   A[number] is the union of every additional source's value type. Mixed
 *   types widen the output; homogeneous sources keep it narrow.
 *
 * Type Safety:
 *   Promises and arrays are accepted and unwrapped — Promise<X> contributes X.
 */

import { of, throwError } from 'rxjs';
import { onErrorResumeNextWith } from 'rxjs';

interface Prefs { theme: string }
const DEFAULT_PREFS: Prefs = { theme: 'light' };

// Homogeneous — Observable<Prefs>
const prefs$ = loadPrefs$.pipe(            // Observable<Prefs>, may error
  onErrorResumeNextWith(of(DEFAULT_PREFS))
);

// Mixed — Observable<number | string>
const mixed$ = throwError(() => new Error('x')).pipe(   // Observable<never>
  onErrorResumeNextWith(of(1, 2), Promise.resolve('three'), ['four'])
);
mixed$.subscribe((v: number | string) => console.log(v));
// Output: 1, 2, three, four
```

## Examples

### Basic Usage — Fall Through a Chain of Data Sources
```typescript
import { throwError, of } from 'rxjs';
import { onErrorResumeNextWith } from 'rxjs';
import { delay } from 'rxjs/operators';

const primary$ = throwError(() => new Error('primary DB unavailable'));
const cache$   = of('cached data (stale)').pipe(delay(100));
const default$ = of('default placeholder');

primary$.pipe(
  onErrorResumeNextWith(cache$, default$)
).subscribe({
  next:     v => console.log('got:', v),
  error:    () => console.log('never called'),
  complete: () => console.log('all sources exhausted')
});
// Output:
// got: cached data (stale)
// got: default placeholder
// all sources exhausted
```

### Common Pattern — Best-Effort Batch Where Individual Items May Fail
```typescript
import { of, throwError, Observable } from 'rxjs';
import { onErrorResumeNextWith } from 'rxjs';

const files = ['a.txt', 'b.txt', 'c.txt', 'd.txt'];

function processFile(name: string): Observable<string> {
  return name === 'b.txt' || name === 'c.txt'
    ? throwError(() => new Error(`failed: ${name}`))
    : of(`processed: ${name}`);
}

const [first$, ...rest$] = files.map(processFile);

first$.pipe(
  onErrorResumeNextWith(...rest$)       // spread form; an array works too
).subscribe({
  next:     r => console.log(r),
  complete: () => console.log('batch complete')
});
// Output:
// processed: a.txt
// processed: d.txt
// batch complete
```

### Common Pattern — Startup Sequence That Skips Non-Critical Steps
```typescript
import { of, throwError } from 'rxjs';
import { onErrorResumeNextWith } from 'rxjs';
import { tap, delay } from 'rxjs/operators';

const loadPrefs$    = of({ theme: 'dark' }).pipe(delay(200), tap(() => console.log('prefs loaded')));
const initAnalytics$ = throwError(() => new Error('analytics down'));   // optional step
const ready$        = of('app ready');

loadPrefs$.pipe(
  onErrorResumeNextWith(initAnalytics$, ready$)
).subscribe({
  next:     v => console.log('value:', v),
  complete: () => console.log('startup complete')
});
// Output:
// prefs loaded
// value: { theme: 'dark' }
// value: app ready
// startup complete
```

### Edge Cases — Synchronous Errors, Never-Terminating Source, Empty Argument List
```typescript
import { throwError, NEVER, of, EMPTY } from 'rxjs';
import { onErrorResumeNextWith } from 'rxjs';

// Edge case 1: every source errors synchronously — output completes with no values
throwError(() => new Error('1')).pipe(
  onErrorResumeNextWith(throwError(() => new Error('2')), EMPTY)
).subscribe({
  next:     v => console.log(v),
  complete: () => console.log('completed, no values')
});
// Output: completed, no values

// Edge case 2: a never-terminating source blocks the rest of the chain
of('first').pipe(
  onErrorResumeNextWith(NEVER, of('never reached'))
).subscribe(v => console.log(v));
// Output: first   (then nothing — the chain is stuck on NEVER)

// Edge case 3: no extra sources — errors are still swallowed
throwError(() => new Error('x')).pipe(
  onErrorResumeNextWith()
).subscribe({ complete: () => console.log('completed silently') });
// Output: completed silently
```

## Common Pitfalls

### Anti-pattern: Swallowing Errors That Someone Needs to See
```typescript
import { throwError, of } from 'rxjs';
import { onErrorResumeNextWith } from 'rxjs';
import { catchError } from 'rxjs/operators';

// ❌ INCORRECT — a critical failure vanishes; the UI just shows the fallback
loadAccount$.pipe(
  onErrorResumeNextWith(of(EMPTY_ACCOUNT))
).subscribe(render);

// ✅ CORRECT — log (or rethrow after logging) on the source, then fall through
loadAccount$.pipe(
  catchError(err => {
    reportError('loadAccount failed', err);     // visibility preserved
    return throwError(() => err);               // rethrow so the chain still moves on
  }),
  onErrorResumeNextWith(of(EMPTY_ACCOUNT))
).subscribe(render);

// WHY: onErrorResumeNextWith discards the error object entirely. Nothing reaches
// your logging, your error boundary, or your retry logic. Reserve it for steps
// whose failure is genuinely irrelevant, and surface everything else first.
```

### Anti-pattern: Using It for Single-Source Recovery Instead of `catchError`
```typescript
import { throwError, of } from 'rxjs';
import { onErrorResumeNextWith } from 'rxjs';
import { catchError } from 'rxjs/operators';

// ❌ INCORRECT — one source, one fallback, but no access to the error
fetchUser$.pipe(
  onErrorResumeNextWith(of(GUEST))
).subscribe(render);   // was it a 401? a 500? a network drop? unknown.

// ✅ CORRECT — catchError sees the error and can choose the fallback
fetchUser$.pipe(
  catchError(err => err.status === 401 ? of(GUEST) : throwError(() => err))
).subscribe({ next: render, error: showFatal });

// WHY: catchError is the tool for "this source failed, decide what to do".
// onErrorResumeNextWith is the tool for "run these N things in order, no
// matter what". Using the latter for the former throws away the decision.
```

### Anti-pattern: Expecting It to Retry the Same Source
```typescript
import { onErrorResumeNextWith } from 'rxjs';
import { retry } from 'rxjs/operators';

// ❌ INCORRECT — three DIFFERENT subscriptions; a cold source is re-executed, but a hot one
//    that already errored is skipped instantly, and errors are still invisible
request$.pipe(
  onErrorResumeNextWith(request$, request$)
).subscribe(render);

// ✅ CORRECT — retry re-executes the same source and surfaces the final error
request$.pipe(
  retry(2)
).subscribe({ next: render, error: showError });

// WHY: onErrorResumeNextWith sequences distinct sources; it has no notion of
// "the same source again" and no final error. retry has both.
```

## Related Operators

**Same Category (Error Handling)**:
- **`catchError`**: Error-aware recovery for a single source — preferred whenever the error matters
- **`retry`**: Re-executes the same source; use for "try again" rather than "try the next thing"
- **`onErrorResumeNext`** (static): Same semantics as a creation function — `onErrorResumeNext(a$, b$, c$)`

**Complementary Operators**:
- **`concatWith`**: Same sequencing *with* error propagation — use when a failure should stop the chain
- **`tap({ error })`**: Observe an error on an individual source before it is swallowed
- **`defaultIfEmpty`**: Supply a value if the whole chain completes without emitting

**Alternatives by Use Case**:

| Use Case | Instead of `onErrorResumeNextWith` | Use This | Why |
|----------|-----------------------------------|----------|-----|
| Fallback with error visibility | `onErrorResumeNextWith(fb$)` | `catchError(() => fb$)` | Error object available |
| Sequence that must not hide failures | `onErrorResumeNextWith(b$, c$)` | `concatWith(b$, c$)` | Errors propagate |
| Re-run the same source | `onErrorResumeNextWith(src$, src$)` | `retry(n)` | Real retry semantics |
| Pick one of two sources up front | `onErrorResumeNextWith` | `iif(cond, a$, b$)` | Condition, not failure, decides |

**Migration Notes**:
```typescript
// Deprecated pipeable form (RxJS 6 / early 7)
import { onErrorResumeNext } from 'rxjs/operators';
source$.pipe(onErrorResumeNext(fallback1$, fallback2$));

// Current API (RxJS 7+)
import { onErrorResumeNextWith } from 'rxjs';
source$.pipe(onErrorResumeNextWith(fallback1$, fallback2$));
```

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/onErrorResumeNextWith](https://rxjs.dev/api/operators/onErrorResumeNextWith)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/operators/catch.html](http://reactivex.io/documentation/operators/catch.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/onErrorResumeNextWith.ts](https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/onErrorResumeNextWith.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Best-Effort Sequential Fallback (error-blind concatenation)
- **Cognitive Load**: 2/5 — "Concat, but errors also mean next"; the only subtlety is that the error is gone for good
- **Usage Frequency**: 2/5 — Niche; most fallback logic wants `catchError`'s visibility
- **Composability**: 3/5 — Composes like `concatWith`, but its error suppression surprises downstream `retry`/`catchError` that never fire

**Teaching Sequence**:
- **Prerequisites**: `concatWith`, `catchError`, the difference between the complete and error notifications
- **Teaches**: That error handling is a design choice — suppress, recover, or re-execute — and the cost of suppression
- **Leads to**: `catchError` with selective rethrow, `retry`, resilient pipeline design
- **Common with**: `of`, `defer`, `tap`, `catchError`, `throwError`
