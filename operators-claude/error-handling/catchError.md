# catchError

## Identity
- **Name**: catchError
- **Category**: Error Handling Operators
- **Type**: Error recovery — intercepts errors and returns a replacement Observable
- **Import**:
  ```typescript
  import { catchError } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  function catchError<T, O extends ObservableInput<any>>(
    selector: (err: any, caught: Observable<T>) => O
  ): OperatorFunction<T, T | ObservedValueOf<O>>
  ```

## Functional Specification

**Input**: `Observable<T>` — a source Observable that may error

**Output**: `Observable<T | R>` — an Observable that either mirrors the source (if no error occurs) or emits values from the replacement Observable returned by `selector` (on error)

**Transformation**: Subscribes to the source. All source emissions are forwarded unchanged. If the source errors, the `selector` function is called with the error and the source Observable itself (`caught`). The selector must return an `ObservableInput` — its emissions replace the errored source and become the output stream's values. If the replacement completes, the output completes. If the replacement also errors, the new error propagates (no further catch).

**Mathematical representation**:
```
Let S be the source Observable.
Let selector: (err, caught) → ObservableInput<R> be the handler.

Case 1 — S completes normally:
  Output = S  (exact mirror, no selector involvement)

Case 2 — S errors with e at some point:
  Let R = toObservable(selector(e, S))
  Output = { values from S before error } ++ { values from R }

Case 3 — R (replacement) also errors with e₂:
  Output = { values from S before e } ++ { values from R before e₂ } ++ error(e₂)
```

**Invariants**:
- **Transparent on success**: If source never errors, `catchError` adds zero overhead
- **Selector receives the caught Observable**: Passing `caught` back to the selector enables retry loops
- **Single-level catch**: Only one `catchError` in the chain handles a given error; nested catches require additional `catchError` in the replacement stream
- **Synchronous selector**: The selector is called synchronously when the error arrives

## Marble Diagram

```
Source:   --a--b--#
          catchError(_ => of('x', 'y'))
Result:   --a--b--x--y--|

Legend:
  - : time unit (10ms)
  a,b : emitted values
  # : error
  x,y : values from replacement Observable
  | : completion
  Error is intercepted; replacement Observable runs to completion.
```

**Rethrowing the error**:
```
Source:   --a--b--#
          catchError(e => throwError(() => new CustomError(e)))
Result:   --a--b--#  (CustomError)

Error is transformed/re-thrown as a different error type.
```

**Returning EMPTY to silently swallow**:
```
Source:   --a--b--#
          catchError(_ => EMPTY)
Result:   --a--b--|

Error suppressed; stream completes without replacement values.
```

**Retrying with `caught`**:
```
Source:   --a--#  (first attempt)
          catchError((err, caught) => caught)
          Retries source indefinitely on error — dangerous without limit!
Result:   --a--a--a--a-- (loops until source succeeds or stack overflows)
```

**Key observation**: `catchError` turns an `error` notification into a `complete` notification (by returning a replacement Observable) — this is the fundamental contract of error recovery in RxJS.

## Behavioral Characteristics

**Subscription**:
- Subscribes to source lazily when output is subscribed
- Subscribes to replacement Observable only if and when source errors
- Both subscriptions are mutually exclusive — the source is already terminated when replacement begins

**Completion semantics**:
- Source completes normally → output completes; selector never called
- Source errors → selector called; replacement Observable subscribed; output continues until replacement completes or errors
- Selector returns EMPTY → output completes immediately with no further values
- Selector returns a never-completing Observable → output runs forever

**Error handling**:
- Errors in `selector` itself propagate downstream
- Errors in the replacement Observable propagate downstream (no automatic re-catch)
- Stack: if you need to catch replacement errors, add another `catchError` in the replacement stream or after this one in the pipe

**Backpressure**:
- None — synchronous pass-through on source emissions
- Selector is called once per error event; O(1) overhead

**Hot vs. Cold**:
- The replacement Observable starts fresh at error time; if it's cold, a new subscription begins
- Passing `caught` (the source) back as the replacement creates a cold re-subscription loop — safe only with an outer guard (e.g., `retryWhen`, `take`, counter) or with a genuinely finite source

## Type System Integration

```typescript
/**
 * Type Parameters:
 *   T - Source Observable value type
 *   O - The ObservableInput returned by selector; its value type is ObservedValueOf<O>
 *
 * Output Type: Observable<T | ObservedValueOf<O>>
 *   — union of source type and replacement type
 *
 * When T = R: output is Observable<T>   (same type — clean fallback)
 * When T ≠ R: output is Observable<T | R> (union type — TypeScript enforces handling both)
 *
 * selector signature: (err: any, caught: Observable<T>) => ObservableInput<O>
 *   — err is typed as `any`; narrow it yourself with instanceof
 *   — caught is typed as Observable<T>
 */

import { of, throwError, EMPTY } from 'rxjs';
import { catchError, map } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

// Same type: fallback value of same type as source
ajax.getJSON<User[]>('/api/users').pipe(
  catchError(_ => of([] as User[])) // Observable<User[]> | Observable<User[]> → Observable<User[]>
).subscribe((users: User[]) => renderList(users));

// Different type: union
ajax.getJSON<User[]>('/api/users').pipe(
  catchError(_ => of({ error: 'unavailable' }))
  // output: Observable<User[] | { error: string }>
).subscribe(result => {
  if (Array.isArray(result)) {
    renderList(result);
  } else {
    showError(result.error);
  }
});

// Narrowing the error type
ajax.getJSON<User[]>('/api/users').pipe(
  catchError((err: unknown) => {
    if (err instanceof Error) {
      console.error('Network error:', err.message);
    }
    return EMPTY; // Observable<never> — output: Observable<User[]>
  })
).subscribe((users: User[]) => renderList(users));

// EMPTY as replacement: Observable<never> is absorbed into T
// TypeScript infers output as Observable<T> — no union widening
```

## Examples

### Basic Usage — Fallback Value on Error
```typescript
import { of, throwError } from 'rxjs';
import { catchError } from 'rxjs/operators';

throwError(() => new Error('oops')).pipe(
  catchError(err => {
    console.warn('Caught:', err.message);
    return of('fallback');
  })
).subscribe({
  next:     v => console.log(v),
  complete: () => console.log('done'),
});
// Output: Caught: oops
//         fallback
//         done
```

### Common Pattern — HTTP Fallback with Retry
```typescript
import { of } from 'rxjs';
import { catchError, retry } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

interface UserProfile { id: number; name: string; }

function loadProfile(userId: number): Observable<UserProfile | null> {
  return ajax.getJSON<UserProfile>(`/api/users/${userId}`).pipe(
    retry(2), // try up to 3 times before catchError
    catchError(err => {
      if (err.status === 404) {
        return of(null); // user not found — return null sentinel
      }
      throw err; // re-throw unexpected errors
    })
  );
}

loadProfile(42).subscribe({
  next:  profile => profile ? showProfile(profile) : showNotFound(),
  error: err => showGenericError(err),
});
```

### Common Pattern — Error Type Transformation
```typescript
import { throwError } from 'rxjs';
import { catchError } from 'rxjs/operators';

class AppError extends Error {
  constructor(message: string, public readonly code: string) {
    super(message);
    this.name = 'AppError';
  }
}

apiCall().pipe(
  catchError((err: unknown) => {
    if (err instanceof HttpErrorResponse) {
      // Transform HTTP error into domain error
      throw new AppError(
        `Request failed: ${err.statusText}`,
        `HTTP_${err.status}`
      );
    }
    // Re-throw unknown errors unchanged
    throw err;
  })
).subscribe({
  next:  handleSuccess,
  error: (err: AppError) => showError(err.code, err.message),
});
```

### Common Pattern — Silent Error Swallowing in Stream Composition
```typescript
import { merge, EMPTY, interval } from 'rxjs';
import { catchError, map } from 'rxjs/operators';

// Multiple data sources — one failing should not kill the merge
const sources$ = [
  fetch('/api/source-a').then(r => r.json()),
  fetch('/api/source-b').then(r => r.json()),
  fetch('/api/source-c').then(r => r.json()),
].map((p, i) =>
  from(p).pipe(
    map(data => ({ source: i, data })),
    catchError(err => {
      console.warn(`Source ${i} failed:`, err);
      return EMPTY; // failed source drops out silently, merge continues
    })
  )
);

merge(...sources$).subscribe(result => processResult(result));
// If source-b errors, source-a and source-c still emit normally
```

### Common Pattern — Global Error Handler with Logging
```typescript
import { Observable, EMPTY } from 'rxjs';
import { catchError } from 'rxjs/operators';

function withErrorLogging<T>(
  source$: Observable<T>,
  context: string
): Observable<T> {
  return source$.pipe(
    catchError(err => {
      errorTracker.log({ error: err, context, timestamp: Date.now() });
      return EMPTY;
    })
  );
}

// Usage:
withErrorLogging(userEvents$, 'UserEventStream').subscribe(handleEvent);
```

## Common Pitfalls

### Anti-pattern: Catching Errors in the Wrong Place in the Chain
```typescript
import { of, throwError } from 'rxjs';
import { switchMap, catchError, map } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

const ids$ = of(1, 2, 3);

// ❌ INCORRECT — catchError at top level catches only outer errors
// HTTP errors from inner switchMap Observable are NOT caught here
ids$.pipe(
  switchMap(id => ajax.getJSON(`/api/items/${id}`)),
  catchError(err => {
    // If any inner ajax call errors, the entire outer stream terminates
    // and ALL subsequent IDs are abandoned
    console.error(err);
    return EMPTY;
  })
).subscribe(console.log);
// If item #2 returns 404: items 1 forwarded, item 2 kills the stream, item 3 never fetched

// ✅ CORRECT — catchError INSIDE switchMap handles per-item errors
ids$.pipe(
  switchMap(id =>
    ajax.getJSON(`/api/items/${id}`).pipe(
      catchError(err => {
        console.warn(`Item ${id} failed:`, err);
        return of(null); // null sentinel for this item only
      })
    )
  )
).subscribe(item => item && process(item));
// If item #2 returns 404: items 1, 3 still fetched normally

// WHY: catchError only catches errors from the Observable it is directly applied to.
// Errors from inner Observables (switchMap, mergeMap, etc.) bubble up and terminate
// the outer stream if not caught inside the inner pipe.
```

### Anti-pattern: Accidentally Creating an Infinite Retry Loop
```typescript
import { interval } from 'rxjs';
import { switchMap, catchError, map } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

// ❌ DANGEROUS — passing `caught` without a limit causes infinite loops
ajax.getJSON('/api/data').pipe(
  catchError((err, caught) => caught) // re-subscribes to source on every error
).subscribe(console.log);
// If the server always returns errors → infinite requests → server overload

// ✅ CORRECT — use retry() / retryWhen() for controlled retry logic
ajax.getJSON('/api/data').pipe(
  retry({ count: 3, delay: 1000 }) // 3 retries with 1s delay between
).pipe(
  catchError(err => of(defaultValue)) // fallback after all retries exhausted
).subscribe(console.log);

// ✅ ALSO CORRECT — manual counter with catchError + caught
let retries = 0;
ajax.getJSON('/api/data').pipe(
  catchError((err, caught) => {
    if (retries++ < 3) return caught;
    throw err; // re-throw after 3 attempts
  })
).subscribe({ next: console.log, error: e => console.error('failed', e) });

// WHY: catchError's second argument (caught) is a reference to the *source* Observable.
// Returning it re-subscribes from the beginning. Without a guard, a persistently failing
// source creates an unbounded retry loop.
```

### Anti-pattern: Swallowing All Errors Silently
```typescript
import { EMPTY } from 'rxjs';
import { catchError } from 'rxjs/operators';

// ❌ DANGEROUS — all errors silently discarded
criticalStream$.pipe(
  catchError(_ => EMPTY)
).subscribe(handleCriticalEvent);

// If an error indicates data corruption, it is silently suppressed.
// The subscriber has no way to know the stream ended abnormally.

// ✅ CORRECT — log at minimum; re-throw critical errors
criticalStream$.pipe(
  catchError(err => {
    if (isCriticalError(err)) {
      throw err; // propagate critical errors
    }
    console.warn('Non-critical error suppressed:', err);
    return EMPTY;
  })
).subscribe({ next: handleCriticalEvent, error: alertOncall });

// WHY: EMPTY completes the stream normally — the subscriber's complete() handler
// fires. If the subscriber relies on seeing all events, silent suppression masks
// data loss. Always at minimum log; always propagate errors that indicate
// system integrity issues.
```

### Anti-pattern: Using `catchError` Instead of `retry` for Transient Failures
```typescript
import { ajax } from 'rxjs/ajax';
import { catchError } from 'rxjs/operators';

// ❌ VERBOSE — reimplementing retry logic in catchError
let attempts = 0;
ajax.getJSON('/api/data').pipe(
  catchError((err, caught) => {
    if (attempts++ < 3) return caught;
    throw err;
  })
).subscribe(console.log);

// ✅ CLEANER — use retry() for fixed retry counts
ajax.getJSON('/api/data').pipe(
  retry(3),
  catchError(err => of(fallback)) // only runs if all retries exhausted
).subscribe(console.log);

// ✅ CLEANER — use retryWhen() / retry({ delay }) for exponential backoff
ajax.getJSON('/api/data').pipe(
  retry({ count: 3, delay: (err, count) => timer(1000 * 2 ** count) }),
  catchError(err => of(fallback))
).subscribe(console.log);

// WHY: retry() is purpose-built for transient error recovery and composes
// cleanly with catchError for final fallback. Using catchError for both
// retry and fallback conflates two distinct concerns.
```

## Related Operators

**Same Category (Error Handling)**:
- **`retry(n)`**: Resubscribes to source on error up to N times — use for transient failures before `catchError` handles the final fallback
- **`retryWhen(notifier)`** / **`retry({ delay })`**: Retry with custom delay/backoff strategy
- **`throwError`**: Creates an Observable that immediately errors — useful as a selector return to re-throw errors
- **`onErrorResumeNextWith`**: Continues with replacement regardless of whether source completed normally or errored

**Complementary Operators**:
- **`finalize`**: Runs a side-effect callback on completion OR error — use for cleanup that must happen regardless of success/failure; does not handle errors
- **`tap({ error })`**: Observe errors without handling them — use for logging when you want to let the error propagate
- **`materialize`**: Converts errors into `Notification` values — use when you need to handle errors as data in a stream

**Alternatives by Use Case**:

| Use Case | Instead of `catchError` | Use This | Why |
|----------|-------------------------|----------|-----|
| Retry then fallback | `catchError(_, caught) + counter` | `retry(N)` + `catchError(fallback)` | Cleaner separation of concerns |
| Log error, propagate | `catchError(e => { log(e); throw e; })` | `tap({ error: log })` | More idiomatic for observation-only |
| Convert error to value | `catchError(e => of(sentinel))` | `catchError(e => of(sentinel))` | This IS `catchError` — correct form |

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/catchError](https://rxjs.dev/api/operators/catchError)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/operators/catch.html](http://reactivex.io/documentation/operators/catch.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/src/internal/operators/catchError.ts](https://github.com/ReactiveX/rxjs/blob/master/src/internal/operators/catchError.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Error Recovery Gateway (Observable-level try/catch)
- **Cognitive Load**: 3/5 — The `caught` parameter and the inner-vs-outer placement rule are the main stumbling blocks
- **Usage Frequency**: 5/5 — Present in virtually every production RxJS codebase; error handling is non-optional
- **Composability**: 5/5 — Composes cleanly with retry, finalize, tap; can be chained multiple times in one pipe

**Problem Domain**:
Preventing observable chains from terminating on recoverable errors. Provides a boundary equivalent to `try/catch` in synchronous code, but for the asynchronous Observable error channel.

**When to Teach**:
Teach immediately after explaining error semantics in Observables (the three notification types: next, error, complete). `catchError` is the first and most important error handling tool.

- **Prerequisites**: `map`, `filter`, `throwError`, `EMPTY`
- **Teaches**: Observable error channel, recovery vs. re-throw, selector pattern, inner-vs-outer error scope
- **Leads to**: `retry`, `retryWhen`, `finalize`, error handling strategy design
- **Common with**: `switchMap`, `mergeMap`, `ajax`, `retry`, `EMPTY`, `of`

**Common Misconceptions**:
1. **"catchError at the top catches all errors everywhere"** — only catches errors from the Observable it's directly applied to; inner Observables need their own `catchError`
2. **"Returning EMPTY is always safe"** — swallows errors silently; always log at minimum
3. **"caught is the current error"** — `caught` is the *source Observable*, not the error; the error is the first argument
4. **"I can use catchError instead of retry"** — possible but wrong abstraction; use `retry` for transient errors, `catchError` for final fallback
