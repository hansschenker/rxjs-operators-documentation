# throwError

## Identity

- **Name**: throwError
- **Category**: Creation Operators
- **Type**: Error factory — creates an Observable that immediately errors with a specified value on subscription
- **Import**:
  ```typescript
  import { throwError } from 'rxjs';
  ```
- **Signature**:
  ```typescript
  // RxJS 7+ — preferred form
  function throwError<T = never>(
    errorFactory: () => any
  ): Observable<T>

  // Legacy form (deprecated, still works)
  function throwError<T = never>(error: any): Observable<T>
  ```

## Functional Specification

**Concept**: `throwError` creates an Observable that, on each subscription, immediately calls the error callback with the provided error value. It never emits a `next` value and never completes — it only errors.

**Factory form vs value form**:
- `throwError(() => new Error('msg'))` — factory called fresh per subscription (RxJS 7 preferred)
- `throwError(new Error('msg'))` — same Error instance reused across subscriptions (deprecated)
- The factory form prevents shared mutable state between retries

**The four creation primitives**:
```
of(value)              → next(value), complete()
EMPTY                  → complete()
NEVER                  → (nothing)
throwError(() => err)  → error(err)
```

## Marble Diagram

```
throwError(() => new Error('oops')):
Result:  #      (immediate error, no next/complete)

of(1, 2).pipe(
  mergeMap(n => n === 2 ? throwError(() => new Error('bad')) : of(n))
):
Result:  --1--#   (1 passes, 2 triggers error)

throwError(() => err).pipe(
  catchError(e => of('recovered'))
):
Result:  recovered|   (error caught, fallback emitted, complete)
```

## Type System Integration

```typescript
import { throwError, of } from 'rxjs';
import { catchError } from 'rxjs/operators';

// throwError<T> defaults to Observable<never>
// Observable<never> is assignable to any Observable<T>
const err$: Observable<never> = throwError(() => new Error('fail'));
const safe$: Observable<number> = err$.pipe(catchError(() => of(0)));

// Common in switchMap / mergeMap for conditional errors
import { switchMap } from 'rxjs/operators';
source$.pipe(
  switchMap(value =>
    value > 0
      ? processValue(value)                          // Observable<Result>
      : throwError(() => new RangeError('non-positive')) // Observable<never>
  )
  // Output: Observable<Result>  (never absorbed by union)
).subscribe({ next: handleResult, error: handleError });
```

## Examples

### Basic Usage — Immediate Error
```typescript
import { throwError } from 'rxjs';
import { catchError } from 'rxjs/operators';
import { of } from 'rxjs';

throwError(() => new Error('something went wrong')).subscribe({
  next:     v => console.log('value:', v),       // never called
  error:    e => console.log('error:', e.message), // "something went wrong"
  complete: () => console.log('complete')        // never called
});

// Recover with catchError
throwError(() => new Error('fail')).pipe(
  catchError(err => {
    console.log('caught:', err.message);
    return of('fallback');
  })
).subscribe(console.log); // 'fallback'
```

### Common Pattern — Guard Validation in a Pipe
```typescript
import { throwError, of } from 'rxjs';
import { mergeMap } from 'rxjs/operators';

interface FormData { email: string; age: number }

function validateForm(data: FormData): Observable<FormData> {
  if (!data.email.includes('@')) {
    return throwError(() => new Error('Invalid email'));
  }
  if (data.age < 18) {
    return throwError(() => new Error('Must be 18 or older'));
  }
  return of(data); // valid — pass through
}

formSubmit$.pipe(
  mergeMap(data => validateForm(data)),
  mergeMap(data => submitToApi(data))
).subscribe({
  next:  result => showSuccess(result),
  error: err    => showValidationError(err.message)
});
```

### Common Pattern — Selective Re-throw in `catchError`
```typescript
import { throwError, of } from 'rxjs';
import { catchError } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

ajax.getJSON('/api/data').pipe(
  catchError(err => {
    // Handle 404 gracefully, re-throw everything else
    if (err.status === 404) {
      return of(null); // not found is OK — return null
    }
    return throwError(() => err); // re-throw 500, network errors, etc.
  })
).subscribe({
  next:  data => data ? renderData(data) : showEmptyState(),
  error: err  => showCriticalError(err)
});
```

### Common Pattern — Custom Error Types
```typescript
import { throwError } from 'rxjs';
import { mergeMap, retry } from 'rxjs/operators';

class RetryableError extends Error {
  constructor(message: string, public readonly retryable: boolean) {
    super(message);
    this.name = 'RetryableError';
  }
}

apiCall$.pipe(
  mergeMap(res =>
    res.rateLimited
      ? throwError(() => new RetryableError('Rate limited', true))
      : of(res.data)
  ),
  retry({
    count: 3,
    delay: (err) => {
      if (err instanceof RetryableError && err.retryable) return timer(1000);
      return throwError(() => err); // non-retryable — stop immediately
    }
  })
).subscribe(handleData);
```

## Common Pitfalls

### Anti-pattern: Using the Deprecated Value Form (RxJS 7+)
```typescript
import { throwError } from 'rxjs';
import { retry } from 'rxjs/operators';

// ❌ DEPRECATED — same Error instance reused across all retries
throwError(new Error('shared error')).pipe(
  retry(3)
).subscribe({ error: e => console.log(e) });
// All 3 retry attempts share the exact same Error object
// Mutations to the error object in one handler affect others

// ✅ CORRECT — factory creates a fresh Error per subscription/retry
throwError(() => new Error('fresh error')).pipe(
  retry(3)
).subscribe({ error: e => console.log(e) });
// Each retry gets its own Error instance

// WHY: The factory form () => error is called fresh on each subscription.
// This prevents subtle bugs where a single Error object is mutated
// (e.g., stack traces modified, custom properties added) by one subscriber
// and those mutations are visible to others or to retry attempts.
```

### Anti-pattern: `throw` Inside an Observable Pipe vs `throwError`
```typescript
import { of } from 'rxjs';
import { map } from 'rxjs/operators';

// ❌ INCORRECT — throwing synchronously inside map() works but is not idiomatic
of(1, 2, 3).pipe(
  map(n => {
    if (n === 2) throw new Error('bad value'); // works, but avoid
    return n * 10;
  })
).subscribe({ error: e => console.log('caught:', e.message) }); // caught: bad value

// ✅ IDIOMATIC — use throwError inside mergeMap/switchMap for async clarity
import { throwError } from 'rxjs';
import { mergeMap } from 'rxjs/operators';
of(1, 2, 3).pipe(
  mergeMap(n =>
    n === 2
      ? throwError(() => new Error('bad value'))
      : of(n * 10)
  )
).subscribe({ error: e => console.log('caught:', e.message) });

// WHY: Both work (RxJS catches synchronous throws from operators and converts
// them to error notifications). However, throwError inside mergeMap/switchMap
// makes the intent explicit and aligns with the Observable contract of
// returning an Observable rather than throwing synchronously.
// Use throw inside pure transformation operators (map, filter); use throwError
// in projection operators (mergeMap, switchMap) that return Observables.
```

## Related Operators

- **`EMPTY`**: Emits nothing, completes — the "no error, no value" creation constant
- **`NEVER`**: Emits nothing, never completes — the "permanently silent" constant
- **`of(value)`**: Emits one or more values and completes — the "happy path" creation constant
- **`catchError`**: Handles errors; `throwError` inside `catchError` re-throws selectively
- **`retry`**: Resubscribes on error — `throwError` inside `retry.delay` can abort retrying

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/index/function/throwError](https://rxjs.dev/api/index/function/throwError)

---

**Cognitive Load**: 1/5 | **Usage Frequency**: 4/5 | **Composability**: 5/5
**Key teaching points**:
1. Always use the factory form `throwError(() => error)` in RxJS 7+ — not `throwError(error)`
2. `throwError` completes the four creation primitives: `of` / `EMPTY` / `NEVER` / `throwError`
3. Primary use: guards in `mergeMap`/`switchMap`, selective re-throw in `catchError`, custom error types in `retry.delay`
