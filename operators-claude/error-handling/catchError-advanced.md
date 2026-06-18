# catchError — Advanced Patterns

For `catchError` fundamentals see the core [catchError](./catchError) doc. This page covers recovery strategies, error transformation, swallowing vs re-throwing, and integration with retry.

---

## The Three Responses to an Error

```typescript
import { catchError, throwError, EMPTY, of } from 'rxjs';

source$.pipe(
  catchError(err => {
    // 1. RECOVER — return a fallback Observable
    return of(DEFAULT_VALUE);

    // 2. RE-THROW — propagate (possibly transformed)
    return throwError(() => new AppError(err.message));

    // 3. SWALLOW — complete silently
    return EMPTY;
  })
)
```

Choose based on whether the caller needs to know: recover for graceful degradation, re-throw for propagation, swallow for truly optional operations.

---

## Pattern 1: Error Transformation

Convert low-level errors to domain errors before they reach subscribers:

```typescript
import { catchError, throwError } from 'rxjs';
import { HttpErrorResponse } from '@angular/common/http';

class ApiError extends Error {
  constructor(
    public readonly statusCode: number,
    public readonly endpoint: string,
    message: string
  ) { super(message); }
}

function toApiError(endpoint: string) {
  return catchError((err: unknown) => {
    if (err instanceof HttpErrorResponse) {
      return throwError(() => new ApiError(
        err.status,
        endpoint,
        err.error?.message ?? err.message
      ));
    }
    return throwError(() => err);
  });
}

// Reusable throughout the service layer:
this.http.get<User>('/api/users/me').pipe(
  toApiError('/api/users/me')
).subscribe({
  error: (err: ApiError) => {
    if (err.statusCode === 401) this.auth.logout();
    if (err.statusCode === 403) this.router.navigate(['/forbidden']);
    this.logger.error(`API call to ${err.endpoint} failed`, err);
  }
});
```

---

## Pattern 2: Error Type Routing

Handle different error types differently in one `catchError`:

```typescript
import { catchError, throwError, of } from 'rxjs';

source$.pipe(
  catchError((err: unknown) => {
    if (err instanceof NetworkError)   return of(CACHED_DATA);      // offline — use cache
    if (err instanceof AuthError)      { this.auth.logout(); return EMPTY; }
    if (err instanceof ValidationError) return throwError(() => err); // caller must handle
    if (err instanceof TimeoutError)   return source$.pipe(take(1)); // retry once on timeout

    // Unknown — log and re-throw
    this.logger.error('Unexpected error', err);
    return throwError(() => err);
  })
)
```

---

## Pattern 3: `catchError` Inside vs Outside `switchMap`

The most critical placement decision:

```typescript
// ❌ INCORRECT — catchError OUTSIDE switchMap kills the entire stream on error
source$.pipe(
  switchMap(id => this.api.get(id)),
  catchError(err => of(null)) // One error terminates source$ subscription permanently
).subscribe(data => render(data)); // stops receiving new values after first error

// ✅ CORRECT — catchError INSIDE switchMap keeps outer stream alive
source$.pipe(
  switchMap(id =>
    this.api.get(id).pipe(
      catchError(err => of(null)) // each inner Observable handles its own errors
    )
  )
).subscribe(data => render(data)); // outer stream continues despite individual errors
// WHY: catchError terminates the Observable it's attached to. Outside means
// the outer (outer$) terminates. Inside means only the inner terminates —
// the switchMap resubscribes naturally on the next source emission.
```

---

## Pattern 4: Loading/Error State Machine

```typescript
import { startWith, catchError, map } from 'rxjs/operators';

type AsyncState<T> =
  | { status: 'loading' }
  | { status: 'success'; data: T }
  | { status: 'error'; error: string };

function toAsyncState<T>(): OperatorFunction<T, AsyncState<T>> {
  return (source$) =>
    source$.pipe(
      map((data): AsyncState<T>  => ({ status: 'success', data })),
      catchError((err): Observable<AsyncState<T>> =>
        of({ status: 'error', error: err.message })
      ),
      startWith<AsyncState<T>>({ status: 'loading' })
    );
}

// Usage:
readonly userState$ = this.api.getUser().pipe(toAsyncState<User>());

// Template:
// @switch (userState$ | async)?.status {
//   @case ('loading')  { <spinner /> }
//   @case ('success')  { <user-card [user]="userState$.data" /> }
//   @case ('error')    { <error-message [msg]="userState$.error" /> }
// }
```

---

## Pattern 5: Global Error Handler vs Local

```typescript
// Local handler — recover per-operation:
this.api.getUser(id).pipe(
  catchError(() => of(null)) // this operation gracefully returns null
)

// Global handler — log + re-throw:
function globalErrorHandler<T>(): OperatorFunction<T, T> {
  return catchError((err: unknown) => {
    logger.captureException(err);
    analytics.trackError(err);
    return throwError(() => err); // re-throw after logging
  });
}

// Combine both:
this.api.getUser(id).pipe(
  globalErrorHandler(), // log everything
  catchError(() => of(null)) // then recover locally
)
```

---

## Pattern 6: `catchError` with Retry

```typescript
import { catchError, retry, throwError } from 'rxjs';

// Retry first, recover if all retries exhausted:
this.api.getData().pipe(
  retry({ count: 3, delay: 1000 }),
  catchError(err => {
    // Only called after all 3 retries fail
    this.notification.warn('Data temporarily unavailable — showing cached data');
    return this.cache.getData();
  })
).subscribe(render);
```

---

## Pattern 7: `catchError` for Observable Composition Safety

When composing many Observables, protect against individual failures:

```typescript
import { merge, EMPTY } from 'rxjs';
import { catchError } from 'rxjs/operators';

function safe<T>(source$: Observable<T>): Observable<T> {
  return source$.pipe(catchError(() => EMPTY));
}

// Merge N streams — one error won't kill the others:
merge(
  safe(userEvents$),
  safe(systemEvents$),
  safe(analyticsStream$)
).subscribe(handleEvent);
```

---

## Common Pitfalls

### Re-throwing Without `throwError()`

```typescript
// ❌ INCORRECT — throwing synchronously in catchError creates an uncaught error
source$.pipe(
  catchError(err => {
    throw new Error(err.message); // synchronous throw bypasses RxJS error channel
  })
)

// ✅ CORRECT — always use throwError() to stay in the Observable world
source$.pipe(
  catchError(err => throwError(() => new Error(err.message)))
)
// WHY: catchError expects an Observable return value. A synchronous throw
// escapes the Observable error handling and may go unhandled.
```

### Catching Errors You Should Let Propagate

```typescript
// ❌ SWALLOWS ALL ERRORS — caller can never distinguish success from failure
source$.pipe(
  catchError(() => of(null))
).subscribe(data => {
  if (data === null) console.log('might be error, might be null data');
});

// ✅ Only catch errors you can meaningfully handle:
source$.pipe(
  catchError(err => {
    if (err.status === 404) return of(null); // expected — resource not found
    return throwError(() => err);            // unexpected — let caller handle
  })
)
```

---

**Cognitive Load**: 3/5 | **Usage Frequency**: 5/5 | **Composability**: 5/5
**Key rule**: The placement of `catchError` is everything — inside a flattening operator (switchMap/mergeMap) means per-inner-Observable recovery; outside means the entire outer stream terminates on error.
