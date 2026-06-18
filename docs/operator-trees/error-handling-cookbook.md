# Error Handling Cookbook

Advanced error handling compositions beyond `catchError` and basic `retry`. For fundamentals, see [Error Handling Patterns](./error-handling-patterns).

---

## Recipe 1: Circuit Breaker

Stop trying after N consecutive failures; reopen after a cooldown period.

```typescript
import { BehaviorSubject, throwError, timer, of } from 'rxjs';
import { catchError, switchMap, tap } from 'rxjs/operators';

class CircuitBreaker {
  private failures = 0;
  private state$ = new BehaviorSubject<'closed' | 'open' | 'half-open'>('closed');

  wrap<T>(source$: Observable<T>): Observable<T> {
    return this.state$.pipe(
      switchMap(state => {
        if (state === 'open') {
          return throwError(() => new Error('Circuit open'));
        }
        return source$.pipe(
          tap({ next: () => this.onSuccess() }),
          catchError(err => {
            this.onFailure();
            return throwError(() => err);
          })
        );
      })
    );
  }

  private onSuccess() {
    this.failures = 0;
    this.state$.next('closed');
  }

  private onFailure() {
    if (++this.failures >= 5) {
      this.state$.next('open');
      timer(30_000).subscribe(() => this.state$.next('half-open')); // retry after 30s
    }
  }
}

const breaker = new CircuitBreaker();
breaker.wrap(apiCall$).subscribe({
  next:  data => render(data),
  error: e    => e.message === 'Circuit open' ? showCircuitOpen() : showError(e)
});
```

---

## Recipe 2: Retry Budget

Allow a fixed total number of retries across a session — not per-request.

```typescript
import { BehaviorSubject, throwError, timer } from 'rxjs';
import { catchError, retry } from 'rxjs/operators';

function withRetryBudget<T>(
  source$: Observable<T>,
  budget: { remaining: number },
  delayMs = 1000
): Observable<T> {
  return source$.pipe(
    retry({
      count: budget.remaining,
      delay: (err, attempt) => {
        budget.remaining--;
        if (budget.remaining <= 0) return throwError(() => err);
        return timer(delayMs * attempt);
      }
    })
  );
}

// Shared budget across multiple calls in a session:
const sessionBudget = { remaining: 10 };
withRetryBudget(apiCall$, sessionBudget).subscribe();
withRetryBudget(apiCall2$, sessionBudget).subscribe(); // shares the 10 retries
```

---

## Recipe 3: Conditional Retry (by Error Type)

```typescript
import { retry, catchError, throwError, timer } from 'rxjs';

class NetworkError extends Error {}
class AuthError extends Error {}
class NotFoundError extends Error {}

apiCall$.pipe(
  retry({
    count: 5,
    delay: (err: unknown, attempt: number) => {
      if (err instanceof NetworkError) {
        return timer(Math.min(1000 * 2 ** attempt, 30_000)); // exponential backoff
      }
      if (err instanceof AuthError) {
        return this.refreshToken().pipe(
          catchError(() => throwError(() => err)) // fail fast if refresh fails
        );
      }
      // Don't retry client errors (404, 400, etc.):
      return throwError(() => err);
    }
  }),
  catchError(err => {
    this.logger.error(err);
    return of(FALLBACK);
  })
).subscribe(render);
```

---

## Recipe 4: Error Boundary (Isolate Inner Errors)

Prevent one failing item from killing a stream processing many items.

```typescript
import { mergeMap, catchError, of } from 'rxjs/operators';

type Result<T> =
  | { ok: true;  value: T }
  | { ok: false; error: string; id: string };

// Process items in parallel — failures don't stop the stream:
from(itemIds).pipe(
  mergeMap(id =>
    fetchItem(id).pipe(
      map(value => ({ ok: true, value } as Result<Item>)),
      catchError(err => of({ ok: false, error: err.message, id } as Result<Item>))
    )
  )
).subscribe(result => {
  if (result.ok) renderItem(result.value);
  else           logFailure(result.error, result.id);
});
```

---

## Recipe 5: Timeout with Fallback (No Error)

Convert a slow response into a fallback value without surfacing an error.

```typescript
import { timeout } from 'rxjs/operators';
import { of } from 'rxjs';

apiCall$.pipe(
  timeout({
    each: 5000,
    with: () => of(STALE_CACHED_DATA) // return stale data instead of error
  })
).subscribe(render);

// With multiple fallback levels:
primaryApi$.pipe(
  timeout({
    each: 3000,
    with: () => fallbackApi$.pipe(
      timeout({
        each: 2000,
        with: () => of(DEFAULT_VALUE)
      })
    )
  })
).subscribe(render);
```

---

## Recipe 6: Global Error Handler

Intercept all errors in a shared stream without consuming them.

```typescript
import { tap, catchError, throwError } from 'rxjs/operators';

function withErrorLogging<T>(tag: string): MonoTypeOperatorFunction<T> {
  return catchError(err => {
    // Log but re-throw — don't swallow the error:
    console.error(`[${tag}]`, err);
    this.analytics.trackError(tag, err);
    return throwError(() => err);
  });
}

// Apply globally in a service:
apiCall$.pipe(
  withErrorLogging('user-api'),
  retry(2),
  catchError(() => of(FALLBACK)) // this one consumes it
).subscribe(render);
```

---

## Recipe 7: Graceful Degradation Chain

Try sources in order; fall back gracefully on failure.

```typescript
import { catchError, concat } from 'rxjs';

// Try live API → cache → hardcoded default:
liveData$.pipe(
  catchError(() => cachedData$.pipe(
    tap(() => console.warn('Using cached data')),
    catchError(() => of(DEFAULT_DATA).pipe(
      tap(() => console.warn('Using default data'))
    ))
  ))
).subscribe(render);

// Or more readable with a helper:
function fallbackChain<T>(...sources: Observable<T>[]): Observable<T> {
  return sources.reduce((acc, src) =>
    acc.pipe(catchError(() => src))
  );
}

fallbackChain(liveData$, cachedData$, of(DEFAULT_DATA)).subscribe(render);
```

---

## Recipe 8: Error Rate Monitoring

Count errors in a time window without stopping the stream.

```typescript
import { Subject, window, mergeMap, filter, count } from 'rxjs';

const errors$ = new Subject<Error>();
const alerts$ = new Subject<string>();

// Alert if > 5 errors within 60 seconds:
errors$.pipe(
  window(interval(60_000)),            // 1-minute windows
  mergeMap(win$ => win$.pipe(count())),
  filter(count => count > 5)
).subscribe(count =>
  alerts$.next(`High error rate: ${count} errors in last 60s`)
);

// Instrument any stream:
apiCall$.pipe(
  catchError(err => {
    errors$.next(err);   // report to monitor
    return throwError(() => err); // re-throw
  })
).subscribe({ error: showUserError });
```

---

## Recipe 9: Idempotent Retry (Deduplication Key)

Avoid duplicate side effects when retrying non-idempotent operations.

```typescript
import { retry, defer } from 'rxjs';

function createOrder(data: OrderData): Observable<Order> {
  const idempotencyKey = crypto.randomUUID(); // generated once, reused on retry

  return defer(() =>
    this.http.post<Order>('/api/orders', data, {
      headers: { 'Idempotency-Key': idempotencyKey }
      // Server deduplicates by key — safe to retry
    })
  ).pipe(
    retry({
      count: 3,
      delay: (_, n) => timer(1000 * n)
    })
  );
}
```

---

## Decision Guide

| Situation | Recipe |
|---|---|
| Too many failures → stop trying | Circuit Breaker |
| Limit total retries across session | Retry Budget |
| Different errors need different strategies | Conditional Retry |
| One item failing kills everything | Error Boundary |
| Slow response → use stale data | Timeout with Fallback |
| Log all errors without swallowing | Global Error Handler |
| Multiple fallback sources | Degradation Chain |
| Non-idempotent operation retried | Idempotency Key |
