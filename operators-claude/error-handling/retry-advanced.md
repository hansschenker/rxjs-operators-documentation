# retry — Advanced Patterns

For `retry` fundamentals see the core [retry](./retry) doc. This page covers exponential backoff, conditional retry, jitter, budget-capped retry, and the comparison with `catchError`.

---

## `retry` vs `catchError` — The Division of Responsibility

```typescript
// retry: re-subscribe on error (transparent to subscriber)
// catchError: transform the error into a new Observable

// retry handles: transient failures (network hiccup, 503, timeout)
// catchError handles: permanent failures (404, auth error, validation)

// Together:
source$.pipe(
  retry({ count: 3, delay: 1000 }), // try 3 times on ANY error
  catchError(err => {                // handle the error after retries exhausted
    if (err.status === 404) return of(null);
    return throwError(() => err);
  })
)
```

---

## Pattern 1: Exponential Backoff

Double the wait time after each failure:

```typescript
import { retry } from 'rxjs/operators';
import { timer } from 'rxjs';

this.api.getData().pipe(
  retry({
    count: 5,
    delay: (error, retryCount) =>
      timer(1000 * Math.pow(2, retryCount - 1))
      // Delays: 1s, 2s, 4s, 8s, 16s
  })
).subscribe(render);
```

---

## Pattern 2: Exponential Backoff with Jitter

Jitter prevents all clients from retrying simultaneously (thundering herd):

```typescript
import { retry } from 'rxjs/operators';
import { timer } from 'rxjs';

function withExponentialBackoff(maxRetries = 5, baseMs = 1000, maxMs = 30_000) {
  return retry({
    count: maxRetries,
    delay: (error, attempt) => {
      const exponential = baseMs * Math.pow(2, attempt - 1);
      const capped       = Math.min(exponential, maxMs);
      const jitter       = Math.random() * capped * 0.2; // ±20% jitter
      return timer(capped + jitter);
    }
  });
}

// Usage:
this.api.createOrder(order).pipe(
  withExponentialBackoff(4, 500, 15_000)
).subscribe(handleSuccess);
```

---

## Pattern 3: Conditional Retry

Only retry on specific error types:

```typescript
import { retry, throwError } from 'rxjs/operators';
import { timer } from 'rxjs';

this.api.getData().pipe(
  retry({
    count: 3,
    delay: (err, attempt) => {
      // Only retry on transient server errors
      if (err instanceof HttpErrorResponse) {
        if (err.status >= 500) return timer(1000 * attempt); // 503, 502 → retry
        if (err.status === 429) return timer(5000);          // rate limit → wait 5s
        return throwError(() => err);                        // 4xx → don't retry
      }
      if (err instanceof TimeoutError) return timer(2000);   // timeout → retry
      return throwError(() => err);                          // unknown → don't retry
    }
  })
).subscribe(render);
```

---

## Pattern 4: Retry with Notification

Tell the user that a retry is happening:

```typescript
import { retry, tap } from 'rxjs/operators';
import { timer } from 'rxjs';

this.api.fetchReport().pipe(
  retry({
    count: 3,
    delay: (err, attempt) => {
      this.toast.info(`Connection failed — retrying (${attempt}/3)...`);
      return timer(2000 * attempt);
    }
  })
).subscribe({
  next:     report => this.render(report),
  complete: ()     => this.toast.dismiss(),
  error:    err    => this.toast.error(`Failed after 3 retries: ${err.message}`)
});
```

---

## Pattern 5: Reset Retry Counter on Success

RxJS 7's `resetOnSuccess` re-starts the counter after each successful emission:

```typescript
import { retry } from 'rxjs/operators';

// A long-running stream that should reset its retry budget after recovery:
this.websocket$.pipe(
  retry({
    count: 5,
    delay: (_, attempt) => timer(1000 * attempt),
    resetOnSuccess: true // reset to 0 retries on successful emission
  })
).subscribe(handleMessage);
// Without resetOnSuccess: 5 total failures for the entire lifetime of the stream
// With resetOnSuccess:    5 failures allowed between each successful emission
```

---

## Pattern 6: Budget-Capped Retry (Total Time Budget)

Retry until a total elapsed time budget is exhausted:

```typescript
import { retry, timer } from 'rxjs';

function withTimeBudget(budgetMs: number, delayMs = 1000) {
  const start = Date.now();
  let attempt = 0;

  return retry({
    delay: (err) => {
      attempt++;
      const elapsed   = Date.now() - start;
      const remaining = budgetMs - elapsed;

      if (remaining <= 0) {
        return throwError(() => new Error(
          `Retry budget of ${budgetMs}ms exhausted after ${attempt} attempts`
        ));
      }

      const wait = Math.min(delayMs, remaining);
      return timer(wait);
    }
  });
}

// Retry for up to 30 seconds:
this.api.getData().pipe(
  withTimeBudget(30_000, 2000)
).subscribe(render);
```

---

## Pattern 7: Retry with Circuit Breaker

After N failures in a time window, open the circuit and stop retrying:

```typescript
import { BehaviorSubject } from 'rxjs';
import { retry, throwError, tap } from 'rxjs/operators';
import { timer } from 'rxjs';

class CircuitBreaker {
  private failures = 0;
  private openUntil = 0;
  private readonly threshold = 5;
  private readonly cooldownMs = 60_000;

  operator<T>(): MonoTypeOperatorFunction<T> {
    return (source$) =>
      source$.pipe(
        tap({
          next:  ()  => { this.failures = 0; },    // success resets counter
          error: ()  => {
            this.failures++;
            if (this.failures >= this.threshold) {
              this.openUntil = Date.now() + this.cooldownMs;
            }
          }
        }),
        retry({
          delay: (err, attempt) => {
            if (Date.now() < this.openUntil) {
              // Circuit open — fail fast
              return throwError(() => new CircuitOpenError(
                `Circuit open for ${Math.round((this.openUntil - Date.now()) / 1000)}s`
              ));
            }
            return timer(1000 * attempt);
          }
        })
      );
  }
}

const breaker = new CircuitBreaker();
this.api.getData().pipe(breaker.operator()).subscribe(render);
```

---

## `retryWhen` — Deprecated in RxJS 7

`retryWhen` (RxJS 6) is superseded by `retry({ delay: fn })` in RxJS 7:

```typescript
// ❌ RxJS 6 retryWhen (deprecated):
source$.pipe(
  retryWhen(errors$ =>
    errors$.pipe(
      delayWhen((_, i) => timer(1000 * (i + 1)))
    )
  )
)

// ✅ RxJS 7 retry({ delay }):
source$.pipe(
  retry({
    delay: (_, attempt) => timer(1000 * attempt)
  })
)
// WHY: retryWhen is complex and easy to get wrong (forgetting to re-throw
// creates infinite loops). retry({ delay }) is explicit and composable.
```

---

## Common Pitfalls

### Retrying Non-Idempotent Operations

```typescript
// ❌ DANGEROUS — may create duplicate orders if the first request succeeded
//    but the response was lost (network timeout after server processed):
this.api.createOrder(order).pipe(
  retry({ count: 3 }) // might create 2-4 duplicate orders!
)

// ✅ Check idempotency first:
// - GET, HEAD, PUT (with full resource), DELETE → safe to retry
// - POST → NOT safe unless backend supports idempotency keys

// ✅ Add idempotency key for POST:
this.api.createOrder({ ...order, idempotencyKey: uuid() }).pipe(
  retry({ count: 3 }) // server deduplicates using idempotencyKey
)
```

### Infinite Retry Without Count

```typescript
// ❌ INFINITE RETRY — if the server is permanently down, this never stops
source$.pipe(
  retry({ delay: 1000 }) // no count — retries forever
)

// ✅ Always set a count for bounded retry:
source$.pipe(
  retry({ count: 5, delay: 1000 }) // bounded
)

// ✅ Or explicitly acknowledge infinite retry is intentional:
// WebSocket reconnect — intentionally infinite
ws$.pipe(
  retry({ delay: (_, n) => timer(Math.min(1000 * 2 ** n, 30_000)) })
  // Documented as intentional — will retry forever until unsubscribed
)
```

---

**Cognitive Load**: 3/5 | **Usage Frequency**: 4/5 | **Composability**: 4/5
**Key rule**: Always set `count` for request-level retry. Reserve infinite retry for long-lived connections (WebSocket, SSE). Use the `delay` function form for exponential backoff — it's cleaner and more flexible than `retryWhen`.
