# Error Handling Patterns

A practical guide to the four error-handling strategies in RxJS and when to use each.

---

## The Observable Error Contract

An error in an Observable terminates the stream — after an error, no more `next` or `complete` notifications arrive. This is intentional: the stream is in an unknown state and cannot safely continue.

**Consequence**: All error handling must happen at the boundary — either recover before the error propagates, or let it propagate and handle it in the subscriber.

---

## Strategy 1: `catchError` — Recover with a Fallback

Replace the errored Observable with a fallback Observable. The subscriber never sees the error.

```typescript
import { catchError, EMPTY, of } from 'rxjs';

// Replace error with empty completion:
source$.pipe(
  catchError(() => EMPTY)
).subscribe(console.log); // stream ends silently on error

// Replace error with a default value:
source$.pipe(
  catchError(() => of(DEFAULT_VALUE))
).subscribe(console.log);

// Replace error with another Observable (retry with different source):
primarySource$.pipe(
  catchError(() => fallbackSource$)
).subscribe(console.log);

// Re-throw after logging (preserve error for subscriber):
source$.pipe(
  catchError(err => {
    logger.error(err);
    throw err; // or return throwError(() => err)
  })
).subscribe({ error: e => showUserError(e) });
```

**Use when**: The error is expected and you have a meaningful fallback. Keeps the stream alive for the subscriber.

---

## Strategy 2: `retry` / `retryWhen` — Resubscribe on Error

Re-subscribe to the source Observable after an error, optionally with a delay.

```typescript
import { retry } from 'rxjs/operators';
import { timer } from 'rxjs';

// Retry up to 3 times immediately:
source$.pipe(retry(3)).subscribe();

// Retry with exponential backoff (RxJS 7):
source$.pipe(
  retry({
    count: 5,
    delay: (error, attempt) => timer(Math.min(1000 * 2 ** attempt, 30_000))
  })
).subscribe();

// Retry only on specific error types:
source$.pipe(
  retry({
    count: 3,
    delay: (err) => err instanceof NetworkError ? timer(1000) : throwError(() => err)
  })
).subscribe();
```

**Key rule**: `retry` must come **before** `catchError`. If `catchError` handles the error first, `retry` never sees it.

```typescript
// ❌ WRONG ORDER — catchError swallows error before retry sees it
source$.pipe(
  catchError(() => EMPTY), // handles it first
  retry(3)                 // never triggers
)

// ✅ CORRECT ORDER
source$.pipe(
  retry(3),              // retries first
  catchError(() => EMPTY) // handles if all retries exhausted
)
```

**Use when**: The error is transient (network timeout, rate limit) and the same operation may succeed on a subsequent attempt.

---

## Strategy 3: `timeout` — Error on Silence

Convert a slow or stalled stream into an error.

```typescript
import { timeout } from 'rxjs/operators';
import { TimeoutError } from 'rxjs';

// Error if no emission within 5s:
source$.pipe(
  timeout(5000)
).subscribe({ error: e => e instanceof TimeoutError && showTimeout() });

// With fallback Observable (no error):
source$.pipe(
  timeout({ each: 5000, with: () => of(DEFAULT_VALUE) })
).subscribe();

// Combine with retry for resilient polling:
pollEndpoint$.pipe(
  timeout({ each: 3000 }),
  retry({ count: 3, delay: () => timer(1000) }),
  catchError(() => of(STALE_DATA))
).subscribe(render);
```

**Use when**: You need an SLA guarantee — the stream must emit within N ms or be considered failed.

---

## Strategy 4: `onErrorResumeNext` — Continue After Error (Silent)

Silently swallow errors and continue with the next source.

```typescript
import { onErrorResumeNext } from 'rxjs';

// Try three sources — errors are silently ignored
onErrorResumeNext(
  primarySource$,
  fallbackSource$,
  defaultSource$
).subscribe(render);
```

**Use when**: Best-effort chains where error visibility is genuinely unnecessary. Prefer `catchError` when you need to know what failed.

---

## Isolating Inner Observable Errors (mergeMap / switchMap)

A key pattern: errors inside inner Observables (inside `mergeMap`, `switchMap`, etc.) propagate to the outer stream and kill it. Isolate errors per-inner with `catchError` inside the projection:

```typescript
import { mergeMap, catchError, of } from 'rxjs/operators';

// ❌ ONE FAILURE KILLS EVERYTHING
from(ids).pipe(
  mergeMap(id => fetchItem(id)) // one 404 ends the stream
).subscribe();

// ✅ ISOLATE PER-ITEM ERRORS
from(ids).pipe(
  mergeMap(id =>
    fetchItem(id).pipe(
      catchError(err => of({ id, error: err.message })) // item fails gracefully
    )
  )
).subscribe(result => {
  if ('error' in result) logFailure(result);
  else renderItem(result);
});
```

---

## Decision Table

| Situation | Strategy | Operator |
|---|---|---|
| Expected error, known fallback | Recover | `catchError(() => fallback$)` |
| Transient error (network, rate limit) | Retry | `retry({ count: 3, delay: ... })` |
| Stream too slow / stalled | Timeout | `timeout(ms)` |
| Multiple fallback sources, drop errors | Continue | `onErrorResumeNext(a$, b$, c$)` |
| Error in inner Observable killing outer | Isolate | `mergeMap(v => inner$.pipe(catchError(...)))` |
| Error must reach subscriber | Pass through | Re-throw in `catchError` or no handler |

---

## Composing Strategies

Real-world resilience often combines multiple strategies:

```typescript
// Resilient API call: isolate → retry → timeout → fallback
function fetchWithResilience<T>(url: string, fallback: T): Observable<T> {
  return ajax.getJSON<T>(url).pipe(
    timeout(5000),                                         // 5s SLA
    retry({ count: 3, delay: (_, n) => timer(500 * n) }), // 3 retries, backoff
    catchError(() => of(fallback))                         // final fallback
  );
}
```

**Order matters**:
1. `timeout` — converts silence to error (innermost)
2. `retry` — retries the timed-out attempt
3. `catchError` — handles if all retries fail (outermost)
