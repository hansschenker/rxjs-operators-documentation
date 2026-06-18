# Error Resilience Patterns with RxJS

Circuit breaker, bulkhead isolation, timeout chains, fallback hierarchies, and production-grade error handling beyond basic `catchError`.

---

## The Resilience Vocabulary

| Pattern | What it does | RxJS implementation |
|---|---|---|
| **Retry** | Re-attempt after failure | `retry({ count, delay })` |
| **Timeout** | Fail fast if too slow | `timeout({ each, with })` |
| **Fallback** | Return cached/default on failure | `catchError(() => of(fallback))` |
| **Circuit Breaker** | Stop trying after N failures | Custom state machine |
| **Bulkhead** | Isolate failures per domain | Separate `catchError` per source |
| **Retry Budget** | Cap total retry time | `retryWhen` + `scan` + `timer` |
| **Hedge** | Race primary against backup | `race([primary$, backup$])` |

---

## Pattern 1: Full Resilience Chain

The complete production stack — timeout + retry + fallback:

```typescript
import { timeout, retry, catchError } from 'rxjs/operators';
import { timer, of } from 'rxjs';

function resilientFetch<T>(
  source$: Observable<T>,
  fallback: T,
  options = { timeoutMs: 5000, retries: 3, retryDelayMs: 1000 }
): Observable<T> {
  return source$.pipe(
    timeout(options.timeoutMs),
    retry({
      count: options.retries,
      delay: (err, attempt) => {
        const isTimeout = err.name === 'TimeoutError';
        const delay = options.retryDelayMs * Math.pow(2, attempt - 1); // exponential
        return isTimeout || err.status >= 500
          ? timer(delay)
          : throwError(() => err); // don't retry 4xx
      }
    }),
    catchError(err => {
      logger.error('Resilient fetch failed after retries', err);
      return of(fallback);
    })
  );
}
```

---

## Pattern 2: Circuit Breaker

Stop calling a failing service after N consecutive failures; auto-recover after a cooldown:

```typescript
import { BehaviorSubject, throwError, timer } from 'rxjs';
import { switchMap, tap } from 'rxjs/operators';

type CircuitState = 'closed' | 'open' | 'half-open';

class CircuitBreaker {
  private state$    = new BehaviorSubject<CircuitState>('closed');
  private failures  = 0;
  private readonly THRESHOLD   = 5;
  private readonly COOLDOWN_MS = 30_000;

  wrap<T>(source$: Observable<T>): Observable<T> {
    const state = this.state$.getValue();

    if (state === 'open') {
      return throwError(() => new Error('Circuit open — service unavailable'));
    }

    return source$.pipe(
      tap({
        next: () => {
          // Success in half-open → close circuit:
          if (state === 'half-open') {
            this.failures = 0;
            this.state$.next('closed');
          }
        },
        error: () => {
          this.failures++;
          if (this.failures >= this.THRESHOLD) {
            this.state$.next('open');
            // Auto-reset to half-open after cooldown:
            timer(this.COOLDOWN_MS).subscribe(() => {
              this.state$.next('half-open');
            });
          }
        }
      }),
      catchError(err => {
        if (this.failures >= this.THRESHOLD) {
          return throwError(() => Object.assign(err, { circuitOpen: true }));
        }
        return throwError(() => err);
      })
    );
  }

  readonly status$ = this.state$.asObservable();
}

// Usage:
const breaker = new CircuitBreaker();

userRequest$.pipe(
  switchMap(req => breaker.wrap(this.api.call(req)))
).subscribe({
  next:  renderResult,
  error: err => err.circuitOpen ? showServiceUnavailable() : showError(err)
});
```

---

## Pattern 3: Bulkhead (Domain Isolation)

Prevent one failing domain from crashing unrelated streams:

```typescript
import { merge, EMPTY } from 'rxjs';
import { catchError, retry } from 'rxjs/operators';

function isolate<T>(
  source$: Observable<T>,
  label:   string,
  fallback?: T
): Observable<T> {
  return source$.pipe(
    retry({ count: 2, delay: 1000 }),
    catchError(err => {
      logger.error(`[${label}] isolated failure:`, err);
      return fallback !== undefined ? of(fallback) : EMPTY;
    })
  );
}

// Dashboard: one panel failing doesn't break the others:
merge(
  isolate(userMetrics$,    'users',    { count: 0, active: 0 }),
  isolate(orderMetrics$,   'orders',   { total: 0, pending: 0 }),
  isolate(systemMetrics$,  'system',   { cpu: 0, memory: 0 }),
  isolate(revenueMetrics$, 'revenue')  // no fallback — just hide on error
).subscribe(renderMetric);
```

---

## Pattern 4: Retry with Jitter (Thundering Herd Prevention)

```typescript
import { retry, timer } from 'rxjs/operators';

function retryWithJitter(maxRetries = 5, baseDelayMs = 1000) {
  return retry({
    count: maxRetries,
    delay: (_, attempt) => {
      const exponential = baseDelayMs * Math.pow(2, attempt - 1);
      const cap         = Math.min(exponential, 30_000);
      const jitter      = Math.random() * cap * 0.2; // ±20% jitter
      return timer(cap + jitter);
    }
  });
}

// Multiple clients all retry at slightly different times — no thundering herd:
this.api.getData().pipe(retryWithJitter()).subscribe(render);
```

---

## Pattern 5: Fallback Hierarchy (Cache → Stale → Default)

Try the best data source first, fall back progressively:

```typescript
import { of, EMPTY } from 'rxjs';
import { catchError, timeout, switchMap } from 'rxjs/operators';

function withFallbackHierarchy<T>(
  live$:    Observable<T>,
  cache:    () => T | null,
  stale$:   Observable<T>,
  defaults: T
): Observable<T> {
  return live$.pipe(
    timeout(3000),
    catchError(() => {
      const cached = cache();
      if (cached) return of(cached); // L2: in-memory cache

      return stale$.pipe(            // L3: stale/offline store
        catchError(() => of(defaults)) // L4: hardcoded defaults
      );
    })
  );
}

// Usage:
withFallbackHierarchy(
  this.api.getProducts(),
  () => this.memCache.get('products'),
  this.offlineDb.getProducts(),
  [] as Product[]
).subscribe(renderProducts);
```

---

## Pattern 6: Hedging (Race Primary Against Backup)

If primary is slow, fire backup after a delay — take whoever responds first:

```typescript
import { race, timer, EMPTY } from 'rxjs';
import { switchMap, tap } from 'rxjs/operators';

function hedge<T>(
  primary$:  Observable<T>,
  backup$:   Observable<T>,
  hedgeAfterMs = 500
): Observable<T> {
  const backup = timer(hedgeAfterMs).pipe(
    tap(() => logger.debug('Hedging to backup')),
    switchMap(() => backup$)
  );

  return race([primary$, backup]);
}

// Primary region, hedge to secondary after 500ms:
hedge(
  this.api.primary.getData(),
  this.api.secondary.getData(),
  500
).subscribe(render);
```

---

## Pattern 7: Retry Budget (Max Total Wait Time)

```typescript
import { retryWhen, scan, delayWhen, timer, throwError } from 'rxjs/operators';

function retryWithBudget<T>(source$: Observable<T>, budgetMs = 30_000) {
  const start = Date.now();

  return source$.pipe(
    retryWhen(errors =>
      errors.pipe(
        scan((attempt, err) => {
          const elapsed = Date.now() - start;
          if (elapsed >= budgetMs) throw new Error(`Retry budget exhausted after ${elapsed}ms`);
          return attempt + 1;
        }, 0),
        delayWhen(attempt => timer(Math.min(1000 * attempt, 5000)))
      )
    )
  );
}
```

---

## Resilience Decision Tree

```
Is the operation idempotent (safe to retry)?
├── No (write, payment) → catchError with rollback; no retry
└── Yes →
    Is it time-sensitive?
    ├── Yes → timeout(ms) first, then retry
    └── No →
        Is the service occasionally flaky?
        ├── Yes → retry(3) with exponential backoff
        └── Frequently down? → Circuit breaker + fallback
```

---

## Common Pitfalls

### Retrying Non-Idempotent Operations

```typescript
// ❌ Retrying a POST payment — may charge twice:
paymentRequest$.pipe(
  retry(3) // DANGEROUS for non-idempotent writes
)

// ✅ Only retry idempotent ops; use deduplication for writes:
paymentRequest$.pipe(
  retry({
    count: 3,
    delay: (err) =>
      err.status === 409 // idempotency key conflict — don't retry
        ? throwError(() => err)
        : timer(1000)
  })
)
```

### Catching Too Broadly — Hiding Bugs

```typescript
// ❌ Catches everything — 404 for missing resource same as 500:
source$.pipe(catchError(() => of(FALLBACK)))
// A programming error (wrong URL, missing auth) silently returns fallback

// ✅ Only catch recoverable errors:
source$.pipe(
  catchError(err => {
    if (err.status >= 500 || err.name === 'TimeoutError') return of(FALLBACK);
    throw err; // re-throw 4xx and programming errors
  })
)
```
