# timeout — Advanced Patterns

For `timeout` fundamentals see the core [timeout](./timeout) doc. This page covers per-operation timeouts, fallback strategies, progressive timeouts, and integration with retry.

---

## `timeout` Config Object (RxJS 7)

RxJS 7 added a rich config form that supports fallback Observables:

```typescript
import { timeout } from 'rxjs/operators';
import { of } from 'rxjs';

// Simple ms form — throws TimeoutError after N ms:
source$.pipe(timeout(3000))

// Config form — with fallback Observable:
source$.pipe(
  timeout({
    each: 3000,                          // ms per value (between emissions)
    first: 5000,                         // ms for first emission only
    with: () => of(DEFAULT_VALUE),       // fallback Observable on timeout
    meta: { operationId: 'fetch-user' }  // included in TimeoutError
  })
)
```

---

## Pattern 1: Timeout with Fallback Value

The most common production pattern — don't error, use cached/default data:

```typescript
import { timeout, catchError } from 'rxjs/operators';
import { of } from 'rxjs';

// RxJS 7 — clean fallback via `with`:
this.api.getUser(id).pipe(
  timeout({
    each: 3000,
    with: () => of(this.cache.getUser(id) ?? DEFAULT_USER)
  })
).subscribe(renderUser);

// RxJS 6 / universal — catchError after timeout:
this.api.getUser(id).pipe(
  timeout(3000),
  catchError(err =>
    err.name === 'TimeoutError'
      ? of(this.cache.getUser(id) ?? DEFAULT_USER)
      : throwError(() => err)
  )
).subscribe(renderUser);
```

---

## Pattern 2: Different Timeouts for First vs Subsequent Values

```typescript
import { timeout } from 'rxjs/operators';

// Long-running streams: generous first-value timeout, tight per-value timeout:
this.eventStream$.pipe(
  timeout({
    first: 10_000,  // allow 10s for connection/first event
    each:  2_000    // after that, expect events at least every 2s
  })
).subscribe({
  next:  handleEvent,
  error: err => {
    if (err.name === 'TimeoutError') reconnect();
  }
});
```

---

## Pattern 3: Per-Operation Timeout in a Pool

Apply different timeouts to different operations:

```typescript
import { mergeMap, timeout, catchError } from 'rxjs/operators';
import { of } from 'rxjs';

interface Operation {
  type: 'fast' | 'normal' | 'slow';
  execute: () => Observable<unknown>;
}

const TIMEOUTS = { fast: 500, normal: 3000, slow: 30_000 };

operations$.pipe(
  mergeMap(op =>
    op.execute().pipe(
      timeout(TIMEOUTS[op.type]),
      catchError(err =>
        err.name === 'TimeoutError'
          ? of({ error: `${op.type} operation timed out`, op })
          : throwError(() => err)
      )
    ),
    5 // max 5 concurrent operations
  )
).subscribe(handleResult);
```

---

## Pattern 4: Progressive Timeout (Tighten Over Time)

Start lenient, get stricter on subsequent attempts:

```typescript
import { defer, timeout, retry } from 'rxjs';

let attempt = 0;

defer(() => this.api.fetchReport()).pipe(
  timeout(10_000 / (attempt + 1)), // 10s, 5s, 3.3s, 2.5s...
  retry({
    count: 3,
    delay: (_, n) => {
      attempt = n;
      return timer(500 * n);
    }
  }),
  finalize(() => { attempt = 0; }) // reset on completion
).subscribe(render);
```

---

## Pattern 5: Timeout as SLA Monitoring

Track which operations exceed SLA and log them:

```typescript
import { timeout, tap, catchError } from 'rxjs/operators';
import { timer } from 'rxjs';

const SLA_MS = 2000;

function withSlaMonitoring<T>(
  operationName: string,
  source$: Observable<T>
): Observable<T> {
  const start = Date.now();
  return source$.pipe(
    tap({
      next:     () => {
        const elapsed = Date.now() - start;
        if (elapsed > SLA_MS * 0.8) {
          monitoring.warn(`${operationName} near SLA: ${elapsed}ms`);
        }
      },
      complete: () => {
        monitoring.record(operationName, Date.now() - start);
      }
    }),
    timeout({
      each: SLA_MS,
      with: () => {
        monitoring.breach(operationName, SLA_MS);
        return throwError(() => new SlaBreachError(operationName, SLA_MS));
      }
    })
  );
}
```

---

## Pattern 6: Timeout + Retry + Fallback Chain

The full resilience pattern:

```typescript
import { timeout, retry, catchError, of } from 'rxjs/operators';
import { timer } from 'rxjs';

this.api.getData().pipe(
  timeout(5000),                              // fail if no response in 5s
  retry({                                     // retry up to 3 times on timeout or error
    count: 3,
    delay: (err, attempt) =>
      err.name === 'TimeoutError'
        ? timer(1000 * attempt)               // backoff for timeouts
        : throwError(() => err)               // don't retry non-timeout errors
  }),
  catchError(err => {                         // after 3 retries, use fallback
    logger.error('getData failed after retries', err);
    return this.cache.getData().pipe(
      catchError(() => of(EMPTY_STATE))       // if cache also fails, use empty state
    );
  })
).subscribe(render);
```

---

## Pattern 7: Race Between Timeout and User Cancellation

```typescript
import { race, timer, Subject } from 'rxjs';
import { map, switchMap } from 'rxjs/operators';

const cancel$ = new Subject<void>();

race(
  this.api.getLongReport().pipe(map(r => ({ result: r, cancelled: false }))),
  cancel$.pipe(map(() => ({ result: null, cancelled: true }))),
  timer(30_000).pipe(map(() => ({ result: null, cancelled: false, timedOut: true })))
).pipe(take(1)).subscribe(({ result, cancelled, timedOut }) => {
  if (result)    renderReport(result);
  if (cancelled) showCancelledMessage();
  if (timedOut)  showTimeoutMessage();
});

cancelButton.addEventListener('click', () => cancel$.next());
```

---

## `timeout` vs `takeUntil(timer(...))` vs `race`

```typescript
// timeout — throws TimeoutError (or uses fallback Observable):
source$.pipe(timeout(3000))
// ✓ Cleanest. Error tells you WHY it stopped.

// takeUntil(timer(...)) — completes silently after N ms:
source$.pipe(takeUntil(timer(3000)))
// ✓ No error thrown. Subscriber sees complete, not error. Good for "give up silently."

// race with timer — emit fallback value when timeout wins:
race(source$, timer(3000).pipe(map(() => FALLBACK)))
// ✓ Most explicit. Fallback value emitted rather than error or silent complete.
```

---

## Common Pitfalls

### Timeout Applies to Silence Between Emissions, Not Total Duration

```typescript
// ❌ Misconception: timeout(5000) means "give up after 5 seconds total"
// ACTUAL behavior of timeout({ each: 5000 }): fail if 5s passes WITHOUT A NEW EMISSION
// A source that emits at 4.9s, 9.8s, 14.7s will NEVER timeout!

// ✅ For a total wall-clock deadline, use takeUntil or race:
race(
  slowSource$,
  timer(5000).pipe(switchMap(() => throwError(() => new Error('Total deadline exceeded'))))
)
```

### Not Distinguishing `TimeoutError` from Other Errors in `catchError`

```typescript
// ❌ Catches ALL errors — hides non-timeout failures:
source$.pipe(
  timeout(3000),
  catchError(() => of(FALLBACK)) // hides 401, 500, network errors!
)

// ✅ Only catch TimeoutError, re-throw others:
source$.pipe(
  timeout(3000),
  catchError(err =>
    err.name === 'TimeoutError'
      ? of(FALLBACK)
      : throwError(() => err) // propagate non-timeout errors
  )
)
```

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 3/5 | **Composability**: 4/5
**Key rule**: Prefer `timeout({ each: ms, with: () => fallback$ })` over `timeout(ms)` + `catchError` in RxJS 7 — the `with` fallback is cleaner and makes intent obvious. Always distinguish `TimeoutError` from other errors when using `catchError` after `timeout`.
