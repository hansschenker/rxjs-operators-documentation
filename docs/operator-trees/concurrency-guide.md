# Concurrency Patterns with RxJS

Managing concurrent Observables — limiting parallelism, request pooling, queue management, and backpressure strategies.

---

## The Four Concurrency Models

```typescript
// mergeMap — UNBOUNDED concurrency: all inner Observables run simultaneously
source$.pipe(mergeMap(id => api.get(id)))            // N requests at once

// mergeMap(fn, N) — BOUNDED concurrency: max N inner Observables at a time
source$.pipe(mergeMap(id => api.get(id), 3))         // max 3 requests at once

// concatMap — SERIAL: one at a time, queued
source$.pipe(concatMap(id => api.get(id)))           // 1 request at a time

// exhaustMap — DROP: ignore new while current is in flight
source$.pipe(exhaustMap(id => api.get(id)))          // skip if busy

// switchMap — CANCEL: cancel current, start new
source$.pipe(switchMap(id => api.get(id)))           // always latest
```

---

## Pattern 1: Bounded Request Pool

Limit the number of concurrent HTTP requests (avoids overwhelming servers or hitting rate limits):

```typescript
import { from, mergeMap, toArray } from 'rxjs';

// Process up to 5 items concurrently:
const ids = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'];

from(ids).pipe(
  mergeMap(id => this.api.fetchItem(id), 5), // max 5 concurrent
  toArray()
).subscribe(results => {
  console.log(`All ${results.length} items loaded`);
  render(results);
});
// Items 1-5 start immediately; 6 starts when any of 1-5 finishes, etc.
```

---

## Pattern 2: Dynamic Concurrency Based on Network Conditions

```typescript
import { BehaviorSubject, mergeMap, switchMap } from 'rxjs';

// Adjust concurrency based on available bandwidth:
const concurrency$ = new BehaviorSubject(3);

// Reduce concurrency on slow connection:
navigator.connection?.addEventListener('change', () => {
  const effectiveType = (navigator.connection as any).effectiveType;
  const limit = { '4g': 6, '3g': 3, '2g': 1, 'slow-2g': 1 }[effectiveType] ?? 3;
  concurrency$.next(limit);
});

// Use current concurrency for each batch:
uploadQueue$.pipe(
  switchMap(files =>
    from(files).pipe(
      mergeMap(file => this.upload(file), concurrency$.getValue())
    )
  )
).subscribe(onProgress);
```

---

## Pattern 3: Request Queue with Priority

```typescript
import { Subject, merge, mergeMap } from 'rxjs';
import { map } from 'rxjs/operators';

type Priority = 'high' | 'normal' | 'low';

interface QueuedRequest<T> {
  id: string;
  priority: Priority;
  execute: () => Observable<T>;
}

class PriorityRequestQueue<T> {
  private high$   = new Subject<QueuedRequest<T>>();
  private normal$ = new Subject<QueuedRequest<T>>();
  private low$    = new Subject<QueuedRequest<T>>();

  // Results stream — high priority requests run with more concurrency slots
  readonly results$ = merge(
    this.high$.pipe(mergeMap(req => req.execute().pipe(map(r => ({ id: req.id, result: r }))), 4)),
    this.normal$.pipe(mergeMap(req => req.execute().pipe(map(r => ({ id: req.id, result: r }))), 2)),
    this.low$.pipe(mergeMap(req => req.execute().pipe(map(r => ({ id: req.id, result: r }))), 1))
  );

  enqueue(request: QueuedRequest<T>): void {
    switch (request.priority) {
      case 'high':   this.high$.next(request);   break;
      case 'normal': this.normal$.next(request); break;
      case 'low':    this.low$.next(request);    break;
    }
  }
}
```

---

## Pattern 4: Retry with Concurrency Backoff

When requests fail, reduce concurrency to avoid thundering herd:

```typescript
import { BehaviorSubject, from, mergeMap, tap, catchError } from 'rxjs';
import { of } from 'rxjs';

const concurrency$ = new BehaviorSubject(5);
let errorCount = 0;

from(ids).pipe(
  mergeMap(id =>
    this.api.fetchItem(id).pipe(
      tap(() => {
        // Recover concurrency on success
        if (errorCount > 0) {
          errorCount = Math.max(0, errorCount - 1);
          if (errorCount === 0) concurrency$.next(5);
        }
      }),
      catchError(err => {
        errorCount++;
        // Reduce concurrency on repeated errors
        const newConcurrency = Math.max(1, concurrency$.getValue() - 1);
        concurrency$.next(newConcurrency);
        return of(null); // null = failed item
      })
    ),
    concurrency$.getValue()
  )
).subscribe();
```

---

## Pattern 5: Fan-Out / Fan-In

Scatter work across multiple workers, collect results:

```typescript
import { forkJoin, from, mergeMap, toArray, groupBy, mergeAll } from 'rxjs';

// Fan-out: split work into chunks, process each chunk concurrently:
function chunk<T>(arr: T[], size: number): T[][] {
  return Array.from({ length: Math.ceil(arr.length / size) }, (_, i) =>
    arr.slice(i * size, (i + 1) * size)
  );
}

const items = Array.from({ length: 100 }, (_, i) => i);
const chunks = chunk(items, 10); // 10 chunks of 10

// Fan-in: collect all chunk results:
from(chunks).pipe(
  mergeMap(
    chunk => from(chunk).pipe(
      mergeMap(item => this.process(item), 3), // 3 concurrent per chunk
      toArray()
    ),
    4 // 4 chunks processed concurrently
  ),
  toArray(),
  map(chunkResults => chunkResults.flat())
).subscribe(allResults => render(allResults));
```

---

## Pattern 6: Sequential with Timeout Fallback

Process items one at a time, but fall back to parallel if sequential is too slow:

```typescript
import { concatMap, timeout, catchError, mergeMap } from 'rxjs/operators';

const SERIAL_TIMEOUT_MS = 500;

items$.pipe(
  concatMap(item =>
    this.process(item).pipe(
      timeout(SERIAL_TIMEOUT_MS),
      catchError(() => {
        // This item timed out in serial mode — run it independently
        console.warn(`Item ${item.id} timed out in serial mode, running independently`);
        return this.process(item); // retry without timeout constraint
      })
    )
  )
).subscribe();
```

---

## Pattern 7: Semaphore — Limit Concurrency Across Multiple Streams

```typescript
import { Subject, mergeMap, finalize } from 'rxjs';

class Semaphore {
  private readonly queue$ = new Subject<() => Observable<unknown>>();
  readonly results$;

  constructor(private readonly maxConcurrent: number) {
    this.results$ = this.queue$.pipe(
      mergeMap(fn => fn(), maxConcurrent)
    );
    this.results$.subscribe(); // keep semaphore active
  }

  run<T>(fn: () => Observable<T>): Observable<T> {
    return new Observable<T>(subscriber => {
      this.queue$.next(() =>
        fn().pipe(
          tap({
            next:     v   => subscriber.next(v),
            error:    err => subscriber.error(err),
            complete: ()  => subscriber.complete()
          })
        )
      );
    });
  }
}

const semaphore = new Semaphore(3);

// Any number of callers, max 3 run concurrently:
merge(
  semaphore.run(() => api.fetchA()),
  semaphore.run(() => api.fetchB()),
  semaphore.run(() => api.fetchC()),
  semaphore.run(() => api.fetchD()),
  semaphore.run(() => api.fetchE())
).subscribe(handleResult);
```

---

## Concurrency Decision Table

| Scenario | Strategy | Operator |
|---|---|---|
| Parallel HTTP (fast, independent) | Unbounded | `mergeMap` / `forkJoin` |
| Parallel HTTP (avoid rate limits) | Bounded pool | `mergeMap(fn, N)` |
| Upload queue (ordered) | Serial | `concatMap` |
| Form submit (prevent double) | Drop new | `exhaustMap` |
| Search (cancel stale) | Cancel old | `switchMap` |
| Large dataset processing | Fan-out | `mergeMap(fn, N)` + `toArray()` |
| Shared concurrency budget | Semaphore | Custom semaphore |

---

## Common Pitfalls

### Unbounded `mergeMap` Overwhelming a Server

```typescript
// ❌ All 1000 requests fired simultaneously:
from(thousandIds).pipe(
  mergeMap(id => api.get(id)) // 1000 concurrent requests!
).subscribe();

// ✅ Bounded pool — 10 at a time:
from(thousandIds).pipe(
  mergeMap(id => api.get(id), 10)
).subscribe();
```

### `concatMap` Queue Growing Unboundedly

```typescript
// ❌ If processing is slow, the queue grows without bound:
fastSource$.pipe(
  concatMap(item => slowProcess(item)) // queue grows forever if source > processing speed
).subscribe();

// ✅ Use exhaustMap to drop excess, or mergeMap(fn, N) to bound the queue:
fastSource$.pipe(
  exhaustMap(item => slowProcess(item)) // skip while busy
).subscribe();
```
