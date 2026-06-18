# mergeMap — Advanced Patterns

For `mergeMap` fundamentals, see the core [mergeMap](./mergeMap) doc. This page covers concurrency control, request deduplication, and production patterns.

---

## Concurrency Parameter — The Most Underused Feature

`mergeMap(project, concurrent)` limits how many inner Observables run simultaneously. Without it, every source emission spawns a new inner Observable immediately.

```typescript
import { mergeMap } from 'rxjs/operators';
import { from } from 'rxjs';

// ❌ Unlimited — 1000 items = 1000 simultaneous HTTP requests
from(itemIds).pipe(
  mergeMap(id => this.api.getItem(id))
).subscribe(render);

// ✅ Bounded — max 5 concurrent requests at any time
from(itemIds).pipe(
  mergeMap(id => this.api.getItem(id), 5)
).subscribe(render);
// When one of the 5 completes, the next queued item starts automatically
```

**Choosing concurrency**:

| Use case | Recommended concurrent |
|---|---|
| REST API (typical) | 4–10 |
| Database writes | 2–5 (protect DB connection pool) |
| File I/O | OS open-file limit / 10 |
| WebSocket messages | Unlimited (messages are cheap) |
| CPU-bound (workers) | `navigator.hardwareConcurrency` |

---

## Pattern 1: Parallel Fetch with Ordered Results

`mergeMap` runs in parallel; results arrive out of order. To run in parallel but emit in order, use `concatMap` — but that adds latency. A better option: run parallel and sort after:

```typescript
import { mergeMap, toArray, map } from 'rxjs/operators';

// Run in parallel (fast), collect all, then sort:
from(ids).pipe(
  mergeMap(id => this.api.getItem(id).pipe(map(item => ({ id, item }))), 10),
  toArray(),
  map(results => results.sort((a, b) => ids.indexOf(a.id) - ids.indexOf(b.id))),
  map(results => results.map(r => r.item))
).subscribe(orderedItems => render(orderedItems));
```

---

## Pattern 2: Request Deduplication (In-Flight Cache)

Prevent duplicate concurrent requests for the same resource:

```typescript
import { mergeMap, share, finalize } from 'rxjs/operators';
import { Observable } from 'rxjs';

class RequestCache {
  private inflight = new Map<string, Observable<unknown>>();

  dedupe<T>(key: string, factory: () => Observable<T>): Observable<T> {
    if (!this.inflight.has(key)) {
      const req$ = factory().pipe(
        share(),
        finalize(() => this.inflight.delete(key))
      );
      this.inflight.set(key, req$);
    }
    return this.inflight.get(key) as Observable<T>;
  }
}

const cache = new RequestCache();

// Multiple subscribers for the same ID → one HTTP request:
userIds$.pipe(
  mergeMap(id =>
    cache.dedupe(`user:${id}`, () => this.api.getUser(id))
  )
).subscribe(renderUser);
```

---

## Pattern 3: Fan-Out / Fan-In (Scatter-Gather)

Broadcast a single item to multiple services, collect all results:

```typescript
import { mergeMap, forkJoin, map } from 'rxjs/operators';

// For each order: validate + price + inventory check in parallel
orders$.pipe(
  mergeMap(order =>
    forkJoin({
      validated:  this.validator.check(order),
      pricing:    this.pricer.calculate(order),
      inventory:  this.inventory.reserve(order)
    }).pipe(
      map(checks => ({ order, checks }))
    )
  )
).subscribe(({ order, checks }) => processOrder(order, checks));
```

---

## Pattern 4: Retry Per-Item Without Killing the Stream

```typescript
import { mergeMap, retry, catchError, of } from 'rxjs/operators';

from(items).pipe(
  mergeMap(item =>
    processItem(item).pipe(
      retry({ count: 3, delay: () => timer(1000) }),
      catchError(err => of({ item, error: err.message, failed: true }))
    ),
    5 // max 5 concurrent
  )
).subscribe(result => {
  if ('failed' in result) logFailure(result.item, result.error);
  else                   saveResult(result);
});
```

---

## Pattern 5: Streaming Aggregation

Accumulate results as they arrive (don't wait for all to complete):

```typescript
import { mergeMap, scan } from 'rxjs/operators';

// Running totals as parallel requests complete:
from(productIds).pipe(
  mergeMap(id => this.api.getProduct(id), 8),
  scan(
    (totals, product) => ({
      count:   totals.count + 1,
      revenue: totals.revenue + product.price * product.stock
    }),
    { count: 0, revenue: 0 }
  )
).subscribe(totals => updateDashboard(totals));
// Dashboard updates incrementally as each product loads
```

---

## Pattern 6: Backpressure via Concurrency Limit

The concurrency parameter naturally applies backpressure — the source is read only as fast as inner Observables complete.

```typescript
import { Subject, mergeMap } from 'rxjs';

const taskQueue$ = new Subject<Task>();

// Bounded worker pool — only 4 tasks run at once
taskQueue$.pipe(
  mergeMap(task => executeTask(task), 4)
).subscribe({
  next:  result => handleResult(result),
  error: err    => handleError(err)
});

// Push tasks at any rate — concurrency:4 throttles naturally
for (const task of massiveBatch) {
  taskQueue$.next(task);
}
```

---

## `mergeMap` vs `forkJoin` — When to Use Each

```typescript
// forkJoin: when you have a FIXED set of Observables known up front
forkJoin({
  user:   this.api.getUser(id),
  config: this.api.getConfig()
}).subscribe(({ user, config }) => init(user, config));
// All must complete; result emitted once

// mergeMap: when Observables are generated DYNAMICALLY from a stream
ids$.pipe(
  mergeMap(id => this.api.getItem(id), 5)
).subscribe(item => renderItem(item));
// Each result emitted as it arrives; stream stays open for more IDs
```

---

## Common Pitfalls

### No Concurrency Limit on External APIs

```typescript
// ❌ Rate limit violation / memory spike — 500 simultaneous requests
from(fiveHundredIds).pipe(
  mergeMap(id => this.api.getItem(id)) // default: unlimited
).subscribe();

// ✅ Respect rate limits and resource budgets:
from(fiveHundredIds).pipe(
  mergeMap(id => this.api.getItem(id), 8) // 8 at a time
).subscribe();
// WHY: Most APIs have rate limits (429 Too Many Requests). Even without
// explicit limits, unbounded parallelism can exhaust server connections.
```

### `catchError` Outside Kills the Stream

```typescript
// ❌ One failure stops all processing
from(ids).pipe(
  mergeMap(id => this.api.getItem(id)),
  catchError(() => EMPTY) // kills the whole stream on first error
).subscribe();

// ✅ Catch inside to isolate per-item failures:
from(ids).pipe(
  mergeMap(id =>
    this.api.getItem(id).pipe(
      catchError(() => EMPTY) // skip this item, continue with others
    ),
    8
  )
).subscribe();
```

## Related Operators

- **`mergeMap`** (core): Fundamentals, signature, basic marble diagrams
- **`concatMap`**: Sequential alternative (no concurrency)
- **`switchMap`**: Cancels previous on new emission
- **`exhaustMap`**: Drops new emissions while inner is active
- **`forkJoin`**: Fixed parallel set — all must complete before emitting

---

**Cognitive Load**: 3/5 | **Usage Frequency**: 5/5 | **Composability**: 5/5
**Key insight**: Always specify the `concurrent` parameter when processing a bounded collection. The default (unlimited) is appropriate only for live streams where each emission is independent and cheap.
