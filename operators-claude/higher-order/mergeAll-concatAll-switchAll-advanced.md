# mergeAll / concatAll / switchAll — Advanced Patterns

For fundamentals see the core [mergeAll / concatAll / switchAll](./mergeAll-concatAll-switchAll) doc. This page covers the `*All` vs `*Map` equivalence, dynamic source registration, ordered concurrent processing, and the full higher-order decision guide.

---

## The `*All` / `*Map` Equivalence

Every `*Map` operator is exactly `map(project)` + the corresponding `*All`:

```typescript
// These are identical:
source$.pipe(mergeMap(x  => inner$(x)))
source$.pipe(map(x       => inner$(x)), mergeAll())

source$.pipe(concatMap(x => inner$(x)))
source$.pipe(map(x       => inner$(x)), concatAll())

source$.pipe(switchMap(x => inner$(x)))
source$.pipe(map(x       => inner$(x)), switchAll())
```

Use `*All` directly when the Observable-of-Observables already exists (e.g., from a factory, subject, or `groupBy`).

---

## Pattern 1: Dynamic Source Registration with `mergeAll`

Add and remove data sources at runtime:

```typescript
import { Subject, mergeAll, share } from 'rxjs';

class DynamicSourceManager<T> {
  private sources$ = new Subject<Observable<T>>();
  readonly output$ = this.sources$.pipe(
    mergeAll(), // subscribe to each registered source
    share()
  );

  add(source$: Observable<T>): void {
    this.sources$.next(source$);
  }
}

const manager = new DynamicSourceManager<SensorReading>();

// Register sensors as they connect:
sensorConnected$.subscribe(sensor =>
  manager.add(
    sensor.readings$.pipe(
      catchError(() => EMPTY),  // isolate each sensor's errors
      takeUntil(sensor.disconnected$)
    )
  )
);

manager.output$.pipe(
  takeUntilDestroyed()
).subscribe(reading => processSensorReading(reading));
```

---

## Pattern 2: `concatAll` for Ordered Processing

Process an Observable-of-Observables in strict sequence:

```typescript
import { from, concatAll, map } from 'rxjs';

// Process migration scripts in order, one at a time:
const migrations$: Observable<Observable<void>> = from([
  migration001$,
  migration002$,
  migration003$
]);

migrations$.pipe(
  map((migration$, index) =>
    migration$.pipe(
      tap(() => console.log(`Migration ${index + 1} complete`)),
      catchError(err => {
        console.error(`Migration ${index + 1} failed:`, err);
        return throwError(() => err); // abort remaining migrations
      })
    )
  ),
  concatAll() // run each migration after previous completes
).subscribe({
  complete: () => console.log('All migrations complete'),
  error:    err => rollbackAll()
});
```

---

## Pattern 3: `switchAll` for Latest-Only Streams

Flatten an Observable-of-Observables, keeping only the latest inner stream:

```typescript
import { switchAll, BehaviorSubject } from 'rxjs';

// Route selection: switch to the selected route's data stream
const selectedRoute$ = new BehaviorSubject<string>('/home');

const routeData$: Observable<Observable<RouteData>> = selectedRoute$.pipe(
  map(route => fetchRouteData$(route).pipe(
    startWith({ loading: true } as RouteData)
  ))
);

routeData$.pipe(
  switchAll() // cancel previous route load when route changes
).subscribe(data => renderPage(data));
```

---

## Pattern 4: `concatAll` with Progress Reporting

Process a queue sequentially and report progress after each item:

```typescript
import { from, concatAll, scan, map } from 'rxjs';

interface Job { id: string; work$: Observable<Result>; }

function processQueue(jobs: Job[]): Observable<{ done: number; total: number; result: Result }> {
  const total = jobs.length;

  return from(jobs).pipe(
    map(job =>
      job.work$.pipe(
        map(result => ({ id: job.id, result }))
      )
    ),
    concatAll(),
    scan((acc, { result }) => ({
      done:   acc.done + 1,
      total,
      result
    }), { done: 0, total, result: null as unknown as Result })
  );
}

processQueue(uploadJobs).subscribe(({ done, total, result }) => {
  updateProgressBar(done / total);
  addToLog(result);
  if (done === total) showComplete();
});
```

---

## Pattern 5: `mergeAll` with Concurrency Limit

`mergeAll(N)` limits concurrent inner subscriptions — the same as `mergeMap` with concurrency:

```typescript
import { from, mergeAll, map } from 'rxjs';

// Process 50 items with max 5 concurrent requests:
const items = Array.from({ length: 50 }, (_, i) => i);

from(items).pipe(
  map(item => this.api.process(item)),
  mergeAll(5) // exactly 5 concurrent subscriptions
).subscribe(result => handleResult(result));

// Equivalent to:
from(items).pipe(
  mergeMap(item => this.api.process(item), 5)
)
```

---

## `mergeAll` vs `concatAll` vs `switchAll` vs `exhaustAll`

```typescript
// mergeAll — all inner streams run concurrently:
outer$.pipe(mergeAll())
// ✓ Maximum throughput; order not preserved
// Use: parallel independent work (file downloads, parallel API calls)

// concatAll — queues inner streams; one at a time, in order:
outer$.pipe(concatAll())
// ✓ Preserves order; no overlapping
// Use: sequential workflows (migrations, ordered uploads)

// switchAll — cancels previous on new outer emission:
outer$.pipe(switchAll())
// ✓ Always working on latest; previous work discarded
// Use: user-triggered searches, navigation (cancel stale loads)

// exhaustAll — ignores new outer while current inner is running:
outer$.pipe(exhaustAll())
// ✓ Prevents duplicate submissions; first wins
// Use: form submit buttons, double-click prevention
```

---

## Common Pitfalls

### Using `concatAll` on Infinite Inner Observables

```typescript
// ❌ Second inner Observable never starts because first never completes:
of(ws1$, ws2$).pipe(
  concatAll() // ws1$ is a WebSocket that never completes — ws2$ never subscribes!
)

// ✅ Use mergeAll for streams that may not complete:
of(ws1$, ws2$).pipe(mergeAll())

// Or bound each inner stream:
of(ws1$, ws2$).pipe(
  map(ws$ => ws$.pipe(take(100))), // take 100 events from each
  concatAll()
)
```

### Not Handling Inner Observable Errors with `mergeAll`

```typescript
// ❌ One inner error terminates the entire outer stream:
sources$.pipe(mergeAll())
// If any source$ errors → entire stream errors

// ✅ Isolate errors per inner stream:
sources$.pipe(
  map(source$ => source$.pipe(catchError(() => EMPTY))),
  mergeAll()
)
```

---

**Cognitive Load**: 3/5 | **Usage Frequency**: 3/5 | **Composability**: 5/5
**When to use `*All` over `*Map`**: Reach for `*All` when the Observable-of-Observables is already constructed (from `groupBy`, a `Subject`, or a factory function). For inline transformations, `*Map` is more readable. The concurrency semantics are identical.
