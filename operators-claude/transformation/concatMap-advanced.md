# concatMap — Advanced Patterns

For `concatMap` fundamentals, see the core [concatMap](./concatMap) doc. This page covers advanced use cases: task queues, ordered processing, sequential state machines, and safe side effects.

---

## When `concatMap` Is the Right Choice

`concatMap` = **serialized mergeMap** — processes one inner Observable at a time, in order, queuing the rest.

| Scenario | Use |
|---|---|
| Order matters AND concurrent is wrong | `concatMap` |
| Order matters AND concurrent is fine | `mergeMap` |
| Only latest matters (cancel previous) | `switchMap` |
| Drop during active | `exhaustMap` |

**The key trade-off**: `concatMap` guarantees order but its queue is **unbounded**. If the source emits faster than the inner Observable resolves, the queue grows without limit.

---

## Pattern 1: Sequential HTTP Requests (Dependent)

```typescript
import { concatMap } from 'rxjs/operators';

// Step 1 result feeds Step 2 — must be sequential, not parallel:
getUserId$.pipe(
  concatMap(userId =>
    this.http.get<User>(`/api/users/${userId}`)
  ),
  concatMap(user =>
    this.http.get<Org>(`/api/orgs/${user.orgId}`)
  ),
  concatMap(org =>
    this.http.get<Config>(`/api/orgs/${org.id}/config`)
  )
).subscribe(config => this.initWithConfig(config));

// Equivalent using a single chain:
startTrigger$.pipe(
  concatMap(() => this.http.get<User>('/api/me')),
  concatMap(user => this.http.get<Permissions>(`/api/permissions/${user.role}`)),
  concatMap(perms => this.initialize(perms))
).subscribe();
```

---

## Pattern 2: Task Queue — Process in Arrival Order

```typescript
import { Subject, concatMap, from } from 'rxjs';

// FIFO task queue: tasks execute one at a time, in submission order
const taskQueue$ = new Subject<() => Observable<void>>();

taskQueue$.pipe(
  concatMap(task => task()) // each task waits for the previous to complete
).subscribe({
  error: e => console.error('Task failed:', e)
});

// Enqueue tasks from anywhere:
function enqueue(task: () => Observable<void>) {
  taskQueue$.next(task);
}

enqueue(() => saveDocument(doc1));  // starts immediately
enqueue(() => saveDocument(doc2));  // waits for doc1 to finish
enqueue(() => saveDocument(doc3));  // waits for doc2 to finish
```

---

## Pattern 3: Sequential State Machine

```typescript
import { concatMap, scan, startWith } from 'rxjs/operators';

type Step = 'validate' | 'save' | 'notify' | 'done';

const steps: Step[] = ['validate', 'save', 'notify', 'done'];

// Run workflow steps in strict order:
from(steps).pipe(
  concatMap(step => executeStep(step).pipe(
    map(result => ({ step, result }))
  ))
).subscribe({
  next:     ({ step, result }) => updateProgress(step, result),
  error:    err               => rollback(err),
  complete: ()                => console.log('Workflow complete')
});
```

---

## Pattern 4: Serialized Animations

```typescript
import { concatMap, from } from 'rxjs';

// Animations must play in sequence — cannot overlap:
const animations = [
  fadeIn(header),
  slideIn(sidebar),
  scaleUp(content),
  appear(footer)
];

from(animations).pipe(
  concatMap(anim => runAnimation(anim))
).subscribe({
  complete: () => console.log('All animations done')
});
```

---

## Pattern 5: Safe File Write Queue

```typescript
import { Subject, concatMap } from 'rxjs';
import * as fs from 'fs/promises';

// All writes to the same file go through a queue — no partial writes or races
const writeQueue = new Subject<{ path: string; content: string }>();

writeQueue.pipe(
  concatMap(({ path, content }) =>
    from(fs.writeFile(path, content, 'utf8')).pipe(
      catchError(err => {
        console.error(`Write failed for ${path}:`, err);
        return EMPTY; // skip failed write, continue queue
      })
    )
  )
).subscribe();

// Usage — safe concurrent callers:
writeQueue.next({ path: './output.json', content: JSON.stringify(data1) });
writeQueue.next({ path: './output.json', content: JSON.stringify(data2) });
// data2 write waits for data1 to finish — no interleaving
```

---

## Pattern 6: Bounded Queue (Backpressure)

`concatMap`'s queue is unbounded by default. For a bounded queue, combine with `bufferCount`:

```typescript
import { concatMap, bufferCount, mergeMap } from 'rxjs/operators';

// Process in batches of 10, one batch at a time:
from(largeItemList).pipe(
  bufferCount(10),                    // group into batches of 10
  concatMap(batch =>                  // process one batch at a time
    from(batch).pipe(
      mergeMap(item => processItem(item), 3) // 3 concurrent within batch
    )
  )
).subscribe(result => collect(result));
```

---

## Pattern 7: Sequential With Retry

`concatMap` + `retry` retries each inner Observable independently:

```typescript
import { concatMap, retry, catchError, of } from 'rxjs/operators';

apiRequests$.pipe(
  concatMap(req =>
    makeRequest(req).pipe(
      retry(3),                           // 3 retries per request
      catchError(err => of({ error: err.message, req })) // recover individually
    )
  )
).subscribe(result => {
  if ('error' in result) logFailure(result);
  else                   processSuccess(result);
});
// Each request retries independently; failures don't block subsequent requests
```

---

## Pattern 8: Ordered Merge of Parallel Results

Run requests in parallel but **emit results in request order** (not arrival order):

```typescript
import { from, zip, mergeMap, concatMap } from 'rxjs';

const requests = [fetchA(), fetchB(), fetchC()];

// ❌ mergeMap: results arrive in completion order (A, C, B if B is slow)
from(requests).pipe(mergeMap(req => req)).subscribe(console.log);

// ✅ concatMap: results arrive in request order (A, B, C) — but sequential
from(requests).pipe(concatMap(req => req)).subscribe(console.log);

// ✅ BEST: run in parallel, emit in order (zip approach):
// Start all requests simultaneously, emit in order:
combineLatest(requests).subscribe(([a, b, c]) => {
  console.log(a, b, c); // in order, arrives when ALL complete
});
```

---

## Common Pitfalls

### Unbounded Queue Growth

```typescript
// ❌ HIGH MEMORY — source emits faster than inner Observable resolves
// Queue grows without bound
fastSource$.pipe(
  concatMap(item => slowOperation(item)) // 5s per item, source emits every 10ms
).subscribe();
// After 1 minute: 6,000 items in queue × average 2.5s remaining = high memory

// ✅ OPTIONS:
// 1. Switch to mergeMap with concurrency limit:
fastSource$.pipe(mergeMap(item => slowOperation(item), 5)).subscribe();

// 2. switchMap if only latest matters:
fastSource$.pipe(switchMap(item => slowOperation(item))).subscribe();

// 3. exhaustMap if drops are acceptable:
fastSource$.pipe(exhaustMap(item => slowOperation(item))).subscribe();

// 4. Rate-limit the source:
fastSource$.pipe(
  throttleTime(5000),     // max one per 5s
  concatMap(item => slowOperation(item))
).subscribe();
```

### Using `concatMap` When Tasks Are Independent

```typescript
// ❌ UNNECESSARY SERIALIZATION — items could be parallel
userIds$.pipe(
  concatMap(id => this.http.get(`/api/users/${id}`))
)
// Sequential: user 2 waits for user 1's request to complete

// ✅ mergeMap is correct for independent requests:
userIds$.pipe(
  mergeMap(id => this.http.get(`/api/users/${id}`), 5) // 5 concurrent
)
// WHY: concatMap's serialization adds latency when requests are independent.
// Use concatMap only when order or atomicity actually matters.
```

## Related Operators

- **`concatMap`** (core): Fundamentals, marble diagrams, completion semantics
- **`mergeMap`**: Concurrent alternative — no ordering guarantee
- **`switchMap`**: Cancels previous inner on new emission
- **`exhaustMap`**: Drops new emissions while inner is active
- **`mergeScan`**: Like `concatMap` but with accumulated state
- **`queue scheduler`**: Related scheduling mechanism for synchronous queuing

---

**Cognitive Load**: 3/5 | **Usage Frequency**: 3/5 | **Composability**: 4/5
**Key insight**: `concatMap` is `mergeMap` with `concurrent = 1`. It guarantees order at the cost of latency. The hidden risk is an unbounded queue — always consider whether the source can emit faster than the inner resolves.
