# zipAll — Advanced Patterns

For fundamentals see the core [zipAll](./zipAll) doc. This page covers ordered pairing across dynamic stream sets, parallel task coordination, round-robin distribution, and comparison with `combineLatestAll` and `forkJoin`.

---

## Mental Model

```typescript
import { zipAll } from 'rxjs/operators';

// zipAll — pairs nth emission from every inner Observable
// Emits only when ALL inners have emitted their nth value
// Completes when any inner completes

of(
  of(1, 2, 3),
  of('a', 'b', 'c'),
  of(true, false, true)
).pipe(
  zipAll()
).subscribe(console.log);
// [1, 'a', true]
// [2, 'b', false]
// [3, 'c', true]

// Key: strict positional pairing — 1st with 1st, 2nd with 2nd, etc.
// Unlike combineLatestAll which pairs "latest with latest"
```

**When to use `zipAll`**: When inner streams emit at the same conceptual pace and you need the nth result from each paired together. Classic example: parallel operations that produce result sequences of the same length.

---

## Pattern 1: Parallel Task Results Pairing

Run N tasks in parallel, pair each task's input with its output:

```typescript
import { zipAll, map, from } from 'rxjs/operators';

interface Task   { id: string; input: unknown }
interface Result { taskId: string; output: unknown; durationMs: number }

// Run tasks in parallel; pair each task with its result in order:
function runTasksWithResults$(tasks: Task[]): Observable<[Task, Result]> {
  const taskInputs$  = from(tasks);
  const taskResults$ = from(tasks.map(task =>
    processTask$(task).pipe(
      map(output => ({ taskId: task.id, output, durationMs: 0 }))
    )
  ));

  return of(taskInputs$, taskResults$).pipe(
    zipAll<Task | Result>()
  ) as Observable<[Task, Result]>;
}

// More idiomatic: zip individual task-result pairs:
function runWithTiming$(tasks: Task[]): Observable<{ task: Task; result: Result }> {
  return from(tasks).pipe(
    mergeMap(task => {
      const start = Date.now();
      return processTask$(task).pipe(
        map(output => ({
          task,
          result: { taskId: task.id, output, durationMs: Date.now() - start }
        }))
      );
    })
  );
}

// zipAll shines when streams are already separate and need ordered pairing:
const requestStream$  = of('req1', 'req2', 'req3');
const responseStream$ = httpClient.post$('/batch', requests);

of(requestStream$, responseStream$).pipe(
  zipAll<string | Response>(),
  map(([req, res]) => ({ request: req as string, response: res as Response }))
).subscribe(pair => logRequestResponse(pair));
```

---

## Pattern 2: Multi-Source Sequence Alignment

Align sequences from different sources that should correspond positionally:

```typescript
import { zipAll, map } from 'rxjs/operators';

// CSV export from DB, JSON from API, XML from legacy system
// All represent the same logical records — align them by position:
function alignDataSources$(
  sources: Observable<unknown>[]
): Observable<unknown[]> {
  return of(...sources).pipe(zipAll());
}

// Usage — align product data from 3 systems:
const productIds$    = databaseQuery$('SELECT id FROM products ORDER BY id');
const productPrices$ = priceApiStream$(); // emits prices in same order
const productStock$  = warehouseStream$(); // emits stock levels in same order

alignDataSources$([productIds$, productPrices$, productStock$]).pipe(
  map(([id, price, stock]) => ({
    id:    id    as string,
    price: price as number,
    stock: stock as number
  })),
  filter(p => p.stock > 0),
  toArray()
).subscribe(inStockProducts => updateCatalog(inStockProducts));

// Time-series alignment — pair measurements at same time step:
const temperatureReadings$ = sensorA$.pipe(bufferCount(10), mergeAll());
const humidityReadings$    = sensorB$.pipe(bufferCount(10), mergeAll());
const pressureReadings$    = sensorC$.pipe(bufferCount(10), mergeAll());

of(temperatureReadings$, humidityReadings$, pressureReadings$).pipe(
  zipAll<number>(),
  map(([temp, humidity, pressure]) => ({ temp, humidity, pressure, ts: Date.now() }))
).subscribe(reading => storeSyncedReading(reading));
```

---

## Pattern 3: Round-Robin Distribution

Use `zipAll` semantics to distribute work items evenly across workers:

```typescript
import { Subject, merge, zipAll, map } from 'rxjs';

// Round-robin: pair each item with the next-available worker
function roundRobin$<T, R>(
  items$:   Observable<T>,
  workers:  ((item: T) => Observable<R>)[],
): Observable<R> {
  const workerCount = workers.length;

  // Split items into N round-robin sub-streams, one per worker:
  const workerStreams$ = workers.map((worker, i) =>
    items$.pipe(
      // Every Nth item goes to worker i:
      filter((_, idx) => idx % workerCount === i),
      mergeMap(item => worker(item))
    )
  );

  return merge(...workerStreams$);
}

// Usage: distribute image processing across 4 workers:
const images$  = from(imageList);
const workers  = [
  (img: ImageData) => processWithGPU$(img, 0),
  (img: ImageData) => processWithGPU$(img, 1),
  (img: ImageData) => processWithGPU$(img, 2),
  (img: ImageData) => processWithGPU$(img, 3),
];

roundRobin$(images$, workers).pipe(
  toArray()
).subscribe(results => saveResults(results));
```

---

## Pattern 4: Versioned Data Pairing

Pair "before" and "after" snapshots from an update stream:

```typescript
import { zipAll, pairwise, map } from 'rxjs/operators';

// Get consecutive pairs from a single stream (pairwise is usually better here):
stateUpdates$.pipe(
  pairwise(),
  map(([before, after]) => computeDiff(before, after))
).subscribe(applyDiff);

// zipAll version — pair independent before/after streams:
const snapshotsBefore$ = snapshotService.getBefore$();
const snapshotsAfter$  = snapshotService.getAfter$();

of(snapshotsBefore$, snapshotsAfter$).pipe(
  zipAll<Snapshot>(),
  map(([before, after]) => ({
    before,
    after,
    diff: computeDiff(before, after),
    changed: JSON.stringify(before) !== JSON.stringify(after)
  })),
  filter(pair => pair.changed)
).subscribe(pair => reportChange(pair));
```

---

## `zipAll` vs `combineLatestAll` vs `forkJoin` — Decision Matrix

```typescript
// zipAll — nth from each inner, in lockstep
// Use when: streams emit at the same pace, positional pairing matters
// Completes when: shortest inner completes
of(stream1$, stream2$, stream3$).pipe(zipAll())
// [1st_A, 1st_B, 1st_C] → [2nd_A, 2nd_B, 2nd_C] → ...

// combineLatestAll — latest from each inner, on any change
// Use when: streams are live/ongoing, need latest-value sync
// Completes when: all inners complete
of(stream1$, stream2$, stream3$).pipe(combineLatestAll())
// Emits on every change, always with latest values from each

// forkJoin — last value from each inner, when all complete
// Use when: one-shot operations (HTTP requests), need all results at end
forkJoin([req1$, req2$, req3$])
// [final_A, final_B, final_C] — emits once, when all complete

// Summary:
// zipAll      → "pair row-by-row across streams"
// combineLatestAll → "keep dashboard current across streams"
// forkJoin    → "wait for all one-shot operations to finish"
```

---

## Common Pitfalls

### Unequal Stream Lengths — Shorter Stream Ends Combination Early

```typescript
// ❌ Streams of different lengths — combination stops at shortest:
of(
  of(1, 2, 3, 4, 5),  // 5 items
  of('a', 'b', 'c')   // 3 items — this one ends first
).pipe(zipAll()).subscribe(console.log);
// [1,'a'], [2,'b'], [3,'c'] — items 4 and 5 from first stream are DROPPED

// ✅ Pad shorter streams or ensure equal lengths:
of(
  of(1, 2, 3, 4, 5),
  of('a', 'b', 'c').pipe(
    concat(repeat('—', 2)) // pad to same length
  )
).pipe(zipAll()).subscribe(console.log);
// [1,'a'], [2,'b'], [3,'c'], [4,'—'], [5,'—']
```

### Using `zipAll` on Live Streams (Timing Mismatch)

```typescript
// ❌ zipAll on streams that don't emit at the same pace — values pile up:
of(
  interval(100),  // emits every 100ms
  interval(1000)  // emits every 1000ms — buffers 9 values waiting for this
).pipe(zipAll()).subscribe(console.log);
// interval(100) emits 9 values before interval(1000) emits 1
// Those 9 values are buffered — memory grows unboundedly

// ✅ Use combineLatestAll for live streams where timing differs:
of(
  interval(100).pipe(map(i => ({ fast: i }))),
  interval(1000).pipe(map(i => ({ slow: i })))
).pipe(combineLatestAll()).subscribe(console.log);
// Always shows latest from each — no unbounded buffering
```

---

**Cognitive Load**: 3/5 | **Usage Frequency**: 1/5 | **Composability**: 3/5
**Key insight**: `zipAll` is the "CSV join" operator — it merges N sequences by row number, like joining tables where the row index is the implicit key. It's rarely needed in real-time UI code (where `combineLatestAll` is usually correct) but has clear use cases in batch processing, data alignment across sources, and any context where you have parallel sequences that represent the same logical records. Always ensure streams are of equal length or add explicit padding — silent data loss from the shorter-stream-completes rule is the primary production bug.
