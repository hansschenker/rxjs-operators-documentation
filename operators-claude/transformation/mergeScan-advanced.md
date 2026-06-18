# mergeScan — Advanced Patterns

For `mergeScan` fundamentals see the core [mergeScan](./mergeScan) doc. This page covers concurrent accumulation, resource pooling, streaming aggregation, and the critical differences from `scan`, `mergeMap`, and `switchScan`.

---

## What `mergeScan` Does

`mergeScan` is `scan` + `mergeMap`. Each source emission calls the project function with `(accumulator, value)` → Observable. Unlike `switchScan`, it does **not** cancel previous inner Observables — they run concurrently and the accumulator is updated as each one completes.

```typescript
import { mergeScan } from 'rxjs/operators';

source$.pipe(
  mergeScan(
    (acc, value) => innerObservable$(acc, value),
    initialAccumulator,
    concurrency // optional: max concurrent inner Observables (default: Infinity)
  )
)
```

The critical semantics:
- Each new emission starts a new inner Observable
- All inner Observables run concurrently (up to `concurrency` limit)
- Accumulator is passed to each new project call with the CURRENT acc at time of emission
- Each inner Observable's emissions update the accumulator (via scan)

---

## Pattern 1: Parallel File Processing with Running Total

Process files concurrently and accumulate results:

```typescript
import { mergeScan, map } from 'rxjs/operators';
import { from } from 'rxjs';

interface ProcessResult {
  file:      string;
  wordCount: number;
  errors:    string[];
}

interface RunningStats {
  totalWords:     number;
  processedFiles: number;
  allErrors:      string[];
}

from(fileList).pipe(
  mergeScan(
    (stats, file) =>
      processFile(file).pipe(         // runs concurrently for all files
        map(result => ({
          totalWords:     stats.totalWords + result.wordCount,
          processedFiles: stats.processedFiles + 1,
          allErrors:      [...stats.allErrors, ...result.errors]
        }))
      ),
    { totalWords: 0, processedFiles: 0, allErrors: [] },
    4 // max 4 concurrent file processors
  )
).subscribe(stats => updateProgressUI(stats));
// UI updates as each file completes — no waiting for all to finish
```

---

## Pattern 2: Streaming Search with Parallel Requests

Fire multiple search requests concurrently and accumulate results as they arrive:

```typescript
import { mergeScan } from 'rxjs/operators';

interface SearchAccumulator {
  results: SearchResult[];
  inFlight: number;
  done: boolean;
}

// Multiple search providers queried in parallel:
const providers = ['google', 'bing', 'duckduckgo'];

from(providers).pipe(
  mergeScan(
    (acc, provider) =>
      searchApi(provider, query).pipe(
        map(results => ({
          results:  [...acc.results, ...results],
          inFlight: acc.inFlight - 1,
          done:     acc.inFlight - 1 === 0
        })),
        startWith({
          ...acc,
          inFlight: acc.inFlight // keep in-flight count while request runs
        })
      ),
    { results: [], inFlight: providers.length, done: false }
  ),
  distinctUntilChanged((a, b) => a.results.length === b.results.length)
).subscribe(({ results, done }) => {
  renderResults(results);
  if (done) hideLoadingSpinner();
});
```

---

## Pattern 3: Concurrent Database Writes with Order Tracking

Write multiple records concurrently but track insertion order:

```typescript
import { mergeScan, concatMap } from 'rxjs/operators';

interface WriteAccumulator {
  written:  number;
  failed:   number;
  ids:      string[];
}

newRecords$.pipe(
  mergeScan(
    (acc, record) =>
      db.insert(record).pipe(
        map(id => ({
          written: acc.written + 1,
          failed:  acc.failed,
          ids:     [...acc.ids, id]
        })),
        catchError(() => of({
          written: acc.written,
          failed:  acc.failed + 1,
          ids:     acc.ids
        }))
      ),
    { written: 0, failed: 0, ids: [] },
    10 // max 10 concurrent writes
  )
).subscribe(status => updateWriteStatus(status));
```

---

## Pattern 4: Lazy Resource Pool

Build a pool of async resources with concurrent initialisation:

```typescript
import { mergeScan, toArray } from 'rxjs/operators';
import { range } from 'rxjs';

interface Pool<T> {
  resources: T[];
  ready:     boolean;
}

function createPool<T>(
  factory:  () => Observable<T>,
  size:     number,
  concurrency = 4
): Observable<Pool<T>> {
  return range(0, size).pipe(
    mergeScan(
      (pool) =>
        factory().pipe(
          map(resource => ({
            resources: [...pool.resources, resource],
            ready:     pool.resources.length + 1 === size
          }))
        ),
      { resources: [] as T[], ready: false },
      concurrency
    ),
    filter(pool => pool.ready), // only emit when pool is full
    take(1)
  );
}

// Usage — pre-warm 8 DB connections, 4 at a time:
createPool(() => DatabaseConnection.create(), 8, 4).subscribe(
  pool => startApp(pool.resources)
);
```

---

## Pattern 5: Incremental Tree Build (Parallel Branch Expansion)

Expand tree nodes concurrently and accumulate into a growing tree:

```typescript
import { mergeScan } from 'rxjs/operators';
import { from } from 'rxjs';

interface TreeNode { id: string; children?: TreeNode[]; }

function expandTree(
  rootIds:     string[],
  fetchNode$:  (id: string) => Observable<TreeNode>
): Observable<Map<string, TreeNode>> {
  return from(rootIds).pipe(
    mergeScan(
      (tree, nodeId) =>
        fetchNode$(nodeId).pipe(
          map(node => {
            const updated = new Map(tree);
            updated.set(node.id, node);
            return updated;
          })
        ),
      new Map<string, TreeNode>(),
      8 // expand 8 nodes concurrently
    )
  );
}
```

---

## `mergeScan` vs `scan` vs `switchScan` vs `mergeMap` + `scan`

```typescript
// scan — synchronous accumulation only:
source$.pipe(scan((acc, v) => newAcc, seed))
// ✗ Cannot handle async project functions

// switchScan — async + accumulation, but CANCELS previous inner on new emission:
source$.pipe(switchScan((acc, v) => asyncOp$(acc, v), seed))
// ✗ Concurrent emissions: later arrival cancels earlier in-flight ops
// ✓ Good for: search where only latest query matters

// mergeMap then scan — async + accumulation, but acc NOT in project:
source$.pipe(
  mergeMap(v => asyncOp$(v)),
  scan((acc, result) => [...acc, result], [])
)
// ✗ Project can't see the current accumulator
// ✓ OK when project doesn't need running state

// mergeScan — async + accumulation + acc available + CONCURRENT:
source$.pipe(mergeScan((acc, v) => asyncOp$(acc, v), seed, concurrency))
// ✓ All three: async project, running acc in project, concurrent execution
// ✓ Good for: parallel work where all results are needed, running totals
```

---

## Common Pitfalls

### Race Conditions in Accumulator Updates

```typescript
// ❌ Accumulator race: two concurrent ops both read the same acc at emission time,
//    then both try to "add 1" to it — one update is lost:
source$.pipe(
  mergeScan((acc, v) =>
    asyncCount$(v).pipe(map(n => acc + n)), // both read acc = 5, both return 6
    0
  )
)

// ✅ Use an immutable append pattern — each result is self-describing:
source$.pipe(
  mergeScan((acc, v) =>
    asyncOp$(v).pipe(
      map(result => [...acc, result]) // append, don't increment a shared counter
    ),
    []
  )
)
// If you truly need to count, use toArray() + length on the final acc instead
```

### Forgetting the Concurrency Parameter on High-Volume Sources

```typescript
// ❌ Infinite concurrency on high-volume source — floods server:
highVolumeSource$.pipe(
  mergeScan((acc, item) => api.process(item).pipe(...), acc)
  // concurrency defaults to Infinity — sends all requests simultaneously!
)

// ✅ Always set concurrency for resource-constrained work:
highVolumeSource$.pipe(
  mergeScan((acc, item) => api.process(item).pipe(...), acc, 5)
)
```

---

**Cognitive Load**: 4/5 | **Usage Frequency**: 2/5 | **Composability**: 4/5
**Key insight**: `mergeScan` fills the niche where you need BOTH a running accumulator in the project function AND concurrent execution of inner Observables. The most common production use is parallel data loading where partial results should update the UI as each request completes — rather than waiting for all to finish (which would use `forkJoin` + `scan`).
