# toArray / count — Advanced Patterns

For fundamentals see the core [toArray / count](./toArray-count) doc. This page covers batched collection, conditional counting, grouped aggregation, and stream materialization patterns.

---

## What They Do

```typescript
import { toArray, count } from 'rxjs/operators';

// toArray() — collect ALL emissions into a single array on completion:
of(1, 2, 3).pipe(toArray()).subscribe(arr => console.log(arr)); // [1, 2, 3]

// count() — emit the total count on completion:
of(1, 2, 3).pipe(count()).subscribe(n => console.log(n)); // 3

// count(predicate) — count matching values:
of(1, 2, 3, 4).pipe(count(x => x % 2 === 0)).subscribe(n => console.log(n)); // 2
```

Both operators wait for the source to **complete** before emitting — they don't work on infinite streams without a `take`, `takeUntil`, or `takeWhile`.

---

## Pattern 1: Paginated Collection

Collect all pages into a single flat array:

```typescript
import { expand, toArray, map, filter, mergeMap, EMPTY } from 'rxjs/operators';

interface PageResponse<T> {
  items:   T[];
  cursor:  string | null;
  hasMore: boolean;
}

function fetchAllPages<T>(
  fetchPage: (cursor: string | null) => Observable<PageResponse<T>>
): Observable<T[]> {
  return fetchPage(null).pipe(
    expand(response =>
      response.hasMore ? fetchPage(response.cursor) : EMPTY
    ),
    map(response => response.items),
    toArray(), // collects arrays-of-items
    map(pages => pages.flat()) // flatten into single array
  );
}

// Usage:
fetchAllPages(cursor => this.api.getUsers({ cursor, limit: 100 })).subscribe(
  allUsers => {
    console.log(`Loaded ${allUsers.length} users total`);
    renderUserTable(allUsers);
  }
);
```

---

## Pattern 2: Group-Then-Aggregate

Collect grouped streams into a map:

```typescript
import { groupBy, mergeMap, toArray, map, reduce } from 'rxjs/operators';

interface Sale { region: string; amount: number; }

function aggregateSalesByRegion(sales$: Observable<Sale>): Observable<Map<string, number>> {
  return sales$.pipe(
    groupBy(sale => sale.region),
    mergeMap(group$ =>
      group$.pipe(
        reduce((total, sale) => total + sale.amount, 0),
        map(total => ({ region: group$.key, total }))
      )
    ),
    toArray(),
    map(entries =>
      new Map(entries.map(e => [e.region, e.total]))
    )
  );
}

// Collect all grouped results:
aggregateSalesByRegion(salesStream$.pipe(take(1000))).subscribe(totals => {
  totals.forEach((total, region) =>
    console.log(`${region}: $${total.toLocaleString()}`)
  );
});
```

---

## Pattern 3: Conditional Count with Statistics

```typescript
import { count, toArray, map, combineLatest } from 'rxjs/operators';
import { share } from 'rxjs/operators';

interface Event { type: string; value: number; }

function eventStatistics(events$: Observable<Event>) {
  // Share a single subscription:
  const shared$ = events$.pipe(share());

  return combineLatest([
    shared$.pipe(count()),
    shared$.pipe(count(e => e.value > 0)),
    shared$.pipe(count(e => e.value < 0)),
    shared$.pipe(count(e => e.type === 'error')),
    shared$.pipe(toArray(), map(evts => evts.reduce((s, e) => s + e.value, 0)))
  ]).pipe(
    map(([total, positive, negative, errors, sum]) => ({
      total,
      positive,
      negative,
      errors,
      sum,
      average: total > 0 ? sum / total : 0
    }))
  );
}
```

---

## Pattern 4: Windowed Collection (toArray in Windows)

Collect emissions in time windows rather than all-at-once:

```typescript
import { windowTime, mergeMap, toArray, filter } from 'rxjs/operators';

function collectInWindows<T>(
  source$:  Observable<T>,
  windowMs: number
): Observable<T[]> {
  return source$.pipe(
    windowTime(windowMs),
    mergeMap(window$ =>
      window$.pipe(
        toArray(),
        filter(arr => arr.length > 0) // skip empty windows
      )
    )
  );
}

// Batch DOM updates into 100ms windows:
collectInWindows(domMutations$, 100).subscribe(batch => {
  requestAnimationFrame(() => applyBatchedMutations(batch));
});
```

---

## Pattern 5: Count-Based Progress Reporting

Use `count` to report progress when processing a fixed set:

```typescript
import { count, tap, scan, map } from 'rxjs/operators';
import { Subject, merge } from 'rxjs';

function processWithProgress<T, R>(
  items:    T[],
  process$: (item: T) => Observable<R>
): Observable<{ results: R[]; progress: number; done: boolean }> {
  const total = items.length;

  return merge(
    ...items.map(item => process$(item))
  ).pipe(
    scan((acc, result) => ({
      results:  [...acc.results, result],
      processed: acc.processed + 1
    }), { results: [] as R[], processed: 0 }),
    map(({ results, processed }) => ({
      results,
      progress: processed / total,
      done:     processed === total
    }))
  );
}

// Usage — upload files with progress:
processWithProgress(files, file =>
  this.upload.file(file).pipe(last()) // wait for each upload to complete
).subscribe(({ progress, done, results }) => {
  updateProgressBar(progress);
  if (done) showSuccessWithResults(results);
});
```

---

## Pattern 6: Materialize Stream into Array for Replay

Snapshot a stream for later replay or inspection:

```typescript
import { toArray, shareReplay } from 'rxjs/operators';

function snapshotStream<T>(source$: Observable<T>): Observable<T[]> {
  return source$.pipe(
    toArray(),
    shareReplay(1) // cache the snapshot
  );
}

// Useful for testing or debugging — capture emissions for inspection:
const snapshot$ = snapshotStream(
  apiEvents$.pipe(take(100), takeUntilDestroyed())
);

// Now snapshot$ can be subscribed multiple times; same array each time:
snapshot$.subscribe(events => analyzeEvents(events));
snapshot$.subscribe(events => renderEventLog(events));
```

---

## `toArray` vs `reduce` vs `scan`

```typescript
// toArray() — collect all into array, emit on completion:
source$.pipe(toArray())
// → [a, b, c]  (emits once)
// ✓ Preserves all values, ordered
// ✗ Requires completion; no partial results

// reduce(fn, seed) — fold into single value, emit on completion:
source$.pipe(reduce((acc, val) => acc + val, 0))
// → 6  (sum, emits once)
// ✓ Arbitrary fold; any result type
// ✗ Requires completion; no partial results

// scan(fn, seed) — like reduce but emits after EACH value:
source$.pipe(scan((acc, val) => [...acc, val], []))
// → [a]  [a,b]  [a,b,c]  (emits on each)
// ✓ Running result visible; works on infinite streams
// ✗ More emissions, more object creation

// toArray() = reduce((acc, val) => [...acc, val], [])
// Use toArray() for readability; reduce() for custom aggregations
```

---

## Common Pitfalls

### Using `toArray` on an Infinite Stream

```typescript
// ❌ toArray() on an infinite stream — never emits:
interval(100).pipe(toArray()).subscribe(arr => console.log(arr));
// subscribe() callback never called — toArray() waits for completion forever

// ✅ Bound the stream first:
interval(100).pipe(
  take(10),     // or takeUntil(), takeWhile()
  toArray()
).subscribe(arr => console.log(arr)); // [0, 1, 2, ..., 9]
```

### `count` Predicate Returns Non-Boolean

```typescript
// ❌ Predicate that returns truthy/falsy, not strict boolean:
source$.pipe(count(item => item.name)) // item.name could be '' — truthy check

// ✅ Explicit boolean:
source$.pipe(count(item => item.name.length > 0))
```

---

**Cognitive Load**: 1/5 | **Usage Frequency**: 3/5 | **Composability**: 3/5
**Key insight**: `toArray` and `count` are terminal — they hold everything in memory until completion. This makes them perfect for finite HTTP responses and test assertions, but inappropriate for live event streams without a `take`-family operator to bound them. The most common production pattern is `expand` + `toArray` for fetching all paginated results in one call.
