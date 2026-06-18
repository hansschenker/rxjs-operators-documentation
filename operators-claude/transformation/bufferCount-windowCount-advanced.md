# bufferCount / windowCount — Advanced Patterns

For fundamentals see [buffer / window](./buffer-window) and the core docs. This page covers sliding windows, moving averages, overlap buffering, event chunking, and comparison with `bufferTime`.

---

## Mental Model: Fixed vs Sliding Windows

```typescript
import { bufferCount, windowCount } from 'rxjs/operators';

// bufferCount(size) — collect N items, emit as array, repeat
// bufferCount(size, startEvery) — NEW window starts every `startEvery` items

// Tumbling window (no overlap):
source$.pipe(bufferCount(3))
// 1,2,3,4,5,6,7 → [1,2,3], [4,5,6], [7] (partial last batch if not divisible)

// Sliding window (overlap):
source$.pipe(bufferCount(3, 1))
// 1,2,3,4,5 → [1,2,3], [2,3,4], [3,4,5], [4,5], [5] (trailing partials until complete)

// Skip window (gap between windows):
source$.pipe(bufferCount(2, 4))
// 1,2,3,4,5,6,7,8 → [1,2], [5,6] (items 3,4 and 7,8 skipped)

// windowCount — same semantics but emits Observable<T> instead of T[]:
source$.pipe(
  windowCount(3),
  mergeMap(window$ => window$.pipe(toArray()))
)
// Equivalent to bufferCount(3) but inner stream is Observable — use for streaming processing
```

---

## Pattern 1: Moving Average (Financial / Sensor Data)

```typescript
import { bufferCount, map } from 'rxjs/operators';

// Simple Moving Average — average of last N readings:
function movingAverage$(source$: Observable<number>, period: number): Observable<number> {
  return source$.pipe(
    bufferCount(period, 1),          // sliding window, advances 1 at a time
    filter(buf => buf.length === period), // skip partial leading windows
    map(buf => buf.reduce((a, b) => a + b, 0) / period)
  );
}

// Usage — 5-period SMA on stock prices:
const prices$ = stockTicker$.pipe(pluck('close'));

movingAverage$(prices$, 5).subscribe(sma => {
  console.log(`SMA(5): ${sma.toFixed(2)}`);
});

// Exponential Moving Average (EMA) — weightings decay exponentially:
function ema$(source$: Observable<number>, period: number): Observable<number> {
  const k = 2 / (period + 1);
  return source$.pipe(
    scan((prev: number | null, curr: number) => {
      if (prev === null) return curr;
      return curr * k + prev * (1 - k);
    }, null as number | null),
    filter((v): v is number => v !== null)
  );
}

// Bollinger Bands — SMA ± 2 standard deviations:
function bollingerBands$(
  prices$: Observable<number>,
  period = 20
): Observable<{ upper: number; middle: number; lower: number }> {
  return prices$.pipe(
    bufferCount(period, 1),
    filter(buf => buf.length === period),
    map(buf => {
      const mean = buf.reduce((a, b) => a + b, 0) / period;
      const variance = buf.reduce((acc, v) => acc + Math.pow(v - mean, 2), 0) / period;
      const stdDev = Math.sqrt(variance);
      return { upper: mean + 2 * stdDev, middle: mean, lower: mean - 2 * stdDev };
    })
  );
}

bollingerBands$(stockPrices$, 20).pipe(
  takeUntilDestroyed()
).subscribe(bands => updateChart(bands));
```

---

## Pattern 2: Event Batching for API Calls

Collect UI events into batches for efficient server-side processing:

```typescript
import { bufferCount, concatMap, from } from 'rxjs/operators';

// User tags items in a list — batch save calls to reduce API requests:
const tagEvents$ = fromEvent<CustomEvent>(listEl, 'item-tagged').pipe(
  map(e => e.detail as { itemId: string; tag: string })
);

tagEvents$.pipe(
  bufferCount(10),        // batch 10 tag events at a time
  concatMap(batch =>      // concatMap preserves order, prevents concurrent batch requests
    this.tagsApi.saveBatch$(batch).pipe(
      catchError(err => {
        // Re-queue failed items — don't swallow the error silently
        failedItems$.next(batch);
        return EMPTY;
      })
    )
  ),
  takeUntilDestroyed()
).subscribe(result => showSavedCount(result.savedCount));

// Dynamic batch size based on network speed:
function adaptiveBatch$<T>(
  source$:        Observable<T>,
  minBatch:       number,
  maxBatch:       number,
  networkSpeed$:  Observable<'fast' | 'slow'>
): Observable<T[]> {
  return networkSpeed$.pipe(
    switchMap(speed =>
      source$.pipe(
        bufferCount(speed === 'fast' ? maxBatch : minBatch)
      )
    )
  );
}
```

---

## Pattern 3: Sliding Window Correlation

Detect correlations between consecutive events in a stream:

```typescript
import { bufferCount, map, filter } from 'rxjs/operators';

interface ClickEvent { x: number; y: number; t: number }

// Detect double-click by checking 2 consecutive clicks for proximity and timing:
const clicks$ = fromEvent<MouseEvent>(document, 'click').pipe(
  map(e => ({ x: e.clientX, y: e.clientY, t: Date.now() }))
);

const doubleClicks$ = clicks$.pipe(
  bufferCount(2, 1),                           // pairs of consecutive clicks
  filter(([a, b]) =>
    b.t - a.t < 300 &&                         // within 300ms
    Math.abs(b.x - a.x) < 20 &&               // within 20px X
    Math.abs(b.y - a.y) < 20                   // within 20px Y
  ),
  map(([, second]) => second),                 // emit position of second click
  throttleTime(300)                            // prevent triple-click triggering twice
);

// Detect trend direction from a sequence of values (3-period):
const prices$ = ticker$.pipe(map(t => t.price));

const trend$ = prices$.pipe(
  bufferCount(3, 1),
  filter(buf => buf.length === 3),
  map(([a, b, c]) => {
    if (a < b && b < c) return 'up'   as const;
    if (a > b && b > c) return 'down' as const;
    return                     'flat' as const;
  }),
  distinctUntilChanged()
);

trend$.subscribe(dir => updateTrendIndicator(dir));
```

---

## Pattern 4: `windowCount` for Streaming Processing

Use `windowCount` when you want to process inner items as a stream (not wait for the whole array):

```typescript
import { windowCount, mergeMap, reduce, map } from 'rxjs/operators';

// Processing a large CSV stream — reduce each chunk of 1000 rows:
csvRows$.pipe(
  windowCount(1000),
  mergeMap(window$ =>
    window$.pipe(
      reduce((acc: ProcessedRow[], row: CsvRow) => {
        acc.push(parseRow(row));
        return acc;
      }, []),
      map(batch => ({ batch, count: batch.length }))
    )
  ),
  tap(({ count }) => console.log(`Processed chunk of ${count} rows`)),
  mergeMap(({ batch }) => saveBatchToDatabase$(batch))
).subscribe({
  complete: () => console.log('All rows processed'),
  error:    err => console.error('Processing failed:', err)
});

// windowCount with inner max processing — process each window's top 3:
realtimeEvents$.pipe(
  windowCount(20),                             // 20-event windows
  mergeMap(window$ =>
    window$.pipe(
      toArray(),
      map(events => events
        .sort((a, b) => b.score - a.score)
        .slice(0, 3)                           // top 3 by score per window
      )
    )
  )
).subscribe(top3 => updateLeaderboard(top3));
```

---

## Pattern 5: Chunk Transfer with Progress

Use `bufferCount` to display progress while processing a large dataset:

```typescript
import { bufferCount, scan, map } from 'rxjs/operators';

function processWithProgress$<T, R>(
  items$:     Observable<T>,
  totalCount: number,
  batchSize:  number,
  process:    (batch: T[]) => Observable<R[]>
): Observable<{ results: R[]; progress: number }> {
  let processed = 0;

  return items$.pipe(
    bufferCount(batchSize),
    concatMap(batch =>
      process(batch).pipe(
        map(results => {
          processed += batch.length;
          return {
            results,
            progress: Math.min(100, Math.round(processed / totalCount * 100))
          };
        })
      )
    )
  );
}

// Usage:
processWithProgress$(
  from(records),
  records.length,
  50,
  batch => this.api.bulkInsert$(batch)
).subscribe(({ results, progress }) => {
  updateProgressBar(progress);
  if (progress === 100) showSuccessBanner(results.length);
});
```

---

## `bufferCount` vs `bufferTime` vs `windowCount`

```typescript
// bufferCount(N) — emit every N items (count-driven)
// Best for: batch API calls, pagination, fixed-size chunking
source$.pipe(bufferCount(10))

// bufferCount(N, 1) — sliding window (every item, last N values)
// Best for: moving averages, trend detection, correlation analysis
source$.pipe(bufferCount(10, 1), filter(b => b.length === 10))

// bufferTime(ms) — emit every N milliseconds (time-driven)
// Best for: real-time data throttling, rate-limited API calls
source$.pipe(bufferTime(1000))

// windowCount(N) — like bufferCount but inner Observable instead of array
// Best for: streaming processing where you don't want to wait for full array
// More memory-efficient for large N when processing can start immediately
source$.pipe(windowCount(100), mergeMap(win$ => win$.pipe(take(10)))) // first 10 of each 100

// Decision rule:
// Need the whole array → bufferCount
// Need to process items as they arrive within window → windowCount
// Windows triggered by time → bufferTime / windowTime
// Windows triggered by signal → bufferWhen / windowWhen
// Windows triggered by open/close events → bufferToggle / windowToggle
```

---

## Common Pitfalls

### Sliding Window Without Filtering Partial Leading Windows

```typescript
// ❌ bufferCount(5, 1) emits partial arrays at the start:
source$.pipe(
  bufferCount(5, 1),
  map(buf => average(buf)) // average([1]) = 1, average([1,2]) = 1.5 — misleading!
).subscribe(console.log);

// ✅ Filter until the window is full:
source$.pipe(
  bufferCount(5, 1),
  filter(buf => buf.length === 5),  // only full windows
  map(buf => average(buf))
).subscribe(console.log);
```

### Using `bufferCount` with `mergeMap` Instead of `concatMap`

```typescript
// ❌ mergeMap processes batches concurrently — order not preserved:
events$.pipe(
  bufferCount(10),
  mergeMap(batch => api.saveBatch$(batch)) // batch 2 may complete before batch 1
).subscribe();

// ✅ concatMap preserves order — each batch waits for the previous:
events$.pipe(
  bufferCount(10),
  concatMap(batch => api.saveBatch$(batch)) // sequential, ordered
).subscribe();
```

---

**Cognitive Load**: 3/5 | **Usage Frequency**: 3/5 | **Composability**: 4/5
**Key insight**: `bufferCount(N)` is the "tumbling window" — non-overlapping batches of N. `bufferCount(N, 1)` is the "sliding window" — the most powerful form, advancing one item at a time, enabling moving averages and correlation detection. Always filter out partial leading windows with `filter(buf => buf.length === N)` in the sliding case. Choose `windowCount` over `bufferCount` when processing can begin before the window fills — it trades array allocation for streaming throughput.
