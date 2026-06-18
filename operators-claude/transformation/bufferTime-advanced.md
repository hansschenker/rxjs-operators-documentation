# bufferTime — Advanced Patterns

For `bufferTime` fundamentals see the core [bufferTime](./bufferTime) doc. This page covers micro-batching for performance, adaptive buffer sizing, event coalescing, and sliding windows.

---

## Why Buffer?

Collecting values into batches before processing can dramatically improve performance:
- **Network**: batch 100 analytics events → 1 HTTP request instead of 100
- **DOM**: collect 50 list updates → 1 render instead of 50
- **Database**: batch inserts instead of row-by-row
- **Rate-limiting**: process N events per second regardless of burst rate

---

## Pattern 1: Analytics Event Batching

```typescript
import { Subject, bufferTime, filter, switchMap } from 'rxjs/operators';

const events$ = new Subject<AnalyticsEvent>();

events$.pipe(
  bufferTime(5000),                    // collect 5 seconds of events
  filter(batch => batch.length > 0),   // skip empty windows
  switchMap(batch =>
    this.api.trackEvents(batch).pipe(
      retry(3),
      catchError(() => EMPTY)          // don't let failed batch kill the stream
    )
  )
).subscribe();

// Usage throughout app:
function track(event: AnalyticsEvent) { events$.next(event); }
```

---

## Pattern 2: DOM Update Batching (Virtual List / Table)

Coalesce rapid data changes into a single re-render:

```typescript
import { Subject, bufferTime, filter, map } from 'rxjs/operators';
import { animationFrameScheduler } from 'rxjs';

const rowUpdates$ = new Subject<{ id: string; data: RowData }>();

rowUpdates$.pipe(
  // Buffer until next animation frame:
  bufferTime(0, null, Infinity, animationFrameScheduler),
  filter(updates => updates.length > 0),
  map(updates => {
    // Deduplicate: keep only the latest update per id:
    const map = new Map<string, RowData>();
    updates.forEach(u => map.set(u.id, u.data));
    return Array.from(map.entries()).map(([id, data]) => ({ id, data }));
  })
).subscribe(batchedUpdates => {
  // One DOM update per animation frame, regardless of how many row changes arrived:
  batchedUpdates.forEach(({ id, data }) => updateRow(id, data));
});
```

---

## Pattern 3: Adaptive Buffer Size

Adjust buffer window based on load:

```typescript
import { bufferTime, map, scan, switchMap } from 'rxjs/operators';
import { BehaviorSubject } from 'rxjs';

const windowMs$ = new BehaviorSubject(1000); // start at 1s

events$.pipe(
  bufferTime(0), // collect until switchMap switches
  switchMap(batch => {
    // Adjust window based on throughput:
    const rate = batch.length; // events per window
    if (rate > 100) windowMs$.next(Math.min(windowMs$.getValue() * 2, 10_000));
    if (rate <  10) windowMs$.next(Math.max(windowMs$.getValue() / 2, 100));
    return of(batch);
  })
).subscribe(processBatch);

// Better pattern — dynamic window via switchMap on windowMs$:
windowMs$.pipe(
  switchMap(ms =>
    events$.pipe(
      bufferTime(ms),
      filter(b => b.length > 0)
    )
  )
).subscribe(processBatch);
```

---

## Pattern 4: Count-or-Time (Whichever Comes First)

Flush when either N items collected OR time limit reached:

```typescript
import { bufferTime, filter } from 'rxjs/operators';

// bufferTime(timeSpan, creationInterval, maxBufferSize):
events$.pipe(
  bufferTime(5000, null, 100),    // flush after 5s OR when 100 items collected
  filter(b => b.length > 0)
).subscribe(processBatch);

// Equivalent using bufferCount + bufferTime race:
import { merge, bufferCount } from 'rxjs';

const byCount$ = events$.pipe(bufferCount(100));
const byTime$  = events$.pipe(bufferTime(5000), filter(b => b.length > 0));

// Both emit arrays — take whichever fires first:
// (Note: this approach doesn't fully deduplicate across the two — use bufferTime maxBufferSize instead)
```

---

## Pattern 5: Sliding Window Statistics

```typescript
import { bufferTime, map, filter } from 'rxjs/operators';

interface Metric { value: number; timestamp: number; }

metrics$.pipe(
  // Non-overlapping 10s windows:
  bufferTime(10_000),
  filter(window => window.length > 0),
  map(window => ({
    count:  window.length,
    avg:    window.reduce((s, m) => s + m.value, 0) / window.length,
    min:    Math.min(...window.map(m => m.value)),
    max:    Math.max(...window.map(m => m.value)),
    p95:    percentile(window.map(m => m.value), 0.95)
  }))
).subscribe(updateDashboard);

// Overlapping 10s windows, advancing every 1s (sliding):
metrics$.pipe(
  bufferTime(10_000, 1_000),  // 10s window, new window starts every 1s
  filter(w => w.length > 0),
  map(computeStats)
).subscribe(updateDashboard);

function percentile(values: number[], p: number): number {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length * p)] ?? 0;
}
```

---

## Pattern 6: Undo/Redo with History Batching

Group rapid changes into single undo steps:

```typescript
import { bufferTime, filter, map } from 'rxjs/operators';
import { Subject } from 'rxjs';

const changes$ = new Subject<Change>();
const history: Change[][] = [];

changes$.pipe(
  bufferTime(300),             // group changes within 300ms → single undo step
  filter(batch => batch.length > 0)
).subscribe(batch => {
  history.push(batch);
  applyChanges(batch);
});

function undo() {
  const batch = history.pop();
  if (batch) revertChanges(batch.reverse()); // revert in reverse order
}
```

---

## Pattern 7: Bulk Database Write

```typescript
import { bufferTime, concatMap, filter, retry } from 'rxjs/operators';

const writeQueue$ = new Subject<DbRecord>();

writeQueue$.pipe(
  bufferTime(200, null, 500),       // max 200ms wait, max 500 records per batch
  filter(batch => batch.length > 0),
  concatMap(batch =>                // process batches sequentially
    this.db.bulkInsert(batch).pipe(
      retry(2),
      catchError(err => {
        logger.error('Batch write failed:', err);
        // Re-queue failed records individually for retry:
        batch.forEach(r => writeQueue$.next(r));
        return EMPTY;
      })
    )
  )
).subscribe();
```

---

## `bufferTime` vs `bufferCount` vs `bufferWhen`

```typescript
// bufferTime — time-based windows:
source$.pipe(bufferTime(1000))
// ✓ Predictable timing (flush every second)
// ✗ Variable batch sizes (may be empty)

// bufferCount — size-based windows:
source$.pipe(bufferCount(100))
// ✓ Predictable batch sizes (always 100)
// ✗ Last batch may wait indefinitely for 100 items

// bufferTime(time, null, maxSize) — both constraints:
source$.pipe(bufferTime(5000, null, 100))
// ✓ Bounded size AND bounded wait time

// bufferWhen — dynamic close trigger:
source$.pipe(bufferWhen(() => signal$))
// ✓ Close buffer on external signal (user action, other stream)
```

---

## Common Pitfalls

### Memory Leak from Accumulating Empty Buffers

```typescript
// ❌ Empty buffers processed on every window close — wasteful if source is sparse:
source$.pipe(bufferTime(1000)).subscribe(batch => processBatch(batch))
// When source emits rarely: [] [] [] [] are processed constantly

// ✅ Filter empty windows:
source$.pipe(
  bufferTime(1000),
  filter(batch => batch.length > 0)
).subscribe(processBatch);
```

### Not Handling Buffer Errors Internally

```typescript
// ❌ One batch failure kills the entire buffering stream:
events$.pipe(
  bufferTime(5000),
  filter(b => b.length > 0),
  switchMap(batch => this.api.send(batch)) // error here ends everything!
)

// ✅ Isolate errors inside the batch processor:
events$.pipe(
  bufferTime(5000),
  filter(b => b.length > 0),
  mergeMap(batch =>
    this.api.send(batch).pipe(
      catchError(err => {
        logger.error('Batch send failed', err);
        return EMPTY; // this batch fails, stream continues
      })
    )
  )
)
```

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 3/5 | **Composability**: 4/5
**Performance rule**: `bufferTime(ms, null, maxSize)` is the production form — always set `maxBufferSize` to prevent unbounded memory growth during traffic spikes. Always `filter(b => b.length > 0)` to skip empty windows. Use `mergeMap`/`concatMap` (not `switchMap`) for batch processing — you don't want to cancel an in-flight batch write.
