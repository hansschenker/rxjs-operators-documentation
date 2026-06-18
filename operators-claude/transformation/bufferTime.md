# bufferTime

## Identity

- **Name**: bufferTime
- **Category**: Transformation Operators
- **Type**: Time-window collector — accumulates source values into arrays over fixed time intervals
- **Import**:
  ```typescript
  import { bufferTime } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  function bufferTime<T>(
    bufferTimeSpan: number,
    bufferCreationInterval?: number | null,
    maxBufferSize?: number,
    scheduler?: SchedulerLike
  ): OperatorFunction<T, T[]>
  ```

## Functional Specification

**`bufferTime(timeSpan)`**: Collects all source values that arrive within `timeSpan` ms into an array, then emits that array. The process repeats indefinitely. Emits `[]` for windows with no values.

**`bufferTime(timeSpan, creationInterval)`**: Opens a new buffer every `creationInterval` ms, each lasting `timeSpan` ms. Windows can overlap (`creationInterval < timeSpan`) or have gaps (`creationInterval > timeSpan`).

**`maxBufferSize`**: Cap on values per buffer. Buffer closes and emits as soon as it reaches `maxBufferSize`, even if `timeSpan` hasn't elapsed.

**Comparison — bufferTime vs windowTime**:

| | `bufferTime` | `windowTime` |
|---|---|---|
| Output | `T[]` (array) | `Observable<T>` (inner stream) |
| Values available | All at once when window closes | As they arrive (streaming) |
| Use when | You need the complete batch | You need to pipe each window |

## Marble Diagram

```
Source:  --a--b--c-----d--e--|
bufferTime(30ms):
         Window 1 (0–30ms):   closes → [a, b, c]
         Window 2 (30–60ms):  closes → [d, e]  (or [] if empty)
Result:  --------[a,b,c]-------[d,e]--|

bufferTime(50ms, 20ms):  (overlapping windows)
         Window opens every 20ms, lasts 50ms
         Window 1 (0–50ms):   [a,b,c,d]
         Window 2 (20–70ms):  [b,c,d,e]   (overlaps with window 1)
         Window 3 (40–90ms):  [c,d,e]
         ...

bufferTime(30ms) on empty source:
Result:  --------[]-------[]--|   (empty arrays emitted each window)
```

## Type System Integration

```typescript
import { interval } from 'rxjs';
import { bufferTime, take } from 'rxjs/operators';

// Output is T[] — always an array
interval(100).pipe(
  bufferTime(500),
  take(3)
).subscribe((batch: number[]) => console.log(batch));
// [0, 1, 2, 3, 4]
// [5, 6, 7, 8, 9]
// [10, 11, 12, 13, 14]
```

## Examples

### Basic Usage — Batch Processing
```typescript
import { interval } from 'rxjs';
import { bufferTime } from 'rxjs/operators';

// Collect sensor readings every 500ms and process as a batch
interval(50).pipe(
  bufferTime(500)
).subscribe(readings => {
  if (readings.length > 0) {
    const avg = readings.reduce((a, b) => a + b, 0) / readings.length;
    console.log(`Batch of ${readings.length}, avg: ${avg.toFixed(2)}`);
  }
});
```

### Common Pattern — Batch HTTP Writes
```typescript
import { Subject } from 'rxjs';
import { bufferTime, filter, mergeMap } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

const analyticsEvent$ = new Subject<AnalyticsEvent>();

// Collect events for 2 seconds, then send as a single batch request
analyticsEvent$.pipe(
  bufferTime(2000),
  filter(batch => batch.length > 0), // skip empty windows
  mergeMap(batch =>
    ajax.post('/api/analytics/batch', { events: batch })
  )
).subscribe({
  next: () => console.log('batch sent'),
  error: err => console.error('batch failed:', err)
});

// Events are emitted one at a time but sent in batches every 2 seconds
document.addEventListener('click', e =>
  analyticsEvent$.next({ type: 'click', x: e.clientX, y: e.clientY })
);
```

### Common Pattern — Overlapping Windows for Sliding Analysis
```typescript
import { fromEvent } from 'rxjs';
import { bufferTime, map, filter } from 'rxjs/operators';

// Detect rapid clicks: 3+ clicks in any 1-second window
// New window every 200ms (overlapping)
fromEvent(document, 'click').pipe(
  bufferTime(1000, 200),     // 1s window, new window every 200ms
  filter(clicks => clicks.length >= 3),
  map(clicks => clicks.length)
).subscribe(count => console.log(`Rapid click burst: ${count} clicks`));
```

### Common Pattern — `maxBufferSize` for Backpressure
```typescript
import { Subject } from 'rxjs';
import { bufferTime } from 'rxjs/operators';

const events$ = new Subject<Event>();

// Process up to 100 events per batch, or whatever arrived in 500ms
events$.pipe(
  bufferTime(500, null, 100) // timeSpan=500ms, creationInterval=null (no overlap), max=100
).subscribe(batch => processBatch(batch));

// If 100 events arrive in 50ms, the buffer closes immediately and emits
// without waiting for the full 500ms window
```

## Common Pitfalls

### Anti-pattern: Not Filtering Empty Buffers
```typescript
import { interval } from 'rxjs';
import { bufferTime } from 'rxjs/operators';

// ❌ PROCESSES EMPTY ARRAYS — bufferTime emits [] for windows with no values
interval(1000).pipe(
  bufferTime(300)  // interval(1000) only emits once per second
).subscribe(batch => {
  processExpensiveOperation(batch); // called ~3x per second with [] batches!
});

// ✅ CORRECT — filter empty windows before processing
import { filter } from 'rxjs/operators';
interval(1000).pipe(
  bufferTime(300),
  filter(batch => batch.length > 0) // skip empty windows
).subscribe(batch => processExpensiveOperation(batch));

// WHY: bufferTime emits an array for every window interval, including empty ones.
// If your source emits less frequently than bufferTimeSpan, most windows are [].
// Always filter(batch => batch.length > 0) unless empty batches are meaningful.
```

### Anti-pattern: `bufferTime` When `debounceTime` / `auditTime` Is Needed
```typescript
import { fromEvent } from 'rxjs';
import { bufferTime, map } from 'rxjs/operators';

// ❌ WRONG TOOL — using bufferTime just to get the latest value
fromEvent(window, 'scroll').pipe(
  bufferTime(16),
  map(events => events[events.length - 1]) // take the last event in the window
).subscribe(handleScroll);
// Allocates an array on every frame just to take the last element

// ✅ CORRECT — auditTime gives you the latest value with no array allocation
import { auditTime } from 'rxjs/operators';
fromEvent(window, 'scroll').pipe(
  auditTime(16)
).subscribe(handleScroll);

// WHY: bufferTime's purpose is batch collection. If you only need the latest
// value in a time window, auditTime or debounceTime are more efficient —
// no array allocation, no filter needed.
```

## Related Operators

- **`windowTime`**: Like `bufferTime` but emits inner `Observable<T>` instead of `T[]` — use when you need to pipe values as they arrive
- **`bufferCount(n)`**: Buffer by count instead of time — emit array of exactly N values
- **`buffer(notifier$)`**: Buffer until an Observable fires — variable-size windows driven by a signal
- **`auditTime / debounceTime`**: Rate limiting — take only the latest value, no array
- **`groupBy`**: Partition by key rather than time

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/bufferTime](https://rxjs.dev/api/operators/bufferTime)

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 3/5 | **Composability**: 4/5
**Key teaching points**:
1. Always `filter(batch => batch.length > 0)` unless empty batches are intentional
2. `bufferTime` vs `windowTime`: array vs inner Observable
3. Use `auditTime` / `debounceTime` when you need the latest single value, not a batch
