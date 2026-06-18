# bufferCount

## Identity

- **Name**: bufferCount
- **Category**: Transformation Operators
- **Type**: Count-based collector — accumulates source values into arrays of a fixed size
- **Import**:
  ```typescript
  import { bufferCount } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  function bufferCount<T>(
    bufferSize: number,
    startBufferEvery?: number
  ): OperatorFunction<T, T[]>
  ```

## Functional Specification

**`bufferCount(n)`**: Collects exactly `n` values, emits the array, then starts a new buffer. Emits on count, not on time.

**`bufferCount(n, every)`**: Opens a new buffer every `every` emissions.
- `every < n` → overlapping windows (sliding window)
- `every > n` → gaps between windows (values dropped between close and next open)
- `every === n` → same as `bufferCount(n)` (non-overlapping)

**Comparison with `bufferTime`**:

| | `bufferCount(n)` | `bufferTime(ms)` |
|---|---|---|
| Buffer closes when | N values collected | Time window expires |
| Empty buffers | Never (always N values) | Yes (emits `[]` for empty windows) |
| Use when | Need fixed-size batches | Need time-based batches |

**Invariants**:
- The final buffer (on source completion) is emitted even if it has fewer than `n` values
- Never emits `[]` — a buffer always contains at least one value

## Marble Diagram

```
Source:   --1--2--3--4--5--6--|

bufferCount(3):
Result:   --------[1,2,3]--------[4,5,6]--|

bufferCount(3, 1):  (sliding window, new buffer every 1 value)
Result:   ----[1,2,3]--[2,3,4]--[3,4,5]--[4,5,6]--|

bufferCount(3, 2):  (new buffer every 2, size 3 → overlap of 1)
Result:   --------[1,2,3]--[3,4,5]--|  (buffer at 1,3; overlap at 3)

Source completes before buffer fills:
Source:   --1--2--|
bufferCount(5):
Result:   -------[1,2]|   (partial buffer emitted on completion)
```

## Type System Integration

```typescript
import { interval } from 'rxjs';
import { bufferCount, take } from 'rxjs/operators';

// Output is always T[]
interval(100).pipe(
  take(9),
  bufferCount(3)
).subscribe((batch: number[]) => console.log(batch));
// [0, 1, 2]
// [3, 4, 5]
// [6, 7, 8]
```

## Examples

### Basic Usage — Fixed-Size Batches
```typescript
import { from, interval } from 'rxjs';
import { bufferCount, take } from 'rxjs/operators';

// Batch array items into groups of 3
from([1, 2, 3, 4, 5, 6, 7]).pipe(
  bufferCount(3)
).subscribe(console.log);
// [1, 2, 3]
// [4, 5, 6]
// [7]  ← partial final buffer on completion

// Process a stream in chunks of 10
interval(100).pipe(
  take(25),
  bufferCount(10)
).subscribe(batch => processBatch(batch));
// [0..9], [10..19], [20..24]
```

### Common Pattern — Sliding Window Analysis
```typescript
import { from } from 'rxjs';
import { bufferCount, map, filter } from 'rxjs/operators';

const prices = [10, 12, 11, 13, 15, 14, 16, 18, 17, 19];

// 3-period moving average
from(prices).pipe(
  bufferCount(3, 1),         // window of 3, slide by 1
  filter(w => w.length === 3), // skip partial windows at start
  map(w => w.reduce((a, b) => a + b, 0) / w.length)
).subscribe(avg => console.log(avg.toFixed(2)));
// 11.00, 12.00, 13.00, 14.00, 15.00, 16.00, 17.33, 18.00
```

### Common Pattern — Batch HTTP Writes
```typescript
import { Subject } from 'rxjs';
import { bufferCount, mergeMap } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

const events$ = new Subject<AnalyticsEvent>();

// Send to server in batches of 50
events$.pipe(
  bufferCount(50),
  mergeMap(batch => ajax.post('/api/analytics', { events: batch }))
).subscribe({
  error: err => console.error('batch failed:', err)
});
```

### Common Pattern — Pairwise Alternative
```typescript
import { of } from 'rxjs';
import { bufferCount, map } from 'rxjs/operators';

// bufferCount(2, 1) is a generalized pairwise
of(1, 2, 3, 4, 5).pipe(
  bufferCount(2, 1),
  filter(w => w.length === 2),
  map(([prev, curr]) => curr - prev)
).subscribe(console.log); // 1, 1, 1, 1  (differences between consecutive values)

// pairwise() is cleaner for this specific case:
// of(1,2,3,4,5).pipe(pairwise(), map(([p,c]) => c - p))
```

## Common Pitfalls

### Anti-pattern: Expecting No Partial Final Buffer
```typescript
import { of } from 'rxjs';
import { bufferCount } from 'rxjs/operators';

// ❌ SURPRISE — final buffer has fewer than bufferSize values
of(1, 2, 3, 4, 5).pipe(
  bufferCount(3)
).subscribe(batch => {
  if (batch.length !== 3) throw new Error('expected exactly 3!'); // throws on [4,5]
});

// ✅ CORRECT — handle partial final buffers
of(1, 2, 3, 4, 5).pipe(
  bufferCount(3),
  filter(batch => batch.length === 3) // only full batches
).subscribe(processFullBatch);

// Or handle all including partial:
of(1, 2, 3, 4, 5).pipe(
  bufferCount(3)
).subscribe(batch => {
  const isFull = batch.length === 3;
  processBatch(batch, isFull);
});

// WHY: When the source completes mid-buffer, bufferCount emits the remaining
// values as a partial array. Unlike time-based buffers (which emit [] for
// empty windows), bufferCount never drops values — it always emits the tail.
```

### Anti-pattern: Using `bufferCount` for Deduplication
```typescript
import { from } from 'rxjs';
import { bufferCount } from 'rxjs/operators';

// ❌ WRONG — bufferCount doesn't know about value content
from([1, 1, 2, 2, 3]).pipe(
  bufferCount(2)
).subscribe(console.log); // [1,1], [2,2], [3]  (pairs, not deduplicated)

// ✅ CORRECT — use distinct or distinctUntilChanged for dedup
import { distinct } from 'rxjs/operators';
from([1, 1, 2, 2, 3]).pipe(distinct()).subscribe(console.log); // 1, 2, 3
```

## Related Operators

- **`bufferTime(ms)`**: Same batch concept but triggered by time intervals — emits `[]` for empty windows
- **`buffer(notifier$)`**: Buffer until an Observable fires — variable-size windows driven by signal
- **`windowCount(n)`**: Like `bufferCount` but emits inner `Observable<T>` instead of `T[]`
- **`pairwise()`**: Equivalent to `bufferCount(2, 1)` for consecutive pairs
- **`toArray()`**: Collect all values into one array on source completion

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/bufferCount](https://rxjs.dev/api/operators/bufferCount)

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 3/5 | **Composability**: 4/5
**Key teaching points**:
1. Final buffer on completion may be partial (fewer than `bufferSize`) — handle it
2. `startBufferEvery < bufferSize` → overlapping sliding windows
3. Never emits `[]` (unlike `bufferTime`) — a buffer always has at least one value
