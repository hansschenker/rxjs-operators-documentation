# windowCount

## Identity

- **Name**: windowCount
- **Category**: Transformation Operators (Higher-Order)
- **Type**: Count-based window opener — emits inner `Observable<T>` windows of a fixed count
- **Import**:
  ```typescript
  import { windowCount } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  function windowCount<T>(
    windowSize: number,
    startWindowEvery?: number
  ): OperatorFunction<T, Observable<T>>
  ```

## Functional Specification

`windowCount` is the streaming counterpart to `bufferCount`. Instead of emitting `T[]` arrays, it emits `Observable<T>` windows. Each inner Observable emits up to `windowSize` values and then completes.

**`windowCount(n)`**: Opens a new window every `n` values. The current window completes after `n` values; a new window opens immediately.

**`windowCount(n, every)`**: Opens a new window every `every` emissions.
- `every < n` → overlapping windows (sliding)
- `every > n` → gaps (some values belong to no window)

**`windowCount` vs `bufferCount`**:

| | `windowCount(n)` | `bufferCount(n)` |
|---|---|---|
| Emits | `Observable<T>` | `T[]` |
| Values available | As they arrive (streaming) | After window closes |
| Memory | Potentially lower (stream processing) | Full batch in memory |
| Use when | Need to apply operators per window | Need the full array |

**Key invariant**: You MUST subscribe to each inner Observable (via `mergeMap`, `switchMap`, etc.) or the source backpressures.

## Marble Diagram

```
Source:   --1--2--3--4--5--6--|

windowCount(3):
Outer:    W1----W2----W3-----|
W1:       --1--2--3|
W2:       ----------4--5--6|
W3:       (partial) ----------|  (empty if source completes exactly on window boundary)

windowCount(3, 1):  sliding window
Outer:    W1-W2-W3-W4...
W1:       --1--2--3|
W2:        -2--3--4|
W3:          -3--4--5|
...
```

## Type System Integration

```typescript
import { interval } from 'rxjs';
import { windowCount, take, mergeMap, toArray } from 'rxjs/operators';

// Each inner window is Observable<number>
interval(100).pipe(
  take(9),
  windowCount(3),
  mergeMap(window$ => window$.pipe(toArray()))
).subscribe((batch: number[]) => console.log(batch));
// [0, 1, 2]
// [3, 4, 5]
// [6, 7, 8]
```

## Examples

### Basic Usage — Process Windows as They Stream
```typescript
import { interval } from 'rxjs';
import { windowCount, take, mergeMap, reduce } from 'rxjs/operators';

// Sum each window of 3 values as values arrive
interval(100).pipe(
  take(9),
  windowCount(3),
  mergeMap(window$ => window$.pipe(reduce((acc, v) => acc + v, 0)))
).subscribe(console.log);
// 3 (0+1+2)
// 12 (3+4+5)
// 21 (6+7+8)
```

### Common Pattern — Sliding Window (Moving Average)
```typescript
import { from } from 'rxjs';
import { windowCount, mergeMap, toArray, map, filter } from 'rxjs/operators';

const prices = [10, 12, 11, 13, 15, 14, 16, 18, 17, 19];

// 3-period moving average with windowCount
from(prices).pipe(
  windowCount(3, 1),
  mergeMap(w$ => w$.pipe(toArray())),
  filter(w => w.length === 3),
  map(w => w.reduce((a, b) => a + b, 0) / 3)
).subscribe(avg => console.log(avg.toFixed(2)));
// 11.00, 12.00, 13.00, 14.00, 15.00, 16.00, 17.33, 18.00
```

### Common Pattern — First Value Per Window
```typescript
import { Subject } from 'rxjs';
import { windowCount, mergeMap, first } from 'rxjs/operators';

const clicks$ = new Subject<MouseEvent>();

// Take only the first click from every group of 5
clicks$.pipe(
  windowCount(5),
  mergeMap(window$ => window$.pipe(first()))
).subscribe(firstClick => handleFirstOfFive(firstClick));

// This is a throttle-by-count: every 5 clicks, process only the first
```

### Edge Case — Accessing Window Index
```typescript
import { interval } from 'rxjs';
import { windowCount, take, mergeMap, scan, map, toArray } from 'rxjs/operators';

// windowCount doesn't expose index — track it yourself with scan on the outer
interval(100).pipe(
  take(6),
  windowCount(3),
  scan((idx, window$) => [idx[0] + 1, window$] as [number, typeof window$], [0, null as any]),
  mergeMap(([idx, window$]) =>
    window$.pipe(toArray(), map(vals => ({ window: idx, values: vals })))
  )
).subscribe(console.log);
// { window: 1, values: [0, 1, 2] }
// { window: 2, values: [3, 4, 5] }
```

## Common Pitfalls

### Anti-pattern: Not Subscribing to Inner Observables
```typescript
import { interval } from 'rxjs';
import { windowCount, take, tap } from 'rxjs/operators';

// ❌ WRONG — inner windows are never subscribed; values are dropped
interval(100).pipe(
  take(9),
  windowCount(3),
  tap(window$ => console.log('got window')) // tap doesn't subscribe!
).subscribe(() => {}); // outer subscribes but inner windows are ignored

// ✅ CORRECT — use mergeMap (or switchMap/concatMap) to subscribe to each window
import { mergeMap, toArray } from 'rxjs/operators';
interval(100).pipe(
  take(9),
  windowCount(3),
  mergeMap(window$ => window$.pipe(toArray()))
).subscribe(console.log);

// WHY: windowCount emits Observable<T> values (higher-order Observable).
// The inner Observables must be subscribed to. Simply logging them or using
// tap gives you the window Observable object, not its values.
```

### Anti-pattern: Choosing `windowCount` When `bufferCount` Is Simpler
```typescript
// ❌ NEEDLESSLY COMPLEX — converting window back to array
source$.pipe(
  windowCount(3),
  mergeMap(w$ => w$.pipe(toArray())), // just gives you T[]
  map(batch => batch.map(transform))
).subscribe(console.log);

// ✅ SIMPLER — bufferCount when you just need the array
source$.pipe(
  bufferCount(3),
  map(batch => batch.map(transform))
).subscribe(console.log);

// WHY: windowCount shines when you want to apply reactive operators
// (take, first, filter, reduce) INSIDE each window before it closes.
// If you just need a T[] batch, use bufferCount.
```

## Related Operators

- **`bufferCount(n)`**: Same semantics, emits `T[]` instead of `Observable<T>` — simpler when you need the full array
- **`windowTime(ms)`**: Time-based windows — same streaming window model
- **`window(notifier$)`**: Signal-driven windows — open/close on an Observable
- **`mergeMap`**: Required to subscribe to inner windows from `windowCount`

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/windowCount](https://rxjs.dev/api/operators/windowCount)

---

**Cognitive Load**: 3/5 | **Usage Frequency**: 2/5 | **Composability**: 4/5
**Key teaching points**:
1. Emits `Observable<T>` — you MUST subscribe via `mergeMap`/`concatMap`/`switchMap`
2. Use `windowCount` over `bufferCount` only when you need to apply reactive operators inside each window (e.g., `first()`, `filter()`, `reduce()`)
3. `windowCount(n, 1)` = overlapping sliding windows; same as `bufferCount(n, 1)` semantics but streamed
