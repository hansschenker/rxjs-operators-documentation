# windowTime

## Identity

- **Name**: windowTime
- **Category**: Transformation Operators
- **Type**: Time-window streaming — emits inner `Observable<T>` windows over fixed time intervals; the streaming counterpart to `bufferTime`
- **Import**:
  ```typescript
  import { windowTime } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  function windowTime<T>(
    windowTimeSpan: number,
    windowCreationInterval?: number | null,
    maxWindowSize?: number,
    scheduler?: SchedulerLike
  ): OperatorFunction<T, Observable<T>>
  ```

## Functional Specification

**Concept**: Like `bufferTime`, but instead of emitting `T[]` arrays, `windowTime` emits `Observable<T>` windows. Each window is a Subject that forwards source values as they arrive during the window's lifetime.

**When to use `windowTime` over `bufferTime`**:
- When values need to be processed as they arrive (streaming), not after the window closes
- When you want to pipe each window through its own operator chain
- When memory efficiency matters for high-throughput sources (no array allocation)

**`windowTime` vs `bufferTime`**:

| | `windowTime` | `bufferTime` |
|---|---|---|
| Output type | `Observable<Observable<T>>` | `Observable<T[]>` |
| Values available | As they arrive (streaming) | All at once when window closes |
| Memory | Lower (no array accumulation) | Higher (array grows during window) |
| Use when | Need to pipe/react within window | Need complete batch after window |

**`windowCreationInterval`**: Like `bufferTime`'s second param — open a new window every N ms (overlapping if `< windowTimeSpan`).

## Marble Diagram

```
Source:  --a--b--c-----d--e--|
windowTime(30ms):

Outer:   W1--------W2--------W3--|
         |         |
         W1: --a--b--c|   (inner Observable, completes at 30ms)
         W2: -----d--e|   (inner Observable, completes at 30ms or source end)
         W3: |           (empty window if source completes immediately)

Compare to bufferTime(30ms):
Result:  --------[a,b,c]-------[d,e]--|  (arrays, not Observables)
```

## Type System Integration

```typescript
import { interval } from 'rxjs';
import { windowTime, mergeMap, toArray, take } from 'rxjs/operators';

// Output is Observable<Observable<number>>
interval(100).pipe(
  windowTime(500),
  mergeMap(window$ => window$.pipe(toArray())),  // collect each window
  take(3)
).subscribe((batch: number[]) => console.log(batch));
// [0, 1, 2, 3, 4]
// [5, 6, 7, 8, 9]
// [10, 11, 12, 13, 14]
```

## Examples

### Basic Usage — Window Into Streaming Pipeline
```typescript
import { interval } from 'rxjs';
import { windowTime, mergeMap, max, take } from 'rxjs/operators';

// Find the maximum value in each 500ms window as values stream in
interval(50).pipe(
  windowTime(500),
  mergeMap(window$ => window$.pipe(max())), // max() processes values as they arrive
  take(5)
).subscribe(maxVal => console.log('window max:', maxVal));
// 9, 19, 29, 39, 49  (max of each 10-value window)
```

### Common Pattern — Per-Window Pipe Chain
```typescript
import { fromEvent } from 'rxjs';
import { windowTime, mergeMap, scan, last } from 'rxjs/operators';

// Count clicks per second, emitting the running total at the end of each second
fromEvent(document, 'click').pipe(
  windowTime(1000),
  mergeMap(window$ =>
    window$.pipe(
      scan(count => count + 1, 0), // running count within window
      last(null, 0)                 // emit final count (0 if no clicks)
    )
  )
).subscribe(clicksPerSecond => updateClickRate(clicksPerSecond));
```

### Common Pattern — Rate Analysis on Streaming Data
```typescript
import { Subject } from 'rxjs';
import { windowTime, mergeMap, reduce, map } from 'rxjs/operators';

interface Trade { symbol: string; price: number; volume: number }

const trades$ = new Subject<Trade>();

// Calculate VWAP (volume-weighted average price) per 1-minute window
trades$.pipe(
  windowTime(60_000),
  mergeMap(window$ =>
    window$.pipe(
      reduce(
        (acc, trade) => ({
          totalValue:  acc.totalValue + trade.price * trade.volume,
          totalVolume: acc.totalVolume + trade.volume
        }),
        { totalValue: 0, totalVolume: 0 }
      ),
      map(({ totalValue, totalVolume }) =>
        totalVolume > 0 ? totalValue / totalVolume : 0
      )
    )
  )
).subscribe(vwap => console.log('1-min VWAP:', vwap.toFixed(2)));
```

### When to Choose `bufferTime` vs `windowTime`
```typescript
import { interval } from 'rxjs';
import { bufferTime, windowTime, mergeMap, toArray } from 'rxjs/operators';

// USE bufferTime when you need the complete array at once
interval(100).pipe(
  bufferTime(500)
  // ↑ simple, you get T[] ready to use
).subscribe(batch => sendBatchToServer(batch));

// USE windowTime when you need to pipe values through operators
interval(100).pipe(
  windowTime(500),
  mergeMap(w$ => w$.pipe(
    // Can apply operators to each value AS IT ARRIVES
    // e.g., filter, map, scan — without waiting for window to close
    toArray() // or reduce, max, custom aggregation
  ))
).subscribe(batch => sendBatchToServer(batch));
```

## Common Pitfalls

### Anti-pattern: Not Subscribing to Inner Windows
```typescript
import { interval } from 'rxjs';
import { windowTime } from 'rxjs/operators';

// ❌ BROKEN — inner Observables not subscribed; values are lost
interval(100).pipe(
  windowTime(500)
).subscribe(window$ => {
  console.log('new window'); // fires every 500ms
  // window$ is an Observable — never subscribed!
  // Values inside it are emitted and immediately discarded
});

// ✅ CORRECT — always subscribe to inner windows with mergeMap
import { mergeMap, toArray } from 'rxjs/operators';
interval(100).pipe(
  windowTime(500),
  mergeMap(window$ => window$.pipe(toArray())) // subscribe + collect
).subscribe(batch => console.log(batch));

// WHY: windowTime emits Observables (hot inner Subjects). If you don't
// subscribe to them, their values are delivered to nothing and discarded.
// This is the same pattern as groupBy — always use mergeMap to consume
// inner Observables.
```

### Anti-pattern: Using `windowTime` When `bufferTime` Is Simpler
```typescript
import { interval } from 'rxjs';
import { windowTime, bufferTime, mergeMap, toArray } from 'rxjs/operators';

// ❌ OVERENGINEERED — using windowTime just to get an array
interval(100).pipe(
  windowTime(500),
  mergeMap(w$ => w$.pipe(toArray())) // effectively recreates bufferTime
).subscribe(console.log);

// ✅ SIMPLER — bufferTime gives you the array directly
interval(100).pipe(
  bufferTime(500)
).subscribe(console.log);

// WHY: windowTime is for streaming pipelines where you need to process values
// as they arrive. If all you need is an array at the end of each window,
// bufferTime is the simpler, more direct choice.
```

## Related Operators

- **`bufferTime`**: Same time-windowing logic, emits `T[]` — use when you need the complete batch
- **`window(notifier$)`**: Like `windowTime` but opened/closed by an Observable signal
- **`windowCount(n)`**: Opens a new window every N values instead of every N ms
- **`groupBy`**: Partition by key (not time) into persistent named inner Observables
- **`mergeMap`**: Required companion — always needed to consume `windowTime`'s inner Observables

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/windowTime](https://rxjs.dev/api/operators/windowTime)

---

**Cognitive Load**: 3/5 | **Usage Frequency**: 2/5 | **Composability**: 4/5
**Key teaching point**: `windowTime` = `bufferTime` but streaming. The inner Observable MUST be subscribed to (via `mergeMap`). Choose `bufferTime` for batch output; choose `windowTime` when you need to react to values as they arrive within each window.
