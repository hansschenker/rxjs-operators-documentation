# auditTime

**Category**: Filtering  
**Import**: `import { auditTime } from 'rxjs';`

## Description

`auditTime` is a simplified version of `audit` that uses a fixed time duration. When the first source value arrives, a timer of `duration` milliseconds starts. When the timer expires, the most recent source value received during that window is emitted. Then the process repeats for the next incoming value.

This is the trailing-edge counterpart of `throttleTime` (which emits the leading edge). It is distinct from `debounceTime` in that `auditTime` emits on a regular cadence regardless of source activity, whereas `debounceTime` only emits after a silence period.

## Signature

```typescript
function auditTime<T>(duration: number, scheduler?: SchedulerLike): MonoTypeOperatorFunction<T>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| duration | `number` | Time in milliseconds to wait before emitting the most recent value. |
| scheduler | `SchedulerLike` | Optional. Defaults to `asyncScheduler`. |

## Return Type

`MonoTypeOperatorFunction<T>` — rate-limited emissions; always the most recent value at end of each `duration` window.

## Marble Diagram

```
Source:    --a-b-c--------d-e--|
           auditTime(3)        (3 time units)
Output:    ---------c---------e|
           (most recent value when each timer expires)

Time:      0  1  2  3  4  5  6  7  8  9
Source:    a  b  c           d  e
Timer:          [---]           [---]
Output:            c               e
```

## Examples

### Example 1: Limit click rate to at most one per second

```typescript
import { fromEvent } from 'rxjs';
import { auditTime } from 'rxjs';

fromEvent(document, 'click').pipe(
  auditTime(1000)
).subscribe(ev => console.log('Click processed:', ev));

// Rapid clicks are coalesced; only the last click in each 1s window is logged
```

### Example 2: Smooth resize events for layout recalculation

```typescript
import { fromEvent } from 'rxjs';
import { auditTime, map } from 'rxjs';

fromEvent(window, 'resize').pipe(
  auditTime(200),
  map(() => ({ width: window.innerWidth, height: window.innerHeight }))
).subscribe(size => {
  console.log('Recalculating layout for size:', size);
  // Expensive layout calculation here
});
```

### Example 3: Batch rapid state updates before rendering

```typescript
import { Subject } from 'rxjs';
import { auditTime } from 'rxjs';

const stateUpdate$ = new Subject<Record<string, unknown>>();

stateUpdate$.pipe(
  auditTime(16) // roughly one animation frame at 60fps
).subscribe(state => {
  console.log('Rendering with latest state:', state);
  // Re-render component
});

// Multiple rapid updates — only the last within 16ms is rendered
stateUpdate$.next({ count: 1 });
stateUpdate$.next({ count: 2 });
stateUpdate$.next({ count: 3 }); // this one gets rendered
```

## Common Pitfalls

- **Emits the most recent value, not the first**: If you need the first value in each window (leading edge), use `throttleTime` instead.
- **Silent periods reset nothing**: Unlike `debounceTime`, the timer in `auditTime` ticks from when the first value arrives and fires regardless of subsequent activity. More values during the window only change which value is "most recent" — the timer keeps running.
- **Values are dropped, not buffered**: Intermediate values within a window are permanently discarded. Use `bufferTime` to collect all values in a time window.

## Related Operators

- `audit` — like `auditTime` but with a dynamic duration Observable
- `throttleTime` — emits the first value in each time window (leading edge)
- `debounceTime` — only emits after the source has been silent for `dueTime`
- `sampleTime` — samples the source at regular periodic intervals
