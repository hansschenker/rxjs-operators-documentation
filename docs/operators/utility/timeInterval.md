# timeInterval

**Category**: Utility  
**Import**: `import { timeInterval } from 'rxjs';`

## Description

`timeInterval` transforms each source emission into a `TimeInterval<T>` object that pairs the original value with the number of milliseconds that elapsed since the previous emission (or since subscription, for the first value). This makes it easy to measure throughput, detect slow producers, or display timing information in a UI.

The elapsed time is measured using the scheduler's `now()` method, which defaults to `asyncScheduler` and therefore returns wall-clock milliseconds. Because JavaScript timers are non-deterministic, the `interval` values will never be perfectly precise.

## Signature

```typescript
function timeInterval<T>(scheduler: SchedulerLike = asyncScheduler): OperatorFunction<T, TimeInterval<T>>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| scheduler | `SchedulerLike` | Scheduler used to get the current timestamp. Defaults to `asyncScheduler`. |

## Return Type

`OperatorFunction<T, TimeInterval<T>>` — an operator that transforms each source value into an object `{ value: T, interval: number }` where `interval` is the milliseconds since the previous emission.

## Marble Diagram

```
Source:  --a------b--c--|
         timeInterval()
Output:  --{v:a,i:~2}---------{v:b,i:~7}--{v:c,i:~3}--|
```

## Examples

### Example 1: Measure time between keystrokes

```typescript
import { fromEvent, timeInterval, map } from 'rxjs';

fromEvent<KeyboardEvent>(document, 'keydown').pipe(
  timeInterval(),
  map(({ value, interval }) => ({
    key: value.key,
    msSinceLast: Math.round(interval)
  }))
).subscribe(console.log);

// { key: 'h', msSinceLast: 843 }
// { key: 'e', msSinceLast: 112 }
// { key: 'l', msSinceLast: 98 }
```

### Example 2: Detect a slow producer

```typescript
import { interval, timeInterval, filter, tap } from 'rxjs';

const dataStream$ = interval(1000);

dataStream$.pipe(
  timeInterval(),
  tap(({ value, interval }) => {
    if (interval > 1500) {
      console.warn(`Slow emission detected: ${interval}ms for value ${value}`);
    }
  })
).subscribe(({ value }) => console.log('Value:', value));
```

### Example 3: Display typing cadence for analytics

```typescript
import { fromEvent, timeInterval, bufferCount, map } from 'rxjs';

const keyPresses$ = fromEvent(document, 'keydown');

keyPresses$.pipe(
  timeInterval(),
  bufferCount(5),
  map(events => {
    const total = events.reduce((sum, e) => sum + e.interval, 0);
    return `Average interval over last 5 keys: ${(total / 5).toFixed(0)}ms`;
  })
).subscribe(console.log);
```

## Common Pitfalls

- **First value includes startup time**: The `interval` for the very first emission measures the time from subscription, not from a prior emission. This is usually larger than subsequent intervals.
- **Non-deterministic timing**: JavaScript's event loop means interval values will drift from nominal values, especially under load. Do not use for precise timing requirements.
- **Changed output type**: Remember that `timeInterval` changes the type from `T` to `TimeInterval<T>`. Downstream operators must destructure `{ value, interval }`.

## Related Operators

- `timestamp` — records the absolute wall-clock time of each emission rather than the relative interval
- `delay` — shifts emissions by a fixed duration
- `throttleTime` — limits emission rate by discarding values within a window
