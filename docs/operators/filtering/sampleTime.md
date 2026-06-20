# sampleTime

**Category**: Filtering  
**Import**: `import { sampleTime } from 'rxjs';`

## Description

`sampleTime` periodically samples the source Observable at a regular interval and emits the most recently received value at each sampling point. If no new value has arrived since the last sample, nothing is emitted at that interval tick.

Sampling begins as soon as the output Observable is subscribed, regardless of whether the source has emitted anything. This makes `sampleTime` useful for polling the "current state" of a fast-changing stream at a controlled rate.

## Signature

```typescript
function sampleTime<T>(period: number, scheduler?: SchedulerLike): MonoTypeOperatorFunction<T>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| period | `number` | The sampling interval in milliseconds. |
| scheduler | `SchedulerLike` | Optional. Defaults to `asyncScheduler`. |

## Return Type

`MonoTypeOperatorFunction<T>` — emits the most recent source value at each `period` tick (if a new value has been received).

## Marble Diagram

```
Source:    --a-b-c---d------e--|
           sampleTime(4)
Output:    ----c-------d-------e|

Time:      0  1  2  3  4  5  6  7  8  9
Source:    a  b  c        d           e
Sample:          |        |        |
Output:          c        d        (nothing — e arrives after)
```

## Examples

### Example 1: Sample mouse position at 100ms intervals

```typescript
import { fromEvent } from 'rxjs';
import { sampleTime, map } from 'rxjs';

fromEvent<MouseEvent>(document, 'mousemove').pipe(
  sampleTime(100),
  map(ev => ({ x: ev.clientX, y: ev.clientY }))
).subscribe(pos => {
  console.log('Sampled position:', pos);
  // Update a smooth-moving UI element at 10fps
});
```

### Example 2: Log the most recent sensor reading every second

```typescript
import { Subject } from 'rxjs';
import { sampleTime } from 'rxjs';

const sensorData$ = new Subject<number>();

sensorData$.pipe(
  sampleTime(1000)
).subscribe(reading => console.log('Sensor reading at second boundary:', reading));

// High-frequency sensor; we only log once per second
let val = 0;
const sensor = setInterval(() => sensorData$.next(val += Math.random()), 50);
```

### Example 3: Aggregate UI events for analytics

```typescript
import { fromEvent, merge } from 'rxjs';
import { sampleTime, scan, map } from 'rxjs';

const clicks$ = fromEvent(document, 'click');
const keydowns$ = fromEvent(document, 'keydown');

merge(clicks$, keydowns$).pipe(
  scan(count => count + 1, 0),  // count total interactions
  sampleTime(5000)               // report every 5 seconds
).subscribe(count => {
  console.log(`User interactions in this period: ${count}`);
  // Send analytics beacon
});
```

## Common Pitfalls

- **Does not emit on every tick**: If the source has not emitted a new value since the last sample, that period's tick produces nothing. Use `timer` or `interval` if you need regular emissions even without source activity.
- **Period starts from subscription, not first emission**: The sampling clock starts immediately on subscription. The first sample fires after `period` ms regardless of when the source first emits.
- **vs `auditTime`**: `sampleTime` samples on a fixed-clock schedule independent of source activity; `auditTime` starts a new timer each time a source value arrives.

## Related Operators

- `sample` — like `sampleTime` but with any Observable as the notifier
- `auditTime` — emits most recent value after a duration triggered by each source emission
- `throttleTime` — emits the first value in each time window
- `debounceTime` — emits only after the source has been silent
