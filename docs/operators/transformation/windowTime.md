# windowTime

**Category**: Transformation  
**Import**: `import { windowTime } from 'rxjs';`

## Description

Branches the source Observable values as a nested Observable periodically in time. Like `bufferTime`, but emits nested Observables (windows) rather than arrays.

The simplest form emits a new window Observable every `windowTimeSpan` milliseconds. With `windowCreationInterval`, you can control how often new windows open independently of how long they last. With `maxWindowSize`, each window closes early once it has emitted that many values.

## Signature

```typescript
function windowTime<T>(windowTimeSpan: number, scheduler?: SchedulerLike): OperatorFunction<T, Observable<T>>
function windowTime<T>(windowTimeSpan: number, windowCreationInterval: number, scheduler?: SchedulerLike): OperatorFunction<T, Observable<T>>
function windowTime<T>(windowTimeSpan: number, windowCreationInterval: number | null | void, maxWindowSize: number, scheduler?: SchedulerLike): OperatorFunction<T, Observable<T>>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `windowTimeSpan` | `number` | How long (ms) each window lasts before completing. |
| `windowCreationInterval` | `number \| null` | Optional. How often (ms) to open a new window. If omitted, a new window opens when the previous one closes. |
| `maxWindowSize` | `number` | Optional. Maximum number of values per window. The window closes early if this limit is reached. |
| `scheduler` | `SchedulerLike` | Optional. Defaults to `asyncScheduler`. |

## Return Type

`OperatorFunction<T, Observable<T>>` — emits a new window Observable on a time-based schedule.

## Marble Diagram

```
Source (windowTime(1000)):
  --a-b-c-d-e-f--|
  --w1-----w2----|
    w1: --a-b-c|
    w2:      --d-e-f|
```

## Examples

### Example 1: Limit click handling to at most 2 per second

```typescript
import { fromEvent, windowTime, map, take, mergeAll } from 'rxjs';

const clicks = fromEvent(document, 'click');

clicks.pipe(
  windowTime(1000),
  map(win => win.pipe(take(2))),
  mergeAll()
).subscribe(() => console.log('Processed click'));
```

### Example 2: Compute throughput metrics every 5 seconds

```typescript
import { Subject, windowTime, mergeMap, reduce, map } from 'rxjs';

interface Request { url: string; durationMs: number }

const request$ = new Subject<Request>();

request$.pipe(
  windowTime(5000),
  mergeMap(win =>
    win.pipe(
      reduce(
        (stats, req) => ({
          count: stats.count + 1,
          totalMs: stats.totalMs + req.durationMs,
        }),
        { count: 0, totalMs: 0 }
      ),
      map(stats => ({
        requestsPerSecond: stats.count / 5,
        avgDurationMs: stats.count ? stats.totalMs / stats.count : 0,
      }))
    )
  )
).subscribe(metrics => console.log('5s metrics:', metrics));
```

### Example 3: Overlapping windows — open every 5s, last 2s

```typescript
import { fromEvent, windowTime, mergeMap, toArray } from 'rxjs';

fromEvent<MouseEvent>(document, 'mousemove').pipe(
  windowTime(2000, 5000),
  mergeMap(win => win.pipe(toArray()))
).subscribe(positions => {
  console.log(`Captured ${positions.length} mouse positions in 2s window`);
});
```

## Common Pitfalls

- **Higher-order Observable**: Subscribe to window Observables using `mergeMap`, `concatMap`, or flattening operators. Without flattening you receive Observable objects.
- **Missed early emissions**: Window Observables start emitting values immediately when opened. Subscribe promptly inside `mergeMap` or similar operators to avoid missing early values.
- **Scheduler required for testing**: The default `asyncScheduler` uses real time. To test `windowTime` in unit tests, inject a `TestScheduler` as the last argument.

## Related Operators

- `bufferTime` — like `windowTime` but collects into arrays instead of Observables
- `window` — boundary determined by a separate Observable
- `windowCount` — boundary determined by item count
- `windowToggle` — separate open/close Observables
- `windowWhen` — dynamic closing factory function
