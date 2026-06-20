# window

**Category**: Transformation  
**Import**: `import { window } from 'rxjs';`

## Description

Branches the source Observable values as a nested Observable whenever `windowBoundaries` emits. Like `buffer`, but emits a nested Observable (a window) instead of an array. Each window is an Observable that subscribers can independently process.

The output is a higher-order Observable: each emission is itself an Observable. When `windowBoundaries` emits, the current window Observable is completed and a new one is emitted downstream. This allows consumers to apply different operators to each window using operators like `mergeAll`, `concatAll`, or `switchAll`.

## Signature

```typescript
function window<T>(windowBoundaries: ObservableInput<any>): OperatorFunction<T, Observable<T>>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `windowBoundaries` | `ObservableInput<any>` | An Observable that signals window boundaries. Each emission completes the current window and opens a new one. |

## Return Type

`OperatorFunction<T, Observable<T>>` — emits a new Observable window each time `windowBoundaries` fires.

## Marble Diagram

```
Source:     --1--2--3--4--5--6--|
Boundaries: --------x-------x--|
              window(boundaries)
Output:     --w1-----w2-----w3--|
  w1:       --1--2--3|
  w2:       --------4--5|
  w3:       ----------------6--|
```

## Examples

### Example 1: Limit clicks per second using window

```typescript
import { fromEvent, interval, window, map, take, mergeAll } from 'rxjs';

const clicks = fromEvent(document, 'click');
const seconds = interval(1000);

clicks.pipe(
  window(seconds),
  map(win => win.pipe(take(2))), // Allow at most 2 clicks per second
  mergeAll()
).subscribe(click => console.log('Allowed click:', click));
```

### Example 2: Compute statistics per window of time

```typescript
import { Subject, interval, window, mergeMap, reduce, map } from 'rxjs';

const price$ = new Subject<number>();
const second$ = interval(1000);

price$.pipe(
  window(second$),
  mergeMap(win =>
    win.pipe(
      reduce(
        (stats, price) => ({
          min: Math.min(stats.min, price),
          max: Math.max(stats.max, price),
          sum: stats.sum + price,
          count: stats.count + 1,
        }),
        { min: Infinity, max: -Infinity, sum: 0, count: 0 }
      ),
      map(stats => ({ ...stats, avg: stats.sum / stats.count }))
    )
  )
).subscribe(stats => console.log('1s stats:', stats));
```

### Example 3: Process WebSocket frames grouped by message boundary

```typescript
import { Subject, window, filter, toArray, mergeMap } from 'rxjs';

interface Frame { isEnd: boolean; data: Uint8Array }

const frame$ = new Subject<Frame>();
const endFrame$ = frame$.pipe(filter(f => f.isEnd));

frame$.pipe(
  window(endFrame$),
  mergeMap(win => win.pipe(toArray()))
).subscribe(messageFrames => {
  const totalBytes = messageFrames.reduce((acc, f) => acc + f.data.byteLength, 0);
  console.log(`Received message: ${totalBytes} bytes across ${messageFrames.length} frames`);
});
```

## Common Pitfalls

- **Higher-order Observable**: The output emits Observables, not values. If you forget to flatten (e.g., with `mergeAll`), you will receive Observable objects rather than their emitted values.
- **Late subscription to a window**: Each window is a Subject-based Observable. If you subscribe to a window Observable after it has already completed, you may miss values or get an `ObjectUnsubscribedError`. Subscribe promptly in operators like `mergeMap`.
- **Boundaries completing the source**: When `windowBoundaries` completes, it no longer creates new windows, but the source can still accumulate values in the last open window until the source itself completes.

## Related Operators

- `buffer` — like `window` but collects values into arrays rather than nested Observables
- `windowCount` — creates fixed-size windows by item count
- `windowTime` — creates time-bounded windows
- `windowToggle` — opens and closes windows with separate Observables
- `windowWhen` — uses a factory function for dynamic window boundaries
