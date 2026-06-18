# windowWhen

**Category**: Transformation  
**Import**: `import { windowWhen } from 'rxjs';`

## Description

Branches the source Observable values as a nested Observable using a factory function of closing Observables to determine when to start a new window. Like `bufferWhen`, but emits nested Observables (windows) instead of arrays.

A window opens immediately on subscription. When the Observable returned by `closingSelector` emits, the current window is completed, a new window is opened, and `closingSelector` is called again to get the next closing Observable. There is always exactly one active window at any time.

## Signature

```typescript
function windowWhen<T>(closingSelector: () => ObservableInput<any>): OperatorFunction<T, Observable<T>>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `closingSelector` | `() => ObservableInput<any>` | A zero-argument function called each time a new window opens. Returns an Observable; the first emission from that Observable closes the current window and triggers a new one. |

## Return Type

`OperatorFunction<T, Observable<T>>` — emits a new window Observable each time the current window closes.

## Marble Diagram

```
Source:    --1--2--3--4--5--6--7--|
Closing 1: --------x
Closing 2:                 x
Output:    --w1-----w2-----w3-----|
  w1:      --1--2--3|
  w2:               --4--5|
  w3:                      --6--7|
```

## Examples

### Example 1: Take at most 2 clicks per random-duration window

```typescript
import { fromEvent, windowWhen, interval, map, take, mergeAll } from 'rxjs';

const clicks = fromEvent(document, 'click');

clicks.pipe(
  windowWhen(() => interval(1000 + Math.random() * 4000)),
  map(win => win.pipe(take(2))),
  mergeAll()
).subscribe(click => console.log('Click processed:', click));
```

### Example 2: Process log lines in adaptive batches

```typescript
import { Subject, windowWhen, mergeMap, toArray, bufferCount } from 'rxjs';

const logLine$ = new Subject<string>();

// Each window closes after 100 lines or 5 seconds, whichever comes first
logLine$.pipe(
  windowWhen(() => logLine$.pipe(bufferCount(100))),
  mergeMap(win => win.pipe(toArray()))
).subscribe(lines => {
  console.log(`Flushing ${lines.length} log lines`);
});
```

### Example 3: Group streaming data into configurable epochs

```typescript
import { Subject, windowWhen, timer, mergeMap, reduce, map } from 'rxjs';

interface DataPoint { value: number; ts: number }

const data$ = new Subject<DataPoint>();

// Close each epoch after a configurable duration
function getEpochDuration() { return 10_000; } // 10 seconds

data$.pipe(
  windowWhen(() => timer(getEpochDuration())),
  mergeMap(win =>
    win.pipe(
      reduce(
        (agg, point) => ({ sum: agg.sum + point.value, count: agg.count + 1 }),
        { sum: 0, count: 0 }
      ),
      map(({ sum, count }) => ({ avg: count ? sum / count : 0, count }))
    )
  )
).subscribe(epoch => console.log('Epoch summary:', epoch));
```

## Common Pitfalls

- **Higher-order Observable**: Flatten the output with `mergeMap`, `concatMap`, or `mergeAll` to access individual values.
- **`closingSelector` invoked immediately**: The factory is called as soon as you subscribe (to open the first window) and each time a window closes. Side effects in the factory happen right away.
- **Only first emission from closing Observable matters**: After the first emission closes the window, `windowWhen` unsubscribes from that closing Observable and calls `closingSelector` again for the next window.

## Related Operators

- `bufferWhen` — like `windowWhen` but collects into arrays instead of Observables
- `window` — single boundary Observable without a factory
- `windowCount` — boundary determined by item count
- `windowTime` — boundary determined by elapsed time
- `windowToggle` — separate open and close Observables, allowing overlapping windows
