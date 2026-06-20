# windowCount

**Category**: Transformation  
**Import**: `import { windowCount } from 'rxjs';`

## Description

Branches the source Observable values as a nested Observable with each nested Observable emitting at most `windowSize` values. Like `bufferCount`, but emits nested Observables (windows) instead of arrays.

When `startWindowEvery` is provided, a new window opens every `startWindowEvery` source values, enabling overlapping or strided windows. When the source completes or errors, the current windows are completed or errored with the same notification.

## Signature

```typescript
function windowCount<T>(windowSize: number, startWindowEvery?: number): OperatorFunction<T, Observable<T>>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `windowSize` | `number` | The maximum number of values each window emits before completing. |
| `startWindowEvery` | `number` | Optional. Defaults to `windowSize`. The interval at which to start a new window. A value smaller than `windowSize` creates overlapping windows. |

## Return Type

`OperatorFunction<T, Observable<T>>` — emits a new Observable window for each group of `windowSize` (or fewer, for the last window) values.

## Marble Diagram

```
Source (windowCount(3)):
  --1--2--3--4--5--6--|
  --w1-----------w2---|
    w1: --1--2--3|
    w2:         --4--5--6|

Source (windowCount(3, 1) — sliding):
  --1--2--3--4--|
  --w1-w2-w3-w4-|
    w1: --1--2--3|
    w2:  --2--3--4|
    ...
```

## Examples

### Example 1: Skip the first click in every group of three

```typescript
import { fromEvent, windowCount, map, skip, mergeAll } from 'rxjs';

const clicks = fromEvent(document, 'click');

clicks.pipe(
  windowCount(3),
  map(win => win.pipe(skip(1))), // Drop first click of each triplet
  mergeAll()
).subscribe(() => console.log('Accepted click'));
```

### Example 2: Compute rolling average of the last N values

```typescript
import { from, windowCount, mergeMap, toArray, map, filter } from 'rxjs';

const values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
const WINDOW = 3;

from(values).pipe(
  windowCount(WINDOW, 1),
  mergeMap(win => win.pipe(toArray())),
  filter(arr => arr.length === WINDOW),
  map(arr => arr.reduce((a, b) => a + b, 0) / WINDOW)
).subscribe(avg => console.log('Rolling average:', avg.toFixed(2)));
```

### Example 3: Paginate streamed results into groups

```typescript
import { Subject, windowCount, mergeMap, toArray } from 'rxjs';

const result$ = new Subject<{ id: number; name: string }>();

// Group results into pages of 10
result$.pipe(
  windowCount(10),
  mergeMap(win => win.pipe(toArray()))
).subscribe((page, i) => {
  console.log(`Page ${i + 1}:`, page);
});
```

## Common Pitfalls

- **Higher-order Observable**: You must flatten window Observables downstream. Use `mergeAll`, `concatAll`, or `mergeMap(win => win.pipe(...))`.
- **Late subscription to a window**: Window Observables start emitting immediately. If you subscribe after the window has already emitted some values, you miss those values. Always subscribe in the same synchronous turn.
- **Partial last window**: If the source completes before a window reaches `windowSize`, the last window emits its partial contents and then completes.

## Related Operators

- `bufferCount` — like `windowCount` but collects into arrays instead of Observables
- `window` — boundaries determined by a separate Observable
- `windowTime` — boundaries determined by elapsed time
- `windowToggle` — separate open/close Observables
- `windowWhen` — dynamic closing factory function
