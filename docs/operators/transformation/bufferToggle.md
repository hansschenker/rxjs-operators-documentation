# bufferToggle

**Category**: Transformation  
**Import**: `import { bufferToggle } from 'rxjs';`

## Description

Buffers the source Observable values starting from an emission from `openings` and ending when the Observable returned by `closingSelector` emits. Multiple buffers can be open simultaneously if `openings` emits before a previous buffer closes.

This is the most flexible buffer operator: you independently control when each buffer starts (via `openings`) and when it closes (via the `closingSelector`). Each opening event creates a new buffer, and the corresponding closing Observable determines when that specific buffer is emitted and cleared.

## Signature

```typescript
function bufferToggle<T, O>(
  openings: ObservableInput<O>,
  closingSelector: (value: O) => ObservableInput<any>
): OperatorFunction<T, T[]>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `openings` | `ObservableInput<O>` | An Observable of notifications that open a new buffer. The emitted value is passed to `closingSelector`. |
| `closingSelector` | `(value: O) => ObservableInput<any>` | A function called with each value from `openings`. Returns an Observable; when that Observable emits, the corresponding buffer is emitted and closed. |

## Return Type

`OperatorFunction<T, T[]>` — emits arrays of buffered values, one per buffer opened by `openings`.

## Marble Diagram

```
Source:   --1--2--3--4--5--6--7--|
Openings: --o1-----------o2------|
Close o1:       ---x
Close o2:                    --x
Output:   ---------[1,2,3]-------[5,6,7]--|
```

## Examples

### Example 1: Capture events during specific activity windows

```typescript
import { fromEvent, interval, bufferToggle, EMPTY } from 'rxjs';

const clicks = fromEvent<MouseEvent>(document, 'click');
// Open a buffer every 2 seconds; close it after 500ms for odd-indexed openings
const openings = interval(2000);

clicks.pipe(
  bufferToggle(
    openings,
    i => i % 2 === 0 ? interval(500) : EMPTY
  )
).subscribe(batch => {
  console.log('Clicks captured in window:', batch.length);
});
```

### Example 2: Record keystrokes during form-field focus

```typescript
import { fromEvent, bufferToggle, map } from 'rxjs';

const input = document.querySelector<HTMLInputElement>('#search')!;
const keydown$ = fromEvent<KeyboardEvent>(document, 'keydown');
const focus$ = fromEvent(input, 'focus');
const blur$ = fromEvent(input, 'blur');

keydown$.pipe(
  map(e => e.key),
  bufferToggle(focus$, () => blur$)
).subscribe(keys => {
  console.log('Keys typed while focused:', keys);
});
```

### Example 3: Capture telemetry during a loading operation

```typescript
import { Subject, bufferToggle } from 'rxjs';

const metric$ = new Subject<{ name: string; value: number }>();
const loadStart$ = new Subject<string>(); // emits operation name
const loadEnd$ = new Subject<string>();   // emits operation name

metric$.pipe(
  bufferToggle(loadStart$, opName => loadEnd$)
).subscribe(metrics => {
  console.log(`Metrics during load (${metrics.length}):`, metrics);
});

loadStart$.next('fetchUser');
metric$.next({ name: 'cpu', value: 45 });
metric$.next({ name: 'memory', value: 200 });
loadEnd$.next('fetchUser');
```

## Common Pitfalls

- **Overlapping buffers**: If `openings` emits before the previous buffer's `closingSelector` fires, multiple buffers are open simultaneously, each independently collecting values. This is by design but can be memory-intensive if closings are slow.
- **`EMPTY` to skip a window**: Returning `EMPTY` from `closingSelector` immediately closes the buffer (emitting an empty array). This effectively skips certain openings.
- **No automatic flush on source completion**: Unlike `buffer`, `bufferToggle` does not automatically emit open buffers when the source completes.

## Related Operators

- `buffer` — single buffer, closes on every notifier emission
- `bufferCount` — closes buffer based on item count
- `bufferTime` — closes buffer based on elapsed time
- `bufferWhen` — single buffer with a dynamic closing Observable factory
- `windowToggle` — like `bufferToggle` but emits nested Observables instead of arrays
