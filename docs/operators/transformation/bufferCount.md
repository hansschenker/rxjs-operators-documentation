# bufferCount

**Category**: Transformation  
**Import**: `import { bufferCount } from 'rxjs';`

## Description

Buffers the source Observable values until the buffer reaches the maximum `bufferSize`, then emits the buffer as an array and starts a new one. Optionally, a `startBufferEvery` value can be provided to create overlapping or strided buffers.

When `startBufferEvery` is provided, a new buffer starts every `startBufferEvery` values rather than immediately after the previous one closes. This allows for sliding windows (overlapping) or strided windows (non-contiguous). When the source completes, any partial buffers are emitted.

## Signature

```typescript
function bufferCount<T>(bufferSize: number, startBufferEvery?: number | null): OperatorFunction<T, T[]>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `bufferSize` | `number` | The maximum number of items to collect in each buffer before emitting. |
| `startBufferEvery` | `number \| null` | Optional. Defaults to `bufferSize`. The interval at which to start a new buffer. If less than `bufferSize`, buffers overlap. If greater than `bufferSize`, values between buffers are skipped. |

## Return Type

`OperatorFunction<T, T[]>` — emits arrays of at most `bufferSize` values from the source.

## Marble Diagram

```
Source (bufferCount(3)):
  --1--2--3--4--5--6--|
  ---------[1,2,3]---[4,5,6]--|

Source (bufferCount(3, 1) — sliding window):
  --1--2--3--4--|
  -----[1,2,3]--[2,3,4]--|
```

## Examples

### Example 1: Process sensor readings in fixed batches

```typescript
import { Subject, bufferCount } from 'rxjs';

const sensorReading$ = new Subject<number>();

sensorReading$.pipe(
  bufferCount(10)
).subscribe(batch => {
  const avg = batch.reduce((sum, v) => sum + v, 0) / batch.length;
  console.log(`Batch average: ${avg.toFixed(2)}`);
});

// Simulate sensor data
for (let i = 0; i < 30; i++) {
  sensorReading$.next(Math.random() * 100);
}
```

### Example 2: Sliding window for trend detection

```typescript
import { from, bufferCount, filter, map } from 'rxjs';

const prices = [10, 12, 11, 13, 15, 14, 16, 18, 17, 20];

// Compute a 3-value moving average
from(prices).pipe(
  bufferCount(3, 1),
  filter(window => window.length === 3), // skip incomplete windows at start
  map(window => window.reduce((a, b) => a + b, 0) / 3)
).subscribe(avg => console.log('Moving average:', avg.toFixed(2)));
```

### Example 3: Emit pairs of consecutive user actions

```typescript
import { fromEvent, bufferCount, map } from 'rxjs';

// Detect double-clicks by looking at consecutive click timestamps
fromEvent<MouseEvent>(document, 'click').pipe(
  bufferCount(2, 1), // sliding window of size 2
  map(([a, b]) => b.timeStamp - a.timeStamp),
  // filter for clicks less than 300ms apart
).subscribe(gap => {
  if (gap < 300) console.log('Double-click detected!');
});
```

## Common Pitfalls

- **Partial buffers on completion**: When the source completes, any in-progress buffers with fewer than `bufferSize` items are emitted as partial arrays. Handle partial arrays downstream if your logic requires a fixed size.
- **Overlapping buffers with `startBufferEvery < bufferSize`**: Multiple buffers are active simultaneously, each holding a reference to values. This can increase memory usage significantly for large `bufferSize` values.
- **`startBufferEvery` greater than `bufferSize`**: Values between buffer windows are permanently dropped. This is intentional for strided sampling but can be surprising.

## Related Operators

- `buffer` — buffers until a closing notifier Observable emits
- `bufferTime` — buffers values over a fixed time span
- `bufferToggle` — opens and closes buffers with separate Observables
- `bufferWhen` — uses a factory function for dynamic buffer boundaries
- `pairwise` — shorthand for `bufferCount(2, 1)` that emits `[prev, curr]` pairs
- `windowCount` — like `bufferCount` but emits nested Observables instead of arrays
