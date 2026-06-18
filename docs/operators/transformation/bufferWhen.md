# bufferWhen

**Category**: Transformation  
**Import**: `import { bufferWhen } from 'rxjs';`

## Description

Buffers the source Observable values using a factory function of closing Observables to determine when to close, emit, and reset the buffer. A buffer opens immediately and is closed when the Observable returned by calling `closingSelector` emits. A new buffer then opens immediately, repeating the process.

Unlike `bufferToggle`, there is always exactly one active buffer. Unlike `buffer`, the closing Observable is created fresh each time a new buffer opens, allowing for dynamic (e.g., randomized) buffer durations.

## Signature

```typescript
function bufferWhen<T>(closingSelector: () => ObservableInput<any>): OperatorFunction<T, T[]>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `closingSelector` | `() => ObservableInput<any>` | A zero-argument function called each time a new buffer opens. Returns an Observable; the first emission from that Observable closes the current buffer and triggers a new one. |

## Return Type

`OperatorFunction<T, T[]>` — emits arrays of buffered values whenever the current closing Observable emits.

## Marble Diagram

```
Source:    --1--2--3--4--5--6--7--|
Closing 1: --------x
Closing 2:                 x
Output:    --------[1,2,3]--[4,5]--[6,7]--|
```

## Examples

### Example 1: Buffer with random-duration windows

```typescript
import { fromEvent, bufferWhen, interval } from 'rxjs';

const clicks = fromEvent(document, 'click');

clicks.pipe(
  bufferWhen(() => interval(1000 + Math.random() * 4000))
).subscribe(batch => {
  console.log(`Collected ${batch.length} clicks in this window`);
});
```

### Example 2: Buffer user input until they pause typing

```typescript
import { fromEvent, bufferWhen, debounceTime, map } from 'rxjs';

const input = document.querySelector<HTMLInputElement>('#cmd')!;
const keyup$ = fromEvent<KeyboardEvent>(input, 'keyup');

keyup$.pipe(
  map(e => e.key),
  bufferWhen(() => keyup$.pipe(debounceTime(1000)))
).subscribe(keys => {
  console.log('Command fragment entered:', keys.join(''));
});
```

### Example 3: Adaptive batching based on system load

```typescript
import { Subject, bufferWhen, from, switchMap, timer } from 'rxjs';

const event$ = new Subject<string>();

// Dynamically determine batch window based on queue length
let pendingCount = 0;

event$.pipe(
  bufferWhen(() => {
    // Shorter window when events pile up, longer when idle
    const delay = pendingCount > 10 ? 500 : 2000;
    return timer(delay);
  })
).subscribe(batch => {
  pendingCount = 0;
  console.log('Processing batch of', batch.length);
});

event$.subscribe(() => pendingCount++);
```

## Common Pitfalls

- **`closingSelector` called immediately on subscription**: The first closing Observable is created as soon as you subscribe, not when the first source value arrives. If your factory has side effects, they fire right away.
- **Closing by first emission only**: Only the first emission from each closing Observable matters. Subsequent emissions from the same closing Observable are ignored (the internal subscriber is unsubscribed after the first notification).
- **Error in `closingSelector`**: If `closingSelector` throws, the error is forwarded to the subscriber and the stream terminates.

## Related Operators

- `buffer` — closes buffer on every emission of a single static closing Observable
- `bufferCount` — closes buffer based on item count
- `bufferTime` — closes buffer based on elapsed time
- `bufferToggle` — opens and closes with separate independent Observables
- `windowWhen` — like `bufferWhen` but emits nested Observables instead of arrays
