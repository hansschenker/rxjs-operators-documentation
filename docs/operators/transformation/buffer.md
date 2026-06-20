# buffer

**Category**: Transformation  
**Import**: `import { buffer } from 'rxjs';`

## Description

Buffers the source Observable values until `closingNotifier` emits, then emits the collected values as an array and starts a new buffer. Each time the `closingNotifier` emits, the current buffer is flushed to the output as an array, and a fresh buffer begins collecting.

When the source completes, the remaining buffered values are emitted as a final array (even if `closingNotifier` has not fired).

## Signature

```typescript
function buffer<T>(closingNotifier: ObservableInput<any>): OperatorFunction<T, T[]>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `closingNotifier` | `ObservableInput<any>` | An Observable (or Promise, array, etc.) that triggers the buffer to be emitted. The emitted value from the notifier is ignored; only the timing matters. |

## Return Type

`OperatorFunction<T, T[]>` — emits arrays of buffered source values each time the closing notifier fires.

## Marble Diagram

```
Source:   --1--2--3--4--5--6--|
Notifier: --------x--------x--|
           buffer(notifier)
Output:   --------[1,2,3]--[4,5,6]--|
```

## Examples

### Example 1: Batch interval events on every user click

```typescript
import { fromEvent, interval, buffer } from 'rxjs';

const clicks = fromEvent(document, 'click');
const ticks = interval(200);

// Collect ticks between clicks
ticks.pipe(
  buffer(clicks)
).subscribe(ticksBatch => {
  console.log(`${ticksBatch.length} ticks since last click:`, ticksBatch);
});
```

### Example 2: Batch log messages and send every 5 seconds

```typescript
import { Subject, buffer, interval, filter } from 'rxjs';

const logEntry$ = new Subject<string>();
const flush$ = interval(5000);

logEntry$.pipe(
  buffer(flush$),
  filter(batch => batch.length > 0)
).subscribe(batch => {
  console.log('Sending log batch:', batch);
  // send to logging service
});

logEntry$.next('User logged in');
logEntry$.next('Page viewed: /home');
logEntry$.next('Button clicked: submit');
```

### Example 3: Group WebSocket messages between heartbeat signals

```typescript
import { Subject, buffer, filter } from 'rxjs';

interface WsMessage {
  type: 'heartbeat' | 'data';
  payload?: unknown;
}

const ws$ = new Subject<WsMessage>();
const heartbeat$ = ws$.pipe(filter(m => m.type === 'heartbeat'));
const data$ = ws$.pipe(filter(m => m.type === 'data'));

data$.pipe(
  buffer(heartbeat$)
).subscribe(batch => {
  console.log('Messages since last heartbeat:', batch);
});
```

## Common Pitfalls

- **Empty buffer emitted**: If the `closingNotifier` fires multiple times before any source values arrive, empty arrays (`[]`) are emitted for each firing. Use `filter(arr => arr.length > 0)` downstream to discard empty buffers.
- **Memory growth**: If the `closingNotifier` fires infrequently and the source emits at a high rate, the buffer can grow large. Consider `bufferCount` or `bufferTime` to limit buffer size.
- **Source completes before notifier**: When the source completes, the current buffer is flushed immediately, regardless of whether the `closingNotifier` has fired.
- **Notifier completes**: When the `closingNotifier` completes, no more buffer flushes occur via the notifier, but the source can still accumulate values until it also completes.

## Related Operators

- `bufferCount` — buffers a fixed number of values
- `bufferTime` — buffers values over a fixed time span
- `bufferToggle` — opens and closes buffers with separate Observables
- `bufferWhen` — uses a factory function to produce dynamic closing Observables
- `window` — like `buffer` but emits nested Observables instead of arrays
