# endWith

**Category**: Combination  
**Import**: `import { endWith } from 'rxjs';`

## Description

`endWith` returns an Observable that emits all values from the source Observable, and then, immediately after the source completes, synchronously emits all values provided as arguments (in the order given). The appended values are emitted synchronously as part of the completion sequence — they arrive before the `complete` notification reaches the subscriber.

`endWith` is useful for appending a known final value, a sentinel, or a status message to any stream. It pairs naturally with `startWith` and `takeUntil` to bracket a stream with meaningful bookend values.

## Signature

```typescript
function endWith<T, A extends readonly unknown[] = T[]>(
  ...values: A
): OperatorFunction<T, T | ValueFromArray<A>>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| values | `...A` | One or more values to emit synchronously after the source completes. |

## Return Type

`OperatorFunction<T, T | ValueFromArray<A>>` — An Observable that emits all source values followed by the provided values before completing.

## Marble Diagram

```
Source:     --1----2----3--|
            endWith(99)
Output:     --1----2----3--99--|
                              ^-- emitted synchronously on source completion
```

## Examples

### Example 1: Bookending a timer stream with start and end status messages

```typescript
import { interval, map, fromEvent, startWith, takeUntil, endWith } from 'rxjs';

const stopBtn = document.getElementById('stop-btn')!;
const stop$ = fromEvent(stopBtn, 'click');

interval(1000).pipe(
  map(i => `Tick ${i + 1}`),
  startWith('Timer started'),
  takeUntil(stop$),
  endWith('Timer stopped')
).subscribe(message => {
  appendToLog(message);
});
// Timer started
// Tick 1
// Tick 2
// Tick 3
// Timer stopped  <-- user clicked stop
```

### Example 2: Appending a completion record to an audit log stream

```typescript
import { from, endWith, map } from 'rxjs';
import { ajax } from 'rxjs/ajax';

const userIds = [1, 2, 3, 4, 5];

from(userIds).pipe(
  map(id => ajax.getJSON<User>(`/api/users/${id}`)),
  // (would concatAll or mergeAll here to resolve the inner observables)
  endWith({ type: 'BATCH_COMPLETE', processedCount: userIds.length, timestamp: Date.now() })
).subscribe(event => {
  if ('type' in event && event.type === 'BATCH_COMPLETE') {
    console.log(`Batch complete: ${event.processedCount} users processed.`);
  } else {
    processUser(event as User);
  }
});
```

### Example 3: Emitting a "disconnected" sentinel when a WebSocket stream ends

```typescript
import { map, endWith, catchError, EMPTY } from 'rxjs';
import { webSocket } from 'rxjs/webSocket';

type ChatMessage = { user: string; text: string };
type ConnectionEvent = { type: 'connected' | 'disconnected' };

webSocket<ChatMessage>('wss://chat.example.com/room/42').pipe(
  map(msg => ({ ...msg, type: 'message' as const })),
  endWith({ type: 'disconnected' as const }),
  catchError(() => {
    // On error, still emit disconnected before completing
    return EMPTY;
  })
).subscribe(event => {
  if (event.type === 'disconnected') {
    showDisconnectedBanner();
  } else {
    appendChatMessage(event);
  }
});
```

## Common Pitfalls

- **Appended values are only emitted if the source completes normally**: If the source errors, `endWith` values are never emitted. The error bypasses the appended values. Use `finalize` for cleanup logic that must run regardless of how the source terminates.
- **Synchronous emission**: Like `startWith`, the appended values are emitted synchronously during completion. They arrive in the same call stack frame as the completion notification from the source.
- **Type widening**: `endWith` widens the output type to `T | ValueFromArray<A>`. Downstream subscribers must handle the union. Use discriminated unions or conditional checks to differentiate the sentinel values from regular source emissions.
- **Not emitted if source never completes**: If the source is an infinite stream (e.g. an unended `interval`), `endWith` values will never be emitted unless the stream is terminated by an operator like `takeUntil`, `take`, or `first`.

## Related Operators

- `startWith` — prepends values synchronously at the beginning of a stream
- `concatWith` — a more general way to append entire Observables (not just static values)
- `finalize` — runs a callback (not an emission) when the source terminates for any reason
- `takeUntil` — commonly paired with `endWith` to terminate a stream and then append a final value
