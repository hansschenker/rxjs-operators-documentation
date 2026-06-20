# mergeAll

**Category**: Combination  
**Import**: `import { mergeAll } from 'rxjs';`

## Description

`mergeAll` converts a higher-order Observable (an Observable that emits other Observables) into a first-order Observable by subscribing to each inner Observable as it arrives and merging all of their emissions into a single output stream. Subscriptions to inner Observables happen concurrently, so values from multiple inner Observables can be interleaved in the output.

The output Observable completes only once the source has completed and all active inner Observables have also completed. Any error from an inner Observable is immediately forwarded to the output. An optional `concurrent` parameter limits how many inner Observables are subscribed to simultaneously — excess inner Observables are queued and processed as active ones complete.

## Signature

```typescript
function mergeAll<O extends ObservableInput<any>>(concurrent?: number): OperatorFunction<O, ObservedValueOf<O>>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| concurrent | `number` | Optional. Maximum number of inner Observables to subscribe to concurrently. Defaults to `Infinity`. |

## Return Type

`OperatorFunction<O, ObservedValueOf<O>>` — An Observable that emits all values from all inner Observables emitted by the source, merged concurrently.

## Marble Diagram

```
Source:   --A-----B-----C----|
A:          --1--2--|
B:                --3--4--|
C:                      --5--|
          mergeAll()
Output:   ----1--2--3--4--5--|
```

## Examples

### Example 1: Polling multiple API endpoints concurrently

```typescript
import { of, interval, mergeAll, map, take } from 'rxjs';
import { ajax } from 'rxjs/ajax';

const apiEndpoints = [
  'https://api.example.com/users',
  'https://api.example.com/posts',
  'https://api.example.com/comments',
];

// Create a stream of HTTP requests and flatten them all concurrently
of(...apiEndpoints).pipe(
  map(url => ajax.getJSON(url)),
  mergeAll()
).subscribe({
  next: data => console.log('Received data:', data),
  error: err => console.error('Request failed:', err),
  complete: () => console.log('All requests complete'),
});
```

### Example 2: Processing user-triggered streams with concurrency limit

```typescript
import { fromEvent, map, interval, take, mergeAll } from 'rxjs';

// Each button click starts a 5-second countdown
// Limit to 2 concurrent countdowns at a time
const button = document.getElementById('start-btn')!;

fromEvent(button, 'click').pipe(
  map((_, index) => interval(1000).pipe(
    take(5),
    map(tick => `Timer ${index + 1}: ${tick + 1}s`)
  )),
  mergeAll(2) // At most 2 timers running simultaneously
).subscribe(message => console.log(message));
```

### Example 3: Flattening a stream of WebSocket messages

```typescript
import { Subject, mergeAll, map } from 'rxjs';
import { webSocket } from 'rxjs/webSocket';

const channels$ = new Subject<string>();

// Each channel name arrives on channels$, open a WebSocket for each
channels$.pipe(
  map(channel => webSocket(`wss://chat.example.com/${channel}`)),
  mergeAll() // Subscribe to all channels concurrently
).subscribe(message => console.log('Received:', message));

// Later, emit channel names as the user joins rooms
channels$.next('general');
channels$.next('random');
channels$.next('help');
```

## Common Pitfalls

- **Unbounded concurrency with infinite inner Observables**: If the source emits many Observables and each one never completes (e.g., `interval` streams), subscriptions accumulate indefinitely. Use the `concurrent` parameter to cap active subscriptions and prevent memory leaks.
- **Error from any inner Observable terminates the whole stream**: A single failing inner Observable propagates its error to the output, cancelling all other active inner subscriptions. Use `catchError` inside the inner Observable mapping to handle errors per-stream.
- **Completion order**: The output only completes when the source completes AND all active inner Observables complete. If any inner Observable is long-lived, the output will remain open longer than expected.

## Related Operators

- `concatAll` — like `mergeAll` but subscribes to inner Observables one at a time in order (equivalent to `mergeAll(1)`)
- `switchAll` — like `mergeAll` but cancels the previous inner Observable when a new one arrives
- `exhaustAll` — like `mergeAll` but ignores new inner Observables while one is still active
- `mergeMap` — combines the mapping and flattening steps in one operator
- `merge` — creation operator equivalent; merges static Observable inputs rather than a higher-order source
