# repeat

**Category**: Utility  
**Import**: `import { repeat } from 'rxjs';`

## Description

`repeat` resubscribes to the source Observable each time it completes, effectively repeating the entire sequence. By default it repeats indefinitely. You can pass a number to limit the total number of subscriptions, or a `RepeatConfig` object to control both the count and an optional delay between repetitions.

`repeat` does not catch errors — it only reacts to completion. Use `retry` for error-based resubscription.

**Key forms:**
- `repeat()` — repeat forever
- `repeat(3)` — repeat 3 times (3 subscriptions total)
- `repeat(0)` — return an empty observable
- `repeat({ count: 3, delay: 1000 })` — repeat 3 times, 1 second between each
- `repeat({ delay: count => timer(count * 1000) })` — exponential back-off using a factory

## Signature

```typescript
function repeat<T>(countOrConfig?: number | RepeatConfig): MonoTypeOperatorFunction<T>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| countOrConfig | `number \| RepeatConfig` | Optional. Number of repetitions, or a config object. |

### `RepeatConfig` properties

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `count` | `number` | `Infinity` | Total number of subscriptions to the source. |
| `delay` | `number \| ((count: number) => ObservableInput<any>)` | — | Delay between repetitions in ms, or a factory returning a notifier observable. |

## Return Type

`MonoTypeOperatorFunction<T>` — an operator that returns an Observable that re-subscribes to the source on completion.

## Marble Diagram

```
Source:  --a--b--|
         repeat(3)
Output:  --a--b----a--b----a--b--|
                  (resubscribes on each completion, 3 total)
```

## Examples

### Example 1: Poll an API endpoint every 5 seconds

```typescript
import { defer, from, repeat, tap } from 'rxjs';

const pollStatus$ = defer(() =>
  from(fetch('/api/status').then(r => r.json()))
).pipe(
  tap(status => console.log('Status:', status)),
  repeat({ delay: 5000 }) // wait 5s after each completion, then repeat
);

const subscription = pollStatus$.subscribe();

// Stops polling when unsubscribed
setTimeout(() => subscription.unsubscribe(), 30_000);
```

### Example 2: Repeat a message a fixed number of times

```typescript
import { of, repeat } from 'rxjs';

of('ping').pipe(
  repeat(3)
).subscribe(console.log);

// ping
// ping
// ping
```

### Example 3: Exponential back-off with increasing delay

```typescript
import { defer, from, repeat, timer } from 'rxjs';

const apiCall$ = defer(() => from(fetch('/api/data').then(r => r.json())));

apiCall$.pipe(
  repeat({
    delay: (count) => {
      const delayMs = Math.min(30_000, Math.pow(2, count) * 1000);
      console.log(`Retrying in ${delayMs}ms (attempt ${count + 1})`);
      return timer(delayMs);
    }
  })
).subscribe(data => console.log('Data:', data));

// Retrying in 2000ms (attempt 1)
// Retrying in 4000ms (attempt 2)
// Retrying in 8000ms (attempt 3)
// ...
```

## Common Pitfalls

- **Does not handle errors**: `repeat` only resubscribes on `complete`. If the source errors, the error propagates downstream. Combine with `retry` or `catchError` for resilient polling.
- **`repeat(0)` returns `EMPTY`**: Passing zero as the count returns an immediately-completing empty observable.
- **Infinite loops with synchronous sources**: `repeat()` on a synchronous source (like `of(...)`) will loop synchronously and block the thread. Always add a `delay` for synchronous sources.
- **`repeatWhen` is deprecated**: The older `repeatWhen` API is deprecated. Use `repeat({ delay: () => notifier$ })` instead.

## Related Operators

- `repeatWhen` — deprecated; replaced by `repeat({ delay: () => notifier$ })`
- `retry` — similar resubscription behavior, but triggered by errors rather than completion
- `defer` — creates a fresh observable for each subscription, needed for things like re-running `fetch` on each repeat
