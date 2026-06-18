# retry

**Category**: Error Handling  
**Import**: `import { retry } from 'rxjs';`

## Description

`retry` resubscribes to the source observable when it errors, transparently re-emitting all values from each attempt. By default it retries indefinitely, but you can cap the number of attempts with a `count` and add a `delay` between attempts — either a fixed number of milliseconds or a custom notifier function that lets you implement exponential back-off or other strategies.

All values emitted during failed subscription attempts are forwarded downstream before the retry restarts. For example, if a source emits `[1, 2]` then errors and then succeeds with `[1, 2, 3]`, the subscriber sees `[1, 2, 1, 2, 3, complete]`. Once the maximum retry count is exhausted, the last error is propagated to the subscriber's error handler. The optional `resetOnSuccess` flag resets the retry counter each time the source emits a value, useful for long-lived streams that should keep recovering as long as they are making progress.

## Signature

```typescript
function retry<T>(count?: number): MonoTypeOperatorFunction<T>
function retry<T>(config: RetryConfig): MonoTypeOperatorFunction<T>
```

### `RetryConfig` interface

```typescript
interface RetryConfig {
  count?: number;
  delay?: number | ((error: any, retryCount: number) => ObservableInput<any>);
  resetOnSuccess?: boolean;
}
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `count` | `number` | (shorthand form) Maximum number of retry attempts. Defaults to `Infinity` if omitted. |
| `config.count` | `number` | Maximum number of retry attempts. Defaults to `Infinity` if omitted. |
| `config.delay` | `number \| ((error, retryCount) => ObservableInput)` | Milliseconds to wait before each retry, or a function returning a notifier observable. The notifier's first emission triggers the retry; if the notifier completes without emitting, the stream completes; if the notifier errors, that error propagates. |
| `config.resetOnSuccess` | `boolean` | When `true`, the retry counter resets to 0 each time the source emits a value successfully. Defaults to `false`. |

## Return Type

`MonoTypeOperatorFunction<T>` — an operator that returns an observable mirroring the source, resubscribing on error up to `count` times before propagating the error.

## Marble Diagram

```
Source:  --1--2--X
                  retry(2)
Retry 1: --1--2--X
Retry 2: --1--2--X
Output:  --1--2--1--2--1--2--X
(X = error; after 2 retries the final error propagates)

Source:  --1--2--X
                  retry({ count: 1, delay: 1000 })
Output:  --1--2---[1s]---1--2--X
```

## Examples

### Example 1: Retry an HTTP request up to 3 times

```typescript
import { of, throwError } from 'rxjs';
import { ajax } from 'rxjs/ajax';
import { retry, catchError } from 'rxjs';

function fetchUserProfile(userId: string) {
  return ajax.getJSON(`/api/users/${userId}`).pipe(
    retry(3), // retry up to 3 times on network errors
    catchError(err => {
      console.error('All retries exhausted:', err.message);
      return of(null); // return null profile as fallback
    })
  );
}

fetchUserProfile('abc123').subscribe(profile => {
  console.log('Profile:', profile);
});
```

### Example 2: Exponential back-off with `RetryConfig` delay function

```typescript
import { ajax } from 'rxjs/ajax';
import { retry, catchError } from 'rxjs';
import { timer } from 'rxjs';

function fetchWithBackoff(url: string) {
  return ajax.getJSON(url).pipe(
    retry({
      count: 4,
      delay: (error, retryCount) => {
        const backoffMs = Math.min(1000 * 2 ** (retryCount - 1), 30000);
        console.log(`Retry ${retryCount} in ${backoffMs}ms after: ${error.message}`);
        return timer(backoffMs); // 1s, 2s, 4s, 8s
      }
    }),
    catchError(err => {
      throw new Error(`Request failed after retries: ${err.message}`);
    })
  );
}

fetchWithBackoff('/api/orders').subscribe({
  next: data => console.log('Orders:', data),
  error: err => console.error(err.message)
});
```

### Example 3: Long-lived stream with `resetOnSuccess`

```typescript
import { webSocket } from 'rxjs/webSocket';
import { retry } from 'rxjs';

// WebSocket that reconnects on disconnect; resets retry counter after
// each successful message so transient errors don't accumulate.
const priceUpdates$ = webSocket('wss://prices.example.com/stream').pipe(
  retry({
    count: 5,
    delay: 2000,       // wait 2s before reconnecting
    resetOnSuccess: true // a successful message resets the counter
  })
);

priceUpdates$.subscribe({
  next: tick => console.log('Price update:', tick),
  error: err => console.error('WebSocket permanently failed:', err)
});
```

## Common Pitfalls

- **Retrying non-idempotent operations**: Retrying a POST or payment request can cause duplicate side effects. Only use `retry` on safe, idempotent operations (GET requests, read-only queries). For mutating calls, implement retry logic at a higher level with deduplication or idempotency keys.

- **Emitting duplicate values on retry**: All values emitted during failed attempts are forwarded downstream. If your subscriber accumulates results (e.g., pushing to an array), it will see duplicate values from retried subscriptions. Use `retry` only when the subscriber is stateless with respect to partial results, or reset the accumulator on each subscription.

- **Infinite retries on permanent errors**: `retry()` with no argument retries forever. Always set a `count` unless you have a deliberate reason for infinite retries, and combine with `catchError` so eventual failures are handled gracefully.

- **Combining `count` and `resetOnSuccess` incorrectly**: With `resetOnSuccess: true`, the counter resets on every successful `next`. A source that emits one value then errors will always reset to 0, making `count` effectively unlimited for that error pattern. Be intentional when enabling this flag.

## Related Operators

- `catchError` — handles errors with a selector function, giving access to the error value and allowing substitution with a fallback observable; more flexible than `retry` when recovery logic depends on the error
- `retryWhen` — deprecated; provides a notifier-based retry mechanism; replaced by `retry({ delay: ... })`
- `repeat` — resubscribes on *completion* rather than error, for scenarios where you want to loop a completed stream
