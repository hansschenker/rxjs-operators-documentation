# retry

## Identity

- **Name**: retry
- **Category**: Error Handling Operators
- **Type**: Automatic resubscription on error — transparently re-executes the source Observable on failure
- **Import**:
  ```typescript
  import { retry } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  // Simple form
  function retry(count?: number): MonoTypeOperatorFunction<T>

  // Config form (RxJS 7+)
  function retry<T>(config: RetryConfig): MonoTypeOperatorFunction<T>

  interface RetryConfig {
    count?: number;           // max retries (default: Infinity)
    delay?: number | ((error: any, retryCount: number) => ObservableInput<any>);
    resetOnSuccess?: boolean; // reset retry count after successful emission
  }
  ```

## Functional Specification

**Concept**: When the source Observable errors, `retry` unsubscribes from the source and immediately resubscribes (re-executes) it from the beginning. This continues until either:
1. The source completes successfully
2. The retry count is exhausted (error is then forwarded to the subscriber)

**Critical invariant — cold vs hot sources**:
- **Cold sources** (HTTP, `defer`, `ajax`): each retry creates a fresh execution — new HTTP request, new connection, etc.
- **Hot sources** (Subject, BroadcastChannel): retrying resubscribes to a shared source; may miss values emitted during the retry gap

**`count` semantics**:
```
retry()        → retry forever (Infinity)
retry(0)       → never retry (same as no retry)
retry(3)       → retry up to 3 times (4 total attempts: 1 original + 3 retries)
```

**`delay` in RetryConfig**:
- `delay: 1000` — fixed 1000ms wait between retries
- `delay: (err, count) => timer(2 ** count * 1000)` — exponential backoff
- The delay function receives the error and the current retry count (1-based)

## Marble Diagram

```
Source (cold, errors on first attempt):
Attempt 1:  --a--b--#
Attempt 2:  ------a--b--c--|  (success)

retry(3):
Result:     --a--b----a--b--c--|
            ^retry 1^  ^success^

retry(1) — exhausted:
Attempt 1:  --a--b--#
Attempt 2:  ------a--b--#
Result:     --a--b----a--b--#  (error forwarded after 1 retry)

retry({ count: 2, delay: 1000 }):
Attempt 1:  --a--#
            (1000ms wait)
Attempt 2:  --------a--#
            (1000ms wait)
Attempt 3:  --------a--c--|  (success)
```

## Behavioral Characteristics

**Resubscription**: `retry` calls `subscribe()` on the source Observable again — for cold Observables (ajax, defer, HTTP), this creates a completely fresh execution.

**Error transparency**: Errors during successful retries restart the count. After exhausting retries, the last error is forwarded.

**`resetOnSuccess`**: When `true`, the retry counter resets after any successful `next()` emission. Useful for long-running streams where transient errors should not accumulate toward the limit.

**Completion**: Source completion passes through without retry logic. `retry` only reacts to errors.

## Type System Integration

```typescript
import { ajax } from 'rxjs/ajax';
import { retry } from 'rxjs/operators';
import { timer } from 'rxjs';

// Simple retry — type preserved
const user$: Observable<User> = ajax.getJSON<User>('/api/user').pipe(
  retry(3) // up to 3 retries
);

// Config form with delay function
const robust$: Observable<Data> = ajax.getJSON<Data>('/api/data').pipe(
  retry({
    count: 5,
    delay: (error, retryCount) => {
      console.log(`Retry #${retryCount} after error:`, error.message);
      return timer(retryCount * 1000); // 1s, 2s, 3s, 4s, 5s
    }
  })
);
```

## Examples

### Basic — Retry HTTP Request
```typescript
import { ajax } from 'rxjs/ajax';
import { retry, catchError } from 'rxjs/operators';
import { EMPTY } from 'rxjs';

// Retry up to 3 times on failure, then give up
ajax.getJSON<User[]>('/api/users').pipe(
  retry(3),
  catchError(err => {
    console.error('Failed after 3 retries:', err);
    return EMPTY;
  })
).subscribe(users => renderTable(users));
```

### Common Pattern — Exponential Backoff
```typescript
import { ajax } from 'rxjs/ajax';
import { retry } from 'rxjs/operators';
import { timer } from 'rxjs';

ajax.getJSON('/api/data').pipe(
  retry({
    count: 4,
    delay: (_, retryCount) => timer(Math.pow(2, retryCount - 1) * 1000)
    // Retry 1: wait 1s, Retry 2: wait 2s, Retry 3: wait 4s, Retry 4: wait 8s
  })
).subscribe({
  next: data => console.log('got data:', data),
  error: err => console.error('exhausted retries:', err)
});
```

### Common Pattern — Retry With Condition (Selective Retry)
```typescript
import { ajax } from 'rxjs/ajax';
import { retry } from 'rxjs/operators';
import { throwError, timer } from 'rxjs';

// Only retry on network errors (5xx), not client errors (4xx)
ajax.getJSON('/api/data').pipe(
  retry({
    count: 3,
    delay: (error, retryCount) => {
      // AjaxError has status property
      if (error.status && error.status >= 400 && error.status < 500) {
        // 4xx errors — don't retry, re-throw immediately
        return throwError(() => error);
      }
      // Network / 5xx errors — retry with backoff
      return timer(retryCount * 1000);
    }
  })
).subscribe(handleResult);
```

### Common Pattern — `resetOnSuccess` for Long-Running Streams
```typescript
import { webSocket } from 'rxjs/webSocket';
import { retry } from 'rxjs/operators';

// WebSocket stream: retry on disconnect, but reset counter after each
// successful message so transient disconnects don't exhaust the limit
webSocket('wss://api.example.com/stream').pipe(
  retry({
    count: 5,
    delay: (_, retryCount) => timer(retryCount * 500),
    resetOnSuccess: true  // ← successful emission resets the counter
  })
).subscribe({
  next: msg => handleMessage(msg),
  error: err => console.error('permanently disconnected:', err)
});
```

## Common Pitfalls

### Anti-pattern: `retry()` Without a Count on Persistent Errors
```typescript
import { ajax } from 'rxjs/ajax';
import { retry } from 'rxjs/operators';

// ❌ INFINITE LOOP — server is down; retries forever, hammering the server
ajax.getJSON('/api/data').pipe(
  retry() // count defaults to Infinity!
).subscribe(console.log);

// ✅ CORRECT — always specify a count (and ideally a delay)
ajax.getJSON('/api/data').pipe(
  retry({ count: 3, delay: 1000 })
).subscribe(console.log);

// WHY: retry() with no argument retries forever. If the error is persistent
// (server down, bad URL, auth failure), this creates an infinite tight loop.
// Always set a count. Add a delay to avoid hammering the server.
```

### Anti-pattern: `retry` on Hot Sources
```typescript
import { Subject } from 'rxjs';
import { retry } from 'rxjs/operators';

// ❌ WRONG — Subject is hot; resubscription on error reconnects to the same
// shared multicaster but misses all values emitted during the retry window
const events$ = new Subject<Event>();

events$.pipe(retry(3)).subscribe(handleEvent);
events$.error(new Error('bad event')); // triggers retry
// During the "retry" (resubscription), any events$ emissions are lost

// ✅ CORRECT — wrap the hot source in defer to recreate it per subscription,
// OR use catchError to recover without resubscription,
// OR handle errors in the Subject pipeline before they reach subscribers

// WHY: retry works by resubscribing to the source. For cold Observables,
// this creates a fresh execution (new HTTP request, etc.). For hot Observables,
// resubscription reconnects to the same stream but cannot recover missed values.
```

### Anti-pattern: `retry` After `catchError` (Wrong Order)
```typescript
import { ajax } from 'rxjs/ajax';
import { retry, catchError } from 'rxjs/operators';
import { EMPTY } from 'rxjs';

// ❌ WRONG ORDER — catchError swallows the error before retry sees it
ajax.getJSON('/api/data').pipe(
  catchError(_ => EMPTY),  // error is consumed here — retry never fires
  retry(3)
).subscribe(console.log);  // retries 0 times

// ✅ CORRECT — retry before catchError
ajax.getJSON('/api/data').pipe(
  retry(3),           // retry first: 3 attempts before giving up
  catchError(_ => EMPTY)  // then handle the final error
).subscribe(console.log);

// WHY: Operators in a pipe are applied in order. catchError sits downstream
// and receives errors after they propagate through operators above it.
// If catchError is above retry, it consumes the error before retry can act on it.
// Put retry upstream (before) catchError.
```

## Related Operators

- **`catchError`**: Handle errors without retrying — return a fallback Observable
- **`retryWhen`** (deprecated in RxJS 7): Use `retry({ delay: fn })` in RxJS 7+ instead
- **`defer`**: Wrap source in `defer` to ensure each retry creates a truly fresh execution with fresh state
- **`timeout`**: Complement — errors if no emission within a window; often combined with retry
- **`repeat`**: Like retry but for completion, not errors — resubscribes on complete

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/retry](https://rxjs.dev/api/operators/retry)

---

**Cognitive Load**: 3/5 | **Usage Frequency**: 5/5 | **Composability**: 4/5
**Key teaching points**:
1. `retry()` with no count = infinite loop — always set count
2. `retry` comes BEFORE `catchError` in a pipe
3. Only meaningful on cold (re-executable) sources
4. RxJS 7 `RetryConfig` replaces `retryWhen`
