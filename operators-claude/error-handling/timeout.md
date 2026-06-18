# timeout

## Identity

- **Name**: timeout
- **Category**: Error Handling Operators
- **Type**: Time-bound guard — errors if no value (or completion) arrives within a specified window
- **Import**:
  ```typescript
  import { timeout } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  // Simple form — errors after ms with no emission
  function timeout<T>(config: number | Date): OperatorFunction<T, T>

  // Config form (RxJS 7+)
  function timeout<T, O extends ObservableInput<unknown> = ObservableInput<T>>(
    config: TimeoutConfig<T, O> & { with: (info: TimeoutInfo<T>) => O }
  ): OperatorFunction<T, T | ObservedValueOf<O>>

  interface TimeoutConfig<T, O> {
    each?: number;         // ms timeout between each emission
    first?: number | Date; // ms (or Date) for first emission only
    with?: (info: TimeoutInfo<T>) => ObservableInput<unknown>; // fallback instead of error
    scheduler?: SchedulerLike;
  }
  ```

## Functional Specification

**Concept**: `timeout` monitors an Observable for "too slow" conditions. It errors (or switches to a fallback) when:
- `timeout(ms)`: no value arrives within `ms` milliseconds of subscription (or previous value)
- `timeout({ first: ms })`: no value arrives within `ms` ms of subscription only
- `timeout({ each: ms })`: the gap between any two consecutive emissions exceeds `ms` ms
- `timeout({ with: fn })`: instead of erroring, switches to the Observable returned by `fn`

**Error type**: `TimeoutError` — has `info` property with `{ lastValue, seen, meta }`.

**The `with` fallback** enables graceful degradation instead of propagating an error:
```typescript
timeout({
  each: 5000,
  with: info => of(DEFAULT_VALUE)  // switch to fallback on timeout
})
```

## Marble Diagrams

```
timeout(100ms):

Source:  --a-----------b--|    (gap between a and b > 100ms)
Result:  --a-----------#       (TimeoutError after 100ms of silence)

Source:  --a--b--c--|          (all values arrive within 100ms of each other)
Result:  --a--b--c--|          (passes through unchanged)

timeout({ first: 200ms }):

Source:  --(500ms)--a--|       (first value arrives after 200ms)
Result:  --#                   (TimeoutError — first value too slow)

Source:  --a--(500ms)--b--|    (first value fine, subsequent slow)
Result:  --a--(500ms)--b--|    (passes through — only first emission is guarded)

timeout({ with: () => of('fallback') }):

Source:  --(500ms)...          (no emission within default window)
Result:  --fallback|           (switches to fallback Observable instead of error)
```

## Type System Integration

```typescript
import { ajax } from 'rxjs/ajax';
import { timeout } from 'rxjs/operators';
import { of } from 'rxjs';
import { TimeoutError } from 'rxjs';

// Simple — type preserved
ajax.getJSON<User>('/api/user').pipe(
  timeout(5000) // Observable<User>
).subscribe({
  next: (u: User) => render(u),
  error: (e: TimeoutError) => showError('Request timed out')
});

// With fallback — union type
ajax.getJSON<User>('/api/user').pipe(
  timeout({ each: 5000, with: () => of(ANONYMOUS_USER) })
).subscribe((u: User) => render(u));
// Observable<User>  (ANONYMOUS_USER satisfies User)
```

## Examples

### Basic — HTTP Request Timeout
```typescript
import { ajax } from 'rxjs/ajax';
import { timeout, catchError, retry } from 'rxjs/operators';
import { TimeoutError, of } from 'rxjs';

ajax.getJSON<Data>('/api/data').pipe(
  timeout(5000),  // error if no response within 5 seconds
  retry(2),       // retry up to 2 times (including timed-out attempts)
  catchError(err => {
    if (err instanceof TimeoutError) {
      console.warn('Request timed out after retries');
      return of(EMPTY_DATA);
    }
    throw err; // re-throw non-timeout errors
  })
).subscribe(renderData);
```

### Common Pattern — `each` for Streaming Heartbeat
```typescript
import { webSocket } from 'rxjs/webSocket';
import { timeout } from 'rxjs/operators';

// WebSocket should receive messages at least every 30 seconds
// (server sends heartbeat pings to maintain this)
webSocket<Message>('wss://api.example.com/stream').pipe(
  timeout({ each: 30_000 }) // error if silent for 30s
).subscribe({
  next: msg => handleMessage(msg),
  error: err => reconnect() // TimeoutError means connection went silent
});
```

### Common Pattern — Graceful Fallback With `with`
```typescript
import { ajax } from 'rxjs/ajax';
import { timeout } from 'rxjs/operators';
import { of } from 'rxjs';

const CACHED_CONFIG = { theme: 'light', locale: 'en' };

// Try to load fresh config; if it takes > 2s, use the cached version
ajax.getJSON<Config>('/api/config').pipe(
  timeout({
    first: 2000,
    with: () => of(CACHED_CONFIG)
  })
).subscribe(config => applyConfig(config));
// No error handler needed — timeout is handled gracefully
```

### Common Pattern — Race Against Timeout
```typescript
import { race, timer } from 'rxjs';
import { map } from 'rxjs/operators';

// Explicit race approach — alternative to timeout operator
function withTimeout<T>(source$: Observable<T>, ms: number): Observable<T> {
  return race(
    source$,
    timer(ms).pipe(map(() => { throw new TimeoutError(); }))
  );
}

// The timeout operator approach is cleaner for most cases:
import { timeout } from 'rxjs/operators';
source$.pipe(timeout(ms)).subscribe(...);
```

## Common Pitfalls

### Anti-pattern: `timeout` Before `retry` (Wrong Order for Retry Logic)
```typescript
import { ajax } from 'rxjs/ajax';
import { timeout, retry } from 'rxjs/operators';

// ❌ WRONG ORDER — retry is inside the timeout guard
// timeout fires after X ms of silence, then retry resubscribes
// BUT the retry is now outside the timeout — timed-out requests get retried
// but fresh attempts each have their own full timeout window. This is actually
// the CORRECT order for most retry cases:

ajax.getJSON('/api').pipe(
  timeout(5000), // each attempt gets 5 seconds
  retry(3)       // retry up to 3 times after timeout
).subscribe(console.log);
// Each retry attempt gets a fresh 5-second window. Usually what you want.

// ❌ ACTUALLY WRONG — timeout wrapping the entire retry sequence
import { defer } from 'rxjs';
defer(() =>
  ajax.getJSON('/api').pipe(retry(3))
).pipe(
  timeout(5000) // 5 seconds for ALL retries combined — probably too tight
).subscribe(console.log);

// ✅ CORRECT — choose based on intent:
// "each attempt gets N seconds": timeout BEFORE retry
// "total time including retries": timeout OUTSIDE the entire pipe (rare)

// WHY: timeout position determines what gets measured. Before retry = per-attempt
// window. After retry = total time budget for all attempts. Most use cases
// want per-attempt windows — put timeout before retry.
```

### Anti-pattern: Not Distinguishing `TimeoutError` From Other Errors
```typescript
import { timeout, catchError } from 'rxjs/operators';
import { EMPTY } from 'rxjs';

// ❌ SILENT — swallows all errors including non-timeout failures
ajax.getJSON('/api/data').pipe(
  timeout(5000),
  catchError(_ => EMPTY) // 404, 500, network error — all treated the same
).subscribe(console.log);

// ✅ CORRECT — distinguish timeout from other error types
import { TimeoutError } from 'rxjs';
ajax.getJSON('/api/data').pipe(
  timeout(5000),
  catchError(err => {
    if (err instanceof TimeoutError) {
      console.warn('Slow server — serving cached data');
      return of(CACHE.get('data'));
    }
    // network errors, 4xx, 5xx — handle differently
    console.error('Request failed:', err);
    return EMPTY;
  })
).subscribe(console.log);

// WHY: TimeoutError means "the source was too slow." Other errors mean
// "the source actively failed." These typically warrant different responses:
// timeout → retry or fallback; other errors → error reporting, user feedback.
```

## Related Operators

- **`retry`**: Complement — resubscribes on error (including TimeoutError)
- **`catchError`**: Handle the TimeoutError with a fallback or recovery Observable
- **`delay`**: Complement — delays emissions; timeout guards against too much delay
- **`race`**: Alternative approach — first Observable to emit wins (can use for timeout logic)
- **`debounceTime`**: Guards against too-frequent emissions; timeout guards against too-infrequent

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/timeout](https://rxjs.dev/api/operators/timeout)

---

**Cognitive Load**: 3/5 | **Usage Frequency**: 4/5 | **Composability**: 4/5
**Key teaching points**:
1. `timeout(ms)` errors on any gap > ms — use `{ first }` or `{ each }` for finer control
2. `with` turns timeout from error into graceful fallback
3. Ordering with `retry`: usually `timeout` before `retry` (per-attempt window)
4. Check `instanceof TimeoutError` in `catchError` to differentiate from other failures
