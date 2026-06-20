# retryWhen

**Category**: Error Handling  
**Import**: `import { retryWhen } from 'rxjs';`

> **Deprecated**: `retryWhen` will be removed in RxJS v9 or v10. Use `retry` with the `delay` option instead. See the migration examples below.

## Description

`retryWhen` mirrors the source observable and, when the source errors, emits that error to a notifier observable returned by the provided function. Each time the notifier emits a value, `retryWhen` resubscribes to the source. If the notifier completes, the result observable completes. If the notifier itself errors, that error propagates to the result observable.

The notifier pattern gave fine-grained control over retry timing and conditions — for example, using `delayWhen` to implement back-off, or `take` to cap retries. However, this API is awkward and has been superseded by `retry({ delay: ... })`, which exposes the same capabilities more directly. New code should use `retry` instead.

## Signature

```typescript
function retryWhen<T>(
  notifier: (errors: Observable<any>) => ObservableInput<any>
): MonoTypeOperatorFunction<T>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `notifier` | `(errors: Observable<any>) => ObservableInput<any>` | A function that receives an observable of errors. Each emission on the returned notifier observable triggers a resubscription to the source. A `complete` on the notifier completes the result; an `error` on the notifier propagates that error. |

## Return Type

`MonoTypeOperatorFunction<T>` — an operator that returns an observable mirroring the source, resubscribing whenever the notifier emits.

## Marble Diagram

```
Source:   --a--b--X
                   retryWhen(errors$ => errors$.pipe(delay(1000)))
Notifier: --------[1s]--emit
Output:   --a--b------a--b--X  (retries once after 1s delay)
(X = error)
```

## Examples

### Example 1: Retry with a fixed delay (deprecated pattern)

```typescript
import { interval, map, retryWhen, delayWhen, timer, take } from 'rxjs';

const source$ = interval(500).pipe(
  map(value => {
    if (value > 2) throw new Error(`Value ${value} exceeded limit`);
    return value;
  })
);

// DEPRECATED — shown for reference only
source$.pipe(
  retryWhen(errors$ =>
    errors$.pipe(
      delayWhen(() => timer(2000)), // wait 2s before each retry
      take(3)                       // give up after 3 retries
    )
  )
).subscribe({
  next: val => console.log(val),
  error: err => console.error('Gave up:', err)
});
```

### Example 2: Modern equivalent using `retry` with `delay`

```typescript
import { interval, map, retry } from 'rxjs';
import { timer } from 'rxjs';

const source$ = interval(500).pipe(
  map(value => {
    if (value > 2) throw new Error(`Value ${value} exceeded limit`);
    return value;
  })
);

// PREFERRED — use retry({ delay }) instead of retryWhen
source$.pipe(
  retry({
    count: 3,
    delay: (error, retryCount) => {
      console.log(`Retry ${retryCount} after error: ${error.message}`);
      return timer(2000); // wait 2s before each retry
    }
  })
).subscribe({
  next: val => console.log(val),
  error: err => console.error('Gave up:', err)
});
```

### Example 3: Conditional retry — only retry on recoverable errors

```typescript
import { ajax } from 'rxjs/ajax';
import { retry, catchError } from 'rxjs';
import { timer, throwError } from 'rxjs';

// PREFERRED modern pattern: inspect error inside the delay function
function fetchWithConditionalRetry(url: string) {
  return ajax.getJSON(url).pipe(
    retry({
      count: 3,
      delay: (error, retryCount) => {
        // Only retry on server errors (5xx), not client errors (4xx)
        if (error.status >= 400 && error.status < 500) {
          // Client error — do not retry, rethrow immediately
          return throwError(() => error);
        }
        // Server error — retry with exponential back-off
        return timer(500 * retryCount);
      }
    }),
    catchError(err => {
      throw new Error(`Failed: ${err.status} ${err.message}`);
    })
  );
}

fetchWithConditionalRetry('/api/resource').subscribe({
  next: data => console.log(data),
  error: err => console.error(err.message)
});
```

## Migration Guide

Replace `retryWhen` with `retry({ delay })`:

```typescript
// Before (retryWhen)
source$.pipe(
  retryWhen(errors$ => errors$.pipe(delay(1000)))
)

// After (retry with delay)
source$.pipe(
  retry({ delay: () => timer(1000) })
)

// Before (retryWhen with take to cap retries)
source$.pipe(
  retryWhen(errors$ =>
    errors$.pipe(delayWhen((_, i) => timer(i * 1000)), take(3))
  )
)

// After
source$.pipe(
  retry({
    count: 3,
    delay: (_, retryCount) => timer(retryCount * 1000)
  })
)
```

## Common Pitfalls

- **Using `retryWhen` in new code**: This operator is deprecated and will be removed. Always use `retry({ delay: ... })` for new projects to avoid a migration burden later.

- **Notifier completing without emitting**: If the notifier observable completes without ever emitting a value, the result observable completes silently — it does not error and does not retry. This is easy to accidentally trigger with operators like `EMPTY` or a `Subject` that completes early.

- **Shared notifier state across retries**: The `notifier` function is called only once; the returned observable must handle all retry signals on the same stream. Using a `Subject` incorrectly inside the notifier can lead to missed emissions or unexpected completion.

- **Errors thrown by the notifier propagate as-is**: There is no inner error handling for the notifier itself. If the observable returned by `notifier` errors, that error immediately terminates the result observable.

## Related Operators

- `retry` — the modern, preferred replacement; accepts `count`, `delay`, and `resetOnSuccess` directly without requiring a notifier observable; use this for all new code
- `catchError` — intercepts errors with full access to the error value, returning a fallback observable rather than resubscribing to the source
- `repeat` — analogous operator for the completion channel; resubscribes on complete instead of error
