# catchError

**Category**: Error Handling  
**Import**: `import { catchError } from 'rxjs';`

## Description

`catchError` intercepts errors emitted by a source observable and maps them to a new observable, allowing the stream to continue rather than terminate. It only listens to the error channel and ignores `next` and `complete` notifications — all non-error events pass through unchanged.

When the source observable errors, `catchError` calls the provided selector function with the error and a reference to the caught (wrapped) source observable. Whatever observable the selector returns is subscribed to and its values are forwarded downstream. This means you can recover with fallback data, rethrow a transformed error, or even resubscribe to the original source to implement a basic retry.

## Signature

```typescript
function catchError<T, O extends ObservableInput<any>>(
  selector: (err: any, caught: Observable<T>) => O
): OperatorFunction<T, T | ObservedValueOf<O>>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `selector` | `(err: any, caught: Observable<T>) => O` | A function called when the source errors. Receives the error and the caught (re-wrapped) source observable. Must return an `ObservableInput` whose values continue the stream. Throwing inside this function emits a new error downstream. |

## Return Type

`OperatorFunction<T, T | ObservedValueOf<O>>` — an operator that returns an observable emitting values from the source, then (on error) values from the observable returned by the selector.

## Marble Diagram

```
Source:  --a--b--X
                  catchError(err => --c--d--|)
Output:  --a--b--c--d--|

Source:  --a--b--X
                  catchError(err => { throw newErr })
Output:  --a--b--X'
(X = error, X' = transformed error)
```

## Examples

### Example 1: Fallback to cached data on HTTP failure

```typescript
import { of, throwError, EMPTY } from 'rxjs';
import { catchError, map } from 'rxjs';
import { ajax } from 'rxjs/ajax';

const cachedProducts = [{ id: 1, name: 'Widget (cached)' }];

function getProducts() {
  return ajax.getJSON('/api/products').pipe(
    catchError(err => {
      console.warn('API unavailable, falling back to cache:', err.message);
      return of(cachedProducts);
    })
  );
}

getProducts().subscribe({
  next: products => console.log('Products:', products),
  error: err => console.error('This will not be called'),
  complete: () => console.log('Done')
});
```

### Example 2: Rethrowing a transformed error

```typescript
import { ajax } from 'rxjs/ajax';
import { catchError, map } from 'rxjs';

function getUser(id: number) {
  return ajax.getJSON(`/api/users/${id}`).pipe(
    map((response: any) => response.data),
    catchError(err => {
      // Wrap low-level HTTP error in a domain error
      if (err.status === 404) {
        throw new Error(`User ${id} not found`);
      }
      if (err.status === 403) {
        throw new Error('Access denied — please log in again');
      }
      throw new Error(`Unexpected error: ${err.message}`);
    })
  );
}

getUser(42).subscribe({
  next: user => console.log(user),
  error: err => console.error(err.message) // 'User 42 not found'
});
```

### Example 3: Silently completing on error with EMPTY

```typescript
import { of, EMPTY } from 'rxjs';
import { catchError, map } from 'rxjs';
import { ajax } from 'rxjs/ajax';

// Fire-and-forget analytics ping: if it fails, just do nothing
function sendAnalyticsEvent(event: object) {
  return ajax({
    url: '/api/analytics',
    method: 'POST',
    body: event
  }).pipe(
    catchError(() => EMPTY) // swallow the error, complete silently
  );
}

sendAnalyticsEvent({ action: 'page_view', page: '/home' }).subscribe();
// No error callback needed — failures are silently ignored
```

## Common Pitfalls

- **Accidentally swallowing all errors**: Returning `EMPTY` or a fallback from every error hides problems that may need attention. Always log or report the error before recovering, and consider being selective about which errors you catch by inspecting the error type or status code before deciding to recover.

- **Infinite retry loops**: Returning the `caught` observable (the second argument to the selector) from inside `catchError` creates a retry loop that will run forever if the source keeps erroring. Always combine with `take` or add a retry counter to prevent runaway subscriptions.

- **Errors thrown inside the selector are not caught by the same `catchError`**: If you throw inside the selector function, that error propagates downstream as a new error. You must add another `catchError` further down the pipe if you want to catch that too.

- **`catchError` only catches errors from the source above it in the pipe**: Operators chained *after* `catchError` are not covered by it. Place `catchError` after the operators whose errors you want to handle.

## Related Operators

- `retry` — automatically resubscribes to the source a fixed number of times without requiring a selector function; use when all errors should trigger the same re-attempt behavior
- `retryWhen` — deprecated; allows custom retry logic driven by a notifier observable; prefer `retry({ delay: ... })` instead
- `onErrorResumeNextWith` — continues with the next observable on error but gives you no access to the error value; useful when you want sequential fallbacks and do not care about error details
- `throwError` — creates an observable that immediately errors; useful inside a `catchError` selector to rethrow or transform errors
