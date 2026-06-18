# finalize

**Category**: Utility  
**Import**: `import { finalize } from 'rxjs';`

## Description

`finalize` calls a specified callback when the source Observable terminates — whether by completing normally, erroring, or being explicitly unsubscribed. It mirrors the source exactly, passing all notifications downstream unchanged, and only invokes the callback at the moment the subscription is torn down.

This is analogous to a `finally` block in synchronous code: it is guaranteed to run regardless of how the observable terminates. Common use cases include releasing resources, clearing timers, updating loading indicators, or logging cleanup events.

## Signature

```typescript
function finalize<T>(callback: () => void): MonoTypeOperatorFunction<T>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| callback | `() => void` | A function called when the source completes, errors, or is unsubscribed. Receives no arguments. |

## Return Type

`MonoTypeOperatorFunction<T>` — an operator that returns an Observable identical to the source but calls `callback` on teardown.

## Marble Diagram

```
Source:  --a--b--c--|
         finalize(() => cleanup())
Output:  --a--b--c--|  (cleanup() called after completion)

Source:  --a--b--X     (error)
Output:  --a--b--X     (cleanup() called after error)

Unsub:   --a--b--      (unsubscribe at any time)
Output:  --a--b--      (cleanup() called on unsubscribe)
```

## Examples

### Example 1: Hide a loading spinner when an HTTP request finishes

```typescript
import { from, finalize } from 'rxjs';

function fetchData(url: string) {
  showSpinner();
  return from(fetch(url).then(r => r.json())).pipe(
    finalize(() => hideSpinner())
  );
}

function showSpinner() { console.log('Loading...'); }
function hideSpinner() { console.log('Done loading.'); }

fetchData('/api/data').subscribe({
  next: data => console.log('Data:', data),
  error: err => console.error('Error:', err)
});
// Loading...
// Data: {...}
// Done loading.
```

### Example 2: Release a lock or WebSocket connection on unsubscription

```typescript
import { webSocket } from 'rxjs/webSocket';
import { finalize } from 'rxjs';

const socket$ = webSocket('wss://example.com/stream');

const subscription = socket$.pipe(
  finalize(() => {
    console.log('WebSocket subscription ended — releasing lock');
    releaseLock();
  })
).subscribe(message => console.log('Message:', message));

// Unsubscribing triggers finalize
setTimeout(() => subscription.unsubscribe(), 5000);

function releaseLock() { /* ... */ }
```

### Example 3: Log completion status of inner observables

```typescript
import { of, concatMap, interval, take, finalize } from 'rxjs';

of('request-1', 'request-2', 'request-3').pipe(
  concatMap(name =>
    interval(500).pipe(
      take(2),
      finalize(() => console.log(`${name} finalized`))
    )
  )
).subscribe(val => console.log(val));

// 0
// 1
// request-1 finalized
// 0
// 1
// request-2 finalized
// 0
// 1
// request-3 finalized
```

## Common Pitfalls

- **Callback receives no information**: Unlike a `finally` block, `finalize`'s callback does not know whether the observable completed, errored, or was unsubscribed. If you need to distinguish these cases, use `tap({ error, complete })` combined with `finalize`.
- **Placement matters**: `finalize` only tears down when the subscription it wraps is torn down. Placing it inside an inner observable (e.g., inside `switchMap`) means it fires when the inner subscription ends, not when the outer one does.
- **Errors still propagate**: `finalize` does not catch errors. It runs the callback and then the error continues downstream. Use `catchError` upstream if you need to handle the error.

## Related Operators

- `tap` — has a `finalize` callback option as part of `TapObserver`; use `tap` when you also need `next`/`error`/`complete` side effects
- `catchError` — intercept errors; use alongside `finalize` when you need both recovery and cleanup
