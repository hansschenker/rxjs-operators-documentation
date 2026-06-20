# tap

**Category**: Utility  
**Import**: `import { tap } from 'rxjs';`

## Description

`tap` is designed to allow the developer a designated place to perform side effects — logging, debugging, updating external state — without altering the notification or the stream itself. The observable returned by `tap` is an exact mirror of the source, passing every `next`, `error`, and `complete` notification downstream unchanged.

While you could perform side effects inside `map` or `mergeMap`, doing so makes those functions impure and prevents memoization. `tap` is the correct place to isolate side effects. Beyond the standard observer callbacks, `tap` also accepts a `TapObserver` with `subscribe`, `unsubscribe`, and `finalize` hooks so you can react to the full subscription lifecycle.

## Signature

```typescript
function tap<T>(observerOrNext?: Partial<TapObserver<T>> | ((value: T) => void) | null): MonoTypeOperatorFunction<T>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| observerOrNext | `Partial<TapObserver<T>>` \| `((value: T) => void)` \| `null` | A next handler function, or a partial `TapObserver` with any of `next`, `error`, `complete`, `subscribe`, `unsubscribe`, and `finalize` callbacks. |

## Return Type

`MonoTypeOperatorFunction<T>` — an operator that returns an Observable identical to the source but executes the provided side-effect callbacks for each notification.

## Marble Diagram

```
Source:  --a--b--c--|
         tap(sideEffect)
Output:  --a--b--c--|
```

Each value passes through unchanged; `sideEffect` is called for every `next`.

## Examples

### Example 1: Debug values mid-pipeline

```typescript
import { of, tap, map } from 'rxjs';

of(Math.random()).pipe(
  tap(n => console.log('Raw value:', n)),
  map(n => n > 0.5 ? 'big' : 'small'),
  tap(label => console.log('Label:', label))
).subscribe(console.log);

// Raw value: 0.73
// Label: big
// big
```

### Example 2: Track subscription lifecycle for cleanup logging

```typescript
import { interval, take, tap } from 'rxjs';

const source$ = interval(1000).pipe(
  take(3),
  tap({
    subscribe: () => console.log('Subscribed to interval'),
    next: n => console.log('Tick:', n),
    complete: () => console.log('Interval complete'),
    unsubscribe: () => console.log('Manually unsubscribed'),
    finalize: () => console.log('Finalized (always called)')
  })
);

const sub = source$.subscribe();
// Subscribed to interval
// Tick: 0
// Tick: 1
// Tick: 2
// Interval complete
// Finalized (always called)
```

### Example 3: Force an error based on a condition

```typescript
import { of, tap } from 'rxjs';

const source$ = of(1, 2, 3, 4, 5);

source$.pipe(
  tap(n => {
    if (n > 3) {
      throw new RangeError(`Value ${n} exceeds maximum of 3`);
    }
  })
).subscribe({
  next: console.log,
  error: err => console.error(err.message)
});

// 1
// 2
// 3
// RangeError: Value 4 exceeds maximum of 3
```

## Common Pitfalls

- **Mutating objects**: `tap` receives references to emitted values. Mutating those objects will affect downstream operators and subscribers. Treat values as immutable inside `tap`.
- **Throwing synchronously**: If a `tap` handler throws synchronously, that error is re-emitted as an error notification from the resulting observable. This is intentional but can be surprising — ensure error-throwing logic is deliberate.
- **Confusing `complete` with `finalize`**: The `complete` callback in `tap` fires only when the source completes normally. Use `finalize` (or the `finalize` key in `TapObserver`) to run cleanup on both completion and unsubscription.
- **Using `tap` as a transform**: `tap` never changes the emitted value. If you find yourself wanting to return something from the `tap` callback, use `map` instead.

## Related Operators

- `finalize` — runs a callback only on termination (complete, error, or unsubscribe); equivalent to `tap`'s `finalize` callback but as a standalone operator
- `map` — transforms values; use instead of `tap` when the side effect should change what flows downstream
- `do` — the legacy alias for `tap` removed in RxJS 6
