# exhaustMap

**Category**: Transformation  
**Import**: `import { exhaustMap } from 'rxjs';`

## Description

Projects each source value to an Observable which is merged in the output Observable only if the previous projected Observable has completed. When an inner Observable is currently active, `exhaustMap` ignores any new source values until that inner Observable finishes.

This "exhaust" strategy is the opposite of `switchMap`. Where `switchMap` always switches to the latest, `exhaustMap` locks onto the current and ignores new requests until it is done. This makes it ideal for preventing duplicate submissions — for example, preventing a user from clicking a "Submit" button multiple times while a request is already in flight.

## Signature

```typescript
function exhaustMap<T, O extends ObservableInput<any>>(
  project: (value: T, index: number) => O
): OperatorFunction<T, ObservedValueOf<O>>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `project` | `(value: T, index: number) => ObservableInput<O>` | A function that returns an Observable (or Promise, array, etc.) for each source value. Ignored if a previous inner Observable is still active. |

## Return Type

`OperatorFunction<T, ObservedValueOf<O>>` — emits values from each accepted inner Observable; source values that arrive while an inner Observable is active are silently dropped.

## Marble Diagram

```
Source:  --a-----b--c-----d--|
            exhaustMap(x => inner)
Inner a: ----1--2--|
  (b is ignored — inner a still active)
  (c is ignored — inner a still active)
Inner d:                 --3--|
Output:  ----1--2---------3--|
```

## Examples

### Example 1: Prevent duplicate form submissions

```typescript
import { fromEvent, exhaustMap, from } from 'rxjs';

const submitBtn = document.querySelector<HTMLButtonElement>('#submit')!;

fromEvent(submitBtn, 'click').pipe(
  exhaustMap(() =>
    from(
      fetch('/api/orders', { method: 'POST', body: JSON.stringify({ item: 'book' }) })
        .then(r => r.json())
    )
  )
).subscribe({
  next: result => console.log('Order placed:', result),
  error: err => console.error('Order failed:', err),
});
```

### Example 2: Run a finite timer once per click, ignore rapid clicks

```typescript
import { fromEvent, exhaustMap, interval, take, map } from 'rxjs';

const startBtn = document.querySelector('#start')!;

fromEvent(startBtn, 'click').pipe(
  exhaustMap(() =>
    interval(1000).pipe(
      take(5),
      map(i => `Tick ${i + 1} of 5`)
    )
  )
).subscribe(msg => console.log(msg));
// Rapid clicks during the 5-second sequence are ignored
```

### Example 3: Rate-limit a polling trigger

```typescript
import { Subject, exhaustMap, from, delay } from 'rxjs';

const poll$ = new Subject<void>();

poll$.pipe(
  exhaustMap(() =>
    from(fetch('/api/status').then(r => r.json()))
  )
).subscribe(status => console.log('Status:', status));

// If poll$ emits while a request is in flight, it is dropped
poll$.next();
poll$.next(); // Dropped if the first request hasn't returned
```

## Common Pitfalls

- **Silent drops**: Source values that arrive while an inner Observable is active are silently discarded. If you need to process every emission, use `concatMap` or `mergeMap`.
- **No feedback on ignored emissions**: `exhaustMap` does not indicate which source values were dropped. If you need to notify the user that their action was ignored (e.g., "saving in progress"), add a guard before `exhaustMap`.
- **Confusion with `switchMap`**: Both operators respond to one inner Observable at a time, but with opposite strategies. `exhaustMap` keeps the current and ignores the new. `switchMap` cancels the current and uses the new.

## Related Operators

- `switchMap` — cancels the active inner Observable when a new source value arrives
- `concatMap` — queues all source values; processes them one at a time in order
- `mergeMap` — subscribes to all inner Observables concurrently without dropping any
