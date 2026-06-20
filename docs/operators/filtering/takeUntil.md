# takeUntil

**Category**: Filtering  
**Import**: `import { takeUntil } from 'rxjs';`

## Description

`takeUntil` mirrors the source Observable until a second Observable (the `notifier`) emits its first value, at which point the output Observable completes. This is the idiomatic RxJS pattern for tearing down subscriptions in response to events — for example, stopping a polling loop when a component is destroyed.

If the notifier completes without ever emitting a value, `takeUntil` passes all source values through until the source itself completes. Errors from either Observable are forwarded to the output.

## Signature

```typescript
function takeUntil<T>(notifier: ObservableInput<any>): MonoTypeOperatorFunction<T>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| notifier | `ObservableInput<any>` | An Observable (or Promise, array, etc.) whose first emission causes the output to complete. |

## Return Type

`MonoTypeOperatorFunction<T>` — emits source values until the notifier fires, then completes.

## Marble Diagram

```
Source:   --a--b--c--d--e--|
Notifier: ----------n------|
          takeUntil(notifier)
Output:   --a--b--c|
```

## Examples

### Example 1: Stop an interval when the user clicks

```typescript
import { interval, fromEvent } from 'rxjs';
import { takeUntil } from 'rxjs';

const tick$ = interval(1000);
const stop$ = fromEvent(document, 'click');

tick$.pipe(
  takeUntil(stop$)
).subscribe({
  next: n => console.log('Tick:', n),
  complete: () => console.log('Stopped on click')
});
```

### Example 2: Angular-style component teardown

```typescript
import { interval, Subject } from 'rxjs';
import { takeUntil } from 'rxjs';

class MyComponent {
  private destroy$ = new Subject<void>();

  ngOnInit() {
    interval(500).pipe(
      takeUntil(this.destroy$)
    ).subscribe(n => console.log('Component tick:', n));
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
```

### Example 3: Race two streams and take from the first to emit

```typescript
import { fromEvent, Subject } from 'rxjs';
import { takeUntil, map } from 'rxjs';

const pointerMove$ = fromEvent<PointerEvent>(document, 'pointermove');
const pointerUp$ = fromEvent(document, 'pointerup');

// Track pointer position until the user releases
pointerMove$.pipe(
  takeUntil(pointerUp$),
  map(ev => ({ x: ev.clientX, y: ev.clientY }))
).subscribe(pos => console.log('Position:', pos));
```

## Common Pitfalls

- **Memory leaks if the notifier never emits**: If the `destroy$` subject is never completed or nexted, the subscription will not be cleaned up. Always call `.next()` and `.complete()` on teardown subjects.
- **Notifier completing without emitting**: If the notifier completes without emitting any values, `takeUntil` passes all source values through. This can be unintuitive if you expect a Subject's `.complete()` call to trigger teardown — use `.next()` before `.complete()`.
- **Placement in the pipe chain**: Put `takeUntil` as the last operator in a chain. Placing it earlier can cause inner subscriptions created by operators like `switchMap` to outlive the outer subscription.

## Related Operators

- `takeWhile` — completes based on a predicate over source values
- `take` — completes after a fixed count
- `skipUntil` — the complement: skips values until the notifier fires
