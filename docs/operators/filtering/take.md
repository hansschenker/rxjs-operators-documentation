# take

**Category**: Filtering  
**Import**: `import { take } from 'rxjs';`

## Description

`take` emits only the first `count` values emitted by the source Observable, then completes. If the source emits fewer than `count` values before completing, all values are forwarded and the output simply mirrors the source's completion. If `count` is zero or negative, the output Observable completes immediately without emitting anything.

Unlike `first`, `take` never throws an error on an empty source — it simply completes.

## Signature

```typescript
function take<T>(count: number): MonoTypeOperatorFunction<T>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| count | `number` | The maximum number of `next` values to emit. If `<= 0`, the output completes immediately. |

## Return Type

`MonoTypeOperatorFunction<T>` — emits at most `count` values from the source, then completes.

## Marble Diagram

```
Source: --a--b--c--d--e--|
        take(3)
Output: --a--b--c|
```

## Examples

### Example 1: Take only the first 5 ticks of an interval

```typescript
import { interval } from 'rxjs';
import { take } from 'rxjs';

interval(1000).pipe(
  take(5)
).subscribe({
  next: n => console.log(n),
  complete: () => console.log('Done!')
});

// Logs: 0, 1, 2, 3, 4, Done!
```

### Example 2: Limit user input to the first 3 keystrokes

```typescript
import { fromEvent } from 'rxjs';
import { take, map } from 'rxjs';

const keyup$ = fromEvent<KeyboardEvent>(document, 'keyup');

keyup$.pipe(
  take(3),
  map(ev => ev.key)
).subscribe({
  next: key => console.log('Key:', key),
  complete: () => console.log('Captured 3 keystrokes')
});
```

### Example 3: Combine with other operators to process a fixed-size batch

```typescript
import { Subject } from 'rxjs';
import { take, toArray } from 'rxjs';

const events$ = new Subject<number>();

events$.pipe(
  take(4),
  toArray()
).subscribe(batch => console.log('Batch:', batch));

events$.next(10);
events$.next(20);
events$.next(30);
events$.next(40); // triggers completion and toArray output

// Logs: Batch: [10, 20, 30, 40]
```

## Common Pitfalls

- **`take(0)` emits nothing**: Passing `0` or a negative number immediately produces an empty Observable. This can be surprising if the count comes from a runtime variable.
- **`take` vs `first`**: `take(1)` completes silently on empty sources, while `first()` throws `EmptyError`. Choose based on whether an empty source is a valid state or a bug.
- **Unsubscription**: `take` automatically unsubscribes from the source after the count is reached, so infinite Observables like `interval` are cleaned up properly.

## Related Operators

- `takeLast` — emits the last N values instead of the first N
- `takeUntil` — emits values until a notifier fires
- `takeWhile` — emits values as long as a predicate holds
- `skip` — skips the first N values (the complement of `take`)
- `first` — like `take(1)` but throws on empty sources
