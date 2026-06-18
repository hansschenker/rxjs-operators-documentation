# concatMapTo

**Category**: Transformation  
**Import**: `import { concatMapTo } from 'rxjs';`

> **Deprecated**: `concatMapTo` will be removed in RxJS v9. Use `concatMap(() => innerObservable)` instead.

## Description

Projects each source value to the same Observable which is merged in a serialized fashion on the output Observable. Like `concatMap`, but maps every source value to the same fixed inner Observable rather than computing one per source value.

Each new inner Observable instance starts only after the previous one completes, guaranteeing order. This is equivalent to `mergeMapTo` with `concurrent = 1`.

**Warning**: if source values arrive faster than the inner Observable completes, they accumulate in an unbounded buffer.

## Signature

```typescript
function concatMapTo<O extends ObservableInput<unknown>>(
  innerObservable: O
): OperatorFunction<unknown, ObservedValueOf<O>>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `innerObservable` | `ObservableInput<O>` | The Observable (or Promise, array, etc.) to subscribe to for each source emission, one at a time. |

## Return Type

`OperatorFunction<unknown, ObservedValueOf<O>>` — emits values from each inner Observable in source order, one at a time.

## Marble Diagram

```
Source:   --a-----b-----c---|
         concatMapTo(of(1,2,3))
Inner:    --(1,2,3)|
Output:   --(1,2,3)--(1,2,3)--(1,2,3)|
```

## Examples

### Example 1: Tick 0–3 for each click, sequentially (deprecated form)

```typescript
import { fromEvent, concatMapTo, interval, take } from 'rxjs';

// Deprecated — shown for reference only
const clicks = fromEvent(document, 'click');
const result = clicks.pipe(
  concatMapTo(interval(1000).pipe(take(4)))
);
result.subscribe(x => console.log(x));
```

### Example 2: Preferred modern equivalent using `concatMap`

```typescript
import { fromEvent, concatMap, interval, take } from 'rxjs';

const clicks = fromEvent(document, 'click');
const result = clicks.pipe(
  concatMap(() => interval(1000).pipe(take(4)))
);
result.subscribe(x => console.log(x));
```

## Common Pitfalls

- **Deprecated API**: Migrate to `concatMap(() => innerObservable)` before upgrading to RxJS v9.
- **Queuing problem**: Since every source emission queues a new inner subscription, a fast source will accumulate many pending subscriptions waiting for the current one to finish.

## Related Operators

- `concatMap` — the modern replacement; `concatMap(() => obs)` is equivalent
- `mergeMapTo` — deprecated; subscribes concurrently
- `switchMapTo` — deprecated; cancels previous subscriptions on each new source emission
