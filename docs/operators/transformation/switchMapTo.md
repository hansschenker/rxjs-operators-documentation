# switchMapTo

**Category**: Transformation  
**Import**: `import { switchMapTo } from 'rxjs';`

> **Deprecated**: `switchMapTo` will be removed in RxJS v9. Use `switchMap(() => innerObservable)` instead.

## Description

Projects each source value to the same Observable which is flattened using `switchMap` behavior. Each time the source emits, `switchMapTo` unsubscribes from the previous inner Observable and subscribes to `innerObservable` again. Only values from the most recently started inner subscription are emitted.

## Signature

```typescript
function switchMapTo<O extends ObservableInput<unknown>>(
  innerObservable: O
): OperatorFunction<unknown, ObservedValueOf<O>>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `innerObservable` | `ObservableInput<O>` | The Observable (or Promise, array, etc.) to switch to on every source emission. |

## Return Type

`OperatorFunction<unknown, ObservedValueOf<O>>` — emits values only from the most recently started inner subscription.

## Marble Diagram

```
Source:  --a---------b---------|
        switchMapTo(interval(1000))
Inner a: --0--1--2--...
Inner b:             --0--1--|
Output:  --0--1--2---0--1--|
                (a cancelled when b arrives)
```

## Examples

### Example 1: Restart an interval on every click (deprecated form)

```typescript
import { fromEvent, switchMapTo, interval } from 'rxjs';

// Deprecated — shown for reference only
const clicks = fromEvent(document, 'click');
const result = clicks.pipe(switchMapTo(interval(1000)));

result.subscribe(x => console.log(x));
```

### Example 2: Preferred modern equivalent using `switchMap`

```typescript
import { fromEvent, switchMap, interval } from 'rxjs';

const clicks = fromEvent(document, 'click');
const result = clicks.pipe(switchMap(() => interval(1000)));

result.subscribe(x => console.log(x));
```

## Common Pitfalls

- **Deprecated API**: Migrate to `switchMap(() => innerObservable)` before upgrading to RxJS v9.
- **Shared reference**: All inner subscriptions use the same `innerObservable` reference. For cold Observables this is fine (each subscription is independent). For hot Observables (e.g., Subjects), they all share the same stream.

## Related Operators

- `switchMap` — the modern replacement; `switchMap(() => obs)` is equivalent
- `mergeMapTo` — deprecated; does not cancel previous inner subscriptions
- `concatMapTo` — deprecated; serializes inner subscriptions
