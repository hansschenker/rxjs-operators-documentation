# mergeMapTo

**Category**: Transformation  
**Import**: `import { mergeMapTo } from 'rxjs';`

> **Deprecated**: `mergeMapTo` will be removed in RxJS v9. Use `mergeMap(() => innerObservable)` instead.

## Description

Projects each source value to the same Observable, which is merged multiple times in the output Observable. It is like `mergeMap`, but maps every value to the same inner Observable rather than computing one per source value.

Every time the source emits, `mergeMapTo` subscribes to `innerObservable` again and merges its emissions into the output. All inner subscriptions run concurrently.

## Signature

```typescript
function mergeMapTo<O extends ObservableInput<unknown>>(
  innerObservable: O,
  concurrent?: number
): OperatorFunction<unknown, ObservedValueOf<O>>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `innerObservable` | `ObservableInput<O>` | The Observable (or Promise, array, etc.) to subscribe to for each source emission. |
| `concurrent` | `number` | Optional. Defaults to `Infinity`. Maximum number of concurrent inner subscriptions. |

## Return Type

`OperatorFunction<unknown, ObservedValueOf<O>>` — emits all values from each inner Observable subscription, interleaved in arrival order.

## Marble Diagram

```
Source:  --a-----------b---------|
        mergeMapTo(interval(1000))
Inner1:   --0--1--2--3--...
Inner2:              --0--1--2--...
Output: --0--1--2--3--0--1--2--...
```

## Examples

### Example 1: Start an interval for every click (deprecated form)

```typescript
import { fromEvent, mergeMapTo, interval } from 'rxjs';

// Deprecated — shown for reference only
const clicks = fromEvent(document, 'click');
const result = clicks.pipe(mergeMapTo(interval(1000)));

result.subscribe(x => console.log(x));
```

### Example 2: Preferred modern equivalent using `mergeMap`

```typescript
import { fromEvent, mergeMap, interval } from 'rxjs';

const clicks = fromEvent(document, 'click');
const result = clicks.pipe(mergeMap(() => interval(1000)));

result.subscribe(x => console.log(x));
```

### Example 3: Poll a status endpoint on a trigger stream

```typescript
import { Subject, mergeMap, from, take } from 'rxjs';

const trigger$ = new Subject<void>();

// Modern equivalent
trigger$.pipe(
  mergeMap(() => from(fetch('/api/status').then(r => r.json())).pipe(take(1)))
).subscribe(status => console.log('Status:', status));

trigger$.next();
trigger$.next();
```

## Common Pitfalls

- **Deprecated API**: Migrate to `mergeMap(() => innerObservable)` before upgrading to RxJS v9.
- **Shared reference**: All inner subscriptions use the exact same `innerObservable` reference. If the inner Observable has side effects on subscription (e.g., a Subject or a cold HTTP Observable), they run independently for each source emission — which is usually the desired behavior.
- **Unbounded concurrency**: Like `mergeMap`, the default `concurrent` is `Infinity`. Limit it when the inner Observable is a resource-intensive task.

## Related Operators

- `mergeMap` — the modern replacement; `mergeMap(() => obs)` is equivalent
- `concatMapTo` — deprecated variant that serializes inner subscriptions
- `switchMapTo` — deprecated variant that cancels the previous inner subscription
