# concatMap

**Category**: Transformation  
**Import**: `import { concatMap } from 'rxjs';`

## Description

Projects each source value to an Observable which is merged in the output Observable in a serialized fashion — waiting for each inner Observable to complete before subscribing to the next. Equivalent to `mergeMap` with a `concurrent` limit of `1`.

`concatMap` guarantees that inner Observables are processed strictly in source order. This makes it ideal when the order of side effects matters, such as sequentially submitting a queue of form updates or replaying a series of user actions.

**Warning**: if source values arrive faster than their corresponding inner Observables complete, inner Observables accumulate in an unbounded buffer. For an infinite or very fast source, this can cause memory issues.

## Signature

```typescript
function concatMap<T, O extends ObservableInput<any>>(
  project: (value: T, index: number) => O
): OperatorFunction<T, ObservedValueOf<O>>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `project` | `(value: T, index: number) => ObservableInput<O>` | A function that returns an Observable (or Promise, array, etc.) for each source value. Each result is subscribed to only after the previous one completes. |

## Return Type

`OperatorFunction<T, ObservedValueOf<O>>` — emits values from each inner Observable in source order, one at a time.

## Marble Diagram

```
Source:  --a-----b-----c---|
           concatMap(x => inner)
Inner a: --1--2--|
Inner b:         --3--4--|
Inner c:                  --5--|
Output:  --1--2----3--4----5--|
```

## Examples

### Example 1: Submit a queue of updates sequentially

```typescript
import { from, concatMap } from 'rxjs';

const updates = [
  { id: 1, status: 'processing' },
  { id: 2, status: 'complete' },
  { id: 3, status: 'failed' },
];

from(updates).pipe(
  concatMap(update =>
    fetch(`/api/orders/${update.id}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ status: update.status }),
    }).then(r => r.json())
  )
).subscribe({
  next: result => console.log('Updated:', result),
  complete: () => console.log('All updates complete'),
});
```

### Example 2: Play a sequence of animations in order

```typescript
import { from, concatMap, timer, map } from 'rxjs';

const steps = ['fadeIn', 'slideLeft', 'fadeOut'];

function animateStep(step: string) {
  console.log(`Starting: ${step}`);
  return timer(1000).pipe(map(() => `${step} done`));
}

from(steps).pipe(
  concatMap(animateStep)
).subscribe({
  next: msg => console.log(msg),
  complete: () => console.log('Animation sequence complete'),
});
```

### Example 3: Navigate wizard steps, loading data for each

```typescript
import { Subject, concatMap, from } from 'rxjs';

const nextStep$ = new Subject<number>();

nextStep$.pipe(
  concatMap(step =>
    from(fetch(`/api/wizard/step/${step}`).then(r => r.json()))
  )
).subscribe(stepData => console.log('Step data:', stepData));

nextStep$.next(1);
nextStep$.next(2);
nextStep$.next(3);
```

## Common Pitfalls

- **Buffer overflow**: Since `concatMap` queues all source values while an inner Observable is active, a fast source paired with slow inner Observables can exhaust memory. Consider `switchMap` or `exhaustMap` to limit pending work.
- **No cancellation**: Unlike `switchMap`, `concatMap` never cancels an in-flight inner Observable. This guarantees all work completes, but means the output may lag significantly behind the source.
- **Hanging stream**: If any inner Observable never completes, `concatMap` will never subscribe to subsequent inner Observables. Ensure inner Observables always terminate (use `take`, `first`, `timeout`, etc.).

## Related Operators

- `mergeMap` — subscribes concurrently; does not preserve order
- `switchMap` — cancels the previous inner Observable on each new source emission
- `exhaustMap` — ignores new source values while an inner Observable is active
- `concatMapTo` — deprecated variant that maps all values to the same inner Observable
