# mergeMap

**Category**: Transformation  
**Import**: `import { mergeMap } from 'rxjs';`

## Description

Projects each source value to an Observable which is merged into the output Observable. Also known as `flatMap`, `mergeMap` maps each incoming value to an inner Observable and subscribes to all of them concurrently, merging their emissions into a single output stream.

Unlike `concatMap` (which queues inner subscriptions) or `switchMap` (which cancels the previous inner subscription), `mergeMap` lets all inner Observables run at the same time. This makes it ideal for fire-and-forget tasks such as HTTP requests that are independent of each other and where ordering does not matter.

## Signature

```typescript
function mergeMap<T, O extends ObservableInput<any>>(
  project: (value: T, index: number) => O,
  concurrent?: number
): OperatorFunction<T, ObservedValueOf<O>>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `project` | `(value: T, index: number) => ObservableInput<O>` | A function that returns an Observable (or Promise, array, etc.) for each source value. |
| `concurrent` | `number` | Optional. Defaults to `Infinity`. Maximum number of inner Observables subscribed to simultaneously. |

## Return Type

`OperatorFunction<T, ObservedValueOf<O>>` — emits values from all inner Observables, interleaved in arrival order.

## Marble Diagram

```
Source:  --a---------b--------|
           mergeMap(x => inner)
Inner a: ----1--2--|
Inner b:       ----3--4--|
Output:  ----1--2--3--4--|
```

## Examples

### Example 1: Save multiple records concurrently

```typescript
import { from, mergeMap } from 'rxjs';

interface Record { id: number; name: string; }

const records: Record[] = [
  { id: 1, name: 'Alice' },
  { id: 2, name: 'Bob' },
  { id: 3, name: 'Carol' },
];

from(records).pipe(
  mergeMap(record =>
    fetch(`/api/records/${record.id}`, {
      method: 'PUT',
      body: JSON.stringify(record),
    }).then(res => res.json())
  )
).subscribe({
  next: result => console.log('Saved:', result),
  error: err => console.error('Save failed:', err),
});
```

### Example 2: Load user details for a stream of user IDs

```typescript
import { Subject, mergeMap, from } from 'rxjs';

const userId$ = new Subject<number>();

userId$.pipe(
  mergeMap(id => from(fetch(`/api/users/${id}`).then(r => r.json())))
).subscribe(user => console.log('Loaded user:', user));

userId$.next(1);
userId$.next(2);
userId$.next(3);
```

### Example 3: Limit concurrency when making API calls

```typescript
import { from, mergeMap } from 'rxjs';

const imageUrls = ['/img/1.jpg', '/img/2.jpg', '/img/3.jpg', '/img/4.jpg', '/img/5.jpg'];

// Process at most 2 images at a time
from(imageUrls).pipe(
  mergeMap(url => from(fetch(url).then(r => r.blob())), 2)
).subscribe(blob => console.log('Downloaded blob:', blob.size));
```

## Common Pitfalls

- **Unbound concurrency**: The default `concurrent` value is `Infinity`, which means every source value spawns a new subscription immediately. For potentially large sources (e.g., reading thousands of IDs), always pass a `concurrent` limit to avoid flooding resources.
- **Order not guaranteed**: Emissions from inner Observables arrive in completion order, not source order. If you need to preserve order, use `concatMap` instead.
- **Memory leak from long-lived inner Observables**: If inner Observables never complete, they accumulate. Consider using `takeUntil` or `take` on inner Observables.
- **Error propagation**: An error from any inner Observable propagates to the output and tears down the whole chain. Isolate errors per inner stream with `catchError` if needed.

## Related Operators

- `concatMap` — like `mergeMap` with `concurrent = 1`; serializes inner subscriptions
- `switchMap` — cancels the previous inner Observable when a new source value arrives
- `exhaustMap` — ignores new source values while an inner Observable is still active
- `mergeMapTo` — deprecated variant that maps all values to the same inner Observable
- `mergeScan` — like `mergeMap` but carries accumulated state between emissions
