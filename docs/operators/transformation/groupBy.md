# groupBy

**Category**: Transformation  
**Import**: `import { groupBy } from 'rxjs';`

## Description

Groups the items emitted by an Observable according to a specified criterion and emits these grouped items as `GroupedObservable` instances, one per unique key. Each `GroupedObservable` exposes a `key` property and emits values belonging to that group.

When the source emits a value, a key is computed using the `key` function. If a `GroupedObservable` for that key already exists, the value is routed to it. Otherwise, a new `GroupedObservable` is created and emitted on the output Observable.

The modern API accepts an options object as the second argument, allowing you to specify an `element` projection, a `duration` Observable (to expire groups), and a custom `connector` factory for the underlying Subject.

## Signature

```typescript
function groupBy<T, K>(key: (value: T) => K, options?: BasicGroupByOptions<K, T>): OperatorFunction<T, GroupedObservable<K, T>>
function groupBy<T, K, E>(key: (value: T) => K, options: GroupByOptionsWithElement<K, E, T>): OperatorFunction<T, GroupedObservable<K, E>>
```

### Options interfaces

```typescript
interface BasicGroupByOptions<K, T> {
  element?: undefined;
  duration?: (grouped: GroupedObservable<K, T>) => ObservableInput<any>;
  connector?: () => SubjectLike<T>;
}

interface GroupByOptionsWithElement<K, E, T> {
  element: (value: T) => E;
  duration?: (grouped: GroupedObservable<K, E>) => ObservableInput<any>;
  connector?: () => SubjectLike<E>;
}
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `key` | `(value: T) => K` | A function that extracts the grouping key from each source value. |
| `options.element` | `(value: T) => E` | Optional. A projection function applied to each value before routing it to a group. |
| `options.duration` | `(group: GroupedObservable<K, E>) => ObservableInput<any>` | Optional. A function that returns an Observable; when that Observable emits, the corresponding group is completed and removed, allowing it to be re-created on the next matching value. |
| `options.connector` | `() => SubjectLike<E>` | Optional. A factory that creates the Subject powering each group. Defaults to `new Subject()`. |

## Return Type

`OperatorFunction<T, GroupedObservable<K, T>>` — emits a `GroupedObservable<K, T>` for each distinct key. Each `GroupedObservable` has a `key` property and emits values belonging to that group.

## Marble Diagram

```
Source: --{id:1,n:'a'}--{id:2,n:'b'}--{id:1,n:'c'}--|
              groupBy(x => x.id)
Output: --g1-----------g2--------------------------|
  g1 (key=1): --{id:1,n:'a'}----------{id:1,n:'c'}|
  g2 (key=2):            --{id:2,n:'b'}|
```

## Examples

### Example 1: Group objects by ID and collect into arrays

```typescript
import { of, groupBy, mergeMap, reduce } from 'rxjs';

of(
  { id: 1, name: 'JavaScript' },
  { id: 2, name: 'Parcel' },
  { id: 2, name: 'webpack' },
  { id: 1, name: 'TypeScript' },
  { id: 3, name: 'TSLint' }
).pipe(
  groupBy(p => p.id),
  mergeMap(group$ =>
    group$.pipe(reduce((acc, cur) => [...acc, cur], [] as typeof cur[]))
  )
).subscribe(group => console.log(group));

// [{ id: 1, name: 'JavaScript' }, { id: 1, name: 'TypeScript' }]
// [{ id: 2, name: 'Parcel' }, { id: 2, name: 'webpack' }]
// [{ id: 3, name: 'TSLint' }]
```

### Example 2: Group with element projection

```typescript
import { of, groupBy, mergeMap, reduce, map } from 'rxjs';

of(
  { id: 1, name: 'JavaScript' },
  { id: 2, name: 'Parcel' },
  { id: 2, name: 'webpack' },
  { id: 1, name: 'TypeScript' },
  { id: 3, name: 'TSLint' }
).pipe(
  groupBy(p => p.id, { element: p => p.name }),
  mergeMap(group$ =>
    group$.pipe(
      reduce((acc, name) => [...acc, name], [group$.key.toString()] as string[])
    )
  ),
  map(arr => ({ id: parseInt(arr[0], 10), values: arr.slice(1) }))
).subscribe(p => console.log(p));

// { id: 1, values: ['JavaScript', 'TypeScript'] }
// { id: 2, values: ['Parcel', 'webpack'] }
// { id: 3, values: ['TSLint'] }
```

### Example 3: Group live events with expiring groups

```typescript
import { Subject, groupBy, mergeMap, take } from 'rxjs';
import { timer } from 'rxjs';

interface UserEvent { userId: string; action: string }

const event$ = new Subject<UserEvent>();

event$.pipe(
  groupBy(
    e => e.userId,
    {
      // Each group expires after 30 seconds of its first emission
      duration: group$ => timer(30_000)
    }
  ),
  mergeMap(group$ => {
    console.log(`New group for user: ${group$.key}`);
    return group$.pipe(take(10)); // process up to 10 events per user
  })
).subscribe(event => console.log(`User ${event.userId}: ${event.action}`));
```

## Common Pitfalls

- **Subscribing to groups**: You must subscribe to each emitted `GroupedObservable` (typically via `mergeMap`). If you don't subscribe to a group, its values are buffered internally and may cause memory leaks.
- **Groups never complete (without `duration`)**: By default, groups live as long as the source Observable. To release memory for inactive groups, provide a `duration` function that returns a timeout Observable.
- **Deprecated positional parameters**: Passing `element`, `duration`, and `connector` as positional arguments (the old API) is deprecated. Use the options object instead.
- **Key equality**: Group keys use `Map` equality (strict equality for primitives, reference equality for objects). Use primitive keys or stable string representations.

## Related Operators

- `partition` — splits a source into exactly two Observables based on a predicate
- `mergeMap` — commonly used together with `groupBy` to process each group
- `reduce` — aggregate values within each group
- `window` — splits into time- or count-based windows rather than key-based groups
