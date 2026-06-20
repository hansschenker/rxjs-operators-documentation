# skip

**Category**: Filtering  
**Import**: `import { skip } from 'rxjs';`

## Description

`skip` returns an Observable that ignores the first `count` values emitted by the source, then passes through all subsequent values. It is the counterpart of `take`: while `take` keeps the first N and discards the rest, `skip` discards the first N and keeps the rest.

Internally, `skip` is implemented using `filter` with a counter, so it has no internal buffering — values are simply not forwarded until the skip count has been satisfied.

## Signature

```typescript
function skip<T>(count: number): MonoTypeOperatorFunction<T>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| count | `number` | The number of initial values to ignore before forwarding subsequent values. |

## Return Type

`MonoTypeOperatorFunction<T>` — emits all source values after the first `count` have been skipped.

## Marble Diagram

```
Source: --a--b--c--d--e--|
        skip(2)
Output: --------c--d--e--|
```

## Examples

### Example 1: Skip the initial burst of events on an interval

```typescript
import { interval } from 'rxjs';
import { skip } from 'rxjs';

interval(500).pipe(
  skip(10)
).subscribe(n => console.log('Value:', n));

// First output at ~5500ms: Value: 10
// Then: 11, 12, 13 ...
```

### Example 2: Skip the initial value from a BehaviorSubject

```typescript
import { BehaviorSubject } from 'rxjs';
import { skip } from 'rxjs';

// BehaviorSubject replays its current value on subscription.
// skip(1) lets you ignore the initial/seed value.
const state$ = new BehaviorSubject<number>(0);

state$.pipe(
  skip(1) // ignore the seed value
).subscribe(val => console.log('State changed to:', val));

state$.next(1); // Logs: State changed to: 1
state$.next(2); // Logs: State changed to: 2
```

### Example 3: Skip header rows when parsing a stream of CSV lines

```typescript
import { from } from 'rxjs';
import { skip, map } from 'rxjs';

const csvLines$ = from([
  'name,age,city',       // header
  'Alice,30,NYC',
  'Bob,25,LA',
  'Carol,35,Chicago'
]);

csvLines$.pipe(
  skip(1), // skip header
  map(line => {
    const [name, age, city] = line.split(',');
    return { name, age: Number(age), city };
  })
).subscribe(record => console.log(record));
```

## Common Pitfalls

- **`skip(0)` is a no-op**: Passing `0` makes the operator a transparent passthrough. This is valid but can be confusing if the count comes from a variable.
- **Does not error on over-skipping**: If `count` is greater than the number of values emitted, the output simply completes without emitting anything — no error is thrown.
- **Skipping vs filtering**: `skip` is position-based (first N values). To skip values based on content, use `filter` or `skipWhile`.

## Related Operators

- `take` — the complement: keeps the first N values
- `skipLast` — skips the last N values
- `skipUntil` — skips until a notifier fires
- `skipWhile` — skips while a predicate holds
- `filter` — drops values that fail a predicate, without a positional limit
