# count

**Category**: Utility  
**Import**: `import { count } from 'rxjs';`

## Description

`count` transforms a source Observable into one that emits a single number representing how many values were emitted by the source before it completed. An optional `predicate` function lets you count only the values that satisfy a condition.

Like all aggregation operators, `count` only emits when the source completes. If the source errors before completing, `count` passes the error through without emitting a count. If the source never completes, `count` never emits.

## Signature

```typescript
function count<T>(predicate?: (value: T, index: number) => boolean): OperatorFunction<T, number>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| predicate | `(value: T, index: number) => boolean` | Optional. A function called for each value. Return `true` to include that value in the count. If omitted, all values are counted. |

## Return Type

`OperatorFunction<T, number>` — an operator that returns an Observable emitting a single number.

## Marble Diagram

```
Source:  --a--b--c--d--|
         count()
Output:  --------------4|

Source:  --1--2--3--4--|
         count(n => n % 2 === 0)
Output:  --------------2|
```

## Examples

### Example 1: Count how many seconds pass before the first click

```typescript
import { interval, fromEvent, takeUntil, count } from 'rxjs';

const seconds$ = interval(1000);
const click$ = fromEvent(document, 'click');

seconds$.pipe(
  takeUntil(click$),
  count()
).subscribe(n => console.log(`You waited ${n} second(s) before clicking`));
```

### Example 2: Count only values matching a predicate

```typescript
import { range, count } from 'rxjs';

range(1, 10).pipe(
  count(n => n % 2 === 0)
).subscribe(evenCount => console.log('Even numbers:', evenCount));

// Even numbers: 5
```

### Example 3: Count HTTP errors in a batch of requests

```typescript
import { from, mergeMap, materialize, count, filter } from 'rxjs';

const urls = ['/api/1', '/api/bad', '/api/2', '/api/also-bad'];

from(urls).pipe(
  mergeMap(url =>
    from(fetch(url).then(r => r.json())).pipe(materialize())
  ),
  filter(n => n.kind === 'E'),
  count()
).subscribe(errorCount => {
  console.log(`${errorCount} requests failed`);
});
```

## Common Pitfalls

- **Only emits on completion**: `count` is an aggregation operator. It buffers state silently until the source completes, then emits once. For running counts, use `scan` instead.
- **Never emits for infinite observables**: Without a completion event, `count` will hold its accumulator forever without emitting.
- **Predicate index is zero-based**: The `index` argument in the predicate starts at 0 for the first emission, regardless of the value.

## Related Operators

- `scan` — emits a running count/accumulation on every `next` emission, not just on completion
- `reduce` — generalized aggregation; `count` is a specialization of `reduce`
- `toArray` — collects all values; useful when you need the actual values, not just a count
- `max` / `min` — find extreme values using similar aggregation semantics
