# every

**Category**: Utility  
**Import**: `import { every } from 'rxjs';`

## Description

`every` returns an Observable that emits a single boolean value: `true` if every item emitted by the source satisfies the given predicate before the source completes, or `false` as soon as any item fails the predicate.

When a failing item is encountered, `every` immediately emits `false` and completes — it does not wait for the source to complete. If all items pass the predicate and the source completes, `every` emits `true` and then completes. This mirrors the behavior of `Array.prototype.every`.

## Signature

```typescript
function every<T>(predicate: (value: T, index: number) => boolean): OperatorFunction<T, boolean>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| predicate | `(value: T, index: number) => boolean` | A function called for each source value. Return `true` if the value satisfies the condition. Receives the value and its zero-based emission index. |

## Return Type

`OperatorFunction<T, boolean>` — an operator that returns an Observable emitting a single `boolean`.

## Marble Diagram

```
Source:  --1--2--3--4--|
         every(n => n < 5)
Output:  --------------true|

Source:  --1--2--6--3--|
         every(n => n < 5)
Output:  --------false|
         (emits false immediately when 6 is seen, unsubscribes)
```

## Examples

### Example 1: Validate all form inputs pass a rule

```typescript
import { from, every } from 'rxjs';

const inputValues = ['alice@example.com', 'bob@example.com', 'not-an-email'];

from(inputValues).pipe(
  every(email => /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email))
).subscribe(allValid => {
  console.log('All emails valid:', allValid);
});

// All emails valid: false
```

### Example 2: Check that all API responses are successful

```typescript
import { from, mergeMap, map, every } from 'rxjs';

const endpoints = ['/api/check1', '/api/check2', '/api/check3'];

from(endpoints).pipe(
  mergeMap(url => from(fetch(url).then(r => ({ url, ok: r.ok })))),
  every(result => result.ok)
).subscribe(allHealthy => {
  console.log('All services healthy:', allHealthy);
});
```

### Example 3: Verify a numeric sequence is strictly increasing

```typescript
import { from, pairwise, every, startWith } from 'rxjs';

const sequence = [1, 3, 5, 7, 9];

from(sequence).pipe(
  startWith(Number.NEGATIVE_INFINITY),
  pairwise(),
  every(([prev, curr]) => curr > prev)
).subscribe(isAscending => {
  console.log('Strictly ascending:', isAscending);
});

// Strictly ascending: true
```

## Common Pitfalls

- **Early termination on `false`**: When `every` emits `false`, it immediately unsubscribes from the source. Any remaining values from the source are discarded. This is efficient but can be surprising in side-effectful pipelines.
- **Emits `true` only on source completion**: `every` does not emit `true` eagerly. It must wait for the source to complete to confirm all values passed. For infinite sources, `every` never emits `true`.
- **Index argument starts at 0**: The second argument to the predicate is the zero-based index of the emission, not a count.

## Related Operators

- `find` — emits the first value that satisfies a predicate, instead of a boolean
- `some` / `any equivalent` — RxJS does not have a built-in `some`, but `find` combined with `map(Boolean)` achieves the same effect
- `filter` — pass values through that match a predicate rather than returning a boolean summary
- `isEmpty` — a simpler boolean check with no predicate
