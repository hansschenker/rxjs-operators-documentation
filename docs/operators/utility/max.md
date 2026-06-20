# max

**Category**: Utility  
**Import**: `import { max } from 'rxjs';`

## Description

`max` emits a single value — the largest value emitted by the source Observable — when the source completes. For numbers, the default comparison uses the `>` operator. For non-numeric or custom types, you can provide a `comparer` function with the same contract as `Array.prototype.sort`'s comparator: return a negative number if `x < y`, positive if `x > y`, or zero if equal.

Like all aggregation operators, `max` only emits after the source completes. If the source is empty or never completes, `max` never emits.

## Signature

```typescript
function max<T>(comparer?: (x: T, y: T) => number): MonoTypeOperatorFunction<T>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| comparer | `(x: T, y: T) => number` | Optional. A comparator function. Should return a negative number if `x < y`, positive if `x > y`, 0 if equal. If omitted, uses native `>` comparison. |

## Return Type

`MonoTypeOperatorFunction<T>` — an operator that returns an Observable emitting the single maximum value from the source.

## Marble Diagram

```
Source:  --5--3--8--1--4--|
         max()
Output:  -----------------8|
```

## Examples

### Example 1: Find the maximum numeric value

```typescript
import { of, max } from 'rxjs';

of(5, 4, 7, 2, 8, 1).pipe(
  max()
).subscribe(x => console.log('Max:', x));

// Max: 8
```

### Example 2: Find the object with the highest property value

```typescript
import { of, max } from 'rxjs';

of(
  { name: 'Alice', score: 87 },
  { name: 'Bob', score: 92 },
  { name: 'Charlie', score: 78 }
).pipe(
  max((a, b) => a.score - b.score)
).subscribe(winner => console.log('High scorer:', winner.name));

// High scorer: Bob
```

### Example 3: Find the most recent date from a stream

```typescript
import { from, max } from 'rxjs';

const dates = [
  new Date('2024-01-15'),
  new Date('2024-06-20'),
  new Date('2023-12-01'),
  new Date('2024-03-08')
];

from(dates).pipe(
  max((a, b) => a.getTime() - b.getTime())
).subscribe(latest => console.log('Most recent:', latest.toDateString()));

// Most recent: Thu Jun 20 2024
```

## Common Pitfalls

- **Only emits on completion**: `max` waits for the source to complete before emitting. Never use it on an infinite observable.
- **Empty source emits nothing**: If the source completes without emitting any value, `max` also completes without emitting. Combine with `defaultIfEmpty` if you need a fallback.
- **Default comparison is not suitable for all types**: The default `>` comparison works for numbers and strings via JavaScript's coercion rules, but results are undefined for arbitrary objects. Always provide a `comparer` for non-primitive types.

## Related Operators

- `min` — finds the smallest value using the same API
- `reduce` — general-purpose aggregation; `max` is a specialization
- `count` — counts values rather than finding an extreme
- `defaultIfEmpty` — emit a fallback if the source completes empty
