# min

**Category**: Utility  
**Import**: `import { min } from 'rxjs';`

## Description

`min` emits a single value — the smallest value emitted by the source Observable — when the source completes. For numbers, the default comparison uses the `<` operator. For non-numeric or custom types, you can provide a `comparer` function: return a negative number if `x < y`, positive if `x > y`, or zero if equal.

Like all aggregation operators, `min` only emits after the source completes. If the source is empty or never completes, `min` never emits.

## Signature

```typescript
function min<T>(comparer?: (x: T, y: T) => number): MonoTypeOperatorFunction<T>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| comparer | `(x: T, y: T) => number` | Optional. A comparator function. Should return a negative number if `x < y`, positive if `x > y`, 0 if equal. If omitted, uses native `<` comparison. |

## Return Type

`MonoTypeOperatorFunction<T>` — an operator that returns an Observable emitting the single minimum value from the source.

## Marble Diagram

```
Source:  --5--3--8--1--4--|
         min()
Output:  -----------------1|
```

## Examples

### Example 1: Find the minimum numeric value

```typescript
import { of, min } from 'rxjs';

of(5, 4, 7, 2, 8, 1).pipe(
  min()
).subscribe(x => console.log('Min:', x));

// Min: 1
```

### Example 2: Find the object with the lowest property value

```typescript
import { of, min } from 'rxjs';

of(
  { name: 'Alice', score: 87 },
  { name: 'Bob', score: 92 },
  { name: 'Charlie', score: 78 }
).pipe(
  min((a, b) => a.score - b.score)
).subscribe(lowest => console.log('Lowest scorer:', lowest.name));

// Lowest scorer: Charlie
```

### Example 3: Find the earliest date in a stream

```typescript
import { from, min } from 'rxjs';

const timestamps = [
  new Date('2024-06-20'),
  new Date('2024-01-15'),
  new Date('2023-12-01'),
  new Date('2024-03-08')
];

from(timestamps).pipe(
  min((a, b) => a.getTime() - b.getTime())
).subscribe(earliest => console.log('Earliest:', earliest.toDateString()));

// Earliest: Fri Dec 01 2023
```

## Common Pitfalls

- **Only emits on completion**: `min` is an aggregation operator that must see all values before it can determine the minimum. Never use it on an infinite observable.
- **Empty source emits nothing**: If the source completes without emitting any value, `min` also completes without emitting. Combine with `defaultIfEmpty` if you need a fallback.
- **Default comparison is coercive**: The default `<` operator works well for numbers and strings, but produces unpredictable results for objects. Always provide a `comparer` for non-primitive types.

## Related Operators

- `max` — finds the largest value using the same API
- `reduce` — general-purpose aggregation; `min` is a specialization
- `count` — counts values rather than finding an extreme
- `defaultIfEmpty` — emit a fallback if the source completes empty
