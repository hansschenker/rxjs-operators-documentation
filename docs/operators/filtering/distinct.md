# distinct

**Category**: Filtering  
**Import**: `import { distinct } from 'rxjs';`

## Description

`distinct` emits only values that have not been seen before over the entire lifetime of the subscription. It maintains an internal `Set` of all previously seen keys and suppresses any value whose key is already in the set.

An optional `keySelector` function extracts a comparison key from each value, allowing object-based distinctness checks. An optional `flushes` Observable can be used to clear the internal `Set`, effectively resetting the "seen" history.

Note that `distinct` remembers **all** previously seen values for the lifetime of the subscription. For long-running streams this can be a memory concern. Use `distinctUntilChanged` if you only need to suppress consecutive duplicates.

## Signature

```typescript
function distinct<T, K>(
  keySelector?: (value: T) => K,
  flushes?: ObservableInput<any>
): MonoTypeOperatorFunction<T>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| keySelector | `(value: T) => K` | Optional. A function to extract the comparison key from each value. Defaults to the value itself. |
| flushes | `ObservableInput<any>` | Optional. An Observable that, when it emits, clears the internal seen-values Set. |

## Return Type

`MonoTypeOperatorFunction<T>` — emits only values that have not previously been emitted.

## Marble Diagram

```
Source: --a--b--a--c--b--d--|
        distinct()
Output: --a--b-----c-----d--|
```

## Examples

### Example 1: Deduplicate a stream of numbers

```typescript
import { of } from 'rxjs';
import { distinct } from 'rxjs';

of(1, 1, 2, 2, 2, 1, 2, 3, 4, 3, 2, 1).pipe(
  distinct()
).subscribe(n => console.log(n));

// Logs: 1, 2, 3, 4
```

### Example 2: Deduplicate objects by a key

```typescript
import { of } from 'rxjs';
import { distinct } from 'rxjs';

of(
  { id: 1, name: 'Alice' },
  { id: 2, name: 'Bob' },
  { id: 1, name: 'Alice (duplicate)' },
  { id: 3, name: 'Carol' }
).pipe(
  distinct(user => user.id)
).subscribe(user => console.log(user));

// Logs:
// { id: 1, name: 'Alice' }
// { id: 2, name: 'Bob' }
// { id: 3, name: 'Carol' }
```

### Example 3: Reset seen values periodically with a flush

```typescript
import { Subject, interval } from 'rxjs';
import { distinct } from 'rxjs';

const values$ = new Subject<number>();
const flush$ = interval(5000); // clear seen values every 5 seconds

values$.pipe(
  distinct(v => v, flush$)
).subscribe(v => console.log('Distinct value:', v));

values$.next(1); // emitted
values$.next(1); // suppressed
values$.next(2); // emitted

// After 5 seconds, Set is cleared...
// values$.next(1) would be emitted again
```

## Common Pitfalls

- **Memory growth on long-running streams**: The internal `Set` grows indefinitely as new distinct values arrive. For streams that produce many unique values, use `distinctUntilChanged` (which only checks consecutive pairs) or use the `flushes` parameter.
- **`distinct` vs `distinctUntilChanged`**: `distinct` suppresses any previously seen value; `distinctUntilChanged` only suppresses a value if it is identical to the immediately preceding emission. `1,2,1` through `distinct()` yields `1,2`; through `distinctUntilChanged()` it yields `1,2,1`.
- **Reference equality for objects**: Without a `keySelector`, object identity (`===`) is used. Two objects with the same content are not considered equal unless they are the same reference.

## Related Operators

- `distinctUntilChanged` — suppresses only consecutive duplicates
- `distinctUntilKeyChanged` — `distinctUntilChanged` specialized for a single object property
- `filter` — general-purpose value filtering
