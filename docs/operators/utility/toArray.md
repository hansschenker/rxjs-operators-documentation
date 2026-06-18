# toArray

**Category**: Utility  
**Import**: `import { toArray } from 'rxjs';`

## Description

`toArray` collects all values emitted by the source Observable into an array and emits that array as a single value when the source completes. If the source errors, no array is emitted and the error propagates normally.

This operator is the Observable equivalent of `Array.from()` applied to a stream. It is useful when you need to work with an entire set of values at once — for example, to sort, deduplicate, or pass to an API that expects an array — and you know the source will complete.

Because `toArray` waits for completion before emitting, it will never emit for infinite observables.

## Signature

```typescript
function toArray<T>(): OperatorFunction<T, T[]>
```

## Parameters

None.

## Return Type

`OperatorFunction<T, T[]>` — an operator that returns an Observable that emits a single array containing all values from the source.

## Marble Diagram

```
Source:  --a--b--c--d--|
         toArray()
Output:  --------------([a,b,c,d])|
```

## Examples

### Example 1: Collect paginated results into a single array

```typescript
import { from, concatMap, toArray } from 'rxjs';

const pageNumbers$ = from([1, 2, 3]);

pageNumbers$.pipe(
  concatMap(page => from(fetchPage(page))),
  toArray()
).subscribe(allItems => {
  console.log('All items:', allItems);
});

function fetchPage(page: number) {
  return Promise.resolve([`item-${page}-1`, `item-${page}-2`]);
}
// All items: ['item-1-1', 'item-1-2', 'item-2-1', ...]
```

### Example 2: Sort stream values after all are emitted

```typescript
import { of, toArray, map } from 'rxjs';

of(5, 3, 8, 1, 4).pipe(
  toArray(),
  map(arr => [...arr].sort((a, b) => a - b))
).subscribe(sorted => console.log('Sorted:', sorted));

// Sorted: [1, 3, 4, 5, 8]
```

### Example 3: Collect timed interval values

```typescript
import { interval, take, toArray } from 'rxjs';

interval(200).pipe(
  take(5),
  toArray()
).subscribe(values => {
  console.log('Collected after 1 second:', values);
});

// Collected after 1 second: [0, 1, 2, 3, 4]
```

## Common Pitfalls

- **Never emits for infinite observables**: `toArray` waits for the source to complete. If your source never completes (e.g., `interval()` without `take()`), `toArray` will never emit and the array will grow unboundedly in memory.
- **Error prevents array emission**: If the source errors partway through, no array is emitted. Use `catchError` before `toArray` if partial results are acceptable.
- **Memory**: All emitted values are held in memory until completion. Avoid for very large or long-lived streams.

## Related Operators

- `reduce` — generalized accumulation; `toArray` is implemented as `reduce((arr, v) => (arr.push(v), arr), [])`
- `bufferCount` / `bufferTime` — collect values into arrays at intervals rather than waiting for completion
- `count` — count the number of emissions without collecting the values
