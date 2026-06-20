# isEmpty

**Category**: Utility  
**Import**: `import { isEmpty } from 'rxjs';`

## Description

`isEmpty` emits `false` if the source Observable emits any value, or emits `true` if the source completes without emitting any value. As soon as the first value arrives, `isEmpty` emits `false` and completes — it does not wait for the source to complete in that case.

This makes `isEmpty` more efficient than `count() === 0` for checking emptiness, because it short-circuits on the very first emission rather than accumulating a full count.

## Signature

```typescript
function isEmpty<T>(): OperatorFunction<T, boolean>
```

## Parameters

None.

## Return Type

`OperatorFunction<T, boolean>` — an operator that returns an Observable emitting a single boolean.

## Marble Diagram

```
Source:  --|       (completes immediately, no values)
         isEmpty()
Output:  --true|

Source:  --a--b--|
         isEmpty()
Output:  --false|  (emits false on first value, completes)
```

## Examples

### Example 1: Check if a filter leaves any results

```typescript
import { from, filter, isEmpty } from 'rxjs';

const items = [1, 3, 5, 7, 9];

from(items).pipe(
  filter(n => n % 2 === 0), // no even numbers in this set
  isEmpty()
).subscribe(empty => {
  console.log('No even numbers:', empty);
});

// No even numbers: true
```

### Example 2: Show a "no results" message

```typescript
import { Subject, isEmpty, tap } from 'rxjs';

const searchResults$ = new Subject<string>();

searchResults$.pipe(
  isEmpty()
).subscribe(noResults => {
  if (noResults) {
    console.log('No results found. Try a different search term.');
  }
});

searchResults$.complete(); // trigger: source completed without emitting
// No results found. Try a different search term.
```

### Example 3: Guard against processing an empty stream

```typescript
import { EMPTY, isEmpty, switchMap, of } from 'rxjs';

function process(source$: any) {
  return source$.pipe(
    isEmpty(),
    switchMap(empty => {
      if (empty) {
        return of({ error: 'Nothing to process' });
      }
      return source$; // resubscribe (assuming cold observable)
    })
  );
}

process(EMPTY).subscribe(console.log);
// { error: 'Nothing to process' }
```

## Common Pitfalls

- **Emits `false` immediately on first value**: `isEmpty` does not wait for the source to complete if a value arrives. Any values after the first are ignored. Do not rely on `isEmpty` to drain the source.
- **Must complete to emit `true`**: For `isEmpty` to emit `true`, the source must complete. An infinite observable that never emits and never completes will cause `isEmpty` to also never emit.
- **Not the same as `filter`**: `isEmpty` is a boolean check on the stream as a whole, not a per-value filter.

## Related Operators

- `count` — counts all values; less efficient than `isEmpty` for the zero-check because it waits for completion
- `defaultIfEmpty` — emit a fallback value if the source is empty, without producing a boolean
- `throwIfEmpty` — throw an error if the source completes empty
- `every` — generalized boolean check with a predicate per value
