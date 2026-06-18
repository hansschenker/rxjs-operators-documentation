# sequenceEqual

**Category**: Utility  
**Import**: `import { sequenceEqual } from 'rxjs';`

## Description

`sequenceEqual` compares all values of the source Observable with all values of a second `ObservableInput`, in order, and emits a single boolean: `true` if the sequences are equal in length and every corresponding pair of values matches, `false` otherwise.

The operator subscribes to both the source and the `compareTo` observable simultaneously, buffering values from whichever arrives first. When both have completed with matching buffers, it emits `true`. If any value pair does not match, or if one completes while the other still has buffered values, it emits `false` immediately.

An optional `comparator` function can replace the default strict equality (`===`) check.

## Signature

```typescript
function sequenceEqual<T>(
  compareTo: ObservableInput<T>,
  comparator?: (a: T, b: T) => boolean
): OperatorFunction<T, boolean>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| compareTo | `ObservableInput<T>` | The sequence to compare against the source. |
| comparator | `(a: T, b: T) => boolean` | Optional. A function to compare pairs of values. Defaults to strict equality (`===`). |

## Return Type

`OperatorFunction<T, boolean>` — an operator that returns an Observable emitting a single `boolean`.

## Marble Diagram

```
Source:  --a--b--c--|
Compare: --a--b--c--|
         sequenceEqual(compare$)
Output:  -----------true|

Source:  --a--b--c--|
Compare: --a--x--c--|
         sequenceEqual(compare$)
Output:  -----false|  (mismatch detected at 'b'/'x')
```

## Examples

### Example 1: Check if user entered the Konami code

```typescript
import { from, fromEvent, map, bufferCount, mergeMap, sequenceEqual } from 'rxjs';

const konamiCode = from([
  'ArrowUp', 'ArrowUp', 'ArrowDown', 'ArrowDown',
  'ArrowLeft', 'ArrowRight', 'ArrowLeft', 'ArrowRight',
  'KeyB', 'KeyA', 'Enter'
]);

const keyPresses$ = fromEvent<KeyboardEvent>(document, 'keyup').pipe(
  map(e => e.code)
);

keyPresses$.pipe(
  bufferCount(11, 1),
  mergeMap(last11 => from(last11).pipe(sequenceEqual(konamiCode)))
).subscribe(matched => {
  if (matched) console.log('Cheat code activated!');
});
```

### Example 2: Compare two observable streams for equality

```typescript
import { of, sequenceEqual } from 'rxjs';

const source1$ = of(1, 2, 3, 4, 5);
const source2$ = of(1, 2, 3, 4, 5);

source1$.pipe(
  sequenceEqual(source2$)
).subscribe(equal => {
  console.log('Sequences are equal:', equal);
});

// Sequences are equal: true
```

### Example 3: Compare objects using a custom comparator

```typescript
import { of, sequenceEqual } from 'rxjs';

const actual$ = of({ id: 1, name: 'Alice' }, { id: 2, name: 'Bob' });
const expected$ = of({ id: 1, name: 'Alice' }, { id: 2, name: 'Bob' });

actual$.pipe(
  sequenceEqual(
    expected$,
    (a, b) => a.id === b.id && a.name === b.name
  )
).subscribe(match => console.log('Deep equal:', match));

// Deep equal: true
```

## Common Pitfalls

- **Both sources must complete**: `sequenceEqual` waits for both observables to complete before it can determine equality. If either never completes, the operator never emits.
- **Order matters**: The comparison is sequential. Two observables with the same values in a different order will produce `false`.
- **Buffering**: Values are buffered until a matching pair is available from the other side. If one source is much faster than the other, the buffer can grow. For very large or unbounded sequences this can cause memory issues.
- **Only one emission**: `sequenceEqual` always emits exactly one boolean and then completes.

## Related Operators

- `every` — checks that all values from a single stream satisfy a predicate
- `combineLatest` / `zip` — pair values from multiple streams without a boolean equality check
