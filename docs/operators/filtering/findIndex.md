# findIndex

**Category**: Filtering  
**Import**: `import { findIndex } from 'rxjs';`

## Description

`findIndex` emits the zero-based index of the first value emitted by the source Observable that satisfies the given predicate, then completes. If the source completes without any value matching the predicate, `-1` is emitted (and then the output completes) — no error is thrown.

It mirrors the behavior of `Array.prototype.findIndex`, adapted for Observables. Like `find`, it unsubscribes from the source as soon as the first match is found.

## Signature

```typescript
function findIndex<T>(
  predicate: (value: T, index: number, source: Observable<T>) => boolean
): OperatorFunction<T, number>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| predicate | `(value: T, index: number, source: Observable<T>) => boolean` | A function to test each emitted value. The index of the first value for which it returns `true` is emitted. |

## Return Type

`OperatorFunction<T, number>` — emits the index of the first matching value, or `-1` if no match is found.

## Marble Diagram

```
Source: --a--b--c--d--|
        findIndex(x => x === 'c')
Output: --------2|

Source: --a--b--c--|
        findIndex(x => x === 'z')
Output: -----------(-1)|
```

## Examples

### Example 1: Find the position of a specific item

```typescript
import { from } from 'rxjs';
import { findIndex } from 'rxjs';

const fruits$ = from(['apple', 'banana', 'cherry', 'date']);

fruits$.pipe(
  findIndex(fruit => fruit === 'cherry')
).subscribe(index => console.log('cherry is at index:', index));

// Logs: cherry is at index: 2
```

### Example 2: Detect which click was on a target element

```typescript
import { fromEvent } from 'rxjs';
import { findIndex } from 'rxjs';

const div = document.createElement('div');
document.body.appendChild(div);

fromEvent(document, 'click').pipe(
  findIndex(ev => (ev.target as HTMLElement).tagName === 'DIV')
).subscribe(index => {
  if (index === -1) {
    console.log('No click on div detected before stream ended');
  } else {
    console.log(`Click #${index} was on the div`);
  }
});
```

### Example 3: Use the index to process surrounding values

```typescript
import { from, zip } from 'rxjs';
import { findIndex, switchMap } from 'rxjs';

const items = ['alpha', 'beta', 'gamma', 'delta'];
const items$ = from(items);

items$.pipe(
  findIndex(item => item === 'gamma')
).subscribe(idx => {
  if (idx >= 0) {
    console.log('Found at:', idx);
    console.log('Previous item:', items[idx - 1]);
    console.log('Next item:', items[idx + 1]);
  }
});

// Logs:
// Found at: 2
// Previous item: beta
// Next item: delta
```

## Common Pitfalls

- **-1 means no match**: Check for `-1` explicitly in subscribers; it does not cause an error.
- **Index counts all source emissions**: The `index` argument in the predicate is the position among all emitted values (zero-based), whether or not they match. This is also what's emitted when a match is found.
- **`findIndex` vs `find`**: Use `findIndex` when you need the position (e.g., for array lookups), `find` when you need the value itself.

## Related Operators

- `find` — like `findIndex` but emits the value instead of the index
- `first` — emits the first matching value and throws on no match
- `elementAt` — emits the value at a specific known index
- `filter` — emits all matching values
