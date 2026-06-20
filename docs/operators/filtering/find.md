# find

**Category**: Filtering  
**Import**: `import { find } from 'rxjs';`

## Description

`find` emits only the first value emitted by the source Observable that satisfies the given predicate, then completes. If the source completes without any value matching the predicate, `undefined` is emitted (and then the output completes) — no error is thrown.

This is the key behavioral difference from `first` with a predicate: `first` throws `EmptyError` when no match is found; `find` emits `undefined` instead. The predicate is required for `find`.

## Signature

```typescript
function find<T>(
  predicate: (value: T, index: number, source: Observable<T>) => boolean
): OperatorFunction<T, T | undefined>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| predicate | `(value: T, index: number, source: Observable<T>) => boolean` | A function to test each emitted value. The first value for which it returns `true` is emitted. |

## Return Type

`OperatorFunction<T, T | undefined>` — emits the first matching value, or `undefined` if no match is found.

## Marble Diagram

```
Source: --a--b--c--d--|
        find(x => x === 'c')
Output: --------c|

Source: --a--b--c--|
        find(x => x === 'z')
Output: -----------undefined|
```

## Examples

### Example 1: Find the first matching element in a list

```typescript
import { from } from 'rxjs';
import { find } from 'rxjs';

const users$ = from([
  { id: 1, name: 'Alice' },
  { id: 2, name: 'Bob' },
  { id: 3, name: 'Carol' }
]);

users$.pipe(
  find(user => user.name === 'Bob')
).subscribe(user => console.log('Found:', user));

// Logs: Found: { id: 2, name: 'Bob' }
```

### Example 2: Find the first click on a specific element

```typescript
import { fromEvent } from 'rxjs';
import { find } from 'rxjs';

const div = document.createElement('div');
div.style.cssText = 'width: 200px; height: 200px; background: #09c;';
document.body.appendChild(div);

fromEvent(document, 'click').pipe(
  find(ev => (ev.target as HTMLElement).tagName === 'DIV')
).subscribe(ev => console.log('Clicked the div:', ev));
```

### Example 3: Handle the no-match case

```typescript
import { of } from 'rxjs';
import { find, map } from 'rxjs';

const items$ = of(1, 2, 3, 4, 5);

items$.pipe(
  find(n => n > 10),
  map(n => n ?? 'not found')
).subscribe(result => console.log(result));

// Logs: not found
```

## Common Pitfalls

- **Returns `undefined` on no match**: Unlike `Array.prototype.find` returning `undefined`, this might be hard to distinguish from a valid `undefined` value in the stream. Use `findIndex` if you need to reliably detect the no-match case (-1 returned).
- **`find` vs `first`**: Both emit the first matching value and complete. The difference is when no match exists: `find` emits `undefined`; `first` (without a defaultValue) throws `EmptyError`.
- **Unsubscribes after first match**: `find` unsubscribes from the source as soon as a match is found, so it does not consume the entire stream unnecessarily.

## Related Operators

- `findIndex` — like `find` but emits the index instead of the value; returns `-1` on no match
- `first` — emits the first match but throws on no match (unless a default is given)
- `filter` — emits all matching values without completing
- `single` — asserts exactly one match exists
