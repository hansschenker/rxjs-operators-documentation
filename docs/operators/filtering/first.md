# first

**Category**: Filtering  
**Import**: `import { first } from 'rxjs';`

## Description

`first` emits only the first value emitted by the source Observable (or the first value that passes an optional predicate), then completes. If a `defaultValue` is provided and no matching value is found before the source completes, the default value is emitted instead of an error.

Unlike `take(1)`, `first` throws an `EmptyError` if the source completes without emitting any value (or any value matching the predicate) and no `defaultValue` was provided. This strict behavior makes `first` useful when you expect at least one value and want an explicit error if none arrives.

## Signature

```typescript
function first<T, D = T>(
  predicate?: ((value: T, index: number, source: Observable<T>) => boolean) | null,
  defaultValue?: D
): OperatorFunction<T, T | D>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| predicate | `(value: T, index: number, source: Observable<T>) => boolean` \| `null` | Optional. A function to test each value. Only the first value for which it returns `true` is emitted. |
| defaultValue | `D` | Optional. A default value to emit if no matching value is found before source completion. |

## Return Type

`OperatorFunction<T, T | D>` — emits exactly one value (the first match or the default), then completes.

## Marble Diagram

```
Source: --a--b--c--d--|
        first()
Output: --a|

Source: --a--b--c--d--|
        first(x => x === 'c')
Output: --------c|

Source: ----|
        first()          (no default)
Output: ----#            EmptyError
```

## Examples

### Example 1: Capture the very first user interaction

```typescript
import { fromEvent } from 'rxjs';
import { first } from 'rxjs';

const click$ = fromEvent(document, 'click');

click$.pipe(
  first()
).subscribe({
  next: ev => console.log('First click at:', (ev as MouseEvent).clientX, (ev as MouseEvent).clientY),
  complete: () => console.log('Done listening')
});
```

### Example 2: Get the first item matching a condition with a fallback

```typescript
import { from } from 'rxjs';
import { first } from 'rxjs';

const items$ = from([
  { id: 1, active: false },
  { id: 2, active: true },
  { id: 3, active: true }
]);

items$.pipe(
  first(item => item.active, { id: -1, active: false })
).subscribe(item => console.log('First active item:', item));

// Logs: First active item: { id: 2, active: true }
```

### Example 3: Handle the EmptyError when no default is given

```typescript
import { EMPTY } from 'rxjs';
import { first } from 'rxjs';

EMPTY.pipe(
  first()
).subscribe({
  next: v => console.log(v),
  error: err => console.error('Error:', err.name) // EmptyError
});
```

## Common Pitfalls

- **`first()` vs `take(1)`**: `take(1)` simply completes after one value and does not error on an empty source. Use `first()` when you need the guarantee that a value exists.
- **Providing a predicate without a default**: If the predicate never matches and no `defaultValue` is given, an `EmptyError` is thrown. Always consider providing a default when the match is not guaranteed.
- **Index in predicate**: The `index` argument to the predicate is the position among all emitted values, not just matching ones. Keep this in mind if you're using it for positional logic.

## Related Operators

- `take` — emits the first N values; does not error on empty source
- `last` — emits only the last matching value
- `find` — like `first` with a required predicate, but emits `undefined` instead of erroring
- `filter` — keeps all matching values open-endedly
