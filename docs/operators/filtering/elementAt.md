# elementAt

**Category**: Filtering  
**Import**: `import { elementAt } from 'rxjs';`

## Description

`elementAt` emits only the value at the specified zero-based index in the sequence of emissions from the source Observable, then completes. All other values are discarded.

If the source completes before emitting enough values to reach the specified index and no `defaultValue` was provided, an `ArgumentOutOfRangeError` is thrown. Providing a `defaultValue` prevents this error. Passing a negative `index` immediately throws `ArgumentOutOfRangeError` at construction time.

## Signature

```typescript
function elementAt<T, D = T>(index: number, defaultValue?: D): OperatorFunction<T, T | D>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| index | `number` | The zero-based index of the value to emit. Must be `>= 0`; a negative value throws `ArgumentOutOfRangeError` immediately. |
| defaultValue | `D` | Optional. Emitted instead of an error when the source completes before reaching `index`. |

## Return Type

`OperatorFunction<T, T | D>` — emits exactly one value (the item at `index` or the default), then completes.

## Marble Diagram

```
Source: --a--b--c--d--e--|
        elementAt(2)
Output: --------c|

Source: --a--b--|
        elementAt(5)         (no default)
Output: --------#  ArgumentOutOfRangeError
```

## Examples

### Example 1: Get the third click event

```typescript
import { fromEvent } from 'rxjs';
import { elementAt } from 'rxjs';

fromEvent(document, 'click').pipe(
  elementAt(2) // zero-based, so this is the 3rd click
).subscribe({
  next: ev => console.log('Third click:', ev),
  complete: () => console.log('Done')
});
```

### Example 2: Get a specific page from a paginated source

```typescript
import { from } from 'rxjs';
import { elementAt } from 'rxjs';

const pages$ = from(['page0', 'page1', 'page2', 'page3', 'page4']);

pages$.pipe(
  elementAt(3)
).subscribe(page => console.log('Page 3:', page));

// Logs: Page 3: page3
```

### Example 3: Use a default value when the stream is too short

```typescript
import { of } from 'rxjs';
import { elementAt } from 'rxjs';

of('a', 'b', 'c').pipe(
  elementAt(10, 'not found')
).subscribe(v => console.log(v));

// Logs: not found
```

## Common Pitfalls

- **Negative index throws immediately**: Unlike most operators, `elementAt` throws synchronously if `index < 0` — it doesn't wait until subscription.
- **`ArgumentOutOfRangeError` without a default**: If the stream completes before reaching the index, and no `defaultValue` is given, the subscriber receives an error. Always provide a default when the stream length is uncertain.
- **Not the same as `skip(n).take(1)`**: The behavior is equivalent in most cases, but `elementAt` provides the `defaultValue` / error behavior, making intent clearer.

## Related Operators

- `first` — emits the first (optionally matching) value; throws `EmptyError` on empty source
- `last` — emits the last value
- `take` — emits the first N values
- `skip` — skips the first N values
- `single` — asserts exactly one matching value exists
