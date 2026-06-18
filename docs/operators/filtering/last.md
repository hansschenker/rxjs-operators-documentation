# last

**Category**: Filtering  
**Import**: `import { last } from 'rxjs';`

## Description

`last` waits for the source Observable to complete, then emits only the last value it produced (or the last value that satisfied an optional predicate). It buffers nothing until completion, simply tracking the most recently seen matching value.

If the source completes without emitting any value that matches the predicate (or any value at all when no predicate is given), and no `defaultValue` was provided, an `EmptyError` is delivered to the error handler. Providing a `defaultValue` prevents this error and emits the fallback instead.

## Signature

```typescript
function last<T, D = T>(
  predicate?: ((value: T, index: number, source: Observable<T>) => boolean) | null,
  defaultValue?: D
): OperatorFunction<T, T | D>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| predicate | `(value: T, index: number, source: Observable<T>) => boolean` \| `null` | Optional. A condition any source-emitted value must satisfy. Only the final value passing this test is emitted. |
| defaultValue | `D` | Optional. Emitted when no matching value was found. Prevents `EmptyError`. |

## Return Type

`OperatorFunction<T, T | D>` — emits exactly one value on source completion, or errors with `EmptyError`.

## Marble Diagram

```
Source: --a--b--c--d--|
        last()
Output: --------------d|

Source: --a--b--c--d--|
        last(x => x < 'd')
Output: --------------c|

Source: ----|
        last()          (no default)
Output: ----#            EmptyError
```

## Examples

### Example 1: Get the last letter emitted by a sequence

```typescript
import { from } from 'rxjs';
import { last } from 'rxjs';

const letters$ = from(['x', 'y', 'z']);

letters$.pipe(
  last()
).subscribe(letter => console.log('Last letter:', letter));

// Logs: Last letter: z
```

### Example 2: Get the last form submission matching a condition

```typescript
import { Subject } from 'rxjs';
import { last, takeUntil } from 'rxjs';

const formSubmit$ = new Subject<{ valid: boolean; value: string }>();
const destroy$ = new Subject<void>();

formSubmit$.pipe(
  takeUntil(destroy$),
  last(submission => submission.valid, { valid: false, value: '' })
).subscribe(submission => console.log('Last valid submission:', submission));

formSubmit$.next({ valid: false, value: 'bad' });
formSubmit$.next({ valid: true, value: 'good' });
formSubmit$.next({ valid: true, value: 'better' });
destroy$.next(); // triggers completion

// Logs: Last valid submission: { valid: true, value: 'better' }
```

### Example 3: Handle EmptyError with a default value

```typescript
import { from } from 'rxjs';
import { last } from 'rxjs';

const numbers$ = from([1, 2, 3]);

numbers$.pipe(
  last(n => n > 10, -1)
).subscribe(n => console.log('Result:', n));

// Logs: Result: -1  (no number > 10 exists)
```

## Common Pitfalls

- **Never emits from infinite streams**: `last` waits for completion. An Observable that never completes means `last` will never emit. Pair with `takeUntil` or `take` to bound infinite sources.
- **EmptyError without a default**: When the source is empty or no values match the predicate and no `defaultValue` is given, the subscription receives an error. Always consider providing a default when emissivity is uncertain.
- **Memory usage**: `last` only stores the single most-recent matching value — it does not buffer the entire stream, so memory usage is constant.

## Related Operators

- `first` — emits only the first matching value
- `takeLast` — emits the last N values instead of just one
- `skipLast` — skips the last N values
- `filter` — emits all matching values, not just the last
