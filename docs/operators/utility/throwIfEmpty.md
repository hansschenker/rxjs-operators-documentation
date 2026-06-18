# throwIfEmpty

**Category**: Utility  
**Import**: `import { throwIfEmpty } from 'rxjs';`

## Description

`throwIfEmpty` throws an error if the source Observable completes without emitting any `next` value. If the source does emit at least one value, it is mirrored transparently. This is useful for making "at least one result expected" semantics explicit in your pipeline — for example, when you require an HTTP response to contain data, or when a `filter` must match at least one item.

By default, `throwIfEmpty` throws an `EmptyError`. You can provide an `errorFactory` function to throw a custom error with a more descriptive message.

## Signature

```typescript
function throwIfEmpty<T>(errorFactory?: () => any): MonoTypeOperatorFunction<T>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| errorFactory | `() => any` | Optional. A factory function called to produce the error when the source completes empty. Defaults to `() => new EmptyError()`. |

## Return Type

`MonoTypeOperatorFunction<T>` — an operator that mirrors the source and errors if it completes empty.

## Marble Diagram

```
Source:  --|     (completes with no values)
         throwIfEmpty()
Output:  --X     (EmptyError)

Source:  --a--b--|
         throwIfEmpty()
Output:  --a--b--|  (values pass through unchanged)
```

## Examples

### Example 1: Throw if no click happens within a time window

```typescript
import { fromEvent, takeUntil, timer, throwIfEmpty } from 'rxjs';

const click$ = fromEvent(document, 'click');

click$.pipe(
  takeUntil(timer(5000)),
  throwIfEmpty(() => new Error('No click received within 5 seconds'))
).subscribe({
  next: () => console.log('Clicked!'),
  error: err => console.error(err.message)
});
```

### Example 2: Require at least one matching item after a filter

```typescript
import { from, filter, throwIfEmpty } from 'rxjs';

const items = [2, 4, 6, 8, 10];

from(items).pipe(
  filter(n => n > 15), // no items > 15
  throwIfEmpty(() => new RangeError('No items exceeded 15'))
).subscribe({
  next: n => console.log(n),
  error: err => console.error(err.message)
});

// RangeError: No items exceeded 15
```

### Example 3: Guard against empty API responses

```typescript
import { from, throwIfEmpty, map } from 'rxjs';

function fetchUsers(): Promise<any[]> {
  return fetch('/api/users').then(r => r.json());
}

from(fetchUsers()).pipe(
  // Flatten array items into individual emissions
  (source$) => source$.pipe(
    map(arr => arr as any[])
  ),
  throwIfEmpty(() => new Error('API returned no users'))
).subscribe({
  next: users => console.log('Users:', users),
  error: err => console.error(err.message)
});
```

## Common Pitfalls

- **Only checks for `next` emissions**: `throwIfEmpty` fires on completion without prior `next` values. If your observable errors before completing, `throwIfEmpty` passes that error through without adding its own.
- **Works with `defaultIfEmpty` as an alternative**: If you'd rather emit a fallback than throw an error, use `defaultIfEmpty` instead.
- **Custom error factory is called lazily**: The `errorFactory` is not called until an empty completion is detected, so it is safe to put expensive logic there.

## Related Operators

- `defaultIfEmpty` — emit a fallback value when the source is empty, instead of throwing
- `isEmpty` — check emptiness and get a boolean result without throwing
- `filter` — reduce values; `throwIfEmpty` is often used right after `filter`
