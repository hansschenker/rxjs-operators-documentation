# defaultIfEmpty

**Category**: Utility  
**Import**: `import { defaultIfEmpty } from 'rxjs';`

## Description

`defaultIfEmpty` emits a specified default value if the source Observable completes without emitting any `next` value. If the source does emit at least one value, all values pass through unchanged and the default is never emitted.

This is useful as a safe fallback for potentially empty streams — for example, when a search might return no results, when a `filter` might match nothing, or when an API might return an empty collection.

## Signature

```typescript
function defaultIfEmpty<T, R>(defaultValue: R): OperatorFunction<T, T | R>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| defaultValue | `R` | The value to emit if the source Observable completes without emitting anything. |

## Return Type

`OperatorFunction<T, T | R>` — an operator that returns an Observable emitting the source values, or the `defaultValue` if the source was empty.

## Marble Diagram

```
Source:  --|     (empty)
         defaultIfEmpty('nothing')
Output:  --'nothing'|

Source:  --a--b--|
         defaultIfEmpty('nothing')
Output:  --a--b--|  (default is NOT emitted)
```

## Examples

### Example 1: Show a "no results" message for an empty search

```typescript
import { from, filter, defaultIfEmpty } from 'rxjs';

const searchResults = ['apple', 'apricot', 'avocado'];
const query = 'banana';

from(searchResults).pipe(
  filter(item => item.startsWith(query)),
  defaultIfEmpty(`No results for "${query}"`)
).subscribe(result => console.log(result));

// No results for "banana"
```

### Example 2: Provide fallback for a click that never happened

```typescript
import { fromEvent, takeUntil, interval, defaultIfEmpty } from 'rxjs';

const click$ = fromEvent(document, 'click');

click$.pipe(
  takeUntil(interval(5000)),
  defaultIfEmpty('no clicks')
).subscribe(result => {
  console.log(result); // 'no clicks' if nobody clicked in 5 seconds
});
```

### Example 3: Return a default API response for empty collections

```typescript
import { from, mergeMap, defaultIfEmpty, map } from 'rxjs';

interface User { id: number; name: string; }

function getActiveUsers(): Promise<User[]> {
  return fetch('/api/users/active').then(r => r.json());
}

from(getActiveUsers()).pipe(
  mergeMap(users => from(users)),
  defaultIfEmpty({ id: 0, name: 'Guest' } as User)
).subscribe(user => {
  console.log('User:', user.name);
});
// If no active users: User: Guest
```

## Common Pitfalls

- **Default is only emitted on completion, not on error**: If the source errors before completing, `defaultIfEmpty` does not emit the default value; the error propagates normally.
- **Type widening**: `defaultIfEmpty` widens the output type to `T | R`. If `R` is different from `T`, downstream code must handle both types. Use the same type for `defaultValue` to avoid union types.
- **Does not prevent errors**: `defaultIfEmpty` only handles the "empty and completed" case. For error handling, use `catchError`.

## Related Operators

- `throwIfEmpty` — throw an error instead of providing a fallback when the source is empty
- `isEmpty` — emit a boolean indicating whether the source was empty, without changing the value type
- `first` — throws `EmptyError` by default if the source is empty; use `first(null, defaultValue)` as an alternative
