# single

**Category**: Filtering  
**Import**: `import { single } from 'rxjs';`

## Description

`single` asserts that the source Observable emits exactly one value (or exactly one value matching an optional predicate). It waits for the source to complete, then emits that single matching value.

If any of the following are true, an error is delivered instead:

- The source completes without emitting any value — throws `EmptyError`
- No value matches the predicate — throws `NotFoundError`
- More than one value matches the predicate (or more than one value is emitted when no predicate is given) — throws `SequenceError`

This makes `single` a strict assertion operator, useful when correctness requires exactly one match.

## Signature

```typescript
function single<T>(
  predicate?: (value: T, index: number, source: Observable<T>) => boolean
): MonoTypeOperatorFunction<T>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| predicate | `(value: T, index: number, source: Observable<T>) => boolean` | Optional. A condition to test each value. If omitted, the source must emit exactly one value total. |

## Return Type

`MonoTypeOperatorFunction<T>` — emits exactly one matching value, or errors.

## Marble Diagram

```
Source: --a--b--c--|
        single(x => x === 'b')
Output: -----------b|

Source: --a--b--c--|
        single()          (more than one value)
Output: -----------#  SequenceError

Source: ----|
        single()          (no value)
Output: ----#  EmptyError
```

## Examples

### Example 1: Verify a list has exactly one matching item

```typescript
import { of } from 'rxjs';
import { single } from 'rxjs';

of(
  { name: 'Alice', role: 'admin' },
  { name: 'Bob', role: 'user' },
  { name: 'Carol', role: 'user' }
).pipe(
  single(u => u.role === 'admin')
).subscribe({
  next: admin => console.log('Admin:', admin.name),
  error: err => console.error('Error:', err.message)
});

// Logs: Admin: Alice
```

### Example 2: Error when multiple items match

```typescript
import { of } from 'rxjs';
import { single } from 'rxjs';

of('Ben', 'Tracy', 'Bradley', 'Lincoln').pipe(
  single(name => name.startsWith('B'))
).subscribe({
  next: v => console.log(v),
  error: err => console.error(err.message) // SequenceError: Too many matching values
});
```

### Example 3: Error when no items match

```typescript
import { of } from 'rxjs';
import { single } from 'rxjs';

of('Laney', 'Tracy', 'Lily').pipe(
  single(name => name.startsWith('Z'))
).subscribe({
  next: v => console.log(v),
  error: err => console.error(err.message) // NotFoundError: No matching values
});
```

## Common Pitfalls

- **Three distinct error types**: `single` can throw `EmptyError`, `NotFoundError`, or `SequenceError`. Always attach an error handler when using this operator.
- **Waits for completion**: `single` buffers the matching value and only emits it on source completion. It will not emit early even when two values match (it needs to verify no more will arrive).
- **Differs from `first`**: `first` returns the first match and unsubscribes immediately; `single` must see the entire stream to confirm uniqueness.

## Related Operators

- `first` — emits the first match without checking for uniqueness
- `find` — emits the first match as `undefined` instead of erroring
- `filter` — emits all matching values
- `elementAt` — emits the value at a specific index
