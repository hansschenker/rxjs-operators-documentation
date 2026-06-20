# ignoreElements

**Category**: Filtering  
**Import**: `import { ignoreElements } from 'rxjs';`

## Description

`ignoreElements` suppresses all `next` emissions from the source Observable and passes through only the terminal notifications: `complete` and `error`. The output Observable never calls its subscriber's `next` handler.

This is useful when you care only about whether an Observable finishes (successfully or with an error), not about the values it produces — for example, waiting for a background task to complete before proceeding.

## Signature

```typescript
function ignoreElements(): OperatorFunction<unknown, never>
```

## Parameters

None.

## Return Type

`OperatorFunction<unknown, never>` — the output Observable emits no values (`never`), only completion or error.

## Marble Diagram

```
Source: --a--b--c--|
        ignoreElements()
Output: -----------|

Source: --a--b--#
        ignoreElements()
Output: ---------#
```

## Examples

### Example 1: Wait for a task to complete without caring about its output

```typescript
import { of } from 'rxjs';
import { ignoreElements } from 'rxjs';

of('processing...', 'still going...', 'done').pipe(
  ignoreElements()
).subscribe({
  next: () => { /* never called */ },
  error: err => console.error('Task failed:', err),
  complete: () => console.log('Task completed!')
});

// Logs: Task completed!
```

### Example 2: Use as a signal that an HTTP request finished

```typescript
import { from } from 'rxjs';
import { ignoreElements, concat, of } from 'rxjs';

// Simulate an HTTP request
const saveRequest$ = from(fetch('/api/save', { method: 'POST', body: '{}' }));

// Only show "Saved!" after the request completes, ignore the response body
saveRequest$.pipe(
  ignoreElements()
).subscribe({
  error: err => console.error('Save failed:', err),
  complete: () => console.log('Saved!')
});
```

### Example 3: Combine with other operators to create a side-effect-only stream

```typescript
import { interval } from 'rxjs';
import { ignoreElements, tap, take } from 'rxjs';

// Log 5 values as a side effect, then signal completion
interval(1000).pipe(
  take(5),
  tap(n => console.log('Side effect:', n)),
  ignoreElements()
).subscribe({
  complete: () => console.log('All side effects done')
});
```

## Common Pitfalls

- **Errors still propagate**: `ignoreElements` does not suppress errors. An error from the source will still be delivered to the subscriber's `error` handler. Use `catchError` if you need to handle or suppress errors.
- **The return type is `never` for values**: TypeScript will infer the output type as `Observable<never>`. This is correct because no `next` values will ever arrive, but it can cause type errors if you try to use the output values downstream.
- **Use for side-effect pipelines**: The most common use is signaling completion. Avoid using it to suppress values you actually need — use `filter` for that.

## Related Operators

- `filter` — selectively passes values based on a predicate
- `take` — limits the number of values then completes
- `catchError` — handles errors in the stream
