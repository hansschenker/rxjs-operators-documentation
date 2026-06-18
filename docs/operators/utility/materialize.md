# materialize

**Category**: Utility  
**Import**: `import { materialize } from 'rxjs';`

## Description

`materialize` wraps every notification from the source Observable — `next`, `error`, and `complete` — into an `ObservableNotification` object and emits those wrapper objects as `next` values on the output Observable. This effectively "materializes" the notification protocol into a stream of plain values.

Each notification object has a `kind` property:
- `'N'` for `next` (with a `value` property)
- `'E'` for `error` (with an `error` property)
- `'C'` for `complete`

This is useful when you need to treat errors as values (for instance, to prevent an inner observable from terminating an outer one), log all notification types, or pass notifications through channels that only support `next` emissions. Use with `dematerialize` to unwrap.

## Signature

```typescript
function materialize<T>(): OperatorFunction<T, ObservableNotification<T>>
```

## Parameters

None.

## Return Type

`OperatorFunction<T, ObservableNotification<T>>` — an operator that wraps all source notifications in `ObservableNotification` objects and emits them as `next` values.

## Marble Diagram

```
Source:  --a--b--X  (error)
         materialize()
Output:  --N(a)--N(b)--N(E)--|
            (next wrapping a, b; then next wrapping the error; then complete)
```

## Examples

### Example 1: Convert errors to values to prevent stream termination

```typescript
import { of, map, materialize, mergeMap, dematerialize } from 'rxjs';

const items$ = of('a', 'b', 13, 'd');

// Without materialize, a single error kills the stream.
// With materialize, we can handle it gracefully.
items$.pipe(
  map((x: any) => x.toUpperCase()),
  materialize()
).subscribe(notification => {
  if (notification.kind === 'N') {
    console.log('Value:', notification.value);
  } else if (notification.kind === 'E') {
    console.error('Error caught as value:', notification.error);
  } else if (notification.kind === 'C') {
    console.log('Completed');
  }
});

// Value: A
// Value: B
// Error caught as value: TypeError: x.toUpperCase is not a function
// Completed
```

### Example 2: Safe inner observable — prevent errors from killing outer stream

```typescript
import { of, from, mergeMap, materialize, filter, map } from 'rxjs';

const sources = ['valid', 'bad', 'valid2'];

from(sources).pipe(
  mergeMap(name =>
    processItem(name).pipe(materialize())
  ),
  filter(n => n.kind === 'N'), // discard error/complete notifications
  map(n => n.value)
).subscribe(result => console.log('Result:', result));

function processItem(name: string) {
  if (name === 'bad') return of(null).pipe(map(() => { throw new Error('bad item'); }));
  return of(`processed-${name}`);
}

// Result: processed-valid
// Result: processed-valid2
// (error from 'bad' is swallowed)
```

### Example 3: Log all notification types from a stream

```typescript
import { interval, take, materialize } from 'rxjs';

interval(500).pipe(
  take(3),
  materialize()
).subscribe(notification => {
  switch (notification.kind) {
    case 'N': console.log(`[next] ${notification.value}`); break;
    case 'E': console.error(`[error] ${notification.error}`); break;
    case 'C': console.log('[complete]'); break;
  }
});

// [next] 0
// [next] 1
// [next] 2
// [complete]
```

## Common Pitfalls

- **Output stream still completes**: After materializing, the output Observable emits a `complete` notification as a `next` value and then completes. The downstream observer will receive the `C` notification wrapper and then see the output complete.
- **Double-wrapping errors**: Because the error is emitted as a `next` value and then the output completes, downstream operators using `catchError` will not intercept the error (it is no longer a real error notification).
- **Use `dematerialize` to unwrap**: `materialize` and `dematerialize` are inverses of each other. Always pair them when you need to pass notifications through a `next`-only channel and then restore them.

## Related Operators

- `dematerialize` — the inverse; converts `ObservableNotification` values back into real `next`/`error`/`complete` emissions
- `tap` — observe notifications without wrapping them
- `catchError` — handle errors while keeping the stream type unchanged
