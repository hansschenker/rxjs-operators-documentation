# dematerialize

**Category**: Utility  
**Import**: `import { dematerialize } from 'rxjs';`

## Description

`dematerialize` is the inverse of `materialize`. It takes an Observable that emits `ObservableNotification` objects — the wrapper type produced by `materialize` — and converts each notification back into real `next`, `error`, and `complete` emissions on the output Observable.

This is useful when you need to pass notification metadata through a channel that only supports `next` values (such as a `Subject`, a queue, or a WebSocket), and then restore them on the other end as real observable events.

The input Observable is assumed to emit only `ObservableNotification` objects and never produce real errors itself.

## Signature

```typescript
function dematerialize<N extends ObservableNotification<any>>(): OperatorFunction<N, ValueFromNotification<N>>
```

## Parameters

None.

## Return Type

`OperatorFunction<N, ValueFromNotification<N>>` — an operator that unwraps `ObservableNotification` objects back into real `next`, `error`, and `complete` emissions.

## Marble Diagram

```
Source:  --N(a)--N(b)--N(E)--|
          (materialized notifications)
         dematerialize()
Output:  --a--b--X
          (real next values, then real error emission)
```

## Examples

### Example 1: Round-trip materialize/dematerialize for error isolation

```typescript
import { of, map, materialize, dematerialize, catchError } from 'rxjs';

of('a', 'b', 13 as any, 'd').pipe(
  map((x: any) => x.toUpperCase()),
  materialize(),         // errors become next values
  dematerialize()        // restore them as real notifications
).subscribe({
  next: x => console.log('Value:', x),
  error: err => console.error('Error:', err.message)
});

// Value: A
// Value: B
// Error: x.toUpperCase is not a function
```

### Example 2: Replay notifications from a static array

```typescript
import { of, dematerialize } from 'rxjs';
import type { NextNotification, ErrorNotification } from 'rxjs';

const notifA: NextNotification<string> = { kind: 'N', value: 'Hello' };
const notifB: NextNotification<string> = { kind: 'N', value: 'World' };
const notifC: ErrorNotification = {
  kind: 'E',
  error: new TypeError('something went wrong')
};

of(notifA, notifB, notifC).pipe(
  dematerialize()
).subscribe({
  next: x => console.log(x),
  error: e => console.error(e.message)
});

// Hello
// World
// something went wrong
```

### Example 3: Transfer notifications over a Subject pipeline

```typescript
import { Subject, from, materialize, dematerialize, map } from 'rxjs';
import type { ObservableNotification } from 'rxjs';

// A channel that only accepts plain next values
const channel$ = new Subject<ObservableNotification<number>>();

// Consumer: receives notifications and restores them
channel$.pipe(
  dematerialize()
).subscribe({
  next: n => console.log('Received:', n),
  error: err => console.error('Error restored:', err.message),
  complete: () => console.log('Complete restored')
});

// Producer: sends notifications through the channel
from([1, 2, 3]).pipe(
  materialize()
).subscribe(notification => channel$.next(notification));

// Received: 1
// Received: 2
// Received: 3
// Complete restored
```

## Common Pitfalls

- **Input must be materialized notifications**: `dematerialize` expects every `next` emission to be an `ObservableNotification`. Passing non-notification values will produce incorrect results or runtime errors.
- **Errors in the wrapping observable**: If the outer observable (the one emitting `ObservableNotification` values) itself errors before completing, `dematerialize` will propagate that error directly, not via the notification protocol.
- **Always pair with `materialize`**: These two operators are designed to be used together. Avoid constructing `ObservableNotification` objects manually unless you have a very specific need.

## Related Operators

- `materialize` — the inverse; converts all `next`/`error`/`complete` notifications into wrapped `ObservableNotification` objects emitted as `next` values
- `catchError` — handle errors in a more conventional way when you don't need the full notification protocol
