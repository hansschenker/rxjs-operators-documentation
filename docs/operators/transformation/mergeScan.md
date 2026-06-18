# mergeScan

**Category**: Transformation  
**Import**: `import { mergeScan } from 'rxjs';`

## Description

Applies an accumulator function over the source Observable where the accumulator function itself returns an Observable, then each intermediate Observable returned is merged into the output Observable. The accumulated state from each inner Observable emission is passed to the accumulator on the next source value.

`mergeScan` is like `scan`, but allows the accumulator to perform asynchronous work (e.g., an HTTP request) and return an Observable. Multiple inner Observables can run concurrently (controlled by the `concurrent` parameter).

## Signature

```typescript
function mergeScan<T, R>(
  accumulator: (acc: R, value: T, index: number) => ObservableInput<R>,
  seed: R,
  concurrent?: number
): OperatorFunction<T, R>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `accumulator` | `(acc: R, value: T, index: number) => ObservableInput<R>` | The accumulator function. Receives the last state emitted by any inner Observable (or the `seed` initially), the current source value, and the emission index. Returns an Observable whose emissions become the new state. |
| `seed` | `R` | The initial accumulated state passed to the first accumulator call. |
| `concurrent` | `number` | Optional. Defaults to `Infinity`. Maximum number of inner Observables subscribed to simultaneously. |

## Return Type

`OperatorFunction<T, R>` — emits each value produced by the inner Observables returned by the accumulator, representing intermediate accumulated states.

## Marble Diagram

```
Source:  --a--b--c--|
         mergeScan((acc, x) => asyncOp(acc, x), seed)
Output:  --r1--r2--r3--|
         (each rN is a value from the inner Observable)
```

## Examples

### Example 1: Asynchronously count click events

```typescript
import { fromEvent, map, mergeScan, of } from 'rxjs';

const click$ = fromEvent(document, 'click');
const one$ = click$.pipe(map(() => 1));

const count$ = one$.pipe(
  mergeScan((acc, one) => of(acc + one), 0)
);

count$.subscribe(x => console.log('Click count:', x));
// Click count: 1
// Click count: 2
// Click count: 3
```

### Example 2: Accumulate search results from an API

```typescript
import { Subject, mergeScan, from } from 'rxjs';

const search$ = new Subject<string>();

search$.pipe(
  mergeScan(
    (allResults, query) =>
      from(
        fetch(`/api/search?q=${query}`)
          .then(r => r.json())
          .then((newResults: string[]) => [...allResults, ...newResults])
      ),
    [] as string[],
    1 // Process one search at a time
  )
).subscribe(results => console.log('All results so far:', results));

search$.next('cats');
search$.next('dogs');
```

### Example 3: Authenticate and then fetch user data (chained async state)

```typescript
import { of, mergeScan, from } from 'rxjs';

interface AuthState {
  token: string | null;
  user: { name: string } | null;
}

of('login-credentials').pipe(
  mergeScan(
    (state, credentials) =>
      from(
        fetch('/api/auth', { method: 'POST', body: credentials })
          .then(r => r.json())
          .then(({ token }) =>
            fetch('/api/me', { headers: { Authorization: `Bearer ${token}` } })
              .then(r => r.json())
              .then(user => ({ token, user }))
          )
      ),
    { token: null, user: null } as AuthState
  )
).subscribe(state => console.log('Auth state:', state));
```

## Common Pitfalls

- **Seed is not emitted**: The `seed` value is used as the initial state but is never emitted. The first emission from the output Observable is the first value from the first inner Observable.
- **Concurrent updates can interleave**: With the default `concurrent = Infinity`, multiple inner Observables may update the state simultaneously and the `acc` seen by the next accumulator call will be whichever inner Observable emitted most recently. Set `concurrent = 1` if you need strictly sequential state updates.
- **Inner Observable never completes**: If an inner Observable never completes, `mergeScan` keeps it open. Ensure inner Observables have a defined lifetime.

## Related Operators

- `scan` — like `mergeScan` but the accumulator returns a plain value synchronously
- `switchScan` — like `mergeScan` but cancels the previous inner Observable on each new source value
- `mergeMap` — like `mergeScan` but does not carry accumulated state between calls
- `expand` — recursively feeds output values back into the project function
