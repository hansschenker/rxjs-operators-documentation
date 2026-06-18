# scan

**Category**: Transformation  
**Import**: `import { scan } from 'rxjs';`

## Description

Applies an accumulator function over the source Observable, emitting the current accumulated state after each update. Like `reduce`, but instead of waiting for the source to complete, it emits the intermediate state after every source value.

If a `seed` value is provided, it is used as the initial state. If no seed is provided, the first value from the source is used as the initial state and emitted directly (bypassing the accumulator). All subsequent values go through the accumulator.

`scan` is the streaming equivalent of `Array.prototype.reduce` and is the foundation of Redux-style state management in reactive applications.

## Signature

```typescript
function scan<V, A = V>(accumulator: (acc: A | V, value: V, index: number) => A): OperatorFunction<V, V | A>
function scan<V, A>(accumulator: (acc: A, value: V, index: number) => A, seed: A): OperatorFunction<V, A>
function scan<V, A, S>(accumulator: (acc: A | S, value: V, index: number) => A, seed: S): OperatorFunction<V, A>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `accumulator` | `(acc: A \| V, value: V, index: number) => A` | The reducer function called with the current accumulated state and the next source value. |
| `seed` | `S` | Optional. The initial accumulated state. If omitted, the first source value is used as the initial state. |

## Return Type

`OperatorFunction<V, V | A>` — emits the accumulated state after each source value.

## Marble Diagram

```
Source: --1--2--3--4--|
        scan((acc, x) => acc + x, 0)
Output: --1--3--6--10--|
```

## Examples

### Example 1: Running total of user actions

```typescript
import { Subject, scan } from 'rxjs';

type Action = { type: 'ADD'; amount: number } | { type: 'RESET' };

const action$ = new Subject<Action>();

action$.pipe(
  scan((total, action) => {
    switch (action.type) {
      case 'ADD': return total + action.amount;
      case 'RESET': return 0;
    }
  }, 0)
).subscribe(total => console.log('Cart total: $', total));

action$.next({ type: 'ADD', amount: 10 });  // $10
action$.next({ type: 'ADD', amount: 25 });  // $35
action$.next({ type: 'RESET' });            // $0
action$.next({ type: 'ADD', amount: 5 });   // $5
```

### Example 2: Redux-style state management

```typescript
import { Subject, scan, startWith } from 'rxjs';

interface AppState {
  loading: boolean;
  items: string[];
  error: string | null;
}

type AppAction =
  | { type: 'LOAD_START' }
  | { type: 'LOAD_SUCCESS'; items: string[] }
  | { type: 'LOAD_ERROR'; error: string };

const initialState: AppState = { loading: false, items: [], error: null };

const dispatch$ = new Subject<AppAction>();

const state$ = dispatch$.pipe(
  scan((state, action): AppState => {
    switch (action.type) {
      case 'LOAD_START':  return { ...state, loading: true, error: null };
      case 'LOAD_SUCCESS': return { loading: false, items: action.items, error: null };
      case 'LOAD_ERROR':  return { ...state, loading: false, error: action.error };
    }
  }, initialState),
  startWith(initialState)
);

state$.subscribe(state => console.log('State:', state));

dispatch$.next({ type: 'LOAD_START' });
dispatch$.next({ type: 'LOAD_SUCCESS', items: ['a', 'b', 'c'] });
```

### Example 3: Collect all values into an array

```typescript
import { interval, scan, take } from 'rxjs';

interval(500).pipe(
  take(5),
  scan((acc, value) => [...acc, value], [] as number[])
).subscribe(arr => console.log(arr));
// [0]
// [0, 1]
// [0, 1, 2]
// [0, 1, 2, 3]
// [0, 1, 2, 3, 4]
```

## Common Pitfalls

- **Mutating accumulator state**: If you mutate the `acc` object rather than returning a new one (e.g., using `acc.push(value)` and then returning `acc`), downstream subscribers may see unexpected behavior because they all share the same reference. Always return a new array/object.
- **No-seed first emission**: When no seed is provided, the first value is emitted as-is without passing through the accumulator. This can be surprising if consumers expect a transformed type. Provide a seed to ensure consistent types.
- **Comparing `scan` with `reduce`**: `scan` emits on every value; `reduce` emits only once when the source completes. Use `scan` for live state, `reduce` for batch aggregation.

## Related Operators

- `reduce` — like `scan` but only emits the final accumulated value when the source completes
- `mergeScan` — like `scan` but the accumulator returns an Observable
- `switchScan` — like `mergeScan` but switches to the latest returned Observable
- `expand` — recursively feeds output values back through a project function
