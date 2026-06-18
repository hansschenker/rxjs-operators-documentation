# switchScan

## Identity

- **Name**: switchScan
- **Category**: Transformation Operators
- **Type**: Stateful `switchMap` — like `switchMap` but passes accumulated state to each projection, and new emissions cancel previous inner Observables
- **Import**:
  ```typescript
  import { switchScan } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  function switchScan<T, R, O extends ObservableInput<R>>(
    accumulator: (acc: R, value: T, index: number) => O,
    seed: R
  ): OperatorFunction<T, R>
  ```
- **Added**: RxJS 7.0

## Functional Specification

`switchScan` combines the cancellation behavior of `switchMap` with the state accumulation of `scan`. For each source emission, it calls `accumulator(currentState, value)` to produce an inner Observable. When that inner completes, its **last emitted value becomes the new state** passed to the next `accumulator` call. If a new source emission arrives before the inner completes, the inner is cancelled (switchMap behavior) — but the state is NOT updated from the cancelled inner.

**Comparison**:

| | `scan` | `switchMap` | `switchScan` |
|---|---|---|---|
| State | Yes (synchronous) | No | Yes (from async inner) |
| Cancels previous | N/A | Yes | Yes |
| Inner Observable | No | Yes | Yes |
| State from | Sync accumulator | N/A | Last value of completed inner |

## Marble Diagram

```
Source:    --a--b---------c--|
Seed:      { results: [] }

switchScan((state, v) => searchApi(v, state)):
  a arrives → inner A starts with state=seed
  b arrives → inner A cancelled; inner B starts with state=seed (A didn't complete)
  inner B completes with value stateB → state becomes stateB
  c arrives → inner C starts with state=stateB

Result:    ---------stateB-----------stateC--|
           (state from B flows into C; a's result was discarded)
```

## Examples

### Basic Usage — Autocomplete with Accumulated Context
```typescript
import { fromEvent } from 'rxjs';
import { switchScan, map } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

interface SearchState {
  query:   string;
  results: string[];
  page:    number;
}

const INITIAL_STATE: SearchState = { query: '', results: [], page: 0 };

fromEvent<InputEvent>(searchInput, 'input').pipe(
  map(e => (e.target as HTMLInputElement).value),
  switchScan(
    (state, query) =>
      ajax.getJSON<string[]>(`/api/search?q=${query}`).pipe(
        map(results => ({ query, results, page: 0 }))
      ),
    INITIAL_STATE
  )
).subscribe(state => renderResults(state));
// Each keystroke cancels the previous request AND carries accumulated state
```

### Common Pattern — Paginated Load More
```typescript
import { Subject } from 'rxjs';
import { switchScan } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

interface PageState { items: Item[]; page: number; hasMore: boolean }

const loadMore$ = new Subject<void>();

loadMore$.pipe(
  switchScan(
    (state) =>
      ajax.getJSON<Item[]>(`/api/items?page=${state.page + 1}`).pipe(
        map(newItems => ({
          items:   [...state.items, ...newItems],
          page:    state.page + 1,
          hasMore: newItems.length === PAGE_SIZE
        }))
      ),
    { items: [], page: 0, hasMore: true }
  )
).subscribe(state => {
  renderItems(state.items);
  toggleLoadMoreBtn(state.hasMore);
});
```

### Common Pattern — Stateful Search with History
```typescript
import { Subject } from 'rxjs';
import { switchScan, map } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

interface QueryState { current: string; history: string[]; results: Result[] }

const search$ = new Subject<string>();

search$.pipe(
  switchScan(
    (state, query) =>
      ajax.getJSON<Result[]>(`/api/search?q=${query}`).pipe(
        map(results => ({
          current: query,
          history: [...new Set([query, ...state.history])].slice(0, 10),
          results
        }))
      ),
    { current: '', history: [], results: [] }
  )
).subscribe(state => renderSearchUI(state));
```

## Common Pitfalls

### Anti-pattern: Expecting State Update on Cancellation
```typescript
import { Subject, timer } from 'rxjs';
import { switchScan, map } from 'rxjs/operators';

// ❌ MISUNDERSTANDING — cancelled inner's value does NOT update state
const clicks$ = new Subject<void>();
let tick = 0;

clicks$.pipe(
  switchScan(
    (state, _) =>
      timer(500).pipe(map(() => { tick++; return state + 1; })),
    0
  )
).subscribe(v => console.log('state:', v));

clicks$.next(); // inner starts
clicks$.next(); // inner cancelled before timer fires — state stays at 0
// After 500ms: state becomes 1 (from second click's inner completing)
// NOT 2 — first click's inner was cancelled, its state update was discarded

// ✅ UNDERSTAND: switchScan only updates state from COMPLETED inners.
// If you need every click to accumulate state, use mergeScan instead.
```

## Related Operators

- **`scan`**: Synchronous accumulation — no inner Observable, no cancellation
- **`mergeScan`**: Like `switchScan` but uses `mergeMap` semantics (concurrent, no cancellation)
- **`switchMap`**: Cancels previous inner but carries no state between invocations
- **`expand`**: Recursive `mergeMap` — each emission feeds back into the projection

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/switchScan](https://rxjs.dev/api/operators/switchScan)

---

**Cognitive Load**: 4/5 | **Usage Frequency**: 2/5 | **Composability**: 4/5
**Key teaching point**: State is only updated from **completed** inners — cancelled inners leave state unchanged. Use `mergeScan` when every inner must contribute to state.
