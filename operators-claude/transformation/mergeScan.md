# mergeScan

## Identity

- **Name**: mergeScan
- **Category**: Transformation Operators
- **Type**: Stateful `mergeMap` — accumulates state across concurrent inner Observables
- **Import**:
  ```typescript
  import { mergeScan } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  function mergeScan<T, R>(
    accumulator: (acc: R, value: T, index: number) => ObservableInput<R>,
    seed:        R,
    concurrent?: number
  ): OperatorFunction<T, R>
  ```

## Functional Specification

`mergeScan` combines `mergeMap` concurrency with `scan`-style state accumulation. Each source emission calls `accumulator(currentState, value)` to produce an inner Observable. When any inner Observable emits, that value **becomes the new state** for the next `accumulator` call. Multiple inner Observables can be active simultaneously (up to `concurrent` limit).

**`mergeScan` vs `switchScan`**:

| | `mergeScan` | `switchScan` |
|---|---|---|
| On new source emission | Starts new inner concurrently | Cancels current, starts new |
| State updated from | Each inner emission as it arrives | Last emission of completed inner |
| Use when | All source emissions must update state | Only latest source emission matters |

**State flow**: State is updated by the most recent inner emission across ALL active inners — this can produce interleaving. Use `concatScan` (via `mergeScan(fn, seed, 1)`) when state updates must be sequential.

## Marble Diagram

```
Source:    --a--b------c--|
Seed:      0

mergeScan((state, v) => of(state + v.length)):
  a: state=0 → inner emits 1 → state=1
  b: state=1 → inner emits 2 → state=3
  c: state=3 → inner emits 4 → state=7

Result:    --1--3------7--|

With async inners (concurrent):
Source:    --a--b--|
mergeScan((state, v) => timer(100).pipe(map(() => state + 1)), 0):
  a: starts inner, still pending
  b: starts inner concurrently (state still 0 — neither has emitted yet)
  inner a emits → state=1
  inner b emits → state=2

Result:    ----1--2--|
```

## Examples

### Basic Usage — Running Accumulation
```typescript
import { fromEvent } from 'rxjs';
import { mergeScan, map } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

// Accumulate search results across multiple queries
const search$ = fromEvent<InputEvent>(searchInput, 'input').pipe(
  map(e => (e.target as HTMLInputElement).value)
);

search$.pipe(
  mergeScan(
    (allResults, query) =>
      ajax.getJSON<Result[]>(`/api/search?q=${query}`).pipe(
        map(newResults => [...allResults, ...newResults])
      ),
    [] as Result[]
  )
).subscribe(accumulated => renderAll(accumulated));
// Each search appends to cumulative results — all requests run concurrently
```

### Common Pattern — Sequential Accumulation (`concurrent=1`)
```typescript
import { from } from 'rxjs';
import { mergeScan } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

// Process items sequentially, passing result of each to the next
from([1, 2, 3, 4, 5]).pipe(
  mergeScan(
    (acc, id) =>
      ajax.getJSON<number>(`/api/items/${id}/value`).pipe(
        map(value => acc + value)  // running total
      ),
    0,
    1  // concurrent=1 → sequential (like concatScan)
  )
).subscribe(runningTotal => console.log(runningTotal));
// Each item's HTTP response feeds into the next accumulation
```

### Common Pattern — Infinite Scroll Pagination
```typescript
import { Subject } from 'rxjs';
import { mergeScan, startWith } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

interface PageState { items: Item[]; nextPage: number }

const loadMore$ = new Subject<void>();

loadMore$.pipe(
  startWith(undefined), // trigger initial load
  mergeScan(
    (state) =>
      ajax.getJSON<Item[]>(`/api/items?page=${state.nextPage}`).pipe(
        map(newItems => ({
          items:    [...state.items, ...newItems],
          nextPage: state.nextPage + 1
        }))
      ),
    { items: [], nextPage: 0 } as PageState,
    1 // sequential — one page load at a time
  )
).subscribe(state => renderList(state.items));
```

## Common Pitfalls

### Anti-pattern: Concurrent State Updates Causing Stale Reads
```typescript
import { Subject, timer } from 'rxjs';
import { mergeScan, map } from 'rxjs/operators';

// ❌ RACE CONDITION — concurrent inners both read the same stale state
const clicks$ = new Subject<void>();
clicks$.pipe(
  mergeScan(
    (count) => timer(500).pipe(map(() => count + 1)),
    0
    // no concurrent limit — both inners read count=0 before either completes
  )
).subscribe(console.log);

clicks$.next(); // inner A starts, reads count=0
clicks$.next(); // inner B starts, reads count=0 (before A completed!)
// Both emit 1 — state jumps to 1 twice, never reaches 2

// ✅ CORRECT — use concurrent=1 when state updates must be atomic
clicks$.pipe(
  mergeScan(
    (count) => timer(500).pipe(map(() => count + 1)),
    0,
    1  // sequential: B waits for A to complete before reading state
  )
).subscribe(console.log); // 1, 2 — correct

// WHY: With concurrent > 1, multiple inners can start before any
// completes, all reading the same stale state value.
```

## Related Operators

- **`scan`**: Synchronous accumulation — no inner Observable
- **`switchScan`**: Like `mergeScan` but cancels previous inner on new source emission
- **`mergeMap`**: Concurrent flattening without state accumulation
- **`expand`**: Recursive `mergeMap` — each output feeds back as input

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/mergeScan](https://rxjs.dev/api/operators/mergeScan)

---

**Cognitive Load**: 4/5 | **Usage Frequency**: 2/5 | **Composability**: 4/5
**Key teaching point**: Use `concurrent=1` (sequential) when state must be updated atomically — concurrent inners both read the same stale state, potentially causing lost updates.
