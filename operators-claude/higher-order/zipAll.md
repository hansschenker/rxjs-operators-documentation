# zipAll

## Identity

- **Name**: zipAll
- **Category**: Higher-Order Operators (Join)
- **Type**: Higher-order zip — collects all inner Observables, then combines their emissions by index
- **Import**:
  ```typescript
  import { zipAll } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  function zipAll<T>(): OperatorFunction<ObservableInput<T>, T[]>
  function zipAll<T, R>(
    project: (...values: T[]) => R
  ): OperatorFunction<ObservableInput<T>, R>
  ```

## Functional Specification

`zipAll` operates on a higher-order Observable (an Observable that emits Observables). It:
1. **Collects** all inner Observables until the outer source completes
2. **Subscribes** to all of them simultaneously once the outer completes
3. **Pairs** their emissions by index — first emission from each, then second from each, etc. — exactly like `zip([...])`

**`zipAll` vs `combineLatestAll`**:

| | `zipAll` | `combineLatestAll` |
|---|---|---|
| Pairing | By index (1st with 1st, 2nd with 2nd) | Latest from each |
| Buffers | Yes — waits for each index across all inners | No — reactive on any emission |
| Emits | Once per complete "row" across all inners | On every emission once all seeded |
| Use when | Parallel streams of same length | Reactive combination of live streams |

**Invariants**:
- Outer source MUST complete for `zipAll` to subscribe to the inners
- Completes when the shortest inner Observable completes
- Buffers emissions from faster inners waiting for slower ones at the same index

## Marble Diagram

```
Outer:   --A$--B$--C$--|   (outer completes at |)
                      ↓ outer completes → subscribe to all three inners

A$: --1------3--|
B$: ---2--4-----|
C$: ------5--6--|

zipAll:  pairs by index:
  Index 0: wait for A[0]=1, B[0]=2, C[0]=5 → emit [1,2,5]
  Index 1: wait for A[1]=3, B[1]=4, C[1]=6 → emit [3,4,6]
  A and B complete after index 1 → zipAll completes

Result:  --------[1,2,5]----[3,4,6]--|
```

## Examples

### Basic Usage
```typescript
import { of, timer } from 'rxjs';
import { map, zipAll, take } from 'rxjs/operators';

// Three streams of 3 values each — paired by index
of(
  timer(0, 100).pipe(take(3), map(v => `A${v}`)),
  timer(0, 150).pipe(take(3), map(v => `B${v}`)),
  timer(0, 200).pipe(take(3), map(v => `C${v}`))
).pipe(
  zipAll()
).subscribe(console.log);
// ['A0','B0','C0'], ['A1','B1','C1'], ['A2','B2','C2']
```

### Common Pattern — Parallel Tasks, Row-by-Row Results
```typescript
import { from, of } from 'rxjs';
import { map, zipAll } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

// Run N parallel streams, consume results row-by-row across all streams
const streamConfigs = [
  { url: '/api/prices', transform: (v: number) => v * 1.2 },
  { url: '/api/volumes', transform: (v: number) => v },
  { url: '/api/timestamps', transform: (v: string) => new Date(v) },
];

from(streamConfigs).pipe(
  map(({ url, transform }) =>
    ajax.getJSON<any[]>(url).pipe(
      map(arr => from(arr)), // each config → array → Observable
    )
  ),
  zipAll()
  // emits [price[0], volume[0], ts[0]], [price[1], volume[1], ts[1]], ...
).subscribe(([price, volume, timestamp]) => {
  renderDataPoint(price, volume, timestamp);
});
```

### Common Pattern — With Project Function
```typescript
import { of, interval } from 'rxjs';
import { map, zipAll, take } from 'rxjs/operators';

of(
  interval(100).pipe(take(4)),
  interval(150).pipe(take(4), map(v => v * 10))
).pipe(
  zipAll((a, b) => ({ index: a, value: b }))
).subscribe(console.log);
// {index:0, value:0}, {index:1, value:10}, {index:2, value:20}, {index:3, value:30}
```

## Common Pitfalls

### Anti-pattern: Outer Source That Never Completes
```typescript
import { Subject } from 'rxjs';
import { zipAll } from 'rxjs/operators';

// ❌ HANGS — Subject never completes; zipAll never subscribes to inners
const source$ = new Subject<Observable<number>>();
source$.pipe(zipAll()).subscribe(console.log);

source$.next(of(1, 2, 3));
source$.next(of(4, 5, 6));
// Nothing emitted — waiting for source$ to complete

source$.complete(); // NOW zipAll subscribes and emits [1,4], [2,5], [3,6]

// WHY: zipAll (like combineLatestAll) needs to know the full set of
// inner Observables before subscribing. Complete the outer when done.
```

### Anti-pattern: Using `zipAll` When Streams Have Different Lengths
```typescript
import { of } from 'rxjs';
import { zipAll } from 'rxjs/operators';

// ❌ SURPRISE — zipAll stops at the shortest inner
of(
  of(1, 2, 3),  // 3 values
  of(4, 5)      // only 2 values
).pipe(
  zipAll()
).subscribe(console.log);
// [1,4], [2,5]  ← only 2 rows; [3] is never emitted

// ✅ UNDERSTAND — this is intentional zip behavior:
// zipAll (like zip) stops when the shortest inner completes.
// If you need all values from all streams, use combineLatestAll or mergeAll.
```

## Related Operators

- **`zip([...])`**: Static version — sources known at construction time
- **`combineLatestAll`**: Combines by latest value, not index — reactive on any emission
- **`mergeAll`**: No pairing — flattens all inner Observables concurrently
- **`forkJoin`**: Waits for all to complete, emits last value of each — one combined result

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/zipAll](https://rxjs.dev/api/operators/zipAll)

---

**Cognitive Load**: 4/5 | **Usage Frequency**: 1/5 | **Composability**: 3/5
**Key teaching point**: Combines by **index**, not latest value. Completes when the shortest inner completes — use only when all inners have the same length or you intentionally want to stop at the shortest.
