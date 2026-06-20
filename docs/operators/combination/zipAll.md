# zipAll

**Category**: Combination  
**Import**: `import { zipAll } from 'rxjs';`

## Description

`zipAll` collects all inner Observables emitted by a higher-order source, waits for the source Observable to complete, then subscribes to all collected inner Observables simultaneously using the `zip` strategy. Values are combined by index: the first value from every inner Observable is emitted together as an array, then the second value from each, and so on. The output completes as soon as any inner Observable completes and exhausts its buffered values.

An optional `project` function may be supplied to transform each positional group of values into a custom output before emission. `zipAll` is most appropriate when all inner streams emit at roughly the same rate and produce the same number of values.

## Signature

```typescript
function zipAll<T>(): OperatorFunction<ObservableInput<T>, T[]>
function zipAll<T, R>(project: (...values: T[]) => R): OperatorFunction<ObservableInput<T>, R>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| project | `(...values: T[]) => R` | Optional. A function that takes the nth value from each inner Observable as positional arguments and returns the value to emit. If omitted, an array of the nth values is emitted. |

## Return Type

`OperatorFunction<ObservableInput<T>, T[]>` — without `project`, emits arrays where each element is the nth value from the corresponding inner Observable. With `project`, emits the result of calling the projection function with those values.

## Marble Diagram

```
Source:   --A--B--|   (source completes, then zip begins)
A:                 --1--2--3--|
B:                 --a--b--c--|
          zipAll()
Output:             --[1,a]--[2,b]--[3,c]--|
```

## Examples

### Example 1: Pairing questions with answers from separate streams

```typescript
import { of, from, delay, map, zipAll } from 'rxjs';

const questions = ['What is 2+2?', 'Capital of France?', 'Speed of light?'];
const answers = ['4', 'Paris', '299,792,458 m/s'];

// Create a higher-order Observable that emits one Observable per question
of(questions, answers).pipe(
  map(arr => from(arr)), // Each array becomes a stream of strings
  zipAll()               // Pair each question with its matching answer by index
).subscribe(([question, answer]) => {
  console.log(`Q: ${question}  A: ${answer}`);
});
// Q: What is 2+2?  A: 4
// Q: Capital of France?  A: Paris
// Q: Speed of light?  A: 299,792,458 m/s
```

### Example 2: Synchronising results from parallel batch jobs

```typescript
import { from, of, delay, map, zipAll } from 'rxjs';

const batchIds = ['batch-001', 'batch-002', 'batch-003'];

// Each batch returns results in an ordered stream
of(...batchIds).pipe(
  map(id =>
    from(fetchBatchResults(id)) // returns Observable<Result[]>
  ),
  zipAll(
    (b1Results, b2Results, b3Results) => ({
      combined: [...b1Results, ...b2Results, ...b3Results],
      batchCount: 3,
    })
  )
).subscribe(summary => console.log('Combined batch summary:', summary));
```

### Example 3: Transposing rows to columns in a data grid

```typescript
import { from, zipAll } from 'rxjs';

// rows is an array of arrays — each row is a stream of cell values
const rows = [
  [1, 2, 3],
  [4, 5, 6],
  [7, 8, 9],
];

// Emit each row as an Observable, then zip them to get columns
from(rows.map(row => from(row))).pipe(
  zipAll()
).subscribe(column => console.log('Column:', column));
// Column: [1, 4, 7]
// Column: [2, 5, 8]
// Column: [3, 6, 9]
```

## Common Pitfalls

- **Source must complete first**: Like `combineLatestAll`, `zipAll` waits for the outer source to complete before subscribing to any inner Observable. A source that never completes will prevent any output from ever being emitted.
- **Shortest inner Observable controls length**: Once any inner Observable completes, the output also completes after emitting all arrays it can form. Values buffered by longer-running inner Observables are discarded.
- **Memory concerns with mismatched rates**: If inner Observables emit at very different speeds, faster ones buffer their values waiting for slower ones to catch up. This can cause significant memory usage. Consider `combineLatestWith` if you need the most recent value rather than positional pairing.
- **Often confused with `combineLatest`**: `zipAll` pairs values by their emission index. `combineLatestAll` pairs the most recent value from each source whenever any source emits. Choose based on whether positional matching or recency matters.

## Related Operators

- `zipWith` — pipeable operator for zipping a known, static set of Observables
- `zip` — creation operator that takes a fixed array of Observable inputs
- `combineLatestAll` — like `zipAll` but combines the latest values rather than values by index
- `mergeAll` — flattens inner Observables concurrently without index-based pairing
