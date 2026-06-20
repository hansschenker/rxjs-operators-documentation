# reduce

**Category**: Transformation  
**Import**: `import { reduce } from 'rxjs';`

## Description

Applies an accumulator function over the source Observable and returns the accumulated result when the source completes, given an optional seed value. Like `Array.prototype.reduce()`, it combines all values into a single output value.

Unlike `scan`, which emits each intermediate state, `reduce` only emits once — after the source Observable completes. It is equivalent to `scan` followed by `last`.

If a `seed` value is provided, it is used as the initial accumulator value. If no seed is provided, the first value from the source serves as the initial state and is not passed through the accumulator.

## Signature

```typescript
function reduce<V, A = V>(accumulator: (acc: A | V, value: V, index: number) => A): OperatorFunction<V, V | A>
function reduce<V, A>(accumulator: (acc: A, value: V, index: number) => A, seed: A): OperatorFunction<V, A>
function reduce<V, A, S = A>(accumulator: (acc: A | S, value: V, index: number) => A, seed: S): OperatorFunction<V, A>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `accumulator` | `(acc: A \| V, value: V, index: number) => A` | The reducer function called with the accumulated state and each source value. |
| `seed` | `S` | Optional. The initial accumulated value. If omitted, the first source value is used. |

## Return Type

`OperatorFunction<V, V | A>` — emits exactly one value (the final accumulated result) when the source completes.

## Marble Diagram

```
Source: --1--2--3--4--|
        reduce((acc, x) => acc + x, 0)
Output: --------------10|
        (emits only on source completion)
```

## Examples

### Example 1: Count click events over a time window

```typescript
import { fromEvent, takeUntil, timer, map, reduce } from 'rxjs';

// Count clicks in the next 5 seconds
fromEvent(document, 'click').pipe(
  takeUntil(timer(5000)),
  map(() => 1),
  reduce((count, one) => count + one, 0)
).subscribe(total => console.log(`${total} clicks in 5 seconds`));
```

### Example 2: Aggregate a stream of transactions

```typescript
import { from, reduce } from 'rxjs';

interface Transaction {
  type: 'credit' | 'debit';
  amount: number;
}

const transactions: Transaction[] = [
  { type: 'credit', amount: 500 },
  { type: 'debit', amount: 120 },
  { type: 'credit', amount: 300 },
  { type: 'debit', amount: 45 },
];

from(transactions).pipe(
  reduce((balance, tx) =>
    tx.type === 'credit' ? balance + tx.amount : balance - tx.amount,
    0
  )
).subscribe(balance => console.log('Final balance:', balance));
// Final balance: 635
```

### Example 3: Collect emitted values into a grouped map

```typescript
import { from, reduce } from 'rxjs';

interface LogEntry {
  level: 'info' | 'warn' | 'error';
  message: string;
}

const logs: LogEntry[] = [
  { level: 'info', message: 'Server started' },
  { level: 'error', message: 'Connection refused' },
  { level: 'warn', message: 'High memory usage' },
  { level: 'error', message: 'Disk nearly full' },
];

from(logs).pipe(
  reduce((groups, entry) => {
    const group = groups[entry.level] ?? [];
    return { ...groups, [entry.level]: [...group, entry.message] };
  }, {} as Record<string, string[]>)
).subscribe(grouped => console.log('Log summary:', grouped));
```

## Common Pitfalls

- **No emission until completion**: If the source never completes (e.g., a `Subject` or `interval`), `reduce` never emits. Use `scan` for live accumulation, or pair `reduce` with a `takeUntil`/`take` to force completion.
- **No-seed first value bypass**: Without a seed, the first emitted value is used as the initial state and emitted directly without going through the accumulator — it is treated as the starting accumulation. If this is undesirable, provide an explicit seed.
- **Empty source**: If the source completes without emitting any values and no seed was provided, `reduce` will throw an `EmptyError`. With a seed, the seed itself is emitted.

## Related Operators

- `scan` — like `reduce` but emits the intermediate accumulated state after each source value
- `count` — specialized reducer that counts the number of source emissions
- `sum` / `min` / `max` — specialized reductions (not in RxJS core; use `reduce`)
- `toArray` — collects all source values into a single array on completion
