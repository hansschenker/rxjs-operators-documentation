# skipLast

**Category**: Filtering  
**Import**: `import { skipLast } from 'rxjs';`

## Description

`skipLast` emits values from the source Observable but delays each emission until it knows the value is not among the last `skipCount` values. It achieves this by holding a ring buffer of `skipCount` items: a value is only forwarded once `skipCount` newer values have arrived to replace it in the buffer.

The practical result is that values are emitted in real time (not buffered until completion), but the final `skipCount` values in the stream are never emitted. This is different from `takeLast`, which buffers everything and only emits on completion.

If `skipCount` is zero or negative, the operator passes all values through unchanged.

## Signature

```typescript
function skipLast<T>(skipCount: number): MonoTypeOperatorFunction<T>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| skipCount | `number` | The number of values at the end of the sequence to skip. Must be a non-negative integer. |

## Return Type

`MonoTypeOperatorFunction<T>` — emits all source values except the last `skipCount`.

## Marble Diagram

```
Source: --a--b--c--d--e--|
        skipLast(2)
Output: --------a--b--c--|
        (a is emitted when c arrives; b when d; c when e)
```

## Examples

### Example 1: Skip the last 2 values

```typescript
import { of } from 'rxjs';
import { skipLast } from 'rxjs';

of(1, 2, 3, 4, 5).pipe(
  skipLast(2)
).subscribe(n => console.log(n));

// Logs: 1, 2, 3
// (4 and 5 are skipped)
```

### Example 2: Streaming log processor that excludes trailing entries

```typescript
import { Subject } from 'rxjs';
import { skipLast } from 'rxjs';

const logEntries$ = new Subject<string>();

// Process log entries but discard the last 1 (e.g., a summary/footer line)
logEntries$.pipe(
  skipLast(1)
).subscribe(entry => console.log('Log:', entry));

logEntries$.next('INFO: Start');
logEntries$.next('INFO: Processing');
logEntries$.next('INFO: Done');
logEntries$.next('SUMMARY: 3 records processed'); // never emitted
logEntries$.complete();

// Logs: INFO: Start, INFO: Processing, INFO: Done
```

### Example 3: Combine with other operators for windowed processing

```typescript
import { range } from 'rxjs';
import { skipLast, take } from 'rxjs';

// Emit values 1-10 but skip first 2 and last 2 (interior values only)
range(1, 10).pipe(
  skip(2),
  skipLast(2)
).subscribe(n => console.log(n));

import { skip } from 'rxjs';
// Logs: 3, 4, 5, 6, 7, 8
```

## Common Pitfalls

- **Delay is proportional to `skipCount`**: Values are delayed by `skipCount` emissions, not by time. A large `skipCount` on a slow source creates a large observable lag.
- **Does not buffer until completion**: Unlike `takeLast`, `skipLast` emits values as soon as it has seen enough newer values to know the current one is safe to forward. This makes it suitable for ongoing streams.
- **`skipLast(0)` is a passthrough**: A count of zero returns an identity operator.

## Related Operators

- `takeLast` — emits only the last N values (buffers until completion)
- `skip` — skips the first N values
- `last` — emits only the single last value
- `take` — emits only the first N values
