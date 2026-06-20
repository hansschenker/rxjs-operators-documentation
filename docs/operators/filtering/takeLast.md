# takeLast

**Category**: Filtering  
**Import**: `import { takeLast } from 'rxjs';`

## Description

`takeLast` waits for the source Observable to complete, then emits the last `count` values in the order they were originally received. It uses an internal ring buffer of size `count` to efficiently hold candidates, replacing the oldest entry whenever the buffer is full.

If the source emits fewer than `count` values, all emitted values are forwarded. If `count` is zero or negative, the output completes immediately without emitting anything.

**Warning**: Because `takeLast` must wait for source completion before emitting, it will never emit a value if the source never completes.

## Signature

```typescript
function takeLast<T>(count: number): MonoTypeOperatorFunction<T>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| count | `number` | The maximum number of values to emit from the end of the source sequence. If `<= 0`, completes immediately. |

## Return Type

`MonoTypeOperatorFunction<T>` — emits the last `count` values after source completion.

## Marble Diagram

```
Source: --a--b--c--d--e--|
        takeLast(3)
Output: -----------------c--d--e|
        (emits on completion, in original order)
```

## Examples

### Example 1: Emit the last 3 values from a finite range

```typescript
import { range } from 'rxjs';
import { takeLast } from 'rxjs';

range(1, 100).pipe(
  takeLast(3)
).subscribe(n => console.log(n));

// Logs: 98, 99, 100
```

### Example 2: Capture the last few API responses before a stream closes

```typescript
import { Subject } from 'rxjs';
import { takeLast } from 'rxjs';

const responses$ = new Subject<string>();

responses$.pipe(
  takeLast(2)
).subscribe({
  next: res => console.log('Response:', res),
  complete: () => console.log('Stream ended')
});

responses$.next('result-1');
responses$.next('result-2');
responses$.next('result-3');
responses$.complete();

// Logs: Response: result-2, Response: result-3, Stream ended
```

### Example 3: Use with HTTP request streams

```typescript
import { from } from 'rxjs';
import { takeLast, map } from 'rxjs';

// Simulate a paginated fetch where you only care about the last page
const pages$ = from(['page1-data', 'page2-data', 'page3-data']);

pages$.pipe(
  takeLast(1),
  map(data => `Final page: ${data}`)
).subscribe(console.log);

// Logs: Final page: page3-data
```

## Common Pitfalls

- **Never emits from infinite streams**: `takeLast` requires source completion. Combine with `takeUntil` or `take` to bound infinite sources first.
- **All values emitted synchronously on completion**: The buffered values are flushed synchronously when the source completes. Downstream operators and subscribers should be prepared to handle a burst of synchronous emissions.
- **`takeLast(0)` completes immediately**: Like `take(0)`, a count of zero produces an empty Observable.

## Related Operators

- `take` — emits the first N values
- `last` — emits only the single last matching value
- `skipLast` — skips the last N values instead of keeping them
- `takeUntil` — completes based on a notifier rather than a count
