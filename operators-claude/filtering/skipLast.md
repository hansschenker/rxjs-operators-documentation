# skipLast

## Identity
- **Name**: skipLast
- **Category**: Filtering Operators
- **Type**: Tail suppression — forwards every value except the final `skipCount`, delaying each emission by `skipCount` positions
- **Import**:
  ```typescript
  import { skipLast } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  function skipLast<T>(skipCount: number): MonoTypeOperatorFunction<T>
  ```

## Functional Specification

**Input**: `Observable<T>` — a finite source whose trailing values should be discarded

**Output**: `Observable<T>` — the same values minus the last `skipCount`, in original order

**Transformation**:
Keeps a ring buffer of size `skipCount`. While the buffer is filling, nothing is forwarded. Once it is full, each new source value pushes the oldest buffered value downstream. When the source completes, whatever is still in the buffer — exactly the last `skipCount` values — is discarded and the output completes. The operator cannot know which values are "last" until completion, so every emission is delayed by `skipCount` arrivals.

**Mathematical representation**:
```
skipLast(n)(S) = { v₁, v₂, ..., v₍|S|₋ₙ₎ }  ++ complete()
  where vᵢ is the ith emission from S

Emission timing:
  vᵢ is emitted when v₍ᵢ₊ₙ₎ arrives   (delayed by n positions)

Special cases:
  n ≤ 0:    identity (every value forwarded immediately)
  n ≥ |S|:  nothing emitted; completes with S
```

**Invariants**:
- **Order preserved**: values leave in the order they arrived
- **Exactly |S| − n emissions** (or zero when n ≥ |S|)
- **Bounded memory**: never holds more than `skipCount` values
- **Lag of n**: a value is forwarded only after `n` further values have been observed

## Marble Diagram

```
Source:   --1--2--3--4--5--|
          skipLast(2)
Result:   --------1--2--3--|

1 is emitted when 3 arrives, 2 when 4 arrives, 3 when 5 arrives.
4 and 5 are still buffered at completion and are dropped.
```

**`skipCount ≥ source length`**:
```
Source:   --1--2--|
          skipLast(5)
Result:   --------|

Buffer never fills; completes with nothing emitted.
```

**`skipCount = 0`**:
```
Source:   --1--2--3--|
          skipLast(0)
Result:   --1--2--3--|

Identity — values forwarded without delay.
```

**Key observation**: `skipLast(n)` is not a cheap "drop the tail" — it is a sliding delay line. Every value waits for `n` successors before it may pass. On a slow or infinite source that delay is visible, and the final `n` values of a never-completing stream are never seen at all.

## Behavioral Characteristics

**Subscription**:
- Subscribes to the source immediately
- With `skipCount ≤ 0` the operator is a pass-through

**Completion semantics**:
- Completes when the source completes; the buffered tail is discarded at that moment
- Requires completion to finish — on an infinite source the output is simply a delayed copy minus the trailing window

**Error handling**:
- Source errors propagate immediately; buffered values are lost, never flushed
- No errors of its own

**Backpressure**:
- O(`skipCount`) memory, fixed
- Does not slow the source; it only shifts emission timing

**Hot vs. Cold**:
- Cold sources: deterministic — the last `n` values of each subscription are removed
- Hot sources: the buffer starts at subscription time; the "last n" are relative to when the hot source completes, which may be never

## Type System Integration

```typescript
/**
 * Type Parameters:
 *   T - Value type (MonoTypeOperatorFunction<T> — type preserved)
 *
 * Input Type:  Observable<T>
 * Output Type: Observable<T>
 *
 * Type Narrowing: none — skipLast never inspects values.
 *
 * Type Safety: skipCount is a plain number; negative or fractional
 * values are not rejected by the compiler. Guard at the call site if
 * the count comes from user input.
 */

import { from } from 'rxjs';
import { skipLast } from 'rxjs/operators';

interface LogLine { ts: number; text: string }

const lines: LogLine[] = [
  { ts: 1, text: 'start' },
  { ts: 2, text: 'work' },
  { ts: 3, text: 'END-OF-FILE' },
];

// Output type stays Observable<LogLine>
const body$ = from(lines).pipe(skipLast(1));
body$.subscribe((line: LogLine) => console.log(line.text));
// Output: start, work
```

## Examples

### Basic Usage — Drop the Last Two Values
```typescript
import { of } from 'rxjs';
import { skipLast } from 'rxjs/operators';

of(1, 2, 3, 4, 5).pipe(
  skipLast(2)
).subscribe({
  next:     v  => console.log(v),
  complete: () => console.log('done'),
});
// Output: 1, 2, 3, done
```

### Common Pattern — Strip a Trailing Sentinel
```typescript
import { from } from 'rxjs';
import { skipLast } from 'rxjs/operators';

// A protocol that terminates each message batch with a marker record
const frames = ['data-1', 'data-2', 'data-3', 'DONE'];

from(frames).pipe(
  skipLast(1) // the marker is always the final element
).subscribe(console.log);
// Output: data-1, data-2, data-3
```

### Common Pattern — Exclude an Incomplete Trailing Window
```typescript
import { from } from 'rxjs';
import { bufferCount, skipLast } from 'rxjs/operators';

// Group readings into pairs; the final pair may be partial and should be ignored
const readings = [10, 12, 11, 13, 9];

from(readings).pipe(
  bufferCount(2),   // [10,12], [11,13], [9]
  skipLast(1)       // drop the trailing partial group
).subscribe(pair => console.log(pair));
// Output: [10, 12], [11, 13]
```

### Edge Cases — Delay Effect, Oversized Count, Zero Count
```typescript
import { interval, of } from 'rxjs';
import { skipLast, take } from 'rxjs/operators';

// Edge case 1: each value is delayed by skipCount ticks
const start = Date.now();
interval(300).pipe(
  take(6),
  skipLast(2)
).subscribe(v => console.log(`${v} @ ~${Date.now() - start}ms`));
// Output: 0 @ ~900ms, 1 @ ~1200ms, 2 @ ~1500ms, 3 @ ~1800ms
// (0 appears only when tick 2 arrives; 4 and 5 are never emitted)

// Edge case 2: skipCount larger than the source — completes empty
of(1, 2).pipe(skipLast(5)).subscribe({
  next:     v  => console.log(v),
  complete: () => console.log('empty, done'),
});
// Output: empty, done

// Edge case 3: skipCount of 0 — identity
of(1, 2, 3).pipe(skipLast(0)).subscribe(console.log);
// Output: 1, 2, 3
```

## Common Pitfalls

### Anti-pattern: Expecting Real-Time Emission From an Infinite Stream
```typescript
import { interval } from 'rxjs';
import { skipLast, take } from 'rxjs/operators';

// ❌ INCORRECT — every value is delayed by 3 ticks, and the stream never
//    completes, so the most recent 3 values are never delivered
interval(500).pipe(
  skipLast(3)
).subscribe(console.log);
// 0 appears at ~2000ms, 1 at ~2500ms, ... — always three ticks behind

// ✅ CORRECT — bound the source first so "last" is well-defined
interval(500).pipe(
  take(10),
  skipLast(3)
).subscribe(console.log);
// Output: 0, 1, 2, 3, 4, 5, 6   (7, 8, 9 skipped; completes after the 10th tick)

// WHY: skipLast cannot identify the tail until completion. On an infinite
// source it degrades into a permanent delay line that silently withholds
// the newest values.
```

### Anti-pattern: Confusing `skipLast` With `takeLast`
```typescript
import { of } from 'rxjs';
import { skipLast, takeLast } from 'rxjs/operators';

// ❌ INCORRECT — meant to keep the final two values, but this discards them
of(1, 2, 3, 4, 5).pipe(skipLast(2)).subscribe(console.log);
// Output: 1, 2, 3

// ✅ CORRECT — takeLast keeps the tail; skipLast removes it
of(1, 2, 3, 4, 5).pipe(takeLast(2)).subscribe(console.log);
// Output: 4, 5

// WHY: The two operators are complements over the same tail window.
// skipLast(n) + takeLast(n) partition the source into "everything but the
// last n" and "only the last n".
```

### Anti-pattern: Using `skipLast` to Remove a Value by Content
```typescript
import { of } from 'rxjs';
import { skipLast, filter } from 'rxjs/operators';

// ❌ INCORRECT — assumes the sentinel is always last; breaks when it is not
of('a', 'END', 'b').pipe(skipLast(1)).subscribe(console.log);
// Output: a, END   (dropped 'b' instead of the sentinel)

// ✅ CORRECT — select by value when the position is not guaranteed
of('a', 'END', 'b').pipe(filter(v => v !== 'END')).subscribe(console.log);
// Output: a, b

// WHY: skipLast is positional. It only expresses "the final n", never
// "the ones that look like X". Use filter for content-based removal.
```

## Related Operators

**Same Category (Position-Based Selection)**:
- **`takeLast(n)`**: The complement — keeps only the last `n` values, emitted at completion
- **`skip(n)`**: Removes the first `n` values instead of the last; no buffering or delay
- **`last()`**: Emits only the final value; errors on empty
- **`elementAt(i)`**: Selects a single value by index

**Complementary Operators**:
- **`take(n)` / `takeUntil`**: Bound an infinite source so `skipLast` can see completion
- **`bufferCount(n)`**: Produces groups whose trailing partial group `skipLast(1)` can discard
- **`filter`**: Content-based removal when the position of unwanted values is not fixed

**Alternatives by Use Case**:

| Use Case | Instead of `skipLast` | Use This | Why |
|----------|-----------------------|----------|-----|
| Keep the tail, not drop it | `skipLast(n)` | `takeLast(n)` | Opposite selection over the same window |
| Drop a known marker value | `skipLast(1)` | `filter(v => v !== MARKER)` | Position may vary |
| Drop the first n instead | `skipLast(n)` | `skip(n)` | No buffering, no delay |
| Static array in hand | `from(arr).pipe(skipLast(n))` | `arr.slice(0, -n)` | Simpler when no stream is involved |

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/skipLast](https://rxjs.dev/api/operators/skipLast)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/operators/skiplast.html](http://reactivex.io/documentation/operators/skiplast.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/skipLast.ts](https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/skipLast.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Trailing-Window Suppression
- **Cognitive Load**: 2/5 — The selection is obvious; the delayed emission and infinite-source behavior are the only subtleties
- **Usage Frequency**: 2/5 — Occasional; most tail-trimming happens on arrays before they become streams
- **Composability**: 3/5 — Works anywhere a finite stream exists, but its lag can interact badly with time-based operators downstream

**Teaching Sequence**:
- **Prerequisites**: `take`, `skip`, `takeLast`, the idea that some operators need completion
- **Teaches**: Buffered delay lines, why "last" requires completion, the `skipLast` / `takeLast` partition
- **Leads to**: `bufferCount`, `pairwise`, window operators that also hold values back
- **Common with**: `takeLast`, `take`, `bufferCount`, `from`, `filter`
