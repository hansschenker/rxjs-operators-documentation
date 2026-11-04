# sequenceEqual

## Identity
- **Name**: sequenceEqual
- **Category**: Conditional Operators
- **Type**: Two-stream equality test — emits one boolean stating whether the source and a comparison stream emit the same values in the same order
- **Import**:
  ```typescript
  import { sequenceEqual } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  function sequenceEqual<T>(
    compareTo: ObservableInput<T>,
    comparator?: (a: T, b: T) => boolean   // default: (a, b) => a === b
  ): OperatorFunction<T, boolean>
  ```

## Functional Specification

**Input**: `Observable<T>` (the source) and `ObservableInput<T>` (`compareTo`) — two finite sequences to compare

**Output**: `Observable<boolean>` — exactly one boolean, then completion

**Transformation**:
Subscribes to both sequences and keeps a buffer for each. When a value arrives from one side and the other side has a buffered value, the two are compared with `comparator` in arrival order; a mismatch emits `false` immediately and tears down both subscriptions. If the other side has nothing buffered, the value is queued. When one side completes while the other still has unmatched values, or a value arrives after the other side has completed, the result is `false`. When both complete with nothing left unmatched, the result is `true`.

**Mathematical representation**:
```
sequenceEqual(B, eq)(A) = { |A| = |B| ∧ ∀ i : eq(aᵢ, bᵢ) }  ++ complete()

Evaluation:
  ∃ i : ¬eq(aᵢ, bᵢ)             → emit false at the ith comparison   (short-circuit)
  one side completes early      → emit false                        (length mismatch)
  both complete, all matched    → emit true
  both empty                    → emit true
```

**Invariants**:
- **Exactly one emission** in every non-error outcome
- **Order matters**: `{1,2}` vs `{2,1}` is `false`
- **Comparison is pairwise by index**, independent of relative timing between the two streams
- **Bounded by the lead**: the buffer holds only the values by which one stream is ahead of the other

## Marble Diagram

```
Source:    --1----2----3--|
compareTo: --1----2----3--|
           sequenceEqual(compareTo)
Result:    ---------------(true|)

Every pair matched; true at the later completion.
```

**Mismatch — short-circuit**:
```
Source:    --1----2----4--|
compareTo: --1----2----3--|
           sequenceEqual(compareTo)
Result:    ------------(false|)

4 ≠ 3; false at the third comparison. Neither completion is awaited.
```

**Length mismatch**:
```
Source:    --1----2--|
compareTo: --1----2----3--|
           sequenceEqual(compareTo)
Result:    ------------(false|)

compareTo produces a 3rd value after the source completed — lengths differ.
```

**Different timing, same values**:
```
Source:    -1-2-3-|
compareTo: -------1---2---3---|
           sequenceEqual(compareTo)
Result:    -------------------(true|)

Source values are buffered until compareTo catches up. Timing is irrelevant.
```

**Key observation**: `sequenceEqual` compares *sequences*, not *timelines*. Two streams that emit identical values at wildly different times are equal; two streams that emit at identical times but in different orders are not. The buffer makes timing invisible — and is also why the operator must only be used on finite streams.

## Behavioral Characteristics

**Subscription**:
- Subscribes to the source first, then to `compareTo` (via `innerFrom`)
- Both subscriptions are torn down together as soon as the result is known

**Completion semantics**:
- Completes immediately after its single boolean
- Requires both streams to be finite for a `true` result; `false` may arrive earlier
- If both streams complete empty, emits `true`

**Error handling**:
- An error on either side propagates immediately; no boolean is emitted
- A throwing `comparator` propagates as an error

**Backpressure**:
- Memory grows with the lead of the faster stream over the slower one — unbounded if one side is much faster or infinite
- No throttling of either source

**Hot vs. Cold**:
- Cold sources: deterministic comparison of full sequences
- Hot sources: only values after subscription count; a hot `compareTo` that already emitted will appear shorter than expected

## Type System Integration

```typescript
/**
 * Type Parameters:
 *   T - Element type shared by source and compareTo
 *
 * Input Types:  Observable<T>, ObservableInput<T>
 * Output Type:  Observable<boolean>
 *
 * Type Narrowing: none — the output is always boolean.
 *
 * Type Safety:
 *   compareTo must have the same element type as the source. The default
 *   comparator is ===, so object streams almost always need an explicit
 *   structural comparator.
 */

import { from } from 'rxjs';
import { sequenceEqual } from 'rxjs/operators';

interface Point { x: number; y: number }

const pathA: Point[] = [{ x: 0, y: 0 }, { x: 1, y: 1 }];
const pathB: Point[] = [{ x: 0, y: 0 }, { x: 1, y: 1 }];

// compareTo accepts any ObservableInput<T> — an array works directly
const samePath$ = from(pathA).pipe(
  sequenceEqual(pathB, (a: Point, b: Point) => a.x === b.x && a.y === b.y)
);

samePath$.subscribe((equal: boolean) => console.log('same path:', equal));
// Output: same path: true
```

## Examples

### Basic Usage — Compare Two Equal Sequences
```typescript
import { of } from 'rxjs';
import { sequenceEqual } from 'rxjs/operators';

of(1, 2, 3, 4, 5).pipe(
  sequenceEqual(of(1, 2, 3, 4, 5))
).subscribe(equal => console.log('Sequences are equal:', equal));
// Output: Sequences are equal: true

of(1, 2, 3).pipe(
  sequenceEqual(of(1, 2, 4))
).subscribe(equal => console.log('Sequences are equal:', equal));
// Output: Sequences are equal: false
```

### Common Pattern — Data Integrity Check Against an Expected Sequence
```typescript
import { from } from 'rxjs';
import { sequenceEqual } from 'rxjs/operators';

const expected = [10, 20, 30, 40];
const actual   = [10, 20, 99, 40]; // 30 vs 99

from(actual).pipe(
  sequenceEqual(expected)   // arrays are valid ObservableInput
).subscribe(equal => {
  console.log(equal
    ? 'Data integrity check passed'
    : 'Data integrity check FAILED: sequences do not match');
});
// Output: Data integrity check FAILED: sequences do not match
```

### Common Pattern — Structural Comparison With a Custom Comparator
```typescript
import { from } from 'rxjs';
import { sequenceEqual } from 'rxjs/operators';

interface Point { x: number; y: number }

const pathA: Point[] = [{ x: 0, y: 0 }, { x: 1, y: 1 }, { x: 2, y: 0 }];
const pathB: Point[] = [{ x: 0, y: 0 }, { x: 1, y: 1 }, { x: 2, y: 0 }];

// Default comparator is === → different object references → false
from(pathA).pipe(
  sequenceEqual(from(pathB))
).subscribe(equal => console.log('By reference:', equal));
// Output: By reference: false

// Structural comparator → true
from(pathA).pipe(
  sequenceEqual(from(pathB), (a, b) => a.x === b.x && a.y === b.y)
).subscribe(equal => console.log('By value:', equal));
// Output: By value: true
```

### Edge Cases — Timing, Length, Empty Streams
```typescript
import { of, EMPTY, interval, timer } from 'rxjs';
import { sequenceEqual, take } from 'rxjs/operators';

// Edge case 1: different timing, same values → true
// interval emits 0,1,2 at 100/200/300ms; timer emits 0,1,2 at 500/550/600ms
interval(100).pipe(
  take(3),
  sequenceEqual(timer(500, 50).pipe(take(3)))
).subscribe(equal => console.log('timing-independent:', equal));
// Output: timing-independent: true   (after ~600ms)

// Edge case 2: prefix match but different length → false
of(1, 2).pipe(
  sequenceEqual(of(1, 2, 3))
).subscribe(equal => console.log('length mismatch:', equal));
// Output: length mismatch: false

// Edge case 3: both empty → true
EMPTY.pipe(
  sequenceEqual(EMPTY)
).subscribe(equal => console.log('both empty:', equal));
// Output: both empty: true

// Edge case 4: same values, different order → false
of(1, 2).pipe(
  sequenceEqual(of(2, 1))
).subscribe(equal => console.log('reordered:', equal));
// Output: reordered: false
```

## Common Pitfalls

### Anti-pattern: Comparing Object Streams With the Default Comparator
```typescript
import { from } from 'rxjs';
import { sequenceEqual } from 'rxjs/operators';

// ❌ INCORRECT — === compares references; fresh object literals never match
from([{ id: 1 }, { id: 2 }]).pipe(
  sequenceEqual(from([{ id: 1 }, { id: 2 }]))
).subscribe(console.log);
// Output: false

// ✅ CORRECT — supply a structural comparator
from([{ id: 1 }, { id: 2 }]).pipe(
  sequenceEqual(from([{ id: 1 }, { id: 2 }]), (a, b) => a.id === b.id)
).subscribe(console.log);
// Output: true

// WHY: The default comparator is strict equality. Only primitives and
// shared references compare equal under ===.
```

### Anti-pattern: Using `sequenceEqual` on Infinite Streams
```typescript
import { interval } from 'rxjs';
import { sequenceEqual, take } from 'rxjs/operators';

// ❌ INCORRECT — neither stream completes; true can never be emitted,
//    and whichever side runs ahead buffers without bound
interval(100).pipe(
  sequenceEqual(interval(100))
).subscribe(equal => console.log('never printed:', equal));

// ✅ CORRECT — bound both sides
interval(100).pipe(
  take(5),
  sequenceEqual(interval(100).pipe(take(5)))
).subscribe(equal => console.log('equal:', equal));
// Output: equal: true

// WHY: A true result is a statement about complete sequences. Without
// completion the operator can only ever say false — and meanwhile it
// stores every value by which one side leads the other.
```

### Anti-pattern: Expecting Timing to Be Part of the Comparison
```typescript
import { timer, zip } from 'rxjs';
import { sequenceEqual, take, every, timestamp } from 'rxjs/operators';

// ❌ INCORRECT — hoping sequenceEqual will flag that slow$ arrived late
const fast$ = timer(0, 10).pipe(take(2));
const slow$ = timer(0, 500).pipe(take(2));

fast$.pipe(sequenceEqual(slow$)).subscribe(console.log);
// Output: true   (values 0,1 on both sides — timing ignored)

// ✅ CORRECT — compare timestamps explicitly if timing matters
zip(fast$.pipe(timestamp()), slow$.pipe(timestamp())).pipe(
  every(([a, b]) => Math.abs(a.timestamp - b.timestamp) < 50)
).subscribe(sameTiming => console.log('same timing:', sameTiming));
// Output: same timing: false

// WHY: sequenceEqual compares the ordered list of values, nothing else.
// Temporal equality needs timestamp(), TestScheduler marbles, or a
// comparator that includes time in the value.
```

## Related Operators

**Same Category (Boolean Reductions)**:
- **`every(predicate)`**: Single-stream universal check; same short-circuit-on-failure shape
- **`isEmpty()`**: Single-stream emptiness check
- **`defaultIfEmpty(v)`**: Completion-time substitution rather than a verdict

**Complementary Operators**:
- **`zip`**: Pairs the two streams by index — `zip(a$, b$).pipe(every(([a, b]) => a === b))` is a hand-rolled variant that ignores length mismatches
- **`distinctUntilChanged`**: Compares consecutive values within one stream, not two streams against each other
- **`toArray`**: Collect both sides and compare arrays when more detail than a boolean is needed (which index differed)

**Alternatives by Use Case**:

| Use Case | Instead of `sequenceEqual` | Use This | Why |
|----------|----------------------------|----------|-----|
| Need the first differing index | `sequenceEqual` | `zip` + `findIndex` | Boolean result is too coarse |
| Compare values pairwise, ignore length | `sequenceEqual` | `zip` + `every` | `zip` stops at the shorter stream |
| Assert in a unit test | `sequenceEqual` | `TestScheduler.expectObservable` | Also verifies timing |
| Detect change within one stream | `sequenceEqual` | `distinctUntilChanged` | Different question |

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/sequenceEqual](https://rxjs.dev/api/operators/sequenceEqual)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/operators/sequenceequal.html](http://reactivex.io/documentation/operators/sequenceequal.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/sequenceEqual.ts](https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/sequenceEqual.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Pairwise Sequence Comparison
- **Cognitive Load**: 2/5 — The intent is clear; the buffering model and reference-equality default are what trip people up
- **Usage Frequency**: 1/5 — Rare in application code; mostly tests, integrity checks, and replay verification
- **Composability**: 2/5 — Consumes two streams and yields one boolean; almost always terminal

**Teaching Sequence**:
- **Prerequisites**: `every`, `zip`, `===` vs structural equality, completion semantics
- **Teaches**: Multi-stream reductions, order-vs-time distinction, the cost of buffering for a lead
- **Leads to**: `TestScheduler` marble assertions, `zip`, `distinctUntilChanged` with comparators
- **Common with**: `from`, `of`, `take`, `every`, `zip`
