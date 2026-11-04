# zipWith

## Identity
- **Name**: zipWith
- **Category**: Combination Operators
- **Type**: Index-aligned pairing — emits the Nth tuple only when the source and every other input have each produced their Nth value
- **Import**:
  ```typescript
  import { zipWith } from 'rxjs/operators';
  // RxJS 7.2+: also exported from the root
  import { zipWith } from 'rxjs';
  ```
- **Signature**:
  ```typescript
  function zipWith<T, A extends readonly unknown[]>(
    ...otherInputs: [...ObservableInputTuple<A>]
  ): OperatorFunction<T, Cons<T, A>>      // Cons<T, A> = [T, ...A]
  ```

## Functional Specification

**Input**: `Observable<T>` — the source, plus any number of `ObservableInput` values whose element types form the tuple `A`

**Output**: `Observable<[T, ...A]>` — tuples whose first element is the source's Nth value and whose remaining elements are each other input's Nth value, in argument order

**Transformation**:
Equivalent to `zip(source$, ...otherInputs)` written inside a pipe. Every input gets its own FIFO buffer. Each arriving value is appended to its input's buffer; whenever every buffer is non-empty, one value is shifted from each and emitted together as a tuple. Pairing is strictly positional — timing plays no role except in determining *when* a tuple becomes available.

**Mathematical representation**:
```
zipWith(B, C)(A) = zip(A, B, C)
                 = { (aₙ, bₙ, cₙ) : n = 0, 1, 2, ... }

emit(n) happens-after max(arrive(aₙ), arrive(bₙ), arrive(cₙ))
|output| = min(|A|, |B|, |C|)
```

**Invariants**:
- **Positional pairing**: The Nth output tuple contains exactly the Nth value of every input — never a "latest" value
- **Lossless up to the shortest input**: No value from any input is skipped or reused
- **Shortest input wins**: Output length equals the length of the shortest input
- **Unbounded buffering**: A faster input's surplus values are held in memory until the slower inputs catch up

## Marble Diagram

```
Source:   --1----2------3-----4--|
other:    ----a------b------c--|
          zipWith(other)
Result:   ----[1,a]--[2,b]--[3,c]|

4 is never paired: other completes after c, so no fourth tuple is possible.
```

**Fast source, slow other** (buffering):
```
Source:   -1-2-3-4-5-----------------|
other:    -------a-------b-------c---|
          zipWith(other)
Result:   -------[1,a]---[2,b]---[3,c]|

Values 1..5 arrive in a burst and sit in the buffer; a, b, c release them one by one.
4 and 5 are still buffered when other completes — then discarded.
```

**Synchronous inputs**:
```
Source:   (123|)
other:    (abc|)
          zipWith(other)
Result:   ([1,a][2,b][3,c]|)

All three tuples are emitted synchronously during subscription.
```

**Completed input with buffered values** (pairing continues):
```
Source:   (123|)                     completes immediately, 3 values buffered
other:    ----a----b----c----d--|
          zipWith(other)
Result:   ----[1,a]-[2,b]-[3,c]|

Output completes at [3,c]: the source is done and its buffer is now empty.
d is never paired.
```

**Key observation**: `zipWith` is a queue, not a snapshot. Compare `combineLatestWith`, which re-emits the *latest* value of every input whenever any input emits. If you find yourself wanting "the current value of the other stream", you want `withLatestFrom` or `combineLatestWith`, not `zipWith`.

## Behavioral Characteristics

**Subscription**:
- Subscribes to the source and to every other input immediately on output subscription, in argument order
- All inputs are consumed concurrently; none waits for another to subscribe

**Completion semantics**:
- Output completes as soon as any input has completed **and** that input's buffer is empty — no further tuple could ever be formed
- An input that completes while still holding buffered values keeps contributing until its buffer drains
- If every input is infinite, the output never completes

**Error handling**:
- An error from any input propagates immediately; all other inputs are unsubscribed
- Buffered values are discarded on error

**Backpressure**:
- Per-input FIFO buffers with no upper bound — O(n) memory in the rate difference between the fastest and slowest input
- There is no drop or sample policy; the only remedies are rate-limiting upstream or choosing another operator

**Hot vs. Cold**:
- Works with both; each input is subscribed once
- With hot inputs, pairing starts at the Nth value *since subscription*, not since the input started emitting
- Two hot inputs with different rates are the classic unbounded-buffer scenario

## Type System Integration

```typescript
/**
 * Type Parameters:
 *   T - Element type of the source
 *   A - Tuple of element types of otherInputs, inferred from the arguments
 *
 * Input Types:  Observable<T>, ...[...ObservableInputTuple<A>]
 * Output Type:  Observable<Cons<T, A>>  — i.e. Observable<[T, ...A]>
 *
 * Type Narrowing:
 *   The output is a positional tuple, so array destructuring in map/subscribe
 *   is fully typed: [source, first, second, ...].
 *
 * Type Safety:
 *   Arity and element types are checked; passing a non-ObservableInput fails to compile.
 */

import { of } from 'rxjs';
import { zipWith, map } from 'rxjs/operators';

const ids$   = of(101, 102, 103);                // Observable<number>
const names$ = of('Alice', 'Bob', 'Carol');      // Observable<string>
const roles$ = of('admin', 'editor', 'viewer');  // Observable<string>

const users$ = ids$.pipe(
  zipWith(names$, roles$),                       // Observable<[number, string, string]>
  map(([id, name, role]) => ({ id, name, role }))
);
// users$: Observable<{ id: number; name: string; role: string }>
```

## Examples

### Basic Usage — Pair Two Sequences by Index
```typescript
import { of } from 'rxjs';
import { zipWith } from 'rxjs/operators';

const letters$ = of('a', 'b', 'c');
const numbers$ = of(1, 2, 3);

letters$.pipe(
  zipWith(numbers$)
).subscribe(pair => console.log(pair));
// Output: ['a', 1], ['b', 2], ['c', 3]
```

### Common Pattern — Assemble Records From Parallel Columns
```typescript
import { of } from 'rxjs';
import { zipWith, map } from 'rxjs/operators';

const ids$   = of(101, 102, 103);
const names$ = of('Alice', 'Bob', 'Charlie');
const roles$ = of('admin', 'editor', 'viewer');

ids$.pipe(
  zipWith(names$, roles$),
  map(([id, name, role]) => ({ id, name, role }))
).subscribe(console.log);
// Output: { id: 101, name: 'Alice', role: 'admin' }
//         { id: 102, name: 'Bob', role: 'editor' }
//         { id: 103, name: 'Charlie', role: 'viewer' }
```

### Common Pattern — Pace a Burst Against a Clock
```typescript
import { from, interval } from 'rxjs';
import { zipWith, map } from 'rxjs/operators';

// All four words are available immediately; the clock ticks once per second
const words$ = from(['reactive', 'programming', 'is', 'fun']);
const tick$  = interval(1000);

// The buffered words are released one per tick, strictly in order
words$.pipe(
  zipWith(tick$),
  map(([word]) => word)
).subscribe(console.log);
// Output (one per second): reactive, programming, is, fun
// The result completes after 'fun': words$ is done and its buffer is empty.
```

### Edge Cases — Unequal Lengths, Empty Input, No Arguments
```typescript
import { of, EMPTY, interval } from 'rxjs';
import { zipWith, take } from 'rxjs/operators';

// Edge case 1: unequal lengths — output stops at the shortest input
of(1, 2, 3, 4, 5).pipe(zipWith(of('a', 'b', 'c'))).subscribe({
  next:     pair => console.log(pair),
  complete: () => console.log('complete')
});
// Output: [1,'a'], [2,'b'], [3,'c'], complete   (4 and 5 are dropped)

// Edge case 2: an empty input — nothing can ever be paired
of(1, 2, 3).pipe(zipWith(EMPTY)).subscribe({
  next:     pair => console.log(pair),
  complete: () => console.log('complete')
});
// Output: complete   (no tuples)

// Edge case 3: completed source still pairs from its buffer
of(1, 2, 3).pipe(zipWith(interval(100))).subscribe({
  next:     pair => console.log(pair),
  complete: () => console.log('complete')
});
// Output: [1,0] (100ms), [2,1] (200ms), [3,2] (300ms), complete
```

## Common Pitfalls

### Anti-pattern: Using `zipWith` When You Mean `combineLatestWith`
```typescript
import { of, BehaviorSubject } from 'rxjs';
import { zipWith, combineLatestWith } from 'rxjs/operators';

const user$     = new BehaviorSubject({ name: 'Ada' });
const settings$ = new BehaviorSubject({ theme: 'dark' });

// ❌ INCORRECT — pairs by index: after the first tuple, a user change waits for a
// settings change (and vice versa) before anything is emitted again
user$.pipe(zipWith(settings$)).subscribe(([u, s]) => render(u, s));
user$.next({ name: 'Grace' });   // nothing happens — waiting for settings$[1]

// ✅ CORRECT — live state wants the latest of each, whenever either changes
user$.pipe(combineLatestWith(settings$)).subscribe(([u, s]) => render(u, s));
user$.next({ name: 'Grace' });   // renders immediately with the current theme

// WHY: zipWith is a positional queue — the Nth output needs the Nth value of
// every input. Reactive state is not positional; it is "latest wins".
```

### Anti-pattern: Zipping Infinite Inputs With Different Rates
```typescript
import { interval } from 'rxjs';
import { zipWith, withLatestFrom, sampleTime } from 'rxjs/operators';

const fast$ = interval(10);     // ~100 values per second
const slow$ = interval(1000);   // 1 value per second

// ❌ INCORRECT — fast$ accumulates ~99 unpaired values every second, forever
fast$.pipe(zipWith(slow$)).subscribe(console.log);

// ✅ CORRECT — if you only need the latest slow value when fast emits:
fast$.pipe(withLatestFrom(slow$)).subscribe(console.log);

// ✅ CORRECT — if you really want one pair per slow tick, rate-limit the fast side first:
fast$.pipe(sampleTime(1000), zipWith(slow$)).subscribe(console.log);

// WHY: zipWith buffers without limit. Two unbounded inputs with different rates
// grow the faster input's buffer linearly for the life of the subscription —
// a memory leak that only shows up in long-running sessions.
```

### Anti-pattern: Expecting the Longer Input to Finish
```typescript
import { of, from } from 'rxjs';
import { zipWith, map } from 'rxjs/operators';

const questions$ = from(['Q1', 'Q2', 'Q3', 'Q4']);
const answers$   = of('A1', 'A2');

// ❌ INCORRECT — the author expects four lines; only two are ever emitted
questions$.pipe(
  zipWith(answers$),
  map(([q, a]) => `${q}: ${a}`)
).subscribe(console.log);
// Output: Q1: A1, Q2: A2   — then complete; Q3 and Q4 are silently dropped

// ✅ CORRECT — pad the shorter input, or guard the lengths explicitly
import { concatWith } from 'rxjs/operators';
questions$.pipe(
  zipWith(answers$.pipe(concatWith(of('(no answer)', '(no answer)')))),
  map(([q, a]) => `${q}: ${a}`)
).subscribe(console.log);
// Output: Q1: A1, Q2: A2, Q3: (no answer), Q4: (no answer)

// WHY: the output completes when any input is exhausted. Surplus values from
// longer inputs are discarded without any notification.
```

## Related Operators

**Same Category (Combination)**:
- **`combineLatestWith(...sources)`**: Emits on *any* input using the latest value of each — for reactive state
- **`withLatestFrom(...sources)`**: Emits only when the *source* emits, sampling the others — no buffering
- **`mergeWith(...sources)`**: Interleaves all inputs without pairing
- **`concatWith(...sources)`**: Appends inputs sequentially without pairing

**Complementary Operators**:
- **`map`**: Turn the positional tuple into a named object
- **`sampleTime` / `throttleTime`**: Rate-limit a fast input before zipping to keep buffers bounded
- **`take(n)`**: Bound an infinite input so the zip can complete
- **`interval` / `timer`**: The usual "clock" partner for pacing a burst

**Alternatives by Use Case**:

| Use Case | Instead of `zipWith` | Use This | Why |
|----------|----------------------|----------|-----|
| All inputs known up front, no pipe needed | `a$.pipe(zipWith(b$))` | `zip(a$, b$)` | Static form reads better when there is no primary source |
| Combine live state | `zipWith` | `combineLatestWith` | Latest-wins, no positional queue |
| Enrich source values with another stream's current value | `zipWith` | `withLatestFrom` | Sampling, no buffering |
| Pair only final values of one-shot streams | `zipWith` | `forkJoin` | Waits for completion, one tuple |
| Inputs arrive dynamically | `zipWith` | `zipAll` | Higher-order form |

**Migration Notes**:
```typescript
// Deprecated API (RxJS 6 / early 7) — the operator form of zip
import { zip } from 'rxjs/operators';
source$.pipe(zip(other$))

// Current API (RxJS 7+)
import { zipWith } from 'rxjs/operators';
source$.pipe(zipWith(other$))

// The creation function zip(a$, b$) from 'rxjs' is unchanged and not deprecated.
```

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/zipWith](https://rxjs.dev/api/operators/zipWith)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/operators/zip.html](http://reactivex.io/documentation/operators/zip.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/zipWith.ts](https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/zipWith.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Positional Join (lock-step pairing of parallel sequences)
- **Cognitive Load**: 3/5 — The index-versus-latest distinction and the unbounded buffer are two genuinely counter-intuitive points
- **Usage Frequency**: 2/5 — Occasional; most "combine two streams" needs are better served by `combineLatestWith` or `withLatestFrom`
- **Composability**: 3/5 — Composes cleanly but demands finite or rate-matched inputs

**Teaching Sequence**:
- **Prerequisites**: The static `zip` creation function, `combineLatest`, completion semantics
- **Teaches**: Positional versus temporal combination; buffering as a hidden cost; shortest-input completion
- **Leads to**: `withLatestFrom`, `zipAll`, `forkJoin`
- **Common with**: `zip`, `map`, `interval`, `combineLatestWith`, `withLatestFrom`
