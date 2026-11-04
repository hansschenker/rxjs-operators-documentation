# concatWith

## Identity
- **Name**: concatWith
- **Category**: Combination Operators
- **Type**: Sequential append — after the source completes, subscribes to each additional source in turn, forwarding every emission in order without overlap
- **Import**:
  ```typescript
  import { concatWith } from 'rxjs/operators';
  // RxJS 7.2+: also exported from the root
  import { concatWith } from 'rxjs';
  ```
- **Signature**:
  ```typescript
  function concatWith<T, A extends readonly unknown[]>(
    ...otherSources: [...ObservableInputTuple<A>]
  ): OperatorFunction<T, T | A[number]>
  ```

## Functional Specification

**Input**: `Observable<T>` — the source, plus any number of `ObservableInput` values whose element types form the tuple `A`

**Output**: `Observable<T | A[number]>` — every value of the source, then every value of `otherSources[0]`, then `otherSources[1]`, and so on

**Transformation**:
Equivalent to `concat(source$, ...otherSources)` written inside a pipe. The operator subscribes to the source first. When the source completes, it subscribes to the next input; when that completes, the one after. Exactly one input is subscribed at any time. The output completes when the last input completes.

**Mathematical representation**:
```
concatWith(B, C)(A) = concat(A, B, C)
                    = A ++ B ++ C          (sequence concatenation)

subscribe(B) happens-after complete(A)
subscribe(C) happens-after complete(B)
|output| = |A| + |B| + |C|
```

**Invariants**:
- **Strict order**: Output order is source order, then each appended input in argument order; no interleaving is possible
- **Single live subscription**: At most one input is subscribed at any instant
- **Completion-gated**: The next input is subscribed only on the previous input's *completion*, never on an emission
- **Error short-circuits**: An error from any input terminates the output; later inputs are never subscribed

## Marble Diagram

```
Source:   --1--2--3--|
                     --4--5--|         (other₁ subscribed at source completion)
                             --6--|    (other₂ subscribed at other₁ completion)
          concatWith(other₁, other₂)
Result:   --1--2--3----4--5----6--|
```

**Error in an appended source**:
```
Source:   --1--2--|
                  --3--#              (other₁ errors)
          concatWith(other₁, other₂)
Result:   --1--2----3--#

other₂ is never subscribed.
```

**Source never completes**:
```
Source:   --1--2--3--4--5--6--...
          concatWith(other₁)
Result:   --1--2--3--4--5--6--...

other₁ is never subscribed — concatenation waits for a completion that never comes.
```

**Hot appended source** (late subscription misses values):
```
other₁:   -a--b--c--d--e--f--|        (hot: emitting regardless of subscribers)
Source:   --1--2--3--|
          concatWith(other₁)
Result:   --1--2--3----d--e--f--|

a, b, c were emitted before the source completed — they are lost.
```

**Key observation**: `concatWith` is about *completion*, not emission. Every appended input waits for the previous one to complete, which makes the operator useless on infinite sources and lossy on hot ones.

## Behavioral Characteristics

**Subscription**:
- Subscribes to the source immediately on output subscription
- Subscribes to each `otherSources[i]` lazily, only when the preceding input completes
- Cold inputs are therefore started late; hot inputs are observed late

**Completion semantics**:
- Output completes when the last appended input completes
- An input that completes without emitting is simply skipped over
- If any input never completes, all later inputs are never subscribed and the output never completes

**Error handling**:
- An error from the source or from any appended input propagates immediately
- The current input is unsubscribed; remaining inputs are never subscribed
- There is no built-in recovery — use `catchError` on the individual input or on the result

**Backpressure**:
- None — values are forwarded synchronously as they arrive; O(1) state
- Nothing is buffered because only one input is live at a time

**Hot vs. Cold**:
- Cold appended inputs behave intuitively: their producer starts exactly when the previous input completes
- Hot appended inputs are subscribed late, so values they emitted earlier are missed; buffer them with `ReplaySubject` or `shareReplay` if every value matters
- With `concatWith()` and no arguments the output is the source itself

## Type System Integration

```typescript
/**
 * Type Parameters:
 *   T - Element type of the source
 *   A - Tuple of element types of otherSources, inferred from the arguments
 *
 * Input Types:  Observable<T>, ...[...ObservableInputTuple<A>]
 * Output Type:  Observable<T | A[number]>  — a union, not a tuple
 *
 * Type Narrowing:
 *   The output is the union of every input's element type. Downstream code
 *   must narrow with typeof / instanceof / discriminants before using members
 *   that are not common to all branches.
 *
 * Type Safety:
 *   Mixing types compiles cleanly; the compiler only protects you if you
 *   actually narrow the union.
 */

import { of } from 'rxjs';
import { concatWith } from 'rxjs/operators';

const numbers$ = of(1, 2, 3);                       // Observable<number>
const label$   = of('end');                         // Observable<string>
const flag$    = of(true);                          // Observable<boolean>

const result$ = numbers$.pipe(concatWith(label$, flag$));
// result$: Observable<number | string | boolean>

result$.subscribe(v => {
  if (typeof v === 'number')  console.log('number', v * 2);
  if (typeof v === 'string')  console.log('label', v.toUpperCase());
  if (typeof v === 'boolean') console.log('flag', v);
});
```

## Examples

### Basic Usage — Append a Completion Marker
```typescript
import { of } from 'rxjs';
import { concatWith, delay } from 'rxjs/operators';

const records$ = of('record-1', 'record-2', 'record-3').pipe(delay(500));
const done$    = of('--- loading complete ---');

records$.pipe(
  concatWith(done$)
).subscribe(console.log);
// Output (after 500ms): record-1, record-2, record-3, --- loading complete ---
```

### Common Pattern — Intro, Main Content, Outro
```typescript
import { of, interval } from 'rxjs';
import { concatWith, map, take } from 'rxjs/operators';

const intro$ = of('Playing intro...');
const main$  = interval(500).pipe(take(4), map(i => `Content frame ${i + 1}`));
const outro$ = of('Playing outro...');

intro$.pipe(
  concatWith(main$, outro$)
).subscribe(console.log);
// Output: Playing intro..., Content frame 1, Content frame 2,
//         Content frame 3, Content frame 4, Playing outro...
```

### Common Pattern — Ordered Initialization Steps
```typescript
import { of } from 'rxjs';
import { concatWith, delay, tap, ignoreElements } from 'rxjs/operators';

// Each step is a cold Observable whose work starts only when the previous step completes
const loadConfig$ = of('config').pipe(
  delay(300),
  tap(() => console.log('Config loaded'))
);
const connectDb$ = of('db').pipe(
  delay(500),
  tap(() => console.log('Database connected'))
);
const startApp$ = of('App started');

loadConfig$.pipe(
  ignoreElements(),                                       // keep the side effect, drop the value
  concatWith(connectDb$.pipe(ignoreElements()), startApp$)
).subscribe(console.log);
// Output: Config loaded (300ms), Database connected (800ms), App started (800ms)
```

### Edge Cases — Empty Input, Error Mid-Chain, No Arguments
```typescript
import { of, EMPTY, throwError } from 'rxjs';
import { concatWith } from 'rxjs/operators';

// Edge case 1: an empty input is skipped over without affecting the result
of(1, 2).pipe(concatWith(EMPTY, of(3))).subscribe({
  next:     v => console.log(v),
  complete: () => console.log('complete')
});
// Output: 1, 2, 3, complete

// Edge case 2: an error in the middle aborts the rest of the chain
of(1).pipe(
  concatWith(throwError(() => new Error('step 2 failed')), of(3))
).subscribe({
  next:  v => console.log(v),
  error: e => console.log('error:', e.message)
});
// Output: 1, error: step 2 failed   (of(3) is never subscribed)

// Edge case 3: no arguments — the operator is the identity
of('a', 'b').pipe(concatWith()).subscribe(console.log);
// Output: a, b
```

## Common Pitfalls

### Anti-pattern: Appending to a Source That Never Completes
```typescript
import { interval, of } from 'rxjs';
import { concatWith, take } from 'rxjs/operators';

// ❌ INCORRECT — interval never completes, so of('done') is never subscribed
interval(1000).pipe(
  concatWith(of('done'))
).subscribe(console.log);
// Output: 0, 1, 2, 3, ...  ('done' never appears)

// ✅ CORRECT — bound the source so it can complete
interval(1000).pipe(
  take(3),
  concatWith(of('done'))
).subscribe(console.log);
// Output: 0, 1, 2, done

// WHY: concatWith subscribes to the next input on *completion* of the previous
// one. Infinite sources (interval, fromEvent, Subjects) never complete, so
// anything appended after them is dead code.
```

### Anti-pattern: Appending a Hot Source and Expecting All Its Values
```typescript
import { Subject, of, ReplaySubject } from 'rxjs';
import { concatWith, delay } from 'rxjs/operators';

const updates$ = new Subject<string>();

// ❌ INCORRECT — updates$ is subscribed only after the 1s delay; earlier values are lost
of('init').pipe(
  delay(1000),
  concatWith(updates$)
).subscribe(console.log);
updates$.next('u1');   // emitted at ~0ms — nobody is listening yet
// Output: init (1s), then only updates emitted after 1s

// ✅ CORRECT — buffer the hot source so late subscription replays what was missed
const buffered$ = new ReplaySubject<string>();
updates$.subscribe(buffered$);
of('init').pipe(
  delay(1000),
  concatWith(buffered$)
).subscribe(console.log);
updates$.next('u1');
// Output: init (1s), u1 (1s), then later updates

// WHY: appended inputs are subscribed lazily. A hot source does not wait for
// subscribers, so everything it emitted before the hand-over is gone unless
// something replays it.
```

### Anti-pattern: Ignoring the Union Return Type
```typescript
import { of } from 'rxjs';
import { concatWith } from 'rxjs/operators';

// ❌ INCORRECT — the cast lies: the first three values are numbers
of(1, 2, 3).pipe(
  concatWith(of('end'))
).subscribe(value => {
  console.log((value as string).toUpperCase());   // TypeError at runtime for 1, 2, 3
});

// ✅ CORRECT — narrow the union before using branch-specific members
of(1, 2, 3).pipe(
  concatWith(of('end'))
).subscribe(value => {
  if (typeof value === 'string') console.log('status:', value.toUpperCase());
  else                           console.log('number:', value * 2);
});
// Output: number: 2, number: 4, number: 6, status: END

// WHY: the output type is T | A[number]. TypeScript will happily compile a cast,
// but only a real type guard keeps the runtime honest.
```

## Related Operators

**Same Category (Combination)**:
- **`mergeWith(...sources)`**: Subscribes to all inputs at once and interleaves — use when order between inputs does not matter
- **`concatAll()`**: Sequential flattening of an `Observable<Observable<T>>` when the inputs arrive dynamically
- **`startWith(...values)`**: Prepends synchronous values *before* the source — the mirror image for values
- **`endWith(...values)`**: Appends synchronous *values* (not Observables) after the source — the lightweight cousin

**Complementary Operators**:
- **`take` / `takeUntil`**: Give an infinite source a completion so something can be appended
- **`ignoreElements`**: Keep a step's side effects and completion but drop its values — the "then" of a pipeline
- **`catchError`**: Recover inside an appended input so one failure does not abort the chain
- **`ReplaySubject` / `shareReplay`**: Buffer hot inputs that will be subscribed late

**Alternatives by Use Case**:

| Use Case | Instead of `concatWith` | Use This | Why |
|----------|-------------------------|----------|-----|
| All inputs known up front, no pipe needed | `a$.pipe(concatWith(b$))` | `concat(a$, b$)` | Static form reads better when there is no primary source |
| Order between inputs is irrelevant | `concatWith(b$)` | `mergeWith(b$)` | No waiting on completion |
| Append plain values | `concatWith(of(x))` | `endWith(x)` | Simpler, no Observable wrapper |
| Inputs produced per value of the source | `concatWith` | `concatMap(v => f(v))` | Sequential flattening with a projection |

**Migration Notes**:
```typescript
// Deprecated API (RxJS 6 / early 7) — the operator form of concat
import { concat } from 'rxjs/operators';
source$.pipe(concat(other$))

// Current API (RxJS 7+)
import { concatWith } from 'rxjs/operators';
source$.pipe(concatWith(other$))

// The creation function concat(a$, b$) from 'rxjs' is unchanged and not deprecated.
```

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/concatWith](https://rxjs.dev/api/operators/concatWith)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/operators/concat.html](http://reactivex.io/documentation/operators/concat.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/concatWith.ts](https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/concatWith.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Sequential Composition (completion-gated hand-over between streams)
- **Cognitive Load**: 2/5 — The mechanics are simple; the two things to internalize are "waits for completion" and "late subscription to hot inputs"
- **Usage Frequency**: 3/5 — Common in setup sequences, animations, and "then" pipelines; the static `concat` is used at least as often
- **Composability**: 4/5 — Slots naturally at the end of a pipe; pairs with `ignoreElements`, `take`, and `endWith`

**Teaching Sequence**:
- **Prerequisites**: Completion semantics, the static `concat` creation function, cold vs. hot Observables
- **Teaches**: Completion as a control signal; why infinite sources block sequential composition
- **Leads to**: `concatMap` (sequential flattening with a projection), `concatAll`, `exhaustMap`
- **Common with**: `concat`, `endWith`, `startWith`, `ignoreElements`, `take`
