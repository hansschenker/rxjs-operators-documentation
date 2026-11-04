# raceWith

## Identity
- **Name**: raceWith
- **Category**: Combination Operators
- **Type**: First-wins selector — subscribes to the source and every contender at once, mirrors whichever delivers the first notification, and unsubscribes all the others
- **Import**:
  ```typescript
  import { raceWith } from 'rxjs/operators';
  // RxJS 7.2+: also exported from the root
  import { raceWith } from 'rxjs';
  ```
- **Signature**:
  ```typescript
  function raceWith<T, A extends readonly unknown[]>(
    ...otherSources: [...ObservableInputTuple<A>]
  ): OperatorFunction<T, T | A[number]>
  ```

## Functional Specification

**Input**: `Observable<T>` — the source, plus any number of contender `ObservableInput` values whose element types form the tuple `A`

**Output**: `Observable<T | A[number]>` — an exact mirror of the winning contender, including its completion or error

**Transformation**:
Equivalent to `race(source$, ...otherSources)` written inside a pipe. All participants are subscribed in order. The first participant to deliver **any** notification — `next`, `error`, or `complete` — becomes the winner. On a winning `next`, every other participant is unsubscribed and the winner's full lifecycle is forwarded. On a winning `error` or `complete`, that terminal notification is forwarded immediately and the race is over.

**Mathematical representation**:
```
raceWith(B, C)(A) = race(A, B, C)

winner = argmin_{X ∈ {A, B, C}} t(first notification of X)
output = X_winner           (identity on the winner's whole sequence)

Ties: the participant that was subscribed first (argument order) wins,
      because subscriptions happen sequentially in that order.
```

**Invariants**:
- **Single winner**: Exactly one participant's notifications ever reach the output
- **Any notification wins**: `next`, `error`, and `complete` all count — a contender that completes empty or errors immediately still ends the race
- **Mirror after the win**: After the first `next`, the output is the winner's sequence verbatim, including its later error or completion
- **Losers are cancelled**: Non-winning participants are unsubscribed as soon as the winner emits; a synchronous winner prevents later participants from being subscribed at all

## Marble Diagram

```
Source:   --------1--2--3--|           (slowest)
other₁:   ----A--B--C--|               (first to emit — wins)
other₂:   ----------X--Y--|            (never gets a chance)
          raceWith(other₁, other₂)
Result:   ----A--B--C--|               (mirrors other₁; Source and other₂ unsubscribed at A)
```

**Synchronous participant always wins**:
```
Source:   (1|)                         (emits during subscription)
other₁:   --a--b--|
          raceWith(other₁)
Result:   (1|)

other₁ is never even subscribed — the race was decided before its turn came.
```

**Contender completes empty before anyone emits**:
```
Source:   ------1--2--|
other₁:   --|                          (completes at frame 2 without a value)
          raceWith(other₁)
Result:   --|

In RxJS 7 the empty completion is the first notification, so the result completes.
Source is unsubscribed; 1 and 2 are never seen.
```

**Contender errors before anyone emits**:
```
Source:   ------1--2--|
other₁:   ---#
          raceWith(other₁)
Result:   ---#

The error is forwarded immediately.
```

**Winner errors later**:
```
Source:   --1--2--#
other₁:   ----a--b--|
          raceWith(other₁)
Result:   --1--2--#                     (winner's error propagates; other₁ was cancelled at 1)
```

**Key observation**: The race is decided by the first *notification*, not the first *value*. An `EMPTY` or `throwError` contender does not drop out quietly — it wins instantly and ends the race. Guard contenders that may complete or fail before producing a value.

## Behavioral Characteristics

**Subscription**:
- Subscribes to the source first, then to each contender in argument order, all before returning
- If a participant delivers a notification *during* its own subscription (synchronous), the loop stops and later participants are never subscribed
- With no arguments, `raceWith()` returns the source unchanged

**Completion semantics**:
- After a winning `next`, the output completes exactly when the winner completes
- A contender that completes without emitting before any other notification ends the output immediately with that completion
- Once the winner is chosen, completion of a cancelled loser is irrelevant (it is no longer subscribed)

**Error handling**:
- An error from any participant before a winner is chosen is forwarded immediately and ends the race
- After a winner is chosen, only the winner's error can reach the output; losers are already unsubscribed
- No built-in recovery — wrap individual contenders in `catchError` if a failing contender should drop out instead of winning

**Backpressure**:
- None — no buffering; O(k) subscriptions until the winner emits, O(1) afterwards

**Hot vs. Cold**:
- Cold contenders are started by the race and actually cancelled when they lose (an `ajax` loser is aborted)
- Hot contenders keep running after they lose; only this subscription is dropped
- Passing a shared `WebSocketSubject` as a contender and letting it lose will unsubscribe it, which may close a connection used elsewhere — race `asObservable()` views instead

## Type System Integration

```typescript
/**
 * Type Parameters:
 *   T - Element type of the source
 *   A - Tuple of element types of otherSources, inferred from the arguments
 *
 * Input Types:  Observable<T>, ...[...ObservableInputTuple<A>]
 * Output Type:  Observable<T | A[number]>  — a union of every participant's element type
 *
 * Type Narrowing:
 *   Downstream code sees the union and must narrow if the participants
 *   emit different types. When all participants share a type, the union collapses.
 *
 * Type Safety:
 *   A contender that only errors (throwError) contributes `never`, which
 *   vanishes from the union — the output type stays T.
 */

import { fromEvent, timer } from 'rxjs';
import { raceWith, map } from 'rxjs/operators';

const click$   = fromEvent<MouseEvent>(document, 'click');          // Observable<MouseEvent>
const timeout$ = timer(10_000).pipe(map(() => 'auto' as const));    // Observable<'auto'>

const outcome$ = click$.pipe(raceWith(timeout$));
// outcome$: Observable<MouseEvent | 'auto'>

outcome$.subscribe(v => {
  if (v === 'auto') proceedAutomatically();
  else              handleClick(v);              // v is MouseEvent here
});
```

## Examples

### Basic Usage — Request Timeout
```typescript
import { of, timer } from 'rxjs';
import { raceWith, delay, map } from 'rxjs/operators';

const apiCall$ = of({ user: 'Alice', score: 42 }).pipe(delay(3000));
const timeout$ = timer(2000).pipe(
  map(() => { throw new Error('Request timed out after 2 seconds'); })
);

apiCall$.pipe(
  raceWith(timeout$)
).subscribe({
  next:  result => console.log('Got result:', result),
  error: err    => console.error(err.message)
});
// Output: Request timed out after 2 seconds
// (timeout$ errors at 2000ms, before apiCall$ emits at 3000ms; apiCall$ is cancelled)
```

### Common Pattern — Fastest of Several Equivalent Endpoints
```typescript
import { of } from 'rxjs';
import { raceWith, delay, map } from 'rxjs/operators';

const cdn1$ = of('https://cdn1.example.com/asset.js').pipe(delay(400), map(url => ({ url, data: 'from CDN1' })));
const cdn2$ = of('https://cdn2.example.com/asset.js').pipe(delay(200), map(url => ({ url, data: 'from CDN2' })));
const cdn3$ = of('https://cdn3.example.com/asset.js').pipe(delay(600), map(url => ({ url, data: 'from CDN3' })));

cdn1$.pipe(
  raceWith(cdn2$, cdn3$)
).subscribe(result => console.log(`Loaded from fastest CDN: ${result.url}`));
// Output (after 200ms): Loaded from fastest CDN: https://cdn2.example.com/asset.js
// cdn1$ and cdn3$ are unsubscribed the moment cdn2$ emits
```

### Common Pattern — First User Gesture Decides the Input Mode
```typescript
import { fromEvent } from 'rxjs';
import { raceWith, map, take } from 'rxjs/operators';

const firstTouch$ = fromEvent(document, 'touchstart').pipe(take(1), map(() => 'touch' as const));
const firstMouse$ = fromEvent(document, 'mousemove').pipe(take(1), map(() => 'mouse' as const));
const firstKey$   = fromEvent(document, 'keydown').pipe(take(1), map(() => 'keyboard' as const));

firstTouch$.pipe(
  raceWith(firstMouse$, firstKey$)
).subscribe(mode => {
  console.log(`User is using: ${mode} input`);
  document.body.dataset.inputMode = mode;
});
// Output (on the first gesture of any kind): User is using: mouse input
```

### Edge Cases — Empty Contender, Erroring Contender, No Arguments
```typescript
import { of, EMPTY, NEVER, throwError, timer } from 'rxjs';
import { raceWith, catchError } from 'rxjs/operators';

// Edge case 1: EMPTY completes synchronously — it wins, and the result completes empty
timer(100).pipe(raceWith(EMPTY)).subscribe({
  next:     v => console.log(v),
  complete: () => console.log('complete')
});
// Output: complete   (the timer never gets to emit)

// Edge case 2: throwError errors synchronously — it wins, and the result errors
timer(100).pipe(raceWith(throwError(() => new Error('boom')))).subscribe({
  next:  v => console.log(v),
  error: e => console.log('error:', e.message)
});
// Output: error: boom

// Edge case 3: make a fallible contender drop out instead of winning
timer(100).pipe(
  raceWith(throwError(() => new Error('boom')).pipe(catchError(() => NEVER)))
).subscribe(v => console.log('winner:', v));
// Output: winner: 0   (the failed contender was turned into NEVER and never wins)

// Edge case 4: no arguments — the operator is the identity
of('a', 'b').pipe(raceWith()).subscribe(console.log);
// Output: a, b
```

## Common Pitfalls

### Anti-pattern: Racing a Synchronous Participant
```typescript
import { of, timer } from 'rxjs';
import { raceWith, delay } from 'rxjs/operators';

// ❌ INCORRECT — of(1) emits during subscription; the race is over before the
// contenders are even subscribed. The delay(100) branch can never win.
of(1).pipe(
  raceWith(of(2), of(99).pipe(delay(100)))
).subscribe(console.log);
// Output: 1   (always)

// ✅ CORRECT — a race only makes sense between asynchronous participants
timer(300).pipe(
  raceWith(timer(100), timer(500))
).subscribe(() => console.log('timer(100) won'));
// Output (after 100ms): timer(100) won

// WHY: participants are subscribed sequentially, and a synchronous emission
// settles the race immediately. Argument order, not speed, decides the outcome.
```

### Anti-pattern: Letting an Empty or Failing Contender Win
```typescript
import { timer, throwError, NEVER, of } from 'rxjs';
import { raceWith, filter, catchError, concatWith } from 'rxjs/operators';

const data$ = timer(500);

// ❌ INCORRECT — the "optional" fast path completes without a value and ends the race
data$.pipe(
  raceWith(of(0).pipe(filter(() => false)))   // completes empty, synchronously
).subscribe({
  next:     v => console.log('value:', v),
  complete: () => console.log('complete')
});
// Output: complete   (data$ is cancelled; no value is ever delivered)

// ✅ CORRECT — a contender that may not produce a value must not complete; make it wait forever
data$.pipe(
  raceWith(of(0).pipe(filter(() => false), concatWith(NEVER)))
).subscribe(v => console.log('value:', v));
// Output: value: 0   (from data$ after 500ms)

// ✅ CORRECT — a contender that may fail should drop out, not win
data$.pipe(
  raceWith(throwError(() => new Error('flaky')).pipe(catchError(() => NEVER)))
).subscribe(v => console.log('value:', v));
// Output: value: 0

// WHY: in RxJS 7, completion and error count as the first notification. A
// contender that finishes empty or throws does not step aside — it wins and
// takes its terminal notification with it.
```

### Anti-pattern: Using `raceWith` When Every Result Is Needed
```typescript
import { forkJoin, combineLatest } from 'rxjs';
import { raceWith } from 'rxjs/operators';

// ❌ INCORRECT — only the fastest response survives; the other two are cancelled
fetchPreferences$().pipe(
  raceWith(fetchProfile$(), fetchPermissions$())
).subscribe(result => setup(result));   // gets one of three, chosen by latency

// ✅ CORRECT — all three must complete
forkJoin({
  preferences: fetchPreferences$(),
  profile:     fetchProfile$(),
  permissions: fetchPermissions$()
}).subscribe(all => setup(all));

// WHY: raceWith is for redundant or mutually exclusive sources (timeouts,
// mirrors, "first gesture"). When the sources carry different data, use
// forkJoin for one-shots or combineLatest for live values.
```

## Related Operators

**Same Category (Combination)**:
- **`mergeWith(...sources)`**: Forwards *every* participant's emissions — no winner, no cancellation
- **`combineLatestWith(...sources)`**: Waits for all participants and combines their latest values
- **`concatWith(...sources)`**: Sequential, not competitive
- **`zipWith(...sources)`**: Positional pairing of all participants

**Complementary Operators**:
- **`timer`**: The standard timeout contender
- **`timeout(ms)`**: Purpose-built for "error if the source is late" — simpler than a timer race
- **`takeUntil(notifier$)`**: Alternative to a race when the goal is to *stop* the source rather than replace it
- **`catchError(() => NEVER)`**: Turn a fallible contender into one that drops out instead of winning

**Alternatives by Use Case**:

| Use Case | Instead of `raceWith` | Use This | Why |
|----------|-----------------------|----------|-----|
| All participants known up front, no pipe needed | `a$.pipe(raceWith(b$))` | `race(a$, b$)` | Static form reads better when there is no primary source |
| Error when the source is too slow | `raceWith(timer(ms).pipe(map(() => { throw ... })))` | `timeout(ms)` | Built-in, clearer intent |
| Fall back to another source on timeout | custom race + error handling | `timeout({ each: ms, with: () => fallback$ })` | Declarative fallback |
| Need every participant's result | `raceWith` | `forkJoin` / `combineLatest` | No cancellation |
| Stop the source on a signal | `raceWith(signal$)` | `takeUntil(signal$)` | Completes instead of switching |

**Migration Notes**:
```typescript
// Deprecated API (RxJS 6 / early 7) — the operator form of race
import { race } from 'rxjs/operators';
source$.pipe(race(other$))

// Current API (RxJS 7+)
import { raceWith } from 'rxjs/operators';
source$.pipe(raceWith(other$))

// The creation function race(a$, b$) from 'rxjs' is unchanged and not deprecated.
```

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/raceWith](https://rxjs.dev/api/operators/raceWith)
- **ReactiveX Documentation** (`amb`): [http://reactivex.io/documentation/operators/amb.html](http://reactivex.io/documentation/operators/amb.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/raceWith.ts](https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/raceWith.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Competitive Selection (first-notification-wins arbitration)
- **Cognitive Load**: 3/5 — The mechanics are simple, but "any notification wins" and the synchronous-participant rule surprise most people at least once
- **Usage Frequency**: 2/5 — Timeouts and mirrors; `timeout()` covers the most common case more directly
- **Composability**: 3/5 — Composes well with `timer` and `catchError`; the union output type needs narrowing when participants differ

**Teaching Sequence**:
- **Prerequisites**: The static `race` creation function, subscription order, terminal notifications (`error`, `complete`)
- **Teaches**: Cancellation of losers; why completion and error are notifications too; why synchronous sources break races
- **Leads to**: `timeout`, `takeUntil`, `withLatestFrom`
- **Common with**: `race`, `timer`, `timeout`, `catchError`, `NEVER`
