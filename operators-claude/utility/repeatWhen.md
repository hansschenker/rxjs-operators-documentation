# repeatWhen

## Identity
- **Name**: repeatWhen
- **Category**: Utility Operators
- **Type**: Notifier-driven resubscription on completion — Deprecated in RxJS 7, removed in RxJS 8. Hands a stream of source completions to a user function whose returned Observable decides when (and whether) to run the source again
- **Import**:
  ```typescript
  // Deprecated — still exported in RxJS 7
  import { repeatWhen } from 'rxjs/operators';

  // Replacement
  import { repeat } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  /** @deprecated Use repeat({ delay }) instead. */
  function repeatWhen<T>(
    notifier: (notifications: Observable<void>) => ObservableInput<any>
  ): MonoTypeOperatorFunction<T>
  ```

## Functional Specification

**Input**: `Observable<T>` — a cold, finite source that can be meaningfully re-executed

**Output**: `Observable<T>` — mirrors the source; resubscribes to it each time the notifier emits

**Transformation**:
On the **first** source completion, `repeatWhen` creates an internal `Subject<void>`, calls `notifier(notifications$)` exactly once, and subscribes to the Observable it returns. Every later completion pushes another `void` into `notifications$`. The notifier's signals steer the output:
- notifier **emits** (value ignored) → resubscribe to the source
- notifier **completes** → the output completes once the *current* source run has also completed
- notifier **errors** → the output errors immediately with that error

Source values pass straight through. Source **errors** propagate immediately; the notifier is never involved in error handling.

**Mathematical representation**:
```
repeatWhen(f)(S):
  run S;  on complete c₁:  C = Subject<void>();  N = f(C);  C.next()
  for each later completion cₖ:            C.next()
  on N.next(_):      resubscribe(S)
  on N.complete():   complete output after the in-flight run of S completes
  on N.error(x):     error output with x
  on S.error(e):     error output with e    (N is not consulted)

Equivalent (RxJS 7+):
  repeatWhen(n => n.pipe(delay(d)))              ≡ repeat({ delay: d })
  repeatWhen(n => n.pipe(take(k), delay(d)))     ≡ repeat({ count: k + 1, delay: d })
```

**Invariants**:
- **Single notifier call**: `notifier` runs once per output subscription, lazily on the first completion
- **Emission = repetition**: Each notifier emission triggers exactly one resubscription; emission values are discarded
- **Completion is graceful**: Notifier completion lets the current run finish before the output completes (unlike `retryWhen`, which completes at once)
- **Errors bypass**: A source error ends everything; `repeatWhen` never retries

## Marble Diagram

```
Source run 1:  --a--b--|
notifications$:        c₁
Notifier:              ----n                      (emits 40 time units after c₁)
Source run 2:              --a--b--|
Notifier:                          ----n
Source run 3:                          --a--b--|
               repeatWhen(n => n.pipe(delay(40)))
Result:        --a--b--------a--b--------a--b-- …  (repeats indefinitely)
```

**Notifier completes → output completes after the current run**:
```
Source run 1:  --a--b--|
Notifier:              -n|                       (take(1): one repetition, then complete)
Source run 2:            --a--b--|
Result:        --a--b-----a--b--|                (completes when run 2 completes)
```

**Source errors → no repetition**:
```
Source:        --a--#
Result:        --a--#                            (notifier never called)
```

**Key observation**: `repeatWhen` is `retryWhen` with the trigger moved from *error* to *completion*. The two share the same notifier mechanics and the same traps — a notifier that ignores its input stream, or completes early, will end the output unexpectedly. `repeat({ count, delay })` provides the same control with explicit options.

## Behavioral Characteristics

**Subscription**:
- Subscribes to the source immediately
- The notifier Observable is created and subscribed only after the first completion; sources that never complete never invoke it
- A synchronous notifier emission (e.g. `n => n`) resubscribes synchronously — a tight loop on a synchronous source

**Completion semantics**:
- Notifier completes while a run is in progress: the run finishes, then the output completes
- Notifier completes with no run in progress: the output completes immediately
- A source that never completes (`interval`, `fromEvent`, a live Subject) never triggers a repetition

**Error handling**:
- Source errors propagate immediately and unsubscribe the notifier
- Notifier errors terminate the output immediately, even mid-run
- A thrown exception inside `notifier` itself also terminates the output

**Backpressure**:
- O(1) state plus whatever the notifier pipe accumulates (`take` counters, pending `delay` timers)
- Each repetition on a cold source is a fresh execution

**Hot vs. Cold**:
- Cold sources: each repetition re-runs the producer — the intended use (polling, replaying a finite sequence)
- Hot sources: a completed Subject stays completed, so every resubscription completes instantly and the notifier fires again — effectively a busy loop if the notifier has no delay

## Type System Integration

```typescript
/**
 * Type Parameters:
 *   T - Value type (MonoTypeOperatorFunction<T> — preserved across repetitions)
 *
 * Input Type:  Observable<T>
 * Output Type: Observable<T>
 *
 * Notifier Type:
 *   (notifications: Observable<void>) => ObservableInput<any>
 *   The input stream carries no payload — only timing. Returned emission
 *   values are ignored, so the return type is effectively Observable<unknown>.
 */

import { timer } from 'rxjs';
import { ajax } from 'rxjs/ajax';
import { repeatWhen, mergeMap, takeWhile } from 'rxjs/operators';

interface JobStatus { done: boolean; progress: number }

const status$: Observable<JobStatus> = ajax.getJSON<JobStatus>('/api/job/7').pipe(
  repeatWhen(completions$ =>
    completions$.pipe(mergeMap((_: void, i: number) => timer(Math.min(1000 * (i + 1), 10_000))))
  ),
  takeWhile(s => !s.done, true)   // stop polling once the job reports done
);
```

## Examples

### Basic Usage — Repeat With a Fixed Delay (Deprecated Form and Replacement)
```typescript
import { of } from 'rxjs';
import { repeatWhen, delay, take, repeat } from 'rxjs/operators';

// Deprecated — shown for reading legacy code
of('ping').pipe(
  repeatWhen(notifications$ => notifications$.pipe(delay(1000))),
  take(3)
).subscribe({
  next:     v => console.log(v),
  complete: () => console.log('done')
});
// Output: ping (t=0), ping (t≈1 s), ping (t≈2 s), done

// ✅ Replacement — identical schedule
of('ping').pipe(
  repeat({ delay: 1000 }),
  take(3)
).subscribe(v => console.log(v));
// Output: ping, ping, ping
```

### Common Pattern — Fixed Number of Repetitions
```typescript
import { of } from 'rxjs';
import { repeatWhen, delay, take, repeat } from 'rxjs/operators';

// Deprecated — take(2) in the notifier allows two repetitions, then completes
of(1, 2, 3).pipe(
  repeatWhen(notifications$ => notifications$.pipe(delay(500), take(2)))
).subscribe({
  next:     v => console.log(v),
  complete: () => console.log('done')
});
// Output: 1 2 3   (500 ms)   1 2 3   (500 ms)   1 2 3   done   ← 3 total runs

// ✅ Replacement — count is the TOTAL number of runs after the first
of(1, 2, 3).pipe(
  repeat({ count: 2, delay: 500 })
).subscribe({
  next:     v => console.log(v),
  complete: () => console.log('done')
});
// Output: 1 2 3   1 2 3   1 2 3   done
```

### Common Pattern — Polling With Progressive Backoff
```typescript
import { timer } from 'rxjs';
import { ajax } from 'rxjs/ajax';
import { repeatWhen, mergeMap, takeWhile, repeat } from 'rxjs/operators';

interface JobStatus { done: boolean }

// Deprecated — the i-th completion waits (i + 1) seconds, capped at 10 s
ajax.getJSON<JobStatus>('/api/job/7').pipe(
  repeatWhen(completions$ =>
    completions$.pipe(mergeMap((_, i) => timer(Math.min(1000 * (i + 1), 10_000))))
  ),
  takeWhile(s => !s.done, true)
).subscribe(s => console.log('done?', s.done));
// Output: done? false (t=0), done? false (t≈1 s), done? false (t≈3 s), done? true (t≈6 s)

// ✅ Replacement — delay receives the 1-based repetition count
ajax.getJSON<JobStatus>('/api/job/7').pipe(
  repeat({ delay: count => timer(Math.min(1000 * count, 10_000)) }),
  takeWhile(s => !s.done, true)
).subscribe(s => console.log('done?', s.done));
```

### Edge Cases — Never-Completing Source, Notifier Completion Timing, Notifier Error
```typescript
import { interval, of, throwError, timer } from 'rxjs';
import { repeatWhen, take, delay, mergeMap } from 'rxjs/operators';

// Edge case 1: source never completes → repeatWhen never fires
interval(100).pipe(
  repeatWhen(n$ => n$.pipe(delay(1000)))
).subscribe(v => console.log(v));
// Output: 0, 1, 2, 3, …  (identical to interval(100) alone)

// Edge case 2: notifier completes mid-run → the run finishes first
of('a', 'b').pipe(
  delay(50),
  repeatWhen(n$ => n$.pipe(take(1)))      // one repetition, then notifier completes
).subscribe({
  next:     v => console.log(v),
  complete: () => console.log('complete')
});
// Output: a b a b complete     ← second run is NOT cut short

// Edge case 3: notifier errors → output errors, even between runs
of(1).pipe(
  repeatWhen(n$ => n$.pipe(mergeMap(() => throwError(() => new Error('stop')))))
).subscribe({
  next:  v   => console.log(v),
  error: err => console.log('error:', err.message)
});
// Output: 1, error: stop
```

## Common Pitfalls

### Anti-pattern: Still Using the Deprecated Form
```typescript
import { timer } from 'rxjs';
import { repeatWhen, delay, take, repeat } from 'rxjs/operators';

// ❌ INCORRECT — deprecated in RxJS 7, removed in RxJS 8
source$.pipe(repeatWhen(n$ => n$.pipe(delay(2000))));
source$.pipe(repeatWhen(n$ => n$.pipe(delay(2000), take(4))));

// ✅ CORRECT — repeat with explicit options
source$.pipe(repeat({ delay: 2000 }));
source$.pipe(repeat({ count: 4, delay: 2000 }));
source$.pipe(repeat({ delay: count => timer(count * 500) }));   // computed per repetition

// WHY: repeat({ count, delay }) covers every repeatWhen use. The delay function
// receives a 1-based repetition count and may return any Observable; when it
// errors, the output errors — the same escape hatch, without a notifier stream.
```

### Anti-pattern: Using `repeatWhen` to Recover From Errors
```typescript
import { throwError, of } from 'rxjs';
import { repeatWhen, delay, retry } from 'rxjs/operators';

// ❌ INCORRECT — the source ERRORS; repeatWhen only reacts to COMPLETION
flakyRequest$.pipe(
  repeatWhen(n$ => n$.pipe(delay(1000)))
).subscribe({ error: err => console.log('failed immediately:', err.message) });
// The notifier is never even created.

// ✅ CORRECT — retry handles errors; repeat handles completion; compose both if needed
flakyRequest$.pipe(
  retry({ count: 3, delay: 1000 }),   // inner: re-run on error
  repeat({ delay: 10_000 })           // outer: poll again after each success
).subscribe(render);

// WHY: Completion and error are different terminal notifications. repeatWhen
// subscribes to completions only; an error tears the whole chain down before
// any repetition logic runs.
```

### Anti-pattern: A Notifier That Ignores Its Input Stream
```typescript
import { timer } from 'rxjs';
import { repeatWhen, mergeMap } from 'rxjs/operators';

// ❌ INCORRECT — timer(1000) emits once and completes; you get ONE repetition, then completion
source$.pipe(
  repeatWhen(() => timer(1000))
).subscribe(render);

// ✅ CORRECT — derive the schedule from the completion stream so it keeps emitting
source$.pipe(
  repeatWhen(n$ => n$.pipe(mergeMap(() => timer(1000))))
).subscribe(render);

// WHY: The notifier is subscribed once and must emit once PER completion. A
// standalone timer is unrelated to the completions, emits once, and then
// completes — and that completion ends the output after the next run.
```

## Related Operators

**Same Category (Utility)**:
- **`repeat`**: The replacement — `repeat({ count, delay })` expresses every `repeatWhen` schedule declaratively
- **`delay`** / **`timer`**: Build the repetition schedule inside the notifier (or inside `repeat`'s `delay` function)
- **`finalize`**: Runs once when the whole repeated sequence ends, not per repetition

**Complementary Operators**:
- **`takeWhile`** / **`takeUntil`**: The usual stop condition for an otherwise infinite repeat loop
- **`retry`** / **`retryWhen`**: The error-driven twins — `retryWhen` is the deprecated sibling with identical notifier mechanics
- **`defer`**: Ensures each repetition re-runs the producer with fresh state

**Alternatives by Use Case**:

| Use Case | Instead of `repeatWhen` | Use This | Why |
|----------|-------------------------|----------|-----|
| Fixed-rate polling | `repeatWhen(n => n.pipe(delay(ms)))` | `repeat({ delay: ms })` or `interval(ms).pipe(switchMap(fetch))` | Explicit, no notifier |
| N repetitions | `repeatWhen(n => n.pipe(take(N)))` | `repeat(N)` | One argument |
| Backoff between runs | `repeatWhen` + `mergeMap` + `timer` | `repeat({ delay: count => timer(…) })` | Count is supplied |
| Re-run on error | `repeatWhen` | `retry` | Wrong trigger otherwise |

**Migration Notes**:
```typescript
// Deprecated API (RxJS 6 / early 7)
import { repeatWhen, delay, take, mergeMap } from 'rxjs/operators';
source$.pipe(repeatWhen(n$ => n$.pipe(delay(1000))));
source$.pipe(repeatWhen(n$ => n$.pipe(delay(1000), take(2))));
source$.pipe(repeatWhen(n$ => n$.pipe(mergeMap((_, i) => timer((i + 1) * 500)))));

// Current API (RxJS 7+)
import { repeat } from 'rxjs/operators';
source$.pipe(repeat({ delay: 1000 }));
source$.pipe(repeat({ count: 2, delay: 1000 }));
source$.pipe(repeat({ delay: count => timer(count * 500) }));
```

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/repeatWhen](https://rxjs.dev/api/operators/repeatWhen)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/operators/repeat.html](http://reactivex.io/documentation/operators/repeat.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/repeatWhen.ts](https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/repeatWhen.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Notifier-Controlled Resubscription on Completion (deprecated predecessor of `repeat({ delay })`)
- **Cognitive Load**: 4/5 — Same two-stream model as `retryWhen`, plus the "finish the current run first" completion rule
- **Usage Frequency**: 1/5 — Rare even in RxJS 6 code; polling was usually written with `interval` or `timer`
- **Composability**: 3/5 — Works inside pipelines, but the leaky notifier completion makes wrapping it fragile

**Teaching Sequence**:
- **Prerequisites**: `repeat`, `retry`, Subjects, `timer`, completion vs. error notifications
- **Teaches**: Symmetry between completion-driven and error-driven resubscription; why explicit config beats control streams
- **Leads to**: `repeat({ delay })`, `retry({ delay })`, `expand` for value-driven recursion
- **Common with**: `timer`, `delay`, `take`, `takeWhile`, `ajax`
