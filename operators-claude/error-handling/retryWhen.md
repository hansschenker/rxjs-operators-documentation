# retryWhen

## Identity
- **Name**: retryWhen
- **Category**: Error Handling Operators
- **Type**: Notifier-driven resubscription on error — Deprecated in RxJS 7, removed in RxJS 8. Hands the stream of source errors to a user function whose returned Observable decides when (and whether) to retry
- **Import**:
  ```typescript
  // Deprecated — still exported in RxJS 7
  import { retryWhen } from 'rxjs/operators';

  // Replacement
  import { retry } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  /** @deprecated Use retry({ delay }) instead. */
  function retryWhen<T>(
    notifier: (errors: Observable<any>) => ObservableInput<any>
  ): MonoTypeOperatorFunction<T>
  ```

## Functional Specification

**Input**: `Observable<T>` — a cold source that may error and can be meaningfully re-executed

**Output**: `Observable<T>` — mirrors the source; resubscribes to it each time the notifier emits

**Transformation**:
On the **first** source error, `retryWhen` creates an internal `Subject` of errors, calls `notifier(errors$)` exactly once, and subscribes to the Observable it returns. Every source error (the first and all later ones) is pushed into `errors$`. From then on three notifier signals control the output:
- notifier **emits** (any value — the value is ignored) → unsubscribe the failed source and resubscribe to it
- notifier **completes** → the output completes immediately
- notifier **errors** → the output errors with that error

Source values pass straight through. Source completion completes the output; the notifier is not consulted.

**Mathematical representation**:
```
retryWhen(f)(S):
  run S;  on error e₁:  E = Subject();  N = f(E);  E.next(e₁)
  for each error eₖ (k ≥ 2):           E.next(eₖ)
  on N.next(_):       resubscribe(S)
  on N.complete():    complete output
  on N.error(x):      error output with x

Equivalent (RxJS 7+):
  retryWhen(errs => errs.pipe(delay(d)))         ≡ retry({ delay: d })
  retryWhen(errs => errs.pipe(mergeMap(g)))      ≡ retry({ delay: (e, i) => g(e, i - 1) })
```

**Invariants**:
- **Single notifier call**: `notifier` runs once per output subscription, lazily on the first error — never on subscribe, never per retry
- **Emission = retry**: Each notifier emission produces exactly one resubscription; emission values are discarded
- **Completion is silent**: If the notifier completes, the output completes *without* an error — the last source error is lost
- **Values are transparent**: Successful emissions are forwarded unchanged; the retry counter (if any) lives in the notifier's own pipe

## Marble Diagram

```
Source run 1:  --a--b--#
errors$:               e₁
Notifier:              ----n                    (emits 40 time units after e₁)
Source run 2:              --a--b--c--|
               retryWhen(errs => errs.pipe(delay(40)))
Result:        --a--b--------a--b--c--|

The error is swallowed, the notifier waits, then the source is re-run from scratch.
```

**Notifier completes → output completes silently**:
```
Source run 1:  --a--#
errors$:            e₁
Notifier:           |                            (take(0), or a finite stream that ended)
Result:        --a--|                            (no error reaches the subscriber)
```

**Notifier errors → output errors**:
```
Source run 1:  --a--#
Notifier:           ---#(x)                      (throwError inside the notifier)
Result:        --a-----#(x)
```

**Key observation**: The notifier is a *second stream* that runs alongside the retries, not a callback per error. That is what made `retryWhen` flexible (any operator chain can shape the retry schedule) and also what made it error-prone: the notifier's own completion and error channels leak into the output. `retry({ delay })` keeps the same expressive power — the `delay` function returns an Observable per error — while fixing the completion footgun.

## Behavioral Characteristics

**Subscription**:
- Subscribes to the source immediately
- The notifier Observable is created and subscribed only after the first error; successful streams never invoke it
- Synchronous notifier emissions (e.g. `errs => errs`) trigger an immediate, synchronous resubscription

**Completion semantics**:
- Source completes: output completes; the notifier subscription is torn down
- Notifier completes: output completes immediately, even if a retry is mid-flight
- Notifier never emits and never completes (e.g. `NEVER`): the output hangs after the first error

**Error handling**:
- Source errors are intercepted and routed to `errors$`; they never reach the subscriber directly
- Notifier errors terminate the output — the idiomatic way to "give up" is to `throwError` inside the notifier
- A thrown exception inside `notifier` itself also terminates the output

**Backpressure**:
- O(1) state plus whatever the notifier pipe accumulates (`scan` counters, `delay` timers)
- Each retry on a cold source is a fresh execution (new HTTP request, new connection)

**Hot vs. Cold**:
- Cold sources: retry re-executes the producer — the intended use
- Hot sources: retry only resubscribes to the shared multicaster; values emitted during the retry gap are missed, and a Subject that has errored stays errored, so the retry fails again immediately

## Type System Integration

```typescript
/**
 * Type Parameters:
 *   T - Value type (MonoTypeOperatorFunction<T> — preserved across retries)
 *
 * Input Type:  Observable<T>
 * Output Type: Observable<T>
 *
 * Notifier Type:
 *   (errors: Observable<any>) => ObservableInput<any>
 *   The error stream is untyped (`any`) — narrow with a type guard inside
 *   the notifier if you branch on error kind. Emission values are ignored.
 */

import { timer, throwError } from 'rxjs';
import { ajax, AjaxError } from 'rxjs/ajax';
import { retryWhen, mergeMap } from 'rxjs/operators';

interface Order { id: string }

const order$: Observable<Order> = ajax.getJSON<Order>('/api/order/1').pipe(
  retryWhen(errors$ =>
    errors$.pipe(
      mergeMap((err: unknown, i: number) =>
        err instanceof AjaxError && err.status >= 500 && i < 2
          ? timer(1000)              // retry 5xx twice
          : throwError(() => err)    // anything else: give up with the original error
      )
    )
  )
);
```

## Examples

### Basic Usage — Fixed Delay Between Retries (Deprecated Form and Replacement)
```typescript
import { defer, throwError, of, timer } from 'rxjs';
import { retryWhen, delay, retry } from 'rxjs/operators';

let attempt = 0;
const flaky$ = defer(() => {
  attempt++;
  return attempt < 3
    ? throwError(() => new Error(`fail #${attempt}`))
    : of(`success on attempt ${attempt}`);
});

// Deprecated — shown for reading legacy code
flaky$.pipe(
  retryWhen(errors$ => errors$.pipe(delay(1000)))   // wait 1 s after every error
).subscribe({
  next:     v => console.log(v),
  complete: () => console.log('done')
});
// Output (after ~2 s): success on attempt 3, done

// ✅ Replacement — identical schedule
attempt = 0;
flaky$.pipe(
  retry({ delay: 1000 })
).subscribe(v => console.log(v));
// Output (after ~2 s): success on attempt 3
```

### Common Pattern — Exponential Backoff With a Retry Cap
```typescript
import { timer, throwError } from 'rxjs';
import { ajax } from 'rxjs/ajax';
import { retryWhen, mergeMap, retry } from 'rxjs/operators';

const MAX_RETRIES = 3;
const backoff = (i: number) => Math.min(1000 * 2 ** i, 16_000); // 1 s, 2 s, 4 s … capped

// Deprecated — the classic retryWhen recipe
ajax.getJSON('/api/data').pipe(
  retryWhen(errors$ =>
    errors$.pipe(
      mergeMap((err, i) => {
        if (i >= MAX_RETRIES) return throwError(() => err); // give up: surface the error
        console.log(`retry #${i + 1} in ${backoff(i)} ms`);
        return timer(backoff(i));
      })
    )
  )
).subscribe({ error: err => console.error('failed:', err.message) });

// ✅ Replacement — count and delay are first-class options
ajax.getJSON('/api/data').pipe(
  retry({
    count: MAX_RETRIES,
    delay: (err, retryCount) => {       // retryCount is 1-based
      console.log(`retry #${retryCount} in ${backoff(retryCount - 1)} ms`);
      return timer(backoff(retryCount - 1));
    }
  })
).subscribe({ error: err => console.error('failed:', err.message) });
// Output: retry #1 in 1000 ms, retry #2 in 2000 ms, retry #3 in 4000 ms, failed: ...
```

### Common Pattern — Retry Only Certain Errors
```typescript
import { timer, throwError } from 'rxjs';
import { ajax, AjaxError } from 'rxjs/ajax';
import { retryWhen, mergeMap } from 'rxjs/operators';

// Retry on network/5xx; fail fast on 4xx
ajax.getJSON('/api/data').pipe(
  retryWhen(errors$ =>
    errors$.pipe(
      mergeMap((err, i) => {
        const retryable = !(err instanceof AjaxError) || err.status >= 500;
        return retryable && i < 2 ? timer(500) : throwError(() => err);
      })
    )
  )
).subscribe({
  next:  data => console.log('data:', data),
  error: err  => console.log('gave up:', err.status ?? err.message)
});
// Output (404): gave up: 404           (no retry attempted)
// Output (503 twice, then 200): data: {...}
```

### Edge Cases — Silent Completion, Hanging Notifier, Synchronous Retry
```typescript
import { throwError, NEVER, of } from 'rxjs';
import { retryWhen, take, delay, mergeMap } from 'rxjs/operators';

// Edge case 1: a finite notifier COMPLETES → output completes, error is lost
throwError(() => new Error('boom')).pipe(
  retryWhen(errors$ => errors$.pipe(take(2), delay(10)))
).subscribe({
  error:    err => console.log('error:', err.message),
  complete: () => console.log('completed silently')
});
// Output (after ~20 ms): completed silently     ← 2 retries, then take(2) completes

// Edge case 2: notifier never emits → output hangs forever after the first error
throwError(() => new Error('boom')).pipe(
  retryWhen(() => NEVER)
).subscribe({ error: () => console.log('never printed') });
// Output: (nothing, ever)

// Edge case 3: synchronous notifier → tight retry loop
let n = 0;
of(null).pipe(
  // errors synchronously twice, then succeeds
  mergeMap(() => (++n < 3 ? throwError(() => new Error('x')) : of('ok'))),
  retryWhen(errors$ => errors$)   // emits synchronously on every error
).subscribe(v => console.log(v, 'after', n, 'attempts'));
// Output: ok after 3 attempts
```

## Common Pitfalls

### Anti-pattern: Still Using the Deprecated Form
```typescript
import { timer } from 'rxjs';
import { retryWhen, delay, mergeMap, retry } from 'rxjs/operators';

// ❌ INCORRECT — deprecated in RxJS 7, removed in RxJS 8
source$.pipe(retryWhen(errors$ => errors$.pipe(delay(500))));
source$.pipe(retryWhen(errors$ => errors$.pipe(mergeMap((e, i) => timer(500 * (i + 1))))));

// ✅ CORRECT — retry with a count and a delay (fixed or computed)
source$.pipe(retry({ count: 3, delay: 500 }));
source$.pipe(retry({ count: 3, delay: (error, retryCount) => timer(500 * retryCount) }));

// WHY: retry({ delay }) covers every retryWhen use case. The delay function
// receives the error and a 1-based retryCount, and returning an Observable that
// errors aborts the retry with that error — the same escape hatch retryWhen had,
// minus the silent-completion trap below.
```

### Anti-pattern: Letting the Notifier Complete to "Stop Retrying"
```typescript
import { throwError, timer } from 'rxjs';
import { retryWhen, take, delay, mergeMap } from 'rxjs/operators';

// ❌ INCORRECT — after 3 retries the notifier completes and the stream ends with NO error
request$.pipe(
  retryWhen(errors$ => errors$.pipe(delay(1000), take(3)))
).subscribe({
  next:  render,
  error: showError      // never called — the failure is invisible to the user
});

// ✅ CORRECT — rethrow the last error once the budget is spent
request$.pipe(
  retryWhen(errors$ =>
    errors$.pipe(
      mergeMap((err, i) => (i < 3 ? timer(1000) : throwError(() => err)))
    )
  )
).subscribe({ next: render, error: showError });

// WHY: A completing notifier completes the OUTPUT. The subscriber sees a clean
// completion and cannot tell that every attempt failed. retry({ count }) errors
// with the last error when the count is exhausted, which is almost always what
// you want.
```

### Anti-pattern: Returning a Timer That Ignores the Error Stream
```typescript
import { timer, throwError } from 'rxjs';
import { retryWhen, mergeMap } from 'rxjs/operators';

// ❌ INCORRECT — the notifier does not derive from errors$ at all
request$.pipe(
  retryWhen(() => timer(1000))
).subscribe(render);
// timer(1000) emits once (→ one retry) and then COMPLETES → output completes
// right after the retry starts, cutting the retried request short.

// ✅ CORRECT — map each error to a delay so the notifier lives as long as errors keep coming
request$.pipe(
  retryWhen(errors$ => errors$.pipe(mergeMap(() => timer(1000))))
).subscribe(render);

// WHY: The notifier Observable is subscribed once and must emit once PER ERROR.
// A standalone timer is unrelated to the error stream, emits once, and completes —
// and its completion propagates to the output.
```

## Related Operators

**Same Category (Error Handling)**:
- **`retry`**: The replacement — `retry({ count, delay, resetOnSuccess })` expresses every `retryWhen` schedule declaratively
- **`catchError`**: One-shot recovery with a fallback Observable instead of re-execution
- **`timeout`**: Often combined upstream so that a slow request errors and triggers a retry

**Complementary Operators**:
- **`defer`**: Guarantees each retry re-runs the producer with fresh state
- **`timer`** / **`delay`**: Build the retry schedule inside the notifier (or inside `retry`'s `delay` function)
- **`repeatWhen`**: The completion-driven twin (also deprecated; replaced by `repeat({ delay })`)

**Alternatives by Use Case**:

| Use Case | Instead of `retryWhen` | Use This | Why |
|----------|------------------------|----------|-----|
| Fixed number of immediate retries | `retryWhen(e => e.pipe(take(n)))` | `retry(n)` | Errors instead of completing silently |
| Fixed delay between retries | `retryWhen(e => e.pipe(delay(ms)))` | `retry({ delay: ms })` | One option, no notifier |
| Backoff / conditional retry | `retryWhen` + `mergeMap` | `retry({ delay: (err, n) => … })` | Same power, 1-based count supplied |
| Fallback value after failure | `retryWhen` | `catchError(() => of(fallback))` | Recovery, not re-execution |

**Migration Notes**:
```typescript
// Deprecated API (RxJS 6 / early 7)
import { retryWhen, delay, mergeMap, take } from 'rxjs/operators';
source$.pipe(retryWhen(errors$ => errors$.pipe(delay(1000), take(3))));
source$.pipe(retryWhen(errors$ => errors$.pipe(mergeMap((err, i) => i < 3 ? timer(2 ** i * 1000) : throwError(() => err)))));

// Current API (RxJS 7+)
import { retry } from 'rxjs/operators';
source$.pipe(retry({ count: 3, delay: 1000 }));
source$.pipe(retry({ count: 3, delay: (err, retryCount) => timer(2 ** (retryCount - 1) * 1000) }));
```

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/retryWhen](https://rxjs.dev/api/operators/retryWhen)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/operators/retry.html](http://reactivex.io/documentation/operators/retry.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/retryWhen.ts](https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/retryWhen.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Notifier-Controlled Resubscription (deprecated predecessor of `retry({ delay })`)
- **Cognitive Load**: 4/5 — Two interleaved streams, a once-only notifier call, and three notifier signals with different effects
- **Usage Frequency**: 2/5 — Common in RxJS 6 codebases and tutorials; should not appear in new RxJS 7 code
- **Composability**: 3/5 — Powerful inside the notifier, but the leaky completion channel makes it fragile when wrapped

**Teaching Sequence**:
- **Prerequisites**: `retry`, `catchError`, Subjects, `mergeMap`, `timer`, cold vs. hot resubscription
- **Teaches**: Why a "control stream" API is flexible but error-prone; the value of config objects with explicit `count`
- **Leads to**: `retry({ delay })`, `repeat({ delay })`, custom backoff operators
- **Common with**: `timer`, `delay`, `mergeMap`, `throwError`, `ajax`, `webSocket`
