# timeoutWith

## Identity
- **Name**: timeoutWith
- **Category**: Error Handling Operators
- **Type**: Time-bound fallback switch — Deprecated in RxJS 7, removed in RxJS 8. Mirrors the source until a time limit is exceeded, then switches to a fallback Observable instead of erroring
- **Import**:
  ```typescript
  // Deprecated — still exported in RxJS 7
  import { timeoutWith } from 'rxjs/operators';

  // Replacement
  import { timeout } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  /** @deprecated Use timeout({ each | first, with }) instead. */
  function timeoutWith<T, R>(
    due: number,
    withObservable: ObservableInput<R>,
    scheduler?: SchedulerLike
  ): OperatorFunction<T, T | R>

  function timeoutWith<T, R>(
    due: Date,
    withObservable: ObservableInput<R>,
    scheduler?: SchedulerLike
  ): OperatorFunction<T, T | R>
  ```

## Functional Specification

**Input**: `Observable<T>` — a source Observable whose emissions are expected within a time budget

**Output**: `Observable<T | R>` — mirrors the source until the deadline is missed, then mirrors `withObservable`

**Transformation**:
In RxJS 7, `timeoutWith(due, fallback, scheduler)` is implemented as a thin wrapper over `timeout`:
- a numeric `due` becomes `timeout({ each: due, with: () => fallback, scheduler })` — **every** gap between emissions (and the gap before the first one) is measured
- a `Date` `due` becomes `timeout({ first: due, with: () => fallback, scheduler })` — only the arrival of the first value is measured against the absolute deadline

When a deadline is missed the source is unsubscribed, `withObservable` is subscribed, and the output mirrors the fallback — including its completion or error. No `TimeoutError` is ever emitted.

**Mathematical representation**:
```
timeoutWith(due: number, F)(S) ≡ timeout({ each: due,  with: () => F })(S)
timeoutWith(due: Date,   F)(S) ≡ timeout({ first: due, with: () => F })(S)

Let tᵢ be the arrival time of the ith source value, t₀ the subscription time.
Numeric due:  switch to F at the first i where tᵢ − tᵢ₋₁ > due
Date due:     switch to F if t₁ > due
```

**Invariants**:
- **At most one switch**: Once the fallback is subscribed, the original source is gone for good — there is no switching back
- **Fallback transparency**: After the switch, the fallback's values, completion, and errors are forwarded unchanged
- **No timeout error**: The operator never raises `TimeoutError`; a missed deadline is always a switch
- **Source completion wins**: If the source completes before the deadline, the fallback is never subscribed

## Marble Diagram

```
Source:    --a--b-----------c--|
Fallback:                 --x--y--|
           timeoutWith(50, fallback$)       (each gap must be ≤ 50 time units)
Result:    --a--b-------x--y--|

The gap after b exceeds 50; source is unsubscribed (c is never seen);
fallback is subscribed and mirrored to completion.
```

**No timeout — identity**:
```
Source:    --a--b--c--|
           timeoutWith(50, fallback$)
Result:    --a--b--c--|
```

**Date deadline — first value only**:
```
Subscribe at t=0, due = Date(t=30)
Source:    -----------a-------------b--|      (a arrives at 55)
Fallback:              --x--|
Result:    ------------x--|                   (first value missed the deadline)

Source:    --a-------------------b--|         (a at 10, gap to b is 110)
Result:    --a-------------------b--|         (only the FIRST arrival is checked)
```

**Key observation**: The numeric form is a per-gap guard, not a "first value" guard. Many migrations assume `timeoutWith(5000, fallback$)` protects only the initial response; it actually trips on any 5-second silence, even deep into a long-lived stream. The `timeout` config form makes this choice explicit with `first` vs `each`.

## Behavioral Characteristics

**Subscription**:
- Subscribes to the source immediately and starts a timer on the given scheduler (default `asyncScheduler`)
- `withObservable` is subscribed lazily — only when a deadline is missed; it is never subscribed otherwise

**Completion semantics**:
- Source completes within budget: output completes with the source
- Deadline missed: output completes when the fallback completes
- The timer is cancelled on source completion, source error, or unsubscription

**Error handling**:
- Source errors propagate immediately; the fallback is not consulted
- Fallback errors propagate after the switch
- `TimeoutError` is never produced by this operator

**Backpressure**:
- O(1) state: one pending timer plus a reference to the fallback
- A numeric `due` reschedules the timer on every emission; cheap, but a very high-frequency source does re-arm it constantly

**Hot vs. Cold**:
- With cold sources: the switch unsubscribes the cold execution (an in-flight HTTP request is aborted)
- With hot sources: the switch only detaches this subscriber; the hot source keeps running for others
- `withObservable` is a single Observable instance, not a factory — a hot fallback is shared, a cold fallback starts fresh at the switch

## Type System Integration

```typescript
/**
 * Type Parameters:
 *   T - Source value type
 *   R - Fallback value type
 *
 * Input Type:  Observable<T>
 * Output Type: Observable<T | R>
 *
 * Type Narrowing:
 *   The output is a union. If the fallback carries the same type as the source,
 *   T | R collapses to T and consumers see no change.
 *
 * Type Safety:
 *   A fallback of a different type widens the output — consumers must handle both.
 */

import { of } from 'rxjs';
import { timeoutWith } from 'rxjs/operators';

interface Quote { symbol: string; price: number }
const live$: Observable<Quote> = getLiveQuotes();
const cached: Quote = { symbol: 'ACME', price: 42 };

// Same type → Observable<Quote>
const quotes$ = live$.pipe(timeoutWith(3000, of(cached)));

// Different type → Observable<Quote | null>
const nullable$ = live$.pipe(timeoutWith(3000, of(null)));
nullable$.subscribe((q: Quote | null) => q ? render(q) : showStale());
```

## Examples

### Basic Usage — Deprecated Form and Its Replacement
```typescript
import { Subject, of } from 'rxjs';
import { timeoutWith, timeout } from 'rxjs/operators';

const source$ = new Subject<string>();

// Deprecated — shown for reading legacy code
source$.pipe(
  timeoutWith(3000, of('fallback value'))
).subscribe({
  next:     v => console.log('value:', v),
  complete: () => console.log('complete')
});

// ✅ Replacement — identical behavior
source$.pipe(
  timeout({ each: 3000, with: () => of('fallback value') })
).subscribe({
  next:     v => console.log('value:', v),
  complete: () => console.log('complete')
});

// With no emission inside 3 s, both print:
// Output: value: fallback value
//         complete
```

### Common Pattern — Cached Config When the Server Is Slow
```typescript
import { of } from 'rxjs';
import { ajax } from 'rxjs/ajax';
import { timeout } from 'rxjs/operators';

interface Config { theme: string; locale: string }
const CACHED_CONFIG: Config = { theme: 'light', locale: 'en' };

// Legacy: ajax.getJSON<Config>('/api/config').pipe(timeoutWith(2000, of(CACHED_CONFIG)))
ajax.getJSON<Config>('/api/config').pipe(
  timeout({
    first: 2000,                   // only the first (and only) response is guarded
    with:  () => of(CACHED_CONFIG) // factory — a fresh fallback per timeout
  })
).subscribe(config => applyConfig(config));
// Output (server responds in 300 ms): applyConfig({ ...server config })
// Output (server silent for 2 s):     applyConfig({ theme: 'light', locale: 'en' })
```

### Common Pattern — Per-Message Heartbeat With Reconnect Fallback
```typescript
import { defer, Observable } from 'rxjs';
import { webSocket } from 'rxjs/webSocket';
import { timeout } from 'rxjs/operators';

interface Msg { type: string }

function stream(): Observable<Msg> {
  return webSocket<Msg>('wss://api.example.com/feed').pipe(
    // Legacy: timeoutWith(30_000, defer(() => stream()))
    timeout({
      each: 30_000,               // every gap between messages is measured
      with: () => defer(() => stream()) // silent for 30 s → open a new socket
    })
  );
}

stream().subscribe(msg => handle(msg));
// Output: messages flow; after any 30 s silence the socket is replaced transparently
```

### Edge Cases — Fallback Errors, Fallback Completes Empty, Date Deadline
```typescript
import { NEVER, EMPTY, throwError, of, timer } from 'rxjs';
import { timeoutWith } from 'rxjs/operators';

// Edge case 1: the fallback errors — that error is forwarded (no TimeoutError)
NEVER.pipe(
  timeoutWith(100, throwError(() => new Error('fallback failed')))
).subscribe({ error: err => console.log(err.message) });
// Output (after 100 ms): fallback failed

// Edge case 2: the fallback is EMPTY — output completes silently
NEVER.pipe(
  timeoutWith(100, EMPTY)
).subscribe({ complete: () => console.log('completed with no values') });
// Output (after 100 ms): completed with no values

// Edge case 3: Date deadline — later gaps are NOT checked
timer(10, 500).pipe(            // 0 at 10 ms, then every 500 ms
  timeoutWith(new Date(Date.now() + 200), of(-1))
).subscribe(v => console.log(v));
// Output: 0, 1, 2, ...   (first value beat the deadline; the 500 ms gaps are ignored)
```

## Common Pitfalls

### Anti-pattern: Still Using the Deprecated Form
```typescript
import { of } from 'rxjs';
import { timeoutWith, timeout } from 'rxjs/operators';

// ❌ INCORRECT — compiles in RxJS 7 with a deprecation warning; gone in RxJS 8
request$.pipe(
  timeoutWith(5000, of(FALLBACK))
);

// ✅ CORRECT — the config form, with the intent spelled out
request$.pipe(
  timeout({ first: 5000, with: () => of(FALLBACK) })   // guard the first value
);
request$.pipe(
  timeout({ each: 5000, with: () => of(FALLBACK) })    // guard every gap (what timeoutWith(number) did)
);

// WHY: timeoutWith was folded into timeout's config object in RxJS 7 and removed
// in RxJS 8. The config form also forces you to choose `first` or `each`
// explicitly instead of inheriting the number-means-each rule.
```

### Anti-pattern: Assuming a Numeric Deadline Guards Only the First Value
```typescript
import { interval, of } from 'rxjs';
import { timeoutWith } from 'rxjs/operators';

// ❌ INCORRECT — meant "respond within 1 s", but trips on ANY 1 s gap later
pollingFeed$.pipe(           // emits a burst, then nothing for a while — normal for this feed
  timeoutWith(1000, of(STALE))
).subscribe(render);
// The feed is replaced by STALE at the first quiet second, then completes.

// ✅ CORRECT — use the Date overload (legacy) or `first` (modern) for a one-time deadline
pollingFeed$.pipe(
  timeout({ first: 1000, with: () => of(STALE) })
).subscribe(render);

// WHY: timeoutWith(number) ≡ timeout({ each }). After the switch the original
// feed is unsubscribed permanently, so a single quiet period silently kills a
// healthy long-lived stream.
```

### Anti-pattern: Passing an Observable Instead of a Factory When Migrating
```typescript
import { of } from 'rxjs';
import { timeout } from 'rxjs/operators';

const fallback$ = of('fallback');

// ❌ INCORRECT — `with` expects a factory; this is a type error
source$.pipe(
  // @ts-expect-error
  timeout({ each: 1000, with: fallback$ })
);

// ✅ CORRECT — wrap it in a function
source$.pipe(
  timeout({ each: 1000, with: () => fallback$ })
);

// WHY: timeoutWith took an Observable INSTANCE; timeout takes a FACTORY that
// receives TimeoutInfo (lastValue, seen, meta). The factory lets you build the
// fallback from what was observed before the timeout fired.
```

## Related Operators

**Same Category (Error Handling)**:
- **`timeout`**: The replacement — `timeout({ first | each, with })` covers every `timeoutWith` use and can also error instead of switching
- **`catchError`**: Alternative migration path — `timeout(ms)` followed by `catchError` that returns the fallback
- **`retry`**: Re-executes the source after a timeout error; pair with `timeout(ms)` rather than a fallback switch

**Complementary Operators**:
- **`defer`**: Build the fallback lazily (e.g. a reconnect) so it is created at switch time
- **`race`**: Manual "whichever comes first" between the source and a `timer`-driven fallback

**Alternatives by Use Case**:

| Use Case | Instead of `timeoutWith` | Use This | Why |
|----------|--------------------------|----------|-----|
| Fallback on first-value timeout | `timeoutWith(Date, fb$)` | `timeout({ first, with })` | Explicit, not deprecated |
| Fallback on any silence | `timeoutWith(number, fb$)` | `timeout({ each, with })` | Same semantics, explicit |
| Error on timeout | `timeoutWith` + `throwError` fallback | `timeout(ms)` | Produces a `TimeoutError` directly |
| Retry instead of fallback | `timeoutWith(ms, retry$)` | `timeout(ms)` + `retry(n)` | Keeps per-attempt windows |

**Migration Notes**:
```typescript
// Deprecated API (RxJS 6 / early 7)
import { timeoutWith } from 'rxjs/operators';
source$.pipe(timeoutWith(5000, fallback$));
source$.pipe(timeoutWith(new Date(deadline), fallback$));

// Current API (RxJS 7+)
import { timeout } from 'rxjs/operators';
source$.pipe(timeout({ each: 5000, with: () => fallback$ }));
source$.pipe(timeout({ first: new Date(deadline), with: () => fallback$ }));
```

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/timeoutWith](https://rxjs.dev/api/operators/timeoutWith)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/operators/timeout.html](http://reactivex.io/documentation/operators/timeout.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/timeoutWith.ts](https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/timeoutWith.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Deadline-Triggered Source Substitution (deprecated alias of `timeout` with `with`)
- **Cognitive Load**: 3/5 — The number-means-each vs. Date-means-first rule is the trap; otherwise it is a single switch
- **Usage Frequency**: 1/5 — Legacy code only; new code uses the `timeout` config form
- **Composability**: 3/5 — Fine inside pipelines, but the permanent switch limits reuse on long-lived streams

**Teaching Sequence**:
- **Prerequisites**: `timeout`, `TimeoutError`, lazy vs. eager Observables, cold-source cancellation
- **Teaches**: Why the config-object style replaced positional overloads; `first` vs. `each` deadlines
- **Leads to**: `timeout({ with })`, `retry` + `timeout` composition, `race` with `timer`
- **Common with**: `ajax`, `webSocket`, `defer`, `of`, `catchError`
