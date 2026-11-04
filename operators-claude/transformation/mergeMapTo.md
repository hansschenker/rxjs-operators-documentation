# mergeMapTo

## Identity
- **Name**: mergeMapTo
- **Category**: Transformation Operators
- **Type**: Concurrent higher-order flattener with a fixed inner Observable — ⚠️ **Deprecated since RxJS 7**, slated for removal in the next major release; use `mergeMap(() => inner$, concurrent?)` instead
- **Import**:
  ```typescript
  import { mergeMapTo } from 'rxjs/operators';
  // RxJS 7.2+: also exported from the root
  import { mergeMapTo } from 'rxjs';
  ```
- **Signature**:
  ```typescript
  function mergeMapTo<O extends ObservableInput<unknown>>(
    innerObservable: O,
    concurrent?: number          // default: Infinity
  ): OperatorFunction<unknown, ObservedValueOf<O>>
  ```

## Functional Specification

**Input**: `Observable<unknown>` — source values act only as triggers; their content is discarded

**Output**: `Observable<ObservedValueOf<O>>` — the values of every active subscription to `innerObservable`, interleaved in arrival order

**Transformation**:
Equivalent to `mergeMap(() => innerObservable, concurrent)`. Every source emission starts a new subscription to the same `innerObservable`. Up to `concurrent` inner subscriptions run simultaneously; emissions beyond that limit are queued until a slot frees up. Source values never reach the output.

**Mathematical representation**:
```
mergeMapTo(I, c)(S) = merge(I, I, I, ..., { concurrent: c })   one I per emission of S
                    = mergeMap(_ => I, c)(S)

|output|  = |S| × |I|        (when I is finite and S completes)
order     = interleaved — determined by inner timing, not by source order
active(t) ≤ c                 at every instant t
```

**Invariants**:
- **Fixed inner**: The same `innerObservable` reference is subscribed every time — evaluated once at pipe-assembly time
- **Bounded concurrency**: Never more than `concurrent` inner subscriptions are active; the default is unbounded
- **No cancellation**: An existing inner is never unsubscribed because a new source value arrived
- **Value-blind**: Source values are ignored; only their timing matters

## Marble Diagram

```
Source:   --a--------b--------c--------|
          mergeMapTo(x--x--x|)
inner(a):   x--x--x|
inner(b):            x--x--x|
inner(c):                     x--x--x|
Result:   --x--x--x--x--x--x--x--x--x|
```

**Overlapping inners** (interleaving):
```
Source:   --a---b------|
          mergeMapTo(x---x---x|)
inner(a):   x---x---x|
inner(b):       x---x---x|
Result:   --x---(xx)--x-x|

Emissions from a and b interleave; synchronous coincidences are grouped as (xx).
```

**`concurrent = 1`** (identical to `concatMapTo`):
```
Source:   --a--b--c------------|
          mergeMapTo(x----x|, 1)
inner(a):   x----x|
inner(b):         x----x|
inner(c):               x----x|
Result:   --x----x-x----x-x----x|
```

**Key observation**: `mergeMapTo` fans out — every source emission adds another live subscription to the *same* inner. With a cold inner that means repeated work (one request per value); with a hot inner it means the same live stream is observed several times over, so each of its values is forwarded once **per active subscription**.

## Behavioral Characteristics

**Subscription**:
- Subscribes to the source immediately on output subscription
- Subscribes to `innerObservable` once per source emission, up to `concurrent` at a time; excess emissions are queued
- If `innerObservable` is a Promise, `from(promise)` is applied — the Promise has already started and resolves to the same value every time

**Completion semantics**:
- Output completes when the source has completed **and** all active and queued inner subscriptions have completed
- A source that completes while inners are still active does not end the output
- If any inner never completes, the output never completes

**Error handling**:
- A source error propagates immediately and every active inner is unsubscribed
- An inner error propagates immediately, terminating the output and unsubscribing all other inners
- No retry or recovery; wrap the inner with `catchError` to isolate failures

**Backpressure**:
- With `concurrent = Infinity`: no backpressure — a burst of n source values opens n inner subscriptions at once
- With a finite `concurrent`: an unbounded queue of pending source emissions (O(n) memory in queued emissions)

**Hot vs. Cold**:
- Cold inner: each subscription re-executes the producer — n source values mean n requests, n timers
- Hot inner (Subject, shared stream): k active subscriptions forward each live value k times — a classic duplicate-emission bug
- Source temperature is irrelevant — only its emission timing matters

## Type System Integration

```typescript
/**
 * Type Parameters:
 *   O - The ObservableInput type of the fixed inner (Observable, Promise, array, iterable)
 *
 * Input Type:  Observable<unknown>  — source values are discarded, so any source type is accepted
 * Output Type: Observable<ObservedValueOf<O>>  — the inner's element type
 *
 * Type Narrowing:
 *   The output type is derived entirely from O; the source type is erased.
 *   The optional `concurrent` parameter has no effect on types.
 *
 * Type Safety:
 *   Every call site is flagged with a deprecation strikethrough (@deprecated JSDoc).
 */

import { fromEvent, interval } from 'rxjs';
import { mergeMapTo, mergeMap, take } from 'rxjs/operators';

const click$ = fromEvent(document, 'click');              // Observable<Event>
const pulse$ = interval(300).pipe(take(4));                 // Observable<number>

const deprecated$ = click$.pipe(mergeMapTo(pulse$, 2));     // Observable<number>
const current$    = click$.pipe(mergeMap(() => pulse$, 2)); // Observable<number> — identical type
```

## Examples

### Basic Usage — Deprecated Form and Its Replacement
```typescript
import { fromEvent, interval } from 'rxjs';
import { mergeMapTo, mergeMap, take } from 'rxjs/operators';

const button = document.getElementById('btn')!;
const pulse$ = interval(300).pipe(take(4));

// ❌ Deprecated — each click spawns a concurrent pulse sequence
fromEvent(button, 'click').pipe(
  mergeMapTo(pulse$)
).subscribe(tick => console.log('Pulse tick:', tick));

// ✅ Current — same behavior with a zero-argument projection
fromEvent(button, 'click').pipe(
  mergeMap(() => pulse$)
).subscribe(tick => console.log('Pulse tick:', tick));
// Click twice 150ms apart:
// Output: 0 (300ms), 0 (450ms), 1 (600ms), 1 (750ms), 2, 2, 3, 3  — interleaved
```

### Common Pattern — Fan Out a Fixed Request per Trigger
```typescript
import { fromEvent } from 'rxjs';
import { ajax } from 'rxjs/ajax';
import { mergeMap } from 'rxjs/operators';

const refresh = document.getElementById('refresh')!;

// Every click fires a request immediately; responses arrive in whatever order
// the server answers — formerly mergeMapTo(ajax.getJSON('/api/metrics'))
fromEvent(refresh, 'click').pipe(
  mergeMap(() => ajax.getJSON<{ cpu: number }>('/api/metrics'))
).subscribe(metrics => console.log('CPU:', metrics.cpu));
// Output (per click, possibly out of order): CPU: 42
```

### Common Pattern — Cap Concurrency for a Work Queue
```typescript
import { from, of } from 'rxjs';
import { mergeMap, delay } from 'rxjs/operators';

const job$ = of('job done').pipe(delay(1000));

// Ten triggers, but at most three jobs run at once — formerly mergeMapTo(job$, 3)
from(Array.from({ length: 10 }, (_, i) => i)).pipe(
  mergeMap(() => job$, 3)
).subscribe({
  next:     msg => console.log(msg),
  complete: () => console.log('all jobs finished')
});
// Output: three 'job done' at ~1s, three at ~2s, three at ~3s, one at ~4s, all jobs finished
```

### Edge Cases — Hot Inner, Promise Inner, `concurrent = 1`
```typescript
import { of, Subject } from 'rxjs';
import { mergeMapTo } from 'rxjs/operators';

// Edge case 1: hot inner — each active subscription forwards every live value
const live$ = new Subject<string>();
of('a', 'b', 'c').pipe(mergeMapTo(live$)).subscribe(console.log);
live$.next('ping');
// Output: ping, ping, ping   (three subscriptions to the same Subject)

// Edge case 2: Promise inner — already running, resolved once, observed three times
let calls = 0;
const once = new Promise<number>(resolve => { calls++; resolve(42); });
of('a', 'b', 'c').pipe(mergeMapTo(once)).subscribe(console.log);
// Output: 42, 42, 42   (calls === 1)

// Edge case 3: concurrent = 1 — degenerates into concatMapTo
import { interval } from 'rxjs';
import { take } from 'rxjs/operators';
of('a', 'b').pipe(
  mergeMapTo(interval(100).pipe(take(2)), 1)
).subscribe(console.log);
// Output: 0, 1, 0, 1   (second run waits for the first to complete)
```

## Common Pitfalls

### Anti-pattern: Still Using the Deprecated Form
```typescript
import { fromEvent, of } from 'rxjs';
import { mergeMapTo, mergeMap } from 'rxjs/operators';

const click$  = fromEvent(document, 'click');
const action$ = of('done');

// ❌ INCORRECT — deprecated; emits a compiler warning and disappears in the next major
click$.pipe(mergeMapTo(action$)).subscribe(console.log);

// ✅ CORRECT — a zero-argument projection is the drop-in replacement
click$.pipe(mergeMap(() => action$)).subscribe(console.log);

// WHY: mergeMapTo(x, c) is implemented as mergeMap(() => x, c). The *To family
// was deprecated because the constant form hides when the inner is created and
// adds nothing the projection form cannot express.
```

### Anti-pattern: Expecting the Inner to Be Created per Value
```typescript
import { fromEvent, defer } from 'rxjs';
import { mergeMapTo, mergeMap } from 'rxjs/operators';

const upload$ = fromEvent(document.getElementById('upload')!, 'click');

// ❌ INCORRECT — the request starts right here, once, before any click
upload$.pipe(
  mergeMapTo(fetch('/api/upload', { method: 'POST' }))
).subscribe(() => console.log('uploaded?'));
// Only ONE request is ever sent, and every click just re-reads its result

// ✅ CORRECT — create the request inside the projection
upload$.pipe(
  mergeMap(() => fetch('/api/upload', { method: 'POST' }))
).subscribe(() => console.log('uploaded'));

// ✅ ALSO CORRECT — wrap the eager producer in defer if you must keep a reference
const upload = defer(() => fetch('/api/upload', { method: 'POST' }));
upload$.pipe(mergeMap(() => upload)).subscribe(() => console.log('uploaded'));

// WHY: mergeMapTo evaluates its argument exactly once, at pipe-assembly time.
// A cold Observable is re-run per subscription; a Promise or an eagerly started
// request is shared across every source value.
```

### Anti-pattern: Unbounded Concurrency on a Bursty Source
```typescript
import { fromEvent } from 'rxjs';
import { ajax } from 'rxjs/ajax';
import { mergeMap, debounceTime, switchMap } from 'rxjs/operators';

const keys$ = fromEvent(document, 'keydown');

// ❌ INCORRECT — every keystroke opens another request; nothing is ever cancelled
keys$.pipe(
  mergeMap(() => ajax.getJSON('/api/search'))      // formerly mergeMapTo(...)
).subscribe(console.log);

// ✅ CORRECT — rate-limit the trigger and cap concurrency…
keys$.pipe(
  debounceTime(300),
  mergeMap(() => ajax.getJSON('/api/search'), 3)
).subscribe(console.log);

// ✅ …or use switchMap when only the latest result matters
keys$.pipe(
  debounceTime(300),
  switchMap(() => ajax.getJSON('/api/search'))
).subscribe(console.log);

// WHY: the default concurrent = Infinity means a burst of n triggers opens n
// simultaneous subscriptions. For network or CPU work that is a resource leak;
// pass a concurrency limit or choose a strategy that cancels or drops.
```

## Related Operators

**Same Category (Higher-Order Transformation)**:
- **`mergeMap(project, concurrent?)`**: The replacement — identical semantics with a per-value projection
- **`concatMapTo(inner$)`**: Deprecated sibling — sequential (`concurrent = 1`) instead of concurrent
- **`switchMapTo(inner$)`**: Deprecated sibling — cancels the previous inner instead of merging
- **`mergeAll(concurrent?)`**: Flattens an `Observable<Observable<T>>` concurrently without any projection

**Complementary Operators**:
- **`debounceTime` / `throttleTime`**: Tame a bursty trigger before fanning out
- **`catchError`**: Applied *inside* the inner so one failed branch does not tear down the rest
- **`defer`**: Wrap an eager producer (Promise, started request) so it is re-created per subscription
- **`share`**: If the inner is meant to be observed once rather than re-run, share it explicitly

**Alternatives by Use Case**:

| Use Case | Instead of `mergeMapTo` | Use This | Why |
|----------|-------------------------|----------|-----|
| Same inner per value, concurrently | `mergeMapTo(x$)` | `mergeMap(() => x$)` | Non-deprecated, identical behavior |
| Bounded parallelism | `mergeMapTo(x$, 3)` | `mergeMap(() => x$, 3)` | Same `concurrent` argument |
| Inner depends on the value | `mergeMapTo` + closure tricks | `mergeMap(v => f(v))` | That is what the projection is for |
| Only the latest result matters | `mergeMapTo` | `switchMap(() => x$)` | Cancels stale work |
| Strict ordering | `mergeMapTo(x$, 1)` | `concatMap(() => x$)` | Says what it means |

**Migration Notes**:
```typescript
// Deprecated API (RxJS 6 / 7)
source$.pipe(mergeMapTo(inner$))
source$.pipe(mergeMapTo(inner$, 4))

// Current API (RxJS 7+)
source$.pipe(mergeMap(() => inner$))
source$.pipe(mergeMap(() => inner$, 4))

// If the inner was an eager Promise or request, also defer its creation:
source$.pipe(mergeMap(() => fetch(url)))         // created per value
```

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/mergeMapTo](https://rxjs.dev/api/operators/mergeMapTo)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/operators/flatmap.html](http://reactivex.io/documentation/operators/flatmap.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/mergeMapTo.ts](https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/mergeMapTo.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Concurrent Flattening with a Constant Inner (deprecated specialization of Concurrent Flattening)
- **Cognitive Load**: 2/5 — Trivial once `mergeMap` is understood; the subtleties are single evaluation of the inner and duplicate forwarding from hot inners
- **Usage Frequency**: 1/5 — Legacy code only; new code should never introduce it
- **Composability**: 2/5 — Composes like `mergeMap`, but the fixed inner removes the ability to react to values

**Teaching Sequence**:
- **Prerequisites**: `mergeMap`, the `concurrent` parameter, cold vs. hot Observables
- **Teaches**: Why the `*To` operators were retired — eager evaluation of the inner argument; what "subscribing to a hot stream k times" does
- **Leads to**: Migration patterns, `defer` for eager producers, `share` for intentional multicasting
- **Common with**: `mergeMap`, `concatMapTo`, `switchMapTo`, `defer`
