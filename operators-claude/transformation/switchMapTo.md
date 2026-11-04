# switchMapTo

## Identity
- **Name**: switchMapTo
- **Category**: Transformation Operators
- **Type**: Cancelling higher-order flattener with a fixed inner Observable — ⚠️ **Deprecated since RxJS 7**, slated for removal in the next major release; use `switchMap(() => inner$)` instead
- **Import**:
  ```typescript
  import { switchMapTo } from 'rxjs/operators';
  // RxJS 7.2+: also exported from the root
  import { switchMapTo } from 'rxjs';
  ```
- **Signature**:
  ```typescript
  function switchMapTo<O extends ObservableInput<unknown>>(
    observable: O
  ): OperatorFunction<unknown, ObservedValueOf<O>>
  ```

## Functional Specification

**Input**: `Observable<unknown>` — source values act only as triggers; their content is discarded

**Output**: `Observable<ObservedValueOf<O>>` — the values of the **most recent** subscription to `observable`; earlier subscriptions are cancelled

**Transformation**:
Equivalent to `switchMap(() => observable)`. Every source emission unsubscribes the currently active inner (if any) and starts a fresh subscription to the same `observable`. Only one inner is ever live. Source values never reach the output.

**Mathematical representation**:
```
switchMapTo(I)(S) = switchMap(_ => I)(S)

For source emissions s₁ < s₂ < ... (by time):
  inner(sᵢ) is active on [sᵢ, min(sᵢ₊₁, complete(inner(sᵢ))))
  output = ⋃ᵢ { values of inner(sᵢ) emitted while inner(sᵢ) is active }

active(t) ≤ 1   at every instant t
```

**Invariants**:
- **Fixed inner**: The same `observable` reference is subscribed every time — evaluated once at pipe-assembly time
- **At most one live inner**: A new source value always cancels the previous inner before subscribing again
- **Latest wins**: Values from a cancelled inner never reach the output, even if they were "in flight"
- **Value-blind**: Source values are ignored; only their timing matters

## Marble Diagram

```
Source:   --a---------b---------c---------|
          switchMapTo(x--x--x--x--x...)
inner(a):   x--x--x--x✂
inner(b):             x--x--x--x✂
inner(c):                       x--x--x--x--...
Result:   --x--x--x--x-x--x--x--x-x--x--x--x--...

✂ = previous inner unsubscribed the instant a new source value arrives.
```

**Inner completes before the next source value** (no cancellation needed):
```
Source:   --a--------b--------|
          switchMapTo(x-x|)
inner(a):   x-x|
inner(b):            x-x|
Result:   --x-x------x-x------|
```

**Source emits faster than the inner can produce** (everything cancelled):
```
Source:   --a-b-c-d-----------|
          switchMapTo(----x|)
inner(a):   ----✂
inner(b):     ----✂
inner(c):       ----✂
inner(d):         ----x|
Result:   ------------x-------|

Only the last trigger's inner survives long enough to emit.
```

**Key observation**: `switchMapTo` restarts the *same* inner on every trigger. That is exactly right for "reset the timer" or "restart the poll"; it is exactly wrong for writes, because a second trigger silently aborts the first write mid-flight.

## Behavioral Characteristics

**Subscription**:
- Subscribes to the source immediately on output subscription
- On each source emission: unsubscribes the active inner (if any), then subscribes to `observable` again
- If `observable` is a Promise, `from(promise)` is applied — the Promise has already started; "cancelling" it only stops observing its result

**Completion semantics**:
- Output completes when the source has completed **and** the currently active inner (if any) has completed
- A source that completes while an inner is still active does not end the output
- If the last inner never completes, the output never completes

**Error handling**:
- A source error propagates immediately and the active inner is unsubscribed
- An inner error propagates immediately, terminating the output
- A cancelled inner cannot error into the output — its subscription is gone

**Backpressure**:
- Natural backpressure by cancellation — at most one inner is live, nothing is queued; O(1) state
- Fast triggers simply discard work in progress

**Hot vs. Cold**:
- Cold inner: each trigger re-executes the producer and aborts the previous execution (a cancelled `ajax` request is actually aborted)
- Hot inner (Subject, shared stream): each trigger just re-observes the live stream; cancelling and resubscribing is a no-op on the producer
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
 *
 * Type Safety:
 *   Every call site is flagged with a deprecation strikethrough (@deprecated JSDoc).
 */

import { fromEvent, timer } from 'rxjs';
import { switchMapTo, switchMap } from 'rxjs/operators';

const activity$ = fromEvent(document, 'mousemove');          // Observable<Event>
const idle$     = timer(30_000);                              // Observable<number>

const deprecated$ = activity$.pipe(switchMapTo(idle$));       // Observable<number>
const current$    = activity$.pipe(switchMap(() => idle$));   // Observable<number> — identical type
```

## Examples

### Basic Usage — Deprecated Form and Its Replacement
```typescript
import { fromEvent, interval } from 'rxjs';
import { switchMapTo, switchMap, take } from 'rxjs/operators';

const button = document.getElementById('btn')!;
const countdown$ = interval(1000).pipe(take(3));

// ❌ Deprecated — each click restarts the countdown, cancelling the previous one
fromEvent(button, 'click').pipe(
  switchMapTo(countdown$)
).subscribe(n => console.log('tick', n));

// ✅ Current — same behavior with a zero-argument projection
fromEvent(button, 'click').pipe(
  switchMap(() => countdown$)
).subscribe(n => console.log('tick', n));
// Click, wait 1.5s, click again:
// Output: tick 0 (1s), tick 0 (2.5s), tick 1 (3.5s), tick 2 (4.5s)
```

### Common Pattern — Inactivity Timer That Resets on Any Activity
```typescript
import { fromEvent, merge, timer } from 'rxjs';
import { switchMap } from 'rxjs/operators';

const activity$ = merge(
  fromEvent(document, 'click'),
  fromEvent(document, 'keydown'),
  fromEvent(document, 'mousemove')
);

// Every activity event cancels the running timer and starts a new one.
// Formerly: switchMapTo(timer(30_000))
activity$.pipe(
  switchMap(() => timer(30_000))
).subscribe(() => {
  console.log('No activity for 30 seconds — logging out');
});
```

### Common Pattern — Restart a Heartbeat on Each Login
```typescript
import { fromEvent, interval } from 'rxjs';
import { switchMap, takeUntil } from 'rxjs/operators';

const login$  = fromEvent(document.getElementById('login')!, 'click');
const logout$ = fromEvent(document.getElementById('logout')!, 'click');

// A fresh heartbeat per login; logging in again restarts it from zero.
// Formerly: switchMapTo(interval(5000).pipe(takeUntil(logout$)))
login$.pipe(
  switchMap(() => interval(5000).pipe(takeUntil(logout$)))
).subscribe(() => console.log('heartbeat'));
// Output: heartbeat every 5s after the latest login, until logout
```

### Edge Cases — Synchronous Inner, Hot Inner, Promise Inner
```typescript
import { of, Subject } from 'rxjs';
import { switchMapTo } from 'rxjs/operators';

// Edge case 1: synchronous inner — nothing to cancel, every value is forwarded
of(1, 2, 3).pipe(switchMapTo(of('x', 'y'))).subscribe(console.log);
// Output: x, y, x, y, x, y

// Edge case 2: hot inner — switching just re-observes; nothing is restarted
const live$ = new Subject<string>();
of('a', 'b', 'c').pipe(switchMapTo(live$)).subscribe(console.log);
live$.next('ping');
// Output: ping   (exactly once — only the latest subscription is live)

// Edge case 3: Promise inner — cannot be cancelled, only ignored
let calls = 0;
const once = new Promise<number>(resolve => { calls++; setTimeout(() => resolve(42), 10); });
of('a', 'b', 'c').pipe(switchMapTo(once)).subscribe(console.log);
// Output: 42   (calls === 1 — the first two subscriptions were dropped before it resolved)
```

## Common Pitfalls

### Anti-pattern: Still Using the Deprecated Form
```typescript
import { fromEvent, of } from 'rxjs';
import { switchMapTo, switchMap } from 'rxjs/operators';

const click$ = fromEvent(document, 'click');
const data$  = of('result');

// ❌ INCORRECT — deprecated; emits a compiler warning and disappears in the next major
click$.pipe(switchMapTo(data$)).subscribe(console.log);

// ✅ CORRECT — a zero-argument projection is the drop-in replacement
click$.pipe(switchMap(() => data$)).subscribe(console.log);

// WHY: switchMapTo(x) is implemented as switchMap(() => x). The *To family was
// deprecated because the constant form hides when the inner is created and
// adds nothing the projection form cannot express.
```

### Anti-pattern: Switching on a Write Operation
```typescript
import { fromEvent } from 'rxjs';
import { ajax } from 'rxjs/ajax';
import { switchMap, exhaustMap, concatMap } from 'rxjs/operators';

const save$ = fromEvent(document.getElementById('save')!, 'click');

// ❌ INCORRECT — a double-click aborts the first POST mid-flight; the server may
// or may not have applied it, and the client never finds out
save$.pipe(
  switchMap(() => ajax.post('/api/save', { data: 'important' }))   // formerly switchMapTo(...)
).subscribe(console.log);

// ✅ CORRECT — ignore extra clicks while a save is in progress…
save$.pipe(
  exhaustMap(() => ajax.post('/api/save', { data: 'important' }))
).subscribe(console.log);

// ✅ …or queue them if every click must be persisted in order
save$.pipe(
  concatMap(() => ajax.post('/api/save', { data: 'important' }))
).subscribe(console.log);

// WHY: switch semantics cancel the previous inner. Cancellation is a feature for
// reads (stale search results) and a bug for writes (lost or half-applied
// mutations). Reserve switchMap for idempotent, read-only inners.
```

### Anti-pattern: Expecting the Inner to Be Created per Value
```typescript
import { fromEvent } from 'rxjs';
import { ajax } from 'rxjs/ajax';
import { switchMapTo, switchMap } from 'rxjs/operators';

const refresh$ = fromEvent(document.getElementById('refresh')!, 'click');

// ❌ INCORRECT — fetch() runs once, right now; later clicks just re-read the same Promise
refresh$.pipe(
  switchMapTo(fetch('/api/feed'))
).subscribe(() => console.log('refreshed?'));
// The feed is fetched exactly once — before anyone clicked

// ✅ CORRECT — create the request inside the projection; a cold ajax call is also abortable
refresh$.pipe(
  switchMap(() => ajax.getJSON('/api/feed'))
).subscribe(() => console.log('refreshed'));

// WHY: switchMapTo evaluates its argument exactly once, at pipe-assembly time.
// A Promise cannot be re-run or cancelled; a cold Observable created per value
// can be both.
```

## Related Operators

**Same Category (Higher-Order Transformation)**:
- **`switchMap(project)`**: The replacement — identical semantics with a per-value projection
- **`concatMapTo(inner$)`**: Deprecated sibling — queues instead of cancelling
- **`mergeMapTo(inner$)`**: Deprecated sibling — runs inners concurrently instead of cancelling
- **`switchAll()`**: Flattens an `Observable<Observable<T>>` with switch semantics, without any projection

**Complementary Operators**:
- **`timer` / `interval`**: The classic inners for "restart on trigger" patterns
- **`takeUntil`**: Bound the inner with an external stop signal
- **`debounceTime`**: Rate-limit the trigger so the inner is not restarted on every keystroke
- **`defer`**: Wrap an eager producer so it is re-created per subscription

**Alternatives by Use Case**:

| Use Case | Instead of `switchMapTo` | Use This | Why |
|----------|--------------------------|----------|-----|
| Restart the same inner per value | `switchMapTo(x$)` | `switchMap(() => x$)` | Non-deprecated, identical behavior |
| Inner depends on the value | `switchMapTo` + closure tricks | `switchMap(v => f(v))` | That is what the projection is for |
| Writes / non-idempotent work | `switchMapTo` | `exhaustMap(() => x$)` or `concatMap(() => x$)` | Never cancels a mutation |
| Every result must be kept | `switchMapTo` | `mergeMap(() => x$)` | No cancellation |

**Migration Notes**:
```typescript
// Deprecated API (RxJS 6 / 7)
source$.pipe(switchMapTo(inner$))

// Current API (RxJS 7+)
source$.pipe(switchMap(() => inner$))

// If the inner was an eager Promise or request, also defer its creation:
source$.pipe(switchMap(() => ajax.getJSON(url)))  // created and abortable per value
```

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/switchMapTo](https://rxjs.dev/api/operators/switchMapTo)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/operators/flatmap.html](http://reactivex.io/documentation/operators/flatmap.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/switchMapTo.ts](https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/switchMapTo.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Cancelling Flattening with a Constant Inner (deprecated specialization of Cancelling Flattening)
- **Cognitive Load**: 2/5 — Trivial once `switchMap` is understood; the subtleties are single evaluation of the inner and that Promises cannot truly be cancelled
- **Usage Frequency**: 1/5 — Legacy code only; new code should never introduce it
- **Composability**: 2/5 — Composes like `switchMap`, but the fixed inner removes the ability to react to values

**Teaching Sequence**:
- **Prerequisites**: `switchMap`, cancellation semantics, cold vs. hot Observables
- **Teaches**: Why the `*To` operators were retired — eager evaluation of the inner argument; cancellation as a feature for reads and a bug for writes
- **Leads to**: Migration patterns, `exhaustMap` for writes, `defer` for eager producers
- **Common with**: `switchMap`, `timer`, `takeUntil`, `concatMapTo`, `mergeMapTo`
