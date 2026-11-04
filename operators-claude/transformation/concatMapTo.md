# concatMapTo

## Identity
- **Name**: concatMapTo
- **Category**: Transformation Operators
- **Type**: Sequential higher-order flattener with a fixed inner Observable — ⚠️ **Deprecated since RxJS 7**, slated for removal in the next major release; use `concatMap(() => inner$)` instead
- **Import**:
  ```typescript
  import { concatMapTo } from 'rxjs/operators';
  // RxJS 7.2+: also exported from the root
  import { concatMapTo } from 'rxjs';
  ```
- **Signature**:
  ```typescript
  function concatMapTo<O extends ObservableInput<unknown>>(
    observable: O
  ): OperatorFunction<unknown, ObservedValueOf<O>>
  ```

## Functional Specification

**Input**: `Observable<unknown>` — source values act only as triggers; their content is discarded

**Output**: `Observable<ObservedValueOf<O>>` — the values of `observable`, replayed through one fresh subscription per source emission, with subscriptions running one at a time in source order

**Transformation**:
Equivalent to `concatMap(() => observable)`. Every source emission enqueues one subscription to the same `observable`. The next subscription begins only after the previous inner subscription completes. Source values themselves never reach the output.

**Mathematical representation**:
```
concatMapTo(I)(S) = concat(I, I, I, ...)      one I per emission of S
                  = concatMap(_ => I)(S)

|output| = |S| × |I|        (when I is finite and S completes)
order    = inner subscriptions never overlap; I₁ runs to completion before I₂ starts
```

**Invariants**:
- **Fixed inner**: The same `observable` reference is subscribed every time — it is evaluated once, when the pipe is assembled, never per value
- **Sequential**: At most one inner subscription is active at any moment (`concurrent = 1`)
- **Order-preserving**: Output order matches source order; no interleaving
- **Value-blind**: Source values are ignored; only their timing matters

## Marble Diagram

```
Source:   --a-------b-------c-------|
          concatMapTo(x--x--x|)
inner(a):   x--x--x|
inner(b):           x--x--x|
inner(c):                   x--x--x|
Result:   --x--x--x-x--x--x-x--x--x|
```

**Source emits faster than the inner completes** (queueing):
```
Source:   --a--b--c------------------|
          concatMapTo(x----x|)
inner(a):   x----x|
inner(b):         x----x|
inner(c):               x----x|
Result:   --x----x-x----x-x----x-----|

b and c are queued; each inner starts only after the previous one completes.
```

**Inner never completes** (stall):
```
Source:   --a--b--c--|
          concatMapTo(x--x--x--x--...)
inner(a):   x--x--x--x--x--x--...
Result:   --x--x--x--x--x--x--...

inner(b) and inner(c) never start — the queue is blocked forever.
```

**Key observation**: `concatMapTo` is `concatMap` with the projection replaced by a constant. The constant is the trap: the inner Observable is created **once**, when the pipeline is built. A cold inner is re-run per source value (fine); a hot inner or an eagerly started Promise is simply observed again, and its work is not repeated.

## Behavioral Characteristics

**Subscription**:
- Subscribes to the source immediately on output subscription
- Subscribes to `observable` once per source emission, strictly one at a time; pending emissions are queued in memory
- If `observable` is a Promise, `from(promise)` is applied — the Promise has already started and resolves to the same value every time

**Completion semantics**:
- Output completes when the source has completed **and** every queued inner subscription has completed
- A source that completes while an inner is still active does not end the output
- If the inner never completes, the queue stalls and the output never completes

**Error handling**:
- A source error propagates immediately and the active inner is unsubscribed
- An inner error propagates immediately, terminating the output and dropping the queue
- No retry or recovery; wrap the inner with `catchError` if one failure should not abort the chain

**Backpressure**:
- Unbounded queue — every source emission that arrives while an inner is active is buffered (O(n) memory in queued emissions)
- Bursty sources against a slow inner grow the queue without limit

**Hot vs. Cold**:
- Cold inner: each queued subscription re-executes the producer (new HTTP request, new timer)
- Hot inner (Subject, shared stream): each subscription just observes the live stream from that moment; nothing is re-run
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
 *   ObservedValueOf<Promise<R>> = R, ObservedValueOf<R[]> = R.
 *
 * Type Safety:
 *   Every call site is flagged with a deprecation strikethrough (@deprecated JSDoc).
 */

import { fromEvent, of } from 'rxjs';
import { concatMapTo, concatMap } from 'rxjs/operators';

const click$ = fromEvent(document, 'click');            // Observable<Event>
const tick$  = of('tick');                                // Observable<string>

const deprecated$ = click$.pipe(concatMapTo(tick$));      // Observable<string>
const current$    = click$.pipe(concatMap(() => tick$));  // Observable<string> — identical type
```

## Examples

### Basic Usage — Deprecated Form and Its Replacement
```typescript
import { of, interval } from 'rxjs';
import { concatMapTo, concatMap, take } from 'rxjs/operators';

const steps$ = interval(100).pipe(take(3));

// ❌ Deprecated — still compiles in RxJS 7 with a strikethrough warning
of('a', 'b').pipe(
  concatMapTo(steps$)
).subscribe(console.log);
// Output: 0, 1, 2, 0, 1, 2   (second run starts only after the first completes)

// ✅ Current — the same behavior, no deprecation
of('a', 'b').pipe(
  concatMap(() => steps$)
).subscribe(console.log);
// Output: 0, 1, 2, 0, 1, 2
```

### Common Pattern — Queue the Same Animation per Click
```typescript
import { fromEvent, of } from 'rxjs';
import { concatMap, delay } from 'rxjs/operators';

const button = document.getElementById('btn')!;

// A two-step animation that takes 500ms per step
const animation$ = of('fade-in', 'fade-out').pipe(
  concatMap(step => of(step).pipe(delay(500)))
);

// Rapid clicks do not overlap — each run queues behind the previous one
fromEvent(button, 'click').pipe(
  concatMap(() => animation$)      // formerly concatMapTo(animation$)
).subscribe(step => console.log('Animation step:', step));
// Click twice quickly:
// Output: fade-in (500ms), fade-out (1000ms), fade-in (1500ms), fade-out (2000ms)
```

### Common Pattern — Re-run a Fixed Request on Each Refresh
```typescript
import { fromEvent } from 'rxjs';
import { ajax } from 'rxjs/ajax';
import { concatMap } from 'rxjs/operators';

const refresh = document.getElementById('refresh')!;

// ajax.getJSON is cold — a new request is issued for every click, strictly in order
fromEvent(refresh, 'click').pipe(
  concatMap(() => ajax.getJSON<{ status: string }>('/api/status'))
).subscribe({
  next:  res => console.log('Status:', res.status),
  error: err => console.error(err)
});
// Output (per click, never overlapping): Status: ok
```

### Edge Cases — Promise Inner, Empty Inner, Erroring Inner
```typescript
import { of, EMPTY, throwError } from 'rxjs';
import { concatMapTo } from 'rxjs/operators';

// Edge case 1: a Promise is already running — it is NOT re-executed per value
let calls = 0;
const once = new Promise<number>(resolve => { calls++; resolve(42); });
of('a', 'b', 'c').pipe(concatMapTo(once)).subscribe(console.log);
// Output: 42, 42, 42   (calls === 1 — the Promise ran exactly once)

// Edge case 2: empty inner — output completes with nothing
of(1, 2, 3).pipe(concatMapTo(EMPTY)).subscribe({
  next:     v => console.log(v),
  complete: () => console.log('complete')
});
// Output: complete

// Edge case 3: erroring inner — the first source value already kills the stream
of(1, 2, 3).pipe(
  concatMapTo(throwError(() => new Error('inner failed')))
).subscribe({
  next:  v => console.log(v),
  error: e => console.log('error:', e.message)
});
// Output: error: inner failed   (values 2 and 3 are never processed)
```

## Common Pitfalls

### Anti-pattern: Still Using the Deprecated Form
```typescript
import { fromEvent, timer } from 'rxjs';
import { concatMapTo, concatMap } from 'rxjs/operators';

const click$ = fromEvent(document, 'click');

// ❌ INCORRECT — deprecated; emits a compiler warning and disappears in the next major
click$.pipe(
  concatMapTo(timer(1000))
).subscribe(() => console.log('one second after each click, in order'));

// ✅ CORRECT — a zero-argument projection is the drop-in replacement
click$.pipe(
  concatMap(() => timer(1000))
).subscribe(() => console.log('one second after each click, in order'));

// WHY: concatMapTo(x) is implemented as concatMap(() => x). The *To family was
// deprecated because the constant form hides when the inner is created (see the
// next pitfall) and adds nothing the projection form cannot express.
```

### Anti-pattern: Expecting the Inner to Be Created per Value
```typescript
import { fromEvent } from 'rxjs';
import { ajax } from 'rxjs/ajax';
import { concatMapTo, concatMap } from 'rxjs/operators';

const save$ = fromEvent(document.getElementById('save')!, 'click');

// ❌ INCORRECT — fetch() starts immediately, once, when this line runs.
// Every click just re-observes the same already-resolved Promise.
save$.pipe(
  concatMapTo(fetch('/api/save', { method: 'POST' }))
).subscribe(() => console.log('saved?'));
// Only ONE request is ever sent, and it was sent before the first click.

// ✅ CORRECT — defer creation until each source value arrives
save$.pipe(
  concatMap(() => fetch('/api/save', { method: 'POST' }))
).subscribe(() => console.log('saved'));

// ✅ ALSO CORRECT — a cold Observable inner is re-subscribed per value anyway
save$.pipe(
  concatMap(() => ajax.post('/api/save'))
).subscribe(() => console.log('saved'));

// WHY: concatMapTo evaluates its argument exactly once, at pipe-assembly time.
// A cold Observable is fine because each subscription re-runs the producer; a
// Promise, an eagerly started request, or a hot Subject is shared across every
// source value.
```

### Anti-pattern: Blocking the Queue With a Never-Completing Inner
```typescript
import { fromEvent, interval } from 'rxjs';
import { concatMap, take, switchMap } from 'rxjs/operators';

const click$ = fromEvent(document, 'click');

// ❌ INCORRECT — interval never completes, so the second click is queued forever
click$.pipe(
  concatMap(() => interval(1000))
).subscribe(console.log);

// ✅ CORRECT — bound the inner so the queue can advance…
click$.pipe(
  concatMap(() => interval(1000).pipe(take(5)))
).subscribe(console.log);

// ✅ …or pick a strategy that does not queue (restart on each click)
click$.pipe(
  switchMap(() => interval(1000))
).subscribe(console.log);

// WHY: concat semantics require each inner to complete before the next starts.
// An infinite inner turns the operator into "first click wins, forever".
```

## Related Operators

**Same Category (Higher-Order Transformation)**:
- **`concatMap(project)`**: The replacement — identical semantics with a per-value projection
- **`mergeMapTo(inner$)`**: Deprecated sibling — concurrent instead of sequential
- **`switchMapTo(inner$)`**: Deprecated sibling — cancels the previous inner instead of queueing
- **`concatAll()`**: Flattens an `Observable<Observable<T>>` sequentially without any projection

**Complementary Operators**:
- **`take(n)` / `takeUntil`**: Bound an otherwise infinite inner so the queue can drain
- **`catchError`**: Applied *inside* the inner so one failure does not abort the whole queue
- **`defer`**: Wrap an eager producer (Promise, started request) so it is re-created per subscription

**Alternatives by Use Case**:

| Use Case | Instead of `concatMapTo` | Use This | Why |
|----------|--------------------------|----------|-----|
| Same inner per value, in order | `concatMapTo(x$)` | `concatMap(() => x$)` | Non-deprecated, identical behavior |
| Inner depends on the value | `concatMapTo` + closure tricks | `concatMap(v => f(v))` | That is what the projection is for |
| Restart on each value | `concatMapTo` | `switchMap(() => x$)` | Cancels instead of queueing |
| Ignore values while busy | `concatMapTo` | `exhaustMap(() => x$)` | Drops instead of queueing |

**Migration Notes**:
```typescript
// Deprecated API (RxJS 6 / 7)
source$.pipe(concatMapTo(inner$))

// Current API (RxJS 7+)
source$.pipe(concatMap(() => inner$))

// If the inner was an eager Promise or request, also defer its creation:
source$.pipe(concatMap(() => fetch(url)))        // created per value
```

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/concatMapTo](https://rxjs.dev/api/operators/concatMapTo)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/operators/flatmap.html](http://reactivex.io/documentation/operators/flatmap.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/concatMapTo.ts](https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/operators/concatMapTo.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Sequential Flattening with a Constant Inner (deprecated specialization of Sequential Flattening)
- **Cognitive Load**: 2/5 — Trivial once `concatMap` is understood; the only subtlety is the single evaluation of the inner argument
- **Usage Frequency**: 1/5 — Legacy code only; new code should never introduce it
- **Composability**: 2/5 — Composes like `concatMap`, but the fixed inner removes the ability to react to values

**Teaching Sequence**:
- **Prerequisites**: `concatMap`, cold vs. hot Observables
- **Teaches**: Why the `*To` operators were retired — eager evaluation of the inner argument
- **Leads to**: Migration patterns, `defer` for eager producers
- **Common with**: `concatMap`, `mergeMapTo`, `switchMapTo`, `defer`
