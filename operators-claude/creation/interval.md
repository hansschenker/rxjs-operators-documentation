# interval

## Identity
- **Name**: interval
- **Category**: Creation Operators
- **Type**: Time-based sequence generator — emits sequential integers at a fixed period
- **Import**:
  ```typescript
  import { interval } from 'rxjs';
  ```
- **Signature**:
  ```typescript
  function interval(
    period?: number,
    scheduler?: SchedulerLike
  ): Observable<number>
  ```

## Functional Specification

**Input**: `period` — milliseconds between emissions (default: 0); `scheduler` — timing provider (default: `asyncScheduler`)

**Output**: `Observable<number>` — an infinite Observable that emits `0, 1, 2, 3, ...` at each period

**Transformation**: Creates a cold Observable. Upon subscription, waits `period` ms, then emits `0`. Waits another `period` ms, emits `1`. Continues indefinitely until unsubscribed.

**Mathematical representation**:
```
interval(period) = Observable that emits:
  n at time: (n + 1) × period  for n = 0, 1, 2, ...

First emission at: period ms  (not 0!)
Value at emission n: n

Never completes. Never errors. Infinite.
```

**Invariants**:
- **First emission is delayed**: Waits one full `period` before emitting `0` — there is no immediate emission at t=0
- **Strictly sequential integers**: Always emits 0, 1, 2, ... in order; no gaps, no duplicates
- **Never completes**: Runs forever until unsubscribed
- **Cold**: Each subscriber gets its own independent counter and timer
- **Timing is approximate**: Subject to JavaScript event loop scheduling

## Marble Diagram

```
interval(40ms):
  0    1    2    3    4  ...
  |    |    |    |    |
--0----1----2----3----4-- (never ends)
  ↑
  First emission at 40ms (not 0ms)
```

**Contrast with `timer`**:
```
timer(0, 40ms):   0----1----2----3----4-- (first emission at 0ms)
interval(40ms):   ----0----1----2----3--- (first emission at 40ms)

timer(100, 40ms): ----------0----1----2-- (initial delay, then 40ms period)
```

**Cold — independent per subscriber**:
```
Sub A at t=0: ----0----1----2----3--...
Sub B at t=20:        ----0----1----2--...

Each subscriber gets its own counter starting from 0.
```

**Key observation**: `interval(n)` is equivalent to `timer(n, n)` — both wait `n` ms for the first emission then emit every `n` ms. The practical difference is that `timer(0, n)` emits immediately at subscription, while `interval(n)` always delays.

## Behavioral Characteristics

**Subscription**:
- Creates a repeating timer upon subscription
- Each subscription creates an independent timer — `interval` is cold
- No emissions occur before the first period elapses

**Completion semantics**:
- Never completes on its own — must be bounded with `take`, `takeUntil`, `takeWhile`, etc.

**Error handling**:
- Never errors — integer emission cannot fail

**Backpressure**:
- None — emissions are independent of downstream consumption rate
- If downstream processing is slower than `period`, values queue up in the subscription

**Scheduler**:
- `asyncScheduler` (default): uses `setInterval` — subject to JS event loop delays, background tab throttling
- `animationFrameScheduler`: aligns with browser repaint (~60fps); useful for animation clocks
- `VirtualTimeScheduler`: deterministic virtual time for testing — `interval` advances with `scheduler.flush()`
- `asapScheduler`: microtask-based, fires as soon as possible after current synchronous block

## Type System Integration

```typescript
/**
 * Return Type: Observable<number>
 *   Always emits numbers (0, 1, 2, ...) — no generics involved.
 *   The value represents the emission index, not meaningful domain data.
 *
 * Commonly mapped to domain values:
 */

import { interval } from 'rxjs';
import { map, take } from 'rxjs/operators';

// Emit current timestamp every second
const clock$ = interval(1000).pipe(
  map(() => new Date()) // Observable<Date>
);

// Poll an API every 5 seconds
const pollResults$ = interval(5000).pipe(
  switchMap(() => ajax.getJSON<Data>('/api/status')), // Observable<Data>
  distinctUntilChanged(deepEqual)
);

// Count-up animation (0–100 over 2 seconds)
const counter$ = interval(20).pipe(
  take(100),
  map(n => n + 1) // Observable<number> (1–100)
);
```

## Examples

### Basic Usage — Counter, Polling, Animation
```typescript
import { interval } from 'rxjs';
import { take, map } from 'rxjs/operators';

// Count up: 0, 1, 2, 3, 4
interval(1000).pipe(take(5)).subscribe(console.log);
// Output (one per second): 0, 1, 2, 3, 4

// 1-indexed counter
interval(500).pipe(
  map(n => n + 1),
  take(3)
).subscribe(n => console.log(`tick ${n}`));
// tick 1, tick 2, tick 3
```

### Common Pattern — Polling with Error Handling
```typescript
import { interval } from 'rxjs';
import { switchMap, catchError, distinctUntilChanged } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';
import { of } from 'rxjs';

interface ServerStatus { healthy: boolean; version: string; }

const statusPoll$ = interval(10_000).pipe( // poll every 10 seconds
  switchMap(() =>
    ajax.getJSON<ServerStatus>('/api/health').pipe(
      catchError(() => of({ healthy: false, version: 'unknown' }))
    )
  ),
  distinctUntilChanged((a, b) => a.healthy === b.healthy && a.version === b.version)
);

statusPoll$.subscribe(status => {
  if (!status.healthy) showHealthWarning();
  else                 clearHealthWarning();
});
```

### Common Pattern — Countdown Timer
```typescript
import { interval } from 'rxjs';
import { take, map, tap, finalize } from 'rxjs/operators';

function countdown(seconds: number): Observable<number> {
  return interval(1000).pipe(
    take(seconds),
    map(elapsed => seconds - elapsed - 1),
    tap(remaining => updateCountdownDisplay(remaining)),
    finalize(() => console.log('countdown complete'))
  );
}

countdown(10).subscribe({
  next:     n => console.log(n),  // 9, 8, 7, ..., 1, 0
  complete: () => executeAction()
});
```

### Common Pattern — Combining with `takeUntil` for Managed Lifecycles
```typescript
import { interval, Subject } from 'rxjs';
import { takeUntil, map, switchMap } from 'rxjs/operators';
import { Component, OnInit, OnDestroy } from '@angular/core';

@Component({ selector: 'app-live-data', template: '{{ data | async | json }}' })
export class LiveDataComponent implements OnInit, OnDestroy {
  private destroy$ = new Subject<void>();

  data$ = interval(2000).pipe(
    takeUntil(this.destroy$),          // tied to component lifetime
    switchMap(() => ajax.getJSON('/api/live'))
  );

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
```

### Common Pattern — Rate-Limited Source for Testing
```typescript
import { interval } from 'rxjs';
import { zip, from } from 'rxjs';
import { map } from 'rxjs/operators';

// Emit items from an array at a controlled rate (one per second)
function emitAtRate<T>(items: T[], periodMs: number): Observable<T> {
  return zip(
    from(items),
    interval(periodMs)
  ).pipe(map(([item]) => item));
}

emitAtRate(['a', 'b', 'c', 'd'], 500).subscribe(console.log);
// a (at 500ms), b (at 1000ms), c (at 1500ms), d (at 2000ms)
```

### Testing with VirtualTimeScheduler
```typescript
import { interval } from 'rxjs';
import { VirtualTimeScheduler } from 'rxjs';
import { take, toArray } from 'rxjs/operators';

const scheduler = new VirtualTimeScheduler();

const values: number[] = [];
interval(100, scheduler).pipe(take(5)).subscribe(v => values.push(v));

scheduler.flush(); // advances virtual time; all timers fire synchronously
console.log(values); // [0, 1, 2, 3, 4]  — deterministic, no real waiting
```

### Edge Cases — Period Zero, Immediate Emission, Concurrent Subscribers
```typescript
import { interval } from 'rxjs';
import { take } from 'rxjs/operators';

// Edge case 1: period = 0 — fires as fast as the event loop allows (macrotask)
// NOT synchronous — each emission still goes through the asyncScheduler
interval(0).pipe(take(3)).subscribe(console.log);
// Outputs asynchronously: 0, 1, 2  (very fast, but NOT synchronous)

// Edge case 2: two subscribers are independent (cold)
const counter$ = interval(1000).pipe(take(3));
counter$.subscribe(v => console.log('A:', v));

setTimeout(() => {
  counter$.subscribe(v => console.log('B:', v));
}, 500);

// A: 0 at 1000ms
// B: 0 at 1500ms  (B has its own independent counter starting from 0)
// A: 1 at 2000ms
// B: 1 at 2500ms
// A: 2 at 3000ms
// B: 2 at 3500ms

// Edge case 3: no unsubscription — memory/CPU leak
const sub = interval(100).subscribe(console.log);
// Must call sub.unsubscribe() or use take/takeUntil to stop
```

## Common Pitfalls

### Anti-pattern: Expecting Immediate First Emission
```typescript
import { interval, timer } from 'rxjs';
import { take, map } from 'rxjs/operators';

// ❌ WRONG ASSUMPTION — first interval emission is at period ms, not 0
interval(1000).pipe(take(1)).subscribe(v => console.log('first value:', v));
// Value '0' arrives after 1000ms — not immediately

// ✅ CORRECT — if you need an immediate first emission, use timer(0, 1000)
timer(0, 1000).pipe(take(2)).subscribe(v => console.log('value:', v));
// value: 0 at 0ms (immediately)
// value: 1 at 1000ms

// ✅ ALSO CORRECT — startWith on interval
interval(1000).pipe(
  startWith(-1) // emit -1 immediately, then 0, 1, 2, ... starting 1s later
).subscribe(console.log);

// WHY: interval(n) is designed to emit n ms AFTER subscription, then every n ms.
// It models a repeating clock tick, not an immediate value followed by ticks.
// Use timer(0, n) for emit-immediately-then-repeat semantics.
```

### Anti-pattern: Forgetting to Unsubscribe from `interval`
```typescript
import { interval } from 'rxjs';
import { switchMap } from 'rxjs/operators';

// ❌ MEMORY/CPU LEAK — interval runs forever if not unsubscribed
class PriceFeedComponent {
  ngOnInit() {
    interval(1000).pipe(
      switchMap(() => fetch('/api/price').then(r => r.json()))
    ).subscribe(price => this.updateDisplay(price));
    // No unsubscription! Component destroyed → interval keeps running → fetch keeps firing
  }
}

// ✅ CORRECT — use takeUntil or store the subscription
class SafePriceFeedComponent implements OnDestroy {
  private destroy$ = new Subject<void>();

  ngOnInit() {
    interval(1000).pipe(
      takeUntil(this.destroy$),
      switchMap(() => fetch('/api/price').then(r => r.json()))
    ).subscribe(price => this.updateDisplay(price));
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }
}

// WHY: interval never completes. If the subscription is not explicitly cleaned up,
// the timer keeps running and the associated callbacks keep executing — even after
// the component/service is destroyed. This leaks memory and causes unexpected
// side effects (updates to destroyed DOM, stale network requests).
```

### Anti-pattern: Using `interval` for One-Shot Delays
```typescript
import { interval, timer, of } from 'rxjs';
import { take, delay } from 'rxjs/operators';

// ❌ VERBOSE — interval + take(1) for a single delayed action
interval(2000).pipe(take(1)).subscribe(() => showToast());

// ✅ CLEANER — timer(2000) is the idiomatic one-shot delay
timer(2000).subscribe(() => showToast());

// ✅ ALSO CLEAN — delay operator for delaying an existing observable
of('hello').pipe(delay(2000)).subscribe(console.log);

// WHY: interval is designed for *repeating* emissions. For a single delayed action,
// timer() or delay() communicate intent more clearly and avoid the take(1) noise.
// Use interval when you genuinely need a repeating clock; use timer for one-shots.
```

## Related Operators

**Same Category (Creation)**:
- **`timer(delay, period?)`**: Like `interval` but with an initial delay before the first emission, and optional period for repeating; `timer(n, n)` is equivalent to `interval(n)` but with explicit control over initial delay
- **`of(...values)`**: Synchronous static emission — use when values are known ahead of time
- **`from(iterable)`**: Convert array, Promise, or iterable to Observable — use for static or async single-result sources
- **`range(start, count)`**: Emits a sequence of integers immediately (synchronous) — use when count is known and timing is irrelevant

**Commonly Composed With**:
- **`take(n)`**: Make interval finite by count
- **`takeUntil(destroy$)`**: Tie interval lifetime to component/service lifecycle
- **`switchMap`**: Trigger a new inner Observable on each tick (polling pattern)
- **`map`**: Transform the emitted index into a meaningful domain value

**Decision Guide — Time-based Observables**:

| Use Case | Operator | Why |
|----------|----------|-----|
| Repeat every N ms, first after N ms | `interval(N)` | Standard repeating clock |
| Repeat every N ms, first immediately | `timer(0, N)` | First tick at t=0 |
| Single delay, then complete | `timer(N)` | One-shot delay |
| Repeating with custom initial delay | `timer(delay, period)` | Full control |
| Delay an existing Observable | `source$.pipe(delay(N))` | Shifts emission timing |

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/index/function/interval](https://rxjs.dev/api/index/function/interval)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/operators/interval.html](http://reactivex.io/documentation/operators/interval.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/observable/interval.ts](https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/observable/interval.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Repeating Clock Source
- **Cognitive Load**: 1/5 — One of the simplest operators; the "first emission is delayed" rule and unsubscription requirement are the two teaching points
- **Usage Frequency**: 5/5 — Used in virtually every RxJS example, tutorial, and production polling pattern
- **Composability**: 5/5 — Foundational creation operator; pairs with every filtering/transformation operator

**Teaching Sequence**:
- **Prerequisites**: None — ideal as a first operator alongside `of`, `from`
- **Teaches**: Cold Observables, time-based sources, infinite streams, the unsubscription requirement
- **Leads to**: `timer`, `take`, `takeUntil`, polling patterns
- **Common with**: `take`, `takeUntil`, `switchMap`, `map`, `VirtualTimeScheduler`
