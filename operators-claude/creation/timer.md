# timer

## Identity
- **Name**: timer
- **Category**: Creation Operators
- **Type**: Delayed one-shot or repeating sequence — emits after an initial delay, optionally continuing at a fixed period
- **Import**:
  ```typescript
  import { timer } from 'rxjs';
  ```
- **Signature**:
  ```typescript
  // One-shot: emits 0 then completes after dueTime ms
  function timer(dueTime: number | Date, scheduler?: SchedulerLike): Observable<0>

  // Repeating: emits 0, 1, 2, ... starting at dueTime, then every periodOrScheduler ms
  function timer(
    dueTime: number | Date,
    periodOrScheduler: number | SchedulerLike,
    scheduler?: SchedulerLike
  ): Observable<number>
  ```

## Functional Specification

**One-shot mode** (`timer(delay)`):
- Waits `delay` ms, emits `0`, then completes
- Equivalent to `of(0).pipe(delay(n))` or `interval(n).pipe(take(1))`

**Repeating mode** (`timer(delay, period)`):
- Waits `delay` ms, emits `0`
- Then emits `1, 2, 3, ...` every `period` ms, never completing

**`dueTime` as `Date`**: the timer fires at the specified absolute time (or immediately if the Date is in the past).

**Mathematical representation**:
```
One-shot:   timer(d)        → 0 at time d, then complete
Repeating:  timer(d, p)     → 0 at d, 1 at d+p, 2 at d+2p, ...

timer(0, p)  ≡  a version of interval(p) that fires at t=0 (not t=p)
timer(p, p)  ≡  interval(p)  (same first-emission timing)
```

**Invariants**:
- **Cold**: Each subscriber gets its own independent timer
- **One-shot always completes**: `timer(n)` (no period) always completes after one emission
- **First emission determined by `dueTime`**: `timer(0, p)` fires immediately; `interval(p)` fires after `p` ms
- **Timing is approximate**: subject to JS event loop scheduling

## Marble Diagram

```
timer(0):         0|          (emits 0 immediately, then completes)

timer(40ms):      ----0|      (emits 0 after 40ms, then completes)

timer(0, 40ms):   0----1----2----3-- ...  (fires at t=0, then every 40ms)

interval(40ms):   ----0----1----2----3--  (fires at t=40ms, then every 40ms)
                  ↑ timer(0,40) vs interval(40) differ only at first emission

timer(100ms, 40ms): ----------0----1----2--  (100ms initial delay, then 40ms period)
```

**Key observation**: `timer` subsumes `interval` — `timer(n, n)` is equivalent to `interval(n)` but with explicit control over the initial delay. `timer(0, n)` gives the "emit immediately then repeat" pattern that `interval` cannot express.

## Behavioral Characteristics

**One-shot** (`timer(delay)`):
- Subscribes → waits `delay` ms → emits `0` → completes → subscription released
- Safe for use with `forkJoin`, `lastValueFrom`, `toPromise` — always completes

**Repeating** (`timer(delay, period)`):
- Runs forever; must be bounded with `take`, `takeUntil`, etc.
- Identical lifecycle to `interval` after first emission

**Scheduler**:
- `asyncScheduler` (default): uses `setTimeout`/`setInterval`
- `VirtualTimeScheduler`: deterministic virtual time for tests
- `Date` as `dueTime`: fires at a wall-clock time; negative delay (past Date) = fires immediately

## Type System Integration

```typescript
/**
 * One-shot return type: Observable<0>  (literal type 0, not number)
 * Repeating return type: Observable<number>
 *
 * The literal type Observable<0> is notable — TypeScript can narrow this
 * in switchMap/map to know the exact value emitted.
 */

import { timer } from 'rxjs';
import { switchMap, map } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

// One-shot delay before action
timer(2000).pipe(
  switchMap(() => ajax.getJSON('/api/data'))
).subscribe(console.log);

// Repeating poll — timer(0, 5000) fires immediately then every 5s
timer(0, 5000).pipe(
  switchMap(() => ajax.getJSON<Status>('/api/status'))
).subscribe((status: Status) => updateStatusDisplay(status));

// Absolute time: fire at a specific Date
const midnight = new Date();
midnight.setHours(24, 0, 0, 0);
timer(midnight).subscribe(() => runDailyJob());
```

## Examples

### Basic Usage — One-Shot Delay and Repeating Clock
```typescript
import { timer } from 'rxjs';
import { take } from 'rxjs/operators';

// One-shot: delay then complete
timer(1000).subscribe({
  next:     v => console.log('fired:', v),   // fired: 0
  complete: () => console.log('done')
});

// Immediate-then-repeating: poll every 3 seconds starting now
timer(0, 3000).pipe(take(4)).subscribe(n => console.log('tick', n));
// tick 0 (immediately), tick 1 (3s), tick 2 (6s), tick 3 (9s)
```

### Common Pattern — Polling with Immediate First Load
```typescript
import { timer } from 'rxjs';
import { switchMap, catchError, distinctUntilChanged } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';
import { of } from 'rxjs';

// Load data immediately, then refresh every 30 seconds
const data$ = timer(0, 30_000).pipe(
  switchMap(() =>
    ajax.getJSON<DashboardData>('/api/dashboard').pipe(
      catchError(() => of(null))
    )
  ),
  distinctUntilChanged(deepEqual)
);

// Compare to interval(30_000): would wait 30s before first load
// timer(0, 30_000): loads immediately on page open
```

### Common Pattern — Delayed Notification / Toast
```typescript
import { timer } from 'rxjs';
import { takeUntil } from 'rxjs/operators';
import { Subject } from 'rxjs';

function showToast(message: string, durationMs = 3000): () => void {
  const dismissed$ = new Subject<void>();

  displayToast(message);

  timer(durationMs).pipe(
    takeUntil(dismissed$)
  ).subscribe(() => hideToast());

  return () => { dismissed$.next(); dismissed$.complete(); };
}

const dismiss = showToast('Saved successfully!', 3000);
// Auto-hides after 3 seconds; or call dismiss() to hide immediately
```

### Common Pattern — Retry with Exponential Backoff
```typescript
import { timer } from 'rxjs';
import { retryWhen, switchMap, delayWhen } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

ajax.getJSON('/api/data').pipe(
  retryWhen(errors =>
    errors.pipe(
      // Retry with exponential backoff: 1s, 2s, 4s, 8s, ...
      delayWhen((_, index) => timer(1000 * Math.pow(2, index)))
    )
  )
).subscribe(console.log);
```

### Common Pattern — Timeout Race
```typescript
import { timer, race } from 'rxjs';
import { map, switchMap } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

// Request must complete within 5 seconds or timeout
race(
  ajax.getJSON('/api/slow-endpoint'),
  timer(5000).pipe(
    map(() => { throw new Error('Request timed out after 5s'); })
  )
).subscribe({
  next:  data => process(data),
  error: err => handleTimeout(err)
});

// Or use the built-in timeout operator:
import { timeout } from 'rxjs/operators';
ajax.getJSON('/api/slow-endpoint').pipe(
  timeout(5000)
).subscribe({ next: process, error: handleTimeout });
```

### Edge Cases — dueTime = 0, Date as dueTime, Unsubscription
```typescript
import { timer } from 'rxjs';

// timer(0): still async (macrotask), not synchronous
let order: string[] = [];
timer(0).subscribe(() => order.push('timer'));
order.push('sync');
// After current call stack: order = ['sync', 'timer']
// timer(0) fires in the NEXT macrotask, not synchronously

// Date as dueTime (past date → fires immediately/soon)
const pastDate = new Date(0); // Jan 1 1970
timer(pastDate).subscribe(v => console.log('past date timer:', v));
// Fires immediately (past date treated as "now or asap")

// Unsubscribing prevents emission
const sub = timer(1000).subscribe(v => console.log('should not fire'));
sub.unsubscribe(); // cancel before 1000ms
// No output
```

## Common Pitfalls

### Anti-pattern: Using `timer` One-Shot When `delay` Operator Suits
```typescript
import { timer, of } from 'rxjs';
import { switchMap, delay } from 'rxjs/operators';

// ❌ VERBOSE — timer + switchMap to delay an existing Observable
of('hello').pipe(
  switchMap(v => timer(1000).pipe(map(() => v)))
).subscribe(console.log);

// ✅ CLEANER — delay operator shifts emission of the source directly
of('hello').pipe(
  delay(1000)
).subscribe(console.log);

// ✅ ALSO VALID — timer when you want the index value (0)
timer(1000).subscribe(() => doSomethingDelayed());

// WHY: timer is primarily for "emit a value after a delay" or "repeat on a schedule."
// When you want to delay an existing stream, delay() is the idiomatic choice.
// Use timer when the start of a new Observable pipeline is the delayed thing.
```

### Anti-pattern: Confusing `timer(n)` and `interval(n)` First-Emission Timing
```typescript
import { timer, interval } from 'rxjs';

// ❌ WRONG ASSUMPTION — thinking interval and timer(n,n) behave the same
// They DO have the same period after first emission, but first emission differs:
interval(1000).pipe(take(1)).subscribe(v => console.log('interval first:', v));
// fires after 1000ms

timer(1000, 1000).pipe(take(1)).subscribe(v => console.log('timer first:', v));
// fires after 1000ms  (same as interval here — timer(n,n) ≡ interval(n))

timer(0, 1000).pipe(take(1)).subscribe(v => console.log('timer(0,1000) first:', v));
// fires IMMEDIATELY (0ms initial delay)

// WHY: timer(0, period) fires at t=0; interval(period) fires at t=period.
// If you need an immediate first emission followed by periodic ones, always use
// timer(0, period) — interval cannot express this without startWith workarounds.
```

## Related Operators

**Same Category (Creation)**:
- **`interval(period)`**: Simpler form of `timer(period, period)` — use when no initial delay is needed
- **`of(...values)`**: Synchronous static values — use when timing is irrelevant
- **`defer(factory)`**: Creates a new Observable on each subscription — use when the Observable itself should be lazily created

**Complementary Operators**:
- **`delay(n)`**: Shifts all emissions of an existing stream by `n` ms
- **`timeout(n)`**: Errors if no emission arrives within `n` ms — pairs with `timer` for timeout races
- **`race`**: Selects the first Observable to emit — pairs with `timer` for deadline patterns
- **`retryWhen` + `delayWhen`**: Use `timer(backoff)` inside `delayWhen` for exponential retry backoff

**Decision Guide — timer vs interval**:

| Need | Use |
|------|-----|
| Repeat every N ms (first at N ms) | `interval(N)` |
| Repeat every N ms (first immediately) | `timer(0, N)` |
| One-shot delay, then complete | `timer(N)` |
| Custom initial delay + period | `timer(initialDelay, period)` |
| Fire at specific clock time | `timer(new Date(...))` |
| Delay an existing stream | `source$.pipe(delay(N))` |

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/index/function/timer](https://rxjs.dev/api/index/function/timer)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/src/internal/observable/timer.ts](https://github.com/ReactiveX/rxjs/blob/master/src/internal/observable/timer.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Scheduled One-Shot or Repeating Source
- **Cognitive Load**: 1/5 — Very simple; the one/two-argument distinction and the timer(0,n) vs interval(n) difference are the only subtleties
- **Usage Frequency**: 5/5 — Used in every polling pattern, retry backoff, delayed notification, and timeout race
- **Composability**: 5/5 — Foundational creation operator; universal building block

**Teaching Sequence**:
- **Prerequisites**: `interval` (contrast is essential)
- **Teaches**: Delayed emission, one-shot vs. repeating, the initial-delay control that interval lacks
- **Common with**: `switchMap`, `retry`/`retryWhen`, `race`, `takeUntil`, `delay`
