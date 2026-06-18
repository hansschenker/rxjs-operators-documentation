# delay / delayWhen — Advanced Patterns

For `delay` and `delayWhen` fundamentals see the core [delay / delayWhen](./delay-delayWhen) doc. This page covers animation staggering, scheduled coordination, adaptive delays, and composable delay utilities.

---

## `delay` vs `delayWhen` — The Core Distinction

```typescript
import { delay, delayWhen } from 'rxjs/operators';
import { timer } from 'rxjs';

// delay(ms) — fixed delay for all values:
source$.pipe(delay(500))
// Every value is delayed by exactly 500ms

// delayWhen(fn) — per-value dynamic delay via Observable:
source$.pipe(
  delayWhen((value, index) => timer(index * 100))
  // 1st value: 0ms delay, 2nd: 100ms, 3rd: 200ms...
)
```

`delayWhen` runs the factory for each value and waits until that Observable emits before forwarding the value.

---

## Pattern 1: Staggered Animation (Cascade)

Animate a list of items entering one after another:

```typescript
import { from, timer } from 'rxjs';
import { delayWhen, map, mergeMap } from 'rxjs/operators';

const items = document.querySelectorAll('.list-item');

from(Array.from(items)).pipe(
  delayWhen((_, index) => timer(index * 80)) // 80ms apart
).subscribe(item => {
  item.classList.add('visible'); // CSS transition takes it from there
});
```

---

## Pattern 2: Retry with Per-Attempt Delay

```typescript
import { defer, retry } from 'rxjs';
import { timer } from 'rxjs';

// Exponential backoff via delayWhen (alternative to retry({ delay })):
function withStaggeredRetry<T>(source$: Observable<T>, attempts = 4): Observable<T> {
  let attempt = 0;
  return source$.pipe(
    delayWhen(() => timer(1000 * Math.pow(2, attempt++))),
    retry({ count: attempts })
  );
}
```

---

## Pattern 3: Sequenced Notification Toasts

Show notifications one after another without overlap:

```typescript
import { Subject, concatMap, delay, finalize } from 'rxjs';

const notifications$ = new Subject<Notification>();

// Each notification stays visible for 3s, then next appears:
notifications$.pipe(
  concatMap(notification =>
    of(notification).pipe(
      tap(n => showToast(n)),
      delay(3000),                // display for 3 seconds
      finalize(() => hideToast()) // cleanup when this completes or is unsubscribed
    )
  )
).subscribe();

// Queue notifications:
notifications$.next({ message: 'Saved!', type: 'success' });
notifications$.next({ message: 'New update available', type: 'info' });
```

---

## Pattern 4: Adaptive Delay Based on Value

Delay proportional to the value being emitted:

```typescript
import { delayWhen } from 'rxjs/operators';
import { timer } from 'rxjs';

// Rate items by priority — low priority items get delayed more:
items$.pipe(
  delayWhen(item => {
    const delayMs = { high: 0, medium: 200, low: 500 }[item.priority] ?? 300;
    return timer(delayMs);
  })
).subscribe(processItem);
```

---

## Pattern 5: Coordinating with External Async Events

`delayWhen` can wait for any Observable — not just timers:

```typescript
import { delayWhen } from 'rxjs/operators';

// Don't emit until the DOM is ready:
const domReady$ = fromEvent(document, 'DOMContentLoaded').pipe(take(1));

dataStream$.pipe(
  delayWhen(() => domReady$) // each value waits for DOM ready before forwarding
).subscribe(renderToDom);

// Coordinate with route transitions:
const routeTransitionComplete$ = this.router.events.pipe(
  filter(e => e instanceof NavigationEnd),
  take(1)
);

criticalData$.pipe(
  delayWhen(() => routeTransitionComplete$) // wait for navigation to finish
).subscribe(renderAfterTransition);
```

---

## Pattern 6: Pulsing / Heartbeat Animation

Animate values with a pulsing rhythm:

```typescript
import { interval, delay, map } from 'rxjs';

// Pulse: each value appears, waits, then next:
from(['Step 1', 'Step 2', 'Step 3', 'Step 4']).pipe(
  concatMap((step, i) =>
    of(step).pipe(delay(i === 0 ? 0 : 600)) // 600ms between steps
  )
).subscribe(step => highlightStep(step));
```

---

## Pattern 7: `delayWhen` for Throttling by Observable Signal

```typescript
import { Subject, delayWhen, exhaustMap } from 'rxjs';

// Don't emit next item until the previous one signals "ready":
const readyForNext$ = new Subject<void>();

items$.pipe(
  delayWhen((_, index) =>
    index === 0
      ? of(undefined)         // first item: no delay
      : readyForNext$.pipe(take(1)) // subsequent: wait for signal
  )
).subscribe(item => {
  processItem(item);
  // When done processing, signal readiness:
  item.onComplete = () => readyForNext$.next();
});
```

---

## `delay` with Negative / Zero Values

```typescript
// delay(0) — defer to next microtask/macrotask (useful for avoiding
// ExpressionChangedAfterItHasBeenCheckedError in Angular):
source$.pipe(delay(0)).subscribe(updateView);

// delay(0) uses asyncScheduler internally — defers to setTimeout(fn, 0)
// Useful for breaking synchronous chains that cause change-detection issues
```

---

## Common Pitfalls

### `delay` Delays Individual Values, Not the Subscription

```typescript
// ❌ Misconception: delay delays when the source is subscribed to
source$.pipe(delay(1000)).subscribe();
// Source is subscribed IMMEDIATELY — values are buffered and forwarded 1s later

// ✅ To delay subscription itself, use timer + switchMap or delayWhen:
timer(1000).pipe(switchMap(() => source$)).subscribe();
// Source is NOT subscribed until 1s has elapsed
```

### `delayWhen` — Factory Called for Each Value (Not Once)

```typescript
// ❌ Incorrect assumption: factory runs once, delay is shared
source$.pipe(
  delayWhen(() => {
    console.log('called once?'); // called for EVERY value!
    return timer(500);
  })
)

// ✅ If you need a one-time computed delay, compute outside:
const delayMs = computeDelay();
source$.pipe(delay(delayMs))
```

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 3/5 | **Composability**: 4/5
**Primary use cases**: Animation staggering (`delayWhen` with index-based timer), notification sequencing (`concatMap` + `delay`), coordinating with external async signals (`delayWhen(() => externalObs$)`). For simple fixed delays, `delay(ms)` is all you need.
