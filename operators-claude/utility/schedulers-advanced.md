# Schedulers — Advanced Patterns

For the scheduler overview see the core [Schedulers](./schedulers) doc. This page covers the internal execution model of each scheduler, zone interaction in Angular, performance optimization with `observeOn`/`subscribeOn`, and virtualTime testing.

---

## The Four Schedulers and Their Execution Contexts

```typescript
import {
  queueScheduler,
  asapScheduler,
  asyncScheduler,
  animationFrameScheduler
} from 'rxjs';

// queueScheduler — synchronous, recursive, uses a queue to prevent stack overflow
// asapScheduler  — microtask queue (Promise.resolve / queueMicrotask)
// asyncScheduler — macrotask queue (setTimeout / setInterval)  ← default for time operators
// animationFrameScheduler — requestAnimationFrame (≈16.7ms at 60fps)
```

**Execution order** when all four are scheduled at time=0:

```typescript
import { scheduled, queueScheduler, asapScheduler, asyncScheduler } from 'rxjs';

console.log('sync start');

scheduled([1], asyncScheduler).subscribe(v => console.log('async:', v));
scheduled([2], asapScheduler).subscribe(v  => console.log('asap:', v));
scheduled([3], queueScheduler).subscribe(v => console.log('queue:', v));

console.log('sync end');

// Output order:
// sync start
// queue: 3      ← synchronous flush
// sync end
// asap: 2       ← microtask (before next macrotask)
// async: 1      ← macrotask (setTimeout 0)
```

---

## Pattern 1: `queueScheduler` — Breadth-First Recursion

`queueScheduler` is synchronous but non-recursive: instead of calling the next action immediately, it queues it. This prevents stack overflow in recursive Observable patterns:

```typescript
import { queueScheduler, Observable } from 'rxjs';
import { observeOn } from 'rxjs/operators';

// ❌ Deep recursion without scheduler — stack overflow at ~10,000 items:
function recursiveObs(n: number): Observable<number> {
  return new Observable(subscriber => {
    subscriber.next(n);
    if (n > 0) recursiveObs(n - 1).subscribe(subscriber);
    else subscriber.complete();
  });
}

// ✅ queueScheduler prevents stack overflow via breadth-first queue:
function safeRecursiveObs(n: number): Observable<number> {
  return new Observable<number>(subscriber => {
    subscriber.next(n);
    if (n > 0) safeRecursiveObs(n - 1).pipe(
      observeOn(queueScheduler)
    ).subscribe(subscriber);
    else subscriber.complete();
  });
}

// Real use: expand() uses queueScheduler internally for this reason:
of(1).pipe(
  expand(n => n < 10000 ? of(n + 1) : EMPTY),
  // queueScheduler is expand's default — no stack overflow on deep trees
).subscribe(/* processes 10,000 items safely */);
```

---

## Pattern 2: `asapScheduler` — Microtask Batching

`asapScheduler` schedules work before the next rendering frame but after the current synchronous block. Useful for batching multiple synchronous state changes:

```typescript
import { Subject, asapScheduler } from 'rxjs';
import { observeOn, bufferTime } from 'rxjs/operators';

// Batch rapid synchronous state updates into a single re-render:
const stateChanges$ = new Subject<Partial<AppState>>();

stateChanges$.pipe(
  observeOn(asapScheduler), // defer until after synchronous block completes
  scan((state, patch) => ({ ...state, ...patch }), initialState),
  distinctUntilChanged((a, b) => JSON.stringify(a) === JSON.stringify(b))
).subscribe(newState => renderApp(newState));

// These three calls happen synchronously:
stateChanges$.next({ loading: true });
stateChanges$.next({ user: currentUser });
stateChanges$.next({ permissions: userPermissions });
// renderApp() called ONCE after the synchronous block, with all three patches applied
```

---

## Pattern 3: `asyncScheduler` — Replacing `setTimeout` / `setInterval`

`asyncScheduler` wraps `setTimeout`/`setInterval` — making timer-based code testable:

```typescript
import { asyncScheduler, Subscription } from 'rxjs';

// Drop-in replacement for setTimeout that can be tested with TestScheduler:
function delayedAction(fn: () => void, delayMs: number): Subscription {
  return asyncScheduler.schedule(fn, delayMs);
}

// Drop-in replacement for setInterval:
function periodicAction(fn: () => void, intervalMs: number): Subscription {
  return asyncScheduler.schedule(function(this: { schedule: Function }) {
    fn();
    this.schedule(undefined, intervalMs); // reschedule
  }, intervalMs);
}

// The key advantage: operators that accept a scheduler can be tested with
// TestScheduler's virtual time — no real 1000ms wait in tests:
interval(1000, asyncScheduler).pipe(
  take(5),
  map(i => `tick ${i}`)
).subscribe(console.log);

// In tests, swap asyncScheduler for TestScheduler:
getTestScheduler().run(({ cold, expectObservable }) => {
  const source$ = interval(1000, getTestScheduler()).pipe(take(5));
  expectObservable(source$).toBe('1000ms a 999ms b 999ms c 999ms d 999ms (e|)', {
    a: 0, b: 1, c: 2, d: 3, e: 4
  });
});
```

---

## Pattern 4: `animationFrameScheduler` — DOM Batching

Group multiple DOM reads/writes into a single animation frame to eliminate layout thrashing:

```typescript
import { animationFrameScheduler, Subject } from 'rxjs';
import { observeOn, debounceTime, map } from 'rxjs/operators';

// Multiple resize events → single DOM measurement per frame:
const resize$ = fromEvent(window, 'resize').pipe(
  observeOn(animationFrameScheduler) // coalesce to one per rAF
);

// Smooth progress bar without jank:
const progress$ = new Subject<number>();

progress$.pipe(
  observeOn(animationFrameScheduler), // one update per frame maximum
  map(p => Math.min(100, Math.max(0, p)))
).subscribe(pct => {
  progressBar.style.width = `${pct}%`;
});

// Batch concurrent property updates on the same element:
const elementUpdates$ = new Subject<{ prop: string; value: string }>();

elementUpdates$.pipe(
  bufferTime(0, animationFrameScheduler), // buffer until next frame
  filter(updates => updates.length > 0)
).subscribe(updates => {
  // Apply all accumulated updates in one synchronous block:
  updates.forEach(({ prop, value }) => {
    (targetElement.style as any)[prop] = value;
  });
});
```

---

## Pattern 5: Angular Zone Escape — `runOutsideAngular`

Angular's change detection triggers on every macrotask (setTimeout, XHR, etc.). Move high-frequency Observables outside the zone to prevent unnecessary change detection cycles:

```typescript
import { NgZone, Component, inject } from '@angular/core';
import { animationFrameScheduler, fromEvent, Observable } from 'rxjs';
import { observeOn } from 'rxjs/operators';

@Component({})
export class HighFrequencyComponent {
  private zone = inject(NgZone);

  // Run outside Angular zone — no change detection on every mousemove:
  private mouseMoves$: Observable<MouseEvent> = new Observable(subscriber => {
    this.zone.runOutsideAngular(() => {
      const handler = (e: MouseEvent) => subscriber.next(e);
      document.addEventListener('mousemove', handler);
      return () => document.removeEventListener('mousemove', handler);
    });
  });

  ngOnInit() {
    this.mouseMoves$.pipe(
      observeOn(animationFrameScheduler), // cap at 60fps
      map(e => ({ x: e.clientX, y: e.clientY })),
      distinctUntilChanged((a, b) => a.x === b.x && a.y === b.y),
      takeUntilDestroyed()
    ).subscribe(pos => {
      // Re-enter zone only for the final DOM update:
      this.zone.run(() => {
        this.cursorPosition = pos;
        // ChangeDetectionRef.markForCheck() if using OnPush
      });
    });
  }
}

// Pattern: outside zone for processing, back inside for rendering
class AnimationService {
  private zone = inject(NgZone);

  createAnimationLoop$(): Observable<number> {
    return new Observable<number>(subscriber => {
      this.zone.runOutsideAngular(() => {
        interval(0, animationFrameScheduler).pipe(
          takeUntilDestroyed()
        ).subscribe(frame => {
          // Heavy computation outside zone:
          const result = heavyComputation(frame);

          // Update Angular state inside zone:
          this.zone.run(() => subscriber.next(result));
        });
      });
    });
  }
}
```

---

## Pattern 6: Custom Scheduler for Testing

Inject the scheduler to make time-based code testable without real timers:

```typescript
import { TestScheduler } from 'rxjs/testing';
import { asyncScheduler, SchedulerLike } from 'rxjs';

// Service that accepts a scheduler (for testing):
class NotificationService {
  constructor(
    private scheduler: SchedulerLike = asyncScheduler
  ) {}

  dismissAfter(notification: Notification, ms: number): Observable<void> {
    return timer(ms, this.scheduler).pipe(
      map(() => void 0),
      take(1)
    );
  }

  retryWithBackoff<T>(
    source$: Observable<T>,
    maxRetries = 3
  ): Observable<T> {
    return source$.pipe(
      retry({
        count: maxRetries,
        delay: (err, n) => timer(1000 * Math.pow(2, n), this.scheduler)
      })
    );
  }
}

// In tests — virtual time, no real timers:
describe('NotificationService', () => {
  it('dismisses after delay', () => {
    const testScheduler = new TestScheduler((actual, expected) => {
      expect(actual).toEqual(expected);
    });

    testScheduler.run(({ cold, expectObservable }) => {
      const service = new NotificationService(testScheduler);
      const dismiss$ = service.dismissAfter(mockNotification, 3000);

      expectObservable(dismiss$).toBe('3000ms (a|)', { a: undefined });
    });
  });
});
```

---

## Scheduler Decision Matrix

```
Which scheduler should I use?

Is the work recursive or risk stack overflow?
  → queueScheduler

Do I need to defer until after the current synchronous block
  but before the next render/macrotask?
  → asapScheduler (microtask)

Do I need setTimeout / setInterval behavior that's testable?
  → asyncScheduler (default for timer-based operators)

Do I need to throttle DOM updates to animation frame rate?
  → animationFrameScheduler

Am I writing a custom operator and want the caller to control scheduling?
  → accept SchedulerLike parameter, default to asyncScheduler

Do I need a scheduler only for tests with virtual time?
  → TestScheduler (rxjs/testing) — not for production
```

---

## Common Pitfalls

### Using `asyncScheduler` for DOM Updates (Missing Frames)

```typescript
// ❌ asyncScheduler defers to setTimeout(0) — can fire multiple times per frame:
resize$.pipe(observeOn(asyncScheduler)).subscribe(updateLayout);
// 10 resize events → 10 separate setTimeout(0) → 10 layout recalculations

// ✅ animationFrameScheduler coalesces to one update per frame:
resize$.pipe(observeOn(animationFrameScheduler)).subscribe(updateLayout);
// 10 resize events → 1 rAF callback → 1 layout recalculation
```

### `observeOn` vs `subscribeOn` Confusion

```typescript
// observeOn — affects where EMISSIONS are delivered (next/error/complete):
cold$.pipe(observeOn(asyncScheduler)).subscribe(val => {
  // val is delivered in setTimeout context
});

// subscribeOn — affects where SUBSCRIPTION happens (the setup code):
cold$.pipe(subscribeOn(asyncScheduler)).subscribe(val => {
  // subscription setup (source creation) runs in setTimeout context
  // val delivery is still synchronous
});

// Most common need is observeOn (control where values are processed).
// subscribeOn is rarely needed — only when the source itself has side effects
// at subscription time that must be deferred.
```

---

**Cognitive Load**: 4/5 | **Usage Frequency**: 2/5 | **Composability**: 3/5
**Key insight**: In day-to-day RxJS work, schedulers are invisible — `timer`, `interval`, and `debounceTime` use `asyncScheduler` automatically. Schedulers become explicit only in three situations: Angular zone performance optimization (escape to `animationFrameScheduler`), making time-dependent code testable (inject `SchedulerLike`), and debugging recursive Observables that could stack-overflow (reach for `queueScheduler`).
