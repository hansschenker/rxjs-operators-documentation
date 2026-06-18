# observeOn / subscribeOn — Advanced Patterns

For fundamentals see the core [observeOn / subscribeOn](./observeOn-subscribeOn) doc. This page covers scheduler selection, Angular zone compatibility, CPU-heavy work offloading, and animation scheduling.

---

## The Two Jobs: Where Work Starts vs Where Values Arrive

```typescript
import { observeOn, subscribeOn } from 'rxjs/operators';
import { asyncScheduler, animationFrameScheduler, queueScheduler } from 'rxjs';

// subscribeOn — controls WHERE the subscription (setup work) runs:
source$.pipe(
  subscribeOn(asyncScheduler)  // subscription deferred to macrotask
)

// observeOn — controls WHERE each emitted value is delivered to the next operator:
source$.pipe(
  observeOn(asyncScheduler)    // each next() call deferred to macrotask
)
```

In most applications you only need `observeOn`. `subscribeOn` is for the rare case where the subscription itself is expensive (e.g., synchronous source that processes on subscribe).

---

## The Four Built-In Schedulers

| Scheduler | Mechanism | Use Case |
|---|---|---|
| `queueScheduler` | Synchronous trampoline | Recursive Observables, prevent stack overflow |
| `asapScheduler` | Microtask (Promise) | Defer past current call stack, before I/O |
| `asyncScheduler` | `setTimeout(fn, 0)` | Break up sync work, yield to event loop |
| `animationFrameScheduler` | `requestAnimationFrame` | DOM animation, smooth visual updates |

---

## Pattern 1: Smooth DOM Animation with `animationFrameScheduler`

```typescript
import { interval, animationFrameScheduler } from 'rxjs';
import { observeOn, map, takeWhile } from 'rxjs/operators';

// Wrong — uses setInterval timing, may stutter:
interval(16).pipe(
  map(i => i / 60),
  takeWhile(t => t <= 1)
).subscribe(t => updateProgress(t));

// Right — syncs with browser repaint cycle:
interval(0, animationFrameScheduler).pipe(
  map(() => performance.now()),
  scan(([start], now) => [start || now, now], [0, 0] as [number, number]),
  map(([start, now]) => Math.min((now - start) / 1000, 1)), // 0→1 over 1 second
  takeWhile(t => t < 1, true)
).subscribe(t => {
  element.style.opacity = String(easeInOut(t));
  element.style.transform = `translateX(${t * 100}px)`;
});
```

---

## Pattern 2: Breaking Up Synchronous CPU Work

Long synchronous computations block the event loop. Use `asyncScheduler` to yield:

```typescript
import { from, asyncScheduler } from 'rxjs';
import { observeOn, mergeMap } from 'rxjs/operators';

// Process large array without blocking UI:
from(largeDataArray).pipe(
  observeOn(asyncScheduler),          // each item processed in its own macrotask
  mergeMap(item => processItem(item)) // processing spreads across event loop turns
).subscribe({
  next:     result => updateProgressUI(result),
  complete: () => showDoneMessage()
});

// More controlled — batch in chunks:
from(chunk(largeDataArray, 100)).pipe(
  observeOn(asyncScheduler),
  mergeMap(batch => from(batch).pipe(
    map(processItem)
  ))
).subscribe(renderBatch);
```

---

## Pattern 3: Angular Zone Compatibility

Angular's change detection runs inside `NgZone`. Observables that emit outside the zone (WebSocket, Web Worker, timer from third-party lib) don't trigger change detection:

```typescript
import { Injectable, NgZone } from '@angular/core';
import { observeOn } from 'rxjs/operators';
import { Observable } from 'rxjs';
import { Scheduler } from 'rxjs';

@Injectable({ providedIn: 'root' })
export class ZoneSchedulerService {
  constructor(private zone: NgZone) {}

  // Scheduler that runs inside Angular zone:
  get insideZone(): Scheduler {
    return {
      schedule: (work, delay = 0, state?: unknown) => {
        return new Subscription(this.zone.run(() => work(state as never)));
      }
    } as unknown as Scheduler;
  }
}

// Usage — bring WebSocket emissions back into Angular zone:
@Injectable()
export class RealtimeService {
  constructor(private ws: WebSocketService, private zones: ZoneSchedulerService) {}

  readonly updates$ = this.ws.messages$.pipe(
    observeOn(this.zones.insideZone)  // ensures change detection fires
  );
}

// Simpler alternative — NgZone.run() in subscribe:
externalStream$.subscribe(value =>
  this.ngZone.run(() => this.data.set(value))
);
```

---

## Pattern 4: Running Outside Angular Zone for Performance

Conversely, run high-frequency events (scroll, mousemove) outside the zone to prevent constant change detection:

```typescript
import { Component, NgZone, OnInit } from '@angular/core';
import { fromEvent } from 'rxjs';
import { throttleTime, map } from 'rxjs/operators';

@Component({ ... })
export class MapComponent implements OnInit {
  constructor(private ngZone: NgZone) {}

  ngOnInit() {
    this.ngZone.runOutsideAngular(() => {
      // mousemove at 60fps outside zone — no CD overhead:
      fromEvent<MouseEvent>(document, 'mousemove').pipe(
        throttleTime(16),
        map(e => ({ x: e.clientX, y: e.clientY }))
      ).subscribe(pos => {
        // Only update DOM directly — skip Angular binding:
        this.cursor.nativeElement.style.transform =
          `translate(${pos.x}px, ${pos.y}px)`;

        // Bring back into zone only for meaningful state changes:
        if (this.isOverTarget(pos)) {
          this.ngZone.run(() => this.hovering.set(true));
        }
      });
    });
  }
}
```

---

## Pattern 5: `queueScheduler` for Recursive Streams

`queueScheduler` prevents stack overflow in recursive Observable patterns:

```typescript
import { queueScheduler, of } from 'rxjs';
import { observeOn, expand, take } from 'rxjs/operators';

// Without queueScheduler: deep recursion can overflow stack:
of(1).pipe(
  expand(n => of(n + 1)),  // synchronous recursion — may overflow
  take(10000)
)

// With queueScheduler: trampolines to prevent overflow:
of(1).pipe(
  observeOn(queueScheduler),
  expand(n => of(n + 1).pipe(observeOn(queueScheduler))),
  take(10000)
)
```

---

## Pattern 6: Web Worker Communication

Route heavy computation to a Web Worker, bring results back to main thread:

```typescript
import { fromEvent, Subject } from 'rxjs';
import { observeOn, asyncScheduler } from 'rxjs/operators';

class WorkerService {
  private worker = new Worker(new URL('./compute.worker', import.meta.url));
  private results$ = fromEvent<MessageEvent>(this.worker, 'message').pipe(
    map(e => e.data)
  );

  compute(data: unknown): Observable<unknown> {
    return new Observable(observer => {
      this.worker.postMessage(data);
      const sub = this.results$.pipe(take(1)).subscribe(observer);
      return () => sub.unsubscribe();
    });
  }
}

// In component — worker result arrives on message thread, observeOn brings to main:
this.workerService.compute(heavyData).pipe(
  observeOn(asyncScheduler)  // ensure we're on main thread for DOM updates
).subscribe(renderResult);
```

---

## `observeOn` vs `subscribeOn` vs Scheduler in Creation Operators

```typescript
// Creation operator scheduler param (most efficient — avoids wrapping):
interval(1000, asyncScheduler)           // emission on asyncScheduler directly
timer(0, 1000, animationFrameScheduler)  // ticks on rAF

// observeOn — wraps an existing stream, changes delivery scheduler:
existingStream$.pipe(observeOn(asyncScheduler))
// Adds overhead: every emission is wrapped in a scheduled action

// subscribeOn — delays subscription itself:
expensiveSetupStream$.pipe(subscribeOn(asyncScheduler))
// The subscribe() call runs on asyncScheduler instead of immediately
```

Prefer scheduler params in creation operators when possible — they're cheaper than wrapping.

---

## Common Pitfalls

### `observeOn` Does Not Make a Stream Async End-to-End

```typescript
// ❌ Misconception: observeOn makes all processing async:
heavyComputation$.pipe(
  observeOn(asyncScheduler),
  map(expensiveTransform)    // still runs synchronously within each scheduled action!
)

// ✅ Each individual emission is deferred, but the map runs synchronously
// when that emission is delivered. For truly async map, use mergeMap + defer:
heavyComputation$.pipe(
  mergeMap(item =>
    defer(() => of(expensiveTransform(item))).pipe(
      subscribeOn(asyncScheduler)  // defer computation to async
    )
  )
)
```

### `animationFrameScheduler` Outside the Browser

```typescript
// ❌ animationFrameScheduler in Node.js or SSR — throws or silently breaks:
stream$.pipe(observeOn(animationFrameScheduler))

// ✅ Guard with platform check (Angular):
import { isPlatformBrowser } from '@angular/common';

const scheduler = isPlatformBrowser(this.platformId)
  ? animationFrameScheduler
  : asyncScheduler;

stream$.pipe(observeOn(scheduler))
```

---

**Cognitive Load**: 3/5 | **Usage Frequency**: 2/5 | **Composability**: 3/5
**Key rule**: Use `animationFrameScheduler` for DOM animations, `asyncScheduler` to yield the event loop during heavy processing, and `observeOn` at the Angular zone boundary to restore change detection for out-of-zone sources. `subscribeOn` is rarely needed in practice.
