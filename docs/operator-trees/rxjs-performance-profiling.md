# RxJS Performance Profiling

Diagnosing slow RxJS pipelines — memory leaks from unsubscribed streams, excessive emissions, synchronous blocking, scheduler misuse, and production-ready instrumentation.

---

## The Performance Problem Space

RxJS performance issues fall into four categories:

```
1. Subscription leaks    — Observable subscribed but never unsubscribed → memory/CPU growth
2. Emission storms       — operator emits too frequently → downstream work overwhelmed
3. Synchronous blocking  — large synchronous pipelines block the event loop
4. Redundant re-execution — same computation repeated on every emission unnecessarily
```

Understanding which category applies determines the fix.

---

## Category 1: Subscription Leaks

The most common production issue — an Observable subscribed in a component or service is never unsubscribed, running indefinitely after the component destroys.

### Detection

```typescript
import { Observable, Subscriber } from 'rxjs';

// Instrument an Observable to count active subscriptions:
function trackSubscriptions<T>(
  source$: Observable<T>,
  label: string
): Observable<T> {
  let count = 0;
  return new Observable<T>(subscriber => {
    count++;
    console.log(`[${label}] subscribed — active: ${count}`);

    const sub = source$.subscribe({
      next:     v  => subscriber.next(v),
      error:    e  => subscriber.error(e),
      complete: () => subscriber.complete()
    });

    return () => {
      count--;
      console.log(`[${label}] unsubscribed — active: ${count}`);
      sub.unsubscribe();
    };
  });
}

// Usage — wrap suspicious stream:
const userData$ = trackSubscriptions(
  this.userService.getUser$(),
  'UserService.getUser$'
);
```

### Angular-Specific Detection

```typescript
import { Component, OnInit, OnDestroy } from '@angular/core';
import { Subscription } from 'rxjs';

// Manual subscription tracking (old pattern — shows the problem):
@Component({ selector: 'app-broken', template: '' })
class BrokenComponent implements OnInit {
  ngOnInit() {
    // ❌ No unsubscribe — runs forever after component destroys:
    this.intervalService.tick$().subscribe(tick => this.update(tick));
  }
}

// ✅ Modern Angular fix — takeUntilDestroyed():
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

@Component({ standalone: true })
class FixedComponent {
  constructor() {
    this.intervalService.tick$().pipe(
      takeUntilDestroyed()
    ).subscribe(tick => this.update(tick));
  }
}
```

### Non-Angular Fix — DestroyRef pattern

```typescript
import { Subject, takeUntil } from 'rxjs';

class SomeService {
  private readonly destroy$ = new Subject<void>();

  init() {
    pollingStream$.pipe(
      takeUntil(this.destroy$)
    ).subscribe(this.handler.bind(this));
  }

  destroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
```

---

## Category 2: Emission Storms

### Finding Excessive Emission Rates

```typescript
import { tap, pairwise, map, filter } from 'rxjs/operators';

// Measure emission rate — flag if >N per second:
function rateLimit$<T>(source$: Observable<T>, label: string, maxPerSec = 60): Observable<T> {
  const window: number[] = [];

  return source$.pipe(
    tap(() => {
      const now = Date.now();
      window.push(now);
      const cutoff = now - 1000;
      while (window[0] < cutoff) window.shift();

      if (window.length > maxPerSec) {
        console.warn(`[${label}] emission rate: ${window.length}/sec — throttling recommended`);
      }
    })
  );
}

// Timing between emissions:
function measureIntervals<T>(source$: Observable<T>, label: string): Observable<T> {
  let lastEmit = Date.now();
  return source$.pipe(
    tap(() => {
      const now = Date.now();
      const interval = now - lastEmit;
      if (interval < 16) { // faster than one animation frame
        console.warn(`[${label}] ${interval}ms between emissions — may need debounce`);
      }
      lastEmit = now;
    })
  );
}
```

### Fixing Emission Storms

```typescript
// ❌ Subscribing to mousemove directly — fires 100+ times/sec:
fromEvent(document, 'mousemove').pipe(
  map(e => ({ x: (e as MouseEvent).clientX, y: (e as MouseEvent).clientY }))
).subscribe(updateCursor);

// ✅ Sample at animation frame rate:
fromEvent(document, 'mousemove').pipe(
  sampleTime(0),                         // or auditTime(0, animationFrameScheduler)
  map(e => ({ x: (e as MouseEvent).clientX, y: (e as MouseEvent).clientY })),
  distinctUntilChanged((a, b) => a.x === b.x && a.y === b.y)
).subscribe(updateCursor);

// ❌ Form valueChanges triggers HTTP on every keystroke:
this.searchForm.valueChanges.pipe(
  switchMap(v => this.api.search$(v.query))
).subscribe(results => this.results = results);

// ✅ Debounce + deduplicate:
this.searchForm.get('query')!.valueChanges.pipe(
  debounceTime(300),
  distinctUntilChanged(),
  filter(q => q.length >= 2),
  switchMap(q => this.api.search$(q))
).subscribe(results => this.results = results);
```

---

## Category 3: Synchronous Blocking

`queueScheduler` and unscheduled `of()`/`from()` emit synchronously — a large pipeline can block the event loop.

### Detecting Synchronous Pipeline Length

```typescript
import { queueScheduler, observeOn, subscribeOn } from 'rxjs';

// Measure synchronous execution time:
function measureSync<T>(label: string) {
  return (source$: Observable<T>) => new Observable<T>(subscriber => {
    const start = performance.now();
    let emitCount = 0;

    return source$.subscribe({
      next: v => {
        emitCount++;
        const elapsed = performance.now() - start;
        if (elapsed > 16 && emitCount % 1000 === 0) {
          console.warn(`[${label}] ${emitCount} sync emissions in ${elapsed.toFixed(1)}ms`);
        }
        subscriber.next(v);
      },
      error:    e  => subscriber.error(e),
      complete: () => {
        const total = performance.now() - start;
        console.log(`[${label}] completed: ${emitCount} emissions in ${total.toFixed(1)}ms`);
        subscriber.complete();
      }
    });
  });
}

// ❌ Processing large arrays synchronously blocks the event loop:
from(largeArray).pipe(
  map(expensiveTransform),
  filter(isValid),
  toArray()
).subscribe(results => render(results));

// ✅ Break into async chunks with observeOn:
from(largeArray).pipe(
  observeOn(asapScheduler),    // schedule each emission as a microtask
  map(expensiveTransform),
  filter(isValid),
  toArray()
).subscribe(results => render(results));

// ✅ For very large arrays, chunk and yield control:
from(chunk(largeArray, 100)).pipe(   // process 100 at a time
  concatMap(batch =>
    of(batch).pipe(
      observeOn(asyncScheduler),     // yield to event loop between batches
      map(b => b.map(expensiveTransform).filter(isValid))
    )
  ),
  map(batch => batch.flat())
).subscribe(results => render(results));
```

---

## Category 4: Redundant Re-Execution

### Finding Unnecessary Recomputation

```typescript
// Instrument to count how often an expensive operation runs:
function countExecutions<T>(label: string, source$: Observable<T>): Observable<T> {
  let count = 0;
  return source$.pipe(
    tap(() => {
      count++;
      if (count > 1) console.warn(`[${label}] executed ${count} times`);
    })
  );
}

// ❌ HTTP request fires every time any subscriber subscribes:
@Injectable()
class DataService {
  // Cold Observable — new HTTP request per subscriber:
  readonly config$ = this.http.get<Config>('/api/config');
}

// Multiple components subscribing = multiple HTTP requests:
// ComponentA: dataService.config$.subscribe(...)   → GET /api/config
// ComponentB: dataService.config$.subscribe(...)   → GET /api/config (again!)
// ComponentC: dataService.config$.subscribe(...)   → GET /api/config (again!)

// ✅ Share with shareReplay(1):
@Injectable({ providedIn: 'root' })
class DataService {
  // Hot Observable — one HTTP request, replayed to all subscribers:
  readonly config$ = this.http.get<Config>('/api/config').pipe(
    shareReplay(1)  // cache last value, replay to new subscribers
  );
}
```

### `shareReplay` vs `share` — When to Use Which

```typescript
// share() — multicasts; new subscribers get nothing if they arrive after completion:
const click$ = fromEvent(button, 'click').pipe(share());

// shareReplay(1) — multicasts + replays last value to late subscribers:
// Use for: HTTP responses, config, user data — "current state" scenarios
const user$ = this.http.get<User>('/api/me').pipe(shareReplay(1));

// shareReplay({ bufferSize: 1, refCount: true }) — cleans up when no subscribers:
// Use for: streams that should stop when no one is listening
const live$ = webSocketStream$.pipe(
  shareReplay({ bufferSize: 1, refCount: true })
);
// When last subscriber unsubscribes, WebSocket closes; when first re-subscribes, reopens

// shareReplay({ bufferSize: 1, refCount: false }) — stays alive forever (default in older RxJS):
// Use for: app-lifetime singletons (config, auth state)
const authState$ = authService.state$.pipe(
  shareReplay({ bufferSize: 1, refCount: false })
);
```

---

## Profiling with Custom Tap Operators

```typescript
import { tap, timestamp, pairwise, map } from 'rxjs/operators';

// Log value + timing to console:
function debug<T>(label: string): MonoTypeOperatorFunction<T> {
  return tap({
    next:       v  => console.log(`[${label}] next:`, v),
    error:      e  => console.error(`[${label}] error:`, e),
    complete:   () => console.log(`[${label}] complete`),
    subscribe:  () => console.log(`[${label}] subscribed`),
    unsubscribe:() => console.log(`[${label}] unsubscribed`)
  });
}

// Measure time between emissions:
function measureLatency<T>(label: string): MonoTypeOperatorFunction<T> {
  return source$ => source$.pipe(
    timestamp(),
    pairwise(),
    tap(([prev, curr]) => {
      const latency = curr.timestamp - prev.timestamp;
      if (latency > 100) {
        console.warn(`[${label}] ${latency}ms gap between emissions`);
      }
    }),
    map(([, curr]) => curr.value)
  );
}

// Count emissions per time window (for throughput analysis):
function throughput<T>(label: string, windowMs = 1000): MonoTypeOperatorFunction<T> {
  return source$ => source$.pipe(
    bufferTime(windowMs),
    tap(batch => console.log(`[${label}] ${batch.length} emissions in ${windowMs}ms`)),
    mergeAll()
  );
}

// Production-safe version — only logs in development:
function debugInDev<T>(label: string): MonoTypeOperatorFunction<T> {
  return isDevMode() ? debug(label) : identity;
}
```

---

## Memory Profiling with Chrome DevTools

```typescript
// Step 1: Mark subscription creation with a custom FinalizationRegistry (V8):
const registry = new FinalizationRegistry((label: string) => {
  console.log(`[GC] ${label} was garbage collected`);
});

function trackGC<T>(source$: Observable<T>, label: string): Observable<T> {
  const tracker = {};
  registry.register(tracker, label);

  return source$.pipe(
    finalize(() => {
      // Subscription ended — tracker should be GC'd shortly
      console.log(`[${label}] finalized — eligible for GC`);
    })
  );
}

// Step 2: Chrome DevTools Memory tab workflow:
// 1. Open DevTools → Memory → Take Heap Snapshot (baseline)
// 2. Navigate to the component that uses the Observable
// 3. Navigate away (should destroy the component)
// 4. Take another Heap Snapshot
// 5. Compare snapshots — look for retained RxJS Subscriber objects
// 6. If Subscribers survive navigation, the subscription was not cleaned up
```

---

## Scheduler Performance Guide

```typescript
import { queueScheduler, asapScheduler, asyncScheduler, animationFrameScheduler } from 'rxjs';

// queueScheduler — synchronous, queued (like a microtask queue)
// Use for: operators that recurse (expand, repeat) — prevents stack overflow
// Never use for: anything that should yield to the event loop

// asapScheduler — after current synchronous block, before setTimeout
// Use for: observeOn() when you want microtask-level concurrency
// Schedules like Promise.resolve().then(...)

// asyncScheduler — setTimeout(fn, 0) equivalent
// Use for: timer(), interval(), or explicitly yielding to event loop
// Overhead: setTimeout latency (~4ms minimum in browsers)

// animationFrameScheduler — requestAnimationFrame
// Use for: DOM updates, canvas rendering, position-based animations
// Automatically throttles to display refresh rate

// Example — choose the right scheduler for DOM batch updates:
const positions$ = fromEvent(document, 'mousemove').pipe(
  observeOn(animationFrameScheduler), // batch DOM writes per frame
  map(e => ({ x: (e as MouseEvent).clientX, y: (e as MouseEvent).clientY })),
  distinctUntilChanged((a, b) => a.x === b.x && a.y === b.y)
);
```

---

## Production Monitoring — RxJS Error and Latency Tracking

```typescript
import { catchError, timeout, retry } from 'rxjs/operators';

// Global error + latency tracking for Observable pipelines:
function monitored<T>(
  source$:   Observable<T>,
  label:     string,
  timeoutMs  = 5000
): Observable<T> {
  const start = performance.now();

  return source$.pipe(
    timeout(timeoutMs),
    tap({ complete: () => {
      const latency = performance.now() - start;
      analytics.track('observable_complete', { label, latency });
    }}),
    catchError(err => {
      const latency = performance.now() - start;
      const isTimeout = err instanceof TimeoutError;

      analytics.track('observable_error', {
        label,
        latency,
        error: err.message,
        timeout: isTimeout
      });

      return throwError(() => err);
    })
  );
}

// Usage:
this.api.getUser$(id).pipe(
  monitored('UserService.getUser', 3000)
).subscribe(user => this.render(user));
```

---

## Common Pitfalls

### `shareReplay` Without `refCount: true` Leaks WebSocket Connections

```typescript
// ❌ Default shareReplay keeps WebSocket alive even after all subscribers leave:
const liveData$ = webSocketService.connect$().pipe(
  shareReplay(1) // refCount defaults to false — connection NEVER closes
);

// ✅ Use refCount: true so the WebSocket closes when no one is subscribed:
const liveData$ = webSocketService.connect$().pipe(
  shareReplay({ bufferSize: 1, refCount: true })
);
```

### `switchMap` Inside `combineLatest` Triggers Entire Pipeline on Every Change

```typescript
// ❌ Every change to any of the 3 inputs triggers a new HTTP request:
combineLatest([userId$, filters$, page$]).pipe(
  switchMap(([userId, filters, page]) => this.api.getItems$(userId, filters, page))
).subscribe(items => this.render(items));
// userId changes → HTTP request
// filters changes → HTTP request (even if just whitespace trimmed)
// page changes → HTTP request

// ✅ debounce the combined input before switching:
combineLatest([userId$, filters$, page$]).pipe(
  debounceTime(50),                     // absorb simultaneous changes
  distinctUntilChanged(isEqual),        // skip if nothing actually changed
  switchMap(([userId, filters, page]) => this.api.getItems$(userId, filters, page))
).subscribe(items => this.render(items));
```

---

**Key insight**: RxJS performance work is 90% leak prevention (`takeUntilDestroyed`, `shareReplay` with `refCount: true`) and 10% emission control (`debounceTime`, `distinctUntilChanged`, `auditTime`). The simplest profiling tool is a `tap` with `console.log` — count how often an expensive operation executes and compare to how often it should. Reach for Chrome DevTools Heap Snapshots only when you've confirmed a leak exists but can't find it by inspection.
