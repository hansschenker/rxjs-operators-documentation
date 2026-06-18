# Performance Patterns

Common performance problems in RxJS applications and how to fix them.

---

## Problem 1: Redundant Subscriptions / Duplicate Work

### Symptom
Multiple subscribers to a cold Observable each trigger independent executions (HTTP requests, timers, computations).

### Fix: `shareReplay(1)`

```typescript
// ❌ TWO HTTP REQUESTS
const user$ = this.http.get<User>('/api/me');
combineLatest([
  user$.pipe(map(u => u.name)),     // request 1
  user$.pipe(map(u => u.avatar))    // request 2
]).subscribe(([name, avatar]) => render(name, avatar));

// ✅ ONE HTTP REQUEST
const user$ = this.http.get<User>('/api/me').pipe(shareReplay(1));
combineLatest([
  user$.pipe(map(u => u.name)),     // shared
  user$.pipe(map(u => u.avatar))    // shared
]).subscribe(([name, avatar]) => render(name, avatar));
```

**Rule of thumb**: Any cold Observable referenced in two or more places should have `shareReplay(1)`.

---

## Problem 2: Recomputing Derived State on Every Emission

### Symptom
Expensive transformation runs more times than necessary — once per subscriber rather than once per source emission.

### Fix: Share the derived Observable

```typescript
// ❌ expensiveTransform runs N times per source emission (once per subscriber)
const source$ = interval(100).pipe(shareReplay(1));
const derived$ = source$.pipe(map(v => expensiveTransform(v))); // NOT shared

derived$.subscribe(renderChart);
derived$.subscribe(updateTable);
derived$.subscribe(logToServer); // 3× expensiveTransform per tick

// ✅ expensiveTransform runs ONCE per source emission
const source$  = interval(100).pipe(shareReplay(1));
const derived$ = source$.pipe(
  map(v => expensiveTransform(v)),
  shareReplay(1) // share the derived value too
);

derived$.subscribe(renderChart);  // all three share one computation
derived$.subscribe(updateTable);
derived$.subscribe(logToServer);
```

---

## Problem 3: Unthrottled High-Frequency Events

### Symptom
`mousemove`, `scroll`, `resize`, `input` fire dozens of times per second — too many for downstream work.

### Fix: Match the throttle strategy to the use case

```typescript
// Search input — wait for pause in typing:
searchInput$.pipe(debounceTime(300)).subscribe(search);

// Scroll handler — sample at end of activity burst:
fromEvent(window, 'scroll').pipe(auditTime(16)).subscribe(updateUI);

// Resize — wait until resize stops:
fromEvent(window, 'resize').pipe(debounceTime(150)).subscribe(recalculate);

// DOM updates — sync to animation frame:
stateChanges$.pipe(observeOn(animationFrameScheduler)).subscribe(renderDOM);

// Game/animation loop — use animationFrames() directly:
animationFrames().pipe(
  map(({ elapsed }) => elapsed / DURATION),
  takeWhile(p => p < 1, true)
).subscribe(updateAnimation);
```

---

## Problem 4: Blocking the Main Thread with Large Synchronous Arrays

### Symptom
Processing a large array synchronously blocks the browser/Node event loop, causing UI freezes.

### Fix: `scheduled(array, asyncScheduler)`

```typescript
import { scheduled } from 'rxjs';
import { asyncScheduler } from 'rxjs';
import { mergeMap } from 'rxjs/operators';

// ❌ BLOCKING — processes all 10,000 items synchronously
from(largeArray).pipe(
  mergeMap(item => processItem(item))
).subscribe(updateUI);

// ✅ NON-BLOCKING — yields to the event loop between batches
scheduled(largeArray, asyncScheduler).pipe(
  mergeMap(item => processItem(item))
).subscribe(updateUI);
```

---

## Problem 5: Unnecessary Inner Observable Creation

### Symptom
`switchMap` / `mergeMap` creates a new inner Observable on every source emission, even when the input hasn't changed.

### Fix: `distinctUntilChanged` before flattening

```typescript
// ❌ Creates new HTTP request on EVERY form change, even unchanged fields
formValues$.pipe(
  switchMap(values => this.api.validate(values))
).subscribe(showErrors);

// ✅ Only re-validates when values actually change
formValues$.pipe(
  distinctUntilChanged((a, b) => JSON.stringify(a) === JSON.stringify(b)),
  switchMap(values => this.api.validate(values))
).subscribe(showErrors);

// Even better — debounce first, then dedupe:
formValues$.pipe(
  debounceTime(300),
  distinctUntilChanged((a, b) => JSON.stringify(a) === JSON.stringify(b)),
  switchMap(values => this.api.validate(values))
).subscribe(showErrors);
```

---

## Problem 6: `combineLatest` Thundering Herd

### Symptom
Multiple source Observables update simultaneously (e.g., during initialization), causing `combineLatest` to emit multiple times in quick succession.

### Fix: `debounceTime(0)` or `auditTime(0)` to coalesce synchronous emissions

```typescript
// ❌ Emits 3 times during initialization as each source emits
combineLatest([a$, b$, c$]).subscribe(render);

// ✅ Coalesces rapid synchronous emissions into one
combineLatest([a$, b$, c$]).pipe(
  debounceTime(0) // waits for the current synchronous batch to settle
).subscribe(render);

// Even more precise — coalesce within the same frame:
combineLatest([a$, b$, c$]).pipe(
  auditTime(0, animationFrameScheduler)
).subscribe(render);
```

---

## Problem 7: Memory Leaks from Unmanaged Subscriptions

See the full [Subscription Management Guide](./subscription-management-guide) for all patterns. In brief:

```typescript
// ❌ Subscription lives forever
interval(1000).subscribe(v => this.tick(v));

// ✅ Always clean up hot/infinite streams
interval(1000).pipe(
  takeUntil(this.destroy$)
).subscribe(v => this.tick(v));
```

---

## Problem 8: `shareReplay` Without `refCount` — Permanent Subscriptions

### Symptom
A `shareReplay({ bufferSize: 1, refCount: false })` Observable keeps the source subscribed even when all consumers unsubscribe — a WebSocket stays open, a timer keeps ticking.

### Fix: Use `shareReplay(1)` (which defaults to `refCount: true` in RxJS 7)

```typescript
// ❌ SOURCE NEVER UNSUBSCRIBED — WebSocket stays open forever
const messages$ = webSocket('wss://api').pipe(
  shareReplay({ bufferSize: 1, refCount: false })
);

// ✅ Disconnects when all subscribers leave
const messages$ = webSocket('wss://api').pipe(
  shareReplay(1) // refCount: true by default in RxJS 7
);

// Only use refCount: false for config/data that should survive zero-subscriber periods:
const appConfig$ = loadConfig().pipe(
  shareReplay({ bufferSize: 1, refCount: false }) // intentional permanent cache
);
```

---

## Performance Checklist

When reviewing an RxJS pipeline for performance:

- [ ] Cold Observable used in 2+ places → add `shareReplay(1)`
- [ ] Expensive transformation before multiple subscribers → share the derived stream
- [ ] High-frequency DOM events → add `debounceTime`, `auditTime`, or `observeOn(animationFrameScheduler)`
- [ ] Large synchronous array → use `scheduled(arr, asyncScheduler)`
- [ ] `switchMap`/`mergeMap` on a stream that repeats values → add `distinctUntilChanged` before
- [ ] `combineLatest` with sources that burst simultaneously → add `debounceTime(0)` or `auditTime(0)`
- [ ] All `interval`/`fromEvent`/`webSocket` subscriptions have a terminator (`takeUntil`, `take`, etc.)
- [ ] `shareReplay` on live streams uses `refCount: true` (the default)
