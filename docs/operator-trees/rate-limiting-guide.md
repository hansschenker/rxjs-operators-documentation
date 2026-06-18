# Rate-Limiting Selection Guide

All eight rate-limiting operators suppress emissions from a source — they differ in **which** emission survives and **when** the window resets.

---

## The Eight Operators at a Glance

| Operator | Window trigger | Which emission survives | Leading | Trailing |
|---|---|---|---|---|
| `throttleTime(ms)` | Fixed timer | First in window | Yes | Optional |
| `debounceTime(ms)` | Silence timer | Last after quiet period | No | Yes |
| `auditTime(ms)` | Fixed timer | Last in window | No | Yes |
| `sampleTime(ms)` | Fixed clock tick | Last at tick moment | No | Yes |
| `throttle(fn)` | Observable signal | First in window | Yes | Optional |
| `debounce(fn)` | Observable signal | Last after signal | No | Yes |
| `audit(fn)` | Observable signal | Last at signal emission | No | Yes |
| `sample(signal$)` | Observable signal | Last at signal emission | No | Yes |

---

## Decision Guide

```
How should the window be triggered?
│
├── Fixed timer (simpler)
│   │
│   ├── Want FIRST emission in window?    → throttleTime(ms)
│   │                                       "accept one request, ignore rest for N ms"
│   │
│   ├── Want LAST after quiet period?     → debounceTime(ms)
│   │                                       "wait until source is silent for N ms"
│   │
│   ├── Want LAST emission in window?     → auditTime(ms)
│   │   (window starts on first emit)       "sample at end of activity window"
│   │
│   └── Want LAST at clock tick?          → sampleTime(ms)
│       (independent of source activity)    "snapshot on a fixed clock"
│
└── Dynamic duration per value
    │
    ├── Want FIRST in window?             → throttle(v => durationFn(v))
    ├── Want LAST after quiet?            → debounce(v => durationFn(v))
    ├── Want LAST at signal?              → audit(v => durationFn(v))
    └── Separate signal Observable?       → sample(signal$)
```

---

## Visual Comparison

Each diagram shows the same source — four rapid events followed by silence.

```
Source:  --a-b-c-d--------e-f-g-h---------|
         ^burst           ^burst

throttleTime(50ms):   --a-----------e---------|
  "First wins, rest silenced for 50ms"

debounceTime(50ms):   --------d---------h-----|
  "Only emits after 50ms of silence"

auditTime(50ms):      ------d-----------h-----|
  "Last in window; window starts on first emit"

sampleTime(50ms):     ------c--------f-----h--|
  "Snapshot every 50ms regardless of activity"
```

---

## Operator Deep Dives

### `throttleTime(ms)` — Rate Gate

```
Source:  --a-b-c---d-e-f---
throttleTime(100ms):
         --a-------d-------
```

Opens on first emission, silences for `ms`, then re-opens. "Accept one, ignore the rest for N ms."

```typescript
import { throttleTime } from 'rxjs/operators';

// Button: prevent double-submit, accept first click
submitBtn$.pipe(
  throttleTime(500)
).subscribe(handleSubmit);

// Leading + trailing (both first and last):
clicks$.pipe(
  throttleTime(300, asyncScheduler, { leading: true, trailing: true })
).subscribe(handler);
```

**Use when**: Prevent repeat actions within a window. The first emission matters most.

---

### `debounceTime(ms)` — Silence Gate

```
Source:  --a-b-c------d-e-------
debounceTime(100ms):
         ---------c---------e--
```

Resets the timer on every emission. Only fires after the source is quiet for `ms`.

```typescript
import { debounceTime } from 'rxjs/operators';

// Search: wait until user stops typing
searchInput$.pipe(
  debounceTime(300),
  distinctUntilChanged(),
  switchMap(query => search(query))
).subscribe(renderResults);

// Resize: wait until resize ends
fromEvent(window, 'resize').pipe(
  debounceTime(150)
).subscribe(recalculateLayout);
```

**Use when**: The final state matters; intermediate values should be dropped. Classic for search inputs and resize.

---

### `auditTime(ms)` — Activity Window Sampler

```
Source:  --a-b-c---d-e-f---
auditTime(100ms):
         -----c-------f----
```

On first emission, opens a window for `ms` — fires the last emission when the window closes.

```typescript
import { auditTime } from 'rxjs/operators';

// Scroll: use last position after 16ms activity window
fromEvent(window, 'scroll').pipe(
  auditTime(16) // ~1 frame
).subscribe(updateUI);

// Mouse moves: sample at end of movement burst
mousemove$.pipe(auditTime(50)).subscribe(renderCursor);
```

**Use when**: Activity bursts occur and you want the last value at the end of each burst. Similar to debounceTime but timer doesn't reset — fires at a fixed interval from the first event.

---

### `sampleTime(ms)` — Fixed Clock

```
Source:  --a-b-c---d-e-f---
sampleTime(100ms):
         ----b-------e-----
  (ticks at 100ms, 200ms, 300ms... regardless of source)
```

Emits whatever the latest value was at each tick. Emits nothing if source was silent.

```typescript
import { sampleTime } from 'rxjs/operators';

// Game: sample player position every frame
playerPosition$.pipe(
  sampleTime(16) // ~60fps
).subscribe(renderPlayer);

// Analytics: record current value every second
sensorData$.pipe(sampleTime(1000)).subscribe(logToServer);
```

**Use when**: You need a regular clock-driven snapshot independent of source cadence.

---

### `throttle(fn)` — Dynamic Throttle

```typescript
import { throttle } from 'rxjs/operators';
import { interval } from 'rxjs';

// Dynamic window: longer window for expensive items
requests$.pipe(
  throttle(req => interval(req.costMs))
).subscribe(send);

// Use auditTime duration as a dynamic window
events$.pipe(
  throttle(event => timer(event.priority > 5 ? 1000 : 100))
).subscribe(handle);
```

**Use when**: The throttle duration depends on the emitted value.

---

### `debounce(fn)` — Dynamic Debounce

```typescript
import { debounce } from 'rxjs/operators';
import { timer } from 'rxjs';

// Adaptive debounce: longer for complex queries
searchInput$.pipe(
  debounce(query => timer(query.length > 3 ? 200 : 500))
).subscribe(search);
```

**Use when**: The silence period should depend on the value — e.g., short queries need more debounce time.

---

### `audit(fn)` / `sample(signal$)` — Observable-Driven Sampling

```typescript
import { audit, sample } from 'rxjs/operators';

// audit: window per-value, fires at window close
mouseMove$.pipe(
  audit(() => animationFrames().pipe(take(1)))
).subscribe(render); // updates on rAF, window per move

// sample: any external signal triggers a snapshot
const tick$ = interval(1000);
stockPrice$.pipe(
  sample(tick$)
).subscribe(displayPrice); // display price every second
```

`sample` and `audit` are nearly identical — `sample` takes an external signal Observable, `audit` takes a duration factory per value.

---

## Common Pitfalls

### Using `debounceTime` When You Want `throttleTime`

```typescript
// ❌ WRONG — fast typist may never trigger search (debounce always resets)
searchInput$.pipe(
  debounceTime(300),
  switchMap(search)
).subscribe(render);
// If user types continuously for 2s, no search fires for 2s

// ✅ CORRECT — depends on intent:
// "Don't search until they pause" → debounceTime (above is right if that's the goal)
// "Search at most once per 300ms" → throttleTime with trailing: true
searchInput$.pipe(
  throttleTime(300, asyncScheduler, { leading: false, trailing: true }),
  switchMap(search)
).subscribe(render);
// WHY: throttleTime fires at most once per 300ms; debounceTime fires only AFTER 300ms quiet
```

### Forgetting `auditTime` vs `debounceTime` Difference

```typescript
// auditTime(100ms): fires ALWAYS 100ms after first burst event
// debounceTime(100ms): fires ONLY if no event for 100ms

// For rapid 10ms events lasting 500ms:
// auditTime(100): fires at 100ms, 200ms, 300ms, 400ms, 500ms (5 times)
// debounceTime(100): fires once at 600ms (100ms after last event)
```

### Using `sampleTime` on Sparse Streams

```typescript
// ❌ sampleTime on slow source → many missed ticks = no output
rareEvent$.pipe(sampleTime(100)).subscribe(); // emits nothing most ticks

// ✅ Use debounceTime or audit for activity-driven sampling
rareEvent$.pipe(debounceTime(100)).subscribe(); // fires after each event settles
```

---

## Quick Selection Matrix

| Use Case | Best Choice |
|---|---|
| Search input / form validation | `debounceTime(300)` |
| Button / form submit protection | `throttleTime(500)` |
| Scroll / resize event handling | `debounceTime(150)` or `auditTime(16)` |
| Game loop / animation sampling | `sampleTime(16)` or `animationFrames()` |
| API rate limiting | `throttleTime(1000)` |
| Sensor / telemetry periodic log | `sampleTime(1000)` |
| Dynamic window by value | `throttle(fn)` / `debounce(fn)` |
| External signal snapshot | `sample(signal$)` |
