# auditTime / sampleTime — Advanced Patterns

For fundamentals see the core [auditTime / sampleTime](./auditTime-sampleTime) doc. This page covers animation loop sampling, high-frequency sensor data, adaptive sampling, and the full rate-limiting decision guide.

---

## The Sampling Family Compared

```
Source:   -a-b-c-d-e-f-g-h-|

throttleTime(30): -a---------d--------g-|   (first in window)
debounceTime(30): ---------------h-|        (last after silence)
auditTime(30):    --------c--------f--h-|   (last in window, timer from FIRST)
sampleTime(30):   --------c--------f-----h-| (last in window, fixed clock)
```

**Key distinction**: `auditTime` starts its timer on first emission; `sampleTime` runs on a fixed wall-clock interval regardless of source activity. `auditTime` won't emit during silence; `sampleTime` will emit `undefined`-equivalent (it skips if no value arrived since last sample — see pitfall below).

---

## Pattern 1: 60fps Animation Loop Sampling

Sample a high-frequency input stream at exactly 60fps:

```typescript
import { auditTime } from 'rxjs/operators';
import { animationFrameScheduler } from 'rxjs';

// Mouse position: potentially hundreds of events per second
const mousePos$ = fromEvent<MouseEvent>(document, 'mousemove').pipe(
  map(e => ({ x: e.clientX, y: e.clientY }))
);

// Audit to one sample per animation frame — sync with display refresh:
mousePos$.pipe(
  auditTime(0, animationFrameScheduler), // 0ms = "next animation frame"
  takeUntilDestroyed()
).subscribe(({ x, y }) => {
  updateCursorElement(x, y); // runs at most once per rAF, never faster
});
```

---

## Pattern 2: Sensor Data Down-Sampling

Reduce a 100Hz sensor stream to 10Hz for UI display, keeping 10Hz for processing:

```typescript
import { share, auditTime, sampleTime } from 'rxjs/operators';

const rawSensor$ = fromEvent<DeviceMotionEvent>(window, 'devicemotion').pipe(
  map(e => ({
    x: e.acceleration?.x ?? 0,
    y: e.acceleration?.y ?? 0,
    z: e.acceleration?.z ?? 0,
    ts: Date.now()
  })),
  share() // one subscription to device motion
);

// UI: sample at 10Hz (100ms) — no need for every update
const displaySample$ = rawSensor$.pipe(
  sampleTime(100),
  takeUntilDestroyed()
);

// Analytics: audit at 200ms — last value in each window
const analyticsSample$ = rawSensor$.pipe(
  auditTime(200),
  takeUntilDestroyed()
);

displaySample$.subscribe(updateAccelerometerDisplay);
analyticsSample$.pipe(
  bufferCount(10) // batch 10 samples at a time
).subscribe(sendBatchToAnalytics);
```

---

## Pattern 3: Adaptive Sampling Rate

Adjust sample interval based on system load or data velocity:

```typescript
import { sample } from 'rxjs/operators';
import { BehaviorSubject, interval } from 'rxjs';

class AdaptiveSampler<T> {
  private rate$ = new BehaviorSubject(100); // ms between samples

  sample(source$: Observable<T>): Observable<T> {
    return source$.pipe(
      sample(
        this.rate$.pipe(
          switchMap(ms => interval(ms))
        )
      )
    );
  }

  setRate(ms: number): void {
    this.rate$.next(Math.max(16, ms)); // minimum ~60fps
  }
}

const sampler = new AdaptiveSampler<SensorReading>();

// Throttle sampling when battery low or tab backgrounded:
fromEvent(document, 'visibilitychange').subscribe(() => {
  sampler.setRate(document.hidden ? 1000 : 100);
});

batteryLevel$.subscribe(level => {
  if (level < 0.2) sampler.setRate(500); // save battery
});

sampler.sample(rawSensor$).subscribe(updateUI);
```

---

## Pattern 4: Signal Sampling with `sample` (Trigger-Based)

`sample(notifier$)` emits the last source value whenever the notifier fires — perfect for click-to-read patterns:

```typescript
import { sample, combineLatest } from 'rxjs/operators';

// "Take a reading when user clicks the button":
const latestReading$ = sensor$.pipe(
  sample(readButton$) // emit latest sensor value on each button press
);

// Form autosave: save when user pauses (focus loss or navigation):
const formValue$ = formGroup.valueChanges;
const saveTrigger$ = merge(
  fromEvent(window, 'beforeunload'),
  routerEvents$.pipe(filter(e => e instanceof NavigationStart))
);

formValue$.pipe(
  sample(saveTrigger$)
).subscribe(value => saveForm(value));

// Chart update: refresh chart on a schedule, always showing freshest data:
const chartUpdate$ = interval(5000);
realtimeData$.pipe(
  sample(chartUpdate$)
).subscribe(data => updateChart(data));
```

---

## Pattern 5: `auditTime` for Search Suggest (Better Than `debounceTime`)

`debounceTime` delays on every keystroke; `auditTime` always fires on a fixed schedule after first input:

```typescript
import { auditTime, switchMap } from 'rxjs/operators';

// debounceTime: waits 300ms of silence AFTER last key — adds latency
searchInput$.pipe(
  debounceTime(300),
  switchMap(q => api.suggest(q))
).subscribe(renderSuggestions);

// auditTime: fires 300ms after FIRST keystroke — caps max latency
searchInput$.pipe(
  auditTime(300),
  distinctUntilChanged(),
  switchMap(q => api.suggest(q))
).subscribe(renderSuggestions);

// WHY auditTime is sometimes better:
// User types "react" over 1s at 200ms intervals
// debounceTime(300): fires 1000ms + 300ms = 1.3s after first key
// auditTime(300):    fires 300ms after first key → lower latency
// Use debounceTime when silence matters (user stopped typing)
// Use auditTime when max response time matters (live suggest)
```

---

## Rate-Limiting Full Decision Guide

```
Source emits too frequently — what do I want?

├── Keep FIRST value in each window, drop the rest
│   └── throttleTime(ms) / throttleTime(ms, undefined, { leading: true, trailing: false })

├── Keep LAST value, but only after source goes silent
│   └── debounceTime(ms)
│       └── Good for: search input, resize handler, form autosave

├── Keep LAST value in each window (timer starts on first emission)
│   └── auditTime(ms)
│       └── Good for: mouse drag, animation input, live search with max latency

├── Keep LAST value in fixed-clock windows (independent of source)
│   └── sampleTime(ms) / sample(notifier$)
│       └── Good for: fixed-rate display update, sensor down-sampling
│       └── Emits undefined on empty windows? No — skips empty windows

├── Keep FIRST and/or LAST value (configurable)
│   └── throttleTime(ms, undefined, { leading: true, trailing: true })

└── I want to control the window with an Observable, not a fixed time
    ├── debounce(selector$)  — silence window from Observable
    ├── throttle(selector$)  — window duration from Observable
    └── audit(selector$)     — window start from Observable
```

---

## Common Pitfalls

### `sampleTime` Does Not Emit on Empty Windows

```typescript
// ❌ Assuming sampleTime emits a "no data" signal every 100ms:
sensorStream$.pipe(
  sampleTime(100),
  tap(v => {
    if (!v) showNoData(); // never happens — empty windows are silently skipped
  })
)

// ✅ Use auditTime + timeout to detect silence:
sensorStream$.pipe(
  auditTime(100),
  timeout({ each: 500, with: () => of(null) }) // emit null after 500ms silence
).subscribe(v => {
  if (v === null) showNoData();
  else            render(v);
});
```

### `auditTime` Swallows the Last Value Before Completion

```typescript
// ❌ Last emission lost if source completes before audit window fires:
of(1, 2, 3).pipe(auditTime(100)).subscribe(v => console.log(v));
// Nothing emitted! of() completes synchronously, audit timer never fires.

// ✅ For finite streams, use debounceTime or buffer instead:
of(1, 2, 3).pipe(
  debounceTime(0) // schedule to next microtask, after of() completes
).subscribe(v => console.log(v)); // 3
```

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 3/5 | **Composability**: 4/5
**Key insight**: `sampleTime` and `auditTime` complement `throttleTime` and `debounceTime` — they're the "always emit last value in window" operators where the window boundary is time-based, not silence-based. The `animationFrameScheduler` with `auditTime(0)` is the canonical way to sync any Observable to the browser's 60fps paint cycle.
