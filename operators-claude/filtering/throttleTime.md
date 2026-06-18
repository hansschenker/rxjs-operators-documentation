# throttleTime

## Identity
- **Name**: throttleTime
- **Category**: Rate Limiting Operators / Filtering Operators
- **Type**: Time-window rate limiter — forwards first emission of each time window, suppresses the rest
- **Import**:
  ```typescript
  import { throttleTime } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  function throttleTime<T>(
    duration: number,
    scheduler: SchedulerLike = asyncScheduler,
    config: ThrottleConfig = { leading: true, trailing: false }
  ): MonoTypeOperatorFunction<T>

  interface ThrottleConfig {
    leading:  boolean;  // emit first value of window (default: true)
    trailing: boolean;  // emit last value of window  (default: false)
  }
  ```

## Functional Specification

**Input**: `Observable<T>` — a source that may emit at high frequency

**Output**: `Observable<T>` — an Observable that enforces a minimum interval between emissions

**Transformation**:
On each source emission, checks whether a silence window is active:
- **If no window active** (`leading: true`): emit the value, start a `duration`-ms silence window
- **During a silence window**: suppress the value (or hold it for trailing emission if `trailing: true`)
- **On window expiry** (`trailing: true`): if a value was suppressed during the window, emit the last suppressed value and start a new window

**Rate guarantee**: At most one emission per `duration` milliseconds (with default `leading: true, trailing: false`).

**Mathematical representation**:
```
Let tₙ = time of nth source emission
Let tₗₐₛₜ = time of last forwarded emission (−∞ initially)

Default (leading: true, trailing: false):
  vₙ forwarded if tₙ − tₗₐₛₜ ≥ duration
  vₙ suppressed if tₙ − tₗₐₛₜ < duration
  tₗₐₛₜ updated to tₙ on forwarding
```

**Invariants**:
- **O(1) memory**: Only the current window state and optional last-value (trailing) are held
- **No reordering**: Forwarded values appear in source order
- **Timing is approximate**: Uses the configured scheduler; precision depends on JS event loop

## Marble Diagram

```
Source:  --a-b-c-d---e-f-g-h--|
         throttleTime(40ms)    (default: leading=true, trailing=false)
         [window=====][window=====]

Result:  --a---------e--------|

a fires, window opens for 40ms → b,c,d suppressed.
Window expires; e fires, new window opens → f,g,h suppressed.
```

**`leading: true, trailing: true`** (both edges):
```
Source:  --a-b-c-d---e-f--|
         throttleTime(40ms, asyncScheduler, { leading: true, trailing: true })
Result:  --a-------d-e---f-|

a fires (leading), window opens.
d is last in window → emitted at window close (trailing).
e fires (leading of new window), f is trailing.
```

**`leading: false, trailing: true`** (trailing only — like throttle with inverted timing):
```
Source:  --a-b-c-d---e-f--|
         throttleTime(40ms, asyncScheduler, { leading: false, trailing: true })
Result:  ----------d-----f-|

Window starts on first emission; leading suppressed.
Last value in each window emitted at window close.
```

**Contrast with `debounceTime`**:
```
Source:       --a-b-c-d----e-f-g---|
debounceTime(40ms): --------d---------g--|  (silence after last; emits LAST)
throttleTime(40ms): --a---------e--------|  (fires first; suppresses rest)

debounceTime: "emit after the dust settles"
throttleTime: "emit immediately, then ignore for a while"
```

**Key observation**: `throttleTime` is for high-frequency streams where you want *immediate* response and *rate control* — button spam prevention, scroll handlers, keyboard shortcuts. `debounceTime` is for streams where you want to react only after activity *pauses* — search inputs, resize handlers.

## Behavioral Characteristics

**Subscription**:
- Subscribes to source lazily when output is subscribed
- Holds one internal timer (the silence window) and optionally one buffered value (trailing)

**Completion semantics**:
- Source completion propagates after the current window, emitting any pending trailing value first
- If source completes mid-window with `trailing: true` and a buffered value: the value is emitted, then the output completes

**Error handling**:
- Source errors propagate immediately; any pending trailing value is discarded

**Backpressure**:
- None — values suppressed synchronously; no queue; O(1) state

**Scheduler**:
- Default `asyncScheduler` uses `setTimeout` — subject to JS event loop delays (≥ 0ms real latency)
- `animationFrameScheduler`: throttle to animation frames (~16ms at 60fps) — ideal for scroll/resize handlers
- `VirtualTimeScheduler`: for deterministic testing without real time passing

**Hot vs. Cold**:
- Transparent; most useful with hot sources (events, subjects)
- With cold sources: the throttle window starts from the first emission and resets each new subscription

## Type System Integration

```typescript
/**
 * Type Parameters:
 *   T - Source and output value type (MonoTypeOperatorFunction<T>)
 *
 * Input Type:  Observable<T>
 * Output Type: Observable<T>
 *
 * throttleTime adds no transformation — T in = T out.
 * The ThrottleConfig object is fully typed; TypeScript enforces boolean fields.
 */

import { fromEvent, animationFrameScheduler } from 'rxjs';
import { throttleTime, map } from 'rxjs/operators';

// Typed: T = MouseEvent
fromEvent<MouseEvent>(document, 'mousemove').pipe(
  throttleTime(16, animationFrameScheduler),
  map(e => ({ x: e.clientX, y: e.clientY }))
).subscribe(pos => updateCursor(pos));
// pos: { x: number, y: number }

// ThrottleConfig — TypeScript enforces the shape
fromEvent(document, 'click').pipe(
  throttleTime(500, asyncScheduler, {
    leading:  true,  // fire on first click
    trailing: false  // do NOT fire again at window close
  })
).subscribe(handleClick);
```

## Examples

### Basic Usage — Button Spam Prevention
```typescript
import { fromEvent } from 'rxjs';
import { throttleTime } from 'rxjs/operators';

const submitBtn = document.getElementById('submit') as HTMLButtonElement;

fromEvent(submitBtn, 'click').pipe(
  throttleTime(2000) // allow at most one submission per 2 seconds
).subscribe(() => submitForm());

// User clicks rapidly:
// t=0ms   → submitForm() fires
// t=100ms → suppressed (within 2s window)
// t=200ms → suppressed
// t=2100ms → submitForm() fires again (new window)
```

### Common Pattern — Scroll Handler (Animation Frame Throttle)
```typescript
import { fromEvent, animationFrameScheduler } from 'rxjs';
import { throttleTime, map } from 'rxjs/operators';

// Throttle scroll events to animation frame rate (~60fps = ~16ms)
fromEvent<Event>(window, 'scroll').pipe(
  throttleTime(0, animationFrameScheduler), // 0ms duration — fires once per animation frame
  map(() => window.scrollY)
).subscribe(y => {
  updateStickyHeader(y);
  updateProgressBar(y);
});

// Without throttleTime, 'scroll' can fire 100+ times per second,
// causing layout thrashing. animationFrameScheduler aligns with the browser's
// repaint cycle — no work done faster than the screen can display.
```

### Common Pattern — Keyboard Shortcut Debounce
```typescript
import { fromEvent } from 'rxjs';
import { throttleTime, filter, map } from 'rxjs/operators';

// Ctrl+S to save — throttle to prevent rapid repeated saves
fromEvent<KeyboardEvent>(document, 'keydown').pipe(
  filter(e => e.ctrlKey && e.key === 's'),
  throttleTime(1000)
).subscribe(e => {
  e.preventDefault();
  saveDocument();
});

// User holds Ctrl+S — saves once per second, not on every keydown repeat
```

### Common Pattern — Leading + Trailing for Click-to-Load
```typescript
import { fromEvent } from 'rxjs';
import { throttleTime } from 'rxjs/operators';

const loadMoreBtn = document.getElementById('load-more')!;

// leading: true  → respond immediately on first click
// trailing: true → also respond to last click in a burst (catches final intent)
fromEvent(loadMoreBtn, 'click').pipe(
  throttleTime(500, asyncScheduler, { leading: true, trailing: true })
).subscribe(() => loadNextPage());

// Burst of 3 clicks in 400ms:
// t=0ms   → loadNextPage() fires (leading)
// t=100ms → suppressed (within 500ms window)
// t=400ms → last click in window; fires at t=500ms (trailing)
// Two loads total — not three.
```

### Common Pattern — Comparing with `debounceTime`
```typescript
import { Subject } from 'rxjs';
import { throttleTime, debounceTime } from 'rxjs/operators';

const events$ = new Subject<string>();

events$.pipe(throttleTime(300)).subscribe(v => console.log('throttle:', v));
events$.pipe(debounceTime(300)).subscribe(v => console.log('debounce:', v));

['a', 'b', 'c'].forEach((v, i) => setTimeout(() => events$.next(v), i * 100));
// t=0:   events$.next('a')
// t=100: events$.next('b')
// t=200: events$.next('c')
// t=600: (300ms after last emission)

// throttle output: 'a'                         (fires first, ignores b/c)
// debounce output:               'c'           (waits for silence, fires last)
```

### Edge Cases — Very Short Duration, Empty, Completion Mid-Window
```typescript
import { of, EMPTY, Subject } from 'rxjs';
import { throttleTime } from 'rxjs/operators';

// Edge case 1: synchronous source with throttleTime — first value passes, rest suppressed
// (of() is synchronous; all values arrive before the timer fires)
of('a', 'b', 'c').pipe(
  throttleTime(100)
).subscribe(console.log);
// Output: 'a'  (b and c arrive within 100ms window — suppressed)

// Edge case 2: empty source
EMPTY.pipe(throttleTime(100)).subscribe({ complete: () => console.log('done') });
// Output: done  (no emissions, no window started)

// Edge case 3: trailing emission on source completion
const subject$ = new Subject<string>();
subject$.pipe(
  throttleTime(200, asyncScheduler, { leading: true, trailing: true })
).subscribe(v => console.log(v));

subject$.next('a');  // leading: 'a' fires immediately
subject$.next('b');  // suppressed (within 200ms)
subject$.complete(); // source completes — trailing 'b' emitted immediately, then complete
// Output: 'a', 'b'
```

## Common Pitfalls

### Anti-pattern: Using `throttleTime` When `debounceTime` Is Needed
```typescript
import { fromEvent } from 'rxjs';
import { throttleTime, debounceTime, map, switchMap } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

const searchInput = document.getElementById('search') as HTMLInputElement;
const input$ = fromEvent(searchInput, 'input').pipe(
  map(e => (e.target as HTMLInputElement).value)
);

// ❌ WRONG TOOL — throttleTime for search input
input$.pipe(
  throttleTime(300), // fires FIRST value of each 300ms window
  switchMap(q => ajax.getJSON(`/api/search?q=${q}`))
).subscribe(renderResults);

// User types "rxjs" quickly:
// t=0:   'r' → search fires for "r"  (leading)
// t=50:  'rx' → suppressed
// t=100: 'rxj' → suppressed
// t=150: 'rxjs' → suppressed (300ms window still open)
// Searches for "r" not "rxjs" — wrong query!

// ✅ CORRECT — debounceTime for search: fire after user STOPS typing
input$.pipe(
  debounceTime(300),       // fire 300ms after last keystroke
  distinctUntilChanged(),
  switchMap(q => ajax.getJSON(`/api/search?q=${q}`))
).subscribe(renderResults);

// WHY: For search inputs, you want to wait until the user has finished typing
// (silence period), then search for the complete query — that's debounceTime.
// throttleTime fires the FIRST value of each burst, which is partial input.
// Rule of thumb: button clicks → throttleTime; text input → debounceTime.
```

### Anti-pattern: Not Accounting for Scheduler Latency
```typescript
import { interval } from 'rxjs';
import { throttleTime } from 'rxjs/operators';

// ❌ FRAGILE — expecting exact timing from asyncScheduler
interval(100).pipe(
  throttleTime(100) // interval period == throttle duration
).subscribe(n => console.log('expected every other:', n));

// In practice, asyncScheduler (setTimeout) has ≥0ms jitter.
// 100ms interval + 100ms throttle may result in 1 per 3 intervals, not 1 per 2.
// Timer precision varies by browser, system load, and background tab throttling.

// ✅ CORRECT — set throttle duration higher than the expected maximum interval
interval(100).pipe(
  throttleTime(150) // slightly more than interval — reliably suppresses every other
).subscribe(console.log);

// ✅ FOR TESTING — use VirtualTimeScheduler for exact timing
import { VirtualTimeScheduler } from 'rxjs';
const vts = new VirtualTimeScheduler();
interval(100, vts).pipe(
  throttleTime(100, vts)
).subscribe(console.log);
vts.flush(); // advances virtual time deterministically

// WHY: asyncScheduler uses macrotask scheduling (setTimeout/setInterval).
// Real execution time depends on the JS event loop and browser throttling policies.
// Background tabs may have their timers throttled to 1Hz. For exact behavior,
// use VirtualTimeScheduler in tests and add margin in production durations.
```

### Anti-pattern: Throttling Inside `switchMap` When the Outer Stream Should Be Throttled
```typescript
import { fromEvent } from 'rxjs';
import { throttleTime, switchMap } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

// ❌ SUBOPTIMAL — throttling inside switchMap throttles the inner request, not outer clicks
fromEvent(button, 'click').pipe(
  switchMap(() =>
    ajax.getJSON('/api/action').pipe(
      throttleTime(1000) // throttles the HTTP response stream, not the click stream
    )
  )
).subscribe(handleResponse);
// Every click still triggers a new HTTP request (switchMap cancels previous).
// The throttle here does nothing useful — HTTP responses don't need throttling.

// ✅ CORRECT — throttle the TRIGGER (click stream) before switchMap
fromEvent(button, 'click').pipe(
  throttleTime(1000), // at most 1 request per second
  switchMap(() => ajax.getJSON('/api/action'))
).subscribe(handleResponse);

// WHY: throttleTime should be placed at the point where you want to limit
// the rate — typically on the event/trigger stream, before the inner Observable
// is created. Placing it inside switchMap after creation throttles the response
// stream, which is already single-valued for HTTP (completes after one emission).
```

## Related Operators

**Same Category (Rate Limiting / Filtering)**:
- **`debounceTime`**: Emits last value after a silence period — use for text inputs, resize events, anything where you want to react after activity *stops*
- **`throttle`**: Like `throttleTime` but accepts a duration Observable for dynamic throttle windows
- **`auditTime`**: Always emits the *last* value of the window at window expiry (trailing only) — no leading emission
- **`sampleTime`**: Emits at fixed intervals regardless of source timing — use for polling/sampling at a fixed clock rate

**Commonly Composed With**:
- **`distinctUntilChanged`**: Prevent re-triggering when throttled value hasn't changed
- **`switchMap`**: Trigger throttled side effects (HTTP, analytics, etc.)
- **`animationFrameScheduler`**: Align throttle windows with browser repaint cycles

**Decision Guide — Rate Limiting**:

| Pattern | Operator | When |
|---------|----------|------|
| Fire immediately, then ignore | `throttleTime` | Button clicks, keyboard shortcuts |
| Wait for silence, fire last | `debounceTime` | Search input, resize |
| Fire last at fixed interval | `auditTime` | Batched analytics |
| Sample at fixed clock | `sampleTime` | Periodic state snapshots |
| Dynamic window | `throttle(obs)` | Variable rate (e.g., user-configurable) |

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/throttleTime](https://rxjs.dev/api/operators/throttleTime)
- **ReactiveX Documentation**: [http://reactivex.io/documentation/operators/sample.html](http://reactivex.io/documentation/operators/sample.html)
- **Source Code**: [https://github.com/ReactiveX/rxjs/blob/master/src/internal/operators/throttleTime.ts](https://github.com/ReactiveX/rxjs/blob/master/src/internal/operators/throttleTime.ts)

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**:
- **Pattern**: Leading-Edge Rate Gate (Fire-First, Suppress-Rest)
- **Cognitive Load**: 2/5 — The leading/trailing config and contrast with debounceTime are the main learning points; the operator itself is intuitive
- **Usage Frequency**: 4/5 — Present in any UI with high-frequency events (scroll, mousemove, clicks)
- **Composability**: 4/5 — Slots naturally into event streams; scheduler parameter enables testing and RAF-alignment

**Teaching Sequence**:
- **Prerequisites**: `debounceTime` (contrast essential), `fromEvent`, scheduler concept
- **Teaches**: Rate limiting, the throttle vs. debounce distinction, leading/trailing edge semantics, scheduler types
- **Common with**: `debounceTime` (contrast pair), `fromEvent`, `switchMap`, `animationFrameScheduler`
