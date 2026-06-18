# throttleTime — Advanced Patterns

For `throttleTime` fundamentals see the core [throttleTime](./throttleTime) doc. This page covers leading/trailing configuration, UI interaction patterns, scroll handling, and the comparison with `debounceTime` and `auditTime`.

---

## The Leading/Trailing Config

`throttleTime` accepts a `ThrottleConfig` second argument controlling when to emit:

```typescript
import { throttleTime } from 'rxjs/operators';
import { asyncScheduler } from 'rxjs';

// Leading only (default) — emit first, ignore for duration:
source$.pipe(throttleTime(500))
// ≡ throttleTime(500, asyncScheduler, { leading: true, trailing: false })

// Trailing only — ignore until quiet, emit last:
source$.pipe(throttleTime(500, asyncScheduler, { leading: false, trailing: true }))
// Similar to debounceTime but at fixed intervals, not silence-based

// Both leading AND trailing — emit first AND last of each window:
source$.pipe(throttleTime(500, asyncScheduler, { leading: true, trailing: true }))
// First event fires immediately; last event fires at end of window
```

---

## Visual Comparison

```
Source: -1-2-3---4-5-6---7-8--|

throttleTime(300, leading:true, trailing:false):
        -1-------4-------7----|   (first of each burst)

throttleTime(300, leading:false, trailing:true):
        -----3-------6-------8|   (last of each burst)

throttleTime(300, leading:true, trailing:true):
        -1---3---4---6---7---8|   (first AND last of each burst)

debounceTime(300):
        ---------3-------6---8|   (only after silence)
```

---

## Pattern 1: Button Click Rate Limiting

Prevent rapid double-clicks while still feeling responsive:

```typescript
import { fromEvent } from 'rxjs';
import { throttleTime, exhaustMap } from 'rxjs/operators';

// Leading: responds immediately to first click, ignores rapid subsequent clicks:
fromEvent(saveButton, 'click').pipe(
  throttleTime(2000),              // respond to click immediately, ignore next 2s
  exhaustMap(() =>                 // additionally: ignore if save is still in flight
    this.api.save(this.form.value).pipe(
      catchError(err => { this.showError(err); return EMPTY; })
    )
  )
).subscribe(() => this.showSuccess());
```

---

## Pattern 2: Scroll Position Tracking

Sample scroll position without flooding handlers:

```typescript
import { fromEvent } from 'rxjs';
import { throttleTime, map } from 'rxjs/operators';
import { asyncScheduler } from 'rxjs';

// Throttle scroll events at ~60fps (16ms) for smooth updates:
fromEvent(window, 'scroll').pipe(
  throttleTime(16, asyncScheduler, { leading: true, trailing: true }),
  map(() => ({
    scrollY:    window.scrollY,
    scrollX:    window.scrollX,
    direction:  window.scrollY > (lastScrollY ?? 0) ? 'down' : 'up'
  }))
).subscribe(updateStickyHeader);
```

---

## Pattern 3: Window Resize

React to window resize without excessive recomputation:

```typescript
import { fromEvent } from 'rxjs';
import { throttleTime, map, distinctUntilChanged } from 'rxjs/operators';

fromEvent(window, 'resize').pipe(
  throttleTime(150, asyncScheduler, { leading: false, trailing: true }),
  map(() => ({ width: window.innerWidth, height: window.innerHeight })),
  distinctUntilChanged((a, b) => a.width === b.width && a.height === b.height)
).subscribe(({ width, height }) => {
  updateLayout(width, height);
  recalculatePositions();
});
```

---

## Pattern 4: Live Typing Indicator

Show "is typing..." throttled to avoid updating on every keystroke:

```typescript
import { Subject } from 'rxjs';
import { throttleTime, map } from 'rxjs/operators';

const keystrokes$ = new Subject<void>();

// Send "typing" signal at most once per second:
keystrokes$.pipe(
  throttleTime(1000, asyncScheduler, { leading: true, trailing: false })
).subscribe(() => this.ws.send({ type: 'typing', userId: this.userId }));

// Call on every keystroke:
onKeystroke() { keystrokes$.next(); }
```

---

## Pattern 5: Mouse Position Tracking

Track mouse position for drag, hover effects, or analytics:

```typescript
import { fromEvent } from 'rxjs';
import { throttleTime, map } from 'rxjs/operators';

const mousePos$ = fromEvent<MouseEvent>(document, 'mousemove').pipe(
  throttleTime(50),           // max 20 updates/second
  map(e => ({ x: e.clientX, y: e.clientY }))
);

// For smooth 60fps animation, use animationFrames instead:
import { animationFrames } from 'rxjs';
import { withLatestFrom } from 'rxjs/operators';

const smoothMousePos$ = animationFrames().pipe(
  withLatestFrom(fromEvent<MouseEvent>(document, 'mousemove')),
  map(([, e]) => ({ x: e.clientX, y: e.clientY }))
);
```

---

## Pattern 6: API Rate Limiting

Enforce API rate limits on outbound requests:

```typescript
import { Subject, merge } from 'rxjs';
import { throttleTime, mergeMap, map } from 'rxjs/operators';

// Rate limit: max 1 request per 100ms (10 req/sec):
const apiRequests$ = new Subject<{ endpoint: string; params: unknown }>();

apiRequests$.pipe(
  throttleTime(100),           // rate limit outbound requests
  mergeMap(req => this.http.get(req.endpoint, { params: req.params as any }))
).subscribe(handleResponse);

// All callers push to this Subject:
function makeApiCall(endpoint: string, params: unknown) {
  apiRequests$.next({ endpoint, params });
}
```

---

## `throttleTime` vs `debounceTime` vs `auditTime` — When to Use Each

| Operator | Fires | Feels like | Best for |
|---|---|---|---|
| `throttleTime(ms)` | First event in window | Immediate, limited | Clicks, scroll, mouse track |
| `throttleTime(ms, {trailing:true})` | First AND last | Responsive + complete | Resize, drag end |
| `debounceTime(ms)` | Last event after silence | Waiting for user to stop | Search, form validation |
| `auditTime(ms)` | Last event at fixed interval | Smooth sampling | Chart updates, telemetry |

```
User types: a--b--c--d-------e--f--|

throttleTime(200): a--------d-------e----|  (immediate, periodic)
debounceTime(200): ----------d--------f--|  (after silence)
auditTime(200):    ---b---d---------f-----|  (at interval marks)
```

---

## Common Pitfalls

### Using `throttleTime` for Search (Should Be `debounceTime`)

```typescript
// ❌ throttleTime for search — fires on first keystroke, misses final value:
searchInput$.pipe(
  throttleTime(300),
  switchMap(q => api.search(q))
)
// If user types "hello", fires immediately with "h" — wrong!

// ✅ debounceTime waits for the user to stop typing:
searchInput$.pipe(
  debounceTime(300),
  switchMap(q => api.search(q))
)
// WHY: throttleTime is for rate-limiting events. debounceTime is for
// waiting until the user finishes a series of events.
```

### `throttleTime` Drops the Trailing Value by Default

```typescript
// ❌ If the most important event is the LAST one, default throttleTime loses it:
resizeEvents$.pipe(
  throttleTime(200) // leading:true, trailing:false — emits FIRST resize only
).subscribe(layout => recalculate(layout));
// Final layout dimensions are LOST — the first event had old dimensions!

// ✅ For resize, use trailing: true to ensure final dimensions are captured:
resizeEvents$.pipe(
  throttleTime(200, asyncScheduler, { leading: false, trailing: true })
).subscribe(layout => recalculate(layout));
```

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 4/5 | **Composability**: 4/5
**Key rule**: `throttleTime` is for **rate-limiting interactions** (clicks, scroll, mousemove) where you want immediate response but not every event. Use `leading: true, trailing: false` (default) for button clicks; use `trailing: true` for resize/drag-end where the final value matters.
