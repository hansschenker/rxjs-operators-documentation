# auditTime / sampleTime

## Identity

| | `auditTime` | `sampleTime` |
|---|---|---|
| **Import** | `import { auditTime } from 'rxjs/operators'` | `import { sampleTime } from 'rxjs/operators'` |
| **Signature** | `auditTime<T>(duration, scheduler?): MonoTypeOperatorFunction<T>` | `sampleTime<T>(period, scheduler?): MonoTypeOperatorFunction<T>` |
| **Category** | Rate Limiting Operators | Rate Limiting Operators |
| **Trigger** | Source emission starts a timer; emits latest when timer expires | Fixed-interval clock; emits latest value at each tick |

```typescript
function auditTime<T>(
  duration: number,
  scheduler?: SchedulerLike
): MonoTypeOperatorFunction<T>

function sampleTime<T>(
  period: number,
  scheduler?: SchedulerLike
): MonoTypeOperatorFunction<T>
```

## Functional Specification

**`auditTime(duration)`**: When the source emits, starts a silent timer for `duration` ms. When the timer expires, emits the **most recent** source value (which may have changed many times during the window). Subsequent source emissions during the timer restart nothing — the timer runs to completion. After emission, the timer is reset and will only start again on the next source emission.

**`sampleTime(period)`**: Emits the most recent source value at a fixed `period` interval, regardless of whether the source has emitted. If the source hasn't emitted since the last sample, nothing is emitted (does not re-emit the previous value).

**Comparison table — rate limiting operators**:

| Operator | Timer trigger | Emits | When |
|----------|--------------|-------|------|
| `debounceTime(ms)` | Source emission (restarts timer) | Latest value | After `ms` silence |
| `throttleTime(ms)` | Source emission (leading) | First value | Leading edge of window |
| `auditTime(ms)` | Source emission (trailing) | Latest value | Trailing edge of window |
| `sampleTime(ms)` | Fixed clock | Latest value | Every `ms` (if source emitted) |

**Key distinction**:
- `auditTime`: timer starts on source emission, like `debounceTime` but doesn't reset on subsequent emissions
- `sampleTime`: timer is independent of source — it ticks at a fixed rate

## Marble Diagrams

```
Source:    --a-bc---d-ef-g--|

auditTime(30ms):
           (timer starts on 'a', runs 30ms)
                (bc arrive during timer — latest is 'c')
           ----c----(timer starts on 'd')
                     (ef arrive during timer — latest is 'f')
           ---------f---(timer starts on 'g')
                          (no more source, g is last)
Result:    ----c--------f---g--|

sampleTime(30ms):
Fixed ticks:  |   |   |   |   |   |
              t=0 t=30 t=60 t=90 t=120
Source:    --a-bc---d-ef-g--|
At t=30:   latest is 'c'   → emit c
At t=60:   latest is 'd'   → emit d
At t=90:   latest is 'g'   → emit g
At t=120:  source complete → no more ticks
Result:    ---c-----d-----g--|
```

## Type System Integration

```typescript
import { fromEvent } from 'rxjs';
import { auditTime, sampleTime } from 'rxjs/operators';

// auditTime — type preserved
fromEvent<MouseEvent>(window, 'mousemove').pipe(
  auditTime(16) // ~60fps: emit latest position after each movement burst
).subscribe((e: MouseEvent) => updatePosition(e.clientX, e.clientY));

// sampleTime — type preserved
priceStream$.pipe(
  sampleTime(1000) // sample latest price every second
).subscribe((price: number) => updateDisplay(price));
```

## Examples

### Basic Usage
```typescript
import { interval, Subject } from 'rxjs';
import { auditTime, sampleTime, take } from 'rxjs/operators';

const source$ = new Subject<number>();

// auditTime: emit latest value 100ms after each burst
source$.pipe(auditTime(100)).subscribe(v => console.log('audit:', v));

source$.next(1);
source$.next(2);
source$.next(3);
// (100ms later) → audit: 3  (only the latest)

source$.next(4);
// (100ms later) → audit: 4

// sampleTime: emit latest every 100ms regardless of source rate
source$.pipe(sampleTime(100)).subscribe(v => console.log('sample:', v));
```

### Common Pattern — Scroll / Resize Handler (`auditTime`)
```typescript
import { fromEvent } from 'rxjs';
import { auditTime, map } from 'rxjs/operators';

// Scroll events fire rapidly — auditTime ensures we process after each burst
fromEvent(window, 'scroll').pipe(
  auditTime(16),  // ~one frame: emit latest scroll position after burst settles
  map(() => ({
    scrollTop:  document.documentElement.scrollTop,
    scrollLeft: document.documentElement.scrollLeft
  }))
).subscribe(pos => updateStickyHeader(pos));

// Why auditTime over debounceTime for scroll:
// debounceTime waits for silence — during fast scrolling it never fires
// auditTime fires at end of each 16ms window even if scrolling continues
```

### Common Pattern — Live Data Dashboard (`sampleTime`)
```typescript
import { webSocket } from 'rxjs/webSocket';
import { sampleTime } from 'rxjs/operators';

// High-frequency market data — sample at human-readable rate
const ticker$ = webSocket<Quote>('wss://stream.example.com/quotes');

ticker$.pipe(
  sampleTime(250)  // update display at 4Hz regardless of incoming rate
).subscribe(quote => updatePriceDisplay(quote));

// sampleTime is better than auditTime here because:
// - We want regular updates even when quotes arrive continuously (no "silence")
// - We don't need to wait for a burst to end
```

### Common Pattern — Animation Frame Rate Limiting
```typescript
import { fromEvent, animationFrameScheduler } from 'rxjs';
import { auditTime, map } from 'rxjs/operators';

// auditTime with animationFrameScheduler = emit once per render frame
fromEvent<MouseEvent>(canvas, 'mousemove').pipe(
  auditTime(0, animationFrameScheduler),
  map(e => ({ x: e.offsetX, y: e.offsetY }))
).subscribe(pos => drawCursor(pos));
// 0ms + animationFrameScheduler means "next animation frame"
// Ensures we never draw more than once per browser frame
```

## Common Pitfalls

### Anti-pattern: `sampleTime` When `debounceTime` Is Needed
```typescript
import { fromEvent } from 'rxjs';
import { sampleTime, debounceTime } from 'rxjs/operators';

// ❌ WRONG for search-as-you-type — sampleTime fires on a fixed clock,
// not when the user stops typing. It may fire mid-word or not at all during typing.
fromEvent(searchInput, 'input').pipe(
  sampleTime(500)
).subscribe(triggerSearch); // may miss the end of a word

// ✅ CORRECT for search-as-you-type — debounceTime waits for the user to pause
fromEvent(searchInput, 'input').pipe(
  debounceTime(300)
).subscribe(triggerSearch); // fires 300ms after last keystroke

// WHY: sampleTime emits on a fixed external clock — it doesn't know or care
// whether the user is actively typing. debounceTime resets on each emission
// so it fires only when the user pauses, which is the right trigger for search.
```

### Anti-pattern: `auditTime` When `throttleTime` Is Needed (Leading vs Trailing)
```typescript
import { fromEvent } from 'rxjs';
import { auditTime, throttleTime } from 'rxjs/operators';

// Button click handler — user wants immediate feedback
fromEvent(submitButton, 'click').pipe(
  auditTime(1000)  // ❌ WRONG: waits 1s after click before doing anything
).subscribe(submitForm);

// ✅ CORRECT for click rate-limiting: throttleTime fires on the FIRST click
// and ignores subsequent clicks for 1s
fromEvent(submitButton, 'click').pipe(
  throttleTime(1000)  // fires immediately, then ignores for 1s
).subscribe(submitForm);

// WHY: auditTime always waits the full duration before emitting (trailing edge).
// For UI actions that need immediate feedback but shouldn't double-fire,
// throttleTime (leading edge) is the right choice.
```

## Related Operators

- **`debounceTime(ms)`**: Resets timer on each emission — fires only after silence; best for "user stopped typing"
- **`throttleTime(ms)`**: Leading-edge gate — fires immediately, then ignores for `ms`; best for click rate-limiting
- **`sample(notifier$)`**: Like `sampleTime` but driven by an Observable instead of a fixed clock
- **`audit(durationSelector)`**: Like `auditTime` but duration is per-value Observable; dynamic windows
- **`bufferTime(ms)`**: Collects all values in a window into an array; auditTime only keeps the last

## References
- **RxJS auditTime**: [https://rxjs.dev/api/operators/auditTime](https://rxjs.dev/api/operators/auditTime)
- **RxJS sampleTime**: [https://rxjs.dev/api/operators/sampleTime](https://rxjs.dev/api/operators/sampleTime)

---

**`auditTime`** — Cognitive Load: 2/5 | Usage: 3/5 | Best for scroll/resize/canvas — trailing-edge latest-value after a burst.
**`sampleTime`** — Cognitive Load: 2/5 | Usage: 2/5 | Best for dashboards/polling — regular snapshots at a fixed rate independent of source frequency.
**Teaching sequence**: After `debounceTime` and `throttleTime` — the four rate-limiting operators form a complete set; the comparison table is the key teaching tool.
