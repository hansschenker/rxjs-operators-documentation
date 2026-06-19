# sample / audit — Advanced Patterns

> **Cognitive Load**: 3/5 | **Usage Frequency**: 2/5 | **Composability**: 4/5
> **Teaching Sequence**: After `auditTime`/`sampleTime` — same semantics with Observable-driven triggers instead of fixed intervals

---

## Advanced Behavioral Model

`sample` and `audit` are the Observable-driven counterparts to `sampleTime` and `auditTime`. The key distinction:

| Operator | Trigger | Emission timing | Duplicates |
|---|---|---|---|
| `sampleTime(ms)` | Fixed timer | At each tick | Possible (same value re-emitted) |
| `sample(notifier$)` | Any Observable | When notifier emits | Yes if source hasn't changed |
| `auditTime(ms)` | Fixed timer | Trailing edge of window | No (always latest) |
| `audit(fn)` | Per-value Observable | When returned Observable emits | No |

**`sample` marble — no emission if source is silent:**
```
Source:   --1-----2--3-----4--|
Notifier: ----x--------x------|

sample:
          ----1--------3------|
               ^          ^
       source had 1     source had 3
       (no emit at 2nd x if source silent since last)
```

**`audit` marble — trailing-edge, always latest since last emission:**
```
Source:   --1--2--3--------4--|
audit(()=>interval(50)):
          --------3--------4--|
                  ^
     50ms after first source value,
     emits the LATEST (3), resets window
```

---

## Type System Integration

```typescript
import { sample, audit } from 'rxjs/operators';
import { Subject, interval, fromEvent } from 'rxjs';

// sample: MonoTypeOperatorFunction — T in, T out
const sampled$: Observable<MouseEvent> = mousemove$.pipe(
  sample(animationFrames())    // notifier: Observable<any>
);

// audit: function returns ObservableInput — flexible trigger per value
const audited$: Observable<FormValue> = formChanges$.pipe(
  audit(value => {
    // trigger duration can depend on the value itself
    return value.type === 'text' ? interval(300) : interval(50);
  })
);

// With Subject as manual trigger
const trigger$ = new Subject<void>();
const onDemand$ = source$.pipe(sample(trigger$));
trigger$.next(); // manually sample current value
```

---

## Advanced Patterns

### 1. Animation-Frame Sampling for Smooth UI

Throttle high-frequency events (pointer, scroll, resize) to animation frames without missing the latest position.

```typescript
import { fromEvent, animationFrames } from 'rxjs';
import { sample, map, distinctUntilChanged } from 'rxjs/operators';

interface Position { x: number; y: number }

const smoothMousePosition$: Observable<Position> = fromEvent<MouseEvent>(
  document, 'mousemove'
).pipe(
  map(e => ({ x: e.clientX, y: e.clientY })),
  sample(animationFrames()),      // emit only on rAF — never more than 60fps
  distinctUntilChanged((a, b) => a.x === b.x && a.y === b.y),
);

smoothMousePosition$.subscribe(pos => {
  canvas.drawCursor(pos.x, pos.y);
});
```

### 2. Adaptive Audit Duration Based on Value

`audit` receives the source value, enabling the audit window duration to vary by content — ideal for form validation with different debounce needs per field type.

```typescript
import { fromEvent } from 'rxjs';
import { audit, map, switchMap } from 'rxjs/operators';
import { interval } from 'rxjs';

interface FieldChange {
  field: 'username' | 'email' | 'bio';
  value: string;
}

const AUDIT_DURATIONS: Record<FieldChange['field'], number> = {
  username: 200,   // fast: availability check
  email: 400,      // medium: format validation
  bio: 800,        // slow: heavy async check
};

const validatedChanges$ = formChanges$.pipe(
  audit((change: FieldChange) =>
    interval(AUDIT_DURATIONS[change.field])
  ),
  switchMap(change => validateField(change.field, change.value)),
);
```

### 3. Manual Sampling with Subject Trigger

Expose a "snapshot now" capability to external callers without exposing the source stream.

```typescript
import { Subject, merge } from 'rxjs';
import { sample } from 'rxjs/operators';

class DataMonitor<T> {
  private manualTrigger$ = new Subject<void>();
  private snapshot$: Observable<T>;

  constructor(source$: Observable<T>, autoInterval = 5000) {
    this.snapshot$ = source$.pipe(
      sample(
        merge(
          interval(autoInterval),         // automatic periodic snapshot
          this.manualTrigger$,           // on-demand snapshot
        )
      )
    );
  }

  snapshot(): void {
    this.manualTrigger$.next();
  }

  get data$(): Observable<T> {
    return this.snapshot$;
  }
}

const monitor = new DataMonitor(sensorReadings$, 10_000);
monitor.data$.subscribe(saveToDatabase);

// Operator presses button → immediate snapshot
saveButton.addEventListener('click', () => monitor.snapshot());
```

### 4. audit for Trailing-Edge Burst Collapse

When a source emits rapid bursts and you want only the final value of each burst (like `debounceTime` but reactive-trigger-based).

```typescript
import { audit, groupBy, mergeMap } from 'rxjs/operators';
import { Subject } from 'rxjs';

// Collapse rapid re-renders: only render when the render queue drains
const renderQueue$ = new Subject<ComponentUpdate>();
const renderBudget$ = animationFrames().pipe(take(1)); // one frame

const debouncedRenders$ = renderQueue$.pipe(
  audit(() => renderBudget$),  // emit latest update per frame
);

debouncedRenders$.subscribe(update => renderer.apply(update));


// Per-key audit: collapse updates per entity ID
interface EntityUpdate { id: string; payload: unknown }

const collapsed$ = entityUpdates$.pipe(
  groupBy(u => u.id),
  mergeMap(group$ =>
    group$.pipe(
      audit(() => animationFrames().pipe(take(1)))
    )
  )
);
```

### 5. Combining sample with WebSocket Backpressure

When a WebSocket pushes faster than the UI can render, `sample` on animation frames prevents dropped frames.

```typescript
import { webSocket } from 'rxjs/webSocket';
import { animationFrames } from 'rxjs';
import { sample, scan } from 'rxjs/operators';

interface TickerUpdate { symbol: string; price: number }

const ws$ = webSocket<TickerUpdate>('wss://feed.example.com/tickers');

// Accumulate latest price per symbol, render at most once per frame
const livePrices$ = ws$.pipe(
  scan((acc, tick) => ({ ...acc, [tick.symbol]: tick.price }), {} as Record<string, number>),
  sample(animationFrames()),  // latest accumulated state at each frame
);

livePrices$.subscribe(prices => updatePriceBoard(prices));
// Expected: even at 1000 msgs/sec, UI updates at max 60fps
```

---

## Common Pitfalls

```typescript
// ❌ INCORRECT — using sample when you need debounce semantics
searchInput$.pipe(
  sample(interval(300))
).subscribe(search);
// If user types on tick boundaries, sample misses input between ticks.
// No emission at all if user types then stops between ticks.

// ✅ CORRECT — debounceTime for "wait for pause" semantics
searchInput$.pipe(
  debounceTime(300)
).subscribe(search);
// WHY: sample is "take a reading at fixed points in time", not
// "wait until the user stops typing".


// ❌ INCORRECT — audit with an infinite Observable
source$.pipe(
  audit(() => new Observable(() => {})) // never emits → source values lost forever
).subscribe(console.log);

// ✅ CORRECT — audit's returned Observable must emit to release the value
source$.pipe(
  audit(() => interval(100).pipe(take(1))) // emits after 100ms, then completes
).subscribe(console.log);
// WHY: audit holds the latest value until the returned Observable emits.
// An Observable that never emits means audit never releases values.


// ❌ INCORRECT — sample on a notifier that completes immediately
source$.pipe(
  sample(of(1)) // of(1) emits synchronously then completes
).subscribe(console.log);
// Only samples once (synchronously at subscription time), then notifier is done.

// ✅ CORRECT — use a repeating notifier
source$.pipe(
  sample(interval(500))
).subscribe(console.log);
// WHY: sample subscribes to the notifier once; if it completes, no further
// sampling occurs. Use an ongoing Observable for ongoing sampling.
```

---

## Operator Comparison: Observable-Triggered Rate Limiting

| Need | Use |
|---|---|
| Sample at fixed time intervals | `sampleTime(ms)` |
| Sample when any external event fires | `sample(notifier$)` |
| Trailing-edge, fixed window | `auditTime(ms)` |
| Trailing-edge, reactive window per value | `audit(value => obs$)` |
| Leading-edge, suppress during window | `throttle(fn)` / `throttleTime(ms)` |
| Wait for pause in emissions | `debounceTime(ms)` / `debounce(fn)` |

---

## Related Operators

- **`sampleTime`** / **`auditTime`** — same semantics with fixed timer instead of Observable trigger
- **`debounce`** / **`debounceTime`** — wait for a pause; `audit` emits on window end regardless of silence
- **`throttle`** / **`throttleTime`** — leading-edge rate limiting
- **`animationFrames`** — ideal pairing for UI sampling
- **`distinctUntilChanged`** — pair with `sample` to suppress duplicate snapshots
