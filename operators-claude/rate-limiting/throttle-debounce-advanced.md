# throttle / debounce — Advanced Patterns

For fundamentals see the core [throttle / debounce](./throttle-debounce) doc. This page covers Observable selectors, adaptive timing, and complex event coordination.

---

## `throttle` and `debounce` vs Their `-Time` Variants

```typescript
// throttleTime(ms) — emit first value, silence for ms:
source$.pipe(throttleTime(300))

// throttle(durationSelector$) — silence until the Observable emits:
source$.pipe(throttle(() => interval(300)))
// ✓ Duration can be dynamic — different silence per value

// debounceTime(ms) — wait ms of silence, then emit last:
source$.pipe(debounceTime(300))

// debounce(durationSelector$) — wait until the Observable emits:
source$.pipe(debounce(() => interval(300)))
// ✓ Duration Observable can adapt per value
```

The `-Time` variants are syntactic sugar. Use the base forms when the silence window should vary per emission.

---

## Pattern 1: Adaptive Debounce (Short Query = Longer Wait)

```typescript
import { debounce, interval } from 'rxjs/operators';

// Short queries are likely partial — wait longer before searching:
searchQuery$.pipe(
  debounce(query =>
    query.length < 3
      ? interval(800)   // wait longer for short/partial queries
      : interval(250)   // respond faster for longer, likely complete queries
  )
).subscribe(search);
```

---

## Pattern 2: Throttle with Leading AND Trailing Emission

`throttleTime` defaults to leading-only. Enable both:

```typescript
import { throttleTime } from 'rxjs/operators';
import { asyncScheduler } from 'rxjs';

// Leading: emit first value immediately
// Trailing: also emit last value after silence period
source$.pipe(
  throttleTime(500, asyncScheduler, { leading: true, trailing: true })
)

// Leading only (default) — good for: button click rate limiting
// Trailing only — good for: scroll position (want final position, not first)
// Both — good for: resize events (instant feedback + final dimensions)
source$.pipe(
  throttleTime(200, asyncScheduler, { leading: false, trailing: true })
).subscribe(handleResize);
```

---

## Pattern 3: `debounce` with Cancellation Signal

```typescript
import { debounce, race, timer } from 'rxjs/operators';
import { Subject } from 'rxjs';

const forceFlush$ = new Subject<void>();

// Normal debounce — but can be forced to flush immediately:
input$.pipe(
  debounce(() => race(timer(500), forceFlush$))
).subscribe(search);

// Force flush (e.g., on form submit):
submitBtn$.subscribe(() => forceFlush$.next());
```

---

## Pattern 4: Per-Key Debounce (Independent Debounce Per Input)

```typescript
import { groupBy, mergeMap, debounceTime } from 'rxjs/operators';

interface FieldChange { field: string; value: string; }

// Each field gets its own independent debounce timer:
fieldChanges$.pipe(
  groupBy(change => change.field),
  mergeMap(group$ =>
    group$.pipe(
      debounceTime(300),
      map(change => validate(change))
    )
  )
).subscribe(handleValidation);
// Changing 'email' doesn't reset 'name's debounce timer
```

---

## Pattern 5: Throttle Network Requests (Minimum Interval Between Calls)

```typescript
import { throttle, exhaustMap } from 'rxjs/operators';
import { timer } from 'rxjs';

// Ensure at least 1 second between API calls, regardless of trigger frequency:
refreshTrigger$.pipe(
  throttle(() => timer(1000), { leading: true, trailing: true }),
  exhaustMap(() => this.api.getData())
).subscribe(renderData);
```

---

## Pattern 6: `debounce` Until Animation Frame

Instead of a fixed delay, wait until the browser is ready to paint:

```typescript
import { debounce, animationFrameScheduler } from 'rxjs/operators';
import { timer } from 'rxjs';

// Coalesce multiple synchronous updates into a single render cycle:
stateChanges$.pipe(
  debounce(() => timer(0, animationFrameScheduler))
).subscribe(renderDOM);
// Multiple synchronous state.next() calls → one DOM update per frame
```

---

## Comparison Table: All Rate-Limiting Operators

| Operator | Emits | Cancels pending? | Use case |
|---|---|---|---|
| `debounceTime(ms)` | Last value after silence | Yes (resets on new input) | Search, form validation |
| `throttleTime(ms)` | First value, then silence | No (drops until silence) | Button clicks, scroll |
| `auditTime(ms)` | Last value every N ms | No | Scroll position logging |
| `sampleTime(ms)` | Last value at interval tick | No | Periodic snapshot |
| `throttle(obs$)` | First value, silence until obs$ | No | Dynamic silence window |
| `debounce(obs$)` | Last value after obs$ | Yes | Adaptive wait |

---

## Common Pitfalls

### `debounceTime` Delays Error Notifications Too

```typescript
// ❌ Errors are also debounced — delayed delivery:
source$.pipe(debounceTime(500))
// An error emitted at t=0 arrives at t=500ms

// ✅ If errors must propagate immediately, use materialize:
source$.pipe(
  materialize(),
  debounce(n => n.kind === 'E' ? timer(0) : timer(500)),
  dematerialize()
)
```

### Using `throttleTime` When You Need Final Value

```typescript
// ❌ throttleTime emits first value — misses final window size:
fromEvent(window, 'resize').pipe(
  throttleTime(200)
)
// Emits the size at start of resize, not after user finishes

// ✅ trailing: true captures final value:
fromEvent(window, 'resize').pipe(
  throttleTime(200, asyncScheduler, { leading: false, trailing: true })
)
```

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 4/5 | **Composability**: 4/5
**Decision rule**: `debounceTime` for "wait until user stops" (search, validation). `throttleTime` with `{ leading: true }` for "respond immediately but rate-limit" (clicks, scroll handlers). `{ trailing: true }` when you need the final value after a burst (resize, drag end).
