# buffer / window

Signal-driven buffering and windowing — the most flexible members of the buffer/window family.

---

## `buffer`

### Identity
- **Import**: `import { buffer } from 'rxjs/operators'`
- **Signature**: `buffer<T>(closingNotifier: ObservableInput<any>): OperatorFunction<T, T[]>`
- **Category**: Transformation — collects source values into `T[]` arrays, emitting each when the notifier fires

### Functional Specification

`buffer(notifier$)` collects source emissions into an array. Whenever `notifier$` emits, the current array is emitted and a new empty array starts collecting.

**Comparison with `bufferTime` and `bufferCount`**:

| | `buffer(notifier$)` | `bufferTime(ms)` | `bufferCount(n)` |
|---|---|---|---|
| Closes when | Notifier emits | Timer fires | N values collected |
| Empty buffers | Yes (emits `[]`) | Yes | No |
| Variable size | Yes | Yes | No |
| Trigger source | Any Observable | Fixed interval | Count |

**Key behaviors**:
- Emits `[]` if notifier fires with no buffered values
- Source completion emits the current buffer (even if partial) and completes
- If notifier completes, the buffer operator completes

### Marble Diagram

```
Source:   --1--2--3--4--5--6--7--|
Notifier: -------x--------x------|

buffer(notifier):
          At first x:  emit [1,2,3]
          At second x: emit [4,5,6]
          On complete: emit [7]   (partial final buffer)

Result:   -------[1,2,3]--------[4,5,6]--[7]|
```

### Examples

```typescript
import { fromEvent, interval } from 'rxjs';
import { buffer, filter } from 'rxjs/operators';

// Collect mouse clicks until user double-clicks
const click$  = fromEvent(document, 'click');
const dblClk$ = fromEvent(document, 'dblclick');

click$.pipe(
  buffer(dblClk$)
).subscribe(clicks => {
  console.log(`${clicks.length} clicks before double-click`);
});

// Collect scroll events in animation-frame-sized batches
const scroll$    = fromEvent(window, 'scroll');
const animFrame$ = interval(1000 / 60); // ~60fps

scroll$.pipe(
  buffer(animFrame$),
  filter(events => events.length > 0) // ignore empty frames
).subscribe(events => processScrollBatch(events));
```

### Common Pattern — Collect Until Signal
```typescript
import { Subject } from 'rxjs';
import { buffer } from 'rxjs/operators';

const events$  = new Subject<Event>();
const flush$   = new Subject<void>();

// Accumulate events until flush is explicitly triggered
events$.pipe(
  buffer(flush$)
).subscribe(batch => {
  if (batch.length > 0) sendBatch(batch);
});

// Later, flush on user action:
document.getElementById('submit')!.addEventListener('click', () => flush$.next());
```

---

## `window`

### Identity
- **Import**: `import { window } from 'rxjs/operators'`
- **Signature**: `window<T>(windowBoundaries: ObservableInput<any>): OperatorFunction<T, Observable<T>>`
- **Category**: Transformation (Higher-Order) — emits inner `Observable<T>` windows, each closed when the boundary notifier fires

### Functional Specification

`window(boundaries$)` is to `buffer(notifier$)` as `windowTime` is to `bufferTime` — it emits `Observable<T>` windows instead of `T[]` arrays. Each window emits values as they arrive (streaming), rather than waiting until the window closes to emit the full array.

**When to use `window` over `buffer`**:
- Need to apply reactive operators (first, filter, reduce) INSIDE each window before it closes
- Working with high-frequency sources where accumulating an array would be wasteful
- Need to react to the first value in a window before the window closes

### Marble Diagram

```
Source:   --1--2--3--4--5--6--|
Boundary: -------x--------x---|

window(boundary):
Outer:    W1------W2--------W3-|
W1:       --1--2--3|
W2:               --4--5--6|
W3:                         |   (empty window opened, source completes)

(compare to buffer which would emit [1,2,3], [4,5,6], [])
```

### Examples

```typescript
import { fromEvent, interval } from 'rxjs';
import { window, mergeMap, take, count } from 'rxjs/operators';

// Count clicks per second
const click$  = fromEvent(document, 'click');
const second$ = interval(1000);

click$.pipe(
  window(second$),
  mergeMap(window$ => window$.pipe(count()))
).subscribe(n => console.log(`${n} clicks this second`));

// Take only the FIRST click per 2-second window
click$.pipe(
  window(interval(2000)),
  mergeMap(window$ => window$.pipe(take(1)))
).subscribe(firstClick => handleFirstPerWindow(firstClick));
// Using buffer would require waiting for the window to close before seeing the first
```

### Common Pattern — Rate Summary
```typescript
import { Subject, interval } from 'rxjs';
import { window, mergeMap, reduce } from 'rxjs/operators';

const measurements$ = new Subject<number>();

// Compute stats per 5-second window
measurements$.pipe(
  window(interval(5000)),
  mergeMap(w$ => w$.pipe(
    reduce(
      (acc, v) => ({ sum: acc.sum + v, count: acc.count + 1, max: Math.max(acc.max, v) }),
      { sum: 0, count: 0, max: -Infinity }
    )
  ))
).subscribe(stats => {
  const avg = stats.count ? stats.sum / stats.count : 0;
  console.log(`avg: ${avg.toFixed(2)}, max: ${stats.max}, n: ${stats.count}`);
});
```

---

## Common Pitfalls

### Anti-pattern: Not Subscribing to `window` Inner Observables
```typescript
import { interval } from 'rxjs';
import { window, take } from 'rxjs/operators';

// ❌ WRONG — window emits Observable objects; tap doesn't subscribe
interval(100).pipe(
  take(9),
  window(interval(300)),
  tap(w$ => console.log('window:', w$)) // logs Observable object, not values!
).subscribe();

// ✅ CORRECT — use mergeMap to subscribe to each window
import { mergeMap, toArray } from 'rxjs/operators';
interval(100).pipe(
  take(9),
  window(interval(300)),
  mergeMap(w$ => w$.pipe(toArray()))
).subscribe(batch => console.log(batch));
// [0,1,2], [3,4,5], [6,7,8]

// WHY: window is a higher-order operator — it emits Observables, not values.
// Always flatten with mergeMap, switchMap, or concatMap.
```

### Anti-pattern: Using `buffer` When `bufferTime` Is Simpler
```typescript
import { interval } from 'rxjs';
import { buffer } from 'rxjs/operators';

// ❌ OVERENGINEERED for a time-based buffer
source$.pipe(
  buffer(interval(1000)) // equivalent to bufferTime(1000)
).subscribe(console.log);

// ✅ SIMPLER for time-based buffering:
import { bufferTime } from 'rxjs/operators';
source$.pipe(bufferTime(1000)).subscribe(console.log);

// WHY: Use buffer/window only when the closing signal is truly dynamic
// (button click, external event, custom logic). For time-based or count-
// based windows, bufferTime/bufferCount/windowTime/windowCount are clearer.
```

---

## The Buffer/Window Family

| Operator | Emits | Trigger | Use when |
|---|---|---|---|
| `bufferTime(ms)` | `T[]` | Timer | Time-based batches |
| `bufferCount(n)` | `T[]` | Count | Fixed-size batches |
| `buffer(notifier$)` | `T[]` | Observable | Signal-driven batches |
| `windowTime(ms)` | `Observable<T>` | Timer | Stream-process time windows |
| `windowCount(n)` | `Observable<T>` | Count | Stream-process count windows |
| `window(notifier$)` | `Observable<T>` | Observable | Signal-driven stream windows |

## References
- [buffer](https://rxjs.dev/api/operators/buffer)
- [window](https://rxjs.dev/api/operators/window)

---

**`buffer`** — Cognitive Load: 2/5 | Usage: 3/5 | Signal-driven batching — use when the close event is an Observable (button, frame, external trigger).
**`window`** — Cognitive Load: 3/5 | Usage: 2/5 | Signal-driven streaming windows — use when you need to apply reactive operators inside the window before it closes.
