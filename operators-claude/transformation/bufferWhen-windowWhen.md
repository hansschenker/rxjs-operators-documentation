# bufferWhen / windowWhen

## Identity

- **Names**: `bufferWhen`, `windowWhen`
- **Category**: Transformation
- **Type**: Signal-driven buffer/window — collect emissions until a closing signal, then start fresh
- **Import**:
  ```typescript
  import { bufferWhen, windowWhen } from 'rxjs/operators';
  ```
- **Signatures**:
  ```typescript
  function bufferWhen<T>(
    closingSelector: () => ObservableInput<any>
  ): OperatorFunction<T, T[]>

  function windowWhen<T>(
    closingSelector: () => ObservableInput<any>
  ): OperatorFunction<T, Observable<T>>
  ```

## Functional Specification

Both operators collect source values until a **closing Observable** emits. When the closing signal fires:
- **`bufferWhen`**: emits the accumulated `T[]`, starts a new buffer
- **`windowWhen`**: completes the current inner Observable, emits a new one

The `closingSelector` is called **once per buffer/window** — it returns an Observable whose first emission triggers the close. The selector is called again for the next window.

**Key distinction from `buffer(signal$)` / `window(signal$)`**:

| Operator | Closing trigger | Selector called |
|---|---|---|
| `buffer(signal$)` | Every emission of `signal$` | Once — same Observable reused |
| `bufferWhen(() => obs$)` | Every emission of `obs$` | Per-window — new Observable each time |
| `bufferTime(ms)` | Fixed timer | N/A |

`bufferWhen` / `windowWhen` are most useful when the window duration should **vary** per window (e.g., user interaction, adaptive timing).

## Marble Diagrams

```
Source:  --1-2-3-----4-5--6--|

bufferWhen(() => timer(3000)):
  [Window 1 ─ 3000ms]  [Window 2 ─ 3000ms]
         --------[1,2,3]--------[4,5,6]--|

bufferWhen(() => clicks$):
  [Window 1 ─ until click]  [Window 2 ─ until click]
  --1-2-click→[1,2]--3-4-5-click→[3,4,5]--|
  (each window ends on next click, regardless of time)
```

## Examples

### Basic Usage — Buffer Until User Clicks
```typescript
import { bufferWhen } from 'rxjs/operators';
import { fromEvent } from 'rxjs';

const clicks$ = fromEvent(document, 'click');

// Collect keystrokes until user clicks "submit"
fromEvent<KeyboardEvent>(document, 'keydown').pipe(
  bufferWhen(() => clicks$)
).subscribe(keys => {
  console.log('Keys since last click:', keys.map(e => e.key));
});
// Each click drains the buffer — next click starts fresh window
```

### Common Pattern — Adaptive Time Windows
```typescript
import { bufferWhen } from 'rxjs/operators';
import { timer, BehaviorSubject } from 'rxjs';
import { switchMap } from 'rxjs/operators';

const windowDuration$ = new BehaviorSubject(1000);

// Buffer with dynamic duration (changes per window)
sensorData$.pipe(
  bufferWhen(() => windowDuration$.pipe(
    switchMap(ms => timer(ms))  // current duration → one-shot timer
  ))
).subscribe(batch => processBatch(batch));

// Change window size at runtime:
windowDuration$.next(500);  // next window will be 500ms
```

### Common Pattern — windowWhen for Stream Partitioning
```typescript
import { windowWhen, mergeMap, toArray } from 'rxjs/operators';
import { fromEvent } from 'rxjs';

const pageBreak$ = fromEvent(document, 'pagebreak');

// Group stream values by page breaks
largeDataStream$.pipe(
  windowWhen(() => pageBreak$),
  mergeMap(window$ => window$.pipe(toArray()))
).subscribe(page => renderPage(page));
// Each window$ contains values for one "page" of data
```

### Edge Case — Empty Buffer on Rapid Closing Signal
```typescript
import { bufferWhen } from 'rxjs/operators';
import { EMPTY } from 'rxjs';

// closingSelector returning EMPTY closes immediately — always emits []
source$.pipe(
  bufferWhen(() => EMPTY) // closes on subscribe → always []
).subscribe(buf => console.log(buf)); // [],[],[],[] rapid fire — effectively drops all values

// closingSelector returning NEVER never closes — buffer accumulates forever
import { NEVER } from 'rxjs';
source$.pipe(
  bufferWhen(() => NEVER) // never closes — emits one final buffer on source complete
).subscribe(buf => console.log(buf)); // [all values] at the end
```

## Common Pitfalls

### Returning the Same Observable Instance from `closingSelector`

```typescript
import { bufferWhen } from 'rxjs/operators';
import { Subject } from 'rxjs';

const signal$ = new Subject<void>();

// ❌ WRONG — same Subject instance reused across windows
source$.pipe(
  bufferWhen(() => signal$) // all windows share same Subject — may fire for past windows
).subscribe(console.log);

// ✅ CORRECT — return a fresh Observable each time (Subject is fine here since
// it only fires future emissions, but generally prefer factory Observables)
source$.pipe(
  bufferWhen(() => fromEvent(document, 'click'))  // new subscription per window
).subscribe(console.log);

// WHY: closingSelector is called once per window. Reusing a cold Observable
// is fine (new subscription each time). Reusing a hot Subject works but
// means a single emission simultaneously closes all open windows.
```

### Using `bufferWhen` When `buffer` Is Sufficient

```typescript
// ❌ OVER-COMPLEX — closingSelector doesn't vary per window
source$.pipe(
  bufferWhen(() => interval(1000)) // same duration every time
)

// ✅ SIMPLER — use bufferTime when duration is fixed
source$.pipe(
  bufferTime(1000)
)

// ✅ OR buffer(signal$) when trigger is a shared Observable
source$.pipe(
  buffer(interval(1000))
)
// WHY: bufferWhen is only needed when the closing Observable must be
// created fresh per window (e.g., varies based on previous window content).
```

### Forgetting to Subscribe to `windowWhen` Inner Observables

```typescript
import { windowWhen } from 'rxjs/operators';

// ❌ INNER WINDOWS NOT SUBSCRIBED — values lost
source$.pipe(
  windowWhen(() => timer(1000))
).subscribe(window$ => {
  console.log('new window'); // window$ is an Observable — not subscribed!
});

// ✅ CORRECT — subscribe to each inner window
source$.pipe(
  windowWhen(() => timer(1000)),
  mergeMap(window$ => window$.pipe(toArray())) // or any operator that subscribes
).subscribe(batch => console.log('batch:', batch));
// WHY: windowWhen emits Observable<T>, not T[]. The inner Observable
// must be subscribed to (via mergeMap, concatMap, etc.) to receive values.
```

## The Full Buffer / Window Family

| Operator | Window boundary | Output |
|---|---|---|
| `bufferTime(ms)` | Fixed timer | `T[]` |
| `bufferCount(n)` | Fixed count | `T[]` |
| `buffer(signal$)` | External signal | `T[]` |
| `bufferWhen(() => obs$)` | Per-window signal factory | `T[]` |
| `windowTime(ms)` | Fixed timer | `Observable<T>` |
| `windowCount(n)` | Fixed count | `Observable<T>` |
| `window(signal$)` | External signal | `Observable<T>` |
| `windowWhen(() => obs$)` | Per-window signal factory | `Observable<T>` |

## Related Operators

- **`buffer(signal$)`**: Signal-driven buffer with a single shared closing Observable
- **`bufferTime`**: Fixed-interval buffer (simpler for constant windows)
- **`bufferCount`**: Count-based buffer
- **`windowWhen`**: Same as `bufferWhen` but emits inner Observables instead of arrays
- **`groupBy`**: Groups by key rather than time/count boundaries

---

**Cognitive Load**: 3/5 | **Usage Frequency**: 2/5 | **Composability**: 3/5
**Teaching sequence**: Teach `bufferTime` → `buffer(signal$)` → `bufferWhen` in that order. Each adds one degree of flexibility.
