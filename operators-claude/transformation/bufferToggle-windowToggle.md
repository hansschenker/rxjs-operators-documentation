# bufferToggle / windowToggle

## Identity

- **Names**: `bufferToggle`, `windowToggle`
- **Category**: Transformation
- **Type**: Open/close signal-driven buffer/window — collect emissions between an opening and closing signal
- **Import**:
  ```typescript
  import { bufferToggle, windowToggle } from 'rxjs/operators';
  ```
- **Signatures**:
  ```typescript
  function bufferToggle<T, O>(
    openings: ObservableInput<O>,
    closingSelector: (openValue: O) => ObservableInput<any>
  ): OperatorFunction<T, T[]>

  function windowToggle<T, O>(
    openings: ObservableInput<O>,
    closingSelector: (openValue: O) => ObservableInput<any>
  ): OperatorFunction<T, Observable<T>>
  ```

## Functional Specification

`bufferToggle(openings$, closingSelector)` manages **multiple concurrent** buffers:

1. Each emission from `openings$` **opens a new buffer**, passing the open value to `closingSelector`
2. `closingSelector` returns an Observable — when it emits, **that specific buffer closes** and is emitted
3. Multiple buffers can be open simultaneously — a source value is added to **all currently open buffers**

This is the most flexible buffer/window operator — it models arbitrary open/close intervals that can overlap.

**Key distinction from other buffer operators**:

| Operator | Window boundary | Overlapping? |
|---|---|---|
| `bufferTime(ms)` | Fixed timer | No |
| `bufferCount(n)` | Fixed count | No (sliding with `startEvery`) |
| `buffer(signal$)` | Single signal | No |
| `bufferWhen(() => obs$)` | Per-window factory | No |
| `bufferToggle(open$, close)` | Open + close signals | **Yes** |

## Marble Diagram

```
Source:   --1--2--3--4--5--6--|
Open$:    -A-----------B------|
Close:    A → timer(3) ─────────→ fires at A+3
          B → timer(2) ──────────────→ fires at B+2

Buffers:
  Buffer A (opens at A, closes 3 units later): collects [1,2,3] → emits [1,2,3]
  Buffer B (opens at B, closes 2 units later): collects [5,6]   → emits [5,6]

bufferToggle result: ------[1,2,3]--------[5,6]--|
```

## Examples

### Basic Usage — Collect While Button Held
```typescript
import { bufferToggle } from 'rxjs/operators';
import { fromEvent } from 'rxjs';

const mousedown$ = fromEvent(document, 'mousedown');
const mouseup$   = fromEvent(document, 'mouseup');

// Collect all mousemove events while button is held down
fromEvent<MouseEvent>(document, 'mousemove').pipe(
  bufferToggle(
    mousedown$,
    () => mouseup$ // close buffer when button released
  )
).subscribe(points => {
  console.log(`Drew ${points.length} points`);
  renderStroke(points);
});
```

### Common Pattern — Record Activity Windows
```typescript
import { bufferToggle } from 'rxjs/operators';
import { Subject, timer } from 'rxjs';

const recordStart$ = new Subject<string>(); // emit session ID
const userActions$ = fromEvent(document, 'click');

// Record 5 seconds of actions per session
userActions$.pipe(
  bufferToggle(
    recordStart$,
    sessionId => timer(5000) // close after 5s
  )
).subscribe(actions => saveSession(actions));

// Start a recording:
recordStart$.next('session-123');
```

### Common Pattern — Overlapping Windows (Sliding Buffer)
```typescript
import { bufferToggle } from 'rxjs/operators';
import { interval } from 'rxjs';

const source$ = interval(100);

// New buffer every 200ms, each lasting 500ms → overlapping windows
source$.pipe(
  bufferToggle(
    interval(200),           // open a new buffer every 200ms
    () => timer(500)         // each buffer lasts 500ms
  )
).subscribe(buf => console.log('window:', buf));
// [0,1,2,3,4], [2,3,4,5,6,7], [4,5,6,7,8,9]...
// Windows overlap because 500ms > 200ms opening interval
```

### `windowToggle` — Observable Windows
```typescript
import { windowToggle, mergeMap, toArray } from 'rxjs/operators';
import { fromEvent, timer } from 'rxjs';

const focus$  = fromEvent(window, 'focus');
const blur$   = fromEvent(window, 'blur');

// Collect events only while window is focused
keystrokes$.pipe(
  windowToggle(
    focus$,
    () => blur$ // close window on blur
  ),
  mergeMap(win$ => win$.pipe(toArray()))
).subscribe(session => saveSession(session));
```

## Complete Buffer / Window Family

| Operator | Boundary | Overlapping | Output |
|---|---|---|---|
| `bufferTime(ms)` | Fixed timer | No | `T[]` |
| `bufferCount(n)` | Fixed count | Optional | `T[]` |
| `buffer(signal$)` | External signal | No | `T[]` |
| `bufferWhen(() => obs$)` | Per-window factory | No | `T[]` |
| `bufferToggle(open$, close)` | Open + close | **Yes** | `T[]` |
| `windowTime(ms)` | Fixed timer | No | `Observable<T>` |
| `windowCount(n)` | Fixed count | Optional | `Observable<T>` |
| `window(signal$)` | External signal | No | `Observable<T>` |
| `windowWhen(() => obs$)` | Per-window factory | No | `Observable<T>` |
| `windowToggle(open$, close)` | Open + close | **Yes** | `Observable<T>` |

## Common Pitfalls

### Closing Observable That Never Emits → Unbounded Buffer

```typescript
import { NEVER } from 'rxjs';

// ❌ MEMORY LEAK — buffer never closes, grows unbounded
source$.pipe(
  bufferToggle(openings$, () => NEVER)
).subscribe(buf => console.log(buf)); // never emits

// ✅ Always ensure closing Observable eventually emits
source$.pipe(
  bufferToggle(openings$, () => timer(5000)) // hard 5s limit
).subscribe(buf => console.log(buf));
// WHY: If closingSelector never emits, the buffer stays open forever,
// accumulating all values — an unbounded memory leak.
```

### Forgetting Values Go Into ALL Open Buffers

```typescript
// With overlapping windows, a source value appears in multiple buffers:
// Open A at t=0, Open B at t=2, Close both at t=5:
// Value at t=3 appears in BOTH buffer A and buffer B
// This is intentional behavior — not a bug — but surprising if unexpected.
```

## Related Operators

- **`buffer(signal$)`**: Single signal-driven buffer, no overlapping
- **`bufferWhen`**: Per-window closing factory (no explicit open signal)
- **`groupBy`**: Group by key rather than time/signal
- **`window(signal$)`**: Observable-emitting equivalent of `buffer`

---

**Cognitive Load**: 4/5 | **Usage Frequency**: 1/5 | **Composability**: 3/5
**Teaching sequence**: Teach after `buffer(signal$)` and `bufferWhen`. bufferToggle is the generalization — most code reaches for simpler operators first.
