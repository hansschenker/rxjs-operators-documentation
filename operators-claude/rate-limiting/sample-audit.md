# sample / audit

Two Observable-driven rate-limiting operators: `sample` (clock-based snapshot) and `audit` (trailing-edge with reactive trigger).

---

## `sample`

### Identity
- **Import**: `import { sample } from 'rxjs/operators'`
- **Signature**: `sample<T>(notifier: ObservableInput<any>): MonoTypeOperatorFunction<T>`
- **Category**: Rate Limiting — emits the most recent source value whenever the notifier emits

### Functional Specification

`sample(notifier$)` is the Observable-driven equivalent of `sampleTime(ms)`. Whenever the `notifier$` emits, the most recent value from the source is emitted — or nothing if the source hasn't emitted since the last sample.

**Behavior**:
- Samples on every `notifier$` emission (not just on timer ticks)
- If the source hasn't emitted since the last sample, nothing is emitted (no duplicate)
- Completes when the source completes (notifier completion is ignored)
- Errors immediately if source errors

**Comparison with `sampleTime`**:

| | `sample(notifier$)` | `sampleTime(ms)` |
|---|---|---|
| Trigger | Observable emission | Fixed interval |
| Flexibility | Any Observable (button click, frame, custom) | Time only |
| Testing | Easier (inject test Observable) | Needs scheduler |

### Marble Diagram

```
Source:   --1--2--3--------4--5--|
Notifier: ------x------x------x--|

sample:
          ------3------3------5--|
          (at first x: last value was 3)
          (at second x: source still on 3 — emits 3 again)
          (at third x: last value was 5)

Source with no emission between ticks:
Source:   --1-----------2--------|
Notifier: ---x--x--x---x---------|
sample:   ---1-----------2-------|
          (second/third notifier tick: source unchanged → no emission)
```

### Examples

```typescript
import { fromEvent, interval } from 'rxjs';
import { sample, map } from 'rxjs/operators';

// Sample mouse position on every animation frame
const mouseMove$ = fromEvent<MouseEvent>(document, 'mousemove').pipe(
  map(e => ({ x: e.clientX, y: e.clientY }))
);
const animFrame$ = new Observable(obs => {
  const id = requestAnimationFrame(() => obs.next(null));
  return () => cancelAnimationFrame(id);
});

mouseMove$.pipe(
  sample(animFrame$)
).subscribe(pos => drawCursor(pos));

// Sample a form field value only when user clicks "Submit"
const fieldValue$ = fromEvent<InputEvent>(input, 'input').pipe(
  map(e => (e.target as HTMLInputElement).value)
);
const submit$ = fromEvent(submitBtn, 'click');

fieldValue$.pipe(
  sample(submit$)
).subscribe(value => sendToServer(value));
```

### Common Pattern — Game Loop Snapshot
```typescript
import { interval } from 'rxjs';
import { sample } from 'rxjs/operators';

const gameState$ = store.select(state => state.game); // updates rapidly
const gameTick$ = interval(1000 / 60); // 60fps render loop

// Render exactly once per frame, using latest state
gameState$.pipe(
  sample(gameTick$)
).subscribe(state => renderFrame(state));
```

### Pitfall
```typescript
// ❌ WRONG — using sample when debounceTime is needed for user input
searchInput$.pipe(
  sample(interval(300)) // samples every 300ms regardless of typing rhythm
).subscribe(search);
// Emits every 300ms even mid-word

// ✅ CORRECT — debounceTime waits for typing pause
searchInput$.pipe(
  debounceTime(300) // waits 300ms after last keystroke
).subscribe(search);
// WHY: sample is for "take a snapshot NOW" (driven by an external clock).
// debounceTime is for "wait until activity stops." Use sample when the
// trigger is external (frames, button clicks); use debounce for user input.
```

---

## `audit`

### Identity
- **Import**: `import { audit } from 'rxjs/operators'`
- **Signature**: `audit<T>(durationSelector: (value: T) => ObservableInput<any>): MonoTypeOperatorFunction<T>`
- **Category**: Rate Limiting — emits the most recent source value after a silence window determined by an Observable

### Functional Specification

`audit(fn)` is the Observable-driven equivalent of `auditTime(ms)`. When the source emits, `audit` calls `fn(value)` to get a duration Observable. When that duration Observable emits, the most recent source value is forwarded. The duration Observable starts fresh per source emission and is cancelled by a new source emission.

**Behavior**:
- Trailing-edge: emits AFTER the silence window, not at the start
- The duration Observable can be dynamic — it receives the source value
- A new source emission REPLACES the current duration window
- If the source completes while waiting, the buffered value is emitted

**`audit` vs `auditTime`**:

| | `audit(fn)` | `auditTime(ms)` |
|---|---|---|
| Duration | Dynamic per-value Observable | Fixed milliseconds |
| Flexibility | Can vary duration based on value | Fixed only |
| Backoff | Yes — return `timer(dynamic_ms)` | No |

### Marble Diagram

```
Source:   --a--b--c-----------d--|
audit(v => timer(3 frames)):
          When a emits → start 3-frame window
          b emits → replaces window (new 3-frame window from b's perspective)
          c emits → replaces window
          3 frames after c → emit c (most recent value)

Result:   ------------c---------d--|
          (a and b were superseded before their windows closed)
```

### Examples

```typescript
import { fromEvent, timer } from 'rxjs';
import { audit } from 'rxjs/operators';

// audit with fixed timer (same as auditTime)
fromEvent(window, 'scroll').pipe(
  audit(() => timer(200))
).subscribe(handleScroll);

// audit with DYNAMIC duration — vary delay based on value priority
const updates$ = new Subject<Update>();

updates$.pipe(
  audit(update => {
    // High-priority updates: 50ms window
    // Low-priority updates: 500ms window
    return timer(update.priority === 'high' ? 50 : 500);
  })
).subscribe(applyUpdate);

// audit for adaptive rate limiting — slow down during high load
updates$.pipe(
  audit(() => {
    const load = getSystemLoad(); // 0–1
    return timer(load * 1000);   // 0ms at idle, up to 1s at full load
  })
).subscribe(processUpdate);
```

### Pitfall
```typescript
// ❌ WRONG — returning a never-completing Observable freezes audit
source$.pipe(
  audit(v => NEVER) // duration Observable never emits → buffered value is never released
).subscribe(console.log); // nothing ever emits

// ✅ CORRECT — duration Observable must emit (or complete) to release the value
source$.pipe(
  audit(v => timer(100)) // releases after 100ms
).subscribe(console.log);
// WHY: audit waits for the duration Observable to emit. If it never emits,
// the buffered value is held indefinitely. Always use timer() or a finite
// Observable as the duration selector.
```

---

## Four Rate-Limiting Operators — Selection Guide

| Operator | Edge | Trigger | Use When |
|----------|------|---------|----------|
| `throttleTime(ms)` | Leading | Fixed timer | Emit FIRST, ignore for N ms |
| `debounceTime(ms)` | Trailing | Fixed timer | Emit AFTER N ms silence |
| `auditTime(ms)` / `audit(fn)` | Trailing | Timer / Observable | Emit LAST at fixed intervals |
| `sampleTime(ms)` / `sample(notifier$)` | Snapshot | Timer / Observable | Sample on external clock |

## References
- **sample**: [https://rxjs.dev/api/operators/sample](https://rxjs.dev/api/operators/sample)
- **audit**: [https://rxjs.dev/api/operators/audit](https://rxjs.dev/api/operators/audit)

---

**`sample`** — Cognitive Load: 2/5 | Usage: 3/5 | Observable-driven snapshot — use for external clocks (animation frames, button clicks).
**`audit`** — Cognitive Load: 3/5 | Usage: 2/5 | Observable-driven trailing edge — use when duration needs to vary per emitted value.
