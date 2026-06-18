# throttle / debounce

Observable-driven counterparts to `throttleTime` and `debounceTime`. Where the `*Time` variants accept a fixed millisecond value, these accept a **duration selector function** — giving you dynamic, per-value control over the silence window.

---

## `throttle`

### Identity
- **Import**: `import { throttle } from 'rxjs/operators'`
- **Signature**:
  ```typescript
  function throttle<T>(
    durationSelector: (value: T) => ObservableInput<any>,
    config?: ThrottleConfig
  ): MonoTypeOperatorFunction<T>

  interface ThrottleConfig {
    leading:  boolean  // emit on leading edge (default: true)
    trailing: boolean  // emit on trailing edge (default: false)
  }
  ```
- **Category**: Rate Limiting

### Functional Specification

`throttle(fn)` calls `fn(value)` on each source emission to get a duration Observable. While that duration Observable is active (has not emitted), new source emissions are suppressed. When the duration emits, the throttle window closes and the next source emission is let through.

**`leading: true, trailing: false`** (default): emit the first value, suppress until duration emits.
**`leading: false, trailing: true`**: suppress first, emit the LAST value when duration emits.
**Both true**: emit first AND last of each window.

**`throttle` vs `throttleTime`**:

| | `throttle(fn)` | `throttleTime(ms)` |
|---|---|---|
| Duration | Dynamic per-value Observable | Fixed milliseconds |
| Variable window | Yes | No |
| Adaptive throttle | Yes | No |

### Marble Diagram

```
Source:   --a--b--c---------d--e--f--|
throttle(() => timer(3)):
          Leading edge (default):
          a passes → 3-frame window opens
          b, c suppressed (window active)
          window closes → d passes → window opens again
          e, f suppressed

Result:   --a-----------d----------|

throttle(() => timer(3), { leading: false, trailing: true }):
          a arrives → window opens; a buffered
          b replaces a in buffer; c replaces b
          window ends → emit c (most recent)

Result:   -----c-----------f-------|
```

### Examples

```typescript
import { fromEvent, timer } from 'rxjs';
import { throttle } from 'rxjs/operators';

// Fixed throttle — same as throttleTime(300)
fromEvent(window, 'scroll').pipe(
  throttle(() => timer(300))
).subscribe(handleScroll);

// Dynamic throttle — longer window under high load
fromEvent(window, 'scroll').pipe(
  throttle(() => {
    const load = getServerLoad(); // 0–1
    return timer(100 + load * 900); // 100ms idle, up to 1s under load
  })
).subscribe(handleScroll);

// Adaptive throttle — vary by emitted value type
const events$ = merge(clickEvents$, keyboardEvents$);
events$.pipe(
  throttle(event => {
    // keyboard events need tighter throttling than clicks
    return timer(event.type === 'keydown' ? 50 : 300);
  })
).subscribe(handleEvent);
```

### Pitfall
```typescript
// ❌ WRONG — returning NEVER freezes throttle permanently
source$.pipe(
  throttle(v => NEVER) // first value passes, all subsequent suppressed forever
).subscribe(console.log); // only first value ever

// ✅ CORRECT — duration Observable must emit to reopen the window
source$.pipe(
  throttle(v => timer(300))
).subscribe(console.log);

// WHY: throttle keeps the window open until the duration Observable emits.
// A duration that never emits means the window never closes.
```

---

## `debounce`

### Identity
- **Import**: `import { debounce } from 'rxjs/operators'`
- **Signature**:
  ```typescript
  function debounce<T>(
    durationSelector: (value: T) => ObservableInput<any>
  ): MonoTypeOperatorFunction<T>
  ```
- **Category**: Rate Limiting

### Functional Specification

`debounce(fn)` calls `fn(value)` on each source emission to get a silence window Observable. If another source emission arrives before the window emits, the previous value is discarded and a new window starts. The value is forwarded only when the window Observable emits and no new source emission has arrived.

**`debounce` vs `debounceTime`**:

| | `debounce(fn)` | `debounceTime(ms)` |
|---|---|---|
| Silence window | Dynamic per-value Observable | Fixed milliseconds |
| Adaptive window | Yes | No |
| Use when | Window varies by value or load | Fixed delay |

### Marble Diagram

```
Source:   --a--b-----c----d--e--|
debounce(() => timer(3)):
          a → start 3-frame window; b arrives before window → cancel a, restart
          b window: fires → emit b
          c window: fires (no new emission) → emit c
          d → start window; e arrives → cancel d, restart
          e window: source completes → emit e

Result:   -----b-----c--------e--|
```

### Examples

```typescript
import { fromEvent, timer, BehaviorSubject } from 'rxjs';
import { debounce, map } from 'rxjs/operators';

// Fixed debounce — same as debounceTime(400)
fromEvent<InputEvent>(searchInput, 'input').pipe(
  map(e => (e.target as HTMLInputElement).value),
  debounce(() => timer(400))
).subscribe(query => search(query));

// Dynamic debounce — shorter window for short queries (fast typers)
fromEvent<InputEvent>(searchInput, 'input').pipe(
  map(e => (e.target as HTMLInputElement).value),
  debounce(query => timer(query.length < 3 ? 600 : 300))
  // short queries need more time to settle; longer queries respond faster
).subscribe(query => search(query));

// Adaptive debounce — pause longer when API is slow
const apiResponseTime$ = new BehaviorSubject(200); // ms

userInput$.pipe(
  debounce(() => timer(apiResponseTime$.value))
).subscribe(search);
```

---

## Complete Rate-Limiting Family

| Operator | Edge | Trigger | Dynamic? | Use when |
|---|---|---|---|---|
| `throttleTime(ms)` | Leading | Fixed ms | No | Emit first; ignore for fixed N ms |
| `throttle(fn)` | Leading | Observable | Yes | Emit first; variable silence window |
| `debounceTime(ms)` | Trailing | Fixed ms | No | Emit after fixed N ms silence |
| `debounce(fn)` | Trailing | Observable | Yes | Emit after variable silence |
| `auditTime(ms)` | Trailing | Fixed ms | No | Emit latest at fixed intervals |
| `audit(fn)` | Trailing | Observable | Yes | Emit latest; variable interval |
| `sampleTime(ms)` | Snapshot | Fixed clock | No | Snapshot on fixed clock |
| `sample(notifier$)` | Snapshot | Observable | Yes | Snapshot on external signal |

## References
- [throttle](https://rxjs.dev/api/operators/throttle)
- [debounce](https://rxjs.dev/api/operators/debounce)

---

**`throttle`** — Cognitive Load: 3/5 | Usage: 3/5 | Use when the silence window needs to adapt per emitted value or runtime conditions.
**`debounce`** — Cognitive Load: 3/5 | Usage: 3/5 | Same as debounceTime but with a dynamic silence window — useful for length-aware or load-aware debouncing.
