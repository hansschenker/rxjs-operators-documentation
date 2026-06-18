# RxJS Schedulers

## Identity

- **Names**: `asyncScheduler`, `queueScheduler`, `asapScheduler`, `animationFrameScheduler`
- **Category**: Utility / Scheduling
- **Type**: Execution context controllers — determine when work is dispatched
- **Import**:
  ```typescript
  import { asyncScheduler, queueScheduler, asapScheduler, animationFrameScheduler } from 'rxjs';
  ```

## What Is a Scheduler?

A scheduler controls **when** an Observable's notifications are dispatched. Without a scheduler, RxJS operators run synchronously by default. Schedulers inject work into different execution queues.

```typescript
import { of, asyncScheduler } from 'rxjs';
import { observeOn } from 'rxjs/operators';

// Synchronous (default):
of(1, 2, 3).subscribe(v => console.log(v));
console.log('after');
// 1, 2, 3, "after"

// Async scheduler — deferred to macro-task queue:
of(1, 2, 3).pipe(observeOn(asyncScheduler)).subscribe(v => console.log(v));
console.log('after');
// "after", 1, 2, 3
```

---

## The Four Schedulers

### `asyncScheduler` — Macro-task Queue (setTimeout)

Schedules work via `setTimeout(fn, delay)`. Equivalent to `setTimeout(fn, 0)` with no delay.

```typescript
import { asyncScheduler } from 'rxjs';

asyncScheduler.schedule(() => console.log('async'), 0);
console.log('sync');
// "sync", "async"
```

**Use when**:
- Deferring synchronous work to the next event loop tick
- Preventing blocking on large synchronous Observables
- `interval()` and `timer()` use this by default

```typescript
import { interval, asyncScheduler } from 'rxjs';

// These are equivalent:
interval(1000);
interval(1000, asyncScheduler);
```

---

### `queueScheduler` — Current Thread Queue (Recursive-safe)

Schedules work synchronously but queues recursive calls to prevent stack overflow. If already inside a scheduler execution, new work is queued rather than called recursively.

```typescript
import { queueScheduler, of } from 'rxjs';
import { observeOn, expand, take } from 'rxjs/operators';

// Safe recursive expansion — queueScheduler prevents stack overflow
of(1).pipe(
  expand(n => of(n + 1).pipe(observeOn(queueScheduler))),
  take(10000) // 10,000 recursive steps — safe with queueScheduler
).subscribe();
```

**Use when**: Deep recursive operators like `expand` that could overflow the call stack.

---

### `asapScheduler` — Micro-task Queue (Promise-like)

Schedules work as a micro-task — runs after the current synchronous code but **before** macro-tasks (before `asyncScheduler`).

```typescript
import { asapScheduler, asyncScheduler } from 'rxjs';

asapScheduler.schedule(() => console.log('asap'));
asyncScheduler.schedule(() => console.log('async'));
console.log('sync');
// "sync", "asap", "async"
// Order: sync → micro-tasks (asap) → macro-tasks (async)
```

**Use when**: Work must run after current synchronous code but before the next setTimeout tick. Rare in application code — mostly useful in library internals and testing.

---

### `animationFrameScheduler` — `requestAnimationFrame`

Schedules work on the next animation frame. All work queued before the frame fires is batched into a single rAF callback.

```typescript
import { animationFrameScheduler } from 'rxjs';
import { observeOn } from 'rxjs/operators';

// Batch DOM updates to animation frames:
stateUpdates$.pipe(
  observeOn(animationFrameScheduler)
).subscribe(state => {
  // runs at most once per frame — no wasted paints
  updateDOM(state);
});
```

**Use when**: DOM updates that should be synchronized with browser rendering.

---

## Scheduler Comparison

| Scheduler | Mechanism | Runs after sync? | Batches per frame? |
|---|---|---|---|
| (none) | Synchronous | No | No |
| `queueScheduler` | Sync queue | No | No |
| `asapScheduler` | Micro-task | Yes | No |
| `asyncScheduler` | `setTimeout(0)` | Yes | No |
| `animationFrameScheduler` | `requestAnimationFrame` | Yes | Yes |

**Execution order**: sync → queueScheduler → asapScheduler → asyncScheduler → animationFrameScheduler

---

## How to Use Schedulers

### With `observeOn` — Downstream Notifications

```typescript
import { observeOn } from 'rxjs/operators';
import { asyncScheduler } from 'rxjs';

// All downstream operators and subscribers run on asyncScheduler:
source$.pipe(
  observeOn(asyncScheduler),
  map(transform),    // async
  filter(predicate)  // async
).subscribe(handler); // async
```

### With `subscribeOn` — Subscription Setup

```typescript
import { subscribeOn } from 'rxjs/operators';
import { asyncScheduler } from 'rxjs';

// The subscribe() call itself is deferred:
source$.pipe(
  subscribeOn(asyncScheduler)
).subscribe(handler);
// The subscription is set up asynchronously
```

### With `scheduled` — Source Creation

```typescript
import { scheduled } from 'rxjs';
import { asyncScheduler } from 'rxjs';

// Array emitted asynchronously:
scheduled([1, 2, 3], asyncScheduler).subscribe(console.log);
console.log('after subscribe');
// "after subscribe", 1, 2, 3
```

### With Creation Operators

```typescript
import { interval, timer, of } from 'rxjs';
import { animationFrameScheduler, asyncScheduler } from 'rxjs';

// Operators that accept schedulers directly:
interval(0, animationFrameScheduler)  // tick on each rAF
timer(1000, asyncScheduler)            // default — rarely needed to specify
of(1, 2, 3, asyncScheduler)           // deprecated scheduler arg — use scheduled() instead
```

---

## Practical Examples

### Prevent Blocking the UI Thread

```typescript
import { scheduled } from 'rxjs';
import { asyncScheduler } from 'rxjs';
import { mergeMap } from 'rxjs/operators';

// Large array processed without blocking the UI:
scheduled(largeArray, asyncScheduler).pipe(
  mergeMap(item => processItem(item))
).subscribe(result => updateUI(result));
```

### Batch DOM Updates to Animation Frames

```typescript
import { Subject } from 'rxjs';
import { animationFrameScheduler } from 'rxjs';
import { observeOn, debounceTime } from 'rxjs/operators';

const stateChange$ = new Subject<AppState>();

stateChange$.pipe(
  debounceTime(0, animationFrameScheduler), // dedupe within same frame
  observeOn(animationFrameScheduler)
).subscribe(state => renderApp(state));
```

### Testing — `VirtualTimeScheduler`

```typescript
import { TestScheduler } from 'rxjs/testing';

// TestScheduler uses virtual time — no real delays needed
const scheduler = new TestScheduler((actual, expected) =>
  expect(actual).toEqual(expected)
);

scheduler.run(({ cold, expectObservable }) => {
  const source = cold('--a--b--|');
  expectObservable(source.pipe(delay(100))).toBe('102ms a--b--|');
});
```

See the [TestScheduler](../testing/TestScheduler) doc for full marble testing coverage.

---

## Common Pitfalls

### Using `asyncScheduler` When Synchrony Is Required

```typescript
// ❌ STATE CORRUPTION — async delivery of synchronous state update
const store = { value: 0 };
of(1).pipe(observeOn(asyncScheduler)).subscribe(v => store.value = v);
console.log(store.value); // 0 — update hasn't arrived yet!

// ✅ Don't add schedulers unless you have a specific reason:
of(1).subscribe(v => store.value = v);
console.log(store.value); // 1 — synchronous, correct
```

### Confusing `observeOn` and `subscribeOn`

```typescript
// observeOn: affects DOWNSTREAM notifications after this point
source$.pipe(
  map(x => x * 2),           // runs on source's scheduler
  observeOn(asyncScheduler),
  map(x => x + 1)            // runs on asyncScheduler
)

// subscribeOn: affects the SUBSCRIPTION setup (when subscribe() is called)
source$.pipe(
  subscribeOn(asyncScheduler) // defer subscription itself
)
// Most code needs observeOn, not subscribeOn.
// subscribeOn is for cold Observables where subscription triggers side effects.
```

## Related

- **`observeOn`**: Insert a scheduler into an existing pipeline
- **`subscribeOn`**: Defer the subscription setup
- **`scheduled`**: Create an Observable with a scheduler from the start
- **`animationFrames()`**: rAF-based Observable with `elapsed`/`timestamp` data
- **`TestScheduler`**: Virtual time scheduler for deterministic testing

---

**Cognitive Load**: 3/5 | **Usage Frequency**: 2/5 | **Composability**: 3/5
**Key teaching point**: Most application code never needs to specify a scheduler. Reach for one only when you need to control execution timing — async deferral, animation synchronization, or recursive safety.
