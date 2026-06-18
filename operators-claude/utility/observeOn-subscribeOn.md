# observeOn / subscribeOn

Two scheduler-control operators that determine WHERE work executes in RxJS.

---

## Scheduler Primer

RxJS **Schedulers** control when and on which context (sync, microtask, macrotask, animationFrame) work executes:

| Scheduler | Runs on | Use for |
|---|---|---|
| `asyncScheduler` | `setTimeout(0)` | Async/event loop delay |
| `asapScheduler` | Microtask queue (Promise-like) | Micro-task ordering |
| `animationFrameScheduler` | `requestAnimationFrame` | DOM rendering |
| `queueScheduler` | Synchronous queue | Recursive, tail-call loops |

---

## `observeOn`

### Identity
- **Import**: `import { observeOn } from 'rxjs/operators'`
- **Signature**: `observeOn<T>(scheduler: SchedulerLike, delay?: number): MonoTypeOperatorFunction<T>`
- **Category**: Utility — moves downstream observer callbacks to the specified scheduler

### Functional Specification

`observeOn(scheduler)` wraps each notification (next/error/complete) in a scheduled task. Downstream operators and the subscriber run on the scheduler; upstream operators continue on their original scheduler.

**What it controls**: The execution context for every `next`, `error`, and `complete` call flowing DOWNSTREAM from the `observeOn` position in the pipe.

```
Source (thread A) → operator1 → observeOn(scheduler) → operator2 → subscriber
                                      ↕
                     operator2 and subscriber now run on `scheduler`
                     operator1 still runs on thread A
```

### Marble Diagram

```
Source (sync):  (1)(2)(3)|
observeOn(asyncScheduler):
Result (async): ---1---2---3---|   (each value deferred to next event loop tick)

Without observeOn:
source.subscribe(v => console.log(v))  // synchronous — runs to completion before next line
With observeOn:
source.pipe(observeOn(asyncScheduler)).subscribe(v => console.log(v)) // async
```

### Examples

```typescript
import { of, animationFrameScheduler, asyncScheduler } from 'rxjs';
import { observeOn } from 'rxjs/operators';

// Move DOM updates to animation frame scheduler
stateChanges$.pipe(
  observeOn(animationFrameScheduler)
).subscribe(state => {
  // runs on requestAnimationFrame — safe for DOM manipulation
  updateDOM(state);
});

// Prevent long-running synchronous loops from blocking the UI
of(1, 2, 3, 4, 5).pipe(
  observeOn(asyncScheduler)
).subscribe(v => heavyComputation(v));
// Each heavyComputation runs in a separate event loop tick

// Testing: inject TestScheduler to control timing
import { TestScheduler } from 'rxjs/testing';
const testScheduler = new TestScheduler((actual, expected) => {
  expect(actual).toEqual(expected);
});

testScheduler.run(({ cold, expectObservable }) => {
  const source = cold('-a-b-|');
  const result = source.pipe(observeOn(testScheduler));
  expectObservable(result).toBe('-a-b-|');
});
```

---

## `subscribeOn`

### Identity
- **Import**: `import { subscribeOn } from 'rxjs/operators'`
- **Signature**: `subscribeOn<T>(scheduler: SchedulerLike, delay?: number): MonoTypeOperatorFunction<T>`
- **Category**: Utility — moves the subscription setup (and teardown) to the specified scheduler

### Functional Specification

`subscribeOn(scheduler)` defers the subscription call itself to the scheduler. This controls:
- When the cold Observable "starts" (the subscription side-effect)
- Which context the subscription logic runs in

**What it controls**: WHEN the upstream subscription happens — not when notifications are delivered.

```
Without subscribeOn:
subscription = source.pipe(op1, op2).subscribe(obs)
→ subscription setup happens SYNCHRONOUSLY here

With subscribeOn(asyncScheduler):
subscription = source.pipe(op1, op2, subscribeOn(asyncScheduler)).subscribe(obs)
→ subscription setup is scheduled for next tick
→ synchronous code AFTER this line runs BEFORE the source starts emitting
```

### Examples

```typescript
import { of, asyncScheduler } from 'rxjs';
import { subscribeOn } from 'rxjs/operators';

// Without subscribeOn — synchronous:
console.log('before');
of(1, 2, 3).subscribe(console.log);
console.log('after');
// Output: before, 1, 2, 3, after

// With subscribeOn — subscription deferred:
console.log('before');
of(1, 2, 3).pipe(
  subscribeOn(asyncScheduler)
).subscribe(console.log);
console.log('after');
// Output: before, after, 1, 2, 3
```

---

## `observeOn` vs `subscribeOn`

| | `observeOn(scheduler)` | `subscribeOn(scheduler)` |
|---|---|---|
| Controls | NOTIFICATION delivery context | SUBSCRIPTION timing/context |
| Affects | `next`, `error`, `complete` calls | When the source "starts" |
| Position matters | Inserted anywhere in pipe | Usually last operator |
| Primary use case | DOM updates, async notifications | Avoid sync subscription side-effects |
| Equivalent to | Wraps each observer call | Defers the subscribe() call |

**Rule of thumb**:
- Need to run subscriber code on animation frame → `observeOn(animationFrameScheduler)`
- Need to defer when a source starts emitting → `subscribeOn(asyncScheduler)`
- Most real-world code needs neither — RxJS operators handle scheduling internally

## Common Pitfalls

### Anti-pattern: Using `observeOn` Instead of Proper Async Operators
```typescript
import { asyncScheduler } from 'rxjs';
import { observeOn, delay } from 'rxjs/operators';

// ❌ WRONG — using observeOn to add delay
source$.pipe(
  observeOn(asyncScheduler) // "delays" by one tick — fragile and unclear
).subscribe(processValue);

// ✅ CORRECT — use delay() when you want to delay emissions by time
source$.pipe(
  delay(0) // explicit 0ms delay — same effect, clearly intentional
).subscribe(processValue);

// Or, if you want async delivery:
source$.pipe(
  delay(0, asyncScheduler) // delay with scheduler injection
).subscribe(processValue);

// WHY: observeOn is for changing the CONTEXT of execution (e.g., animation
// frames, worker threads). Using it as a makeshift delay is confusing.
```

### Anti-pattern: Not Understanding Placement
```typescript
// ❌ CONFUSING — observeOn before a heavy operator
source$.pipe(
  observeOn(asyncScheduler), // ← only affects operators below this line
  heavyTransformation(),      // STILL runs synchronously — observeOn is above it!
  subscribe(v => render(v))   // runs on asyncScheduler
)

// ✅ CLEAR — observeOn after computation, before rendering
source$.pipe(
  heavyTransformation(),     // runs synchronously
  observeOn(animationFrameScheduler), // from here down → animation frame
  map(v => toDisplayFormat(v))       // runs on animation frame
).subscribe(v => render(v)); // runs on animation frame

// WHY: observeOn only affects operators and subscriptions DOWNSTREAM of
// its position. Operators upstream of observeOn are unaffected.
```

## Related Operators

- **`delay(ms)`**: Time-based delay for emissions — simpler than `observeOn` for deferred delivery
- **`throttleTime(ms, scheduler)`**: Most time-based operators accept a scheduler parameter directly
- **`scheduled(source, scheduler)`**: Creation-time scheduler injection — alternative to `subscribeOn`

## References
- **observeOn**: [https://rxjs.dev/api/operators/observeOn](https://rxjs.dev/api/operators/observeOn)
- **subscribeOn**: [https://rxjs.dev/api/operators/subscribeOn](https://rxjs.dev/api/operators/subscribeOn)
- **Schedulers**: [https://rxjs.dev/guide/scheduler](https://rxjs.dev/guide/scheduler)

---

**`observeOn`** — Cognitive Load: 4/5 | Usage: 2/5 | Controls downstream notification context — primary use case is animation frame scheduling for DOM updates.
**`subscribeOn`** — Cognitive Load: 4/5 | Usage: 1/5 | Rarely needed in practice — defers subscription setup to a scheduler.
**Key teaching point**: Most scheduling needs are met by `animationFrameScheduler` with `observeOn`. Both operators are advanced — verify you need them before reaching for them.
