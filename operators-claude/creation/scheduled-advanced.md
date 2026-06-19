# scheduled — Advanced Patterns

> **Cognitive Load**: 4/5 | **Usage Frequency**: 2/5 | **Composability**: 4/5
> **Teaching Sequence**: After `observeOn`/`subscribeOn` — introduces scheduler injection at the creation layer

---

## Advanced Behavioral Model

`scheduled(input, scheduler)` is the low-level primitive that scheduler-aware creation operators (`timer`, `interval`, `of` with schedulers) are built on. It wraps any `ObservableInput` and routes all notifications through the provided scheduler.

**Scheduler dispatch model:**

```
// Without scheduler (from):
from([1, 2, 3]).subscribe(console.log);
console.log('after subscribe');
// Output: 1, 2, 3, after subscribe   ← synchronous, same frame

// With asyncScheduler (scheduled):
scheduled([1, 2, 3], asyncScheduler).subscribe(console.log);
console.log('after subscribe');
// Output: after subscribe, 1, 2, 3   ← deferred to microtask queue
```

**What `scheduled` controls vs `observeOn`:**

| | `scheduled(input, S)` | `observeOn(S)` |
|---|---|---|
| Affects subscription | Yes — subscription is scheduled | No |
| Affects emissions | Yes — from source | Yes — downstream of position |
| Works on | `ObservableInput` (source creation) | Existing `Observable` (mid-pipe) |
| Use when | Creating a scheduler-aware source | Adding scheduler to existing pipeline |

---

## Scheduler Reference

```typescript
import {
  asyncScheduler,         // setTimeout — async, non-blocking
  asapScheduler,          // Promise microtask queue — faster than async
  queueScheduler,         // Synchronous queue — recursive without stack overflow
  animationFrameScheduler, // requestAnimationFrame — visual updates
} from 'rxjs';

// asyncScheduler: macro-task (setTimeout 0)
scheduled(data, asyncScheduler);

// asapScheduler: micro-task (Promise.resolve)
// Faster than async but still non-blocking
scheduled(data, asapScheduler);

// queueScheduler: synchronous but queued — safe for recursion
// Prevents stack overflows in expand() / recursive Observables
scheduled(data, queueScheduler);

// animationFrameScheduler: each value on the next rAF
scheduled(frames, animationFrameScheduler);
```

---

## Advanced Patterns

### 1. Preventing Synchronous Blocking with asyncScheduler

When a large synchronous array would block the main thread, `scheduled` defers emission to the event loop.

```typescript
import { scheduled, asyncScheduler } from 'rxjs';
import { map, filter } from 'rxjs/operators';

const largeDataset: number[] = Array.from({ length: 100_000 }, (_, i) => i);

// ❌ Blocks main thread for the duration of subscription
from(largeDataset).pipe(
  map(expensiveTransform),
  filter(meetsThreshold),
).subscribe(result => updateUI(result));

// ✅ Yields to the event loop between emissions
scheduled(largeDataset, asyncScheduler).pipe(
  map(expensiveTransform),
  filter(meetsThreshold),
).subscribe(result => updateUI(result));

// Each value is scheduled as a separate async task,
// allowing UI events and other tasks to interleave.
```

### 2. Testing Time-Sensitive Pipelines with TestScheduler

`scheduled` + `TestScheduler` gives you deterministic control over async timing in tests.

```typescript
import { TestScheduler } from 'rxjs/testing';
import { scheduled, asyncScheduler } from 'rxjs';
import { delay, map } from 'rxjs/operators';

describe('scheduled with TestScheduler', () => {
  let testScheduler: TestScheduler;

  beforeEach(() => {
    testScheduler = new TestScheduler((actual, expected) => {
      expect(actual).toEqual(expected);
    });
  });

  it('emits values asynchronously', () => {
    testScheduler.run(({ expectObservable, cold }) => {
      // Replace asyncScheduler with testScheduler for control
      const source$ = scheduled([1, 2, 3], testScheduler);

      expectObservable(source$).toBe('(abc|)', { a: 1, b: 2, c: 3 });
    });
  });

  it('scheduled + delay is testable', () => {
    testScheduler.run(({ expectObservable }) => {
      const source$ = scheduled(['x'], testScheduler).pipe(
        delay(100, testScheduler)
      );
      expectObservable(source$).toBe('100ms (x|)');
    });
  });
});
```

### 3. animationFrameScheduler for Frame-Rate-Locked Animation

Schedule a sequence of values to emit one per animation frame — ideal for imperative animation driven by reactive streams.

```typescript
import { scheduled, animationFrameScheduler, range } from 'rxjs';
import { map, takeWhile } from 'rxjs/operators';

interface AnimationFrame {
  progress: number; // 0–1
  eased: number;
}

function easeInOut(t: number): number {
  return t < 0.5 ? 2 * t * t : -1 + (4 - 2 * t) * t;
}

function animateTo(
  element: HTMLElement,
  from: number,
  to: number,
  durationFrames: number,
): Observable<void> {
  return scheduled(
    Array.from({ length: durationFrames }, (_, i) => i / (durationFrames - 1)),
    animationFrameScheduler,
  ).pipe(
    map(t => {
      const eased = easeInOut(t);
      element.style.transform = `translateX(${from + (to - from) * eased}px)`;
    }),
  );
}

animateTo(panel, 0, 300, 30).subscribe(); // smooth 30-frame slide
```

### 4. queueScheduler for Safe Recursive Expansion

Recursive Observables (e.g. `expand`) can overflow the stack with synchronous schedulers. `queueScheduler` processes recursion iteratively.

```typescript
import { scheduled, queueScheduler } from 'rxjs';
import { expand, take, map } from 'rxjs/operators';

// Fibonacci sequence using expand — safe with queueScheduler
const fibonacci$ = scheduled([{ a: 0, b: 1 }], queueScheduler).pipe(
  expand(({ a, b }) =>
    scheduled([{ a: b, b: a + b }], queueScheduler)
  ),
  map(({ a }) => a),
  take(20),
);

fibonacci$.subscribe(console.log);
// Output: 0, 1, 1, 2, 3, 5, 8, 13, 21, 34 ...
// queueScheduler prevents stack overflow in deep recursion
```

### 5. Scheduler-Injected Source for Dependency Injection

Make scheduler a parameter for testable, environment-aware source factories.

```typescript
import { scheduled, SchedulerLike, asyncScheduler } from 'rxjs';
import { map, bufferCount } from 'rxjs/operators';

function createDataPipeline<T>(
  data: T[],
  transform: (item: T) => T,
  scheduler: SchedulerLike = asyncScheduler, // injectable
): Observable<T[]> {
  return scheduled(data, scheduler).pipe(
    map(transform),
    bufferCount(10),
  );
}

// Production: async, non-blocking
const prod$ = createDataPipeline(dataset, normalize);

// Test: synchronous, deterministic
const testScheduler = new TestScheduler(expect);
const test$ = createDataPipeline(dataset, normalize, testScheduler);
```

### 6. Combining scheduled with Promise Arrays

`scheduled` accepts `ObservableInput` — including arrays of Promises — scheduling their resolution through a specific scheduler.

```typescript
import { scheduled, asyncScheduler, from } from 'rxjs';
import { mergeMap, toArray } from 'rxjs/operators';

const urls = ['/api/a', '/api/b', '/api/c'];

// Schedule each fetch to start asynchronously, avoid sync blocking
const responses$ = scheduled(urls, asyncScheduler).pipe(
  mergeMap(url => from(fetch(url).then(r => r.json()))),
  toArray(),
);

responses$.subscribe(allResults => renderDashboard(allResults));
```

---

## Common Pitfalls

```typescript
// ❌ INCORRECT — using scheduled when observeOn would do
existingObservable$.pipe(
  // Can't use scheduled here — it only wraps ObservableInput, not Observable
).subscribe();

// To add a scheduler mid-pipe, use observeOn:
existingObservable$.pipe(
  observeOn(asyncScheduler),
).subscribe();
// WHY: scheduled wraps at creation. observeOn applies mid-pipeline.
// They are complementary, not interchangeable.


// ❌ INCORRECT — using animationFrameScheduler for non-visual work
scheduled(heavyComputationData, animationFrameScheduler)
  .subscribe(compute);
// Ties computation to the display refresh rate unnecessarily.
// If the tab is hidden, rAF pauses, stalling the computation.

// ✅ CORRECT — use asyncScheduler for non-visual background work
scheduled(heavyComputationData, asyncScheduler)
  .subscribe(compute);
// WHY: animationFrameScheduler is purpose-built for visual updates.
// asyncScheduler is the right choice for non-visual async work.


// ❌ INCORRECT — expecting scheduled to parallelize emissions
scheduled([fetchA(), fetchB(), fetchC()], asyncScheduler)
  .subscribe(result => console.log(result));
// This schedules each Promise sequentially, not in parallel.
// fetchA result arrives, then fetchB, then fetchC — ordered by resolution.

// ✅ CORRECT — use forkJoin or merge for parallel Promises
forkJoin([fetchA(), fetchB(), fetchC()])
  .subscribe(([a, b, c]) => console.log(a, b, c));
// WHY: scheduled sequences emissions through the scheduler.
// For parallel execution, forkJoin/merge are the right tools.
```

---

## Operator Comparison: Scheduler Control

| Scenario | Tool |
|---|---|
| Create async source from array | `scheduled(arr, asyncScheduler)` |
| Add scheduler to existing pipeline | `observeOn(scheduler)` |
| Control subscription scheduler | `subscribeOn(scheduler)` |
| Async periodic emissions | `interval(ms, scheduler)` |
| Async single delayed value | `timer(delay, scheduler)` |
| Synchronous safe recursion | `scheduled(x, queueScheduler)` + `expand` |
| Visual animation values | `scheduled(frames, animationFrameScheduler)` |

---

## Related Operators

- **`observeOn`** — add scheduler to emissions in an existing pipeline
- **`subscribeOn`** — schedule the subscription itself
- **`from`** — synchronous equivalent; no scheduler control
- **`timer`** / **`interval`** — scheduler-accepting creation operators built on `scheduled`
- **`expand`** — recursive expansion; pairs with `queueScheduler` to avoid stack overflow
- **`TestScheduler`** — virtual time testing; accept as scheduler parameter for testable pipelines
