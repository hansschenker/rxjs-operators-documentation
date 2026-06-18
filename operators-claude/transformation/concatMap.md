# concatMap

## Identity

- **Name**: concatMap
- **Category**: Transformation Operators
- **Type**: Sequential higher-order flattener — queues inner Observables and subscribes one at a time, in source order
- **Import**:
  ```typescript
  import { concatMap } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  function concatMap<T, O extends ObservableInput<any>>(
    project: (value: T, index: number) => O
  ): OperatorFunction<T, ObservedValueOf<O>>
  ```

## Functional Specification

For each source emission, `concatMap` creates an inner Observable via `project(value)`. It subscribes to the inner Observable and **waits for it to complete** before processing the next source emission. Source emissions that arrive while an inner is active are **queued** — never dropped.

**Mathematical representation**:
```
Output = concat(project(v₁), project(v₂), project(v₃), ...)
```

**Invariants**:
- Only one inner Observable is active at any time
- Output order matches source order exactly
- Source emissions are buffered (queued) — never dropped
- If a source emits faster than inners complete, the queue grows unbounded
- Equivalent to `mergeMap(project, 1)` (concurrency capped at 1)

## Marble Diagram

```
Source:    --1--2--3--|
              |  |  |
       (2 and 3 queue while inner 1 is active)

Inner 1:   --10--20|
Inner 2:           --20--40|
Inner 3:                   --30--60|

concatMap:
Result:    --10--20--20--40--30--60--|

Fast source, slow inner (queue grows):
Source:    -1-2-3-4-5|  (all arrive before inner 1 finishes)
Result:    ----r1----r2----r3----r4----r5|
           (inner 1 completes → inner 2 starts, etc.)
```

## Behavioral Characteristics

**Subscription**: Subscribes to one inner Observable at a time. The next inner starts only after the current one completes.

**Completion**: Completes after the source completes AND the last queued inner completes. If inners never complete, the outer never completes.

**Error handling**: First error — from source or any inner — propagates immediately and cancels everything (including the queue).

**Backpressure**: Queues source emissions. A fast source with slow inners means an ever-growing in-memory queue. Rate-limit the source (debounce, throttle) if this is a concern.

## Type System Integration

```typescript
import { of } from 'rxjs';
import { concatMap } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

// Promises, arrays, and Observables are all valid inner types
of(1, 2, 3).pipe(
  concatMap(id => ajax.getJSON<User>(`/api/users/${id}`))
).subscribe((user: User) => console.log(user));
// Users arrive in order: user 1, then user 2, then user 3
```

## Examples

### Basic Usage — Sequential Processing
```typescript
import { of } from 'rxjs';
import { concatMap, delay } from 'rxjs/operators';

of(1, 2, 3).pipe(
  concatMap(n => of(n * 10).pipe(delay(100)))
).subscribe(console.log);
// Always: 10, 20, 30 — in source order, regardless of timing
```

### Common Pattern — Upload Queue
```typescript
import { Subject } from 'rxjs';
import { concatMap } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

const uploadQueue$ = new Subject<File>();

// Process uploads one at a time — no parallel uploads
uploadQueue$.pipe(
  concatMap(file =>
    ajax.post('/api/upload', file).pipe(
      tap(() => console.log(`${file.name} done`))
    )
  )
).subscribe({
  error: err => console.error('upload failed:', err)
});

// Files are uploaded sequentially in the order they were added
uploadQueue$.next(file1);
uploadQueue$.next(file2);
uploadQueue$.next(file3);
```

### Common Pattern — Ordered Animations
```typescript
import { from } from 'rxjs';
import { concatMap } from 'rxjs/operators';

const steps = ['slide-in', 'fade', 'slide-out'];

// Each animation step must complete before the next begins
from(steps).pipe(
  concatMap(step => runAnimation(step)) // runAnimation returns Observable that completes when done
).subscribe({
  complete: () => console.log('sequence done')
});
```

### Edge Case — Infinite Inner Hangs the Queue
```typescript
import { of, interval } from 'rxjs';
import { concatMap } from 'rxjs/operators';

// ⚠️ HANGS — inner 1 never completes; inner 2 and 3 wait forever in queue
of(1, 2, 3).pipe(
  concatMap(n => interval(100)) // interval never completes
).subscribe(console.log);
// Only inner 1's values arrive; 2 and 3 are queued indefinitely

// Fix: bound the inner Observable
of(1, 2, 3).pipe(
  concatMap(n => interval(100).pipe(take(3)))
).subscribe(console.log);
```

## Common Pitfalls

### Anti-pattern: Using `concatMap` for Parallel Operations
```typescript
import { from } from 'rxjs';
import { concatMap } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

// ❌ SLOW — 100 requests sent one at a time (each waits for previous)
from(Array.from({ length: 100 }, (_, i) => i)).pipe(
  concatMap(id => ajax.getJSON(`/api/items/${id}`))
).subscribe(console.log);
// Total time: sum of all request durations

// ✅ FASTER — use mergeMap for parallel requests when order doesn't matter
import { mergeMap } from 'rxjs/operators';
from(Array.from({ length: 100 }, (_, i) => i)).pipe(
  mergeMap(id => ajax.getJSON(`/api/items/${id}`), 6) // 6 concurrent
).subscribe(console.log);

// WHY: concatMap is sequential by design. Use it only when ORDER matters
// or when parallel execution would cause problems (race conditions, ordering
// constraints). For independent operations, mergeMap is significantly faster.
```

### Anti-pattern: Ignoring Queue Growth
```typescript
import { Subject } from 'rxjs';
import { concatMap } from 'rxjs/operators';

// ❌ MEMORY RISK — fast source, slow inner → queue grows without bound
const events$ = new Subject<Event>();

events$.pipe(
  concatMap(event => slowProcessing(event)) // takes 5s each
).subscribe(console.log);

// If events arrive every 100ms, queue grows by ~50 items/second

// ✅ CORRECT — rate-limit the source, or switch to exhaustMap/switchMap
import { exhaustMap } from 'rxjs/operators';
events$.pipe(
  exhaustMap(event => slowProcessing(event)) // drop new events while busy
).subscribe(console.log);

// WHY: concatMap queues ALL source emissions. exhaustMap drops excess
// when busy; switchMap cancels and uses the latest. Choose based on
// whether you can afford to drop events.
```

## Related Operators

- **`mergeMap`**: Concurrent — all inners active simultaneously, output order not guaranteed
- **`switchMap`**: Latest-only — cancels current inner when source emits again
- **`exhaustMap`**: Ignore-while-busy — drops source emissions while an inner is active
- **`concat`**: Static sequential subscription without a projection function

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/concatMap](https://rxjs.dev/api/operators/concatMap)

---

**Cognitive Load**: 3/5 | **Usage Frequency**: 5/5 | **Composability**: 5/5
**Key teaching points**:
1. Sequential and ordered — each inner must complete before the next starts
2. Queues (never drops) source emissions — watch for unbounded queue growth with fast sources
3. If an inner never completes, all subsequent inners are stuck in the queue
