# mergeMap

## Identity

- **Name**: mergeMap (alias: `flatMap`)
- **Category**: Transformation Operators
- **Type**: Concurrent higher-order flattener — maps each value to an Observable and merges all active inner Observables
- **Import**:
  ```typescript
  import { mergeMap } from 'rxjs/operators';
  ```
- **Signature**:
  ```typescript
  function mergeMap<T, O extends ObservableInput<any>>(
    project:    (value: T, index: number) => O,
    concurrent?: number
  ): OperatorFunction<T, ObservedValueOf<O>>
  ```

## Functional Specification

For each source emission, `mergeMap` calls `project(value)` to create an inner Observable and **immediately subscribes** to it — without waiting for previous inner Observables to complete. All active inner Observables run concurrently; their emissions are merged into the output in arrival order.

**Mathematical representation**:
```
Output = merge(project(v₁), project(v₂), project(v₃), ...)
```

**Invariants**:
- Output order is non-deterministic — it depends on inner Observable timing, not source order
- No cancellation — a new inner Observable does NOT cancel previous ones
- Source completion does NOT cancel active inner Observables; the outer Observable waits for ALL to complete
- `concurrent` (default: `Infinity`) caps how many inner Observables can be active simultaneously; excess are queued

## Marble Diagram

```
Source:   --1---------2-----3------|
              \         \     \
          project(x) → interval that emits x, x after Xms
              \         \     \
Inner 1:      ----10|
Inner 2:            ----20|
Inner 3:                  ----30|

mergeMap(project):
Result:   ------10--------20----30----|   (all concurrent, order by arrival)

Overlapping inners (source faster than inner completes):
Source:   --a--b--c--|
Inner a:  ---a1--a2|
Inner b:     ---b1--b2|
Inner c:        ---c1--c2|

Result:   ---a1b1a2c1b2--c2--|   (interleaved by timing)
```

## Behavioral Characteristics

**Subscription**: Subscribes to each inner Observable as source emits — all run concurrently up to `concurrent` limit.

**Completion**: Completes only after the source completes AND all active inner Observables complete.

**Error handling**: First error from any source (outer or inner) propagates immediately and cancels everything.

**Backpressure**: No automatic backpressure. With `concurrent` set, excess inner Observables are queued until a slot opens.

## Type System Integration

```typescript
import { of } from 'rxjs';
import { mergeMap } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

// Promises, arrays, and Observables are all valid inner types
of(1, 2, 3).pipe(
  mergeMap(id => ajax.getJSON<User>(`/api/users/${id}`))
).subscribe((user: User) => console.log(user));

// Union types preserved
of(1, 2, 3).pipe(
  mergeMap(id =>
    ajax.getJSON<string>(`/api/${id}`).pipe(
      map((data): Success => ({ type: 'success', data })),
      catchError((err): Observable<Failure> => of({ type: 'error', message: err.message }))
    )
  )
).subscribe(result => {
  if (result.type === 'success') console.log(result.data); // TypeScript narrows here
});
```

## Examples

### Basic Usage — Parallel HTTP Requests
```typescript
import { of } from 'rxjs';
import { mergeMap } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

of(1, 2, 3).pipe(
  mergeMap(id => ajax.getJSON<User>(`/api/users/${id}`))
).subscribe(user => console.log(user));
// All 3 requests fire simultaneously; results arrive in response-time order
```

### Common Pattern — Bounded Concurrency
```typescript
import { range } from 'rxjs';
import { mergeMap } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

// Process 100 items, max 6 concurrent (respects browser connection limit)
range(1, 100).pipe(
  mergeMap(
    id => ajax.post('/api/process', { id }),
    6  // concurrent limit
  )
).subscribe(result => console.log(result));
```

### Common Pattern — Per-Item Error Recovery
```typescript
import { from } from 'rxjs';
import { mergeMap, catchError, of } from 'rxjs/operators';
import { ajax } from 'rxjs/ajax';

const ids = [1, 2, 3, 4, 5];

// One item's failure doesn't kill the stream
from(ids).pipe(
  mergeMap(id =>
    ajax.getJSON<Item>(`/api/items/${id}`).pipe(
      catchError(err => of({ id, error: err.message })) // isolate each error
    )
  )
).subscribe(result => console.log(result));
```

## Common Pitfalls

### Anti-pattern: Expecting Ordered Output
```typescript
import { of } from 'rxjs';
import { mergeMap, delay } from 'rxjs/operators';

// ❌ INCORRECT — assumes output follows source order
of(1, 2, 3).pipe(
  mergeMap(n => of(n * 10).pipe(delay(Math.random() * 500)))
).subscribe(v => console.log(v));
// May print: 30, 10, 20 — non-deterministic

// ✅ CORRECT — use concatMap when order must be preserved
import { concatMap } from 'rxjs/operators';
of(1, 2, 3).pipe(
  concatMap(n => of(n * 10).pipe(delay(Math.random() * 500)))
).subscribe(v => console.log(v));
// Always: 10, 20, 30

// WHY: mergeMap subscribes to all inners concurrently; the faster inner
// wins. Use concatMap to queue inners and preserve source order.
```

### Anti-pattern: Infinite Inner Observables Without Cleanup
```typescript
import { fromEvent, interval } from 'rxjs';
import { mergeMap } from 'rxjs/operators';

// ❌ INCORRECT — each click adds a new forever-running interval
fromEvent(document, 'click').pipe(
  mergeMap(() => interval(1000)) // never completes; accumulates per click
).subscribe(console.log);
// After 50 clicks: 50 active intervals, memory leaks

// ✅ CORRECT — use switchMap to cancel previous, or bound the inner
import { switchMap, take } from 'rxjs/operators';

fromEvent(document, 'click').pipe(
  switchMap(() => interval(1000)) // cancel-and-restart on each click
).subscribe(console.log);

// Or limit inner lifetime:
fromEvent(document, 'click').pipe(
  mergeMap(() => interval(1000).pipe(take(10)))
).subscribe(console.log);

// WHY: mergeMap never cancels previous inner Observables.
// Infinite inner Observables will run until the outer subscription ends.
// switchMap is safer when only the latest inner matters.
```

## Related Operators

- **`concatMap`**: Sequential — queues inners, preserves order
- **`switchMap`**: Latest-only — cancels previous inner on new source emission
- **`exhaustMap`**: Ignore-while-busy — drops new emissions while an inner is active
- **`mergeAll`**: Flattens `Observable<Observable<T>>` without a projection function

## References
- **RxJS Official Docs**: [https://rxjs.dev/api/operators/mergeMap](https://rxjs.dev/api/operators/mergeMap)

---

**Cognitive Load**: 4/5 | **Usage Frequency**: 5/5 | **Composability**: 5/5
**Teaching sequence**: Teach after `map`; introduce the "what if project returns an Observable?" problem, then show mergeMap as the flattening solution. Contrast with concatMap/switchMap/exhaustMap to complete the four-strategy picture.
