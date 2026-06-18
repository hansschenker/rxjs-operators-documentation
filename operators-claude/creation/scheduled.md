# scheduled

## Identity

- **Name**: scheduled
- **Category**: Creation Operators
- **Type**: Scheduler-injected creation — wraps any creation source with a specific scheduler
- **Import**:
  ```typescript
  import { scheduled } from 'rxjs';
  ```
- **Signature**:
  ```typescript
  function scheduled<T>(
    input: ObservableInput<T>,
    scheduler: SchedulerLike
  ): Observable<T>
  ```

## Functional Specification

`scheduled(input, scheduler)` is equivalent to `from(input)` but delivers all emissions (including subscription) through the provided scheduler.

**What it accepts** (same as `from`):
- Observable / Promise / AsyncIterable
- Array / Iterable
- `ReadableStream`

**Effect of the scheduler**: Every `next`, `error`, and `complete` notification is dispatched through the scheduler's queue — giving you control over which thread/frame values arrive in.

**When to reach for `scheduled` vs alternatives**:

| Approach | When to use |
|---|---|
| `from(source)` | Don't need scheduler control |
| `scheduled(source, asyncScheduler)` | Emit asynchronously (avoid synchronous blocking) |
| `scheduled(source, queueScheduler)` | Recursive scheduling without stack overflow |
| `scheduled(source, animationFrameScheduler)` | Emit values on animation frames |
| `observeOn(scheduler)` | Add scheduler to an existing pipeline |

**`observeOn` vs `scheduled`**: `scheduled` affects the subscription and all emissions from the start. `observeOn` only affects emissions downstream of its position in the pipe chain. For sources you create, prefer `scheduled`; for existing Observables mid-pipe, use `observeOn`.

## Marble Diagram

```
// Synchronous (from):
from([1, 2, 3]):   (123|)    ← all in same frame

// Async scheduled:
scheduled([1, 2, 3], asyncScheduler):
                   ---(123|) ← deferred to next microtask/task
```

## Examples

### Basic Usage — Async Array Emission
```typescript
import { scheduled } from 'rxjs';
import { asyncScheduler } from 'rxjs';

console.log('before subscribe');

scheduled([1, 2, 3], asyncScheduler).subscribe({
  next:     v  => console.log('value:', v),
  complete: () => console.log('done')
});

console.log('after subscribe');
// Output:
// before subscribe
// after subscribe      ← synchronous code runs first
// value: 1
// value: 2
// value: 3
// done
```

### Common Pattern — Prevent Synchronous Blocking
```typescript
import { scheduled, from } from 'rxjs';
import { asyncScheduler } from 'rxjs';
import { mergeMap } from 'rxjs/operators';

// Avoid blocking the main thread with large arrays:
function processLargeDataset(items: Item[]) {
  return scheduled(items, asyncScheduler).pipe(
    mergeMap(item => processItem(item))
  );
}
```

### Common Pattern — Queue Scheduler for Recursive Processing
```typescript
import { scheduled } from 'rxjs';
import { queueScheduler } from 'rxjs';
import { expand, take } from 'rxjs/operators';

// queueScheduler prevents stack overflow in recursive expand
const fibonacci$ = scheduled([{ a: 0, b: 1 }], queueScheduler).pipe(
  expand(({ a, b }) => scheduled([{ a: b, b: a + b }], queueScheduler)),
  take(15)
);
```

### Common Pattern — Animation Frame Scheduling
```typescript
import { scheduled } from 'rxjs';
import { animationFrameScheduler } from 'rxjs';

// Emit DOM updates on animation frames
scheduled(domUpdates, animationFrameScheduler).subscribe(applyUpdate);
// Each update is applied at the next rAF tick
```

### Advanced — Custom Scheduler for Testing
```typescript
import { scheduled } from 'rxjs';
import { TestScheduler } from 'rxjs/testing';

// In tests: use TestScheduler to control time
const testScheduler = new TestScheduler((actual, expected) =>
  expect(actual).toEqual(expected)
);

testScheduler.run(({ cold, expectObservable }) => {
  const source = scheduled([1, 2, 3], testScheduler);
  expectObservable(source).toBe('(123|)');
});
```

## Common Pitfalls

### Using `scheduled` When `observeOn` Is More Appropriate
```typescript
import { scheduled, from } from 'rxjs';
import { asyncScheduler } from 'rxjs';
import { map } from 'rxjs/operators';
import { observeOn } from 'rxjs/operators';

// ❌ AWKWARD — wrapping an existing Observable with scheduled
const source$ = someObservable$.pipe(toArray()); // not an array
// scheduled(source$, ...) — works but unusual

// ✅ CORRECT — use observeOn for existing Observables
someObservable$.pipe(
  observeOn(asyncScheduler),
  map(transform)
).subscribe(handler);
// WHY: scheduled() is for creation sources (arrays, iterables, promises).
// observeOn() is for inserting a scheduler into an existing pipe.
```

### Forgetting `scheduled` Delivers the Subscription Asynchronously Too
```typescript
import { scheduled } from 'rxjs';
import { asyncScheduler } from 'rxjs';

const result: number[] = [];
scheduled([1, 2, 3], asyncScheduler).subscribe(v => result.push(v));
console.log(result); // [] — not [1, 2, 3]!
// WHY: asyncScheduler defers ALL notifications including the subscription setup.
// Use from() if you need synchronous values, scheduled() for async delivery.
```

## Related Operators

- **`from`**: Synchronous creation from same input types
- **`observeOn`**: Add scheduler downstream to existing Observable
- **`subscribeOn`**: Schedule when subscription setup happens
- **`asyncScheduler`**: Macro-task queue (setTimeout)
- **`queueScheduler`**: Current thread queue (recursive-safe)
- **`animationFrameScheduler`**: rAF-based scheduler

---

**Cognitive Load**: 2/5 | **Usage Frequency**: 2/5 | **Composability**: 3/5
**Teaching note**: Teach `from` first; introduce `scheduled` only when scheduler control is needed. The key insight: `scheduled` = `from` + scheduler awareness for every notification.
