# subscribeOn

**Category**: Utility  
**Import**: `import { subscribeOn } from 'rxjs';`

## Description

`subscribeOn` schedules the subscription to the source Observable on the specified scheduler. Rather than controlling when values are delivered to observers (that's `observeOn`'s role), `subscribeOn` controls when the act of subscribing itself takes place. This is useful for controlling the order in which multiple observables are subscribed to when they are combined with operators like `merge`.

Schedulers control the speed and order of emissions to observers from an Observable stream. By deferring subscription via `subscribeOn(asyncScheduler)`, you push that observable's execution to the next macrotask, allowing synchronous observables to complete first.

## Signature

```typescript
function subscribeOn<T>(scheduler: SchedulerLike, delay?: number): MonoTypeOperatorFunction<T>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| scheduler | `SchedulerLike` | The scheduler on which to perform the subscription action. |
| delay | `number` | Optional delay (in ms) before the subscription is scheduled. Defaults to `0`. |

## Return Type

`MonoTypeOperatorFunction<T>` — an operator that returns an Observable whose subscription to the source is deferred to the specified scheduler.

## Marble Diagram

```
a$ (sync):  (1)(2)(3)|
b$ (sync):  (4)(5)(6)|

a$ with subscribeOn(asyncScheduler):
merge(a$, b$) → 4,5,6 (sync) then 1,2,3 (next tick)
```

## Examples

### Example 1: Control merge order with `asyncScheduler`

```typescript
import { of, merge, subscribeOn, asyncScheduler } from 'rxjs';

const a$ = of(1, 2, 3).pipe(subscribeOn(asyncScheduler));
const b$ = of(4, 5, 6);

merge(a$, b$).subscribe(console.log);

// Output:
// 4
// 5
// 6
// 1
// 2
// 3
// (b$ subscribes and completes synchronously; a$ subscribes after current call stack)
```

### Example 2: Run a heavy initializer off the main thread (conceptually)

```typescript
import { defer, subscribeOn, asyncScheduler } from 'rxjs';

const heavyWork$ = defer(() => {
  console.log('Starting heavy work...');
  return computeResults();
}).pipe(
  subscribeOn(asyncScheduler) // defer subscription to next tick
);

console.log('Registered pipeline, work not started yet');
heavyWork$.subscribe(result => console.log('Done:', result));

// Registered pipeline, work not started yet
// Starting heavy work...  (on next tick)
```

### Example 3: Contrast with `observeOn`

```typescript
import { of, subscribeOn, observeOn, asyncScheduler } from 'rxjs';

// subscribeOn: controls WHEN the source is subscribed to
of(1, 2, 3).pipe(
  subscribeOn(asyncScheduler)
).subscribe(n => console.log('subscribeOn value:', n));

// observeOn: controls WHEN notifications reach the subscriber
of(4, 5, 6).pipe(
  observeOn(asyncScheduler)
).subscribe(n => console.log('observeOn value:', n));

console.log('Synchronous code runs first');

// Synchronous code runs first
// subscribeOn value: 1  (subscription deferred to next tick)
// subscribeOn value: 2
// subscribeOn value: 3
// observeOn value: 4    (source subscribed sync, but delivery deferred)
// observeOn value: 5
// observeOn value: 6
```

## Common Pitfalls

- **`subscribeOn` vs `observeOn`**: `subscribeOn` affects when the source is subscribed to; `observeOn` affects when emitted values reach the observer. Use `observeOn` for controlling delivery context (e.g., UI thread). Use `subscribeOn` for controlling subscription ordering.
- **Rarely needed in application code**: Most real-world use cases that seem to require `subscribeOn` are better solved with `observeOn` or by choosing an appropriate scheduler for the source operator.
- **Does not affect synchronous emissions after subscription**: Once the subscription fires, the source runs synchronously (unless the source itself is asynchronous).

## Related Operators

- `observeOn` — controls the scheduler used to deliver notifications to observers
- `delay` — shifts `next` emissions by a fixed duration without changing subscription timing
