# observeOn

**Category**: Utility  
**Import**: `import { observeOn } from 'rxjs';`

## Description

`observeOn` re-emits all notifications from the source Observable — `next`, `error`, and `complete` — using the specified scheduler. It does not change what is emitted or when the source emits internally; it only reschedules the forwarding of those notifications to downstream observers.

This is useful when you need values to be delivered on a specific execution context. For example, use `animationFrameScheduler` to ensure DOM updates happen just before a browser repaint, or use `asapScheduler` / `asyncScheduler` to push synchronous work off the current call stack.

The key difference from `delay`: `observeOn` reschedules all notifications including errors, while `delay` only shifts `next` notifications and passes errors through immediately.

## Signature

```typescript
function observeOn<T>(scheduler: SchedulerLike, delay?: number): MonoTypeOperatorFunction<T>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| scheduler | `SchedulerLike` | The scheduler used to reschedule notifications. Common choices: `asyncScheduler`, `asapScheduler`, `animationFrameScheduler`, `queueScheduler`. |
| delay | `number` | Optional milliseconds by which to further delay each notification. Defaults to `0`. |

## Return Type

`MonoTypeOperatorFunction<T>` — an operator that returns an Observable emitting the same values as the source, but with notifications delivered on the specified scheduler.

## Marble Diagram

```
Source (sync):  (a)(b)(c)|
                observeOn(asyncScheduler)
Output:         --a--b--c--|
                (values deferred to next microtask/macrotask tick)
```

## Examples

### Example 1: Drive smooth animations with `animationFrameScheduler`

```typescript
import { interval, observeOn, animationFrameScheduler } from 'rxjs';

const box = document.getElementById('animated-box')!;

interval(0, animationFrameScheduler).pipe(
  observeOn(animationFrameScheduler)
).subscribe(frame => {
  box.style.transform = `translateX(${frame % 300}px)`;
});
```

### Example 2: Prevent blocking the UI with synchronous sources

```typescript
import { range, observeOn, asyncScheduler } from 'rxjs';

// Without observeOn, this runs synchronously and blocks the thread
range(1, 10_000).pipe(
  observeOn(asyncScheduler)
).subscribe(n => processItem(n));

function processItem(n: number) {
  // each item is delivered asynchronously, not in a single blocking loop
}
```

### Example 3: Compare `observeOn` vs `subscribeOn`

```typescript
import { of, merge, observeOn, subscribeOn, asyncScheduler } from 'rxjs';

const a$ = of(1, 2, 3).pipe(observeOn(asyncScheduler));   // delivery is async
const b$ = of(4, 5, 6);                                    // delivery is sync

merge(a$, b$).subscribe(console.log);
// 4, 5, 6 (synchronous, logged first)
// 1, 2, 3 (async, logged after current call stack clears)
```

## Common Pitfalls

- **Does not change the source scheduler**: `observeOn` is placed after the source in a pipe. It does not affect how the source generates values internally; it only affects how those values are forwarded downstream.
- **Anti-pattern for synchronous firehoses**: Using `observeOn` after a source that emits thousands of values synchronously does not solve backpressure — it just defers all those emissions asynchronously. You need to adjust the source's scheduler directly for that.
- **Error notifications are also rescheduled**: Unlike `delay`, errors will also be held and delivered on the next scheduler tick. This changes error propagation timing.

## Related Operators

- `subscribeOn` — controls the scheduler used when subscribing to the source (affects when subscription side effects run)
- `delay` — shifts `next` emissions by a fixed time; passes errors through immediately
- `observeOn` with `animationFrameScheduler` — standard pattern for smooth DOM animations
