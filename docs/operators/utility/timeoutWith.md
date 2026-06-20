# timeoutWith

**Category**: Utility  
**Import**: `import { timeoutWith } from 'rxjs';`

> **Deprecated**: `timeoutWith` is deprecated and will be removed in RxJS v8. Use the `timeout` operator with the `with` configuration property instead.

## Description

`timeoutWith` switches the subscription to a different observable if the source does not emit a value within the specified time. If the first argument is a number, it represents the maximum time (in milliseconds) allowed between any two consecutive values. If the first argument is a `Date`, the timeout applies only to the first value.

This operator is a thin convenience wrapper over `timeout({ each/first, with: () => switchTo })`. Prefer that form going forward.

## Signature

```typescript
// @deprecated — use timeout({ each: waitFor, with: () => switchTo })
function timeoutWith<T, R>(waitFor: number, switchTo: ObservableInput<R>, scheduler?: SchedulerLike): OperatorFunction<T, T | R>

// @deprecated — use timeout({ first: dueBy, with: () => switchTo })
function timeoutWith<T, R>(dueBy: Date, switchTo: ObservableInput<R>, scheduler?: SchedulerLike): OperatorFunction<T, T | R>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| due | `number \| Date` | Milliseconds between values (`number`) or exact deadline for the first value (`Date`). |
| switchTo | `ObservableInput<R>` | The observable to switch to when the timeout fires. |
| scheduler | `SchedulerLike` | Optional. Defaults to `asyncScheduler`. |

## Return Type

`OperatorFunction<T, T | R>` — mirrors the source until a timeout occurs, then emits from `switchTo`.

## Marble Diagram

```
Source:    --a--(silent for 900ms)...
switchTo:  --x--y--z--|
           timeoutWith(900, switchTo$)
Output:    --a--x--y--z--|
```

## Examples

### Example 1: Fall back to a faster observable (deprecated usage)

```typescript
import { interval, timeoutWith } from 'rxjs';

const slow$ = interval(1000);
const faster$ = interval(500);

// DEPRECATED — shown for migration reference
slow$.pipe(
  timeoutWith(900, faster$)
).subscribe(console.log);
```

### Example 2: Modern equivalent using `timeout`

```typescript
import { interval, timeout } from 'rxjs';

const slow$ = interval(1000);
const faster$ = interval(500);

// Preferred modern form
slow$.pipe(
  timeout({ each: 900, with: () => faster$ })
).subscribe(console.log);
```

### Example 3: Emit a custom error on timeout (modern form)

```typescript
import { interval, timeout, throwError } from 'rxjs';

class SlowSourceError extends Error {
  constructor() {
    super('Source was too slow');
    this.name = 'SlowSourceError';
  }
}

interval(1000).pipe(
  timeout({
    each: 800,
    with: () => throwError(() => new SlowSourceError())
  })
).subscribe({
  next: console.log,
  error: err => console.error(err.message)
});
```

## Common Pitfalls

- **This operator is deprecated**: Migrate to `timeout({ each: ..., with: () => ... })` or `timeout({ first: ..., with: () => ... })` before upgrading to RxJS v8.
- **`switchTo` is not a factory**: Unlike the `with` option in `timeout`, `switchTo` here is an observable directly (not a factory function). This means the same observable instance is reused, which can cause issues with cold observables in edge cases. The `timeout` `with` factory pattern avoids this.

## Related Operators

- `timeout` — the modern, non-deprecated replacement; supports all `timeoutWith` use cases and more
- `catchError` — for handling `TimeoutError` when no fallback observable is needed
