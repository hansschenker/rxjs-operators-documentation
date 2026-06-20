# repeatWhen

**Category**: Utility  
**Import**: `import { repeatWhen } from 'rxjs';`

> **Deprecated**: `repeatWhen` is deprecated and will be removed in RxJS v9 or v10. Use the `repeat` operator with the `delay` option instead.

## Description

`repeatWhen` mirrors the source Observable with the exception of `complete` notifications. When the source completes, `repeatWhen` passes that completion to a `notifier` function via a `Subject`. If the notifier Observable emits a value, the source is re-subscribed. If the notifier completes or errors, the result Observable completes or errors accordingly.

This operator was the primary way to implement conditional or event-driven repetition before RxJS introduced `repeat({ delay })`. Use the modern `repeat` API going forward.

## Signature

```typescript
function repeatWhen<T>(
  notifier: (notifications: Observable<void>) => ObservableInput<any>
): MonoTypeOperatorFunction<T>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| notifier | `(notifications: Observable<void>) => ObservableInput<any>` | A function that receives an Observable of completion signals and returns an Observable. Emitting from the returned observable triggers a resubscription. Completing it stops repetition. |

## Return Type

`MonoTypeOperatorFunction<T>` — an operator that returns an Observable that re-subscribes to the source whenever the notifier emits.

## Marble Diagram

```
Source:    --a--b--|
           (notifier emits on click)
           repeatWhen(() => click$)
Output:    --a--b--------a--b--------a--b--|
                 (click)(re-subscribed)
```

## Examples

### Example 1: Deprecated usage — repeat on click

```typescript
import { of, fromEvent, repeatWhen } from 'rxjs';

// DEPRECATED — shown for migration reference
const source$ = of('Hello from repeatWhen');
const click$ = fromEvent(document, 'click');

source$.pipe(
  repeatWhen(() => click$)
).subscribe(console.log);

// Logs 'Hello from repeatWhen' on each click
```

### Example 2: Modern equivalent using `repeat({ delay })`

```typescript
import { of, fromEvent, repeat } from 'rxjs';

// Preferred modern form
const source$ = of('Hello from repeat');
const click$ = fromEvent(document, 'click');

source$.pipe(
  repeat({ delay: () => click$ })
).subscribe(console.log);

// Logs 'Hello from repeat' on each click
```

### Example 3: Polling with delay — modern form

```typescript
import { defer, from, repeat, timer } from 'rxjs';

// Modern polling pattern (do NOT use repeatWhen for this)
const poll$ = defer(() =>
  from(fetch('/api/status').then(r => r.json()))
).pipe(
  repeat({ delay: 3000 })
);

// Old pattern (deprecated) for reference:
// defer(() => from(fetch(...))).pipe(
//   repeatWhen(completions$ => completions$.pipe(delay(3000)))
// )

poll$.subscribe(console.log);
```

## Common Pitfalls

- **This operator is deprecated**: Migrate to `repeat({ delay: () => notifier$ })` before upgrading to future RxJS versions.
- **The notifier receives `void` values**: The subject passed to the `notifier` function emits `void` (not the last value from the source). If you need access to previous values, you must capture them separately via `tap`.
- **Notifier completing stops repetition**: If your notifier observable completes (e.g., `take(1)`), the resulting observable completes rather than errors, which can be confusing.

## Related Operators

- `repeat` — the modern, non-deprecated replacement; use `repeat({ delay: () => notifier$ })` for the same behavior
- `retryWhen` — the analogous (also deprecated) operator for error-based resubscription; replaced by `retry({ delay })`
