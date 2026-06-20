# delayWhen

**Category**: Utility  
**Import**: `import { delayWhen } from 'rxjs';`

## Description

`delayWhen` delays each emitted value from the source by a duration that is determined individually, per value. A `delayDurationSelector` function is called for each source emission and must return an `ObservableInput`. The source value is forwarded to the output only when that "duration observable" emits its first `next` value, after which the duration observable is unsubscribed.

This is more flexible than `delay` when you need variable delays — for example, backing off exponentially, or waiting for a resource to become ready before forwarding a particular event.

**Note**: As of RxJS v7, only the first `next` notification from the duration observable triggers emission. If the duration observable completes without emitting, the corresponding source value is silently dropped.

## Signature

```typescript
function delayWhen<T>(
  delayDurationSelector: (value: T, index: number) => ObservableInput<any>
): MonoTypeOperatorFunction<T>
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| delayDurationSelector | `(value: T, index: number) => ObservableInput<any>` | A function called for each source value. Returns an `ObservableInput` whose first `next` emission triggers forwarding the source value downstream. Also receives the zero-based emission index. |

## Return Type

`MonoTypeOperatorFunction<T>` — an operator that returns an Observable where each value is delayed until the corresponding duration observable emits.

## Marble Diagram

```
Source:   --a------b--c--|
Delay(a): -------|
Delay(b): --|
Delay(c): ------|
          delayWhen(delayDurationSelector)
Output:   --------a--b-----c--|
```

## Examples

### Example 1: Random delay per click event

```typescript
import { fromEvent, delayWhen, timer } from 'rxjs';

const clicks$ = fromEvent(document, 'click');

clicks$.pipe(
  delayWhen(() => timer(Math.random() * 2000))
).subscribe(event => {
  console.log('Delayed click:', event);
});
```

### Example 2: Exponential back-off per retry signal

```typescript
import { Subject, delayWhen, timer } from 'rxjs';

const retryTrigger$ = new Subject<number>();

// Each retry attempt is delayed by attempt * 1000ms
retryTrigger$.pipe(
  delayWhen((attempt, index) => timer(index * 1000))
).subscribe(attempt => {
  console.log(`Retrying attempt ${attempt}`);
});

retryTrigger$.next(1); // delayed 0ms
retryTrigger$.next(2); // delayed 1s
retryTrigger$.next(3); // delayed 2s
```

### Example 3: Wait for a dependent resource before forwarding each value

```typescript
import { of, delayWhen, fromEvent } from 'rxjs';

const userIds$ = of(101, 102, 103);
const resourceReady$ = fromEvent(document, 'resourceready');

// Each userId is forwarded only after the next 'resourceready' event
userIds$.pipe(
  delayWhen(() => resourceReady$)
).subscribe(id => {
  console.log('Processing user:', id);
});
```

## Common Pitfalls

- **Dropped values (v7+)**: If the duration observable completes without emitting a `next` value, the corresponding source value is silently swallowed. Ensure your duration observables always emit at least one `next` before completing if you want all values forwarded.
- **Errors propagate**: If any duration observable errors, that error is forwarded to the output observable.
- **`subscriptionDelay` parameter removed in v8**: The second optional `subscriptionDelay` argument was deprecated in v7 and will be removed in v8. Do not use it.

## Related Operators

- `delay` — delay all values by the same fixed duration
- `throttle` — suppress values that arrive before a per-value duration observable emits
- `debounce` — emit the latest value after a quiet period determined by a per-value observable
